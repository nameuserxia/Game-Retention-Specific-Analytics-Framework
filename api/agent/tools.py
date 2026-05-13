# -*- coding: utf-8 -*-
"""Tools exposed to the retention diagnosis agent."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd

from api.models.schemas import AnalysisConfig, ParamMappingConfig
from core.analytics import FieldConfig, calculate_retention


@dataclass
class AgentToolbox:
    """Skill package used by RetentionDiagnosisAgent.

    The toolbox is intentionally a controlled Python runtime: the agent can invoke
    data tools implemented here, while arbitrary user code execution is avoided.
    """

    field_config: FieldConfig
    mapping: Dict[str, Any]
    analysis_config: AnalysisConfig
    param_config: Optional[ParamMappingConfig] = None

    def inspect_data(self, df: pd.DataFrame, virtual_fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """Inspect data health without treating unrelated event rows as missing params.

        Virtual fields generated from JSON params are event-specific in most game logs.
        Therefore we only raise parameter-missing risks after the user configures
        relevant_events and those events are matched in event_name.
        """

        uid = self.field_config.user_id
        event_time = self.field_config.event_time
        event_date = self.field_config.event_date
        reg_date = self.field_config.reg_date
        event_name = self.field_config.event_name
        virtual_fields = virtual_fields or []

        risks: List[str] = []
        warnings: List[str] = []
        notes: List[str] = []
        null_rates: Dict[str, float] = {}

        for col in [uid, event_time, event_date, reg_date, event_name]:
            if col not in df.columns:
                continue
            rate = float(df[col].isna().mean())
            null_rates[col] = round(rate, 4)
            if rate > 0.01:
                risks.append(f"{col} 空值率 {rate:.1%}，可能影响分析口径")

        relevant_events = [
            str(item).strip()
            for item in (self.param_config.relevant_events if self.param_config else [])
            if str(item).strip()
        ]
        event_mask = None
        if relevant_events and event_name in df.columns:
            normalized_events = {item.lower() for item in relevant_events}
            event_mask = df[event_name].astype(str).str.strip().str.lower().isin(normalized_events)

        total_users = int(df[uid].nunique()) if uid in df.columns else 0
        virtual_field_health: Dict[str, Any] = {
            "mode": "event_aligned" if event_mask is not None else "not_evaluated",
            "relevant_events": relevant_events,
            "matched_event_rows": int(event_mask.sum()) if event_mask is not None else 0,
            "fields": {},
            "notes": notes,
        }

        if virtual_fields and not relevant_events:
            notes.append("未配置 relevant_events，虚拟字段仅做全量覆盖观察，不判定参数缺失")
        elif relevant_events and event_mask is None:
            notes.append("已配置 relevant_events，但事件名字段不可用，本次不判定参数缺失")
        elif event_mask is not None and int(event_mask.sum()) == 0:
            notes.append("relevant_events 未命中任何日志行，请检查事件名是否与 event_name 完全一致")

        for col in virtual_fields:
            if col not in df.columns:
                continue

            global_missing_rate = float(df[col].isna().mean())
            null_rates[col] = round(global_missing_rate, 4)

            users_with_value = int(df.loc[df[col].notna(), uid].nunique()) if uid in df.columns else 0
            global_user_coverage = users_with_value / total_users if total_users else 0.0

            field_health: Dict[str, Any] = {
                "global_missing_rate": round(global_missing_rate, 4),
                "global_user_coverage_rate": round(float(global_user_coverage), 4),
                "event_aligned": event_mask is not None,
                "status": "observed_only",
            }

            if event_mask is not None:
                scoped = df.loc[event_mask]
                relevant_rows = int(len(scoped))
                relevant_users = int(scoped[uid].nunique()) if uid in scoped.columns else 0
                relevant_missing = float(scoped[col].isna().mean()) if relevant_rows else None
                relevant_users_with_value = int(scoped.loc[scoped[col].notna(), uid].nunique()) if uid in scoped.columns else 0
                relevant_user_coverage = relevant_users_with_value / relevant_users if relevant_users else None

                field_health.update({
                    "relevant_event_rows": relevant_rows,
                    "relevant_event_missing_rate": round(relevant_missing, 4) if relevant_missing is not None else None,
                    "relevant_users": relevant_users,
                    "relevant_user_coverage_rate": round(float(relevant_user_coverage), 4) if relevant_user_coverage is not None else None,
                    "status": "passed",
                })

                if relevant_rows == 0:
                    field_health["status"] = "no_matching_events"
                elif relevant_missing is not None and relevant_missing > 0.2 and relevant_user_coverage is not None and relevant_user_coverage < 0.8:
                    field_health["status"] = "failed"
                    risks.append(
                        f"{col} 在相关事件内缺失率 {relevant_missing:.1%}，用户级覆盖度 {relevant_user_coverage:.1%}，存在参数采集风险"
                    )
                elif relevant_missing is not None and relevant_missing > 0.2:
                    field_health["status"] = "warning"
                    warnings.append(f"{col} 在相关事件内缺失率 {relevant_missing:.1%}，但用户级覆盖度尚可，建议复核事件范围")
                elif relevant_user_coverage is not None and relevant_user_coverage < 0.8:
                    field_health["status"] = "warning"
                    warnings.append(f"{col} 在相关事件用户中的覆盖度 {relevant_user_coverage:.1%}，建议检查是否有用户未上报该参数")

            virtual_field_health["fields"][col] = field_health

        duplicate_rows = 0
        if uid in df.columns and event_time in df.columns:
            duplicate_rows = int(df.duplicated(subset=[uid, event_time]).sum())
            if duplicate_rows:
                risks.append(f"存在 {duplicate_rows} 条 user_id + event_time 重复记录")

        daily_logs = {}
        date_continuity = {"continuous": False, "missing_dates": []}
        if event_date in df.columns and pd.api.types.is_datetime64_any_dtype(df[event_date]):
            daily = df.groupby(df[event_date].dt.date).size()
            daily_logs = {str(day): int(count) for day, count in daily.items()}
            if len(daily):
                full = pd.date_range(daily.index.min(), daily.index.max(), freq="D").date
                missing = sorted(set(full) - set(daily.index))
                date_continuity = {
                    "continuous": len(missing) == 0,
                    "missing_dates": [str(day) for day in missing[:20]],
                }
                if missing:
                    risks.append(f"日志日期不连续，缺失 {len(missing)} 天")

        quality_score = max(0, 100 - len(risks) * 12 - len(warnings) * 4 - min(20, duplicate_rows // 10000))
        return {
            "quality_score": int(quality_score),
            "risks": risks,
            "warnings": warnings,
            "notes": notes,
            "null_rates": null_rates,
            "virtual_field_health": virtual_field_health,
            "duplicate_rows": duplicate_rows,
            "daily_logs": daily_logs,
            "date_continuity": date_continuity,
        }

    def calculate_retention(self, df: pd.DataFrame, segment_cols: Optional[List[str]] = None) -> Dict[str, Any]:
        reg_start = pd.Timestamp(self.analysis_config.reg_start)
        reg_end = pd.Timestamp(self.analysis_config.reg_end)
        days = self.analysis_config.retention_days

        overall = calculate_retention(
            df,
            self.field_config,
            reg_start,
            reg_end,
            retention_days=days,
        ).to_dict(orient="records")

        segments = {}
        for col in segment_cols or []:
            if col not in df.columns:
                continue
            try:
                segments[col] = calculate_retention(
                    df,
                    self.field_config,
                    reg_start,
                    reg_end,
                    retention_days=days,
                    segment_col=col,
                ).head(20).to_dict(orient="records")
            except Exception as exc:
                segments[col] = [{"error": str(exc)}]

        return {"overall": overall, "segments": segments}

    def train_diagnostic_model(self, df: pd.DataFrame) -> Dict[str, Any]:
        return {
            "status": "disabled",
            "reason": "Correlation-based attribution is disabled in the current phase.",
        }

    def plot_visuals(
        self,
        df: pd.DataFrame,
        cohort_headers: List[str],
        cohort_matrix: List[List[Any]],
        funnel_steps: Optional[List[Dict[str, Any]]] = None,
        legacy_model_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        line_series = []
        if self.field_config.event_date in df.columns:
            daily = df.groupby(df[self.field_config.event_date].dt.date)[self.field_config.user_id].nunique()
            line_series = [{"date": str(k), "value": int(v)} for k, v in daily.items()]

        return {
            "retention_heatmap": {
                "type": "heatmap",
                "headers": cohort_headers,
                "matrix": cohort_matrix,
            },
            "segment_line": {
                "type": "line",
                "series": line_series,
                "metric": "DAU",
            },
            "funnel": {
                "type": "funnel",
                "steps": funnel_steps or [],
            },
        }
