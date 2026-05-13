# -*- coding: utf-8 -*-
"""Four-stage automated diagnosis modules."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd

from api.models.schemas import ParamMappingConfig
from core.analytics import FieldConfig, calculate_retention


@dataclass
class SpecializedAnalyzer:
    """Base class for future business-specific analyzers."""

    df: pd.DataFrame
    field_config: FieldConfig
    param_config: Optional[ParamMappingConfig] = None
    virtual_fields: Optional[List[str]] = None

    def require_virtual_field(self, key: Optional[str]) -> Optional[str]:
        if not key:
            return None
        col = f"v_{key}"
        return col if col in self.df.columns else None

    def relevant_event_mask(self) -> tuple[Optional[pd.Series], List[str]]:
        relevant_events = [
            str(item).strip()
            for item in (self.param_config.relevant_events if self.param_config else [])
            if str(item).strip()
        ]
        event_col = self.field_config.event_name
        if not relevant_events or event_col not in self.df.columns:
            return None, relevant_events
        normalized_events = {item.lower() for item in relevant_events}
        mask = self.df[event_col].astype(str).str.strip().str.lower().isin(normalized_events)
        return mask, relevant_events


class RetentionDiagnosticAnalyzer(SpecializedAnalyzer):
    """Generic retention diagnosis brain built on virtual fields."""

    def run(
        self,
        reg_start: pd.Timestamp,
        reg_end: pd.Timestamp,
        retention_days: int,
    ) -> Dict[str, Any]:
        stage1 = self.data_sanity()
        stage2 = self.segmentation(reg_start, reg_end, retention_days)
        stage3 = self.funnel()
        stage4 = self.context_attribution(reg_start, reg_end)

        top_contributor = stage2.get("top_contributor") or {}
        phenomenon = stage1["issues"][0] if stage1["issues"] else "整体数据质量通过基础校验"
        attribution = top_contributor.get("summary") or stage4.get("summary") or "未发现单一分群可解释主要波动"
        suggestion = self._suggest(stage1, stage2, stage3, stage4)

        return {
            "data_sanity": stage1,
            "segmentation": stage2,
            "funnel": stage3,
            "context_attribution": stage4,
            "structured_diagnosis": {
                "phenomenon": phenomenon,
                "attribution": attribution,
                "suggestion": suggestion,
            },
        }

    def data_sanity(self) -> Dict[str, Any]:
        issues: List[str] = []
        warnings: List[str] = []
        fill_rates: Dict[str, float] = {}
        virtual_field_health: Dict[str, Any] = {}
        event_mask, relevant_events = self.relevant_event_mask()

        uid = self.field_config.user_id
        total_users = int(self.df[uid].nunique()) if uid in self.df.columns else 0

        for col in self.virtual_fields or []:
            if col not in self.df.columns:
                continue

            fill = float(self.df[col].notna().mean())
            fill_rates[col] = round(fill, 4)
            users_with_value = int(self.df.loc[self.df[col].notna(), uid].nunique()) if uid in self.df.columns else 0
            field_health: Dict[str, Any] = {
                "global_fill_rate": round(fill, 4),
                "global_user_coverage_rate": round(float(users_with_value / total_users), 4) if total_users else 0,
                "event_aligned": event_mask is not None,
                "status": "observed_only",
            }

            if event_mask is not None:
                scoped = self.df.loc[event_mask]
                relevant_rows = int(len(scoped))
                relevant_missing = float(scoped[col].isna().mean()) if relevant_rows else None
                relevant_users = int(scoped[uid].nunique()) if uid in scoped.columns else 0
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
                    issues.append(f"{col} 在相关事件内缺失率 {relevant_missing:.1%}，用户级覆盖度 {relevant_user_coverage:.1%}，存在参数采集风险")
                elif relevant_missing is not None and relevant_missing > 0.2:
                    field_health["status"] = "warning"
                    warnings.append(f"{col} 在相关事件内缺失率 {relevant_missing:.1%}，但用户级覆盖度尚可，建议复核事件范围")
                elif relevant_user_coverage is not None and relevant_user_coverage < 0.8:
                    field_health["status"] = "warning"
                    warnings.append(f"{col} 在相关事件用户中的覆盖度 {relevant_user_coverage:.1%}，建议检查是否有用户未上报该参数")

            virtual_field_health[col] = field_health

        daily_logs = {}
        date_col = self.field_config.event_date
        if date_col in self.df.columns:
            daily_logs = {
                str(k.date() if hasattr(k, "date") else k): int(v)
                for k, v in self.df.groupby(self.df[date_col].dt.date).size().items()
            }

        user_device_ratio = None
        if uid in self.df.columns:
            users = self.df[uid].nunique()
            device_col = "device_id" if "device_id" in self.df.columns else None
            if device_col:
                devices = max(self.df[device_col].nunique(), 1)
                user_device_ratio = round(users / devices, 4)

        return {
            "fill_rates": fill_rates,
            "virtual_field_health": virtual_field_health,
            "relevant_events": relevant_events,
            "event_aligned": event_mask is not None,
            "matched_event_rows": int(event_mask.sum()) if event_mask is not None else 0,
            "issues": issues,
            "warnings": warnings,
            "daily_logs": daily_logs,
            "user_device_ratio": user_device_ratio,
            "date_continuity": self._date_continuity(),
        }

    def segmentation(
        self,
        reg_start: pd.Timestamp,
        reg_end: pd.Timestamp,
        retention_days: int,
    ) -> Dict[str, Any]:
        candidates = ["country", "channel"]
        candidates.extend([f"v_{key}" for key in (self.param_config.segment_keys if self.param_config else [])])
        if self.param_config and self.param_config.progress_key:
            candidates.append(f"v_{self.param_config.progress_key}")

        results = {}
        top_contributor = None
        for col in dict.fromkeys(candidates):
            if col not in self.df.columns:
                continue
            try:
                result = calculate_retention(
                    self.df,
                    self.field_config,
                    reg_start,
                    reg_end,
                    retention_days=retention_days,
                    segment_col=col,
                )
                records = result.head(20).to_dict(orient="records")
                results[col] = records
                candidate = self._segment_contributor(col, records)
                if candidate and (
                    top_contributor is None
                    or candidate["impact_score"] > top_contributor["impact_score"]
                ):
                    top_contributor = candidate
            except Exception:
                continue

        return {
            "segment_retention": results,
            "top_contributor": top_contributor,
        }

    def funnel(self) -> Dict[str, Any]:
        progress_col = self.require_virtual_field(self.param_config.progress_key if self.param_config else None)
        result_col = self.require_virtual_field(self.param_config.result_key if self.param_config else None)
        numeric_cols = [
            f"v_{key}"
            for key in (self.param_config.numeric_keys if self.param_config else [])
            if f"v_{key}" in self.df.columns
        ]

        if not progress_col or not result_col:
            return {
                "configured": False,
                "narrowest_step": None,
                "steps": [],
                "diagnosis": "未配置进度维度和结果状态，跳过漏斗诊断",
            }

        step_df = self.df[[self.field_config.user_id, progress_col, result_col] + numeric_cols].dropna(subset=[progress_col])
        grouped = step_df.groupby(progress_col)
        rows = []
        for level, part in grouped:
            total_users = part[self.field_config.user_id].nunique()
            passed_users = part[part[result_col].astype(str).str.lower().eq("pass")][self.field_config.user_id].nunique()
            pass_rate = passed_users / total_users * 100 if total_users else 0
            numeric_summary = {
                col: round(float(pd.to_numeric(part[col], errors="coerce").mean()), 2)
                for col in numeric_cols
            }
            rows.append({
                "step": str(level),
                "total_users": int(total_users),
                "passed_users": int(passed_users),
                "pass_rate": round(pass_rate, 2),
                "numeric_summary": numeric_summary,
            })

        rows = sorted(rows, key=lambda item: item["pass_rate"])
        narrowest = rows[0] if rows else None
        diagnosis = (
            f"最窄漏斗环节为 {narrowest['step']}，通过率 {narrowest['pass_rate']}%"
            if narrowest else "未发现可计算漏斗步骤"
        )
        return {
            "configured": True,
            "narrowest_step": narrowest,
            "steps": rows[:20],
            "diagnosis": diagnosis,
        }

    def context_attribution(self, reg_start: pd.Timestamp, reg_end: pd.Timestamp) -> Dict[str, Any]:
        progress_col = self.require_virtual_field(self.param_config.progress_key if self.param_config else None)
        result_col = self.require_virtual_field(self.param_config.result_key if self.param_config else None)

        if not progress_col or not result_col:
            return {
                "summary": "未配置关卡进度和结果状态，暂不做业务场景归因",
                "level_pass_rates": [],
                "events": [],
            }

        window = self.df[
            (self.df[self.field_config.reg_date] >= reg_start)
            & (self.df[self.field_config.reg_date] <= reg_end)
        ].copy()
        rates = []
        for level, part in window.dropna(subset=[progress_col]).groupby(progress_col):
            total = len(part)
            passed = int(part[result_col].astype(str).str.lower().eq("pass").sum())
            rate = passed / total * 100 if total else 0
            rates.append({"level": str(level), "pass_rate": round(rate, 2), "events": int(total)})

        rates = sorted(rates, key=lambda item: item["pass_rate"])
        worst = rates[0] if rates else None
        summary = (
            f"通关率最低的进度维度为 {worst['level']}，通关率 {worst['pass_rate']}%，可能拉低留存"
            if worst else "未检测到可归因的关卡参数波动"
        )
        return {
            "summary": summary,
            "level_pass_rates": rates[:20],
            "events": [],
        }

    def _date_continuity(self) -> Dict[str, Any]:
        date_col = self.field_config.event_date
        if date_col not in self.df.columns or self.df[date_col].dropna().empty:
            return {"continuous": False, "missing_dates": []}
        days = pd.Series(self.df[date_col].dropna().dt.date.unique()).sort_values()
        full = pd.date_range(days.iloc[0], days.iloc[-1], freq="D").date
        missing = sorted(set(full) - set(days))
        return {"continuous": len(missing) == 0, "missing_dates": [str(day) for day in missing[:20]]}

    def _segment_contributor(self, col: str, records: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not records:
            return None
        total_users = sum(int(item.get("n_total", 0)) for item in records) or 1
        weighted_rate = sum(float(item.get("retention_rate", 0)) * int(item.get("n_total", 0)) for item in records) / total_users
        impacts = []
        for item in records:
            size = int(item.get("n_total", 0))
            rate = float(item.get("retention_rate", 0))
            impact = max(weighted_rate - rate, 0) * size
            impacts.append((impact, item))
        impact_score, worst = max(impacts, key=lambda x: x[0])
        return {
            "field": col,
            "segment": worst.get("segment"),
            "retention_rate": worst.get("retention_rate"),
            "n_total": worst.get("n_total"),
            "impact_score": round(float(impact_score), 2),
            "summary": f"{col}={worst.get('segment')} 的留存率 {worst.get('retention_rate')}%，对整体下滑贡献最高",
        }

    def _suggest(self, stage1: Dict[str, Any], stage2: Dict[str, Any], stage3: Dict[str, Any], stage4: Dict[str, Any]) -> str:
        if stage1.get("issues"):
            return "优先按 relevant_events 对齐事件口径后复核 JSON 参数采集，再决定是否修复埋点。"
        if stage3.get("narrowest_step"):
            step = stage3["narrowest_step"]
            return f"重点复盘进度 {step['step']} 的难度、奖励与引导，降低该环节流失。"
        if stage2.get("top_contributor"):
            item = stage2["top_contributor"]
            return f"针对 {item['field']}={item['segment']} 做渠道质量或版本体验排查。"
        return "持续观察留存热力图、分群折线和参数变动，建立大事件标注机制。"
