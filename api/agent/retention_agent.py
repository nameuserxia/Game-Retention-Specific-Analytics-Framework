# -*- coding: utf-8 -*-
"""Agent orchestration for retention diagnosis."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from api.agent.context_builder import ContextBuilder
from api.agent.knowledge import GLOSSARY, SYSTEM_PROMPT, benchmark_comment, benchmark_for
from api.agent.model_gateway import LLMConfig, ModelGateway
from api.agent.tools import AgentToolbox
from api.models.schemas import AnalysisConfig, ParamMappingConfig
from core.analytics import FieldConfig

logger = logging.getLogger(__name__)

# 默认 LLM 配置路径（相对于项目根目录）
DEFAULT_LLM_CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "llm_config.yaml"


def _first_record(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    return records[0] if records else {}


@dataclass
class RetentionDiagnosisAgent:
    """A deterministic agent shell that follows the retention diagnosis SOP."""

    field_config: FieldConfig
    mapping: Dict[str, Any]
    analysis_config: AnalysisConfig
    param_config: Optional[ParamMappingConfig] = None
    game_genre: str = "casual"
    # 可选：外部传入 LLM 配置路径，默认使用 config/llm_config.yaml
    llm_config_path: Optional[str] = None

    def _load_llm_config(self) -> Optional[LLMConfig]:
        """尝试加载 LLM 配置，文件不存在或配置为空时返回 None。"""
        config_path = Path(self.llm_config_path) if self.llm_config_path else DEFAULT_LLM_CONFIG_PATH
        if not config_path.exists():
            logger.debug("[RetentionDiagnosisAgent] LLM 配置不存在，跳过: %s", config_path)
            return None
        try:
            cfg = LLMConfig.from_yaml(str(config_path))
            if not cfg.is_enabled:
                logger.debug("[RetentionDiagnosisAgent] LLM 未启用（provider 或 api_key 为空）")
                return None
            return cfg
        except Exception as exc:
            logger.warning("[RetentionDiagnosisAgent] LLM 配置加载失败: %s", exc)
            return None

    def run(
        self,
        df: pd.DataFrame,
        cohort_headers: List[str],
        cohort_matrix: List[List[Any]],
        virtual_fields: Optional[List[str]] = None,
        precomputed_ml: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        toolbox = AgentToolbox(
            field_config=self.field_config,
            mapping=self.mapping,
            analysis_config=self.analysis_config,
            param_config=self.param_config,
        )

        data_health = toolbox.inspect_data(df, virtual_fields=virtual_fields)

        segment_cols = self._segment_candidates(df, virtual_fields or [])
        retention = toolbox.calculate_retention(df, segment_cols=segment_cols)
        overall = _first_record(retention.get("overall", []))
        retention_rate = float(overall.get("retention_rate", 0))
        benchmark = benchmark_comment(
            self.analysis_config.retention_days,
            retention_rate,
            self.game_genre,
        )
        anomaly = self._locate_anomaly(retention.get("segments", {}), retention_rate)

        path_result = self._path_diagnosis(df)

        model_result = precomputed_ml or {
            "status": "disabled",
            "reason": "Correlation-based attribution is disabled in the current phase.",
        }
        core_factors: List[Dict[str, Any]] = []
        strategy = self._strategy(core_factors, path_result, data_health)

        visuals = toolbox.plot_visuals(
            df=df,
            cohort_headers=cohort_headers,
            cohort_matrix=cohort_matrix,
            funnel_steps=path_result.get("funnel_steps", []),
            legacy_model_info={},
        )

        # ── 组装结果 ──────────────────────────────────────────────
        result = {
            "brain": {
                "role_prompt": SYSTEM_PROMPT,
                "game_genre": self.game_genre,
                "benchmarks": benchmark_for(self.game_genre),
                "glossary": GLOSSARY,
            },
            "tool_trace": [
                "inspect_data",
                "calculate_retention",
                "plot_visuals",
                "skip_ml_attribution",
            ],
            "data_health": data_health,
            "anomaly_location": {
                "benchmark_comment": benchmark,
                "overall_retention": overall,
                "segment_candidates": segment_cols,
                "top_anomaly": anomaly,
            },
            "path_diagnosis": path_result,
            "model_diagnostics": model_result,
            "visual_specs": visuals,
            "structured_report": {
                "data_checkup": self._data_checkup_text(data_health),
                "anomaly_location": self._anomaly_text(anomaly, benchmark),
                "core_attribution": self._core_attribution_text(core_factors),
                "business_strategy": strategy,
            },
        }

        # ── LLM 分支 ─────────────────────────────────────────────
        llm_config = self._load_llm_config()
        if False:
            result = self._call_llm_and_merge(result, llm_config)
        else:
            result["llm_used"] = False
            result["llm_fallback_reason"] = "LLM 未启用或配置为空，使用规则引擎输出"

        return result

    def _call_llm_and_merge(self, result: Dict[str, Any], llm_config: LLMConfig) -> Dict[str, Any]:
        """
        调用 LLM 生成 Markdown 报告，替换 structured_report。
        失败时按 fallback_on_error 决定是否降级。
        """
        ctx = ContextBuilder(
            agent_result=result,
            game_genre=self.game_genre,
            benchmarks=result.get("brain", {}).get("benchmarks", {}),
        )
        user_prompt = ctx.build()
        system_prompt = ctx.system_prompt()

        gateway = ModelGateway(llm_config)
        try:
            llm_text = gateway.chat_sync(
                prompt=user_prompt,
                system_prompt=system_prompt,
            )
            result["structured_report"] = llm_text
            result["llm_used"] = True
            result["llm_model"] = llm_config.model
            result["llm_fallback_reason"] = None
            logger.info("[RetentionDiagnosisAgent] LLM 调用成功，使用模型: %s", llm_config.model)
        except Exception as exc:
            logger.warning("[RetentionDiagnosisAgent] LLM 调用失败: %s，回退到规则引擎", exc)
            result["llm_used"] = False
            result["llm_error"] = str(exc)
            result["llm_fallback_reason"] = f"LLM 调用失败（{exc}），已自动降级"
            # structured_report 保持为规则引擎输出，不做修改

        return result

    def _segment_candidates(self, df: pd.DataFrame, virtual_fields: List[str]) -> List[str]:
        candidates = []
        for col in ["channel", "country", "app_store_version", "os_version"]:
            if col in df.columns:
                candidates.append(col)
        if self.param_config and self.param_config.progress_key:
            progress_col = f"v_{self.param_config.progress_key}"
            if progress_col in df.columns:
                candidates.append(progress_col)
        candidates.extend([col for col in virtual_fields if col in df.columns and col not in candidates])
        return candidates[:8]

    def _locate_anomaly(self, segments: Dict[str, List[Dict[str, Any]]], overall_rate: float) -> Optional[Dict[str, Any]]:
        best = None
        for field, rows in segments.items():
            if not isinstance(rows, list):
                continue
            for row in rows:
                if "error" in row:
                    continue
                n_total = int(row.get("n_total", 0))
                rate = float(row.get("retention_rate", 0))
                if n_total < 30:
                    continue
                gap = overall_rate - rate
                impact = gap * n_total
                if gap > 0 and (best is None or impact > best["impact_score"]):
                    best = {
                        "field": field,
                        "segment": row.get("segment"),
                        "n_total": n_total,
                        "retention_rate": rate,
                        "gap": round(gap, 2),
                        "impact_score": round(impact, 2),
                    }
        return best

    def _path_diagnosis(self, df: pd.DataFrame) -> Dict[str, Any]:
        if not self.param_config:
            return {"summary": "未配置 JSON 参数，跳过路径节点诊断。", "funnel_steps": []}

        progress_col = f"v_{self.param_config.progress_key}" if self.param_config.progress_key else ""
        result_col = f"v_{self.param_config.result_key}" if self.param_config.result_key else ""
        if progress_col not in df.columns or result_col not in df.columns:
            return {"summary": "缺少进度维度或结果状态虚拟列，跳过路径节点诊断。", "funnel_steps": []}

        steps = []
        for key, part in df.dropna(subset=[progress_col]).groupby(progress_col):
            users = part[self.field_config.user_id].nunique()
            passed = part[part[result_col].astype(str).str.lower().eq("pass")][self.field_config.user_id].nunique()
            pass_rate = passed / users * 100 if users else 0
            steps.append({
                "step": str(key),
                "users": int(users),
                "passed": int(passed),
                "pass_rate": round(pass_rate, 2),
            })

        steps = sorted(steps, key=lambda item: item["pass_rate"])
        narrowest = steps[0] if steps else None
        if narrowest:
            summary = f"关键路径最窄节点是 {narrowest['step']}，通过率 {narrowest['pass_rate']}%。"
        else:
            summary = "未发现可计算的路径节点。"
        return {"summary": summary, "narrowest_node": narrowest, "funnel_steps": steps[:20]}

    def _data_checkup_text(self, health: Dict[str, Any]) -> str:
        risks = health.get("risks", [])
        warnings = health.get("warnings", [])
        virtual_health = health.get("virtual_field_health", {})
        mode = virtual_health.get("mode")
        matched_rows = virtual_health.get("matched_event_rows", 0)
        notes = health.get("notes", []) or virtual_health.get("notes", [])
        if mode == "event_aligned":
            scope_text = f"虚拟字段已按 relevant_events 对齐校验，命中相关事件 {matched_rows} 行。"
        elif notes:
            scope_text = notes[0]
        else:
            scope_text = "虚拟字段未做参数缺失判定。"
        if not risks:
            warning_text = f" 关注项：{'；'.join(warnings[:3])}。" if warnings else ""
            return f"数据质量评分 {health.get('quality_score', 0)}/100，未发现阻断性风险。{scope_text}{warning_text}"
        return f"数据质量评分 {health.get('quality_score', 0)}/100，主要风险：{'；'.join(risks[:3])}。{scope_text}"

    def _anomaly_text(self, anomaly: Optional[Dict[str, Any]], benchmark: str) -> str:
        if not anomaly:
            return f"{benchmark} 暂未定位到样本量充足且明显低于整体的单一分群。"
        return (
            f"{benchmark} 异动源头优先看 {anomaly['field']}={anomaly['segment']}，"
            f"该组留存 {anomaly['retention_rate']}%，低于整体 {anomaly['gap']} 个百分点。"
        )

    def _core_attribution_text(self, factors: List[Dict[str, Any]]) -> str:
        return "当前版本不输出相关性归因结论；请结合分群、路径和业务事件做假设验证。"

    def _strategy(
        self,
        factors: List[Dict[str, Any]],
        path_result: Dict[str, Any],
        health: Dict[str, Any],
    ) -> str:
        if health.get("risks"):
            return "先处理已确认的数据口径风险；JSON 参数缺失只在 relevant_events 对齐后才作为埋点问题处理。"

        top = factors[0]["feature"].lower() if factors else ""
        node = path_result.get("narrowest_node")
        if "step_gap" in top:
            suffix = f"，尤其关注 {node['step']} 节点" if node else ""
            return f"建议检查关卡难度和最优步数设计{suffix}，必要时下调难度或增加补给。"
        if "pass" in top or "fail" in top:
            suffix = f"，优先排查 {node['step']} 的失败反馈" if node else ""
            return f"建议优化失败后的复玩引导和奖励补偿{suffix}。"
        if "active_days" in top or "login" in top:
            return "建议强化次日召回机制，例如首日目标、离线奖励、Push 和登录奖励。"
        if "ad" in top:
            return "建议复盘广告展示节奏，控制早期强插广告并提升激励广告收益感。"
        if node:
            return f"建议针对 {node['step']} 节点做难度、引导和奖励 AB Test。"
        return "建议围绕 Top 特征做分群 AB Test，并监控 D1/D7 留存和 LTV 的联动变化。"
