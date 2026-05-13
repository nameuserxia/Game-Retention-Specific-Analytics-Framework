# -*- coding: utf-8 -*-
"""Generate structured retention reports with optional LLM fallback."""

from __future__ import annotations

import json
from typing import Any, Dict, List

from api.ai.model_gateway import LLMUnavailableError, ModelGateway
from api.ai.prompts import SYSTEM_PROMPT
from api.models.llm_report import LLMRetentionReport, ReportFinding, ReportRecommendation


def _evidence_from_summary(summary: Dict[str, Any]) -> List[str]:
    evidence = []
    if summary:
        evidence.append(
            f"D+{summary.get('retention_days')} 留存率 {summary.get('retention_rate')}%，"
            f"样本 {summary.get('n_total')} 人。"
        )
    return evidence


def build_fallback_report(payload: Dict[str, Any], reason: str = "") -> LLMRetentionReport:
    summary = payload.get("summary", {})
    quality = payload.get("data_quality", {})
    warnings = quality.get("warnings", []) if isinstance(quality, dict) else []
    errors = quality.get("errors", []) if isinstance(quality, dict) else []
    context = payload.get("analysis_context", {})
    dynamic_retention = payload.get("dynamic_retention") or []
    funnel_analysis = payload.get("funnel_analysis") or {}
    game_name = context.get("game_name") or "游戏"

    quality_text = "数据质量校验通过，未发现阻断性问题。"
    if errors:
        quality_text = "数据质量存在阻断性问题：" + "；".join(map(str, errors[:3]))
    elif warnings:
        quality_text = "数据质量存在关注项：" + "；".join(map(str, warnings[:3]))

    rate = summary.get("retention_rate", 0) or 0
    retention_detail = (
        f"当前 D+{summary.get('retention_days')} 留存率为 {rate}%，"
        f"注册窗口 {summary.get('reg_start')} 至 {summary.get('reg_end')}。"
    )

    segment_findings = [
        ReportFinding(
            title="分群结果待复核",
            detail="已基于 Python 计算结果输出国家和渠道分群，可优先关注样本量充足且低于整体留存的分群。",
            evidence=[],
        )
    ]
    if dynamic_retention:
        segment_findings.append(
            ReportFinding(
                title="动态维度分析已完成",
                detail="已按用户配置的字段组合计算多天留存，建议优先查看 gap_vs_overall 为负且样本量充足的分组。",
                evidence=[f"动态维度组合数：{len(dynamic_retention)}"],
            )
        )

    funnel_findings = [
        ReportFinding(
            title="漏斗暂未启用",
            detail="当前未配置漏斗步骤，建议先用路径分析定位流失前常见行为。",
            evidence=[],
        )
    ]
    if funnel_analysis.get("steps"):
        narrowest = min(
            funnel_analysis["steps"][1:] or funnel_analysis["steps"],
            key=lambda item: item.get("step_conversion_rate", 1.0),
        )
        funnel_findings = [
            ReportFinding(
                title="漏斗分析已完成",
                detail=f"转化相对较弱的步骤是 {narrowest.get('event')}，单步转化率 {narrowest.get('step_conversion_rate')}。",
                evidence=[f"漏斗步骤数：{len(funnel_analysis.get('steps', []))}"],
            )
        ]

    return LLMRetentionReport(
        title=f"{game_name}留存诊断报告",
        quality_assessment=quality_text,
        retention_diagnosis=[
            ReportFinding(
                title="整体留存概览",
                detail=retention_detail,
                evidence=_evidence_from_summary(summary),
            )
        ],
        segment_findings=segment_findings,
        funnel_analysis=funnel_findings,
        recommendations=[
            ReportRecommendation(
                priority="high",
                action="优先检查低留存分群的投放、版本、地区和新手体验差异。",
                expected_impact="减少主要低质流量或体验断点对整体 D1/D7 留存的拖累。",
                validation="按相同口径追踪分群 D1/D3/D7 留存，并与调整前 cohort 对比。",
            ),
            ReportRecommendation(
                priority="medium",
                action="结合近期运营事件复盘留存波动日期附近的版本、活动、BUG 和买量变化。",
                expected_impact="区分数据波动来自产品体验、运营节奏还是流量结构变化。",
                validation="将关键事件日期与 cohort 留存矩阵进行对齐复核。",
            ),
        ],
        next_checks=[
            "继续补充 D3/D7/D14 留存观察。",
            "在 Phase 2 接入动态维度和漏斗后复核关键假设。",
        ],
        fallback_used=True,
        fallback_reason=reason or "AI 未启用，使用规则报告。",
    )


class RetentionReporter:
    def __init__(self, gateway: ModelGateway | None = None):
        self.gateway = gateway or ModelGateway()

    def generate(self, payload: Dict[str, Any], ai_enabled: bool = False) -> tuple[LLMRetentionReport, bool, str]:
        if not ai_enabled:
            return build_fallback_report(payload, "用户未开启 AI 分析模式。"), False, "用户未开启 AI 分析模式。"

        try:
            messages = [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False, default=str)},
            ]
            raw = self.gateway.chat(messages)
            parsed = json.loads(raw)
            report = LLMRetentionReport.model_validate(parsed)
            report.fallback_used = False
            report.fallback_reason = ""
            return report, True, ""
        except (LLMUnavailableError, json.JSONDecodeError, ValueError) as exc:
            reason = f"AI 报告生成失败，已降级：{exc}"
            return build_fallback_report(payload, reason), False, reason
