# -*- coding: utf-8 -*-
"""Render structured retention reports to Markdown."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List

from api.models.llm_report import LLMRetentionReport, ReportFinding, ReportRecommendation


def _lines_for_findings(items: Iterable[ReportFinding]) -> List[str]:
    lines: List[str] = []
    for item in items:
        title = item.title or "发现"
        detail = item.detail or "暂无详细说明。"
        lines.append(f"- **{title}**：{detail}")
        for evidence in item.evidence[:3]:
            lines.append(f"  - 依据：{evidence}")
    return lines or ["- 暂无明确发现。"]


def _lines_for_recommendations(items: Iterable[ReportRecommendation]) -> List[str]:
    lines: List[str] = []
    for item in items:
        action = item.action or "继续观察关键留存指标。"
        lines.append(f"- **{item.priority or 'medium'}**：{action}")
        if item.expected_impact:
            lines.append(f"  - 预期影响：{item.expected_impact}")
        if item.validation:
            lines.append(f"  - 验证方式：{item.validation}")
    return lines or ["- 暂无明确建议。"]


def render_markdown_report(report: LLMRetentionReport, payload: Dict[str, Any]) -> str:
    summary = payload.get("summary", {})
    context = payload.get("analysis_context", {})
    quality = payload.get("data_quality", {})
    warnings = quality.get("warnings", []) if isinstance(quality, dict) else []
    errors = quality.get("errors", []) if isinstance(quality, dict) else []
    dynamic_retention = payload.get("dynamic_retention") or []
    funnel_analysis = payload.get("funnel_analysis")
    field_catalog = payload.get("analysis_field_catalog") or {}
    field_catalog_warnings = field_catalog.get("warnings", []) if isinstance(field_catalog, dict) else []
    catalog_fields = field_catalog.get("fields", []) if isinstance(field_catalog, dict) else []

    title = report.title or "留存分析报告"
    generated_at = payload.get("created_at") or datetime.now().isoformat(timespec="seconds")

    context_lines = [
        f"| 游戏名称 | {context.get('game_name', '未填写')} |",
        f"| 游戏类型 | {context.get('game_genre', '未填写')} |",
        f"| 游戏玩法 | {context.get('gameplay', '未填写')} |",
        f"| 近期事件 | {', '.join(context.get('recent_events', [])) or '未填写'} |",
        f"| 当前担心 | {context.get('main_concern', '未填写')} |",
    ]

    next_checks = [f"- {item}" for item in report.next_checks] or ["- 继续按相同口径观察 D1/D3/D7 留存变化。"]
    catalog_lines: List[str] = []
    if catalog_fields:
        recommended = [field for field in catalog_fields if field.get("recommended_for_segmentation")]
        risky = [field for field in catalog_fields if field.get("health_flags")]
        catalog_lines.append(f"- Catalog status: {field_catalog.get('status', 'unknown')}")
        catalog_lines.append(f"- Analysis fields: {len(catalog_fields)}")
        catalog_lines.append(f"- Recommended segmentation fields: {len(recommended)}")
        if risky:
            catalog_lines.append("- Field health flags: " + "; ".join(
                f"{item.get('field_id')}={','.join(item.get('health_flags', []))}"
                for item in risky[:8]
            ))
        for warning in field_catalog_warnings[:5]:
            catalog_lines.append(f"- Warning: {warning}")
    else:
        catalog_lines.append("- AnalysisFieldCatalog unavailable; compatible fallback flow was used.")

    dynamic_lines: List[str] = []
    if dynamic_retention:
        for item in dynamic_retention[:5]:
            dims = " + ".join(item.get("dimensions", [])) or "未命名维度"
            groups = item.get("groups", [])[:5]
            dynamic_lines.append(f"### {dims}")
            if item.get("warnings"):
                dynamic_lines.extend([f"- 提示：{warning}" for warning in item.get("warnings", [])[:3]])
            if groups:
                dynamic_lines.extend(["", "| 分组 | 样本量 | D1 | D3 | D7 | D14 | 样本提示 |", "|------|------:|------:|------:|------:|------:|------|"])
                for group in groups:
                    retention = group.get("retention", {})
                    dynamic_lines.append(
                        f"| {group.get('group_key', '')} | {group.get('cohort_size', 0)} | "
                        f"{retention.get('D1', '-')} | {retention.get('D3', '-')} | "
                        f"{retention.get('D7', '-')} | {retention.get('D14', '-')} | "
                        f"{'样本过小' if group.get('sample_warning') else ''} |"
                    )
            else:
                dynamic_lines.append("- 未产出可展示分组。")
    else:
        dynamic_lines.append("本次未配置动态维度分析。")

    funnel_lines: List[str] = []
    if funnel_analysis and funnel_analysis.get("steps"):
        if funnel_analysis.get("warnings"):
            funnel_lines.extend([f"- 提示：{warning}" for warning in funnel_analysis.get("warnings", [])[:5]])
            funnel_lines.append("")
        funnel_lines.extend(["| 步骤 | 用户数 | 单步转化 | 总体转化 | 流失用户 | 流失率 |", "|------|------:|------:|------:|------:|------:|"])
        for step in funnel_analysis.get("steps", []):
            funnel_lines.append(
                f"| {step.get('event', '')} | {step.get('users', 0)} | "
                f"{step.get('step_conversion_rate', 0)} | {step.get('overall_conversion_rate', 0)} | "
                f"{step.get('dropoff_users', 0)} | {step.get('dropoff_rate', 0)} |"
            )
    else:
        funnel_lines.append("本次未配置漏斗分析。")

    content = [
        f"# {title}",
        "",
        f"> 生成时间：{generated_at}",
        f"> Session ID：{payload.get('session_id', '')}",
        "",
        "## 项目上下文",
        "",
        "| 字段 | 内容 |",
        "|------|------|",
        *context_lines,
        "",
        "## 核心指标",
        "",
        "| 指标 | 数值 |",
        "|------|------|",
        f"| 注册窗口 | {summary.get('reg_start', '')} ~ {summary.get('reg_end', '')} |",
        f"| 留存定义 | D+{summary.get('retention_days', '')} |",
        f"| 总用户数 | {summary.get('n_total', 0):,} |" if isinstance(summary.get("n_total"), int) else f"| 总用户数 | {summary.get('n_total', 0)} |",
        f"| 留存用户 | {summary.get('n_retained', 0)} |",
        f"| 流失用户 | {summary.get('n_churn', 0)} |",
        f"| 留存率 | {summary.get('retention_rate', 0)}% |",
        "",
        "## 数据质量",
        "",
        report.quality_assessment or "暂无明显数据质量结论。",
    ]

    if warnings or errors:
        content.extend(["", "### 质量提示", ""])
        content.extend([f"- {item}" for item in [*errors[:5], *warnings[:5]]])

    content.extend([
        "",
        "## AnalysisFieldCatalog",
        "",
        *catalog_lines,
        "",
        "## 留存诊断",
        "",
        *_lines_for_findings(report.retention_diagnosis),
        "",
        "## 分群发现",
        "",
        *_lines_for_findings(report.segment_findings),
        "",
        "## 动态分群留存分析",
        "",
        *dynamic_lines,
        "",
        "## 漏斗转化分析",
        "",
        *funnel_lines,
        "",
        "## 漏斗分析",
        "",
        *_lines_for_findings(report.funnel_analysis),
        "",
        "## 优化建议",
        "",
        *_lines_for_recommendations(report.recommendations),
        "",
        "## 后续验证建议",
        "",
        *next_checks,
    ])

    if report.fallback_used:
        content.extend([
            "",
            "## 降级说明",
            "",
            report.fallback_reason or "AI 不可用，已使用规则报告降级。",
        ])

    return "\n".join(content).strip() + "\n"
