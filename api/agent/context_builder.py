# -*- coding: utf-8 -*-
"""
ContextBuilder — 将 Python 计算结果组装为 LLM 的结构化 USER_PROMPT。
"""

from __future__ import annotations

import json
import textwrap
from typing import Any, Dict, List, Optional


def _fmt(v: Any) -> str:
    """把任意值转成可读字符串。"""
    if isinstance(v, float):
        return f"{v:.2f}"
    if isinstance(v, dict):
        return json.dumps(v, ensure_ascii=False, indent=2)
    if isinstance(v, list):
        return json.dumps(v, ensure_ascii=False, indent=2)
    return str(v)


def _bullet_table(headers: List[str], rows: List[List[Any]]) -> str:
    if not rows:
        return "（无数据）"
    col_w = [max(len(headers[i]), max((len(str(r[i])) for r in rows), default=0)) for i in range(len(headers))]
    header_line = " | ".join(h.ljust(col_w[i]) for i, h in enumerate(headers))
    sep = "-+-".join("-" * w for w in col_w)
    lines = [header_line, sep]
    for row in rows:
        lines.append(" | ".join(str(row[i]).ljust(col_w[i]) for i in range(len(headers))))
    return "\n".join(lines)


class ContextBuilder:
    """
    把 RetentionDiagnosisAgent.run() 的输出重组为一段面向 LLM 的分析上下文。

    用法:
        ctx = ContextBuilder(agent_result, field_config, analysis_config)
        user_prompt = ctx.build()
        system_prompt = ctx.system_prompt()
    """

    # ── System prompt ──────────────────────────────────────────
    SYSTEM_PROMPT = textwrap.dedent("""\
        你是一位资深游戏留存分析与运营策略专家，擅长从数据中发现问题、归因原因，并给出具体可执行的建议。

        分析风格：
        - 严谨：基于数据说话，不凭空臆测
        - 结构化：结论先行，数据支撑，策略跟进
        - 实战导向：每条建议可直接落地，避免泛泛而谈
        - 中文输出：正文为中文，关键术语保留英文并附中文解释

        输出格式：直接输出 Markdown，无需额外说明。
        请在分析中适度使用 Markdown 表格、加粗、列表等格式增强可读性。

        重要限制：
        - 禁止使用任何 emoji 符号（如 [OK]、[WARN]、[FAIL] 或任何 Unicode emoji）
        - 仅使用中文、英文、阿拉伯数字和标准标点符号

        你的分析框架：
        1. **数据质量判断** — 口径是否一致、样本量是否充足、是否有明显异常值
        2. **留存现状定位** — 当前各留存节点的实际水平，对比品类基准
        3. **异常分群挖掘** — 哪个维度/分群是主要拖累，样本量是否可信
        4. **关键归因识别** — 哪些行为特征或路径节点最能预测流失
        5. **策略建议** — 按优先级给出 3~5 条具体可执行的行动建议
        6. **风险提示** — 当前分析的数据局限和需要进一步验证的假设
    """).strip()

    def __init__(
        self,
        agent_result: Dict[str, Any],
        game_genre: str = "casual",
        benchmarks: Optional[Dict[str, Any]] = None,
    ):
        self.result = agent_result
        self.game_genre = game_genre
        self.benchmarks = benchmarks or {}

    # ── 公开接口 ───────────────────────────────────────────────
    def system_prompt(self) -> str:
        return self.SYSTEM_PROMPT

    def build(self) -> str:
        parts = [
            self._header(),
            self._data_health(),
            self._retention_benchmark(),
            self._anomaly(),
            self._path(),
            self._strategy(),
            self._brain(),
            self._footer(),
        ]
        return "\n\n".join(filter(None, parts))

    # ── 私有：各区块 ───────────────────────────────────────────
    def _header(self) -> str:
        return textwrap.dedent(f"""\
            ## 游戏留存诊断请求

            游戏类型: **{self.game_genre}**
            请基于以下数据，完成一份完整的留存诊断报告。
        """).strip()

    def _data_health(self) -> str:
        health = self.result.get("data_health", {})
        risks = health.get("risks", []) or []
        warnings = health.get("warnings", []) or []

        section = ["### 1. 数据质量概览"]
        section.append(f"数据质量评分：{health.get('quality_score', 'N/A')}/100")
        if risks:
            section.append(f"**风险项（阻断性）**：{'；'.join(risks[:5])}")
        if warnings:
            section.append(f"**关注项**：{'；'.join(warnings[:5])}")
        else:
            section.append("未发现明显风险。")
        return "\n".join(section)

    def _retention_benchmark(self) -> str:
        anomaly = self.result.get("anomaly_location", {})
        overall = anomaly.get("overall_retention", {})
        benchmark_cmt = anomaly.get("benchmark_comment", "")
        days = list(self.benchmarks.keys()) if self.benchmarks else ["D1", "D3", "D7"]

        rows = []
        for d in days:
            b = self.benchmarks.get(d, {})
            rows.append([d, _fmt(b.get("median", "N/A")), _fmt(b.get("good", "N/A")), _fmt(b.get("excellent", "N/A"))])

        section = [
            "### 2. 留存现状 vs 品类基准",
            f"> {benchmark_cmt}",
            "",
            "| 节点 | 品类中位 | 良好 | 优秀 |",
            "|------|---------|------|------|",
        ]
        for row in rows:
            section.append(f"| {' | '.join(str(x) for x in row)} |")
        section.append("")
        section.append(f"**实际整体留存**：{overall.get('retention_rate', 'N/A')}%（样本 {overall.get('n_total', 'N/A')} 人）")
        return "\n".join(section)

    def _anomaly(self) -> str:
        anomaly = self.result.get("anomaly_location", {})
        top = anomaly.get("top_anomaly")
        seg_candidates = anomaly.get("segment_candidates", [])

        section = ["### 3. 异常分群定位"]
        if top:
            section.append(
                f"**最大拖累分群**：{top.get('field', '')} = {top.get('segment', '')}，"
                f"留存 {top.get('retention_rate', '')}%，"
                f"低于整体 {top.get('gap', '')} 个百分点（样本 {top.get('n_total', '')} 人，影响指数 {top.get('impact_score', '')}）。"
            )
        else:
            section.append("未定位到样本量充足且显著低于整体的分群。")
        if seg_candidates:
            section.append(f"已扫描的分群维度：{', '.join(seg_candidates)}")
        return "\n".join(section)

    def _path(self) -> str:
        path = self.result.get("path_diagnosis", {})
        summary = path.get("summary", "")
        steps = path.get("funnel_steps", [])[:8]

        section = ["### 4. 关键路径诊断"]
        section.append(f"> {summary}" if summary else "未配置路径节点参数。")
        if steps:
            rows = [[s["step"], s["users"], s["passed"], f"{s['pass_rate']}%"] for s in steps]
            section.append("")
            section.append(_bullet_table(["节点", "总用户", "通过用户", "通过率"], rows))
        return "\n".join(section)

    def _ml_attribution(self) -> str:
        top_features = []
        translation = ""

        section = ["### 5. 关键假设（规则诊断）"]
        if top_features:
            rows = [[f.get("feature", ""), _fmt(f.get("importance", ""))] for f in top_features]
            section.append(_bullet_table(["特征", "重要性得分"], rows))
        if translation:
            section.append(f"\n**业务解读**：{translation}")
        else:
            section.append("\n模型未产出稳定重要特征。")
        return "\n".join(section)

    def _strategy(self) -> str:
        report = self.result.get("structured_report", {})
        return "\n".join([
            "### 6. 当前规则引擎建议（参考）",
            "",
            "以下为确定性规则引擎基于已知逻辑给出的建议，LLM 可参考但应独立判断：",
            "",
            f"- **数据质量**：{report.get('data_checkup', 'N/A')}",
            f"- **异常定位**：{report.get('anomaly_location', 'N/A')}",
            f"- **核心归因**：{report.get('core_attribution', 'N/A')}",
            f"- **策略建议**：{report.get('business_strategy', 'N/A')}",
        ])

    def _brain(self) -> str:
        brain = self.result.get("brain", {})
        glossary = brain.get("glossary", {})

        section = ["### 附录：术语参考"]
        if glossary:
            items = [f"- **{k}**：{v}" for k, v in list(glossary.items())[:10]]
            section.extend(items)
        else:
            section.append("（无额外术语表）")
        return "\n".join(section)

    def _footer(self) -> str:
        return textwrap.dedent("""\
            ---

            > 分析要求：
            > - 先判断数据质量是否支撑可靠结论，若样本量 < 100 或口径存疑，请明确说明局限性
            > - 归因部分需结合分群和路径节点，不做孤立判断
            > - 策略建议请标注优先级（高/中/低）和预计影响
        """).strip()
