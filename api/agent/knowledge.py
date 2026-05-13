# -*- coding: utf-8 -*-
"""Brain: role definition, benchmarks, and metric glossary."""

from __future__ import annotations

from typing import Dict


SYSTEM_PROMPT = """
你是一名资深游戏数据科学家，擅长通过 SQL 和 Python 分析 DAU、留存率及 LTV。
你遵循“异动诊断四步法”：校验、下钻、路径、归因。
你的目标是识别导致流失的特征，并给出业务对策。
你必须先确认数据质量，再解释留存变化；不能在埋点异常时直接给业务结论。
""".strip()


GAME_BENCHMARKS: Dict[str, Dict[str, Dict[str, float]]] = {
    "casual": {
        "d1": {"low": 25.0, "median": 32.0, "good": 40.0, "excellent": 45.0},
        "d3": {"low": 12.0, "median": 18.0, "good": 25.0, "excellent": 30.0},
        "d7": {"low": 8.0, "median": 12.0, "good": 18.0, "excellent": 25.0},
        "d14": {"low": 5.0, "median": 8.0, "good": 14.0, "excellent": 18.0},
        "d30": {"low": 3.0, "median": 5.0, "good": 10.0, "excellent": 15.0},
        "ltv_hint": {"note": "休闲游戏通常更依赖广告 LTV 和短周期回访。"},
    },
    "competitive": {
        "d1": {"low": 35.0, "median": 42.0, "good": 50.0, "excellent": 55.0},
        "d3": {"low": 20.0, "median": 28.0, "good": 38.0, "excellent": 45.0},
        "d7": {"low": 15.0, "median": 22.0, "good": 32.0, "excellent": 40.0},
        "d14": {"low": 10.0, "median": 16.0, "good": 25.0, "excellent": 32.0},
        "d30": {"low": 6.0, "median": 10.0, "good": 18.0, "excellent": 25.0},
        "ltv_hint": {"note": "竞技游戏需要关注匹配公平性、胜率和排位挫败。"},
    },
    "mmo": {
        "d1": {"low": 30.0, "median": 38.0, "good": 45.0, "excellent": 52.0},
        "d3": {"low": 18.0, "median": 25.0, "good": 35.0, "excellent": 42.0},
        "d7": {"low": 12.0, "median": 18.0, "good": 28.0, "excellent": 35.0},
        "d14": {"low": 8.0, "median": 14.0, "good": 22.0, "excellent": 28.0},
        "d30": {"low": 6.0, "median": 12.0, "good": 20.0, "excellent": 28.0},
        "ltv_hint": {"note": "MMO 更依赖社交关系、成长线和中长期付费深度。"},
    },
}


GLOSSARY = {
    "D1": "D1 留存 = 注册日后第 1 天仍有活跃事件的用户数 / 注册日用户数。",
    "D7": "D7 留存 = 注册日后第 7 天仍有活跃事件的用户数 / 注册日用户数。",
    "D30": "D30 留存 = 注册日后第 30 天仍有活跃事件的用户数 / 注册日用户数。",
    "Rolling Retention": "滑块留存 = 注册日后第 N 天及以后任意一天回访的用户数 / 注册日用户数。",
    "DAU": "DAU = 某自然日有活跃事件的去重用户数。",
    "LTV": "LTV = 用户生命周期内累计收入，常按注册 cohort 观察 D1/D7/D30 LTV。",
}


def benchmark_for(game_genre: str | None) -> Dict[str, Dict[str, float]]:
    genre = (game_genre or "casual").lower()
    return GAME_BENCHMARKS.get(genre, GAME_BENCHMARKS["casual"])


def benchmark_comment(retention_days: int, retention_rate: float, game_genre: str | None) -> str:
    bench = benchmark_for(game_genre)
    key = f"d{retention_days}"
    if key not in bench:
        return "当前留存天数暂无内置 benchmark，请结合历史 cohort 做相对比较。"

    low = bench[key]["low"]
    good = bench[key]["good"]
    excellent = bench[key].get("excellent", good)
    if retention_rate >= excellent:
        level = "高于优秀线"
    elif retention_rate >= good:
        level = "高于良好线"
    elif retention_rate >= low:
        level = "处于可接受区间"
    else:
        level = "低于行业警戒线"
    return f"D{retention_days} 留存率 {retention_rate:.2f}%，{level}（参考区间：警戒 {low:.0f}% / 良好 {good:.0f}% / 优秀 {excellent:.0f}%）。"
