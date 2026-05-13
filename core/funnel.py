# -*- coding: utf-8 -*-
"""Configurable event funnel analysis."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from core.utils import FieldConfig


MIN_FUNNEL_STEPS = 2
MAX_FUNNEL_STEPS = 10
MIN_FUNNEL_USERS = 30


def _normalize_steps(steps: Optional[List[str]]) -> List[str]:
    normalized = []
    for step in steps or []:
        value = str(step).strip()
        if value:
            normalized.append(value)
    return normalized


def calculate_funnel(
    df: pd.DataFrame,
    cfg: FieldConfig,
    steps: Optional[List[str]],
    min_steps: int = MIN_FUNNEL_STEPS,
    max_steps: int = MAX_FUNNEL_STEPS,
    min_users: int = MIN_FUNNEL_USERS,
) -> Dict[str, Any]:
    """Calculate ordered user conversion through arbitrary event steps."""
    warnings: List[str] = []
    normalized_steps = _normalize_steps(steps)
    if not normalized_steps:
        return {"steps": [], "warnings": []}
    if len(normalized_steps) < min_steps:
        return {
            "steps": [],
            "warnings": [f"漏斗至少需要 {min_steps} 个步骤。"],
        }
    if len(normalized_steps) > max_steps:
        warnings.append(f"漏斗步骤数 {len(normalized_steps)} 超过上限 {max_steps}，已截断。")
        normalized_steps = normalized_steps[:max_steps]

    required_cols = [cfg.user_id, cfg.event_name]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        return {"steps": [], "warnings": [f"缺少必要字段 {missing}。"]}

    time_col = cfg.event_time if cfg.event_time in df.columns else cfg.event_date
    if time_col not in df.columns:
        return {"steps": [], "warnings": ["缺少可用于排序的事件时间字段。"]}

    work = df[[cfg.user_id, cfg.event_name, time_col]].dropna(subset=[cfg.user_id, cfg.event_name, time_col]).copy()
    if work.empty:
        return {"steps": [], "warnings": ["没有可用于漏斗分析的事件记录。"]}

    step_set = set(normalized_steps)
    work = work[work[cfg.event_name].astype(str).isin(step_set)]
    if work.empty:
        return {"steps": [], "warnings": ["漏斗步骤没有命中任何事件。"]}

    work = work.sort_values([cfg.user_id, time_col], ascending=[True, True])
    passed_users = [set() for _ in normalized_steps]

    for uid, user_events in work.groupby(cfg.user_id, sort=False):
        next_step_idx = 0
        last_time = None
        for row in user_events.itertuples(index=False):
            event_name = str(getattr(row, cfg.event_name))
            event_time = getattr(row, time_col)
            if next_step_idx >= len(normalized_steps):
                break
            if event_name != normalized_steps[next_step_idx]:
                continue
            if last_time is not None and event_time <= last_time:
                continue
            passed_users[next_step_idx].add(uid)
            last_time = event_time
            next_step_idx += 1

    first_users = len(passed_users[0]) if passed_users else 0
    if first_users < min_users:
        warnings.append(f"漏斗首步用户数 {first_users} 小于 {min_users}，请谨慎解读。")

    output_steps: List[Dict[str, Any]] = []
    previous_users = 0
    for idx, event in enumerate(normalized_steps):
        users = len(passed_users[idx])
        if idx == 0:
            step_conversion = 1.0 if users else 0.0
            dropoff_users = 0
            dropoff_rate = 0.0
        else:
            step_conversion = users / previous_users if previous_users else 0.0
            dropoff_users = max(previous_users - users, 0)
            dropoff_rate = dropoff_users / previous_users if previous_users else 0.0
        overall_conversion = users / first_users if first_users else 0.0
        output_steps.append({
            "event": event,
            "users": int(users),
            "step_conversion_rate": round(step_conversion, 4),
            "overall_conversion_rate": round(overall_conversion, 4),
            "dropoff_users": int(dropoff_users),
            "dropoff_rate": round(dropoff_rate, 4),
        })
        previous_users = users

    return {"steps": output_steps, "warnings": warnings}
