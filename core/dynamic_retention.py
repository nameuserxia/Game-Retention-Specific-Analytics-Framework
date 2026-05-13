# -*- coding: utf-8 -*-
"""Dynamic multi-dimensional retention analysis."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from core.churn import get_churn_users
from core.utils import FieldConfig


MAX_DIMENSIONS = 3
MAX_GROUPS = 100
MIN_COHORT_SIZE = 30
DEFAULT_RETENTION_DAYS = [1, 3, 7, 14]


def _normalize_days(days: Optional[List[int]]) -> List[int]:
    values = days or DEFAULT_RETENTION_DAYS
    normalized = []
    for day in values:
        try:
            value = int(day)
        except (TypeError, ValueError):
            continue
        if value > 0 and value not in normalized:
            normalized.append(value)
    return normalized or DEFAULT_RETENTION_DAYS


def _group_key(row: pd.Series, dimensions: List[str]) -> str:
    parts = []
    for dim in dimensions:
        value = row.get(dim)
        if pd.isna(value):
            value = "未知"
        parts.append(str(value))
    return " | ".join(parts)


def calculate_dynamic_retention(
    df: pd.DataFrame,
    cfg: FieldConfig,
    reg_start: pd.Timestamp,
    reg_end: pd.Timestamp,
    dimension_sets: Optional[List[List[str]]] = None,
    retention_days: Optional[List[int]] = None,
    active_definition: Optional[Callable] = None,
    max_dimensions: int = MAX_DIMENSIONS,
    max_groups: int = MAX_GROUPS,
    min_cohort_size: int = MIN_COHORT_SIZE,
) -> List[Dict[str, Any]]:
    """Calculate retention for arbitrary single or combined dimensions.

    The retention definition is identical to ``get_churn_users`` and therefore
    stays aligned with the existing ``calculate_retention`` behavior.
    """
    if not dimension_sets:
        return []

    days = _normalize_days(retention_days)
    uid_col = cfg.user_id
    warnings: List[str] = []

    valid_dimension_sets: List[List[str]] = []
    seen = set()
    for raw_dims in dimension_sets:
        dims = [str(dim).strip() for dim in raw_dims or [] if str(dim).strip()]
        dims = list(dict.fromkeys(dims))
        if not dims:
            continue
        if len(dims) > max_dimensions:
            warnings.append(f"维度组合 {dims} 超过 {max_dimensions} 个字段，已跳过。")
            continue
        missing = [dim for dim in dims if dim not in df.columns]
        if missing:
            warnings.append(f"维度组合 {dims} 缺少字段 {missing}，已跳过。")
            continue
        key = tuple(dims)
        if key in seen:
            continue
        seen.add(key)
        valid_dimension_sets.append(dims)

    if not valid_dimension_sets:
        return [{"dimensions": [], "groups": [], "warnings": warnings}]

    window_mask = (
        df[cfg.reg_date].notna()
        & (df[cfg.reg_date] >= reg_start)
        & (df[cfg.reg_date] <= reg_end)
    )
    df_window = df.loc[window_mask].copy()
    if df_window.empty:
        return [
            {"dimensions": dims, "groups": [], "warnings": [*warnings, "注册窗口内没有用户。"]}
            for dims in valid_dimension_sets
        ]

    user_reg = (
        df_window.groupby(uid_col, as_index=False)[cfg.reg_date]
        .min()
        .rename(columns={cfg.reg_date: "reg_dt"})
    )
    cohort_users = set(user_reg[uid_col].tolist())

    overall: Dict[str, float] = {}
    retained_by_day: Dict[int, set] = {}
    for day in days:
        _, retained_users = get_churn_users(
            df,
            cfg,
            reg_start,
            reg_end,
            retention_days=day,
            active_definition=active_definition,
        )
        retained_set = set(retained_users)
        retained_by_day[day] = retained_set
        overall[f"D{day}"] = round(len(retained_set) / len(cohort_users), 4) if cohort_users else 0.0

    results: List[Dict[str, Any]] = []
    for dims in valid_dimension_sets:
        user_dims = (
            df_window[df_window[uid_col].isin(cohort_users)]
            .sort_values([uid_col, cfg.event_time] if cfg.event_time in df_window.columns else [uid_col])
            .groupby(uid_col, as_index=False)[dims]
            .first()
        )
        user_dims["group_key"] = user_dims.apply(lambda row: _group_key(row, dims), axis=1)
        group_sizes = user_dims.groupby("group_key")[uid_col].nunique().sort_values(ascending=False)

        local_warnings = list(warnings)
        if len(group_sizes) > max_groups:
            local_warnings.append(f"分组数 {len(group_sizes)} 超过上限 {max_groups}，仅返回样本量最大的前 {max_groups} 组。")
            group_sizes = group_sizes.head(max_groups)

        groups = []
        for group_key, cohort_size in group_sizes.items():
            users = set(user_dims.loc[user_dims["group_key"] == group_key, uid_col].tolist())
            retention: Dict[str, float] = {}
            gap_vs_overall: Dict[str, float] = {}
            for day in days:
                label = f"D{day}"
                rate = len(users & retained_by_day[day]) / cohort_size if cohort_size else 0.0
                retention[label] = round(rate, 4)
                gap_vs_overall[label] = round(rate - overall[label], 4)
            groups.append({
                "group_key": str(group_key),
                "cohort_size": int(cohort_size),
                "retention": retention,
                "gap_vs_overall": gap_vs_overall,
                "sample_warning": int(cohort_size) < min_cohort_size,
            })

        results.append({
            "dimensions": dims,
            "groups": groups,
            "warnings": local_warnings,
        })

    return results
