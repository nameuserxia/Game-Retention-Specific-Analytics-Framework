# -*- coding: utf-8 -*-
"""Retention rate calculation."""

from __future__ import annotations

from typing import Callable, Dict, List, Optional

import pandas as pd

from core.churn import get_churn_users
from core.utils import FieldConfig


def calculate_retention(
    df: pd.DataFrame,
    cfg: FieldConfig,
    reg_start: pd.Timestamp,
    reg_end: pd.Timestamp,
    retention_days: int = 1,
    segment_col: Optional[str] = None,
    active_definition: Optional[Callable] = None,
) -> pd.DataFrame:
    """
    计算留存率，支持按维度分群（如按渠道、国家）。
    
    Parameters
    ----------
    df : pd.DataFrame
        原始日志
    cfg : FieldConfig
        字段配置
    reg_start, reg_end : pd.Timestamp
        注册窗口
    retention_days : int
        留存观察天数（D+1 传 1，D+7 传 7，以此类推）
    segment_col : str, optional
        分群维度列名（如 'country'、'channel'）
        若为 None，只计算全体留存率
    active_definition : Callable, optional
        自定义活跃判定（同 get_churn_users）
    
    Returns
    -------
    pd.DataFrame : 包含列 [segment, n_total, n_retained, retention_rate, note]
    
    Example
    -------
    # 全体 D+1 留存
    result = calculate_retention(df, cfg, reg_start, reg_end, retention_days=1)
    
    # 按国家分群 D+7 留存
    result = calculate_retention(df, cfg, reg_start, reg_end,
                                  retention_days=7, segment_col='country')
    """
    churn_users, retained_users = get_churn_users(
        df, cfg, reg_start, reg_end, retention_days, active_definition
    )
    all_users = set(churn_users) | set(retained_users)
    retained_set = set(retained_users)

    uid_col = cfg.user_id
    df_window = df[
        (df[cfg.reg_date].notna())
        & (df[cfg.reg_date] >= reg_start)
        & (df[cfg.reg_date] <= reg_end)
    ].copy()

    rows: List[Dict] = []

    if segment_col is None:
        n_total = len(all_users)
        n_retained = len(retained_set)
        rate = n_retained / n_total * 100 if n_total > 0 else 0.0
        note = "[!] 样本量不足30，谨慎解读" if n_total < 30 else ""
        rows.append({
            'segment': '全体',
            'n_total': n_total,
            'n_retained': n_retained,
            'retention_rate': round(rate, 2),
            'note': note,
        })
    else:
        status_df = pd.DataFrame({
            uid_col: list(all_users),
            "retained": [uid in retained_set for uid in all_users],
        })
        user_segment = (
            df_window[df_window[uid_col].isin(all_users)]
            .sort_values([uid_col, cfg.event_time] if cfg.event_time in df_window.columns else [uid_col])
            .groupby(uid_col, as_index=False)[segment_col]
            .first()
        )
        segment_status = status_df.merge(user_segment, on=uid_col, how="left")
        segment_status[segment_col] = segment_status[segment_col].fillna("未知")
        grouped = (
            segment_status.groupby(segment_col, dropna=False)
            .agg(n_total=(uid_col, "nunique"), n_retained=("retained", "sum"))
            .reset_index()
            .sort_values("n_total", ascending=False)
        )

        for item in grouped.itertuples(index=False):
            seg = getattr(item, segment_col) if hasattr(item, segment_col) else item[0]
            n_total = int(item.n_total)
            n_retained = int(item.n_retained)
            rate = n_retained / n_total * 100 if n_total > 0 else 0.0
            note = "[!] 样本量不足30，谨慎解读" if n_total < 30 else ""
            rows.append({
                'segment': seg,
                'n_total': n_total,
                'n_retained': n_retained,
                'retention_rate': round(rate, 2),
                'note': note,
            })

    return pd.DataFrame(rows)


# ============================================================
# Cohort 留存矩阵
# ============================================================
