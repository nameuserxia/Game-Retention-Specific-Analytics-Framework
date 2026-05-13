# -*- coding: utf-8 -*-
"""Churn and retained-user identification."""

from __future__ import annotations

from datetime import timedelta
from typing import Callable, List, Optional, Tuple

import pandas as pd

from core.utils import FieldConfig


def get_churn_users(
    df: pd.DataFrame,
    cfg: FieldConfig,
    reg_start: pd.Timestamp,
    reg_end: pd.Timestamp,
    retention_days: int = 1,
    active_definition: Optional[Callable[[pd.DataFrame, str, pd.Timestamp], bool]] = None,
) -> Tuple[List[str], List[str]]:
    """
    识别流失用户与留存用户。
    
    Parameters
    ----------
    df : pd.DataFrame
        原始日志（已解析日期）
    cfg : FieldConfig
        字段配置
    reg_start, reg_end : pd.Timestamp
        注册窗口（含边界）
    retention_days : int
        留存观察天数，默认 1（即 D+1 次日留存）
        D+7 则传 7，D+30 则传 30
    active_definition : Callable, optional
        自定义活跃判定函数，签名为：
            func(user_df: pd.DataFrame, uid: str, check_date: pd.Timestamp) -> bool
        若为 None，则使用默认逻辑：check_date 当天有任意事件记录即为活跃
    
    Returns
    -------
    (churn_users, retention_users) : 流失用户ID列表，留存用户ID列表
    
    Example
    -------
    # 默认 D+1 留存
    churn, retained = get_churn_users(df, cfg, reg_start, reg_end, retention_days=1)
    
    # 自定义活跃定义：D+1 且完成过关卡才算"真活跃"
    def level_active(user_df, uid, check_date):
        day_df = user_df[user_df[cfg.event_date] == check_date]
        return day_df[cfg.event_name].str.startswith('ugd_level').any()
    
    churn, retained = get_churn_users(df, cfg, reg_start, reg_end,
                                       active_definition=level_active)
    """
    uid_col = cfg.user_id
    reg_col = cfg.reg_date
    date_col = cfg.event_date

    window_mask = (
        df[reg_col].notna()
        & (df[reg_col] >= reg_start)
        & (df[reg_col] <= reg_end)
    )
    df_window = df.loc[window_mask].copy()
    if df_window.empty:
        return [], []

    user_reg = (
        df_window.groupby(uid_col, as_index=False)[reg_col]
        .min()
        .rename(columns={reg_col: "reg_dt"})
    )

    if active_definition is not None:
        user_groups = {uid: group for uid, group in df_window.groupby(uid_col)}
        churn_users: List[str] = []
        retention_users: List[str] = []
        for row in user_reg.itertuples(index=False):
            uid = getattr(row, uid_col) if hasattr(row, uid_col) else row[0]
            reg_dt = row.reg_dt
            if pd.isna(reg_dt):
                churn_users.append(uid)
                continue
            check_date = pd.Timestamp(reg_dt) + timedelta(days=retention_days)
            user_df = user_groups.get(uid, pd.DataFrame())
            if active_definition(user_df, uid, check_date):
                retention_users.append(uid)
            else:
                churn_users.append(uid)
        return churn_users, retention_users

    user_reg["check_date"] = user_reg["reg_dt"].dt.normalize() + pd.to_timedelta(retention_days, unit="D")

    active_pairs = (
        df.loc[
            df[uid_col].isin(user_reg[uid_col]) & df[date_col].notna(),
            [uid_col, date_col],
        ]
        .drop_duplicates()
        .copy()
    )
    active_pairs[date_col] = active_pairs[date_col].dt.normalize()

    retained = user_reg.merge(
        active_pairs,
        left_on=[uid_col, "check_date"],
        right_on=[uid_col, date_col],
        how="left",
        indicator=True,
    )
    retained_mask = retained["_merge"].eq("both")
    retention_users = retained.loc[retained_mask, uid_col].tolist()
    churn_users = retained.loc[~retained_mask, uid_col].tolist()
    return churn_users, retention_users


# ============================================================
# 留存率计算
# ============================================================
