# -*- coding: utf-8 -*-
"""Cohort retention matrix construction."""

from __future__ import annotations

from datetime import timedelta
from typing import Callable, Dict, Optional

import pandas as pd

from core.utils import FieldConfig


def build_cohort_matrix(
    df: pd.DataFrame,
    cfg: FieldConfig,
    max_days: int = 30,
    cohort_freq: str = 'W',
    active_definition: Optional[Callable] = None,
) -> pd.DataFrame:
    """
    构建 Cohort 留存矩阵。
    
    Parameters
    ----------
    df : pd.DataFrame
        原始日志（已解析日期）
    cfg : FieldConfig
        字段配置
    max_days : int
        观察天数上限，默认 30 天
    cohort_freq : str
        Cohort 分群频率，'W' = 按周，'M' = 按月，'D' = 按天
    active_definition : Callable, optional
        自定义活跃判定：
            func(user_df, uid, check_date) -> bool
        若为 None，则使用默认逻辑（当天有任意记录）
    
    Returns
    -------
    pd.DataFrame :
        行 = Cohort（注册周/月），列 = D+N，值 = 留存率(%)
        索引为 cohort 开始日期字符串
    
    Example
    -------
    matrix = build_cohort_matrix(df, cfg, max_days=14, cohort_freq='W')
    print(matrix.to_string())
    
    # 自定义活跃：D+N 当天完成过至少1个关卡才算留存
    def level_active(user_df, uid, check_date):
        return (
            user_df[(user_df[cfg.event_date] == check_date) &
                    (user_df[cfg.event_name] == 'ugd_level_start')]
            .shape[0] > 0
        )
    matrix = build_cohort_matrix(df, cfg, max_days=7, active_definition=level_active)
    """
    uid_col = cfg.user_id
    date_col = cfg.event_date
    reg_col = cfg.reg_date

    user_info = (
        df.dropna(subset=[reg_col])
        .groupby(uid_col)[reg_col]
        .min()
        .reset_index()
        .rename(columns={reg_col: 'reg_dt'})
    )
    if user_info.empty:
        return pd.DataFrame(columns=[f'D+{d}' for d in range(max_days + 1)])

    user_info['cohort'] = user_info['reg_dt'].dt.to_period(cohort_freq).apply(
        lambda p: p.start_time
    )

    if active_definition is not None:
        cohort_results: Dict[pd.Timestamp, Dict[int, float]] = {}
        user_groups = {uid: group for uid, group in df.groupby(uid_col)}
        for cohort_dt, cohort_group in user_info.groupby('cohort'):
            cohort_users = cohort_group[uid_col].tolist()
            n_cohort = len(cohort_users)
            if n_cohort == 0:
                continue
            cohort_results[cohort_dt] = {}
            reg_lookup = cohort_group.set_index(uid_col)['reg_dt'].to_dict()
            for d in range(0, max_days + 1):
                active_count = 0
                for uid in cohort_users:
                    check_date = pd.Timestamp(reg_lookup[uid]).normalize() + timedelta(days=d)
                    user_df = user_groups.get(uid, pd.DataFrame())
                    if active_definition(user_df, uid, check_date):
                        active_count += 1
                cohort_results[cohort_dt][d] = round(active_count / n_cohort * 100, 2)
        matrix = pd.DataFrame(cohort_results).T
        matrix.index = [str(dt.date()) for dt in matrix.index]
        matrix.columns = [f'D+{d}' for d in matrix.columns]
        return matrix

    active_days = (
        df.dropna(subset=[date_col])[[uid_col, date_col]]
        .drop_duplicates()
        .merge(user_info[[uid_col, 'reg_dt', 'cohort']], on=uid_col, how='inner')
    )
    active_days['day'] = (
        active_days[date_col].dt.normalize() - active_days['reg_dt'].dt.normalize()
    ).dt.days
    active_days = active_days[
        active_days['day'].between(0, max_days)
    ].drop_duplicates([uid_col, 'cohort', 'day'])

    cohort_sizes = user_info.groupby('cohort')[uid_col].nunique()
    if active_days.empty:
        counts = pd.DataFrame(0, index=cohort_sizes.index, columns=range(max_days + 1))
    else:
        counts = active_days.pivot_table(
            index='cohort',
            columns='day',
            values=uid_col,
            aggfunc='nunique',
            fill_value=0,
        )
        counts = counts.reindex(index=cohort_sizes.index, columns=range(max_days + 1), fill_value=0)
    matrix = counts.div(cohort_sizes, axis=0).mul(100).round(2)
    matrix.index = [str(dt.date()) for dt in matrix.index]
    matrix.columns = [f'D+{d}' for d in matrix.columns]
    return matrix


# ============================================================
# 最后 N 条日志提取
# ============================================================
