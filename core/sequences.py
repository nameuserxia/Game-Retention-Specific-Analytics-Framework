# -*- coding: utf-8 -*-
"""User event sequence and path helpers."""

from __future__ import annotations

from collections import Counter
from typing import Callable, Dict, List, Optional, Set

import pandas as pd

from core.utils import FieldConfig


def get_last_n_events(
    df: pd.DataFrame,
    cfg: FieldConfig,
    user_ids: Optional[List[str]] = None,
    n: int = 5,
    event_filter: Optional[Set[str]] = None,
    time_col: Optional[str] = None,
) -> pd.DataFrame:
    """
    提取每个用户的最后 N 条日志。
    
    Parameters
    ----------
    df : pd.DataFrame
        日志 DataFrame
    cfg : FieldConfig
        字段配置
    user_ids : list, optional
        限定用户列表；若为 None，则对 df 中所有用户处理
    n : int
        取最后 N 条，默认 5
    event_filter : set, optional
        只统计指定事件名的记录（如只看广告事件）
        若为 None，则不过滤
    time_col : str, optional
        排序用的时间列，默认使用 cfg.event_time；
        若 event_time 不在 df 中，则 fallback 到 cfg.event_date
    
    Returns
    -------
    pd.DataFrame : 每个用户最多 N 条记录，按用户升序、时间倒序排列
    """
    uid_col = cfg.user_id
    sort_col = time_col or (cfg.event_time if cfg.event_time in df.columns else cfg.event_date)

    df_target = df.copy()
    if user_ids is not None:
        df_target = df_target[df_target[uid_col].isin(user_ids)]
    if event_filter is not None:
        df_target = df_target[df_target[cfg.event_name].isin(event_filter)]

    df_sorted = df_target.sort_values(
        [uid_col, sort_col], ascending=[True, False]
    )
    last_n = df_sorted.groupby(uid_col).head(n)
    return last_n


# ============================================================
# 行为路径序列构建
# ============================================================

def build_event_sequences(
    df: pd.DataFrame,
    cfg: FieldConfig,
    user_ids: Optional[List[str]] = None,
    n: int = 5,
    simplify_fn: Optional[Callable[[str], str]] = None,
    separator: str = ' → ',
) -> Dict[str, str]:
    """
    为每个用户构建最后 N 步的事件序列字符串。
    
    Parameters
    ----------
    df : pd.DataFrame
        日志 DataFrame
    cfg : FieldConfig
        字段配置
    user_ids : list, optional
        限定用户列表
    n : int
        序列长度，默认 5
    simplify_fn : Callable, optional
        事件名简化函数：str -> str
        用于把长事件名压缩为可读的短名（如 'ugd_level_start' → '关卡开始'）
        若为 None，则直接使用原始事件名
    separator : str
        路径分隔符，默认 ' → '
    
    Returns
    -------
    Dict[str, str] : {user_id: '事件A → 事件B → 事件C'}
    
    Example
    -------
    def simplify(name):
        mapping = {'ugd_level_start': '关卡开始', 'ugd_Interstitial': '插屏广告'}
        return mapping.get(name, name)
    
    seqs = build_event_sequences(df_churn, cfg, simplify_fn=simplify)
    top5 = Counter(seqs.values()).most_common(5)
    """
    uid_col = cfg.user_id
    sort_col = cfg.event_time if cfg.event_time in df.columns else cfg.event_date

    df_target = df.copy()
    if user_ids is not None:
        df_target = df_target[df_target[uid_col].isin(user_ids)]

    # 正序排列，tail(n) = 最后 n 条
    df_sorted = df_target.sort_values([uid_col, sort_col], ascending=[True, True])

    sequences: Dict[str, str] = {}
    for uid, group in df_sorted.groupby(uid_col):
        tail = group.tail(n)[cfg.event_name].tolist()
        if simplify_fn is not None:
            tail = [simplify_fn(e) for e in tail]
        sequences[str(uid)] = separator.join(tail)

    return sequences


# ============================================================
# 便捷函数：获取 Top N 行为路径
# ============================================================

def get_top_paths(
    sequences: Dict[str, str],
    n_total: int,
    top_n: int = 5,
) -> List[Dict]:
    """
    统计 Top N 行为路径。
    
    Parameters
    ----------
    sequences : Dict[str, str]
        build_event_sequences() 的输出
    n_total : int
        总用户数（用于计算百分比）
    top_n : int
        返回前 N 条，默认 5
    
    Returns
    -------
    List[Dict] : 每项包含 rank, path, count, pct
    """
    counter = Counter(sequences.values())
    results = []
    for rank, (path, count) in enumerate(counter.most_common(top_n), 1):
        results.append({
            'rank': rank,
            'path': path,
            'count': count,
            'pct': round(count / n_total * 100, 2) if n_total > 0 else 0.0,
        })
    return results
