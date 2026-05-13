# -*- coding: utf-8 -*-
"""
core/analytics.py
通用游戏留存分析核心模块

包含：
  - sanity_check()          : 数据质量校验（校验先行）
  - calculate_retention()   : 灵活的留存率计算
  - build_cohort_matrix()   : Cohort 留存矩阵
  - get_churn_users()       : 流失用户识别
  - get_last_n_events()     : 最后N条日志提取
  - build_event_sequences() : 行为路径序列构建

设计原则：
  1. 所有函数接受 DataFrame + 字段配置（FieldConfig），不硬编码字段名
  2. 类型提示 + 异常处理，便于 Agent 调用
  3. 校验先行：处理数据前强制执行 sanity_check
"""

from __future__ import annotations

import warnings
from collections import Counter
from datetime import timedelta
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd


# ============================================================
# 字段配置类（多游戏兼容的关键）
# ============================================================

@dataclass
class FieldConfig:
    """
    字段名称配置。
    通过 config_loader 从 YAML 文件加载，实现多游戏兼容。
    
    Example:
        # Example game configuration
        cfg = FieldConfig(
            user_id='user_id',
            event_time='event_time',
            event_date='event_date',
            reg_date='reg_date',
            event_name='event_name',
            country='country',
            channel='channel',
        )
    """
    user_id: str = 'user_id'
    event_time: str = 'event_time'
    event_date: str = 'event_date'
    reg_date: str = 'reg_date'
    event_name: str = 'event_name'
    country: str = 'country'
    channel: str = 'channel'
    # 可选扩展字段
    extra_fields: Dict[str, str] = field(default_factory=dict)


# ============================================================
# 校验先行：sanity_check
# ============================================================

class SanityCheckError(Exception):
    """数据质量校验失败时抛出"""
    pass


class SanityCheckWarning(UserWarning):
    """数据质量存在风险但不阻断流程时发出"""
    pass


def sanity_check(
    df: pd.DataFrame,
    cfg: FieldConfig,
    min_sample_size: int = 30,
    max_date_parse_failure_rate: float = 0.01,
    raise_on_failure: bool = True,
) -> Dict[str, object]:
    """
    数据质量校验（校验先行模式）。
    
    执行以下检查：
      1. 日期解析失败率是否超标
      2. 样本量是否过小（n < min_sample_size）
      3. 生存者偏倚风险提示（留存期内数据是否完整）
      4. 必要字段是否存在
    
    Parameters
    ----------
    df : pd.DataFrame
        原始日志 DataFrame（日期字段已解析为 datetime 类型）
    cfg : FieldConfig
        字段配置
    min_sample_size : int
        最小样本量阈值，默认 30
    max_date_parse_failure_rate : float
        日期解析失败率上限，默认 1%
    raise_on_failure : bool
        True 时严重错误直接 raise SanityCheckError；
        False 时只打印警告（用于探索性分析）
    
    Returns
    -------
    Dict : 校验结果报告，包含每项检查的状态和细节
    
    Raises
    ------
    SanityCheckError : 当 raise_on_failure=True 且发现严重问题时
    """
    report: Dict[str, object] = {
        'passed': True,
        'warnings': [],
        'errors': [],
        'stats': {}
    }

    def _fail(msg: str) -> None:
        report['errors'].append(msg)
        report['passed'] = False
        if raise_on_failure:
            raise SanityCheckError(f"[SanityCheck FAIL] {msg}")
        else:
            warnings.warn(f"[SanityCheck FAIL] {msg}", SanityCheckWarning, stacklevel=3)

    def _warn(msg: str) -> None:
        report['warnings'].append(msg)
        warnings.warn(f"[SanityCheck WARN] {msg}", SanityCheckWarning, stacklevel=3)

    # ── 检查 1：必要字段存在性 ──
    required_fields = [cfg.user_id, cfg.event_date, cfg.reg_date, cfg.event_name]
    missing = [f for f in required_fields if f not in df.columns]
    if missing:
        _fail(f"缺少必要字段：{missing}。当前列：{list(df.columns)}")

    # ── 检查 2：日期字段已解析（不是 object 类型） ──
    for date_col in [cfg.event_date, cfg.reg_date]:
        if date_col in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
                _fail(
                    f"字段 '{date_col}' 类型为 {df[date_col].dtype}，"
                    f"需先用 pd.to_datetime() 解析为 datetime 类型。"
                    f"常见格式：format='%d/%m/%Y'"
                )

    # ── 检查 3：日期解析失败率 ──
    for date_col in [cfg.event_date, cfg.reg_date]:
        if date_col in df.columns and pd.api.types.is_datetime64_any_dtype(df[date_col]):
            null_cnt = df[date_col].isna().sum()
            null_rate = null_cnt / len(df)
            report['stats'][f'{date_col}_null_rate'] = null_rate
            if null_rate > max_date_parse_failure_rate:
                _fail(
                    f"字段 '{date_col}' 解析失败率 {null_rate:.1%}（阈值 {max_date_parse_failure_rate:.1%}）。"
                    f"共 {null_cnt} 行失败，请检查日期格式。"
                )

    # ── 检查 4：用户数样本量 ──
    if cfg.user_id in df.columns and cfg.reg_date in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[cfg.reg_date]):
            n_users = df[cfg.user_id].nunique()
            report['stats']['total_users'] = n_users
            if n_users < min_sample_size:
                _fail(
                    f"样本量过小：仅 {n_users} 个用户（阈值 {min_sample_size}）。"
                    f"结论不具统计意义，请扩大数据范围。"
                )

    # ── 检查 5：生存者偏倚风险 ──
    # 若数据最大日期距今不足 7 天，留存指标可能不完整
    if cfg.event_date in df.columns and pd.api.types.is_datetime64_any_dtype(df[cfg.event_date]):
        max_date = df[cfg.event_date].max()
        today = pd.Timestamp.today().normalize()
        days_to_today = (today - max_date).days
        report['stats']['data_max_date'] = str(max_date.date())
        report['stats']['days_to_today'] = days_to_today
        if days_to_today < 7:
            _warn(
                f"数据最新日期 {max_date.date()} 距今仅 {days_to_today} 天。"
                f"近期注册用户的 D+7/D+30 留存数据尚不完整，存在生存者偏倚风险。"
                f"建议在报告中注明数据截止日期。"
            )

    # ── 检查 6：重复用户ID + 时间的行 ──
    if cfg.user_id in df.columns and cfg.event_time in df.columns:
        dup_count = df.duplicated(subset=[cfg.user_id, cfg.event_time]).sum()
        report['stats']['duplicate_rows'] = int(dup_count)
        if dup_count > 0:
            _warn(f"存在 {dup_count} 条重复记录（user_id + event_time 相同），可能影响序列分析。")

    print(f"[SanityCheck] {'✅ 通过' if report['passed'] else '❌ 失败'} | "
          f"警告 {len(report['warnings'])} 项 | 错误 {len(report['errors'])} 项")
    for w in report['warnings']:
        print(f"  ⚠️  {w}")
    for e in report['errors']:
        print(f"  ❌ {e}")

    return report


# ============================================================
# 流失用户识别
# ============================================================

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
        note = "⚠️ 样本量不足30，谨慎解读" if n_total < 30 else ""
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
            note = "⚠️ 样本量不足30，谨慎解读" if n_total < 30 else ""
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
