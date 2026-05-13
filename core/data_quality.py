# -*- coding: utf-8 -*-
"""Data quality checks for retention analysis."""

from __future__ import annotations

import warnings
from typing import Dict

import pandas as pd

from core.utils import FieldConfig


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

    print(f"[SanityCheck] {'[OK]' if report['passed'] else '[FAIL]'} | "
          f"警告 {len(report['warnings'])} 项 | 错误 {len(report['errors'])} 项")
    for w in report['warnings']:
        print(f"  [WARN] {w}")
    for e in report['errors']:
        print(f"  [ERROR] {e}")

    return report


# ============================================================
# 流失用户识别
# ============================================================
