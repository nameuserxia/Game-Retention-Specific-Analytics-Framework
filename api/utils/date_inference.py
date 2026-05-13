# -*- coding: utf-8 -*-
"""
api/utils/date_inference.py
日期格式自动推断引擎
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import List, Optional, Tuple

import pandas as pd


# 常见日期格式（按优先级排列）
DATE_FORMATS = [
    # 欧洲格式（dayfirst=True 时优先）
    "%d/%m/%Y",
    "%d/%m/%Y %H:%M:%S",
    "%d-%m-%Y",
    "%d-%m-%Y %H:%M:%S",
    "%d.%m.%Y",
    "%d.%m.%Y %H:%M:%S",
    
    # 美国格式
    "%m/%d/%Y",
    "%m/%d/%Y %H:%M:%S",
    "%m-%d-%Y",
    "%m-%d-%Y %H:%M:%S",
    
    # ISO 格式
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d",
    "%Y/%m/%d %H:%M:%S",
    
    # 其他常见格式
    "%Y%m%d",
    "%Y%m%d %H:%M:%S",
    "%d %b %Y",
    "%d %b %Y %H:%M:%S",
    "%b %d, %Y",
    "%B %d, %Y",
]


class DateInference:
    """
    日期格式自动推断引擎
    
    功能：
    1. 尝试多种常见日期格式
    2. 返回最佳匹配格式和置信度
    3. 支持 dayfirst 参数（欧洲 vs 美国）
    """

    @staticmethod
    def infer_format(
        series: pd.Series,
        dayfirst: bool = True,
        max_attempts: int = 1000
    ) -> Tuple[Optional[str], float]:
        """
        推断日期格式
        
        Args:
            series: 日期字符串 Series
            dayfirst: 是否优先欧洲格式（DD/MM/YYYY）
            max_attempts: 最大尝试次数
            
        Returns:
            (format_string, confidence)
            - format_string: 推断的格式（如 "%d/%m/%Y"）
            - confidence: 置信度 (0.0 ~ 1.0)
        """
        # 获取非空值
        sample = series.dropna().head(max_attempts)
        if len(sample) == 0:
            return None, 0.0
        
        # 格式分组（根据 dayfirst）
        if dayfirst:
            format_order = [
                ["%d/%m/%Y", "%d/%m/%Y %H:%M:%S"],
                ["%d-%m-%Y", "%d-%m-%Y %H:%M:%S"],
                ["%m/%d/%Y", "%m/%d/%Y %H:%M:%S"],
                ["%m-%d-%Y", "%m-%d-%Y %H:%M:%S"],
                ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S"],
            ]
        else:
            format_order = [
                ["%m/%d/%Y", "%m/%d/%Y %H:%M:%S"],
                ["%m-%d-%Y", "%m-%d-%Y %H:%M:%S"],
                ["%d/%m/%Y", "%d/%m/%Y %H:%M:%S"],
                ["%d-%m-%Y", "%d-%m-%Y %H:%M:%S"],
                ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S"],
            ]
        
        best_format = None
        best_success_rate = 0.0
        
        for format_group in format_order:
            for fmt in format_group:
                success_count = 0
                total_count = 0
                
                for value in sample:
                    if pd.isna(value):
                        continue
                    
                    total_count += 1
                    try:
                        if isinstance(value, (int, float)):
                            # 尝试解析为时间戳
                            datetime.fromtimestamp(value)
                            success_count += 1
                        else:
                            datetime.strptime(str(value).strip(), fmt)
                            success_count += 1
                    except (ValueError, TypeError, OSError):
                        pass
                
                if total_count > 0:
                    success_rate = success_count / total_count
                    if success_rate > best_success_rate:
                        best_success_rate = success_rate
                        best_format = fmt
        
        return best_format, best_success_rate

    @staticmethod
    def parse_with_inference(
        series: pd.Series,
        dayfirst: bool = True,
        errors: str = "coerce"
    ) -> Tuple[pd.Series, Optional[str], float]:
        """
        使用推断的格式解析日期
        
        Args:
            series: 日期字符串 Series
            dayfirst: 是否优先欧洲格式
            errors: 解析错误处理方式 ('coerce' | 'ignore' | 'raise')
            
        Returns:
            (parsed_series, inferred_format, confidence)
        """
        # 先尝试推断格式
        inferred_format, confidence = DateInference.infer_format(series, dayfirst)
        
        if inferred_format is None or confidence == 0.0:
            # 格式推断失败，尝试 pd.to_datetime 自动推断
            parsed = pd.to_datetime(
                series,
                dayfirst=dayfirst,
                errors=errors
            )
            return parsed, None, 0.0
        
        # 使用推断的格式解析
        try:
            parsed = pd.to_datetime(
                series,
                format=inferred_format,
                errors=errors
            )
            return parsed, inferred_format, confidence
        except Exception:
            # 格式解析失败，fallback 到 pd.to_datetime
            parsed = pd.to_datetime(
                series,
                dayfirst=dayfirst,
                errors=errors
            )
            return parsed, None, 0.0


# ── 便捷函数 ─────────────────────────────────────────────────

def infer_and_parse_dates(
    series: pd.Series,
    dayfirst: bool = True,
    errors: str = "coerce"
) -> pd.Series:
    """
    自动推断并解析日期列
    
    Args:
        series: 日期字符串 Series
        dayfirst: 是否优先日/月/年格式（默认 True，适合部分国际化日志）
        errors: 错误处理
        
    Returns:
        解析后的 datetime Series
    """
    return DateInference.parse_with_inference(series, dayfirst, errors)[0]


def detect_date_columns(
    df: pd.DataFrame,
    sample_size: int = 100
) -> dict:
    """
    检测 DataFrame 中可能是日期的列
    
    Args:
        df: DataFrame
        sample_size: 采样大小
        
    Returns:
        {column_name: {likely_date: bool, inferred_format: str, confidence: float}}
    """
    results = {}
    
    for col in df.columns:
        series = df[col].dropna().head(sample_size)
        
        # 检查是否已经是 datetime 类型
        if pd.api.types.is_datetime64_any_dtype(series):
            results[col] = {
                "likely_date": True,
                "already_parsed": True,
                "inferred_format": "datetime",
                "confidence": 1.0
            }
            continue
        
        # 检查数据类型
        if df[col].dtype == "object":
            # 尝试解析
            parsed, fmt, conf = DateInference.infer_format(series, dayfirst=True)
            results[col] = {
                "likely_date": conf > 0.5,
                "already_parsed": False,
                "inferred_format": fmt,
                "confidence": conf
            }
        else:
            results[col] = {
                "likely_date": False,
                "already_parsed": False,
                "inferred_format": None,
                "confidence": 0.0
            }
    
    return results


def suggest_date_format(examples: List[str]) -> Tuple[Optional[str], float]:
    """
    根据日期字符串示例推断格式
    
    Args:
        examples: 日期字符串列表
        
    Returns:
        (format_string, confidence)
    """
    if not examples:
        return None, 0.0
    
    series = pd.Series(examples)
    return DateInference.infer_format(series, dayfirst=True)
