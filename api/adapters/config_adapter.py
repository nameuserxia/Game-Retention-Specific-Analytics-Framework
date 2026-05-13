# -*- coding: utf-8 -*-
"""
api/adapters/config_adapter.py
ConfigAdapter: JSON 映射 → FieldConfig 转换器
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd

import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.analytics import FieldConfig, SanityCheckError
from api.utils.date_inference import infer_and_parse_dates, detect_date_columns
from api.models.schemas import FieldMappingRequest, DateParseResult

logger = logging.getLogger(__name__)


@dataclass
class FieldMapping:
    """
    前端传入的字段映射（7 个标准字段 + 扩展字段）
    
    与 Pydantic 模型 FieldMappingRequest 保持一致
    """
    user_id: str
    event_time: str
    event_date: str
    reg_date: str
    event_name: str
    country: Optional[str] = None
    channel: Optional[str] = None
    json_params: Optional[str] = None
    extra_fields: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_request(cls, request: FieldMappingRequest) -> "FieldMapping":
        """从 Pydantic 请求创建"""
        return cls(
            user_id=request.user_id,
            event_time=request.event_time,
            event_date=request.event_date,
            reg_date=request.reg_date,
            event_name=request.event_name,
            country=request.country,
            channel=request.channel,
            json_params=request.json_params,
            extra_fields=request.extra_fields or {},
        )

    @classmethod
    def from_dict(cls, data: dict) -> "FieldMapping":
        """从字典创建"""
        return cls(
            user_id=data["user_id"],
            event_time=data["event_time"],
            event_date=data["event_date"],
            reg_date=data["reg_date"],
            event_name=data["event_name"],
            country=data.get("country"),
            channel=data.get("channel"),
            json_params=data.get("json_params"),
            extra_fields=data.get("extra_fields", {}),
        )

    def to_field_config(self) -> FieldConfig:
        """转换为框架的 FieldConfig。

        apply_mapping() 会先把用户选择的原始列名重命名为框架标准列名，
        因此后续核心分析模块必须读取标准列名，而不是原始列名。
        """
        return FieldConfig(
            user_id="user_id",
            event_time="event_time",
            event_date="event_date",
            reg_date="reg_date",
            event_name="event_name",
            country="country",
            channel="channel",
            extra_fields=self.extra_fields,
        )


class ConfigAdapter:
    """
    将前端 JSON 映射转换为 FieldConfig
    
    职责：
    1. 字段重命名（原始列名 → 框架标准名）
    2. 日期自动推断解析
    3. 与 core/analytics.py 无缝衔接
    """

    def __init__(self, mapping: FieldMapping):
        self.mapping = mapping
        self.field_config = mapping.to_field_config()

    def apply_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        应用字段映射 + 日期解析
        
        Args:
            df: 原始 DataFrame
            
        Returns:
            映射并解析后的 DataFrame
        """
        df = df.copy()

        # 1. 字段重命名（实际列名 → 框架标准名）
        rename_map = {}
        for std_name in ["user_id", "event_time", "event_date", "reg_date",
                         "event_name", "country", "channel", "json_params"]:
            actual_name = getattr(self.mapping, std_name)
            if actual_name and actual_name in df.columns:
                rename_map[actual_name] = std_name
        
        df = df.rename(columns=rename_map)

        # 2. 日期自动推断解析
        date_cols = ["event_time", "event_date", "reg_date"]
        for col in date_cols:
            if col in df.columns:
                df[col] = infer_and_parse_dates(df[col])

        logger.info(f"Mapping applied: renamed {len(rename_map)} columns, parsed {len(date_cols)} date columns")
        return df

    def validate_mapping(self, df: pd.DataFrame) -> Dict[str, DateParseResult]:
        """
        验证字段映射是否有效
        
        Args:
            df: DataFrame（前 100 行）
            
        Returns:
            {field_name: DateParseResult}
        """
        results = {}
        
        for std_name in ["event_time", "event_date", "reg_date"]:
            actual_name = getattr(self.mapping, std_name)
            
            if actual_name and actual_name in df.columns:
                # 尝试解析
                parsed = infer_and_parse_dates(df[actual_name])
                null_count = parsed.isna().sum()
                null_rate = null_count / len(parsed) if len(parsed) > 0 else 1.0
                
                results[std_name] = DateParseResult(
                    success=null_rate < 0.01,
                    null_rate=float(null_rate),
                    failed_count=int(null_count),
                    failed_examples=df[actual_name][parsed.isna()].head(3).tolist(),
                    inferred_format=None  # TODO: 返回实际推断的格式
                )
            else:
                results[std_name] = DateParseResult(
                    success=False,
                    null_rate=1.0,
                    failed_count=len(df),
                    failed_examples=[],
                    inferred_format=None
                )
        
        return results


class SchemaSuggester:
    """
    根据 DataFrame 内容自动建议字段映射
    """

    # 标准字段的可能名称（模糊匹配）
    FIELD_PATTERNS = {
        "user_id": [
            "user_id", "userid", "user", "uid", "openid", "unionid",
            "distinct_id", "device_id", "player_id",
            "account_id", "id"
        ],
        "event_time": [
            "event_time", "time", "create_time", "timestamp", "datetime",
            "eventtime", "event_timestamp", "event_time_sec"
        ],
        "event_date": [
            "event_date", "date", "日期", "eventday", "day", "login_date"
        ],
        "reg_date": [
            "reg_date", "register_date", "注册日期", "registration_date",
            "signup_date", "created_at"
        ],
        "event_name": [
            "event_name", "event", "eventname", "action", "事件名称"
        ],
        "country": [
            "country", "country_code", "nation", "region", "国家", "market"
        ],
        "channel": [
            "channel", "source", "渠道", "渠道名称",
            "media_source", "campaign"
        ],
    }

    @classmethod
    def suggest(cls, df: pd.DataFrame) -> Dict[str, List[str]]:
        """
        根据 DataFrame 列名自动建议映射
        
        Args:
            df: DataFrame
            
        Returns:
            {standard_field: [suggested_columns]}
        """
        suggestions = {}
        columns_lower = {col.lower(): col for col in df.columns}
        
        for std_name, patterns in cls.FIELD_PATTERNS.items():
            matches = []
            
            # 精确匹配（优先）
            for col in df.columns:
                col_lower = col.lower()
                for pattern in patterns:
                    if col_lower == pattern.lower():
                        matches.append(col)
                        break
            
            # 模糊匹配（次优先）
            if not matches:
                for col in df.columns:
                    col_lower = col.lower()
                    for pattern in patterns:
                        if pattern.lower() in col_lower or col_lower in pattern.lower():
                            matches.append(col)
                            break
            
            suggestions[std_name] = matches[:3]  # 最多返回 3 个建议
        
        return suggestions

    @classmethod
    def suggest_with_type_detection(cls, df: pd.DataFrame) -> Dict[str, List[str]]:
        """
        结合列名匹配 + 数据类型检测的建议
        
        Args:
            df: DataFrame
            
        Returns:
            {standard_field: [suggested_columns]}
        """
        # 首先尝试列名匹配
        suggestions = cls.suggest(df)
        
        # 检测日期列
        date_detection = detect_date_columns(df)
        
        for col, info in date_detection.items():
            if info["likely_date"] and info["confidence"] > 0.8:
                # 可能是日期字段
                if not suggestions["event_date"] and col not in suggestions["event_date"]:
                    suggestions["event_date"].insert(0, col)
                elif not suggestions["reg_date"] and col not in suggestions["reg_date"]:
                    suggestions["reg_date"].insert(0, col)
                elif not suggestions["event_time"] and col not in suggestions["event_time"]:
                    suggestions["event_time"].insert(0, col)
        
        return suggestions
