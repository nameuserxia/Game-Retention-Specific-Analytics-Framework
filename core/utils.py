# -*- coding: utf-8 -*-
"""Shared analytics configuration objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict


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
