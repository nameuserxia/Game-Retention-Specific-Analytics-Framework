# -*- coding: utf-8 -*-
"""
core/config_loader.py
配置化字段映射加载器

功能：
  - 从 YAML 配置文件读取游戏特定的字段名映射
  - 将 CSV 的原始字段名标准化为框架内部字段名
  - 支持多游戏配置切换，无需修改分析代码

使用方式：
    from core.config_loader import load_config, apply_field_mapping
    
    cfg, game_cfg = load_config('config/game_a_config.yaml')
    df_std = apply_field_mapping(df_raw, game_cfg['field_mapping'])
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False
    import json  # fallback to JSON

from .analytics import FieldConfig


# ============================================================
# 配置加载
# ============================================================

def load_config(config_path: str) -> Tuple[FieldConfig, Dict[str, Any]]:
    """
    从 YAML（或 JSON）配置文件加载游戏字段配置。
    
    Parameters
    ----------
    config_path : str
        配置文件路径，支持 .yaml / .yml / .json
    
    Returns
    -------
    (FieldConfig, raw_config_dict)
        FieldConfig : 解析后的字段配置对象，可直接传给 analytics 函数
        raw_config_dict : 原始配置字典，包含 events、date_format 等扩展配置
    
    Raises
    ------
    FileNotFoundError : 配置文件不存在
    KeyError : 必要配置项缺失
    ValueError : 配置格式不合法
    
    Example
    -------
    cfg, game_cfg = load_config('config/example_game_config.yaml')
    df_std = apply_field_mapping(df_raw, game_cfg['field_mapping'])
    churn, retained = get_churn_users(df_std, cfg, ...)
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"配置文件不存在：{config_path}")

    # 读取文件
    with open(path, 'r', encoding='utf-8') as f:
        if path.suffix in ('.yaml', '.yml'):
            if not HAS_YAML:
                raise ImportError(
                    "解析 YAML 配置需要安装 PyYAML：pip install pyyaml"
                )
            raw_config = yaml.safe_load(f)
        elif path.suffix == '.json':
            raw_config = json.load(f)
        else:
            raise ValueError(f"不支持的配置文件格式：{path.suffix}，请使用 .yaml 或 .json")

    # 必要项检查
    if 'field_mapping' not in raw_config:
        raise KeyError(f"配置文件 {config_path} 缺少 'field_mapping' 节点")

    fm = raw_config['field_mapping']

    # 构建 FieldConfig（字段名映射到框架标准名）
    # field_mapping 中的键是框架内部名，值是 CSV 中的实际列名
    cfg = FieldConfig(
        user_id=fm.get('user_id', 'user_id'),
        event_time=fm.get('event_time', 'event_time'),
        event_date=fm.get('event_date', 'event_date'),
        reg_date=fm.get('reg_date', 'reg_date'),
        event_name=fm.get('event_name', 'event_name'),
        country=fm.get('country', 'country'),
        channel=fm.get('channel', 'channel'),
        extra_fields=fm.get('extra_fields', {}),
    )

    return cfg, raw_config


def apply_field_mapping(
    df: pd.DataFrame,
    field_mapping: Dict[str, str],
    inplace: bool = False,
) -> pd.DataFrame:
    """
    将 CSV 原始字段名重命名为框架标准字段名。
    
    field_mapping 格式：
        {框架内部标准名: CSV实际列名}
    
    例如通用事件日志的映射：
        {
            "user_id": "user_id",
            "event_time": "event_time",
            "event_date": "event_date",
            "reg_date": "reg_date",
            "channel": "channel"
        }
    
    重命名后，框架函数可以统一用 cfg.user_id / cfg.channel 等标准名访问字段。
    
    Parameters
    ----------
    df : pd.DataFrame
        原始 DataFrame
    field_mapping : dict
        {标准名: 实际列名} 的映射字典
    inplace : bool
        是否原地修改（默认 False，返回新 DataFrame）
    
    Returns
    -------
    pd.DataFrame : 字段名已标准化的 DataFrame
    
    Raises
    ------
    KeyError : 映射中指定的实际列名在 df 中不存在时发出警告（不报错，保持宽容）
    """
    # 反转映射：实际列名 → 标准名
    rename_map = {}
    missing_cols = []
    for std_name, actual_name in field_mapping.items():
        if std_name == 'extra_fields':
            continue
        if actual_name in df.columns:
            if actual_name != std_name:  # 只有不同名才需要重命名
                rename_map[actual_name] = std_name
        else:
            missing_cols.append(f"{std_name}（原列名：{actual_name}）")

    if missing_cols:
        import warnings
        warnings.warn(
            f"以下字段在 DataFrame 中不存在，跳过重命名：{missing_cols}\n"
            f"当前列名：{list(df.columns)}",
            UserWarning,
            stacklevel=2,
        )

    if inplace:
        df.rename(columns=rename_map, inplace=True)
        return df
    else:
        return df.rename(columns=rename_map)


def parse_dates_from_config(
    df: pd.DataFrame,
    config: Dict[str, Any],
    cfg: FieldConfig,
) -> pd.DataFrame:
    """
    根据配置文件中的日期格式定义，批量解析日期字段。
    
    配置文件中需有 'date_formats' 节点，格式为：
        date_formats:
          event_date: '%d/%m/%Y'
          event_time: '%d/%m/%Y %H:%M:%S'
          reg_date: '%d/%m/%Y'
    
    Parameters
    ----------
    df : pd.DataFrame
        已完成字段重命名的 DataFrame
    config : dict
        load_config 返回的原始配置字典
    cfg : FieldConfig
        字段配置
    
    Returns
    -------
    pd.DataFrame : 日期字段已解析为 datetime 类型
    """
    date_formats: Dict[str, str] = config.get('date_formats', {})
    df = df.copy()

    # 遍历配置中的日期格式
    date_field_map = {
        'event_date': cfg.event_date,
        'event_time': cfg.event_time,
        'reg_date': cfg.reg_date,
    }

    for field_key, col_name in date_field_map.items():
        if col_name not in df.columns:
            continue
        fmt = date_formats.get(field_key)
        if fmt:
            df[col_name] = pd.to_datetime(df[col_name], format=fmt, errors='coerce')
        else:
            # 没有指定格式，尝试自动推断
            df[col_name] = pd.to_datetime(df[col_name], dayfirst=True, errors='coerce')

        # 验证
        null_rate = df[col_name].isna().mean()
        if null_rate > 0.01:
            import warnings
            warnings.warn(
                f"字段 '{col_name}' 使用格式 '{fmt}' 解析后失败率 {null_rate:.1%}，"
                f"请检查 date_formats 配置。",
                UserWarning,
                stacklevel=2,
            )

    return df
