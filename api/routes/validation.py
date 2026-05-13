# -*- coding: utf-8 -*-
"""
api/routes/validation.py
字段映射预校验 API
"""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException

import pandas as pd

import sys
from pathlib import Path as P

# 添加项目根目录到路径
project_root = P(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from api.models.schemas import (
    FieldMappingRequest,
    JsonKeyDiscoveryResponse,
    ValidationResponse,
    DateParseResult,
)
from api.utils.session_manager import SessionManager
from api.adapters.config_adapter import ConfigAdapter, FieldMapping
from api.adapters.param_converter import ParamConverter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["Validation"])

# 预校验的样本行数
PREVIEW_SAMPLE_SIZE = 100


@router.get("/detect-json-keys", response_model=JsonKeyDiscoveryResponse)
async def detect_json_keys(
    session_id: str,
    json_params_col: str,
    sample_size: int = 5000,
) -> JsonKeyDiscoveryResponse:
    """实时解析 JSON 参数列，提取唯一 Key 返给前端。"""
    if not SessionManager.is_valid(session_id):
        raise HTTPException(status_code=410, detail="Session 已过期或不存在，请重新上传文件")

    parquet_path = SessionManager.get_parquet_path(session_id)
    if not parquet_path.exists():
        raise HTTPException(status_code=404, detail="Parquet 文件不存在")

    try:
        df_sample = pd.read_parquet(parquet_path).head(sample_size)
        discovery = ParamConverter.discover_keys(df_sample, json_params_col, sample_size=sample_size)
        return JsonKeyDiscoveryResponse(
            session_id=session_id,
            json_params_col=json_params_col,
            **discovery,
        )
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"JSON key detection failed: {e}")
        raise HTTPException(status_code=500, detail=f"JSON 参数解析失败：{str(e)}")


@router.post("/validate-mapping", response_model=ValidationResponse)
async def validate_mapping(
    session_id: str,
    mapping: FieldMappingRequest,
    force_proceed: bool = False,
) -> ValidationResponse:
    """
    字段映射预校验（前 100 行）
    
    功能：
    1. 读取 session 对应的 Parquet 文件（前 100 行）
    2. 应用字段映射
    3. 测试日期格式解析
    4. 返回校验结果
    
    参数：
    - session_id: 上传时返回的会话 ID
    - mapping: 字段映射配置
    - force_proceed: 是否强制继续（即使有非致命警告）
    
    返回：
    - can_proceed: 是否可以继续分析
    - parse_results: 各日期字段的解析结果
    - errors: 错误列表（如解析失败率 > 1%）
    - warnings: 警告列表（如样本量不足）
    """
    # ── 1. 检查 Session ──────────────────────────────────────
    
    if not SessionManager.is_valid(session_id):
        raise HTTPException(status_code=410, detail="Session 已过期或不存在，请重新上传文件")
    
    parquet_path = SessionManager.get_parquet_path(session_id)
    
    if not parquet_path.exists():
        raise HTTPException(status_code=404, detail="Parquet 文件不存在")
    
    try:
        # ── 2. 读取样本数据（前 100 行）───────────────────────
        
        df_sample = pd.read_parquet(parquet_path).head(PREVIEW_SAMPLE_SIZE)
        logger.info(f"Read {len(df_sample)} rows for validation")
        
        # ── 3. 创建 ConfigAdapter ─────────────────────────────
        
        field_mapping = FieldMapping.from_request(mapping)
        adapter = ConfigAdapter(field_mapping)
        
        # ── 4. 应用映射 ─────────────────────────────────────
        
        df_mapped = adapter.apply_mapping(df_sample)
        
        # ── 5. 日期解析测试 ──────────────────────────────────
        
        parse_results = {}
        errors = []
        warnings = []
        
        for std_name in ["event_date", "reg_date", "event_time"]:
            if std_name not in df_mapped.columns:
                # 字段不存在
                parse_results[std_name] = DateParseResult(
                    success=False,
                    null_rate=1.0,
                    failed_count=PREVIEW_SAMPLE_SIZE,
                    failed_examples=[],
                    inferred_format=None
                )
                errors.append(f"映射后的 DataFrame 中缺少字段：{std_name}")
                continue
            
            # 检查是否已是 datetime 类型
            if pd.api.types.is_datetime64_any_dtype(df_mapped[std_name]):
                parse_results[std_name] = DateParseResult(
                    success=True,
                    null_rate=0.0,
                    failed_count=0,
                    failed_examples=[],
                    inferred_format="datetime"
                )
                continue
            
            # 尝试解析
            null_count = df_mapped[std_name].isna().sum()
            null_rate = null_count / len(df_mapped) if len(df_mapped) > 0 else 1.0
            
            # 获取失败示例
            failed_mask = df_mapped[std_name].isna()
            failed_examples = []
            if failed_mask.any():
                # 获取原始列名用于展示失败值
                actual_col = getattr(mapping, std_name)
                if actual_col and actual_col in df_sample.columns:
                    failed_examples = df_sample[actual_col][failed_mask].head(3).tolist()
                    failed_examples = [str(v) for v in failed_examples]
            
            parse_results[std_name] = DateParseResult(
                success=null_rate < 0.01,
                null_rate=float(null_rate),
                failed_count=int(null_count),
                failed_examples=failed_examples,
                inferred_format=None  # TODO: 从 date_inference 获取
            )
            
            if null_rate >= 0.01:
                source_col = getattr(mapping, std_name, std_name)
                errors.append(
                    f"字段 '{std_name}'（原始名：{source_col}）"
                    f"解析失败率 {null_rate:.1%}，共 {null_count}/{PREVIEW_SAMPLE_SIZE} 行失败。"
                    f"请检查日期格式是否正确。"
                )
        
        # ── 6. 基本统计检查 ──────────────────────────────────
        
        stats = {}
        
        # 用户数检查
        if "user_id" in df_mapped.columns:
            n_users = df_mapped["user_id"].nunique()
            stats["sample_users"] = n_users
            
            if n_users < 10:
                warnings.append(f"样本用户数过少（{n_users}），可能影响统计准确性")
        
        # 日期范围检查
        if "event_date" in df_mapped.columns and pd.api.types.is_datetime64_any_dtype(df_mapped["event_date"]):
            valid_dates = df_mapped["event_date"].dropna()
            if len(valid_dates) > 0:
                stats["min_date"] = str(valid_dates.min().date())
                stats["max_date"] = str(valid_dates.max().date())
        
        # ── 7. 判断是否可继续 ───────────────────────────────
        
        can_proceed = (
            all(r.success for r in parse_results.values())
            or force_proceed
        )
        
        # 即使有日期解析错误，如果用户选择 force_proceed 也允许继续
        if force_proceed:
            warnings.append("用户选择强制继续，即使存在非致命错误")
        
        return ValidationResponse(
            session_id=session_id,
            success=True,
            can_proceed=can_proceed,
            parse_results={k: r.model_dump() for k, r in parse_results.items()},
            errors=errors,
            warnings=warnings,
            stats=stats
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        raise HTTPException(status_code=500, detail=f"预校验失败：{str(e)}")


@router.post("/test-date-format")
async def test_date_format(
    session_id: str,
    column_name: str,
    date_format: str = None,
    dayfirst: bool = True,
) -> dict:
    """
    测试特定列的日期格式解析
    
    参数：
    - session_id: 会话 ID
    - column_name: 要测试的列名
    - date_format: 指定格式（如 "%d/%m/%Y"），None 则自动推断
    - dayfirst: 是否优先欧洲格式
    
    返回：
    - success: 解析是否成功
    - null_rate: 失败率
    - samples: 成功/失败的示例
    """
    if not SessionManager.is_valid(session_id):
        raise HTTPException(status_code=410, detail="Session 已过期或不存在")
    
    parquet_path = SessionManager.get_parquet_path(session_id)
    
    try:
        df_sample = pd.read_parquet(parquet_path).head(100)
        
        if column_name not in df_sample.columns:
            raise HTTPException(status_code=404, detail=f"列 '{column_name}' 不存在")
        
        # 提取样本值
        sample = df_sample[column_name].dropna().head(50)
        
        if len(sample) == 0:
            return {
                "success": False,
                "null_rate": 1.0,
                "message": "列中无有效值",
                "samples": []
            }
        
        # 尝试解析
        if date_format:
            try:
                parsed = pd.to_datetime(sample, format=date_format, errors="coerce")
            except Exception as e:
                return {
                    "success": False,
                    "null_rate": 1.0,
                    "message": f"格式 '{date_format}' 解析失败：{str(e)}",
                    "samples": []
                }
        else:
            # 自动推断
            from api.utils.date_inference import DateInference
            parsed, inferred_fmt, confidence = DateInference.parse_with_inference(
                sample, dayfirst=dayfirst, errors="coerce"
            )
        
        null_rate = parsed.isna().mean()
        
        # 获取成功/失败的示例
        success_mask = ~parsed.isna()
        failed_mask = parsed.isna()
        
        samples = {
            "success": [
                {"original": str(sample.iloc[i]), "parsed": str(parsed.iloc[i])}
                for i in range(len(sample))
                if success_mask.iloc[i]
            ][:5],
            "failed": [
                {"original": str(sample.iloc[i])}
                for i in range(len(sample))
                if failed_mask.iloc[i]
            ][:5]
        }
        
        return {
            "success": null_rate < 0.01,
            "null_rate": float(null_rate),
            "failed_count": int(parsed.isna().sum()),
            "total_count": len(parsed),
            "date_format": date_format,
            "samples": samples
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Date format test failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
