# -*- coding: utf-8 -*-
"""
api/routes/upload.py
文件上传 + Schema 探测 API
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, File, HTTPException, UploadFile

import pandas as pd

from api.models.schemas import (
    ColumnInfo,
    SchemaDiscoveryResponse,
    CleanupResponse,
)
from api.utils.session_manager import SessionManager
from api.utils.file_parser import FileParser
from api.adapters.config_adapter import SchemaSuggester

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["Upload"])

# 允许的文件类型
ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls", ".parquet"}
MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024  # 2GB


def _infer_date_config_stats(parquet_path: Path, df_preview: pd.DataFrame, suggestions: Dict[str, list]) -> Dict[str, Any]:
    """Infer date ranges for frontend defaults from suggested date columns."""
    candidate_cols = []
    for field in ["reg_date", "event_date", "event_time"]:
        candidate_cols.extend(suggestions.get(field, []) or [])
    candidate_cols = [col for col in dict.fromkeys(candidate_cols) if col in df_preview.columns]
    if not candidate_cols:
        return {"date_ranges": {}}

    try:
        df_dates = pd.read_parquet(parquet_path, columns=candidate_cols)
    except Exception:
        df_dates = df_preview[candidate_cols]

    date_ranges: Dict[str, Dict[str, str]] = {}
    for col in candidate_cols:
        parsed = pd.to_datetime(df_dates[col], errors="coerce", dayfirst=True)
        valid = parsed.dropna()
        if valid.empty:
            continue
        date_ranges[col] = {
            "min": str(valid.min().date()),
            "max": str(valid.max().date()),
        }

    stats: Dict[str, Any] = {"date_ranges": date_ranges}
    reg_col = next((col for col in suggestions.get("reg_date", []) if col in date_ranges), None)
    event_col = next((col for col in suggestions.get("event_date", []) if col in date_ranges), None)
    default_range = date_ranges.get(reg_col or event_col or "")
    if default_range:
        stats["suggested_reg_start"] = default_range["min"]
        stats["suggested_reg_end"] = default_range["max"]
    return stats


@router.post("/upload", response_model=SchemaDiscoveryResponse)
async def upload_file(
    file: UploadFile = File(..., description="上传 CSV/Excel/Parquet 文件"),
) -> SchemaDiscoveryResponse:
    """
    文件上传 + Schema Discovery
    
    流程：
    1. 保存上传文件到 temp/ 目录
    2. 生成 UUID session_id
    3. 转换为 Parquet 格式（便于后续快速读取）
    4. 返回列名、类型、样本预览、映射建议
    
    限制：
    - 最大文件大小：2GB
    - 临时文件有效期：1 小时
    """
    # ── 1. 验证文件 ─────────────────────────────────────────
    
    if not file.filename:
        raise HTTPException(status_code=400, detail="文件名为空")
    
    file_path = Path(file.filename)
    suffix = file_path.suffix.lower()
    
    if suffix not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"不支持的文件格式：{suffix}。支持的格式：{', '.join(ALLOWED_EXTENSIONS)}"
        )
    
    # ── 2. 创建 Session ──────────────────────────────────────
    
    session_id = SessionManager.create_session(file_name=file.filename)
    
    try:
        # ── 3. 保存原始文件 ───────────────────────────────────
        
        upload_path = SessionManager.get_file_path(session_id, suffix.lstrip("."))
        
        # 读取文件内容并保存
        content = await file.read()
        file_size = len(content)
        
        # 检查文件大小
        if file_size > MAX_FILE_SIZE:
            SessionManager.cleanup_session(session_id)
            raise HTTPException(
                status_code=413,
                detail=f"文件过大：{file_size/1024/1024/1024:.1f}GB。最大支持 2GB"
            )
        
        # 写入磁盘
        with open(upload_path, "wb") as f:
            f.write(content)
        
        logger.info(f"File saved: {upload_path} ({file_size/1024/1024:.1f}MB)")
        
        # ── 4. 读取预览 + 获取 Schema ─────────────────────────
        
        # 预览读取（只读 head(100)）
        df_preview = FileParser.read_preview_pandas(upload_path, nrows=100)
        
        # 统计信息
        file_info = FileParser.get_file_info(upload_path)
        
        # 转换为 Parquet（异步进行，但等待完成以返回结果）
        parquet_path, engine = FileParser.convert_to_parquet(upload_path, session_id)
        logger.info(f"Converted to Parquet: {parquet_path} (engine: {engine})")
        
        # ── 5. 构建响应 ───────────────────────────────────────
        
        # 列信息
        column_infos = []
        for col in df_preview.columns:
            dtype_str = str(df_preview[col].dtype)
            sample_values = df_preview[col].dropna().head(5).tolist()
            column_infos.append(ColumnInfo(
                name=col,
                dtype=dtype_str,
                nullable=df_preview[col].isna().any(),
                sample_values=[str(v) for v in sample_values]
            ))
        
        # 预览数据（前 5 行）
        preview = df_preview.head(5).fillna("").to_dict(orient="records")
        preview = [{k: str(v) for k, v in row.items()} for row in preview]
        
        # 字段映射建议
        suggestions = SchemaSuggester.suggest_with_type_detection(df_preview)
        stats = _infer_date_config_stats(parquet_path, df_preview, suggestions)
        
        # ── 6. 更新 Session 状态 ───────────────────────────────
        
        SessionManager.update_session(
            session_id,
            status="ready",
            total_rows=file_info["rows"],
            total_columns=file_info["columns"],
            file_size_mb=file_info["size_mb"]
        )
        
        return SchemaDiscoveryResponse(
            session_id=session_id,
            file_name=file.filename,
            expires_at=SessionManager.get_session(session_id)["expires_at"].isoformat(),
            total_rows=file_info["rows"],
            total_columns=file_info["columns"],
            columns=list(df_preview.columns),
            column_infos=column_infos,
            preview=preview,
            suggestions=suggestions,
            stats=stats,
            file_size_mb=file_info["size_mb"]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        SessionManager.cleanup_session(session_id)
        raise HTTPException(status_code=500, detail=f"文件处理失败：{str(e)}")


@router.delete("/session/{session_id}", response_model=CleanupResponse)
async def destroy_session(session_id: str) -> CleanupResponse:
    """
    销毁 Session（删除临时文件）
    
    前端在分析完成后可调用此接口主动清理数据
    """
    session = SessionManager.get_session(session_id)
    
    if not session:
        raise HTTPException(status_code=404, detail=f"Session 不存在：{session_id}")
    
    deleted = SessionManager.cleanup_session(session_id)
    
    files_deleted = []
    if deleted["csv"]:
        files_deleted.append(f"upload_{session_id}.csv")
    if deleted["parquet"]:
        files_deleted.append(f"upload_{session_id}.parquet")
    
    return CleanupResponse(
        success=True,
        session_id=session_id,
        files_deleted=files_deleted
    )


@router.get("/session/{session_id}")
async def get_session_info(session_id: str) -> dict:
    """
    获取 Session 状态
    
    返回当前 session 的状态、文件信息、剩余有效期等
    """
    session = SessionManager.get_session(session_id)
    
    if not session:
        raise HTTPException(status_code=404, detail=f"Session 不存在：{session_id}")
    
    # 检查文件是否存在
    parquet_path = SessionManager.get_parquet_path(session_id)
    
    if not parquet_path.exists():
        SessionManager.update_session(session_id, status="expired")
        raise HTTPException(status_code=410, detail="Session 已过期，文件已被清理")
    
    # 计算剩余有效期
    from datetime import datetime
    expires_at = session["expires_at"]
    remaining = (expires_at - datetime.now()).total_seconds() / 60
    
    return {
        "session_id": session_id,
        "file_name": session["file_name"],
        "status": session["status"],
        "total_rows": session["total_rows"],
        "total_columns": session["total_columns"],
        "file_size_mb": session["file_size_mb"],
        "created_at": session["created_at"].isoformat(),
        "expires_at": expires_at.isoformat(),
        "remaining_minutes": round(remaining, 1),
        "is_valid": SessionManager.is_valid(session_id)
    }


@router.get("/sessions")
async def list_sessions() -> dict:
    """
    列出所有活跃的 Session
    """
    sessions = SessionManager.list_active_sessions()
    
    return {
        "count": len(sessions),
        "sessions": sessions,
        "total_size_mb": SessionManager.get_temp_dir_size(),
        "total_files": SessionManager.get_temp_files_count()
    }
