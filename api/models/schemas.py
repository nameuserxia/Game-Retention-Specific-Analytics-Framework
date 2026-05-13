# -*- coding: utf-8 -*-
"""
api/models/schemas.py
Pydantic 请求/响应数据模型
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from api.models.analysis_context import AnalysisContext


# ============================================================
# 字段映射模型
# ============================================================

class FieldMappingRequest(BaseModel):
    """前端传入的字段映射"""
    user_id: str = Field(..., description="用户 ID 列名")
    event_time: str = Field(..., description="精确时间列名")
    event_date: str = Field(..., description="日期列名")
    reg_date: str = Field(..., description="注册日期列名")
    event_name: str = Field(..., description="事件名称列名")
    country: Optional[str] = Field(None, description="国家/地区列名（可选）")
    channel: Optional[str] = Field(None, description="渠道来源列名（可选）")
    json_params: Optional[str] = Field(None, description="JSON 参数列名（可选，如 pri_params）")
    extra_fields: Optional[Dict[str, str]] = Field(default_factory=dict, description="扩展字段映射")

    class Config:
        json_schema_extra = {
            "example": {
                "user_id": "user_id",
                "event_time": "event_time",
                "event_date": "event_date",
                "reg_date": "reg_date",
                "event_name": "event_name",
                "country": "country",
                "channel": "channel",
                "json_params": "event_params",
                "extra_fields": {"event_params": "event_params"}
            }
        }


# ============================================================
# 分析配置模型
# ============================================================

class AnalysisConfig(BaseModel):
    """分析参数配置"""
    reg_start: str = Field(..., description="注册窗口开始日期 (YYYY-MM-DD)")
    reg_end: str = Field(..., description="注册窗口结束日期 (YYYY-MM-DD)")
    retention_days: int = Field(default=1, ge=1, le=365, description="留存天数 (D+N)")
    min_sample_size: int = Field(default=30, ge=1, description="最小样本量阈值")
    cohort_freq: str = Field(default="W", description="Cohort 频率: D=天, W=周, M=月")
    max_days: int = Field(default=30, ge=1, le=365, description="观察天数上限")
    segment_by_country: bool = Field(default=False, description="是否按国家分群")
    segment_by_channel: bool = Field(default=False, description="是否按渠道分群")
    game_genre: str = Field(default="casual", description="游戏类型：casual/competitive/mmo，用于 benchmark 注入")

    class Config:
        json_schema_extra = {
            "example": {
                "reg_start": "2026-03-08",
                "reg_end": "2026-04-08",
                "retention_days": 1,
                "min_sample_size": 30,
                "cohort_freq": "W",
                "max_days": 30,
                "segment_by_country": True,
                "segment_by_channel": True
            }
        }


class ParamMappingConfig(BaseModel):
    """JSON 参数 Key 到业务用途的映射配置"""
    json_params_col: Optional[str] = Field(None, description="原始 JSON 参数列")
    progress_key: Optional[str] = Field(None, description="进度维度 Key，如 level_id")
    result_key: Optional[str] = Field(None, description="结果状态 Key，如 state")
    numeric_keys: List[str] = Field(default_factory=list, description="数值指标 Key，如 step/time")
    segment_keys: List[str] = Field(default_factory=list, description="作为分群维度的 JSON Key")
    relevant_events: List[str] = Field(default_factory=list, description="这些 JSON Key 应该出现的 event_name 列表，用于条件缺失率校验")


class AnalyzeRequest(BaseModel):
    """完整分析请求体，显式固定前后端 JSON 契约。"""
    mapping: FieldMappingRequest
    analysis_config: AnalysisConfig
    param_config: Optional[ParamMappingConfig] = None
    analysis_context: Optional[AnalysisContext] = None
    ai_enabled: bool = Field(default=False, description="Whether to run optional AI report generation")


class JsonKeyInfo(BaseModel):
    """JSON Key 探测结果"""
    key: str
    count: int
    fill_rate: float
    sample_values: List[Any] = Field(default_factory=list)
    suggested_role: str = "segment"


class JsonKeyDiscoveryResponse(BaseModel):
    """JSON 参数列 Key 探测响应"""
    session_id: str
    json_params_col: str
    total_sampled_rows: int
    parsed_rows: int
    parse_error_rows: int
    keys: List[JsonKeyInfo] = Field(default_factory=list)


# ============================================================
# Schema Discovery 响应
# ============================================================

class ColumnInfo(BaseModel):
    """列信息"""
    name: str = Field(..., description="列名")
    dtype: str = Field(..., description="数据类型")
    nullable: bool = Field(..., description="是否可为空")
    sample_values: List[Any] = Field(default_factory=list, description="样本值 (前5条)")


class SchemaDiscoveryResponse(BaseModel):
    """Schema Discovery 响应"""
    session_id: str = Field(..., description="会话 ID (UUID)")
    file_name: str = Field(..., description="原始文件名")
    expires_at: Optional[str] = Field(None, description="Session 过期时间 ISO 字符串")
    total_rows: int = Field(..., description="总行数")
    total_columns: int = Field(..., description="总列数")
    columns: List[str] = Field(..., description="所有列名")
    column_infos: List[ColumnInfo] = Field(default_factory=list, description="列详细信息")
    preview: List[Dict[str, Any]] = Field(default_factory=list, description="前5行预览数据")
    suggestions: Dict[str, List[str]] = Field(default_factory=dict, description="字段映射建议")
    stats: Dict[str, Any] = Field(default_factory=dict, description="用于前端默认配置的轻量统计信息")
    file_size_mb: float = Field(..., description="文件大小 (MB)")

    class Config:
        json_schema_extra = {
            "example": {
                "session_id": "550e8400-e29b-41d4-a716-446655440000",
                "file_name": "example_events.csv",
                "total_rows": 15000,
                "total_columns": 12,
                "columns": ["user_id", "event_time", "event_date", "reg_date", "event_name"],
                "column_infos": [
                    {"name": "user_id", "dtype": "object", "nullable": False, "sample_values": ["user_001", "user_002"]}
                ],
                "preview": [{"user_id": "user_001", "event_date": "2026-01-01"}],
                "suggestions": {
                    "user_id": ["user_id"],
                    "event_date": ["event_date"],
                    "event_name": ["event_name"]
                },
                "file_size_mb": 45.2
            }
        }


# ============================================================
# 预校验响应
# ============================================================

class DateParseResult(BaseModel):
    """日期字段解析结果"""
    success: bool = Field(..., description="解析是否成功")
    null_rate: float = Field(..., description="解析失败率")
    failed_count: int = Field(..., description="失败行数")
    failed_examples: List[Any] = Field(default_factory=list, description="失败示例")
    inferred_format: Optional[str] = Field(None, description="推断的日期格式")


class ValidationResponse(BaseModel):
    """预校验响应"""
    session_id: str = Field(..., description="会话 ID")
    success: bool = Field(..., description="校验是否成功")
    can_proceed: bool = Field(..., description="是否可以继续分析")
    parse_results: Dict[str, DateParseResult] = Field(default_factory=dict, description="日期解析结果")
    errors: List[str] = Field(default_factory=list, description="错误列表")
    warnings: List[str] = Field(default_factory=list, description="警告列表")
    stats: Dict[str, Any] = Field(default_factory=dict, description="统计信息")

    class Config:
        json_schema_extra = {
            "example": {
                "session_id": "550e8400-e29b-41d4-a716-446655440000",
                "success": True,
                "can_proceed": True,
                "parse_results": {
                    "event_date": {
                        "success": True,
                        "null_rate": 0.0,
                        "failed_count": 0,
                        "failed_examples": [],
                        "inferred_format": "%d/%m/%Y"
                    }
                },
                "errors": [],
                "warnings": ["数据最新日期距今不足7天，存在生存者偏倚风险"],
                "stats": {"total_users": 1426, "data_max_date": "2026-04-08"}
            }
        }


# ============================================================
# 分析结果响应
# ============================================================

class RetentionResult(BaseModel):
    """留存率计算结果"""
    segment: str = Field(..., description="分群名称")
    n_total: int = Field(..., description="总用户数")
    n_retained: int = Field(..., description="留存用户数")
    retention_rate: float = Field(..., description="留存率 (%)")
    note: str = Field(default="", description="备注")


class CohortCell(BaseModel):
    """Cohort 矩阵单元格"""
    cohort: str = Field(..., description="Cohort 名称")
    day: str = Field(..., description="观察天数 (D+N)")
    retention_rate: float = Field(..., description="留存率 (%)")
    n_users: int = Field(..., description="该日 Cohort 用户数")


class AnalysisResponse(BaseModel):
    """分析结果响应"""
    session_id: str = Field(..., description="会话 ID")
    success: bool = Field(..., description="分析是否成功")
    message: str = Field(default="", description="消息")
    summary: Dict[str, Any] = Field(default_factory=dict, description="汇总信息")
    retention_result: List[RetentionResult] = Field(default_factory=list, description="留存率结果")
    cohort_matrix: List[List[Any]] = Field(default_factory=list, description="Cohort 矩阵 (JSON)")
    cohort_headers: List[str] = Field(default_factory=list, description="Cohort 矩阵表头")
    churn_users_count: int = Field(default=0, description="流失用户数")
    retained_users_count: int = Field(default=0, description="留存用户数")
    country_retention: List[RetentionResult] = Field(default_factory=list, description="按国家分群结果")
    channel_retention: List[RetentionResult] = Field(default_factory=list, description="按渠道分群结果")
    top_paths: List[Dict[str, Any]] = Field(default_factory=list, description="Top 行为路径")
    report_markdown: str = Field(default="", description="报告 Markdown")
    sanity_check_report: Dict[str, Any] = Field(default_factory=dict, description="数据质量校验报告")
    diagnostics: Dict[str, Any] = Field(default_factory=dict, description="四阶段自动化诊断结果")
    virtual_fields: List[str] = Field(default_factory=list, description="JSON 展平后生成的虚拟字段")
    report_id: Optional[str] = Field(default=None, description="Persistent Markdown report ID")
    report_title: Optional[str] = Field(default=None, description="AI or fallback generated report title")
    report_path: Optional[str] = Field(default=None, description="Persistent Markdown report path")
    llm_used: bool = Field(default=False, description="Whether an LLM produced the structured report")
    llm_fallback_reason: Optional[str] = Field(default=None, description="Reason for AI fallback, if any")

    class Config:
        json_schema_extra = {
            "example": {
                "session_id": "550e8400-e29b-41d4-a716-446655440000",
                "success": True,
                "message": "分析完成",
                "summary": {
                    "game_name": "Example Puzzle Game",
                    "reg_start": "2026-01-01",
                    "reg_end": "2026-01-31",
                    "retention_days": 1,
                    "n_total": 1000,
                    "n_retained": 280,
                    "retention_rate": 28.0
                },
                "churn_users_count": 720,
                "retained_users_count": 280
            }
        }


# ============================================================
# Session 管理响应
# ============================================================

class SessionInfo(BaseModel):
    """Session 信息"""
    session_id: str = Field(..., description="会话 ID")
    file_name: Optional[str] = Field(None, description="文件名")
    file_size_mb: Optional[float] = Field(None, description="文件大小 (MB)")
    total_rows: Optional[int] = Field(None, description="总行数")
    created_at: datetime = Field(..., description="创建时间")
    expires_at: datetime = Field(..., description="过期时间")
    status: str = Field(..., description="状态: pending, ready, analyzing, done, expired")


class CleanupResponse(BaseModel):
    """清理响应"""
    success: bool = Field(..., description="清理是否成功")
    session_id: str = Field(..., description="被清理的会话 ID")
    files_deleted: List[str] = Field(default_factory=list, description="已删除的文件列表")


class HealthResponse(BaseModel):
    """健康检查响应"""
    status: str = Field(default="ok")
    version: str = Field(default="1.0.0")
    temp_files_count: int = Field(default=0, description="临时文件数量")
    cleaned_files_count: int = Field(default=0, description="本次清理的文件数量")
