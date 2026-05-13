# -*- coding: utf-8 -*-
"""
api/routes/analysis.py
执行分析 API
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, HTTPException

import pandas as pd

import sys
from pathlib import Path as P

# 添加项目根目录到路径
project_root = P(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from api.models.schemas import (
    AnalyzeRequest,
    FieldMappingRequest,
    AnalysisConfig,
    AnalysisResponse,
    ParamMappingConfig,
    RetentionResult,
)
from api.utils.session_manager import SessionManager
from api.adapters.config_adapter import ConfigAdapter, FieldMapping
from api.adapters.param_converter import ParamConverter
from api.analyzers.specialized import RetentionDiagnosticAnalyzer
from api.agent.retention_agent import RetentionDiagnosisAgent
from api.pipelines.retention_ml import RetentionMLPipeline
from core.analytics import (
    FieldConfig,
    SanityCheckError,
    sanity_check,
    calculate_retention,
    build_cohort_matrix,
    get_churn_users,
    build_event_sequences,
    get_top_paths,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["Analysis"])


@router.post("/analyze", response_model=AnalysisResponse)
async def run_analysis(
    session_id: str,
    request: AnalyzeRequest,
    force_proceed: bool = False,
) -> AnalysisResponse:
    """
    执行完整留存分析
    
    流程：
    1. 检查 Session 有效性
    2. 读取全量数据（从 Parquet）
    3. 应用字段映射
    4. 执行 sanity_check（数据质量校验）
    5. 计算留存率（全体 + 分群）
    6. 构建 Cohort 矩阵
    7. 分析行为路径
    8. 生成报告
    
    参数：
    - session_id: 会话 ID
    - mapping: 字段映射配置
    - analysis_config: 分析参数
    - force_proceed: 是否强制继续（即使有非致命警告）
    """
    # ── 1. 检查 Session ──────────────────────────────────────
    
    mapping = request.mapping
    analysis_config = request.analysis_config
    param_config = request.param_config

    if not SessionManager.is_valid(session_id):
        raise HTTPException(status_code=410, detail="Session 已过期或不存在，请重新上传文件")
    
    SessionManager.update_session(session_id, status="analyzing")
    
    parquet_path = SessionManager.get_parquet_path(session_id)
    
    if not parquet_path.exists():
        raise HTTPException(status_code=404, detail="Parquet 文件不存在")
    
    try:
        # ── 2. 读取全量数据 ──────────────────────────────────
        
        logger.info(f"Reading full data from {parquet_path}")
        df = pd.read_parquet(parquet_path)
        logger.info(f"Loaded {len(df):,} rows")
        
        # ── 3. 应用字段映射 ──────────────────────────────────
        
        field_mapping = FieldMapping.from_request(mapping)
        adapter = ConfigAdapter(field_mapping)
        df_mapped = adapter.apply_mapping(df)
        virtual_fields = []

        effective_param_config = param_config
        if effective_param_config is None and mapping.json_params:
            effective_param_config = ParamMappingConfig(json_params_col=mapping.json_params)
        if effective_param_config and effective_param_config.json_params_col:
            json_col = "json_params" if "json_params" in df_mapped.columns else effective_param_config.json_params_col
            effective_param_config.json_params_col = json_col
            converter = ParamConverter(effective_param_config)
            df_mapped = converter.transform(df_mapped)
            virtual_fields = converter.virtual_fields

        ml_diagnostics = {}
        if effective_param_config:
            try:
                ml_result = RetentionMLPipeline(
                    mapping=mapping.model_dump(),
                    field_config=adapter.field_config,
                    param_config=effective_param_config,
                ).transform(df_mapped)
                df_mapped = ml_result["df"]
                virtual_fields = list(dict.fromkeys([*virtual_fields, *ml_result.get("virtual_fields", [])]))
                labels = ml_result.get("labels")
                ml_diagnostics = {
                    "feature_importance": ml_result.get("feature_importance", {}),
                    "feature_matrix_shape": [
                        int(ml_result["feature_matrix"].shape[0]),
                        int(ml_result["feature_matrix"].shape[1]),
                    ],
                    "label_positive_rates": {
                        col: round(float(labels[col].mean()), 4)
                        for col in labels.columns
                    } if labels is not None else {},
                }
            except Exception as e:
                logger.warning(f"ML pipeline failed: {e}")
                ml_diagnostics = {
                    "error": str(e),
                    "feature_importance": {
                        "method": "failed",
                        "top_features": [],
                        "business_translation": "ML 特征诊断运行失败，请检查 JSON Key 映射与数据类型。",
                    },
                }
        
        # ── 4. 解析日期配置 ──────────────────────────────────
        
        reg_start = pd.Timestamp(analysis_config.reg_start)
        reg_end = pd.Timestamp(analysis_config.reg_end)
        
        # ── 5. 执行 sanity_check ──────────────────────────────
        
        field_config = adapter.field_config
        
        sanity_report = None
        sanity_warnings = []
        
        try:
            sanity_report = sanity_check(
                df_mapped,
                field_config,
                min_sample_size=analysis_config.min_sample_size,
                raise_on_failure=not force_proceed,
            )
            sanity_warnings = sanity_report.get("warnings", [])
            
        except SanityCheckError as e:
            raise HTTPException(
                status_code=422,
                detail={
                    "message": "数据质量校验失败",
                    "error": str(e),
                    "recommendations": [
                        "检查日期格式是否与实际数据一致",
                        "确认 field_mapping 中的字段名与 CSV 列名匹配",
                        "扩大数据范围，确保用户数 >= 30"
                    ]
                }
            )
        
        # ── 6. 计算留存率 ────────────────────────────────────
        
        # 全体留存率
        retention_result = calculate_retention(
            df_mapped,
            field_config,
            reg_start,
            reg_end,
            retention_days=analysis_config.retention_days,
        )
        
        # 提取汇总数据
        if len(retention_result) > 0:
            total_row = retention_result.iloc[0]
            n_total = int(total_row["n_total"])
            n_retained = int(total_row["n_retained"])
            n_churn = n_total - n_retained
            retention_rate = float(total_row["retention_rate"])
        else:
            n_total = n_retained = n_churn = 0
            retention_rate = 0.0
        
        # ── 7. 分群分析 ──────────────────────────────────────
        
        country_retention = []
        channel_retention = []
        
        if analysis_config.segment_by_country and mapping.country:
            try:
                country_df = calculate_retention(
                    df_mapped,
                    field_config,
                    reg_start,
                    reg_end,
                    retention_days=analysis_config.retention_days,
                    segment_col=field_config.country,
                )
                country_retention = [
                    RetentionResult(
                        segment=row["segment"],
                        n_total=row["n_total"],
                        n_retained=row["n_retained"],
                        retention_rate=row["retention_rate"],
                        note=row.get("note", "")
                    ).model_dump()
                    for _, row in country_df.iterrows()
                ]
            except Exception as e:
                logger.warning(f"Country segmentation failed: {e}")
        
        if analysis_config.segment_by_channel and mapping.channel:
            try:
                channel_df = calculate_retention(
                    df_mapped,
                    field_config,
                    reg_start,
                    reg_end,
                    retention_days=analysis_config.retention_days,
                    segment_col=field_config.channel,
                )
                channel_retention = [
                    RetentionResult(
                        segment=row["segment"],
                        n_total=row["n_total"],
                        n_retained=row["n_retained"],
                        retention_rate=row["retention_rate"],
                        note=row.get("note", "")
                    ).model_dump()
                    for _, row in channel_df.iterrows()
                ]
            except Exception as e:
                logger.warning(f"Channel segmentation failed: {e}")
        
        # ── 8. Cohort 矩阵 ───────────────────────────────────
        
        cohort_headers = []
        cohort_data = []
        
        try:
            cohort_matrix = build_cohort_matrix(
                df_mapped,
                field_config,
                max_days=analysis_config.max_days,
                cohort_freq=analysis_config.cohort_freq,
            )
            
            # 转换为列表格式
            cohort_headers = ["Cohort"] + list(cohort_matrix.columns)
            cohort_data = []
            
            for idx, row in cohort_matrix.iterrows():
                cohort_data.append([idx] + row.tolist())
            
        except Exception as e:
            logger.warning(f"Cohort matrix build failed: {e}")
        
        # ── 9. 流失用户行为路径 ──────────────────────────────
        
        top_paths = []
        
        if n_churn > 0:
            try:
                churn_users, _ = get_churn_users(
                    df_mapped,
                    field_config,
                    reg_start,
                    reg_end,
                    retention_days=analysis_config.retention_days,
                )
                
                df_churn = df_mapped[df_mapped[field_config.user_id].isin(churn_users)]
                sequences = build_event_sequences(
                    df_churn,
                    field_config,
                    user_ids=churn_users,
                    n=5
                )
                top_paths = get_top_paths(sequences, n_total=n_churn, top_n=10)
                
            except Exception as e:
                logger.warning(f"Path analysis failed: {e}")

        diagnostics = {}
        if effective_param_config:
            try:
                diagnostics = RetentionDiagnosticAnalyzer(
                    df=df_mapped,
                    field_config=field_config,
                    param_config=effective_param_config,
                    virtual_fields=virtual_fields,
                ).run(
                    reg_start=reg_start,
                    reg_end=reg_end,
                    retention_days=analysis_config.retention_days,
                )
                diagnostics["ml_feature_diagnosis"] = ml_diagnostics
            except Exception as e:
                logger.warning(f"Diagnostic analysis failed: {e}")
                diagnostics = {
                    "error": str(e),
                    "ml_feature_diagnosis": ml_diagnostics,
                    "structured_diagnosis": {
                        "phenomenon": "诊断模块运行失败",
                        "attribution": str(e),
                        "suggestion": "请检查 JSON Key 映射是否与数据一致。",
                    },
                }

        try:
            agent_result = RetentionDiagnosisAgent(
                field_config=field_config,
                mapping=mapping.model_dump(),
                analysis_config=analysis_config,
                param_config=effective_param_config,
                game_genre=getattr(analysis_config, "game_genre", "casual"),
            ).run(
                df=df_mapped,
                cohort_headers=cohort_headers,
                cohort_matrix=cohort_data,
                virtual_fields=virtual_fields,
                precomputed_ml=ml_diagnostics,
            )
            diagnostics["agent_diagnosis"] = agent_result
            if not diagnostics.get("structured_diagnosis"):
                report = agent_result.get("structured_report", {})
                diagnostics["structured_diagnosis"] = {
                    "phenomenon": report.get("data_checkup", "暂无明显异常"),
                    "attribution": report.get("anomaly_location", "暂无明确归因"),
                    "suggestion": report.get("business_strategy", "继续观察分群和关键路径变化"),
                }
        except Exception as e:
            logger.warning(f"Agent diagnosis failed: {e}")
            diagnostics["agent_diagnosis"] = {
                "error": str(e),
                "structured_report": {
                    "data_checkup": "Agent 诊断运行失败",
                    "anomaly_location": str(e),
                    "core_attribution": "未产出",
                    "business_strategy": "请检查字段映射和 JSON 参数配置后重试。",
                },
            }
        
        # ── 10. 生成报告 ────────────────────────────────────
        
        report_markdown = _generate_report_markdown(
            session_id=session_id,
            mapping=mapping,
            analysis_config=analysis_config,
            n_total=n_total,
            n_retained=n_retained,
            n_churn=n_churn,
            retention_rate=retention_rate,
            country_retention=country_retention,
            channel_retention=channel_retention,
            top_paths=top_paths,
            sanity_warnings=sanity_warnings,
            diagnostics=diagnostics,
        )
        
        # ── 11. 更新 Session 状态 ─────────────────────────────
        
        SessionManager.update_session(session_id, status="done")
        
        # ── 12. 返回结果 ────────────────────────────────────
        
        return AnalysisResponse(
            session_id=session_id,
            success=True,
            message="分析完成",
            summary={
                "reg_start": analysis_config.reg_start,
                "reg_end": analysis_config.reg_end,
                "retention_days": analysis_config.retention_days,
                "n_total": n_total,
                "n_retained": n_retained,
                "n_churn": n_churn,
                "retention_rate": retention_rate,
            },
            retention_result=[
                RetentionResult(
                    segment=row["segment"],
                    n_total=row["n_total"],
                    n_retained=row["n_retained"],
                    retention_rate=row["retention_rate"],
                    note=row.get("note", "")
                ).model_dump()
                for _, row in retention_result.iterrows()
            ],
            cohort_headers=cohort_headers,
            cohort_matrix=cohort_data,
            churn_users_count=n_churn,
            retained_users_count=n_retained,
            country_retention=country_retention,
            channel_retention=channel_retention,
            top_paths=top_paths,
            report_markdown=report_markdown,
            sanity_check_report=sanity_report or {},
            diagnostics=diagnostics,
            virtual_fields=virtual_fields,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        SessionManager.update_session(session_id, status="error")
        raise HTTPException(status_code=500, detail=f"分析失败：{str(e)}")


def _generate_report_markdown(
    session_id: str,
    mapping: FieldMappingRequest,
    analysis_config: AnalysisConfig,
    n_total: int,
    n_retained: int,
    n_churn: int,
    retention_rate: float,
    country_retention: List[dict],
    channel_retention: List[dict],
    top_paths: List[dict],
    sanity_warnings: List[str],
    diagnostics: dict = None,
) -> str:
    """生成 Markdown 格式的分析报告"""
    
    # 国家分群表
    country_table = "| 国家/地区 | 总用户数 | 留存用户数 | 留存率 | 备注 |\n|---------|---------|-----------|------|------|\n"
    for row in country_retention[:10]:
        country_table += f"| {row['segment']} | {row['n_total']} | {row['n_retained']} | {row['retention_rate']}% | {row.get('note', '')} |\n"
    
    # 渠道分群表
    channel_table = "| 渠道 | 总用户数 | 留存用户数 | 留存率 | 备注 |\n|------|---------|-----------|------|------|\n"
    for row in channel_retention[:10]:
        channel_table += f"| {row['segment']} | {row['n_total']} | {row['n_retained']} | {row['retention_rate']}% | {row.get('note', '')} |\n"
    
    # 行为路径表
    path_table = "| 排名 | 行为路径 | 用户数 | 占比 |\n|------|---------|------|------|\n"
    for p in top_paths:
        path_table += f"| {p['rank']} | {p['path']} | {p['count']} | {p['pct']}% |\n"
    
    # 警告信息
    warnings_section = ""
    if sanity_warnings:
        warnings_section = "\n## ⚠️ 数据质量警告\n\n"
        for w in sanity_warnings:
            warnings_section += f"- {w}\n"

    diagnostics = diagnostics or {}
    structured = diagnostics.get("structured_diagnosis", {})
    agent = diagnostics.get("agent_diagnosis", {}) if isinstance(diagnostics, dict) else {}
    agent_report = agent.get("structured_report", {}) if isinstance(agent, dict) else {}
    agent_tools = " -> ".join(agent.get("tool_trace", [])) if isinstance(agent, dict) else ""
    ml_diagnosis = diagnostics.get("ml_feature_diagnosis", {})
    feature_importance = ml_diagnosis.get("feature_importance", {}) if isinstance(ml_diagnosis, dict) else {}
    top_features = feature_importance.get("top_features", []) if isinstance(feature_importance, dict) else []
    top_feature_rows = ""
    for item in top_features[:8]:
        top_feature_rows += f"| {item.get('feature')} | {item.get('importance')} |\n"
    if not top_feature_rows:
        top_feature_rows = "| 暂无 | 0 |\n"
    diagnostic_section = f"""
## 自动诊断结论

| 模块 | 结论 |
|------|------|
| 现象 | {structured.get('phenomenon', '暂无明显异常')} |
| 归因 | {structured.get('attribution', '暂无明确归因')} |
| 建议 | {structured.get('suggestion', '继续观察分群和关键路径变化')} |

## Agent 诊断书

| 模块 | 诊断 |
|------|------|
| 数据体检 | {agent_report.get('data_checkup', '暂无 Agent 数据体检')} |
| 异动定位 | {agent_report.get('anomaly_location', '暂无 Agent 异动定位')} |
| 核心归因 | {agent_report.get('core_attribution', '暂无 Agent 核心归因')} |
| 业务策略 | {agent_report.get('business_strategy', '暂无 Agent 业务策略')} |
| 工具调用链 | {agent_tools or '未记录'} |

## ML 特征重要性诊断

| 项目 | 结果 |
|------|------|
| 模型/方法 | {feature_importance.get('method', '未运行')} |
| 预测目标 | {feature_importance.get('target', 'D1 留存标签')} |
| 样本 x 特征 | {ml_diagnosis.get('feature_matrix_shape', ['-', '-'])} |
| 业务翻译 | {feature_importance.get('business_translation', '暂无模型诊断建议')} |

| 特征 | 重要性 |
|------|------|
{top_feature_rows}
"""
    
    content = f"""# 留存分析报告

> 生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}
> Session ID：{session_id}

---

## 一、数据口径

| 维度 | 说明 |
|------|------|
| 注册窗口 | {analysis_config.reg_start} ~ {analysis_config.reg_end} |
| 留存定义 | D+{analysis_config.retention_days} |
| 总用户数 | **{n_total:,}** |
| 留存用户数 | **{n_retained:,}** ({retention_rate:.2f}%) |
| 流失用户数 | **{n_churn:,}** ({100-retention_rate:.2f}%) |

---

## 二、D+{analysis_config.retention_days} 留存率

| 指标 | 值 |
|------|-----|
| 全体留存率 | {retention_rate:.2f}% |
| 留存用户数 | {n_retained:,} |
| 流失用户数 | {n_churn:,} |

---

## 三、分群分析

### 按国家/地区

{country_table if country_retention else '_未启用国家分群_'}

### 按渠道

{channel_table if channel_retention else '_未启用渠道分群_'}

---

## 四、流失用户行为路径（最后5步）

{path_table if top_paths else '_无流失用户_'}

---

{warnings_section}

---

{diagnostic_section}

---

## 五、字段映射配置

| 框架字段 | 原始列名 |
|---------|---------|
| user_id | {mapping.user_id} |
| event_time | {mapping.event_time} |
| event_date | {mapping.event_date} |
| reg_date | {mapping.reg_date} |
| event_name | {mapping.event_name} |
| country | {mapping.country or '(未配置)'} |
| channel | {mapping.channel or '(未配置)'} |

---

*本报告由 game_retention_framework Web API 自动生成*
"""
    
    return content
