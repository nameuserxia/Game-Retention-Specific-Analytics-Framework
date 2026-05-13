# -*- coding: utf-8 -*-
"""
api/routes/analysis.py
执行分析 API
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import AsyncIterator, List, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

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
from api.agent.context_builder import ContextBuilder
from api.agent.model_gateway import LLMConfig, ModelGateway
from api.ai.retention_reporter import RetentionReporter
from api.services.analysis_payload import build_retention_payload
from api.services.analysis_fields import (
    build_analysis_field_catalog,
    fallback_analysis_field_catalog,
    validate_analysis_dimensions,
)
from api.services.markdown_report import render_markdown_report
from api.services.report_store import ReportStore
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
from core.dynamic_retention import calculate_dynamic_retention
from core.funnel import calculate_funnel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["Analysis"])


def _normalize_dynamic_dimensions(
    dimension_sets: Optional[List[List[str]]],
    mapping: FieldMappingRequest,
) -> Optional[List[List[str]]]:
    """Map raw selected columns to standard post-mapping column names."""
    if not dimension_sets:
        return None

    mapping_data = mapping.model_dump(exclude_none=True)
    aliases = {}
    standard_fields = [
        "user_id",
        "event_time",
        "event_date",
        "reg_date",
        "event_name",
        "country",
        "channel",
        "json_params",
    ]
    for field in standard_fields:
        aliases[field] = field
        actual = mapping_data.get(field)
        if actual:
            aliases[str(actual)] = field

    for standard_name, actual_name in (mapping_data.get("extra_fields") or {}).items():
        if standard_name:
            aliases[str(standard_name)] = str(standard_name)
        if actual_name and standard_name:
            aliases[str(actual_name)] = str(standard_name)

    return [
        [aliases.get(str(dim).strip(), str(dim).strip()) for dim in dims if str(dim).strip()]
        for dims in dimension_sets
    ]


@router.post("/analyze/stream")
async def run_analysis_stream(
    session_id: str,
    request: AnalyzeRequest,
    force_proceed: bool = False,
):
    """
    流式分析接口：先推送结构化 JSON 元数据，然后以 SSE 流式推送 LLM Markdown 报告。

    与 /api/analyze 的区别：
    - 立即推送 data_health / anomaly / path / ml 等结构化数据（JSON lines）
    - 随后推送 LLM 生成的 Markdown 报告（流式 SSE）
    - 前端可实时渲染 Markdown，无需等待全部生成
    """
    # ── 加载 LLM 配置 ───────────────────────────────────────────
    llm_config_path = P(__file__).parent.parent.parent / "config" / "llm_config.yaml"
    llm_config: Optional[LLMConfig] = None
    try:
        llm_config = LLMConfig.from_yaml(str(llm_config_path))
        if not llm_config.is_enabled:
            llm_config = None
    except Exception:
        llm_config = None

    async def event_stream() -> AsyncIterator[bytes]:
        # 先推送元数据（分析开始标记）
        yield f"data: {json.dumps({'type': 'status', 'text': '正在计算留存数据…'}, ensure_ascii=False)}\n\n".encode()

        # ── 执行分析（复用 run_analysis 的核心逻辑，但跳过最后的 report_markdown 生成）───
        mapping = request.mapping
        analysis_config = request.analysis_config
        param_config = request.param_config

        if not SessionManager.is_valid(session_id):
            yield f"data: {json.dumps({'type': 'error', 'text': 'Session 已过期或不存在'}, ensure_ascii=False)}\n\n".encode()
            return

        SessionManager.update_session(session_id, status="analyzing")
        parquet_path = SessionManager.get_parquet_path(session_id)

        try:
            df = pd.read_parquet(parquet_path)
        except Exception as exc:
            yield f"data: {json.dumps({'type': 'error', 'text': f'读取数据失败: {exc}'}, ensure_ascii=False)}\n\n".encode()
            return

        try:
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

            model_diagnostics = {
                "status": "disabled",
                "reason": "Correlation-based attribution is disabled in the current phase.",
            }
            reg_start = pd.Timestamp(analysis_config.reg_start)
            reg_end = pd.Timestamp(analysis_config.reg_end)
            field_config = adapter.field_config

            # sanity_check（跳过 force_proceed 的错误抛出，仅获取报告）
            sanity_report = {}
            try:
                sanity_report = sanity_check(
                    df_mapped,
                    field_config,
                    min_sample_size=analysis_config.min_sample_size,
                    raise_on_failure=False,
                )
            except SanityCheckError:
                pass

            retention_result = calculate_retention(
                df_mapped,
                field_config,
                reg_start,
                reg_end,
                retention_days=analysis_config.retention_days,
            )

            # 推送元数据（结构化数据已完成）
            meta = {
                "type": "meta",
                "data_health": sanity_report,
                "retention_result": retention_result.to_dict(orient="records") if len(retention_result) else [],
                "virtual_fields": virtual_fields,
                "llm_enabled": llm_config is not None,
            }
            yield f"data: {json.dumps(meta, ensure_ascii=False, default=str)}\n\n".encode()
            yield f"data: {json.dumps({'type': 'status', 'text': '数据计算完成，正在生成 AI 分析…'}, ensure_ascii=False)}\n\n".encode()

            # ── 调用 LLM 生成报告 ───────────────────────────────────
            if llm_config:
                # 先跑规则引擎（作为 LLM 上下文）
                agent_result = RetentionDiagnosisAgent(
                    field_config=field_config,
                    mapping=mapping.model_dump(),
                    analysis_config=analysis_config,
                    param_config=effective_param_config,
                    game_genre=getattr(analysis_config, "game_genre", "casual"),
                    llm_config_path=str(llm_config_path),
                ).run(
                    df=df_mapped,
                    cohort_headers=[],
                    cohort_matrix=[],
                    virtual_fields=virtual_fields,
                    precomputed_ml=model_diagnostics,
                )
                # 如果规则引擎已经调用了 LLM 并拿到了结果，直接推送
                if agent_result.get("llm_used") and isinstance(agent_result.get("structured_report"), str):
                    for line in agent_result["structured_report"].splitlines(keepends=True):
                        yield f"data: {json.dumps({'type': 'markdown', 'text': line}, ensure_ascii=False)}\n\n".encode()
                else:
                    # 否则用流式接口重新调用 LLM
                    ctx = ContextBuilder(
                        agent_result=agent_result,
                        game_genre=getattr(analysis_config, "game_genre", "casual"),
                        benchmarks=agent_result.get("brain", {}).get("benchmarks", {}),
                    )
                    messages = [
                        {"role": "system", "content": ctx.system_prompt()},
                        {"role": "user", "content": ctx.build()},
                    ]
                    gateway = ModelGateway(llm_config)
                    async for chunk in gateway.chat_stream(messages):
                        if chunk:
                            yield f"data: {json.dumps({'type': 'markdown', 'text': chunk}, ensure_ascii=False)}\n\n".encode()
            else:
                # 无 LLM，推送规则引擎结果
                yield f"data: {json.dumps({'type': 'status', 'text': 'LLM 未启用，展示规则引擎结果。'}, ensure_ascii=False)}\n\n".encode()
                yield f"data: {json.dumps({'type': 'done', 'text': ''}, ensure_ascii=False)}\n\n".encode()
                SessionManager.update_session(session_id, status="done")
                return

            yield f"data: {json.dumps({'type': 'done', 'text': ''}, ensure_ascii=False)}\n\n".encode()
            SessionManager.update_session(session_id, status="done")

        except Exception as exc:
            logger.error("流式分析失败: %s", exc)
            yield f"data: {json.dumps({'type': 'error', 'text': f'分析失败: {exc}'}, ensure_ascii=False)}\n\n".encode()
            SessionManager.update_session(session_id, status="error")

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


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

        analysis_field_warnings = []
        try:
            analysis_field_catalog = build_analysis_field_catalog(
                df=df_mapped,
                mapping=mapping,
                virtual_fields=virtual_fields,
                param_config=effective_param_config,
            )
        except Exception as e:
            logger.warning("Analysis field catalog build failed: %s", e)
            analysis_field_catalog = fallback_analysis_field_catalog(
                df=df_mapped,
                mapping=mapping,
                virtual_fields=virtual_fields,
                reason=str(e),
            )
            analysis_field_warnings.extend(analysis_field_catalog.warnings)

        model_diagnostics = {
            "status": "disabled",
            "reason": "Correlation-based attribution is disabled in the current phase.",
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

        dynamic_retention = []
        if request.dynamic_dimensions:
            try:
                validated_dimensions, dimension_warnings = validate_analysis_dimensions(
                    _normalize_dynamic_dimensions(request.dynamic_dimensions, mapping),
                    analysis_field_catalog,
                    selected_fields=request.analysis_fields,
                )
                analysis_field_warnings.extend(dimension_warnings)
                dynamic_retention = calculate_dynamic_retention(
                    df=df_mapped,
                    cfg=field_config,
                    reg_start=reg_start,
                    reg_end=reg_end,
                    dimension_sets=validated_dimensions,
                    retention_days=request.dynamic_retention_days,
                )
                if analysis_field_warnings and dynamic_retention:
                    dynamic_retention[0].setdefault("warnings", []).extend(analysis_field_warnings)
            except Exception as e:
                logger.warning(f"Dynamic retention failed: {e}")
                dynamic_retention = [{
                    "dimensions": [],
                    "groups": [],
                    "warnings": [f"动态维度留存计算失败：{e}"],
                }]

        funnel_analysis = None
        if request.funnel_steps:
            try:
                funnel_analysis = calculate_funnel(
                    df=df_mapped,
                    cfg=field_config,
                    steps=request.funnel_steps,
                )
            except Exception as e:
                logger.warning(f"Funnel analysis failed: {e}")
                funnel_analysis = {
                    "steps": [],
                    "warnings": [f"漏斗分析计算失败：{e}"],
                }

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
                diagnostics["model_diagnostics"] = model_diagnostics
            except Exception as e:
                logger.warning(f"Diagnostic analysis failed: {e}")
                diagnostics = {
                    "error": str(e),
                    "model_diagnostics": model_diagnostics,
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
                precomputed_ml=model_diagnostics,
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
        
        summary = {
            "reg_start": analysis_config.reg_start,
            "reg_end": analysis_config.reg_end,
            "retention_days": analysis_config.retention_days,
            "n_total": n_total,
            "n_retained": n_retained,
            "n_churn": n_churn,
            "retention_rate": retention_rate,
        }
        retention_records = [
            RetentionResult(
                segment=row["segment"],
                n_total=row["n_total"],
                n_retained=row["n_retained"],
                retention_rate=row["retention_rate"],
                note=row.get("note", "")
            ).model_dump()
            for _, row in retention_result.iterrows()
        ]
        payload = build_retention_payload(
            session_id=session_id,
            analysis_config=analysis_config,
            summary=summary,
            retention_result=retention_records,
            cohort_headers=cohort_headers,
            cohort_matrix=cohort_data,
            country_retention=country_retention,
            channel_retention=channel_retention,
            top_paths=top_paths,
            sanity_report=sanity_report or {},
            diagnostics=diagnostics,
            analysis_context=request.analysis_context,
            analysis_fields=request.analysis_fields,
            analysis_field_catalog=analysis_field_catalog,
            analysis_field_warnings=analysis_field_warnings,
            dynamic_retention=dynamic_retention,
            funnel_analysis=funnel_analysis,
        )
        structured_report, llm_used, llm_fallback_reason = RetentionReporter().generate(
            payload,
            ai_enabled=bool(request.ai_enabled),
        )
        report_markdown = render_markdown_report(structured_report, payload)
        report_metadata = None
        try:
            report_metadata = ReportStore.save(
                session_id=session_id,
                report=structured_report,
                markdown=report_markdown,
                payload=payload,
                ai_enabled=bool(request.ai_enabled),
                llm_used=llm_used,
                fallback_reason=llm_fallback_reason,
            )
        except Exception as e:
            logger.warning("Report persistence failed: %s", e)
        
        # ── 11. 更新 Session 状态 ─────────────────────────────
        
        SessionManager.update_session(session_id, status="done")
        
        # ── 12. 返回结果 ────────────────────────────────────
        
        return AnalysisResponse(
            session_id=session_id,
            success=True,
            message="分析完成",
            summary=summary,
            retention_result=retention_records,
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
            report_id=report_metadata.report_id if report_metadata else None,
            report_title=structured_report.title,
            report_path=report_metadata.markdown_path if report_metadata else None,
            llm_used=llm_used,
            llm_fallback_reason=llm_fallback_reason or None,
            analysis_field_catalog=analysis_field_catalog,
            analysis_field_warnings=analysis_field_warnings,
            dynamic_retention=dynamic_retention,
            funnel_analysis=funnel_analysis,
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
        warnings_section = "\n## [!] 数据质量警告\n\n"
        for w in sanity_warnings:
            warnings_section += f"- {w}\n"

    diagnostics = diagnostics or {}
    structured = diagnostics.get("structured_diagnosis", {})
    agent = diagnostics.get("agent_diagnosis", {}) if isinstance(diagnostics, dict) else {}
    agent_report = agent.get("structured_report", {}) if isinstance(agent, dict) else {}
    agent_tools = " -> ".join(agent.get("tool_trace", [])) if isinstance(agent, dict) else ""
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
