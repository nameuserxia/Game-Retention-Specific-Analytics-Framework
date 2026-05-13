# -*- coding: utf-8 -*-
"""Structured report contract for AI and fallback retention diagnosis."""

from __future__ import annotations

from typing import List

from pydantic import BaseModel, Field


class ReportFinding(BaseModel):
    title: str = Field(default="")
    detail: str = Field(default="")
    evidence: List[str] = Field(default_factory=list)


class ReportRecommendation(BaseModel):
    priority: str = Field(default="medium")
    action: str = Field(default="")
    expected_impact: str = Field(default="")
    validation: str = Field(default="")


class LLMRetentionReport(BaseModel):
    title: str = Field(default="留存分析报告")
    quality_assessment: str = Field(default="")
    retention_diagnosis: List[ReportFinding] = Field(default_factory=list)
    segment_findings: List[ReportFinding] = Field(default_factory=list)
    funnel_analysis: List[ReportFinding] = Field(default_factory=list)
    recommendations: List[ReportRecommendation] = Field(default_factory=list)
    next_checks: List[str] = Field(default_factory=list)
    fallback_used: bool = Field(default=False)
    fallback_reason: str = Field(default="")


def normalize_report(data: object, fallback_reason: str = "") -> LLMRetentionReport:
    """Validate arbitrary model output and fall back to a safe report."""
    if isinstance(data, LLMRetentionReport):
        return data
    if isinstance(data, dict):
        return LLMRetentionReport.model_validate(data)
    return LLMRetentionReport(
        fallback_used=True,
        fallback_reason=fallback_reason or "invalid_report_payload",
        quality_assessment="AI 报告结构无效，已使用规则报告降级。",
    )
