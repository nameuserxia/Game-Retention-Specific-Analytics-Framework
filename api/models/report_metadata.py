# -*- coding: utf-8 -*-
"""Report metadata persisted outside temporary analysis sessions."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class ReportMetadata(BaseModel):
    report_id: str
    session_id: str
    title: str
    report_type: str = Field(default="retention")
    created_at: datetime
    ai_enabled: bool = False
    llm_used: bool = False
    fallback_reason: Optional[str] = None
    markdown_path: str
    payload_path: str
    llm_output_path: str
