# -*- coding: utf-8 -*-
"""Persistent report storage for Markdown reports and metadata."""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from api.models.llm_report import LLMRetentionReport
from api.models.report_metadata import ReportMetadata


PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORTS_DIR = PROJECT_ROOT / "reports"
METADATA_PATH = REPORTS_DIR / "metadata.json"


class ReportStore:
    @staticmethod
    def _ensure_root() -> None:
        REPORTS_DIR.mkdir(exist_ok=True)
        if not METADATA_PATH.exists():
            METADATA_PATH.write_text("[]\n", encoding="utf-8")

    @staticmethod
    def list_metadata() -> List[Dict[str, Any]]:
        ReportStore._ensure_root()
        try:
            data = json.loads(METADATA_PATH.read_text(encoding="utf-8") or "[]")
            return data if isinstance(data, list) else []
        except Exception:
            return []

    @staticmethod
    def save(
        *,
        session_id: str,
        report: LLMRetentionReport,
        markdown: str,
        payload: Dict[str, Any],
        ai_enabled: bool,
        llm_used: bool,
        fallback_reason: str = "",
        report_type: str = "retention",
    ) -> ReportMetadata:
        ReportStore._ensure_root()
        report_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
        report_dir = REPORTS_DIR / report_id
        report_dir.mkdir(parents=True, exist_ok=True)

        markdown_path = report_dir / "report.md"
        payload_path = report_dir / "payload.json"
        llm_output_path = report_dir / "llm_output.json"

        markdown_path.write_text(markdown, encoding="utf-8")
        payload_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        llm_output_path.write_text(
            json.dumps(report.model_dump(), ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

        metadata = ReportMetadata(
            report_id=report_id,
            session_id=session_id,
            title=report.title,
            report_type=report_type,
            created_at=datetime.now(),
            ai_enabled=ai_enabled,
            llm_used=llm_used,
            fallback_reason=fallback_reason or None,
            markdown_path=str(markdown_path),
            payload_path=str(payload_path),
            llm_output_path=str(llm_output_path),
        )

        all_metadata = ReportStore.list_metadata()
        all_metadata.insert(0, metadata.model_dump(mode="json"))
        METADATA_PATH.write_text(
            json.dumps(all_metadata, ensure_ascii=False, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
        return metadata
