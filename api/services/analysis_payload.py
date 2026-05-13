# -*- coding: utf-8 -*-
"""Build an LLM-safe analysis payload from Python-computed metrics."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from api.models.analysis_context import AnalysisContext


def _limit_rows(rows: Optional[List[Dict[str, Any]]], limit: int = 20) -> List[Dict[str, Any]]:
    return list(rows or [])[:limit]


def build_retention_payload(
    *,
    session_id: str,
    analysis_config: Any,
    summary: Dict[str, Any],
    retention_result: List[Dict[str, Any]],
    cohort_headers: List[str],
    cohort_matrix: List[List[Any]],
    country_retention: List[Dict[str, Any]],
    channel_retention: List[Dict[str, Any]],
    top_paths: List[Dict[str, Any]],
    sanity_report: Dict[str, Any],
    diagnostics: Dict[str, Any],
    analysis_context: Optional[AnalysisContext] = None,
) -> Dict[str, Any]:
    """Return aggregated data only; never include raw event logs."""
    return {
        "payload_version": "1.0",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "session_id": session_id,
        "analysis_context": analysis_context.compact() if analysis_context else {},
        "analysis_config": {
            "reg_start": getattr(analysis_config, "reg_start", None),
            "reg_end": getattr(analysis_config, "reg_end", None),
            "retention_days": getattr(analysis_config, "retention_days", None),
            "min_sample_size": getattr(analysis_config, "min_sample_size", None),
            "cohort_freq": getattr(analysis_config, "cohort_freq", None),
            "max_days": getattr(analysis_config, "max_days", None),
            "game_genre": getattr(analysis_config, "game_genre", None),
        },
        "summary": summary,
        "data_quality": sanity_report or {},
        "retention": {
            "overall": _limit_rows(retention_result, 10),
            "country": _limit_rows(country_retention, 20),
            "channel": _limit_rows(channel_retention, 20),
        },
        "cohort": {
            "headers": cohort_headers,
            "preview_rows": cohort_matrix[:12],
            "total_rows": len(cohort_matrix),
        },
        "paths": _limit_rows(top_paths, 20),
        "diagnostics": {
            key: value
            for key, value in (diagnostics or {}).items()
            if key not in {"ml_feature_diagnosis", "model_attribution"}
        },
        "constraints": {
            "llm_may_not_read_raw_logs": True,
            "llm_may_not_recalculate_metrics": True,
            "python_computed_metrics_only": True,
        },
    }
