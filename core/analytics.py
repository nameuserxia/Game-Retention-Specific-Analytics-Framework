# -*- coding: utf-8 -*-
"""Compatibility entrypoint for game retention analytics.

The implementation is split by responsibility, while public imports from
``core.analytics`` remain supported for existing callers.
"""

from __future__ import annotations

from core.cohort import build_cohort_matrix
from core.churn import get_churn_users
from core.data_quality import SanityCheckError, SanityCheckWarning, sanity_check
from core.retention import calculate_retention
from core.sequences import build_event_sequences, get_last_n_events, get_top_paths
from core.utils import FieldConfig

__all__ = [
    "FieldConfig",
    "SanityCheckError",
    "SanityCheckWarning",
    "sanity_check",
    "get_churn_users",
    "calculate_retention",
    "build_cohort_matrix",
    "get_last_n_events",
    "build_event_sequences",
    "get_top_paths",
]
