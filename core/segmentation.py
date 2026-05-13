# -*- coding: utf-8 -*-
"""Segmentation helpers for retention analysis.

This module is intentionally light in the equivalence split. Existing segmented
retention behavior remains implemented by ``calculate_retention(..., segment_col=...)``.
"""

from __future__ import annotations

from core.retention import calculate_retention

__all__ = ["calculate_retention"]
