# -*- coding: utf-8 -*-
"""Build and validate the enhanced analysis field catalog."""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from api.models.schemas import (
    AnalysisField,
    AnalysisFieldCatalog,
    FieldMappingRequest,
    ParamMappingConfig,
)


MOSTLY_NULL_THRESHOLD = 0.8
HIGH_CARDINALITY_LIMIT = 100
HIGH_CARDINALITY_RATIO = 0.2
IDENTIFIER_RE = re.compile(r"(^|_)(id|uuid|guid|trace|device|session|distinct)(_|$)", re.IGNORECASE)
STANDARD_FIELDS = [
    "user_id",
    "event_time",
    "event_date",
    "reg_date",
    "event_name",
    "country",
    "channel",
    "json_params",
]


def _unique(values: Iterable[str]) -> List[str]:
    return list(dict.fromkeys(str(value).strip() for value in values if str(value).strip()))


def _safe_sample(series: pd.Series, limit: int = 5) -> List[Any]:
    values = []
    for value in series.dropna().head(100).unique().tolist():
        if len(values) >= limit:
            break
        if hasattr(value, "isoformat"):
            values.append(value.isoformat())
        else:
            values.append(str(value))
    return values


def _source_columns(mapping: FieldMappingRequest) -> Dict[str, str]:
    data = mapping.model_dump(exclude_none=True)
    sources = {}
    for field in STANDARD_FIELDS:
        actual = data.get(field)
        if actual:
            sources[field] = str(actual)
    for standard_name, actual_name in (data.get("extra_fields") or {}).items():
        if standard_name and actual_name:
            sources[str(standard_name)] = str(actual_name)
    return sources


def _health_flags(field_id: str, cardinality: int, null_ratio: float, row_count: int) -> List[str]:
    flags = []
    if null_ratio >= MOSTLY_NULL_THRESHOLD:
        flags.append("mostly_null")
    if cardinality <= 1:
        flags.append("constant_field")
    high_cardinality = cardinality > HIGH_CARDINALITY_LIMIT or (
        row_count > 0 and cardinality / row_count > HIGH_CARDINALITY_RATIO
    )
    if high_cardinality:
        flags.append("high_cardinality")
    if high_cardinality and IDENTIFIER_RE.search(field_id):
        flags.append("likely_identifier")
    return flags


def _available_for(source_type: str, field_id: str, recommended: bool) -> List[str]:
    values = ["ai_diagnosis", "plotting", "future_causal"]
    if field_id == "event_name":
        values.append("funnel")
    if recommended:
        values.insert(0, "dynamic_retention")
    return values


def _field_from_series(
    *,
    field_id: str,
    label: str,
    source_type: str,
    source_column: Optional[str],
    series: pd.Series,
    row_count: int,
) -> AnalysisField:
    non_null = series.dropna()
    cardinality = int(non_null.nunique(dropna=True))
    null_ratio = round(float(series.isna().mean()), 4) if row_count else 0.0
    flags = _health_flags(field_id, cardinality, null_ratio, row_count)
    recommended = (
        cardinality >= 2
        and cardinality <= HIGH_CARDINALITY_LIMIT
        and not any(flag in flags for flag in ["mostly_null", "constant_field", "high_cardinality", "likely_identifier"])
    )
    return AnalysisField(
        field_id=field_id,
        label=label,
        source_type=source_type,
        source_column=source_column,
        dtype=str(series.dtype),
        cardinality=cardinality,
        null_ratio=null_ratio,
        sample_values=[] if "likely_identifier" in flags else _safe_sample(series),
        health_flags=flags,
        recommended_for_segmentation=recommended,
        available_for=_available_for(source_type, field_id, recommended),
    )


def build_analysis_field_catalog(
    df: pd.DataFrame,
    mapping: FieldMappingRequest,
    virtual_fields: Optional[Sequence[str]] = None,
    param_config: Optional[ParamMappingConfig] = None,
) -> AnalysisFieldCatalog:
    """Build an enhanced field catalog from mapped data and extracted fields."""
    row_count = len(df)
    source_columns = _source_columns(mapping)
    virtual_set = set(virtual_fields or [])
    extracted_key_map: Dict[str, str] = {}
    if param_config:
        for key in _unique(param_config.extracted_keys or []):
            safe_key = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in key)
            extracted_key_map[f"v_{safe_key}"] = key

    fields: List[AnalysisField] = []
    seen = set()
    ordered_columns = [col for col in STANDARD_FIELDS if col in df.columns]
    ordered_columns.extend([col for col in df.columns if col not in ordered_columns])

    for col in ordered_columns:
        if col in seen:
            continue
        seen.add(col)
        if col in virtual_set or col.startswith("v_"):
            label = extracted_key_map.get(col, col[2:] if col.startswith("v_") else col)
            source_type = "virtual"
            source_column = param_config.json_params_col if param_config else source_columns.get("json_params")
        elif col in STANDARD_FIELDS:
            label = col
            source_type = "standard"
            source_column = source_columns.get(col, col)
        else:
            label = col
            source_type = "raw"
            source_column = col

        fields.append(
            _field_from_series(
                field_id=col,
                label=label,
                source_type=source_type,
                source_column=source_column,
                series=df[col],
                row_count=row_count,
            )
        )

    return AnalysisFieldCatalog(status="ok", fields=fields, warnings=[])


def fallback_analysis_field_catalog(
    df: pd.DataFrame,
    mapping: FieldMappingRequest,
    virtual_fields: Optional[Sequence[str]] = None,
    reason: str = "",
) -> AnalysisFieldCatalog:
    """Return a minimal catalog if the enhanced catalog fails."""
    virtual_set = set(virtual_fields or [])
    fields = []
    for col in df.columns:
        source_type = "virtual" if col in virtual_set or col.startswith("v_") else ("standard" if col in STANDARD_FIELDS else "raw")
        fields.append(
            AnalysisField(
                field_id=str(col),
                label=str(col[2:] if str(col).startswith("v_") else col),
                source_type=source_type,
                source_column=str(col),
                health_flags=["catalog_fallback"],
                recommended_for_segmentation=False,
                available_for=["ai_diagnosis"],
            )
        )
    return AnalysisFieldCatalog(
        status="fallback",
        fields=fields,
        warnings=["Analysis field catalog fallback is active."],
        fallback_reason=reason or "catalog build failed",
    )


def validate_analysis_dimensions(
    dynamic_dimensions: Optional[List[List[str]]],
    catalog: Optional[AnalysisFieldCatalog],
    selected_fields: Optional[List[str]] = None,
) -> Tuple[Optional[List[List[str]]], List[str]]:
    """Validate dynamic retention dimensions against selected catalog fields."""
    if not dynamic_dimensions:
        return None, []

    warnings: List[str] = []
    if not catalog or catalog.status == "failed":
        return dynamic_dimensions, ["Analysis field catalog unavailable; dynamic dimensions will use column existence checks."]

    field_map = {field.field_id: field for field in catalog.fields}
    selected = set(_unique(selected_fields or []))
    valid_dimensions: List[List[str]] = []

    for raw_dims in dynamic_dimensions:
        dims = _unique(raw_dims or [])
        if not dims:
            continue
        missing = [dim for dim in dims if dim not in field_map]
        if missing:
            warnings.append(f"Skipped dynamic dimension {dims}: fields not found in catalog: {missing}.")
            continue
        if selected:
            not_selected = [dim for dim in dims if dim not in selected]
            if not_selected:
                warnings.append(f"Skipped dynamic dimension {dims}: fields not selected for analysis: {not_selected}.")
                continue
        risk_flags = {
            dim: field_map[dim].health_flags
            for dim in dims
            if field_map[dim].health_flags
            and not field_map[dim].recommended_for_segmentation
        }
        if risk_flags:
            warnings.append(f"Dynamic dimension {dims} contains non-recommended fields: {risk_flags}.")
        valid_dimensions.append(dims)

    return valid_dimensions, warnings
