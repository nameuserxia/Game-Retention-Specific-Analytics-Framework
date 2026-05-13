# -*- coding: utf-8 -*-
"""
JSON 参数列解析与虚拟字段生成。

ParamConverter 的职责是把前端声明的 JSON Key 映射转化为 pandas 处理算子，
后续诊断模块只依赖生成后的 v_* 虚拟列，不再关心原始 JSON 格式。
"""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from api.models.schemas import JsonKeyInfo, ParamMappingConfig

logger = logging.getLogger(__name__)


def _safe_parse_json(value: Any) -> Dict[str, Any]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return {}
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return {}

    text = value.strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return {}

    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _role_for_key(key: str) -> str:
    lower = key.lower()
    if lower in {"level_id", "level", "stage", "chapter_id", "关卡"}:
        return "progress"
    if lower in {"state", "status", "result", "outcome"}:
        return "result"
    if lower in {"step", "steps", "time", "duration", "score", "count"}:
        return "numeric"
    return "segment"


@dataclass
class ParamConverter:
    """Convert configured JSON params into virtual pandas columns."""

    config: ParamMappingConfig
    prefix: str = "v_"
    parse_errors: int = 0
    virtual_fields: List[str] = field(default_factory=list)

    @property
    def json_col(self) -> Optional[str]:
        return self.config.json_params_col

    @staticmethod
    def discover_keys(df: pd.DataFrame, json_col: str, sample_size: int = 5000) -> Dict[str, Any]:
        if json_col not in df.columns:
            raise KeyError(f"JSON 参数列不存在：{json_col}")

        sample = df[json_col].head(sample_size)
        counter: Counter[str] = Counter()
        samples: Dict[str, List[Any]] = defaultdict(list)
        parsed_rows = 0
        parse_error_rows = 0

        for value in sample:
            parsed = _safe_parse_json(value)
            if not parsed:
                if value not in (None, "") and not (isinstance(value, float) and pd.isna(value)):
                    parse_error_rows += 1
                continue

            parsed_rows += 1
            for key, item in parsed.items():
                counter[str(key)] += 1
                if len(samples[str(key)]) < 5:
                    samples[str(key)].append(item)

        total = max(len(sample), 1)
        keys = [
            JsonKeyInfo(
                key=key,
                count=count,
                fill_rate=round(count / total, 4),
                sample_values=[str(v) for v in samples[key]],
                suggested_role=_role_for_key(key),
            ).model_dump()
            for key, count in counter.most_common()
        ]

        return {
            "total_sampled_rows": len(sample),
            "parsed_rows": parsed_rows,
            "parse_error_rows": parse_error_rows,
            "keys": keys,
        }

    def configured_keys(self) -> List[str]:
        keys: List[str] = []
        keys.extend(self.config.extracted_keys or [])
        for key in [self.config.progress_key, self.config.result_key]:
            if key:
                keys.append(key)
        keys.extend(self.config.numeric_keys or [])
        keys.extend(self.config.segment_keys or [])
        return list(dict.fromkeys(keys))

    def virtual_name(self, key: str) -> str:
        safe_key = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in key)
        return f"{self.prefix}{safe_key}"

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.json_col or self.json_col not in df.columns:
            return df

        keys = self.configured_keys()
        if not keys:
            return df

        out = df.copy()
        parsed_rows = out[self.json_col].map(_safe_parse_json)
        self.parse_errors = int((parsed_rows.map(len) == 0).sum())

        self.virtual_fields = []
        for key in keys:
            col = self.virtual_name(key)
            out[col] = parsed_rows.map(lambda item, k=key: item.get(k))
            if key in (self.config.numeric_keys or []):
                out[col] = pd.to_numeric(out[col], errors="coerce")
            self.virtual_fields.append(col)

        return out

    def transform_chunks(self, chunks: Iterable[pd.DataFrame]) -> Iterable[pd.DataFrame]:
        for chunk in chunks:
            try:
                yield self.transform(chunk)
            except Exception as exc:
                logger.warning("JSON 参数分块解析失败：%s", exc)
                yield chunk
