# -*- coding: utf-8 -*-
"""ML-style pipeline for retention feature diagnosis."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from api.adapters.param_converter import ParamConverter
from api.models.schemas import ParamMappingConfig
from core.analytics import FieldConfig


def _as_mapping_dict(mapping: Any) -> Dict[str, Any]:
    if mapping is None:
        return {}
    if isinstance(mapping, dict):
        return mapping
    if hasattr(mapping, "model_dump"):
        return mapping.model_dump()
    return dict(mapping)


def _business_translation(feature: str) -> str:
    rules = [
        ("v_step_gap", "建议检查关卡难度：实际步数与最优步数差距对留存影响最大。"),
        ("step_gap", "建议检查关卡难度：实际步数与最优步数差距对留存影响最大。"),
        ("pass_rate", "建议检查关卡通过率与失败反馈，优化失败后的复玩引导。"),
        ("fail_rate", "建议检查失败率较高的关卡和失败原因。"),
        ("max_progress", "建议检查早期进度断点，用户可能卡在某个关键关卡。"),
        ("event_count", "建议检查新手期内容密度，事件参与度对留存影响较高。"),
        ("active_days", "建议检查首日到次日的回访触发机制。"),
        ("ad", "建议检查广告展示节奏和奖励广告价值。"),
        ("retry", "建议检查重试体验和失败后的补偿机制。"),
    ]
    lower = feature.lower()
    for token, message in rules:
        if token in lower:
            return message
    return f"建议重点复盘特征 {feature} 对留存的影响，并结合分群进一步验证。"


@dataclass
class DataPipeline:
    """Base pipeline. Subclasses implement transform(df)."""

    mapping: Dict[str, Any]
    field_config: FieldConfig
    param_config: Optional[ParamMappingConfig] = None

    def transform(self, df: pd.DataFrame) -> Dict[str, Any]:
        raise NotImplementedError


@dataclass
class JsonUnpacker(DataPipeline):
    """Dynamically unpack configured JSON params into v_* columns."""

    virtual_fields: List[str] = field(default_factory=list)

    def transform(self, df: pd.DataFrame) -> Dict[str, Any]:
        config = self.param_config
        if config is None:
            json_col = self.mapping.get("json_params")
            config = ParamMappingConfig(json_params_col=json_col)

        if not config.json_params_col:
            return {"df": df, "virtual_fields": []}

        keys = set(config.numeric_keys or [])
        keys.update(config.segment_keys or [])
        for key in [config.progress_key, config.result_key]:
            if key:
                keys.add(key)

        # Common gameplay keys used by FeatureDeriver, added opportunistically.
        keys.update({"step", "optimal_step", "time", "retry_times", "level_id", "state"})
        config.numeric_keys = list(set(config.numeric_keys or []) | {"step", "optimal_step", "time", "retry_times"})
        config.segment_keys = list(set(config.segment_keys or []) | {k for k in keys if k not in config.numeric_keys})

        converter = ParamConverter(config)
        out = converter.transform(df)
        self.virtual_fields = converter.virtual_fields
        return {"df": out, "virtual_fields": converter.virtual_fields}


@dataclass
class FeatureDeriver(DataPipeline):
    """Derive user-level feature matrix from standardized event data."""

    virtual_fields: List[str] = field(default_factory=list)

    def transform(self, df: pd.DataFrame) -> Dict[str, Any]:
        uid = self.field_config.user_id
        event_name = self.field_config.event_name
        event_date = self.field_config.event_date

        work = df.copy()
        if "v_step" in work.columns and "v_optimal_step" in work.columns:
            work["v_step_gap"] = pd.to_numeric(work["v_step"], errors="coerce") - pd.to_numeric(work["v_optimal_step"], errors="coerce")
            self.virtual_fields = list(dict.fromkeys([*self.virtual_fields, "v_step_gap"]))

        if "v_state" in work.columns:
            state = work["v_state"].astype(str).str.lower()
            work["v_is_pass"] = state.eq("pass").astype(int)
            work["v_is_fail"] = state.eq("fail").astype(int)

        numeric_cols = [
            col for col in work.columns
            if col.startswith("v_") and pd.api.types.is_numeric_dtype(work[col])
        ]

        grouped = work.groupby(uid, dropna=False)
        features = pd.DataFrame(index=grouped.size().index)
        features["event_count"] = grouped.size()

        if event_name in work.columns:
            features["unique_event_count"] = grouped[event_name].nunique()
            event_dummies = work[event_name].astype(str).str.lower()
            features["ad_event_count"] = event_dummies.str.contains("ad|广告", regex=True).groupby(work[uid]).sum()
            features["login_event_count"] = event_dummies.str.contains("login|登录", regex=True).groupby(work[uid]).sum()

        if event_date in work.columns:
            features["active_days"] = grouped[event_date].nunique()

        for col in numeric_cols:
            features[f"{col}_mean"] = grouped[col].mean()
            features[f"{col}_max"] = grouped[col].max()

        for col in ["v_is_pass", "v_is_fail"]:
            if col in work.columns:
                features[f"{col}_rate"] = grouped[col].mean()

        if "v_level_id" in work.columns:
            numeric_level = pd.to_numeric(work["v_level_id"], errors="coerce")
            features["max_progress"] = numeric_level.groupby(work[uid]).max()

        if "v_step_gap" in work.columns:
            features["v_step_gap_mean"] = grouped["v_step_gap"].mean()
            features["v_step_gap_max"] = grouped["v_step_gap"].max()

        features = features.replace([np.inf, -np.inf], np.nan).fillna(0)
        return {"df": work, "features": features}


@dataclass
class LabelGenerator(DataPipeline):
    """Generate D1/D7 retention labels at user level."""

    def transform(self, df: pd.DataFrame) -> Dict[str, Any]:
        uid = self.field_config.user_id
        event_date = self.field_config.event_date
        reg_date = self.field_config.reg_date

        user_reg = df.dropna(subset=[reg_date]).groupby(uid)[reg_date].min()
        user_dates = df.dropna(subset=[event_date]).groupby(uid)[event_date].apply(lambda s: set(s.dt.normalize()))
        labels = pd.DataFrame(index=user_reg.index)
        labels["reg_date"] = user_reg

        for day in [1, 7]:
            labels[f"label_D{day}_retained"] = [
                int((reg.normalize() + pd.Timedelta(days=day)) in user_dates.get(user, set()))
                for user, reg in user_reg.items()
            ]

        return {"labels": labels}


@dataclass
class RetentionMLPipeline(DataPipeline):
    """Full pipeline: JSON unpack -> feature deriving -> D1/D7 labels -> feature importance."""

    def transform(self, df: pd.DataFrame) -> Dict[str, Any]:
        unpacker = JsonUnpacker(self.mapping, self.field_config, self.param_config)
        unpacked = unpacker.transform(df)
        df_unpacked = unpacked["df"]

        deriver = FeatureDeriver(self.mapping, self.field_config, self.param_config, unpacked["virtual_fields"])
        derived = deriver.transform(df_unpacked)

        labeler = LabelGenerator(self.mapping, self.field_config, self.param_config)
        labels = labeler.transform(df_unpacked)["labels"]

        matrix = derived["features"].join(labels, how="inner")
        label_col = "label_D1_retained"
        feature_cols = [col for col in matrix.columns if not col.startswith("label_") and col != "reg_date"]
        X = matrix[feature_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
        y = matrix[label_col].astype(int) if label_col in matrix.columns else pd.Series(dtype=int)

        importance = self._feature_importance(X, y)
        return {
            "df": derived["df"],
            "feature_matrix": X,
            "labels": matrix[[col for col in matrix.columns if col.startswith("label_")]],
            "virtual_fields": list(dict.fromkeys([*unpacked["virtual_fields"], *deriver.virtual_fields])),
            "feature_importance": importance,
        }

    def _feature_importance(self, X: pd.DataFrame, y: pd.Series) -> Dict[str, Any]:
        if X.empty or y.empty or y.nunique() < 2:
            return {
                "method": "not_available",
                "top_features": [],
                "business_translation": "样本或标签不足，暂无法训练特征重要性模型。",
            }

        try:
            from sklearn.ensemble import RandomForestClassifier

            model = RandomForestClassifier(
                n_estimators=80,
                max_depth=6,
                min_samples_leaf=10,
                random_state=42,
                n_jobs=-1,
            )
            model.fit(X, y)
            values = model.feature_importances_
            method = "random_forest"
        except Exception:
            pos = X[y == 1].mean()
            neg = X[y == 0].mean()
            values = (pos - neg).abs().fillna(0).to_numpy()
            method = "mean_difference_fallback"

        ranked: List[Tuple[str, float]] = sorted(
            zip(X.columns, values),
            key=lambda item: item[1],
            reverse=True,
        )
        top_features = [
            {"feature": feature, "importance": round(float(score), 6)}
            for feature, score in ranked[:10]
        ]
        top_feature = top_features[0]["feature"] if top_features else ""
        return {
            "method": method,
            "target": "label_D1_retained",
            "n_samples": int(len(X)),
            "n_features": int(X.shape[1]),
            "top_features": top_features,
            "business_translation": _business_translation(top_feature) if top_feature else "未发现有效重要特征。",
        }
