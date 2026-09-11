from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib

from src.producer.schema import FEATURE_COLUMNS

LOG = logging.getLogger("fraud-model-loader")


@dataclass(frozen=True)
class ModelBundle:
    pipeline: Any
    feature_names: list[str]
    decision_threshold: float
    model_version: str
    metadata: dict[str, Any]


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Required artifact not found: {path.resolve()}")
    with path.open("r", encoding="utf-8") as file:
        payload = json.load(file)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return payload


def read_feature_names(schema: dict[str, Any]) -> list[str]:
    raw_features = schema.get("features")
    if not isinstance(raw_features, list):
        raise ValueError("feature_schema.json must contain a features list")

    names: list[str] = []
    for feature in raw_features:
        if isinstance(feature, str):
            names.append(feature)
        elif isinstance(feature, dict) and "name" in feature:
            names.append(str(feature["name"]))
        else:
            raise ValueError("Each schema feature must be a string or contain a name field")
    if not names:
        raise ValueError("The model feature schema is empty")
    return names


def load_model_bundle(models_directory: str | Path = "models") -> ModelBundle:
    models_path = Path(models_directory)
    model_path = models_path / "fraud_pipeline.joblib"
    schema = load_json(models_path / "feature_schema.json")
    threshold_data = load_json(models_path / "threshold.json")
    metadata = load_json(models_path / "model_metadata.json")

    if not model_path.exists():
        raise FileNotFoundError(f"Required artifact not found: {model_path.resolve()}")

    feature_names = read_feature_names(schema)
    if feature_names != FEATURE_COLUMNS:
        raise ValueError(
            "Model features do not match producer feature order. "
            f"Model={feature_names}; producer={FEATURE_COLUMNS}"
        )

    threshold_value = threshold_data.get("decision_threshold", threshold_data.get("threshold"))
    if threshold_value is None:
        raise ValueError("threshold.json does not contain decision_threshold")
    decision_threshold = float(threshold_value)
    if not 0.0 <= decision_threshold <= 1.0:
        raise ValueError("Decision threshold must be between 0 and 1")

    LOG.info("Loading model from %s", model_path)
    pipeline = joblib.load(model_path)
    if not hasattr(pipeline, "predict_proba"):
        raise TypeError("Loaded model pipeline does not implement predict_proba")

    classifier = getattr(pipeline, "named_steps", {}).get("classifier")
    if classifier is not None and hasattr(classifier, "set_params"):
        try:
            classifier.set_params(device="cpu")
            LOG.info("Configured classifier for local CPU inference")
        except (TypeError, ValueError):
            pass

    bundle = ModelBundle(
        pipeline=pipeline,
        feature_names=feature_names,
        decision_threshold=decision_threshold,
        model_version=str(metadata.get("model_version", "unknown")),
        metadata=metadata,
    )
    LOG.info(
        "Loaded model version=%s threshold=%.10f features=%d",
        bundle.model_version,
        bundle.decision_threshold,
        len(bundle.feature_names),
    )
    return bundle
