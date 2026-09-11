from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd


class FraudScorer:
    def __init__(self, model_dir: str | Path = "models") -> None:
        model_dir = Path(model_dir)
        with open(model_dir / "feature_schema.json", encoding="utf-8") as file:
            schema = json.load(file)
        with open(model_dir / "threshold.json", encoding="utf-8") as file:
            threshold_data = json.load(file)
        with open(model_dir / "model_metadata.json", encoding="utf-8") as file:
            self.metadata = json.load(file)

        self.feature_names = [item["name"] for item in schema["features"]]
        self.threshold = float(threshold_data["decision_threshold"])
        self.model_version = str(self.metadata.get("model_version", "1.0.0"))
        self.model = joblib.load(model_dir / "fraud_pipeline.joblib")

    def validate_event(self, event: dict[str, Any]) -> None:
        required = {
            "transaction_id", "event_time", "producer_time", "source",
            "schema_version", "actual_label", "features"
        }
        missing = sorted(required - set(event))
        if missing:
            raise ValueError(f"Missing event fields: {missing}")
        if event["schema_version"] != "1.0":
            raise ValueError(f"Unsupported schema version: {event['schema_version']}")
        if int(event["actual_label"]) not in (0, 1):
            raise ValueError("actual_label must be 0 or 1")
        if not isinstance(event["features"], dict):
            raise ValueError("features must be an object")
        missing_features = sorted(set(self.feature_names) - set(event["features"]))
        if missing_features:
            raise ValueError(f"Missing model features: {missing_features}")
        for name in self.feature_names:
            value = float(event["features"][name])
            if not np.isfinite(value):
                raise ValueError(f"Feature {name} must be finite")
        if float(event["features"]["Amount"]) < 0:
            raise ValueError("Amount cannot be negative")

    def score(self, event: dict[str, Any]) -> dict[str, Any]:
        self.validate_event(event)
        frame = pd.DataFrame([{
            name: float(event["features"][name]) for name in self.feature_names
        }], columns=self.feature_names)
        score = float(self.model.predict_proba(frame)[0, 1])
        return {
            "fraud_score": score,
            "decision_threshold": self.threshold,
            "predicted_label": int(score >= self.threshold),
            "model_version": self.model_version,
        }
