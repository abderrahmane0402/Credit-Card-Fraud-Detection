from __future__ import annotations

import math
from datetime import datetime
from typing import Any

from src.producer.schema import FEATURE_COLUMNS, SCHEMA_VERSION
REQUIRED_EVENT_FIELDS = [
    "transaction_id",
    "run_id",
    "source_row_index",
    "sequence_number",
    "event_time",
    "producer_time",
    "source",
    "replay_mode",
    "schema_version",
    "actual_label",
    "features",
]


def parse_iso_datetime(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty ISO datetime string")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid ISO datetime string") from exc


def validate_transaction_event(event: dict[str, Any]) -> None:
    if not isinstance(event, dict):
        raise ValueError("Transaction event must be a JSON object")

    missing = [field for field in REQUIRED_EVENT_FIELDS if field not in event]
    if missing:
        raise ValueError(f"Transaction event is missing fields: {missing}")

    if not isinstance(event["transaction_id"], str) or not event["transaction_id"].strip():
        raise ValueError("transaction_id must be a non-empty string")

    parse_iso_datetime(event["event_time"], "event_time")
    parse_iso_datetime(event["producer_time"], "producer_time")

    if event["schema_version"] != SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported schema version {event['schema_version']!r}; expected {SCHEMA_VERSION!r}"
        )

    if not isinstance(event["source"], str) or not event["source"].strip():
        raise ValueError("source must be a non-empty string")

    sequence_number = event["sequence_number"]
    if isinstance(sequence_number, bool) or not isinstance(sequence_number, int) or sequence_number <= 0:
        raise ValueError("sequence_number must be a positive integer")

    if event["actual_label"] not in (0, 1):
        raise ValueError("actual_label must be 0 or 1")

    features = event["features"]
    if not isinstance(features, dict):
        raise ValueError("features must be a JSON object")

    missing_features = [name for name in FEATURE_COLUMNS if name not in features]
    if missing_features:
        raise ValueError(f"Transaction features are missing: {missing_features}")

    unexpected = [name for name in features if name not in FEATURE_COLUMNS]
    if unexpected:
        raise ValueError(f"Transaction contains unexpected features: {unexpected}")

    for name in FEATURE_COLUMNS:
        value = features[name]
        if isinstance(value, bool):
            raise ValueError(f"Feature {name} must be numeric")
        try:
            number = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Feature {name} must be numeric") from exc
        if not math.isfinite(number):
            raise ValueError(f"Feature {name} must be finite")

    if float(features["Amount"]) < 0:
        raise ValueError("Amount cannot be negative")


def validate_scored_event(event: dict[str, Any]) -> None:
    validate_transaction_event(event)
    required = [
        "risk_score",
        "predicted_label",
        "decision_threshold",
        "model_version",
        "processed_time",
        "processing_latency_ms",
        "prediction_correct",
    ]
    missing = [field for field in required if field not in event]
    if missing:
        raise ValueError(f"Scored event is missing fields: {missing}")

    risk_score = float(event["risk_score"])
    threshold = float(event["decision_threshold"])
    if not 0.0 <= risk_score <= 1.0:
        raise ValueError("risk_score must be between 0 and 1")
    if not 0.0 <= threshold <= 1.0:
        raise ValueError("decision_threshold must be between 0 and 1")
    if event["predicted_label"] not in (0, 1):
        raise ValueError("predicted_label must be 0 or 1")
    if event["predicted_label"] != int(risk_score >= threshold):
        raise ValueError("predicted_label does not match risk_score and threshold")
    if not isinstance(event["model_version"], str) or not event["model_version"].strip():
        raise ValueError("model_version must be a non-empty string")
    parse_iso_datetime(event["processed_time"], "processed_time")
    latency = float(event["processing_latency_ms"])
    if not math.isfinite(latency) or latency < 0:
        raise ValueError("processing_latency_ms must be a finite non-negative number")
    if not isinstance(event["prediction_correct"], bool):
        raise ValueError("prediction_correct must be boolean")
