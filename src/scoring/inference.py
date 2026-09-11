from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

from src.scoring.model_loader import ModelBundle
from src.scoring.schema import parse_iso_datetime, validate_scored_event, validate_transaction_event


def score_transaction(event: dict[str, Any], bundle: ModelBundle) -> dict[str, Any]:
    validate_transaction_event(event)

    model_input = pd.DataFrame(
        [{name: float(event["features"][name]) for name in bundle.feature_names}],
        columns=bundle.feature_names,
    )
    probabilities = bundle.pipeline.predict_proba(model_input)
    if probabilities.ndim != 2 or probabilities.shape != (1, 2):
        raise ValueError(f"Unexpected predict_proba shape: {probabilities.shape}")

    risk_score = float(probabilities[0, 1])
    if not 0.0 <= risk_score <= 1.0:
        raise ValueError(f"Model returned invalid risk score: {risk_score}")

    processed_at = datetime.now(timezone.utc)
    producer_at = parse_iso_datetime(event["producer_time"], "producer_time")
    if producer_at.tzinfo is None:
        producer_at = producer_at.replace(tzinfo=timezone.utc)

    predicted_label = int(risk_score >= bundle.decision_threshold)
    scored = {
        **event,
        "risk_score": risk_score,
        "predicted_label": predicted_label,
        "decision_threshold": bundle.decision_threshold,
        "model_version": bundle.model_version,
        "processed_time": processed_at.isoformat(),
        "processing_latency_ms": round(max(0.0, (processed_at - producer_at).total_seconds() * 1000), 3),
        "prediction_correct": predicted_label == int(event["actual_label"]),
    }
    validate_scored_event(scored)
    return scored
