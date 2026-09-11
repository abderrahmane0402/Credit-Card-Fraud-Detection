from pathlib import Path
import pandas as pd

from src.consumer.scoring import FraudScorer


def test_exported_model_contract():
    model_dir = Path("models")
    required = ["fraud_pipeline.joblib", "feature_schema.json", "threshold.json", "model_metadata.json"]
    if not all((model_dir / name).exists() for name in required):
        import pytest
        pytest.skip("Kaggle model artifacts not present")

    scorer = FraudScorer(model_dir)
    samples = pd.read_csv(model_dir / "test_samples.csv")
    row = samples.iloc[0]
    event = {
        "transaction_id": "00000000-0000-0000-0000-000000000001",
        "event_time": "2026-01-01T00:00:00+00:00",
        "producer_time": "2026-01-01T00:00:00+00:00",
        "source": "test",
        "schema_version": "1.0",
        "actual_label": int(row["actual_label"]),
        "features": {name: float(row[name]) for name in scorer.feature_names},
    }
    result = scorer.score(event)
    assert 0 <= result["fraud_score"] <= 1
    assert result["predicted_label"] in (0, 1)
    assert result["model_version"]
