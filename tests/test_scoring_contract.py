from pathlib import Path
import pandas as pd

from src.scoring.model_loader import load_model_bundle
from src.scoring.inference import score_transaction

def test_exported_model_contract():
    model_dir = Path("models")
    required = ["fraud_pipeline.joblib", "feature_schema.json", "threshold.json", "model_metadata.json"]
    if not all((model_dir / name).exists() for name in required):
        import pytest
        pytest.skip("Kaggle model artifacts not present")

    bundle = load_model_bundle(model_dir)
    samples = pd.read_csv(model_dir / "test_samples.csv")
    row = samples.iloc[0]
    event = {
        "transaction_id": "00000000-0000-0000-0000-000000000001",
        "run_id": "test-run",
        "source_row_index": 0,
        "sequence_number": 1,
        "event_time": "2026-01-01T00:00:00+00:00",
        "producer_time": "2026-01-01T00:00:00+00:00",
        "source": "test",
        "replay_mode": "demo",
        "schema_version": "1.1",
        "actual_label": int(row["actual_label"]),
        "features": {name: float(row[name]) for name in bundle.feature_names},
    }
    result = score_transaction(event, bundle)
    assert 0 <= result["risk_score"] <= 1
    assert result["predicted_label"] in (0, 1)
    assert result["model_version"]
