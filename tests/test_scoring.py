from datetime import datetime, timezone

import numpy as np
import pytest

from src.producer.schema import FEATURE_COLUMNS
from src.scoring.inference import score_transaction
from src.scoring.model_loader import ModelBundle
from src.scoring.schema import validate_transaction_event


class FakePipeline:
    def predict_proba(self, frame):
        assert list(frame.columns) == FEATURE_COLUMNS
        return np.array([[0.03, 0.97]])


def valid_event():
    return {
        "transaction_id": "test-transaction-1",
        "run_id": "test-run",
        "source_row_index": 0,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "producer_time": datetime.now(timezone.utc).isoformat(),
        "source": "test",
        "replay_mode": "demo",
        "schema_version": "1.1",
        "sequence_number": 1,
        "actual_label": 1,
        "features": {name: (10.0 if name == "Amount" else 0.0) for name in FEATURE_COLUMNS},
    }


def bundle():
    return ModelBundle(FakePipeline(), FEATURE_COLUMNS, 0.9152204990386963, "1.0.0", {})


def test_valid_event_scores_as_fraud():
    scored = score_transaction(valid_event(), bundle())
    assert scored["risk_score"] == pytest.approx(0.97)
    assert scored["predicted_label"] == 1
    assert scored["prediction_correct"] is True


def test_missing_feature_is_rejected():
    event = valid_event()
    del event["features"]["V12"]
    with pytest.raises(ValueError, match="missing"):
        validate_transaction_event(event)


def test_wrong_schema_version_is_rejected():
    event = valid_event()
    event["schema_version"] = "99.0"
    with pytest.raises(ValueError, match="Unsupported"):
        validate_transaction_event(event)
