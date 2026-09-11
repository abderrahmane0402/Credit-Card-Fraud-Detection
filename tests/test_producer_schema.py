import pytest

from src.producer.producer import row_to_event
from src.producer.schema import FEATURE_COLUMNS


def valid_row():
    row = {name: 0.0 for name in FEATURE_COLUMNS}
    row["Amount"] = 12.5
    row["Class"] = 1
    return row


def test_event_contract():
    event = row_to_event(valid_row(), 1)
    assert event["schema_version"] == "1.0"
    assert event["actual_label"] == 1
    assert event["features"]["Amount"] == 12.5
    assert set(event["features"]) == set(FEATURE_COLUMNS)


def test_negative_amount_rejected():
    row = valid_row()
    row["Amount"] = -1
    with pytest.raises(ValueError, match="negative"):
        row_to_event(row, 1)


def test_missing_feature_rejected():
    row = valid_row()
    del row["V10"]
    with pytest.raises(ValueError, match="Missing"):
        row_to_event(row, 1)
