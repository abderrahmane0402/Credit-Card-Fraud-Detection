from __future__ import annotations

from typing import Any

FEATURE_COLUMNS = ["Time"] + [f"V{i}" for i in range(1, 29)] + ["Amount"]
REQUIRED_COLUMNS = FEATURE_COLUMNS + ["Class"]
from src.common.constants import SCHEMA_VERSION


def validate_source_row(row: dict[str, Any]) -> None:
    missing = [name for name in REQUIRED_COLUMNS if name not in row]
    if missing:
        raise ValueError(f"Missing source columns: {missing}")

    for name in FEATURE_COLUMNS:
        try:
            value = float(row[name])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Feature {name} must be numeric") from exc
        if value != value:  # NaN
            raise ValueError(f"Feature {name} cannot be NaN")

    if float(row["Amount"]) < 0:
        raise ValueError("Amount cannot be negative")

    actual_label = int(row["Class"])
    if actual_label not in (0, 1):
        raise ValueError("Class must be 0 or 1")
