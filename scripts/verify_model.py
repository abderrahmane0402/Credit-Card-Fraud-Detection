from pathlib import Path
import json
import platform
import sys

import joblib
import numpy as np
import pandas as pd
import sklearn
import xgboost

ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = ROOT / "models"
required = [
    "fraud_pipeline.joblib", "threshold.json", "feature_schema.json",
    "model_metadata.json", "metrics.json", "test_samples.csv"
]
missing = [name for name in required if not (MODEL_DIR / name).exists()]
if missing:
    raise FileNotFoundError(f"Missing model artifacts in {MODEL_DIR}: {missing}")

with open(MODEL_DIR / "feature_schema.json", encoding="utf-8") as f:
    schema = json.load(f)
with open(MODEL_DIR / "threshold.json", encoding="utf-8") as f:
    threshold_data = json.load(f)
with open(MODEL_DIR / "model_metadata.json", encoding="utf-8") as f:
    metadata = json.load(f)

feature_names = [item["name"] for item in schema["features"]]
samples = pd.read_csv(MODEL_DIR / "test_samples.csv")
missing_features = sorted(set(feature_names) - set(samples.columns))
if missing_features:
    raise ValueError(f"Test samples are missing features: {missing_features}")

model = joblib.load(MODEL_DIR / "fraud_pipeline.joblib")
actual = model.predict_proba(samples[feature_names])[:, 1]
expected = samples["expected_probability"].to_numpy()
np.testing.assert_allclose(actual, expected, rtol=1e-6, atol=1e-8)

threshold = float(threshold_data["decision_threshold"])
predictions = (actual >= threshold).astype(int)
if "expected_prediction" in samples:
    np.testing.assert_array_equal(predictions, samples["expected_prediction"].astype(int).to_numpy())

print("MODEL VERIFICATION PASSED")
print(f"Rows checked: {len(samples)}")
print(f"Decision threshold: {threshold:.10f}")
print(f"Score range: {actual.min():.6f} to {actual.max():.6f}")
print("Runtime versions:")
print(f"  Python: {platform.python_version()} (trained: {metadata.get('python_version')})")
print(f"  scikit-learn: {sklearn.__version__} (trained: {metadata.get('scikit_learn_version')})")
print(f"  XGBoost: {xgboost.__version__} (trained: {metadata.get('xgboost_version')})")
