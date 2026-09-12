from pathlib import Path

# Topics
TOPIC_RAW = "transactions.raw"
TOPIC_SCORED = "transactions.scored"
TOPIC_DEAD_LETTER = "transactions.dead-letter"

# Models
MODELS_DIR = Path("models")
MODEL_PIPELINE_PATH = MODELS_DIR / "fraud_pipeline.joblib"
MODEL_METADATA_PATH = MODELS_DIR / "model_metadata.json"
MODEL_THRESHOLD_PATH = MODELS_DIR / "threshold.json"
MODEL_SCHEMA_PATH = MODELS_DIR / "feature_schema.json"

# Application versions
SCHEMA_VERSION = "1.1"

# Spark
SPARK_CHECKPOINT_LOCATION = "/checkpoints/fraud-scoring-spark"

# Replay Defaults
REPLAY_DEFAULT_EPS = 20
REPLAY_DEFAULT_LIMIT = 10000
REPLAY_DEFAULT_LOOP = True
REPLAY_DEFAULT_MODE = "demo"
REPLAY_DEFAULT_RUN_ID = "default"
