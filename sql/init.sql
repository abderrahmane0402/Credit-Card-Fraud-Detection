CREATE TABLE IF NOT EXISTS model_registry (
    model_version VARCHAR(50) PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    decision_threshold DOUBLE PRECISION NOT NULL CHECK (decision_threshold BETWEEN 0 AND 1),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id UUID PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    producer_time TIMESTAMPTZ NOT NULL,
    source VARCHAR(50) NOT NULL DEFAULT 'kaggle_replay',
    schema_version VARCHAR(20) NOT NULL DEFAULT '1.0',
    amount DOUBLE PRECISION NOT NULL CHECK (amount >= 0),
    elapsed_time DOUBLE PRECISION NOT NULL,
    features JSONB NOT NULL,
    actual_label SMALLINT CHECK (actual_label IN (0, 1)),
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS predictions (
    prediction_id BIGSERIAL PRIMARY KEY,
    transaction_id UUID NOT NULL REFERENCES transactions(transaction_id) ON DELETE CASCADE,
    model_version VARCHAR(50) NOT NULL,
    fraud_score DOUBLE PRECISION NOT NULL CHECK (fraud_score BETWEEN 0 AND 1),
    decision_threshold DOUBLE PRECISION NOT NULL CHECK (decision_threshold BETWEEN 0 AND 1),
    predicted_label SMALLINT NOT NULL CHECK (predicted_label IN (0, 1)),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processing_latency_ms DOUBLE PRECISION,
    stream_batch_id BIGINT,
    CONSTRAINT uq_prediction_transaction_model UNIQUE (transaction_id, model_version)
);

CREATE TABLE IF NOT EXISTS stream_batches (
    batch_id BIGINT PRIMARY KEY,
    status VARCHAR(20) NOT NULL CHECK (status IN ('started', 'completed', 'failed')),
    input_count INTEGER NOT NULL DEFAULT 0,
    output_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS data_quality_events (
    event_id BIGSERIAL PRIMARY KEY,
    transaction_id UUID,
    kafka_topic VARCHAR(200),
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    reason TEXT NOT NULL,
    raw_payload TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transactions_event_time ON transactions(event_time DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_processed_at ON predictions(processed_at DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_label_score ON predictions(predicted_label, fraud_score DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_model_version ON predictions(model_version);
