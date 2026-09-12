CREATE TABLE IF NOT EXISTS transactions (
    transaction_id UUID PRIMARY KEY,
    run_id VARCHAR(100),
    source_row_index INTEGER,
    sequence_number INTEGER,
    event_time TIMESTAMPTZ NOT NULL,
    producer_time TIMESTAMPTZ NOT NULL,
    source VARCHAR(50) NOT NULL,
    replay_mode VARCHAR(50),
    schema_version VARCHAR(20) NOT NULL,
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
    processor VARCHAR(50) NOT NULL,
    fraud_score DOUBLE PRECISION NOT NULL CHECK (fraud_score BETWEEN 0 AND 1),
    decision_threshold DOUBLE PRECISION NOT NULL CHECK (decision_threshold BETWEEN 0 AND 1),
    predicted_label SMALLINT NOT NULL CHECK (predicted_label IN (0, 1)),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processing_latency_ms DOUBLE PRECISION,
    stream_batch_id BIGINT,
    prediction_correct BOOLEAN,
    CONSTRAINT uq_prediction_transaction_model_processor UNIQUE (transaction_id, model_version, processor)
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

CREATE TABLE IF NOT EXISTS service_heartbeats (
    service_name VARCHAR(100) NOT NULL,
    instance_id VARCHAR(100) NOT NULL,
    processor_type VARCHAR(50),
    status VARCHAR(50) NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    details JSONB,
    PRIMARY KEY (service_name, instance_id)
);

CREATE INDEX IF NOT EXISTS idx_transactions_event_time ON transactions(event_time DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_run_id ON transactions(run_id);
CREATE INDEX IF NOT EXISTS idx_transactions_actual_label ON transactions(actual_label);
CREATE INDEX IF NOT EXISTS idx_predictions_processed_at ON predictions(processed_at DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_label_score ON predictions(predicted_label, fraud_score DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_model_version ON predictions(model_version);
CREATE INDEX IF NOT EXISTS idx_predictions_processor ON predictions(processor);
CREATE INDEX IF NOT EXISTS idx_stream_batches_started_at ON stream_batches(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_data_quality_events_created_at ON data_quality_events(created_at DESC);
