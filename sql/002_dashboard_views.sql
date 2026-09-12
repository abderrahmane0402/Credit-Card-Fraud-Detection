CREATE OR REPLACE VIEW dashboard_summary AS
SELECT
    COUNT(*) as total_scored,
    SUM(predicted_label) as total_alerts,
    COALESCE(SUM(predicted_label)::FLOAT / NULLIF(COUNT(*), 0), 0) as alert_rate,
    COALESCE(AVG(processing_latency_ms), 0) as avg_latency,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY processing_latency_ms) as p95_latency,
    MAX(processed_at) as last_processed_at
FROM predictions
WHERE processed_at >= NOW() - INTERVAL '1 hour';

CREATE OR REPLACE VIEW dashboard_minute_metrics AS
SELECT
    date_trunc('minute', p.processed_at) as minute,
    p.processor,
    COUNT(*) as transactions,
    SUM(p.predicted_label) as alerts,
    AVG(p.processing_latency_ms) as avg_latency
FROM predictions p
WHERE p.processed_at >= NOW() - INTERVAL '60 minutes'
GROUP BY 1, 2
ORDER BY 1 DESC;

CREATE OR REPLACE VIEW dashboard_recent_alerts AS
SELECT
    t.transaction_id,
    t.event_time,
    t.amount,
    t.actual_label,
    p.fraud_score,
    p.predicted_label,
    p.processor,
    p.model_version,
    p.processing_latency_ms,
    t.features
FROM transactions t
JOIN predictions p ON t.transaction_id = p.transaction_id
WHERE p.predicted_label = 1
ORDER BY p.processed_at DESC
LIMIT 100;

CREATE OR REPLACE VIEW dashboard_performance AS
SELECT
    model_version,
    processor,
    COUNT(*) as total_predictions,
    SUM(CASE WHEN predicted_label = 1 AND actual_label = 1 THEN 1 ELSE 0 END) as true_positives,
    SUM(CASE WHEN predicted_label = 1 AND actual_label = 0 THEN 1 ELSE 0 END) as false_positives,
    SUM(CASE WHEN predicted_label = 0 AND actual_label = 1 THEN 1 ELSE 0 END) as false_negatives,
    SUM(CASE WHEN predicted_label = 0 AND actual_label = 0 THEN 1 ELSE 0 END) as true_negatives
FROM predictions p
JOIN transactions t ON p.transaction_id = t.transaction_id
WHERE p.processed_at >= NOW() - INTERVAL '24 hours'
GROUP BY 1, 2;

CREATE OR REPLACE VIEW dashboard_processor_health AS
SELECT
    processor_type,
    service_name,
    instance_id,
    status,
    last_seen_at
FROM service_heartbeats
ORDER BY last_seen_at DESC;

CREATE OR REPLACE VIEW dashboard_data_quality AS
SELECT
    kafka_topic,
    reason,
    COUNT(*) as error_count,
    MAX(created_at) as last_error_at
FROM data_quality_events
WHERE created_at >= NOW() - INTERVAL '24 hours'
GROUP BY 1, 2
ORDER BY 3 DESC;
