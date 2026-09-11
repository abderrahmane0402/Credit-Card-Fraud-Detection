from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv(override=True)


def connection_config() -> dict[str, object]:
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5433")),
        "dbname": os.getenv("POSTGRES_DB", "fraud_db"),
        "user": os.getenv("POSTGRES_USER", "fraud_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "fraud_dev_password"),
        "connect_timeout": 5,
    }


@contextmanager
def database_connection() -> Iterator[psycopg2.extensions.connection]:
    connection = psycopg2.connect(**connection_config())
    try:
        yield connection
    finally:
        connection.close()


def read_dataframe(query: str, parameters: tuple | None = None) -> pd.DataFrame:
    with database_connection() as connection:
        return pd.read_sql_query(query, connection, params=parameters)


def fetch_summary() -> dict[str, float | int | None]:
    query = """
        SELECT
            COUNT(*)::bigint AS total_predictions,
            COUNT(*) FILTER (WHERE predicted_label = 1)::bigint AS fraud_alerts,
            COALESCE(AVG(fraud_score), 0)::double precision AS avg_risk_score,
            COALESCE(AVG(processing_latency_ms), 0)::double precision AS avg_latency_ms,
            COALESCE(
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY processing_latency_ms),
                0
            )::double precision AS p95_latency_ms,
            MAX(processed_at) AS last_processed_at
        FROM predictions
    """
    with database_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
    return {
        "total_predictions": int(row[0] or 0),
        "fraud_alerts": int(row[1] or 0),
        "avg_risk_score": float(row[2] or 0),
        "avg_latency_ms": float(row[3] or 0),
        "p95_latency_ms": float(row[4] or 0),
        "last_processed_at": row[5],
    }


def fetch_performance() -> dict[str, float | int]:
    query = """
        SELECT
            COUNT(*) FILTER (
                WHERE p.predicted_label = 1 AND t.actual_label = 1
            )::bigint AS true_positives,
            COUNT(*) FILTER (
                WHERE p.predicted_label = 1 AND t.actual_label = 0
            )::bigint AS false_positives,
            COUNT(*) FILTER (
                WHERE p.predicted_label = 0 AND t.actual_label = 1
            )::bigint AS false_negatives,
            COUNT(*) FILTER (
                WHERE p.predicted_label = 0 AND t.actual_label = 0
            )::bigint AS true_negatives
        FROM predictions p
        JOIN transactions t USING (transaction_id)
        WHERE t.actual_label IS NOT NULL
    """
    with database_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            tp, fp, fn, tn = [int(value or 0) for value in cursor.fetchone()]

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "true_positives": tp,
        "false_positives": fp,
        "false_negatives": fn,
        "true_negatives": tn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def fetch_recent_predictions(limit: int, minimum_score: float, labels: tuple[int, ...]) -> pd.DataFrame:
    query = """
        SELECT
            p.transaction_id::text,
            p.processed_at,
            t.amount,
            t.actual_label,
            p.predicted_label,
            p.fraud_score,
            p.decision_threshold,
            p.model_version,
            p.processing_latency_ms
        FROM predictions p
        JOIN transactions t USING (transaction_id)
        WHERE p.fraud_score >= %s
          AND p.predicted_label = ANY(%s)
        ORDER BY p.processed_at DESC
        LIMIT %s
    """
    return read_dataframe(query, (minimum_score, list(labels), limit))


def fetch_timeseries(minutes: int) -> pd.DataFrame:
    query = """
        SELECT
            DATE_TRUNC('minute', p.processed_at) AS minute,
            COUNT(*)::bigint AS transactions,
            COUNT(*) FILTER (WHERE p.predicted_label = 1)::bigint AS fraud_alerts,
            AVG(p.fraud_score)::double precision AS average_risk,
            AVG(p.processing_latency_ms)::double precision AS average_latency_ms
        FROM predictions p
        WHERE p.processed_at >= NOW() - (%s * INTERVAL '1 minute')
        GROUP BY 1
        ORDER BY 1
    """
    return read_dataframe(query, (minutes,))


def fetch_risk_distribution() -> pd.DataFrame:
    query = """
        SELECT
            width_bucket(fraud_score, 0.0, 1.0, 20) AS bucket,
            COUNT(*)::bigint AS transactions
        FROM predictions
        GROUP BY 1
        ORDER BY 1
    """
    frame = read_dataframe(query)
    if not frame.empty:
        frame["risk_range"] = frame["bucket"].apply(
            lambda value: f"{max(0, value - 1) * 0.05:.2f}-{min(value * 0.05, 1):.2f}"
        )
    return frame
