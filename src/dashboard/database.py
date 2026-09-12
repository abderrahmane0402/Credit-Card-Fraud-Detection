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
    query = "SELECT * FROM dashboard_summary"
    with database_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
    if row is None:
        return {
            "total_predictions": 0,
            "fraud_alerts": 0,
            "alert_rate": 0.0,
            "avg_latency_ms": 0.0,
            "p95_latency_ms": 0.0,
            "last_processed_at": None,
        }
    return {
        "total_predictions": int(row[0] or 0),
        "fraud_alerts": int(row[1] or 0),
        "alert_rate": float(row[2] or 0),
        "avg_latency_ms": float(row[3] or 0),
        "p95_latency_ms": float(row[4] or 0),
        "last_processed_at": row[5],
    }


def fetch_performance() -> pd.DataFrame:
    query = "SELECT * FROM dashboard_performance"
    return read_dataframe(query)


def fetch_recent_predictions(limit: int, minimum_score: float, labels: tuple[int, ...]) -> pd.DataFrame:
    query = """
        SELECT
            t.transaction_id::text,
            t.event_time,
            t.amount,
            t.actual_label,
            p.fraud_score,
            p.predicted_label,
            p.processor,
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
        SELECT *
        FROM dashboard_minute_metrics
        WHERE minute >= NOW() - (%s * INTERVAL '1 minute')
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

def fetch_data_quality() -> pd.DataFrame:
    query = "SELECT * FROM dashboard_data_quality LIMIT 50"
    return read_dataframe(query)

def fetch_processor_health() -> pd.DataFrame:
    query = "SELECT * FROM dashboard_processor_health LIMIT 10"
    return read_dataframe(query)
