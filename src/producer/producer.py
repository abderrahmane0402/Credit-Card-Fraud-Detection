from __future__ import annotations

import argparse
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from confluent_kafka import Producer
from dotenv import load_dotenv

from src.producer.schema import FEATURE_COLUMNS, REQUIRED_COLUMNS, SCHEMA_VERSION, validate_source_row
from src.common.constants import (
    TOPIC_RAW, REPLAY_DEFAULT_EPS, REPLAY_DEFAULT_LIMIT, REPLAY_DEFAULT_LOOP,
    REPLAY_DEFAULT_MODE, REPLAY_DEFAULT_RUN_ID
)

LOG = logging.getLogger("fraud-producer")

# Namespace for UUID5 generation
NAMESPACE_FRAUD = uuid.uuid5(uuid.NAMESPACE_URL, "http://fraud-detection.local")


def generate_deterministic_id(run_id: str, source_row_index: int) -> str:
    name = f"creditcard-{run_id}-{source_row_index}"
    return str(uuid.uuid5(NAMESPACE_FRAUD, name))


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def row_to_event(row: dict[str, Any], sequence_number: int, source_row_index: int, run_id: str, mode: str) -> dict[str, Any]:
    validate_source_row(row)
    features = {name: float(row[name]) for name in FEATURE_COLUMNS}
    return {
        "transaction_id": generate_deterministic_id(run_id, source_row_index),
        "run_id": run_id,
        "source_row_index": source_row_index,
        "sequence_number": sequence_number,
        "event_time": utc_now(),
        "producer_time": utc_now(),
        "source": "kaggle_replay",
        "replay_mode": mode,
        "schema_version": SCHEMA_VERSION,
        "actual_label": int(row["Class"]),
        "features": features,
    }


def delivery_report(error, message) -> None:
    if error is not None:
        LOG.error("Delivery failed: %s", error)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay creditcard.csv into Kafka")
    parser.add_argument("--csv", default="data/creditcard.csv")
    parser.add_argument("--topic", default=os.getenv("KAFKA_INPUT_TOPIC", TOPIC_RAW))
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--events-per-second", type=float, default=REPLAY_DEFAULT_EPS)
    parser.add_argument("--limit", type=int, default=REPLAY_DEFAULT_LIMIT)
    parser.add_argument("--loop", action="store_true", default=REPLAY_DEFAULT_LOOP)
    parser.add_argument("--start-row", type=int, default=0)
    parser.add_argument("--end-row", type=int, default=None)
    parser.add_argument("--shuffle", action="store_true")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--run-id", default=REPLAY_DEFAULT_RUN_ID)
    parser.add_argument("--mode", choices=["chronological", "shuffle", "demo"], default=REPLAY_DEFAULT_MODE)
    parser.add_argument("--fraud-interval", type=int, default=100)
    parser.add_argument("--fraud-ratio", type=float, default=0.01)
    parser.add_argument("--variable-rate", action="store_true")
    parser.add_argument("--min-events-per-second", type=float, default=1.0)
    parser.add_argument("--max-events-per-second", type=float, default=50.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-every", type=int, default=100)
    return parser.parse_args()


def validate_dataframe(df: pd.DataFrame) -> None:
    missing = [name for name in REQUIRED_COLUMNS if name not in df.columns]
    if missing:
        raise ValueError(f"CSV is missing required columns: {missing}")
    if df[REQUIRED_COLUMNS].isna().any().any():
        bad_columns = df[REQUIRED_COLUMNS].columns[df[REQUIRED_COLUMNS].isna().any()].tolist()
        raise ValueError(f"CSV contains missing values in: {bad_columns}")


def run(args: argparse.Namespace) -> int:
    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path.resolve()}")

    LOG.info("Loading %s", csv_path)
    df = pd.read_csv(csv_path)
    validate_dataframe(df)
    
    # Track original index
    df["_source_index"] = df.index
    
    # Apply row limits
    if args.end_row:
        df = df.iloc[:args.end_row]
    if args.start_row:
        df = df.iloc[args.start_row:]

    # Modes
    if args.mode == "shuffle" or args.shuffle:
        df = df.sample(frac=1, random_state=args.seed).reset_index(drop=True)
    elif args.mode == "demo":
        # In demo mode, we interleave occasional real fraud rows so a short presentation displays alerts.
        # Ensure we just keep a separate pool of fraud and normal rows to yield.
        pass

    if args.limit and args.mode != "demo":
        df = df.head(args.limit)

    LOG.info("Loaded %d transactions for run_id=%s", len(df), args.run_id)

    producer = None
    if not args.dry_run:
        producer = Producer({
            "bootstrap.servers": args.bootstrap_servers,
            "client.id": f"fraud-csv-replay-{args.run_id}",
            "acks": "all",
            "enable.idempotence": True,
            "retries": 10,
            "compression.type": "snappy",
        })

    total = 0
    fraud_published = 0
    started = time.perf_counter()
    sequence_number = 1
    
    normal_df = df[df["Class"] == 0] if args.mode == "demo" else df
    fraud_df = df[df["Class"] == 1] if args.mode == "demo" else None
    
    normal_iterator = normal_df.iterrows()
    fraud_iterator = fraud_df.iterrows() if args.mode == "demo" else None

    while True:
        # Determine the row to send
        row_series = None
        
        if args.mode == "demo":
            if total > 0 and total % args.fraud_interval == 0:
                try:
                    _, row_series = next(fraud_iterator)
                except StopIteration:
                    fraud_iterator = fraud_df.iterrows()
                    _, row_series = next(fraud_iterator)
            else:
                try:
                    _, row_series = next(normal_iterator)
                except StopIteration:
                    if not args.loop:
                        break
                    normal_iterator = normal_df.iterrows()
                    _, row_series = next(normal_iterator)
        else:
            try:
                _, row_series = next(normal_iterator)
            except StopIteration:
                if not args.loop:
                    break
                LOG.info("Replay loop completed; restarting dataset")
                normal_iterator = df.iterrows()
                _, row_series = next(normal_iterator)
                
        if row_series is None:
            break

        source_idx = int(row_series["_source_index"])
        event = row_to_event(row_series.to_dict(), sequence_number, source_idx, args.run_id, args.mode)
        payload = json.dumps(event, separators=(",", ":"), allow_nan=False).encode("utf-8")

        if args.dry_run:
            if total < 3:
                print(json.dumps(event, indent=2))
        else:
            while True:
                try:
                    producer.produce(
                        args.topic,
                        key=event["transaction_id"].encode("utf-8"),
                        value=payload,
                        on_delivery=delivery_report,
                    )
                    producer.poll(0)
                    break
                except BufferError:
                    LOG.warning("Producer queue full; waiting")
                    producer.poll(1)

        total += 1
        sequence_number += 1
        if event["actual_label"] == 1:
            fraud_published += 1

        if total % args.log_every == 0:
            elapsed = max(time.perf_counter() - started, 1e-9)
            LOG.info("Published %d events, %d fraud (%.2f events/s)", total, fraud_published, total / elapsed)
            
        if args.limit and total >= args.limit:
            break

        # Calculate delay
        if args.variable_rate:
            eps = random.uniform(args.min_events_per_second, args.max_events_per_second)
            delay = 1.0 / eps
        else:
            delay = 1.0 / args.events_per_second
        time.sleep(delay)

    if producer is not None:
        remaining = producer.flush(30)
        if remaining:
            raise RuntimeError(f"Failed to deliver {remaining} Kafka messages")

    elapsed = max(time.perf_counter() - started, 1e-9)
    LOG.info("Finished run_id=%s mode=%s: %d events in %.2fs (%.2f events/s)", 
             args.run_id, args.mode, total, elapsed, total / elapsed)
    return total


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run(parse_args())


if __name__ == "__main__":
    main()
