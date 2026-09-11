from __future__ import annotations

import argparse
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from confluent_kafka import Producer
from dotenv import load_dotenv

from src.producer.schema import FEATURE_COLUMNS, REQUIRED_COLUMNS, SCHEMA_VERSION, validate_source_row

LOG = logging.getLogger("fraud-producer")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def row_to_event(row: dict[str, Any], sequence_number: int) -> dict[str, Any]:
    validate_source_row(row)
    features = {name: float(row[name]) for name in FEATURE_COLUMNS}
    return {
        "transaction_id": str(uuid.uuid4()),
        "event_time": utc_now(),
        "producer_time": utc_now(),
        "source": "kaggle_replay",
        "schema_version": SCHEMA_VERSION,
        "sequence_number": sequence_number,
        "actual_label": int(row["Class"]),
        "features": features,
    }


def delivery_report(error, message) -> None:
    if error is not None:
        LOG.error("Delivery failed: %s", error)
    else:
        LOG.debug(
            "Delivered topic=%s partition=%s offset=%s",
            message.topic(), message.partition(), message.offset()
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay creditcard.csv into Kafka")
    parser.add_argument("--csv", default="data/creditcard.csv")
    parser.add_argument("--topic", default=os.getenv("KAFKA_INPUT_TOPIC", "transactions.raw"))
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--events-per-second", type=float, default=20.0)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--loop", action="store_true")
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
    if args.events_per_second <= 0:
        raise ValueError("--events-per-second must be greater than zero")
    if args.limit is not None and args.limit <= 0:
        raise ValueError("--limit must be greater than zero")

    LOG.info("Loading %s", csv_path)
    df = pd.read_csv(csv_path)
    validate_dataframe(df)
    if args.limit:
        df = df.head(args.limit)
    LOG.info("Loaded %d transactions; fraud rows=%d", len(df), int(df["Class"].sum()))

    producer = None
    if not args.dry_run:
        producer = Producer({
            "bootstrap.servers": args.bootstrap_servers,
            "client.id": "fraud-csv-replay-producer",
            "acks": "all",
            "enable.idempotence": True,
            "retries": 10,
            "compression.type": "snappy",
        })

    delay = 1.0 / args.events_per_second
    total = 0
    started = time.perf_counter()

    while True:
        for sequence_number, (_, series) in enumerate(df.iterrows(), start=1):
            event = row_to_event(series.to_dict(), sequence_number)
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
            if total % args.log_every == 0:
                elapsed = max(time.perf_counter() - started, 1e-9)
                LOG.info("Published %d events (%.2f events/s)", total, total / elapsed)
            time.sleep(delay)

        if not args.loop:
            break
        LOG.info("Replay loop completed; restarting dataset")

    if producer is not None:
        remaining = producer.flush(30)
        if remaining:
            raise RuntimeError(f"Failed to deliver {remaining} Kafka messages")

    elapsed = max(time.perf_counter() - started, 1e-9)
    LOG.info("Finished: %d events in %.2fs (%.2f events/s)", total, elapsed, total / elapsed)
    return total


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run(parse_args())


if __name__ == "__main__":
    main()
