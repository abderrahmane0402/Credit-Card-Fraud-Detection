from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from dotenv import load_dotenv
from psycopg2.extras import Json

from src.consumer.scoring import FraudScorer

LOG = logging.getLogger("fraud-consumer")
RUNNING = True


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def stop_handler(signum, frame) -> None:
    global RUNNING
    LOG.info("Shutdown requested")
    RUNNING = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score Kafka transactions and write predictions to PostgreSQL")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--input-topic", default=os.getenv("KAFKA_INPUT_TOPIC", "transactions.raw"))
    parser.add_argument("--output-topic", default=os.getenv("KAFKA_OUTPUT_TOPIC", "transactions.scored"))
    parser.add_argument("--dead-letter-topic", default=os.getenv("KAFKA_DEAD_LETTER_TOPIC", "transactions.dead-letter"))
    parser.add_argument("--group-id", default=os.getenv("KAFKA_CONSUMER_GROUP", "fraud-scoring-v1"))
    parser.add_argument("--model-dir", default="models")
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--reset-offset", choices=["earliest", "latest"], default="earliest")
    return parser.parse_args()


def postgres_config() -> dict:
    return {
        "dbname": os.getenv("POSTGRES_DB", "fraud_db"),
        "user": os.getenv("POSTGRES_USER", "fraud_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "fraud_dev_password"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
    }


def publish_json(producer: Producer, topic: str, key: str | None, payload: dict) -> None:
    producer.produce(
        topic,
        key=key.encode("utf-8") if key else None,
        value=json.dumps(payload, separators=(",", ":"), allow_nan=False).encode("utf-8"),
    )
    producer.poll(0)


def write_prediction(connection, event: dict, result: dict, processed_at: datetime) -> None:
    features = event["features"]
    producer_time = datetime.fromisoformat(event["producer_time"].replace("Z", "+00:00"))
    latency_ms = (processed_at - producer_time).total_seconds() * 1000

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO transactions (
                transaction_id, event_time, producer_time, source, schema_version,
                amount, elapsed_time, features, actual_label
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING
            """,
            (
                event["transaction_id"], event["event_time"], event["producer_time"],
                event["source"], event["schema_version"], float(features["Amount"]),
                float(features["Time"]), Json(features), int(event["actual_label"]),
            ),
        )
        cursor.execute(
            """
            INSERT INTO predictions (
                transaction_id, model_version, fraud_score, decision_threshold,
                predicted_label, processed_at, processing_latency_ms
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id, model_version) DO NOTHING
            """,
            (
                event["transaction_id"], result["model_version"], result["fraud_score"],
                result["decision_threshold"], result["predicted_label"], processed_at,
                latency_ms,
            ),
        )
    connection.commit()


def write_quality_event(connection, message, reason: str, raw_payload: str | None) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO data_quality_events (
                kafka_topic, kafka_partition, kafka_offset, reason, raw_payload
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (message.topic(), message.partition(), message.offset(), reason, raw_payload),
        )
    connection.commit()


def run(args: argparse.Namespace) -> int:
    scorer = FraudScorer(args.model_dir)
    connection = psycopg2.connect(**postgres_config())
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": args.group_id,
        "auto.offset.reset": args.reset_offset,
        "enable.auto.commit": False,
    })
    producer = Producer({
        "bootstrap.servers": args.bootstrap_servers,
        "client.id": "fraud-scoring-output",
        "acks": "all",
        "enable.idempotence": True,
    })

    consumer.subscribe([args.input_topic])
    processed = 0
    LOG.info("Consumer started: topic=%s group=%s model=%s threshold=%.10f",
             args.input_topic, args.group_id, scorer.model_version, scorer.threshold)

    try:
        while RUNNING and (args.max_messages is None or processed < args.max_messages):
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            raw_payload = None
            try:
                raw_payload = message.value().decode("utf-8")
                event = json.loads(raw_payload)
                result = scorer.score(event)
                processed_at = utc_now()
                write_prediction(connection, event, result, processed_at)

                scored_event = {
                    **event,
                    **result,
                    "processed_time": processed_at.isoformat(),
                }
                publish_json(producer, args.output_topic, event["transaction_id"], scored_event)
                producer.flush(10)
                consumer.commit(message=message, asynchronous=False)
                processed += 1
                if processed % 10 == 0 or result["predicted_label"] == 1:
                    LOG.info("Processed=%d transaction=%s score=%.6f predicted=%d actual=%d",
                             processed, event["transaction_id"], result["fraud_score"],
                             result["predicted_label"], event["actual_label"])
            except Exception as exc:
                LOG.exception("Invalid or failed message at offset %s", message.offset())
                connection.rollback()
                reason = f"{type(exc).__name__}: {exc}"
                try:
                    write_quality_event(connection, message, reason, raw_payload)
                    publish_json(producer, args.dead_letter_topic, None, {
                        "reason": reason,
                        "source_topic": message.topic(),
                        "source_partition": message.partition(),
                        "source_offset": message.offset(),
                        "raw_payload": raw_payload,
                        "failed_at": utc_now().isoformat(),
                    })
                    producer.flush(10)
                    consumer.commit(message=message, asynchronous=False)
                except Exception:
                    connection.rollback()
                    raise
    finally:
        producer.flush(10)
        consumer.close()
        connection.close()
        LOG.info("Consumer stopped after %d successful messages", processed)
    return processed


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    signal.signal(signal.SIGINT, stop_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, stop_handler)
    run(parse_args())


if __name__ == "__main__":
    main()
