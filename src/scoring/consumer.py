from __future__ import annotations

import argparse
import json
import logging
import os
import signal
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from dotenv import load_dotenv

from src.scoring.inference import score_transaction
from src.scoring.model_loader import load_model_bundle

LOG = logging.getLogger("fraud-scoring-consumer")
RUNNING = True


def stop_consumer(signum: int, frame: Any) -> None:
    global RUNNING
    LOG.info("Shutdown signal received")
    RUNNING = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score raw Kafka transactions")
    parser.add_argument("--input-topic", default=os.getenv("KAFKA_INPUT_TOPIC", "transactions.raw"))
    parser.add_argument("--output-topic", default=os.getenv("KAFKA_OUTPUT_TOPIC", "transactions.scored"))
    parser.add_argument("--dead-letter-topic", default=os.getenv("KAFKA_DEAD_LETTER_TOPIC", "transactions.dead-letter"))
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--group-id", default=os.getenv("KAFKA_SCORING_GROUP_ID", "fraud-scoring-service-v1"))
    parser.add_argument("--models-directory", default="models")
    parser.add_argument("--offset-reset", choices=["earliest", "latest"], default="earliest")
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--log-every", type=int, default=100)
    return parser.parse_args()


def encode_json(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":"), allow_nan=False).encode("utf-8")


def publish_and_wait(producer: Producer, topic: str, key: str, payload: dict[str, Any]) -> None:
    result: dict[str, Any] = {"error": None, "done": False}

    def delivery(error: Any, message: Any) -> None:
        result["error"] = error
        result["done"] = True

    while True:
        try:
            producer.produce(topic, key=key.encode("utf-8"), value=encode_json(payload), on_delivery=delivery)
            break
        except BufferError:
            producer.poll(1)

    while not result["done"]:
        producer.poll(1)
    if result["error"] is not None:
        raise KafkaException(result["error"])


def dead_letter_payload(raw_value: bytes | None, error: Exception, message: Any) -> dict[str, Any]:
    return {
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "error_type": type(error).__name__,
        "error_message": str(error),
        "source_topic": message.topic(),
        "source_partition": message.partition(),
        "source_offset": message.offset(),
        "raw_payload": None if raw_value is None else raw_value.decode("utf-8", errors="replace"),
    }


def run(args: argparse.Namespace) -> int:
    if args.max_messages is not None and args.max_messages <= 0:
        raise ValueError("--max-messages must be greater than zero")

    bundle = load_model_bundle(args.models_directory)
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": args.group_id,
        "auto.offset.reset": args.offset_reset,
        "enable.auto.commit": False,
        "client.id": "fraud-scoring-consumer",
    })
    producer = Producer({
        "bootstrap.servers": args.bootstrap_servers,
        "client.id": "fraud-scoring-output-producer",
        "acks": "all",
        "enable.idempotence": True,
        "retries": 10,
        "compression.type": "snappy",
    })

    processed = 0
    scored_count = 0
    dead_letter_count = 0
    consumer.subscribe([args.input_topic])
    LOG.info(
        "Listening input=%s output=%s group=%s",
        args.input_topic,
        args.output_topic,
        args.group_id,
    )

    try:
        while RUNNING:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            raw_value = message.value()
            try:
                if raw_value is None:
                    raise ValueError("Kafka message value is empty")
                event = json.loads(raw_value.decode("utf-8"))
                scored = score_transaction(event, bundle)
                publish_and_wait(producer, args.output_topic, event["transaction_id"], scored)
                scored_count += 1
                if scored["predicted_label"] == 1:
                    LOG.warning(
                        "FRAUD ALERT transaction=%s score=%.6f actual=%s",
                        scored["transaction_id"], scored["risk_score"], scored["actual_label"]
                    )
            except Exception as exc:
                LOG.exception(
                    "Message failed topic=%s partition=%s offset=%s",
                    message.topic(), message.partition(), message.offset()
                )
                fallback_key = f"{message.topic()}-{message.partition()}-{message.offset()}"
                publish_and_wait(
                    producer,
                    args.dead_letter_topic,
                    fallback_key,
                    dead_letter_payload(raw_value, exc, message),
                )
                dead_letter_count += 1

            # Commit only after the scored or dead-letter event is acknowledged.
            consumer.commit(message=message, asynchronous=False)
            processed += 1
            if processed % args.log_every == 0:
                LOG.info(
                    "Processed=%d scored=%d dead_letter=%d",
                    processed, scored_count, dead_letter_count
                )
            if args.max_messages is not None and processed >= args.max_messages:
                break
    finally:
        producer.flush(30)
        consumer.close()

    LOG.info(
        "Finished processed=%d scored=%d dead_letter=%d",
        processed, scored_count, dead_letter_count
    )
    return processed


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    signal.signal(signal.SIGINT, stop_consumer)
    signal.signal(signal.SIGTERM, stop_consumer)
    run(parse_args())


if __name__ == "__main__":
    main()
