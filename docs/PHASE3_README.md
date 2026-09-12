# Phase 3: Kafka CSV replay producer

Copy `src/`, `tests/`, and `requirements-producer.txt` into the root of your existing Phase 2 project.

## Install

Activate the project's virtual environment, then run:

```bash
python -m pip install -r requirements-producer.txt
```

## Test event construction without Kafka

```bash
python -m src.producer.producer --dry-run --limit 3 --events-per-second 100
```

You should see three JSON events. Each event contains a UUID, timestamps, the true historical label, and all 30 model features.

## Run unit tests

```bash
pytest -q
```

Expected result: `3 passed`.

## Publish a small test batch

Make sure Docker Compose is running, then execute:

```bash
python -m src.producer.producer --limit 100 --events-per-second 20
```

Expected final log: `Finished: 100 events`.

## Verify Kafka received the events

Open Kafka UI at `http://localhost:8080`, select `transactions.raw`, and inspect its messages.

Alternatively, use the console consumer:

```bash
docker exec fraud-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions.raw --from-beginning --max-messages 3
```

## Larger replay

```bash
python -m src.producer.producer --limit 10000 --events-per-second 100
```

Do not replay the complete dataset yet. First verify that the small batch is valid. Phase 4 will add the Spark consumer and model inference.
