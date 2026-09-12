# Phase 4: Model scoring consumer

This phase adds a reliable Python integration consumer before the PySpark optimization phase. It proves the complete contract first:

`transactions.raw -> model inference -> PostgreSQL + transactions.scored`

Invalid messages are written to PostgreSQL and `transactions.dead-letter`.

## Install

Copy this ZIP's contents into the existing project root, activate `.venv`, then run:

```powershell
python -m pip install -r requirements-consumer.txt
```

If model loading reports a package-version mismatch, use the versions listed in `models/model_metadata.json`.

## Important offset behavior

Kafka remembers offsets by consumer group. Use a new group name when you want to replay old messages for a fresh test.

## First end-to-end test

Keep Docker Compose running. If 100 messages already exist in `transactions.raw`, run:

```powershell
python -m src.consumer.consumer --max-messages 100 --group-id fraud-scoring-test-1
```

If the topic is empty, publish a test batch in another terminal:

```powershell
python -m src.producer.producer --limit 100 --events-per-second 20
```

The consumer exits after 100 successful messages. Without `--max-messages`, it stays running until Ctrl+C.

## Verify PostgreSQL

```powershell
python scripts/check_predictions.py
```

The summary should report 100 predictions. The first 100 original dataset rows may contain no fraud, which is normal.

## Verify scored Kafka messages

Open Kafka UI at `http://localhost:8080`, then visit `transactions.scored`.

Or run:

```powershell
docker exec fraud-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions.scored --from-beginning --max-messages 3
```

## Live test

Terminal 1:

```powershell
python -m src.consumer.consumer --group-id fraud-scoring-live-1
```

Terminal 2:

```powershell
python -m src.producer.producer --limit 1000 --events-per-second 20
```

Stop the consumer with Ctrl+C after processing finishes.

## Idempotency

PostgreSQL uses `UNIQUE(transaction_id, model_version)` and the consumer commits Kafka offsets only after the database write and scored-topic publication succeed. Replayed messages with identical transaction IDs will not duplicate database predictions.

## Next phase

After this integration consumer succeeds, the next phase provides the Streamlit live dashboard. PySpark Structured Streaming can then replace this consumer as the scalable implementation while retaining the same event and database contracts.
