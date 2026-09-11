# Phase 4: Fraud-scoring consumer

This package adds a Python Kafka consumer that reads `transactions.raw`, validates each transaction, loads the Kaggle model, calculates a fraud risk score, and publishes the result to `transactions.scored`. Invalid messages are sent to `transactions.dead-letter`.

## Copy into the existing project

Extract this ZIP and copy its contents into the root of the existing project. Allow Windows to merge `src/` and `tests/`; no existing producer files are replaced.

## Install dependencies

With your existing Python or Conda environment active:

```powershell
python -m pip install -r requirements-consumer.txt
```

## Verify the exported model first

```powershell
python scripts\verify_model.py
```

Expected: `MODEL VERIFICATION PASSED`.

## Run tests

```powershell
python -m pytest -q
```

The existing producer tests and the new scoring tests should pass.

## Score the 100 events already stored in Kafka

Use a new consumer group so the consumer reads the existing messages from the beginning:

```powershell
python -m src.scoring.consumer --group-id fraud-scoring-test-v1 --offset-reset earliest --max-messages 100
```

Expected final output:

```text
Finished processed=100 scored=100 dead_letter=0
```

Because your first 100 source rows contain no actual fraud, zero fraud alerts is normal.

## Verify scored events

Open Kafka UI at `http://localhost:8080`, then open `transactions.scored` and inspect its messages.

Or read three messages from the terminal:

```powershell
docker exec fraud-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions.scored --from-beginning --max-messages 3
```

## Run as a continuous service

Terminal 1:

```powershell
python -m src.scoring.consumer --group-id fraud-scoring-service-v1 --offset-reset latest
```

Terminal 2:

```powershell
python -m src.producer.producer --limit 1000 --events-per-second 20
```

Stop the consumer with `Ctrl+C`.

## Offset behavior

Kafka remembers offsets per consumer group. Reusing the same group continues where it stopped. To reprocess old messages for testing, use a new group ID such as `fraud-scoring-test-v2`.

## Delivery behavior

The consumer uses manual offset commits. It commits a source message only after Kafka acknowledges either the scored event or its dead-letter record. This provides at-least-once processing. Duplicate output remains possible after an unusual process failure; PostgreSQL idempotency will handle that in the next phase.

## Next phase

Phase 5 will consume `transactions.scored` and perform idempotent PostgreSQL upserts for transactions and predictions.
