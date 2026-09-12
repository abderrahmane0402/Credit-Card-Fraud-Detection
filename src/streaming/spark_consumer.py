import os
import json
import logging
import argparse
from datetime import datetime, timezone
import pandas as pd
import psycopg2
from psycopg2.extras import Json

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, MapType
)
from pyspark.sql.functions import from_json, col, current_timestamp, lit

from src.scoring.model_loader import load_model_bundle
from src.scoring.inference import score_transaction
from src.common.config import get_postgres_config
from src.common.constants import TOPIC_RAW, TOPIC_SCORED, TOPIC_DEAD_LETTER, MODELS_DIR, SPARK_CHECKPOINT_LOCATION

LOG = logging.getLogger("fraud-spark-consumer")

def create_spark_session(app_name="FraudDetectionSpark") -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

def get_schema() -> StructType:
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("run_id", StringType(), True),
        StructField("source_row_index", IntegerType(), True),
        StructField("sequence_number", IntegerType(), True),
        StructField("event_time", StringType(), True),
        StructField("producer_time", StringType(), True),
        StructField("source", StringType(), True),
        StructField("replay_mode", StringType(), True),
        StructField("schema_version", StringType(), True),
        StructField("actual_label", IntegerType(), True),
        StructField("features", MapType(StringType(), DoubleType()), True)
    ])

def process_batch(df, batch_id, bundle, kafka_producer_conf):
    if df.isEmpty():
        return
        
    LOG.info(f"Processing batch {batch_id}")
    records = df.collect()
    
    scored_events = []
    dead_letters = []
    
    # We will score and separate using standard Python because we need the existing joblib pipeline
    for row in records:
        event = row.asDict(recursive=True)
        # Re-construct JSON for dead letter fallback
        raw_payload = json.dumps(event)
        try:
            # Score
            scored = score_transaction(event, bundle)
            scored["processor"] = "spark"
            scored["stream_batch_id"] = batch_id
            scored_events.append(scored)
        except Exception as e:
            LOG.error(f"Error scoring event {event.get('transaction_id')}: {e}")
            dead_letters.append({
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "error_type": type(e).__name__,
                "error_message": str(e),
                "source_topic": TOPIC_RAW,
                "processor": "spark",
                "raw_payload": raw_payload
            })

    # Save to PostgreSQL
    if scored_events or dead_letters:
        connection = psycopg2.connect(**get_postgres_config())
        try:
            with connection.cursor() as cursor:
                for event in scored_events:
                    features = event["features"]
                    producer_time = datetime.fromisoformat(event["producer_time"].replace("Z", "+00:00"))
                    processed_at = datetime.fromisoformat(event["processed_time"].replace("Z", "+00:00"))
                    latency_ms = (processed_at - producer_time).total_seconds() * 1000

                    cursor.execute(
                        """
                        INSERT INTO transactions (
                            transaction_id, run_id, source_row_index, sequence_number, event_time, 
                            producer_time, source, replay_mode, schema_version,
                            amount, elapsed_time, features, actual_label
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (transaction_id) DO NOTHING
                        """,
                        (
                            event.get("transaction_id"), event.get("run_id"), event.get("source_row_index"), 
                            event.get("sequence_number"), event.get("event_time"), event.get("producer_time"),
                            event.get("source"), event.get("replay_mode"), event.get("schema_version"), 
                            float(features.get("Amount", 0)), float(features.get("Time", 0)), Json(features), int(event.get("actual_label", -1)),
                        ),
                    )
                    cursor.execute(
                        """
                        INSERT INTO predictions (
                            transaction_id, model_version, processor, fraud_score, decision_threshold,
                            predicted_label, processed_at, processing_latency_ms, stream_batch_id, prediction_correct
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (transaction_id, model_version, processor) DO NOTHING
                        """,
                        (
                            event.get("transaction_id"), event.get("model_version"), "spark", 
                            event.get("risk_score"), event.get("decision_threshold"), event.get("predicted_label"), 
                            processed_at, latency_ms, batch_id, event.get("prediction_correct"),
                        ),
                    )
                for dl in dead_letters:
                    cursor.execute(
                        """
                        INSERT INTO data_quality_events (
                            kafka_topic, reason, raw_payload
                        ) VALUES (%s, %s, %s)
                        """,
                        (TOPIC_RAW, dl["error_message"], dl["raw_payload"]),
                    )
            connection.commit()
        except Exception as e:
            LOG.error(f"PostgreSQL batch {batch_id} commit failed: {e}")
            connection.rollback()
            raise
        finally:
            connection.close()
            
    # Send to Kafka (Using standard confluent_kafka since spark-sql-kafka might be heavy for simple dict production)
    from confluent_kafka import Producer
    producer = Producer(kafka_producer_conf)
    
    for event in scored_events:
        producer.produce(
            TOPIC_SCORED,
            key=event["transaction_id"].encode("utf-8"),
            value=json.dumps(event).encode("utf-8")
        )
    for dl in dead_letters:
        producer.produce(
            TOPIC_DEAD_LETTER,
            value=json.dumps(dl).encode("utf-8")
        )
    producer.flush(10)
    LOG.info(f"Batch {batch_id} complete. Scored: {len(scored_events)}, DeadLetters: {len(dead_letters)}")


def run(args):
    spark = create_spark_session()
    bundle = load_model_bundle(args.model_dir)
    
    kafka_producer_conf = {
        "bootstrap.servers": args.bootstrap_servers,
        "client.id": "fraud-spark-output",
        "acks": "all",
        "enable.idempotence": True,
    }

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.bootstrap_servers) \
        .option("subscribe", args.input_topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", 500) \
        .load()

    parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), get_schema()).alias("data")) \
        .select("data.*")

    query = parsed_df.writeStream \
        .foreachBatch(lambda df, batch_id: process_batch(df, batch_id, bundle, kafka_producer_conf)) \
        .option("checkpointLocation", args.checkpoint_location) \
        .start()

    query.awaitTermination()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--input-topic", default=os.getenv("KAFKA_INPUT_TOPIC", TOPIC_RAW))
    parser.add_argument("--model-dir", default=str(MODELS_DIR))
    parser.add_argument("--checkpoint-location", default=SPARK_CHECKPOINT_LOCATION)
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run(args)


if __name__ == "__main__":
    main()
