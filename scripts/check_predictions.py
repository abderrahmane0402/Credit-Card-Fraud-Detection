import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
connection = psycopg2.connect(
    dbname=os.getenv("POSTGRES_DB", "fraud_db"),
    user=os.getenv("POSTGRES_USER", "fraud_user"),
    password=os.getenv("POSTGRES_PASSWORD", "fraud_dev_password"),
    host="localhost",
    port=int(os.getenv("POSTGRES_PORT", "5432")),
)

with connection, connection.cursor() as cursor:
    cursor.execute("""
        SELECT
            COUNT(*) AS predictions,
            COUNT(*) FILTER (WHERE predicted_label = 1) AS fraud_alerts,
            COUNT(*) FILTER (WHERE actual_label = 1) AS actual_fraud,
            COUNT(*) FILTER (WHERE predicted_label = 1 AND actual_label = 1) AS true_positives,
            COUNT(*) FILTER (WHERE predicted_label = 1 AND actual_label = 0) AS false_positives,
            COUNT(*) FILTER (WHERE predicted_label = 0 AND actual_label = 1) AS false_negatives,
            ROUND(AVG(processing_latency_ms)::numeric, 2) AS average_latency_ms
        FROM predictions p
        JOIN transactions t USING (transaction_id)
    """)
    result = cursor.fetchone()

labels = ["predictions", "fraud_alerts", "actual_fraud", "true_positives", "false_positives", "false_negatives", "average_latency_ms"]
print("PREDICTION DATABASE SUMMARY")
for label, value in zip(labels, result):
    print(f"{label}: {value}")
connection.close()
