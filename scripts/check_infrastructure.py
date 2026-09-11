import os
import sys
import time

import psycopg2
from dotenv import load_dotenv

load_dotenv()
config = {
    "dbname": os.getenv("POSTGRES_DB", "fraud_db"),
    "user": os.getenv("POSTGRES_USER", "fraud_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "fraud_dev_password"),
    "host": "localhost",
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
}

last_error = None
for _ in range(15):
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")
                tables = [row[0] for row in cur.fetchall()]
                expected = {"transactions", "predictions", "stream_batches", "model_registry", "data_quality_events"}
                missing = expected - set(tables)
                if missing:
                    raise RuntimeError(f"Missing tables: {sorted(missing)}")
                print("POSTGRES VERIFICATION PASSED")
                print("Tables:", ", ".join(tables))
                sys.exit(0)
    except Exception as exc:
        last_error = exc
        time.sleep(2)
raise RuntimeError(f"PostgreSQL verification failed: {last_error}")
