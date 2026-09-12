import os
from dotenv import load_dotenv

load_dotenv()

def get_kafka_config():
    return {
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    }

def get_postgres_config():
    return {
        "dbname": os.getenv("POSTGRES_DB", "fraud_db"),
        "user": os.getenv("POSTGRES_USER", "fraud_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "fraud_dev_password"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
    }
