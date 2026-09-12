FROM python:3.12-slim

USER root

# Install Java (required for PySpark) and PostgreSQL dependencies
RUN apt-get update && \
    mkdir -p /usr/share/man/man1 && \
    apt-get install -y default-jre libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install our standard consumer requirements plus PySpark
COPY requirements-common.txt .
COPY requirements-consumer.txt .
RUN pip install --default-timeout=1000 --no-cache-dir pyspark==3.5.1 -r requirements-consumer.txt

COPY src/ src/

ENV PYTHONPATH=/app
# Tell PySpark to automatically download the Kafka connector JARs
ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"

ENTRYPOINT ["python", "src/streaming/spark_consumer.py"]
