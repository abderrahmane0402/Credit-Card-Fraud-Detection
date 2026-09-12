FROM python:3.12-slim

WORKDIR /app

# Install postgres dependencies for psycopg2
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

COPY requirements-common.txt .
COPY requirements-consumer.txt .
RUN pip install --no-cache-dir -r requirements-consumer.txt

COPY src/ src/

ENV PYTHONPATH=/app

ENTRYPOINT ["python", "-m", "src.consumer.consumer"]
