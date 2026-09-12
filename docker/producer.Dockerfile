FROM python:3.12-slim

WORKDIR /app

COPY requirements-common.txt .
COPY requirements-producer.txt .
RUN pip install --no-cache-dir -r requirements-producer.txt

COPY src/ src/

ENV PYTHONPATH=/app

ENTRYPOINT ["python", "-m", "src.producer.producer"]
