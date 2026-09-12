FROM python:3.12-slim

WORKDIR /app

# Install postgres dependencies for psycopg2
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

COPY requirements-common.txt .
COPY requirements-dashboard.txt .
RUN pip install --no-cache-dir -r requirements-dashboard.txt

COPY src/ src/

ENV PYTHONPATH=/app

EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "src/dashboard/dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
