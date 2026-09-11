# Phase 5: Live Streamlit dashboard

This package adds a live dashboard for PostgreSQL transaction and prediction data.

## Merge into the existing project

Extract the ZIP and copy its contents into the root of the existing project. Allow Windows to merge `src/` and `tests/`.

## Confirm `.env`

Because local PostgreSQL already uses port 5432, the Docker database should use:

```dotenv
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=fraud_db
POSTGRES_USER=fraud_user
POSTGRES_PASSWORD=fraud_dev_password
```

Use your actual current development password if it differs.

## Install dependencies

```powershell
python -m pip install -r requirements-dashboard.txt
```

## Run tests

```powershell
python -m pytest -q
```

## Start the dashboard

```powershell
python -m streamlit run src/dashboard/dashboard.py
```

Open `http://localhost:8501` if the browser does not open automatically.

## Live demonstration

Use three PowerShell terminals with the same Python environment active.

### Terminal 1: consumer

Use your project's working consumer command:

```powershell
python -m src.consumer.consumer --group-id fraud-live-dashboard-v1
```

### Terminal 2: dashboard

```powershell
python -m streamlit run src/dashboard/dashboard.py
```

### Terminal 3: producer

```powershell
python -m src.producer.producer --limit 1000 --events-per-second 20
```

The dashboard refreshes every five seconds by default. You can change or disable automatic refresh from the sidebar.

## Dashboard content

- Transactions scored
- Fraud alerts and alert rate
- Average and P95 processing latency
- Transactions and alerts by minute
- Risk-score distribution
- Precision, recall, and F1 on historical replay labels
- Confusion matrix
- Recent scored transactions with filters

## Notes

The first rows in the source dataset contain no fraud, so an early test can show zero alerts. This is expected. A later phase can add a clearly labeled demonstration replay mode that includes real fraud rows from the dataset without altering their values or labels.
