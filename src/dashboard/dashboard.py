from __future__ import annotations

import time

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src.dashboard.database import (
    fetch_performance,
    fetch_recent_predictions,
    fetch_risk_distribution,
    fetch_summary,
    fetch_timeseries,
)

st.set_page_config(
    page_title="Real-Time Fraud Detection",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🛡️ Real-Time Fraud Detection")
st.caption("Live Kafka transaction scoring backed by PostgreSQL")

with st.sidebar:
    st.header("Dashboard controls")
    auto_refresh = st.toggle("Auto-refresh", value=True)
    refresh_seconds = st.slider("Refresh interval (seconds)", 2, 30, 5)
    time_window = st.select_slider(
        "Chart window",
        options=[15, 30, 60, 180, 360, 720, 1440],
        value=60,
        format_func=lambda value: f"{value} min" if value < 60 else f"{value // 60} h",
    )
    row_limit = st.slider("Recent transaction rows", 10, 500, 100, step=10)
    minimum_score = st.slider("Minimum risk score", 0.0, 1.0, 0.0, step=0.01)
    label_filter = st.multiselect(
        "Prediction filter",
        options=[0, 1],
        default=[0, 1],
        format_func=lambda value: "Fraud alert" if value == 1 else "Legitimate",
    )
    if st.button("Refresh now", use_container_width=True):
        st.rerun()

if not label_filter:
    label_filter = [0, 1]

try:
    summary = fetch_summary()
    performance = fetch_performance()
    timeseries = fetch_timeseries(time_window)
    recent = fetch_recent_predictions(row_limit, minimum_score, tuple(label_filter))
    distribution = fetch_risk_distribution()
except Exception as error:
    st.error("Could not connect to the fraud database.")
    st.exception(error)
    st.info("Confirm Docker is running and POSTGRES_PORT=5433 is present in .env.")
    st.stop()

fraud_rate = (
    summary["fraud_alerts"] / summary["total_predictions"]
    if summary["total_predictions"]
    else 0.0
)

metric_columns = st.columns(5)
metric_columns[0].metric("Transactions scored", f"{summary['total_predictions']:,}")
metric_columns[1].metric("Fraud alerts", f"{summary['fraud_alerts']:,}")
metric_columns[2].metric("Alert rate", f"{fraud_rate:.3%}")
metric_columns[3].metric("Average latency", f"{summary['avg_latency_ms']:.1f} ms")
metric_columns[4].metric("P95 latency", f"{summary['p95_latency_ms']:.1f} ms")

if summary["last_processed_at"] is not None:
    st.caption(f"Last transaction processed: {summary['last_processed_at']}")
else:
    st.warning("No predictions are stored yet. Start the consumer and producer to populate the dashboard.")

st.subheader("Live processing activity")
left_chart, right_chart = st.columns(2)

with left_chart:
    if timeseries.empty:
        st.info("No time-series data is available in the selected window.")
    else:
        figure = go.Figure()
        figure.add_trace(go.Scatter(
            x=timeseries["minute"], y=timeseries["transactions"],
            mode="lines+markers", name="Transactions"
        ))
        figure.add_trace(go.Scatter(
            x=timeseries["minute"], y=timeseries["fraud_alerts"],
            mode="lines+markers", name="Fraud alerts"
        ))
        figure.update_layout(
            title="Transactions and alerts per minute",
            xaxis_title="Processing minute",
            yaxis_title="Count",
            legend_title="Series",
        )
        st.plotly_chart(figure, use_container_width=True)

with right_chart:
    if distribution.empty:
        st.info("No risk-score distribution is available yet.")
    else:
        figure = px.bar(
            distribution,
            x="risk_range",
            y="transactions",
            title="Risk-score distribution",
            labels={"risk_range": "Risk-score range", "transactions": "Transactions"},
        )
        st.plotly_chart(figure, use_container_width=True)

st.subheader("Model performance on replay labels")
performance_columns = st.columns(7)
performance_columns[0].metric("Precision", f"{performance['precision']:.2%}")
performance_columns[1].metric("Recall", f"{performance['recall']:.2%}")
performance_columns[2].metric("F1", f"{performance['f1']:.2%}")
performance_columns[3].metric("True positives", f"{performance['true_positives']:,}")
performance_columns[4].metric("False positives", f"{performance['false_positives']:,}")
performance_columns[5].metric("False negatives", f"{performance['false_negatives']:,}")
performance_columns[6].metric("True negatives", f"{performance['true_negatives']:,}")

confusion = pd.DataFrame(
    [
        [performance["true_negatives"], performance["false_positives"]],
        [performance["false_negatives"], performance["true_positives"]],
    ],
    index=["Actual legitimate", "Actual fraud"],
    columns=["Predicted legitimate", "Predicted fraud"],
)
confusion_figure = px.imshow(
    confusion,
    text_auto=True,
    color_continuous_scale="Blues",
    title="Cumulative confusion matrix",
    labels={"x": "Prediction", "y": "Ground truth", "color": "Transactions"},
)
st.plotly_chart(confusion_figure, use_container_width=True)

st.subheader("Recent scored transactions")
if recent.empty:
    st.info("No records match the selected filters.")
else:
    display = recent.copy()
    display["status"] = display["predicted_label"].map({0: "Legitimate", 1: "Fraud alert"})
    display["correct"] = display["actual_label"] == display["predicted_label"]
    display = display[[
        "processed_at", "transaction_id", "amount", "fraud_score",
        "decision_threshold", "status", "actual_label", "correct",
        "model_version", "processing_latency_ms",
    ]]
    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "fraud_score": st.column_config.ProgressColumn(
                "Risk score", min_value=0.0, max_value=1.0, format="%.4f"
            ),
            "amount": st.column_config.NumberColumn("Amount", format="%.2f"),
            "processing_latency_ms": st.column_config.NumberColumn("Latency (ms)", format="%.1f"),
            "correct": st.column_config.CheckboxColumn("Correct"),
        },
    )

st.caption(
    "The dataset is historical and is replayed through Kafka to simulate a live payment stream. "
    "Risk scores are model outputs and should not be interpreted as perfectly calibrated probabilities."
)

if auto_refresh:
    time.sleep(refresh_seconds)
    st.rerun()
