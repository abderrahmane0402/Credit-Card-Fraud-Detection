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
    fetch_data_quality,
    fetch_processor_health,
)

# -----------------------------------------------------------------------------
# Page Configuration & Custom CSS
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="FinSec | Real-Time Fraud Operations",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    /* Clean up the main padding */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    
    /* Style the metric cards */
    div[data-testid="metric-container"] {
        background-color: #f8f9fa;
        border: 1px solid #e9ecef;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    
    /* Dark mode support for metric cards */
    @media (prefers-color-scheme: dark) {
        div[data-testid="metric-container"] {
            background-color: #1e1e1e;
            border: 1px solid #333;
        }
    }
    
    /* Header styling */
    .company-header {
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        color: #1f77b4;
        font-weight: 800;
        margin-bottom: 0px;
        padding-bottom: 0px;
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="company-header">🛡️ FinSec Operations Center</h1>', unsafe_allow_html=True)
st.caption("Enterprise Real-Time Transaction Monitoring & Fraud Prevention")

# -----------------------------------------------------------------------------
# Sidebar Controls
# -----------------------------------------------------------------------------
with st.sidebar:
    st.markdown("### 🎛️ Control Panel")
    refresh_seconds = st.slider("Auto-refresh interval (sec)", 2, 30, 5)
    
    st.markdown("---")
    st.markdown("### 🔍 Filters")
    time_window = st.select_slider(
        "Lookback Window",
        options=[15, 30, 60, 180, 360, 720, 1440],
        value=60,
        format_func=lambda value: f"{value} min" if value < 60 else f"{value // 60} h",
    )
    minimum_score = st.slider("Minimum Risk Score Threshold", 0.0, 1.0, 0.0, step=0.01)
    row_limit = st.number_input("Transaction Log Limit", min_value=10, max_value=1000, value=200, step=50)
    
    if st.button("🔄 Force Refresh Now", use_container_width=True):
        st.rerun()

    st.markdown("---")
    st.caption("System Status: **ONLINE** ✅")

@st.fragment(run_every=refresh_seconds)
def live_dashboard():
    try:
        summary = fetch_summary()
        performance = fetch_performance()
        timeseries = fetch_timeseries(time_window)
        # Fetching top transactions
        recent = fetch_recent_predictions(row_limit, minimum_score, (0, 1))
        distribution = fetch_risk_distribution()
        data_quality = fetch_data_quality()
        processor_health = fetch_processor_health()
    except Exception as error:
        st.error("🔌 Disconnected from PostgreSQL Core Database.")
        st.info("Please verify infrastructure health and network connections.")
        st.stop()

    if summary["last_processed_at"] is None:
        st.warning("Awaiting live transaction stream... Please start the producer.")
        st.stop()

    # -----------------------------------------------------------------------------
    # Primary Dashboard Navigation
    # -----------------------------------------------------------------------------
    tab_ops, tab_ml, tab_it = st.tabs([
        "👁️ Security Operations (SOC)", 
        "🧠 Model Diagnostics", 
        "⚙️ Infrastructure Health"
    ])

    # =============================================================================
    # TAB 1: SECURITY OPERATIONS (Business View)
    # =============================================================================
    with tab_ops:
        # High Level KPIs
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        kpi1.metric("Total Volume Scored", f"{summary['total_predictions']:,}", delta="Live")
        kpi2.metric("Fraud Blocked", f"{summary['fraud_alerts']:,}", delta="Alerts", delta_color="inverse")
        kpi3.metric("Alert Rate", f"{summary['alert_rate']:.2%}")
        kpi4.metric("Avg Latency", f"{summary['avg_latency_ms']:.1f} ms", delta="Optimal" if summary['avg_latency_ms'] < 100 else "High", delta_color="inverse")

        st.markdown("<br>", unsafe_allow_html=True)
        
        col_live, col_feed = st.columns([2, 1])
        
        with col_live:
            st.markdown("#### 📈 Live Network Traffic")
            if timeseries.empty:
                st.info("No traffic in selected window.")
            else:
                # Beautiful Plotly Chart
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=timeseries["minute"], y=timeseries["transactions"],
                    mode="lines", name="Total Volume",
                    line=dict(color="#1f77b4", width=3),
                    fill='tozeroy', fillcolor='rgba(31, 119, 180, 0.1)'
                ))
                fig.add_trace(go.Scatter(
                    x=timeseries["minute"], y=timeseries["alerts"],
                    mode="lines+markers", name="Threats Detected",
                    line=dict(color="#d62728", width=2)
                ))
                fig.update_layout(
                    template="plotly_white",
                    margin=dict(l=0, r=0, t=10, b=0),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    hovermode="x unified"
                )
                fig.update_xaxes(showgrid=False)
                fig.update_yaxes(showgrid=True, gridcolor="#f0f0f0")
                st.plotly_chart(fig, use_container_width=True)

        with col_feed:
            st.markdown("#### 🚨 Critical Threat Feed")
            st.caption("Most recent confirmed blocks")
            alerts_only = recent[recent["predicted_label"] == 1].head(10) if not recent.empty else pd.DataFrame()
            
            if alerts_only.empty:
                st.success("Network secure. No recent threats.")
            else:
                # Custom visual feed instead of a raw dataframe
                for _, row in alerts_only.iterrows():
                    amt = row['amount']
                    risk = row['fraud_score'] * 100
                    time_str = row['event_time'].strftime("%H:%mm:%ss") if pd.notnull(row['event_time']) else "Now"
                    
                    st.error(f"**Blocked: ${amt:,.2f}**  \nRisk: {risk:.1f}% | Time: {time_str}")
        
        st.divider()
        st.markdown("#### 📜 Global Transaction Log")
        if not recent.empty:
            display = recent.copy()
            display["status"] = display["predicted_label"].map({0: "✅ Approved", 1: "🛑 Blocked"})
            # Show the most relevant columns
            cols = ["event_time", "transaction_id", "amount", "fraud_score", "status"]
            st.dataframe(
                display[cols],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "event_time": st.column_config.DatetimeColumn("Timestamp", format="YYYY-MM-DD HH:mm:ss"),
                    "transaction_id": st.column_config.TextColumn("Transaction ID"),
                    "amount": st.column_config.NumberColumn("Amount (USD)", format="$ %.2f"),
                    "fraud_score": st.column_config.ProgressColumn("Risk Confidence", min_value=0.0, max_value=1.0, format="%.2f"),
                    "status": st.column_config.TextColumn("Decision"),
                },
            )

    # =============================================================================
    # TAB 2: MODEL DIAGNOSTICS (Data Science View)
    # =============================================================================
    with tab_ml:
        st.subheader("Model Validation & Telemetry")
        if not performance.empty:
            perf = performance.iloc[0]
            tp = perf["true_positives"]
            fp = perf["false_positives"]
            fn = perf["false_negatives"]
            tn = perf["true_negatives"]
            precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
            f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

            perf_cols = st.columns(4)
            perf_cols[0].metric("Precision (Quality)", f"{precision:.2%}", help="How many of the blocked transactions were actually fraud?")
            perf_cols[1].metric("Recall (Coverage)", f"{recall:.2%}", help="How much of the total fraud did we successfully catch?")
            perf_cols[2].metric("F1 Score", f"{f1:.2%}")
            perf_cols[3].metric("False Positive Count", f"{fp:,}", help="Legitimate customers wrongfully blocked.")

            st.markdown("<br>", unsafe_allow_html=True)
            col_conf, col_dist = st.columns(2)
            
            with col_conf:
                st.markdown("##### Cumulative Confusion Matrix")
                confusion = pd.DataFrame(
                    [[tn, fp], [fn, tp]],
                    index=["Actual Legitimate", "Actual Fraud"],
                    columns=["Predicted Legitimate", "Predicted Fraud"],
                )
                confusion_figure = px.imshow(
                    confusion, text_auto=True, color_continuous_scale="Blues", aspect="auto"
                )
                confusion_figure.update_layout(margin=dict(l=0, r=0, t=10, b=0))
                st.plotly_chart(confusion_figure, use_container_width=True)
            
            with col_dist:
                st.markdown("##### Real-Time Risk Score Distribution")
                if not distribution.empty:
                    dist_fig = px.bar(
                        distribution, x="risk_range", y="transactions",
                        labels={"risk_range": "Risk Bucket", "transactions": "Volume"}
                    )
                    dist_fig.update_layout(
                        template="plotly_white", margin=dict(l=0, r=0, t=10, b=0),
                        plot_bgcolor='rgba(0,0,0,0)'
                    )
                    dist_fig.update_traces(marker_color='#1f77b4')
                    st.plotly_chart(dist_fig, use_container_width=True)
        else:
            st.info("Sufficient validation data is not yet available to calculate metrics.")

    # =============================================================================
    # TAB 3: INFRASTRUCTURE HEALTH (IT / DevOps View)
    # =============================================================================
    with tab_it:
        st.subheader("Kafka & Processor Telemetry")
        
        col_dq, col_health = st.columns(2)
        with col_health:
            st.markdown("##### 🔌 Active Consumers")
            if processor_health.empty:
                st.info("No processor heartbeats detected.")
            else:
                st.dataframe(processor_health, use_container_width=True, hide_index=True)

        with col_dq:
            st.markdown("##### ⚠️ Data Quality & Dead-Letter Events")
            if data_quality.empty:
                st.success("System healthy. 0 dropped payloads.")
            else:
                st.dataframe(data_quality, use_container_width=True, hide_index=True)

live_dashboard()
