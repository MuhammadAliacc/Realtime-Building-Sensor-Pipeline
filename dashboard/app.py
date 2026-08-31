"""
Small Streamlit dashboard over the Postgres tables the stream processor
writes to. Not part of the "real-time" path itself -- it just polls
Postgres on a short interval, which is a normal and honest way to build a
monitoring view on top of a stream processing pipeline without adding a
second WebSocket layer.
"""

import os
import sys
import time
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "processor"))
import db  # noqa: E402

REFRESH_SECONDS = int(os.environ.get("DASHBOARD_REFRESH_SECONDS", "5"))

st.set_page_config(page_title="Building Sensor Pipeline", layout="wide")
st.title("Real-time building sensor monitoring")
st.caption(
    "Live view over the Postgres tables the stream processor writes to. "
    f"Refreshes every {REFRESH_SECONDS}s."
)

alerts = db.recent_alerts(limit=25)
alert_df = pd.DataFrame(alerts, columns=["sensor_id", "metric", "value", "z_score", "timestamp"])

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Recent alerts")
    if alert_df.empty:
        st.info("No alerts yet. The producer occasionally injects spikes, so one should show up within a few minutes.")
    else:
        st.dataframe(alert_df, use_container_width=True, hide_index=True)

with col2:
    st.subheader("Alert count by sensor")
    if not alert_df.empty:
        st.bar_chart(alert_df["sensor_id"].value_counts())
    else:
        st.write("Nothing to chart yet.")

st.divider()
st.subheader("Inspect a sensor")

sensor_id = st.text_input("Sensor ID", value="building-1-sensor-1")
metric = st.selectbox("Metric", ["temperature_c", "humidity_pct", "power_kw"])

if sensor_id:
    readings = db.recent_readings(sensor_id, metric, limit=200)
    if readings:
        df = pd.DataFrame(readings, columns=["value", "timestamp", "is_anomaly"])
        df = df.sort_values("timestamp")
        st.line_chart(df.set_index("timestamp")["value"])
        anomalies = df[df["is_anomaly"]]
        if not anomalies.empty:
            st.write(f"{len(anomalies)} anomalous reading(s) in this window:")
            st.dataframe(anomalies, use_container_width=True, hide_index=True)
    else:
        st.info("No readings for this sensor/metric yet.")

time.sleep(REFRESH_SECONDS)
st.rerun()
