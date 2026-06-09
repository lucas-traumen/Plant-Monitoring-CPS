#!/usr/bin/env python3
"""
web_dashboard_plant.py — Streamlit Dashboard for Plant Monitoring CPS

Compatible with the gateway measurement layout:
  sensors   : ESP32 sensor + BBB Edge AI result
  status    : ESP32/BBB status + latest JSON + planting_start ACK
  actuator  : physical pump/light feedback from ESP32
  cmd       : gateway command events SENT/DONE/ERROR
  dt        : Digital Twin/Web/Unity command queue PENDING

Run:
  streamlit run web_dashboard_plant.py --server.address 0.0.0.0 --server.port 8501
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

try:
    from streamlit_autorefresh import st_autorefresh
except Exception:  # pragma: no cover
    st_autorefresh = None


# =============================================================================
# CONFIG
# =============================================================================

load_dotenv()

NODE_ID = os.getenv("NODE_ID", "BRASSICA_JUNCEA_01")
PLANT_NAME = os.getenv("PLANT_NAME", "Rau Cải Mầm (Brassica juncea)")

# Accept both old dashboard env names and new gateway env names.
INFLUX_URL = os.getenv("INFLUX_URL") or os.getenv("INFLUXDB_URL") or "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN") or os.getenv("INFLUXDB_TOKEN") or "6pSuWQaFLlWq6iRVfaRYEMwIO1DDEChBsG42HdDx5En6fuqpUx95j3xswbVNrcWxRrs_sizN6XXESjzNqcHzJA=="
INFLUX_ORG = os.getenv("INFLUX_ORG") or os.getenv("INFLUXDB_ORG") or "DEV_TEAM"
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET") or os.getenv("INFLUXDB_BUCKET") or "digital_twin_data"
                                    
MEAS_SENSORS = os.getenv("MEAS_SENSORS", "sensors")
MEAS_STATUS = os.getenv("MEAS_STATUS", "status")
MEAS_ACTUATOR = os.getenv("MEAS_ACTUATOR", "actuator")
MEAS_CMD = os.getenv("MEAS_CMD", "cmd")
MEAS_DT = os.getenv("MEAS_DT", "dt")

WRITE_PRECISION_SECONDS = getattr(WritePrecision, "S", None) or getattr(WritePrecision, "SECONDS")


# =============================================================================
# HELPERS
# =============================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def check_env() -> list[str]:
    missing = []
    if not INFLUX_URL:
        missing.append("INFLUX_URL hoặc INFLUXDB_URL")
    if not INFLUX_TOKEN:
        missing.append("INFLUX_TOKEN hoặc INFLUXDB_TOKEN")
    if not INFLUX_ORG:
        missing.append("INFLUX_ORG hoặc INFLUXDB_ORG")
    if not INFLUX_BUCKET:
        missing.append("INFLUX_BUCKET hoặc INFLUXDB_BUCKET")
    return missing


@st.cache_resource(show_spinner=False)
def get_client() -> InfluxDBClient:
    return InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)


def safe_value(row: pd.Series | None, col: str, default: Any = "N/A") -> Any:
    try:
        if row is None or col not in row.index:
            return default
        value = row[col]
        if pd.isna(value):
            return default
        return value
    except Exception:
        return default


def safe_float(row: pd.Series | None, col: str, default: float = 0.0) -> float:
    try:
        value = safe_value(row, col, default)
        if value == "N/A":
            return default
        return float(value)
    except Exception:
        return default


def safe_int(row: pd.Series | None, col: str, default: int = 0) -> int:
    try:
        value = safe_value(row, col, default)
        if value == "N/A":
            return default
        return int(float(value))
    except Exception:
        return default


def normalize_dataframe(df: pd.DataFrame | list[pd.DataFrame] | None) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    if isinstance(df, list):
        if not df:
            return pd.DataFrame()
        df = pd.concat([x for x in df if x is not None and not x.empty], ignore_index=True)
    if df.empty:
        return pd.DataFrame()

    for col in ["result", "table", "_start", "_stop", "_measurement"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    if "_time" in df.columns:
        df["_time"] = pd.to_datetime(df["_time"], errors="coerce")
        df = df.dropna(subset=["_time"])
        df = df.sort_values("_time")

    numeric_cols = [
        "temperature", "air_humidity", "lux", "soil_moisture",
        "soil_s1", "soil_s2", "soil_s3", "soil_s4",
        "need_watering", "ai_confidence", "prob_need_watering",
        "pump", "light", "step", "gw_step", "uptime_s", "wifi_rssi",
        "days_after_planting", "duration_s", "planting_start_epoch",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


@st.cache_data(ttl=2, show_spinner=False)
def query_measurement(measurement: str, minutes: int = 60, limit: int = 500) -> pd.DataFrame:
    client = get_client()
    query_api = client.query_api()
    flux = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{int(minutes)}m)
  |> filter(fn: (r) => r["_measurement"] == "{measurement}")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"], desc: false)
  |> limit(n: {int(limit)})
'''
    result = query_api.query_data_frame(flux)
    return normalize_dataframe(result)


def write_dt_command(
    target: str,
    state: str = "",
    duration_s: int = 0,
    reason: str = "web_dashboard",
    action: str = "",
    planting_start_epoch: int = 0,
) -> str:
    client = get_client()
    write_api = client.write_api(write_options=SYNCHRONOUS)
    command_id = f"web-{uuid.uuid4().hex[:12]}"
    target = target.strip().lower()

    point = (
        Point(MEAS_DT)
        .tag("node_id", NODE_ID)
        .tag("command_id", command_id)
        .tag("target", target)
        .tag("status", "PENDING")
        .field("source", "web_dashboard")
        .field("reason", reason or "web_dashboard")
        .time(datetime.now(timezone.utc), WRITE_PRECISION_SECONDS)
    )

    if target == "planting_start":
        action = (action or "SET_NOW").strip().upper()
        point = point.field("action", action)
        if action == "SET_EPOCH" and planting_start_epoch > 0:
            point = point.field("planting_start_epoch", int(planting_start_epoch))
    else:
        point = point.field("state", state.upper()).field("duration_s", int(duration_s))

    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
    return command_id


def show_json_snapshot(value: Any) -> None:
    if not value or value == "N/A":
        st.info("Không có JSON snapshot.")
        return
    try:
        st.json(json.loads(str(value)))
    except Exception:
        st.code(str(value))


# =============================================================================
# STREAMLIT UI
# =============================================================================

st.set_page_config(
    page_title="Plant Monitoring CPS Dashboard",
    page_icon="🌱",
    layout="wide",
)

st.markdown(
    """
    <style>
    .main-title {font-size: 40px; font-weight: 800; margin-bottom: 0px;}
    .sub-title {color: #888; margin-top: -8px; margin-bottom: 24px;}
    .status-card {padding: 16px; border-radius: 12px; background-color: #111827; border: 1px solid #263244; margin-bottom: 10px;}
    .small-label {font-size: 14px; color: #B0B0B0; margin-bottom: 4px;}
    .big-value {font-size: 28px; font-weight: 700; color: white;}
    </style>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.header("⚙️ Cấu hình")
    minutes = st.slider("Khoảng thời gian đọc dữ liệu", min_value=10, max_value=1440, value=120, step=10)
    auto_refresh = st.checkbox("Tự động cập nhật", value=True)
    refresh_seconds = st.slider("Chu kỳ cập nhật", min_value=2, max_value=60, value=5, step=1)

    st.divider()
    st.write("**InfluxDB**")
    st.write(f"URL: `{INFLUX_URL}`")
    st.write(f"Org: `{INFLUX_ORG}`")
    st.write(f"Bucket: `{INFLUX_BUCKET}`")
    st.write("**Measurements**")
    st.code(f"{MEAS_SENSORS}\n{MEAS_STATUS}\n{MEAS_ACTUATOR}\n{MEAS_CMD}\n{MEAS_DT}")

    if st.button("🔄 Refresh ngay"):
        st.cache_data.clear()
        st.rerun()

if auto_refresh and st_autorefresh is not None:
    st_autorefresh(interval=refresh_seconds * 1000, key="dashboard_auto_refresh")
elif auto_refresh and st_autorefresh is None:
    st.warning("Chưa cài streamlit-autorefresh. Chạy: pip install streamlit-autorefresh")

st.markdown(
    f"""
    <div class="main-title">🌱 Plant Monitoring CPS Dashboard</div>
    <div class="sub-title">{PLANT_NAME} — MQTT + BBB Edge AI + InfluxDB + Digital Twin</div>
    """,
    unsafe_allow_html=True,
)

missing = check_env()
if missing:
    st.error("Thiếu cấu hình InfluxDB:")
    st.write(missing)
    st.stop()

try:
    sensors_df = query_measurement(MEAS_SENSORS, minutes=minutes, limit=1000)
    actuator_df = query_measurement(MEAS_ACTUATOR, minutes=minutes, limit=300)
    status_df = query_measurement(MEAS_STATUS, minutes=minutes, limit=300)
    cmd_df = query_measurement(MEAS_CMD, minutes=minutes, limit=500)
    dt_df = query_measurement(MEAS_DT, minutes=minutes, limit=500)
except Exception as exc:
    st.error("Lỗi khi đọc InfluxDB.")
    st.exception(exc)
    st.stop()

latest_sensor = sensors_df.iloc[-1] if not sensors_df.empty else None
latest_actuator = actuator_df.iloc[-1] if not actuator_df.empty else None
latest_status = status_df.iloc[-1] if not status_df.empty else None

if latest_sensor is not None:
    st.info(f"⏱️ Sensor mới nhất: {safe_value(latest_sensor, '_time')}")
else:
    st.warning("Chưa có dữ liệu trong measurement `sensors`.")

# -----------------------------------------------------------------------------
# Sensor + AI cards
# -----------------------------------------------------------------------------

st.subheader("📡 Cảm biến mới nhất")

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f'<div class="status-card"><div class="small-label">Nhiệt độ</div><div class="big-value">{safe_float(latest_sensor, "temperature"):.1f} °C</div></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="status-card"><div class="small-label">Độ ẩm không khí</div><div class="big-value">{safe_float(latest_sensor, "air_humidity"):.1f} %</div></div>', unsafe_allow_html=True)
with c3:
    st.markdown(f'<div class="status-card"><div class="small-label">Ánh sáng</div><div class="big-value">{safe_float(latest_sensor, "lux"):.1f} lux</div></div>', unsafe_allow_html=True)
with c4:
    st.markdown(f'<div class="status-card"><div class="small-label">Độ ẩm đất</div><div class="big-value">{safe_float(latest_sensor, "soil_moisture"):.1f} %</div></div>', unsafe_allow_html=True)

st.subheader("🤖 Edge AI & Điều khiển")
a1, a2, a3, a4, a5 = st.columns(5)
with a1:
    st.metric("Need watering", safe_int(latest_sensor, "need_watering"))
with a2:
    st.metric("AI confidence", f'{safe_float(latest_sensor, "ai_confidence"):.2f}')
with a3:
    st.metric("Prob watering", f'{safe_float(latest_sensor, "prob_need_watering"):.2f}')
with a4:
    st.metric("Pump", "ON" if safe_int(latest_sensor, "pump") == 1 else "OFF")
with a5:
    st.metric("Light", "ON" if safe_int(latest_sensor, "light") == 1 else "OFF")

m1, m2, m3, m4 = st.columns(4)
with m1:
    st.metric("Phase", safe_value(latest_sensor, "phase"))
with m2:
    st.metric("Days after planting", f'{safe_float(latest_sensor, "days_after_planting", -1):.2f}')
with m3:
    st.metric("RSSI", safe_int(latest_sensor, "wifi_rssi"))
with m4:
    st.metric("Gateway step", safe_int(latest_sensor, "gw_step"))

alert = safe_value(latest_sensor, "alert", "")
if alert and alert != "N/A":
    st.warning(str(alert))

# -----------------------------------------------------------------------------
# Actuator feedback
# -----------------------------------------------------------------------------

st.subheader("⚙️ Trạng thái actuator thực tế từ ESP32")

b1, b2, b3, b4 = st.columns(4)
with b1:
    st.metric("Pump feedback", safe_value(latest_actuator, "pump_state", "N/A"))
with b2:
    st.metric("Light feedback", safe_value(latest_actuator, "light_state", "N/A"))
with b3:
    st.metric("Pump mode", safe_value(latest_actuator, "pump_mode", "N/A"))
with b4:
    st.metric("Light mode", safe_value(latest_actuator, "light_mode", "N/A"))

# -----------------------------------------------------------------------------
# Digital Twin command panel
# -----------------------------------------------------------------------------

st.subheader("🧭 Digital Twin / Web Command")

with st.expander("Gửi lệnh xuống ESP32 thông qua measurement `dt`", expanded=True):
    cmd_col1, cmd_col2, cmd_col3 = st.columns(3)

    with cmd_col1:
        st.write("**Pump**")
        pump_duration = st.slider("Thời gian bơm (giây)", min_value=0, max_value=15, value=10, step=1)
        p_on, p_off = st.columns(2)
        with p_on:
            if st.button("💧 Pump ON"):
                cid = write_dt_command("pump", "ON", pump_duration, "web_pump_on")
                st.success(f"Đã ghi lệnh PENDING: {cid}")
                st.cache_data.clear()
        with p_off:
            if st.button("🛑 Pump OFF"):
                cid = write_dt_command("pump", "OFF", 0, "web_pump_off")
                st.success(f"Đã ghi lệnh PENDING: {cid}")
                st.cache_data.clear()

    with cmd_col2:
        st.write("**Light**")
        light_duration = st.slider("Thời gian đèn (giây)", min_value=0, max_value=1800, value=300, step=30)
        l_on, l_off = st.columns(2)
        with l_on:
            if st.button("💡 Light ON"):
                cid = write_dt_command("light", "ON", light_duration, "web_light_on")
                st.success(f"Đã ghi lệnh PENDING: {cid}")
                st.cache_data.clear()
        with l_off:
            if st.button("🌑 Light OFF"):
                cid = write_dt_command("light", "OFF", 0, "web_light_off")
                st.success(f"Đã ghi lệnh PENDING: {cid}")
                st.cache_data.clear()

    with cmd_col3:
        st.write("**Planting start**")
        if st.button("🌱 SET_NOW"):
            cid = write_dt_command("planting_start", action="SET_NOW", reason="web_planting_start_now")
            st.success(f"Đã ghi lệnh PENDING: {cid}")
            st.cache_data.clear()
        if st.button("🔎 GET"):
            cid = write_dt_command("planting_start", action="GET", reason="web_planting_start_get")
            st.success(f"Đã ghi lệnh PENDING: {cid}")
            st.cache_data.clear()
        if st.button("🧹 CLEAR"):
            cid = write_dt_command("planting_start", action="CLEAR", reason="web_planting_start_clear")
            st.success(f"Đã ghi lệnh PENDING: {cid}")
            st.cache_data.clear()

# -----------------------------------------------------------------------------
# Charts
# -----------------------------------------------------------------------------

st.subheader("📈 Biểu đồ")

if not sensors_df.empty:
    chart_df = sensors_df.copy()
    if "_time" in chart_df.columns:
        chart_df = chart_df.set_index("_time")

    sensor_cols = [c for c in ["temperature", "air_humidity", "lux", "soil_moisture"] if c in chart_df.columns]
    if sensor_cols:
        st.write("### Cảm biến")
        st.line_chart(chart_df[sensor_cols])

    ai_cols = [c for c in ["need_watering", "ai_confidence", "prob_need_watering", "pump", "light"] if c in chart_df.columns]
    if ai_cols:
        st.write("### AI / Pump / Light")
        st.line_chart(chart_df[ai_cols])
else:
    st.info("Chưa có dữ liệu `sensors` để vẽ biểu đồ.")

# -----------------------------------------------------------------------------
# Tables
# -----------------------------------------------------------------------------

st.subheader("📋 Bảng dữ liệu")

tab1, tab2, tab3, tab4, tab5 = st.tabs(["sensors", "actuator", "status", "cmd", "dt"])

with tab1:
    st.dataframe(sensors_df.sort_values("_time", ascending=False).head(100) if not sensors_df.empty and "_time" in sensors_df.columns else sensors_df, use_container_width=True)
with tab2:
    st.dataframe(actuator_df.sort_values("_time", ascending=False).head(100) if not actuator_df.empty and "_time" in actuator_df.columns else actuator_df, use_container_width=True)
with tab3:
    st.dataframe(status_df.sort_values("_time", ascending=False).head(100) if not status_df.empty and "_time" in status_df.columns else status_df, use_container_width=True)
    if latest_status is not None and "value_json" in status_df.columns:
        st.write("### Latest JSON snapshot")
        show_json_snapshot(safe_value(latest_status, "value_json"))
with tab4:
    st.dataframe(cmd_df.sort_values("_time", ascending=False).head(100) if not cmd_df.empty and "_time" in cmd_df.columns else cmd_df, use_container_width=True)
with tab5:
    st.dataframe(dt_df.sort_values("_time", ascending=False).head(100) if not dt_df.empty and "_time" in dt_df.columns else dt_df, use_container_width=True)

st.subheader("⬇️ Xuất dữ liệu")
export_df = sensors_df.copy()
if not export_df.empty:
    csv_data = export_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button("Tải sensors CSV", data=csv_data, file_name="sensors_export.csv", mime="text/csv")
else:
    st.info("Chưa có dữ liệu sensors để xuất CSV.")