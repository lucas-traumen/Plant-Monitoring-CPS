import json
import os
from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import Any

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from influxdb_client import InfluxDBClient


# ======================================================
# LOAD ENV
# ======================================================

load_dotenv()

# Dùng tên biến giống BBB/gateway.py trước.
# Vẫn hỗ trợ thêm dạng INFLUXDB_* nếu sau này cần.
INFLUX_URL = os.getenv("INFLUX_URL") or os.getenv("INFLUXDB_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN") or os.getenv("INFLUXDB_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG") or os.getenv("INFLUXDB_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET") or os.getenv("INFLUXDB_BUCKET")

# Measurement theo luồng gateway.py
MEAS_SENSORS = os.getenv("MEAS_SENSORS", "sensors")
MEAS_STATUS = os.getenv("MEAS_STATUS", "status")
MEAS_ACTUATOR = os.getenv("MEAS_ACTUATOR", "actuator")
MEAS_CMD = os.getenv("MEAS_CMD", "cmd")
MEAS_DT = os.getenv("MEAS_DT", "dt")


# ======================================================
# FASTAPI APP
# ======================================================

app = FastAPI(
    title="Plant Monitoring CPS Web API",
    description="Backend API for React Dashboard. Data source: InfluxDB written by BBB/gateway.py",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ======================================================
# COMMON HELPERS
# ======================================================

def check_env() -> None:
    missing = []

    if not INFLUX_URL:
        missing.append("INFLUX_URL")
    if not INFLUX_TOKEN:
        missing.append("INFLUX_TOKEN")
    if not INFLUX_ORG:
        missing.append("INFLUX_ORG")
    if not INFLUX_BUCKET:
        missing.append("INFLUX_BUCKET")

    if missing:
        raise HTTPException(
            status_code=500,
            detail=f"Missing environment variables: {', '.join(missing)}",
        )


def get_client() -> InfluxDBClient:
    check_env()

    return InfluxDBClient(
        url=INFLUX_URL,
        token=INFLUX_TOKEN,
        org=INFLUX_ORG,
    )


def clean_value(value: Any):
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, np.integer):
        return int(value)

    if isinstance(value, np.floating):
        return float(value)

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    if isinstance(value, datetime):
        return value.isoformat()

    return value


def normalize_dataframe(df: Any) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()

    if isinstance(df, list):
        frames = []

        for item in df:
            if item is not None and not item.empty:
                frames.append(item)

        if len(frames) == 0:
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True)

    if df.empty:
        return df

    drop_cols = [
        "result",
        "table",
        "_start",
        "_stop",
        "_measurement",
    ]

    for col in drop_cols:
        if col in df.columns:
            df = df.drop(columns=[col])

    if "_time" in df.columns:
        df["_time"] = pd.to_datetime(df["_time"], errors="coerce")
        df = df.dropna(subset=["_time"])
        df = df.sort_values("_time")

    numeric_cols = [
        "temperature",
        "air_humidity",
        "lux",
        "soil_moisture",
        "soil_avg",
        "soil_s1",
        "soil_s2",
        "soil_s3",
        "soil_s4",
        "need_watering",
        "confidence",
        "ai_confidence",
        "prob_need_watering",
        "pump",
        "light",
        "pump_state_value",
        "light_state_value",
        "phase",
        "step",
        "gw_step",
        "uptime_s",
        "wifi_rssi",
        "days_after_planting",
        "days_after_sowing",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def dataframe_to_records(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []

    records = []

    for _, row in df.iterrows():
        item = {}

        for col in df.columns:
            item[col] = clean_value(row[col])

        records.append(item)

    return records


def build_range_part(minutes: int, selected_date: str | None) -> str:
    if selected_date:
        try:
            start_dt = datetime.strptime(selected_date, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            stop_dt = start_dt + timedelta(days=1)

            start_text = start_dt.isoformat().replace("+00:00", "Z")
            stop_text = stop_dt.isoformat().replace("+00:00", "Z")

            return f'''
      |> range(start: time(v: "{start_text}"), stop: time(v: "{stop_text}"))
            '''
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYY-MM-DD.",
            )

    minutes = max(1, min(minutes, 1440))

    return f'''
      |> range(start: -{minutes}m)
    '''


def query_measurement(
    measurement: str,
    minutes: int = 60,
    selected_date: str | None = None,
) -> pd.DataFrame:
    range_part = build_range_part(minutes, selected_date)

    flux_query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      {range_part}
      |> filter(fn: (r) => r["_measurement"] == "{measurement}")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"])
    '''

    client = get_client()

    try:
        query_api = client.query_api()
        df = query_api.query_data_frame(flux_query)
    finally:
        client.close()

    return normalize_dataframe(df)


def get_latest_record(
    measurement: str,
    minutes: int = 1440,
    selected_date: str | None = None,
) -> dict | None:
    df = query_measurement(
        measurement=measurement,
        minutes=minutes,
        selected_date=selected_date,
    )

    if df.empty:
        return None

    records = dataframe_to_records(df.tail(1))

    if len(records) == 0:
        return None

    return records[0]


def parse_status_record(record: dict | None) -> dict | None:
    if not record:
        return None

    raw = record.get("value_json")

    if not raw:
        return record

    try:
        parsed = json.loads(raw)

        if isinstance(parsed, dict):
            parsed["_time"] = record.get("_time")
            parsed["_status_key"] = record.get("key")
            return parsed

        return record
    except Exception:
        return record


# ======================================================
# ROUTES
# ======================================================

@app.get("/")
def root():
    return {
        "message": "Plant Monitoring CPS Web API is running",
        "docs": "/docs",
    }


@app.get("/api/health")
def health():
    check_env()

    return {
        "status": "OK",
        "influx_url": INFLUX_URL,
        "org": INFLUX_ORG,
        "bucket": INFLUX_BUCKET,
        "measurements": {
            "sensors": MEAS_SENSORS,
            "status": MEAS_STATUS,
            "actuator": MEAS_ACTUATOR,
            "cmd": MEAS_CMD,
            "dt": MEAS_DT,
        },
    }


@app.get("/api/dashboard/latest")
def dashboard_latest(
    minutes: int = Query(default=1440, ge=1, le=1440),
    date: str | None = Query(default=None),
):
    try:
        latest_sensors = get_latest_record(
            measurement=MEAS_SENSORS,
            minutes=minutes,
            selected_date=date,
        )

        latest_actuator = get_latest_record(
            measurement=MEAS_ACTUATOR,
            minutes=minutes,
            selected_date=date,
        )

        latest_status_raw = get_latest_record(
            measurement=MEAS_STATUS,
            minutes=minutes,
            selected_date=date,
        )

        latest_status = parse_status_record(latest_status_raw)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )

    return {
        "sensors": latest_sensors,
        "actuator": latest_actuator,
        "status": latest_status,
    }


@app.get("/api/history/sensors")
def history_sensors(
    minutes: int = Query(default=720, ge=1, le=1440),
    date: str | None = Query(default=None),
):
    try:
        df = query_measurement(
            measurement=MEAS_SENSORS,
            minutes=minutes,
            selected_date=date,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )

    records = dataframe_to_records(df)

    return {
        "measurement": MEAS_SENSORS,
        "count": len(records),
        "data": records,
    }


@app.get("/api/history/actuator")
def history_actuator(
    minutes: int = Query(default=720, ge=1, le=1440),
    date: str | None = Query(default=None),
):
    try:
        df = query_measurement(
            measurement=MEAS_ACTUATOR,
            minutes=minutes,
            selected_date=date,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )

    records = dataframe_to_records(df)

    return {
        "measurement": MEAS_ACTUATOR,
        "count": len(records),
        "data": records,
    }


@app.get("/api/history/status")
def history_status(
    minutes: int = Query(default=720, ge=1, le=1440),
    date: str | None = Query(default=None),
):
    try:
        df = query_measurement(
            measurement=MEAS_STATUS,
            minutes=minutes,
            selected_date=date,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )

    records = dataframe_to_records(df)

    return {
        "measurement": MEAS_STATUS,
        "count": len(records),
        "data": records,
    }


@app.get("/api/export/sensors.csv")
def export_sensors_csv(
    minutes: int = Query(default=720, ge=1, le=1440),
    date: str | None = Query(default=None),
):
    try:
        df = query_measurement(
            measurement=MEAS_SENSORS,
            minutes=minutes,
            selected_date=date,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
    csv_buffer.seek(0)

    filename = "sensors_export.csv"

    if date:
        filename = f"sensors_{date}.csv"

    return StreamingResponse(
        iter([csv_buffer.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
        },
    )