"""
main.py — Plant Monitoring CPS Backend v1.2.0-realtime-mqtt
================================================================

New architecture:
- Realtime path: ESP32 -> MQTT -> Gateway -> Backend ingest -> WebSocket -> Web/Unity.
- Storage path : Gateway/Backend -> HTTPS -> InfluxDB Cloud.
- Command path : Web/Unity -> Backend -> MQTT immediately, and command log to InfluxDB.

InfluxDB is used for history/audit/recovery. It is not the mandatory realtime bus.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import Any, Literal

import numpy as np
import pandas as pd
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from pydantic import BaseModel, Field

# =============================================================================
# CONFIG
# =============================================================================

load_dotenv()

API_VERSION = "1.2.0-realtime-mqtt"
NODE_ID = os.getenv("NODE_ID", "BRASSICA_JUNCEA_01")
NODE_TOPIC_ID = os.getenv("NODE_TOPIC_ID", "brassica_01")
PLANT_NAME = os.getenv("PLANT_NAME", "Rau Cải Mầm (Brassica juncea)")

TOPIC_ROOT = os.getenv("TOPIC_ROOT", "cps/greenhouse")
TOPICS = {
    "telemetry_sensors": f"{TOPIC_ROOT}/{NODE_TOPIC_ID}/telemetry/sensors",
    "state_actuator": f"{TOPIC_ROOT}/{NODE_TOPIC_ID}/state/actuator",
    "status_esp32": f"{TOPIC_ROOT}/{NODE_TOPIC_ID}/status/esp32",
    "status_gateway": f"{TOPIC_ROOT}/gateway/status",
    "cmd_auto_pump": f"{TOPIC_ROOT}/{NODE_TOPIC_ID}/cmd/auto/pump",
    "cmd_direct_pump": f"{TOPIC_ROOT}/{NODE_TOPIC_ID}/cmd/direct/pump",
    "cmd_direct_light": f"{TOPIC_ROOT}/{NODE_TOPIC_ID}/cmd/direct/light",
    "cmd_config_planting_start": f"{TOPIC_ROOT}/{NODE_TOPIC_ID}/cmd/config/planting_start",
}

MQTT_BROKER = os.getenv("MQTT_BROKER", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_QOS = int(os.getenv("MQTT_QOS", "1"))
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "plant_backend_realtime_mqtt")

INFLUX_URL = os.getenv("INFLUX_URL") or os.getenv("INFLUXDB_URL") or "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN") or os.getenv("INFLUXDB_TOKEN") or ""
INFLUX_ORG = os.getenv("INFLUX_ORG") or os.getenv("INFLUXDB_ORG") or "DEV_TEAM"
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET") or os.getenv("INFLUXDB_BUCKET") or "digital_twin_data"

REALTIME_INGEST_TOKEN = os.getenv("REALTIME_INGEST_TOKEN", "")
COMMAND_MODE = os.getenv("COMMAND_MODE", "mqtt_direct").strip().lower()
# mqtt_direct: Backend publishes MQTT immediately and logs dt status=SENT.
# db_queue   : Backend only writes dt status=PENDING; Gateway polls DB and sends MQTT.

MEAS_SENSORS = "sensors"
MEAS_STATUS = "status"
MEAS_ACTUATOR = "actuator"
MEAS_CMD = "cmd"
MEAS_DT = "dt"

WRITE_PRECISION_SECONDS = getattr(WritePrecision, "S", None) or getattr(WritePrecision, "SECONDS")

# =============================================================================
# APP + CORS
# =============================================================================

app = FastAPI(
    title="Plant Monitoring CPS Web API",
    description="Realtime MQTT/WebSocket backend with InfluxDB history for Plant Monitoring CPS.",
    version=API_VERSION,
)

cors_origins_raw = os.getenv(
    "CORS_ORIGINS",
    "http://localhost:5173,http://127.0.0.1:5173,http://192.168.4.1:5173,http://10.42.0.217:5173,http://100.110.157.78:5173",
)
CORS_ORIGINS = [x.strip() for x in cors_origins_raw.split(",") if x.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# MODELS
# =============================================================================

class PumpCommand(BaseModel):
    state: Literal["ON", "OFF"]
    duration_s: int = Field(default=10, ge=0, le=15)
    reason: str = "web_manual"
    source: str = "web"


class LightCommand(BaseModel):
    state: Literal["ON", "OFF"]
    duration_s: int = Field(default=300, ge=0, le=1800)
    reason: str = "web_manual"
    source: str = "web"


class PlantingStartCommand(BaseModel):
    action: Literal["SET_NOW", "SET_EPOCH", "CLEAR", "GET"] = "SET_NOW"
    planting_start_epoch: int | None = Field(default=None, ge=1)
    reason: str = "web_planting_start"
    source: str = "web"


class RealtimeIngestEvent(BaseModel):
    type: str
    source: str = "gateway"
    gateway_version: str | None = None
    node_id: str = NODE_ID
    topic: str | None = None
    timestamp: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)
    command_id: str | None = None
    target: str | None = None
    status: str | None = None

# =============================================================================
# HELPERS
# =============================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def unix_now_s() -> int:
    return int(time.time())


def make_command_id(source: str) -> str:
    src = (source or "web").strip().lower().replace(" ", "_")[:16]
    return f"{src}-{int(time.time() * 1000)}-{uuid.uuid4().hex[:6]}"


def check_env() -> None:
    if not INFLUX_TOKEN:
        raise HTTPException(status_code=500, detail="Missing INFLUX_TOKEN in backend .env")


def get_client() -> InfluxDBClient:
    check_env()
    return InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=30000)


def clean_value(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        if np.isnan(value) or np.isinf(value):
            return None
        return float(value)
    if isinstance(value, float):
        if np.isnan(value) or np.isinf(value):
            return None
        return value
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (dict, list, tuple)):
        return value
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    for col in ("result", "table", "_start", "_stop"):
        if col in df.columns:
            df = df.drop(columns=[col])
    if "_time" in df.columns:
        df["_time"] = pd.to_datetime(df["_time"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return df.replace({np.nan: None})


def dataframe_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    df = normalize_dataframe(df)
    return [{str(k): clean_value(v) for k, v in row.items()} for row in df.to_dict(orient="records")]


def build_range_part(minutes: int | None = None, date: str | None = None) -> str:
    if date:
        try:
            start = datetime.fromisoformat(date).replace(tzinfo=timezone.utc)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD") from exc
        stop = start + timedelta(days=1)
        return f'|> range(start: {start.isoformat()}, stop: {stop.isoformat()})'
    safe_minutes = int(minutes or 720)
    safe_minutes = max(1, min(safe_minutes, 60 * 24 * 31))
    return f"|> range(start: -{safe_minutes}m)"


def query_measurement(measurement: str, minutes: int | None = 720, date: str | None = None, limit: int = 300) -> list[dict[str, Any]]:
    range_part = build_range_part(minutes=minutes, date=date)
    flux = f'''
from(bucket: "{INFLUX_BUCKET}")
  {range_part}
  |> filter(fn: (r) => r._measurement == "{measurement}")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"], desc: false)
  |> limit(n: {int(limit)})
'''
    with get_client() as client:
        df = client.query_api().query_data_frame(flux, org=INFLUX_ORG)
    if isinstance(df, list):
        df = pd.concat(df, ignore_index=True) if df else pd.DataFrame()
    if df.empty:
        return []
    return dataframe_to_records(df)


def parse_status_record(record: dict[str, Any] | None) -> dict[str, Any] | None:
    if not record:
        return None
    value_json = record.get("value_json")
    if isinstance(value_json, str):
        try:
            parsed = json.loads(value_json)
            if isinstance(parsed, dict):
                parsed.setdefault("_time", record.get("_time"))
                parsed.setdefault("status_key", record.get("key"))
                return parsed
        except json.JSONDecodeError:
            pass
    return record

# =============================================================================
# REALTIME CACHE + WEBSOCKET
# =============================================================================

class ConnectionManager:
    def __init__(self) -> None:
        self.active: list[WebSocket] = []
        self.lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self.lock:
            self.active.append(websocket)

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self.lock:
            if websocket in self.active:
                self.active.remove(websocket)

    async def broadcast(self, message: dict[str, Any]) -> None:
        dead: list[WebSocket] = []
        async with self.lock:
            clients = list(self.active)
        for ws in clients:
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        if dead:
            async with self.lock:
                for ws in dead:
                    if ws in self.active:
                        self.active.remove(ws)

    async def count(self) -> int:
        async with self.lock:
            return len(self.active)


manager = ConnectionManager()
LATEST: dict[str, Any] = {
    "sensors": None,
    "actuator": None,
    "status": None,
    "command_event": None,
    "dt_command": None,
    "updated_at": None,
}
EVENT_BUFFER: deque[dict[str, Any]] = deque(maxlen=300)


def flatten_sensor_payload(payload: dict[str, Any]) -> dict[str, Any]:
    sensor = payload.get("sensor", {}) if isinstance(payload.get("sensor"), dict) else {}
    ai = payload.get("ai", {}) if isinstance(payload.get("ai"), dict) else {}
    control = payload.get("control", {}) if isinstance(payload.get("control"), dict) else {}
    pump = control.get("pump", {}) if isinstance(control.get("pump"), dict) else {}
    light = control.get("light", {}) if isinstance(control.get("light"), dict) else {}

    flat = {
        "_time": payload.get("timestamp") or utc_now_iso(),
        "node_id": payload.get("node_id", NODE_ID),
        "plant": payload.get("plant", PLANT_NAME),
        "step": payload.get("step"),
        "gw_step": payload.get("gw_step"),
        "phase": payload.get("phase"),
        "phase_source": payload.get("phase_source"),
        "days_after_planting": payload.get("days_after_planting"),
        "planting_start_epoch": payload.get("planting_start_epoch"),
        "planting_start_valid": payload.get("planting_start_valid"),
        "uptime_s": payload.get("uptime_s"),
        "wifi_rssi": payload.get("wifi_rssi"),
        "alert": payload.get("alert"),
        "temperature": sensor.get("temperature"),
        "air_humidity": sensor.get("air_humidity"),
        "lux": sensor.get("lux"),
        "soil_moisture": sensor.get("soil_moisture"),
        "soil_moisture_fused": sensor.get("soil_moisture_fused"),
        "soil_moisture_mean": sensor.get("soil_moisture_mean"),
        "soil_moisture_min": sensor.get("soil_moisture_min"),
        "soil_moisture_max": sensor.get("soil_moisture_max"),
        "soil_presence_state": sensor.get("soil_presence_state"),
        "soil_control_reliable": sensor.get("soil_control_reliable"),
        "soil_sensor_fault": sensor.get("soil_sensor_fault"),
        "need_watering": ai.get("need_watering"),
        "ai_confidence": ai.get("confidence"),
        "ai_source": ai.get("source"),
        "ai_reason": ai.get("reason"),
        "pump_state": pump.get("state"),
        "pump_mode": pump.get("mode"),
        "pump_reason": pump.get("reason"),
        "light_state": light.get("state"),
        "light_mode": light.get("mode"),
        "light_reason": light.get("reason"),
    }
    return flat


def flatten_actuator_payload(payload: dict[str, Any]) -> dict[str, Any]:
    pump = payload.get("pump", {}) if isinstance(payload.get("pump"), dict) else {}
    light = payload.get("light", {}) if isinstance(payload.get("light"), dict) else {}
    return {
        "_time": payload.get("timestamp") or utc_now_iso(),
        "node_id": payload.get("node_id", NODE_ID),
        "pump_state": pump.get("state"),
        "pump_mode": pump.get("mode"),
        "pump_reason": pump.get("reason"),
        "light_state": light.get("state"),
        "light_mode": light.get("mode"),
        "light_reason": light.get("reason"),
    }


def apply_realtime_event(event: dict[str, Any]) -> dict[str, Any]:
    etype = str(event.get("type") or "unknown")
    data = event.get("data") if isinstance(event.get("data"), dict) else {}
    received_at = utc_now_iso()
    event["received_at"] = received_at

    latest_patch: dict[str, Any] = {}
    if etype == "sensor":
        LATEST["sensors"] = flatten_sensor_payload(data)
        latest_patch["sensors"] = LATEST["sensors"]
    elif etype == "actuator_state":
        LATEST["actuator"] = flatten_actuator_payload(data)
        latest_patch["actuator"] = LATEST["actuator"]
    elif etype in ("esp32_status", "planting_start_ack", "gateway_status"):
        status = dict(data)
        status.setdefault("_time", data.get("timestamp") or received_at)
        LATEST["status"] = status
        latest_patch["status"] = status
        if etype == "planting_start_ack":
            LATEST["command_event"] = {
                "_time": received_at,
                "command_id": event.get("command_id") or data.get("command_id"),
                "target": "planting_start",
                "status": event.get("status") or data.get("status"),
                "message": json.dumps(data, ensure_ascii=False),
            }
            latest_patch["command_event"] = LATEST["command_event"]
    elif etype == "command_sent":
        LATEST["command_event"] = {
            "_time": received_at,
            "command_id": event.get("command_id") or data.get("command_id"),
            "target": event.get("target") or data.get("target"),
            "status": event.get("status") or "SENT",
            "message": json.dumps(data, ensure_ascii=False),
        }
        latest_patch["command_event"] = LATEST["command_event"]

    LATEST["updated_at"] = received_at
    EVENT_BUFFER.append(event)
    return latest_patch

# =============================================================================
# MQTT COMMAND PUBLISHER
# =============================================================================

mqtt_client: mqtt.Client | None = None
mqtt_connected = False


def mqtt_on_connect(client, userdata, flags, rc):
    global mqtt_connected
    mqtt_connected = (rc == 0)


def mqtt_on_disconnect(client, userdata, rc):
    global mqtt_connected
    mqtt_connected = False


def init_mqtt() -> None:
    global mqtt_client
    mqtt_client = mqtt.Client(client_id=MQTT_CLIENT_ID, clean_session=True, protocol=mqtt.MQTTv311)
    mqtt_client.on_connect = mqtt_on_connect
    mqtt_client.on_disconnect = mqtt_on_disconnect
    mqtt_client.reconnect_delay_set(min_delay=2, max_delay=30)
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    mqtt_client.loop_start()


def publish_mqtt(topic: str, payload: dict[str, Any], retain: bool = False) -> None:
    if COMMAND_MODE == "db_queue":
        return
    if mqtt_client is None:
        raise HTTPException(status_code=503, detail="MQTT client is not initialized")
    info = mqtt_client.publish(topic, json.dumps(payload, ensure_ascii=False), qos=MQTT_QOS, retain=retain)
    info.wait_for_publish(timeout=2.0)
    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        raise HTTPException(status_code=503, detail=f"MQTT publish failed rc={info.rc}")

# =============================================================================
# INFLUX WRITES
# =============================================================================

def write_cmd_event(command_id: str, target: str, status: str, message: str = "") -> None:
    point = (
        Point(MEAS_CMD)
        .tag("node_id", NODE_ID)
        .tag("command_id", command_id)
        .tag("target", target)
        .tag("status", status)
        .field("message", message)
        .time(datetime.now(timezone.utc), WRITE_PRECISION_SECONDS)
    )
    with get_client() as client:
        client.write_api(write_options=SYNCHRONOUS).write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)


def write_dt_command_log(
    command_id: str,
    target: str,
    status: str,
    source: str,
    reason: str,
    state: str = "",
    duration_s: int = 0,
    action: str = "",
    planting_start_epoch: int | None = None,
    mqtt_topic: str = "",
) -> None:
    point = (
        Point(MEAS_DT)
        .tag("node_id", NODE_ID)
        .tag("command_id", command_id)
        .tag("target", target)
        .tag("status", status)
        .field("source", source or "web")
        .field("reason", reason or "web_command")
        .field("mqtt_topic", mqtt_topic)
        .time(datetime.now(timezone.utc), WRITE_PRECISION_SECONDS)
    )
    if target == "planting_start":
        point = point.field("action", action or "SET_EPOCH")
        if planting_start_epoch:
            point = point.field("planting_start_epoch", int(planting_start_epoch))
    else:
        point = point.field("state", state.upper()).field("duration_s", int(duration_s))
    with get_client() as client:
        client.write_api(write_options=SYNCHRONOUS).write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)

async def broadcast_command(command: dict[str, Any]) -> None:
    event = {
        "type": "command_sent",
        "source": "backend",
        "timestamp": utc_now_iso(),
        "data": command,
        "command_id": command.get("command_id"),
        "target": command.get("target"),
        "status": command.get("status", "SENT"),
        "latest": {
            "command_event": {
                "_time": utc_now_iso(),
                "command_id": command.get("command_id"),
                "target": command.get("target"),
                "status": command.get("status", "SENT"),
                "message": json.dumps(command, ensure_ascii=False),
            },
            "dt_command": command,
        },
    }
    LATEST["command_event"] = event["latest"]["command_event"]
    LATEST["dt_command"] = command
    LATEST["updated_at"] = event["timestamp"]
    EVENT_BUFFER.append(event)
    await manager.broadcast(event)

async def create_and_send_command(target: str, payload: dict[str, Any], source: str, reason: str) -> dict[str, Any]:
    command_id = payload["command_id"]
    topic = ""
    retain = False

    if target == "pump":
        topic = TOPICS["cmd_direct_pump"]
    elif target == "light":
        topic = TOPICS["cmd_direct_light"]
    elif target == "planting_start":
        topic = TOPICS["cmd_config_planting_start"]
        # SET_EPOCH/CLEAR are config state, retained for reconnect. GET is one-shot.
        retain = payload.get("action") in ("SET_EPOCH", "CLEAR")
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported target={target}")

    status = "PENDING" if COMMAND_MODE == "db_queue" else "SENT"
    try:
        if COMMAND_MODE == "mqtt_direct":
            publish_mqtt(topic, payload, retain=retain)
        write_dt_command_log(
            command_id=command_id,
            target=target,
            status=status,
            source=source,
            reason=reason,
            state=payload.get("state", ""),
            duration_s=int(payload.get("duration_s") or 0),
            action=payload.get("action", ""),
            planting_start_epoch=payload.get("planting_start_epoch"),
            mqtt_topic=topic,
        )
        write_cmd_event(command_id, target, status, json.dumps(payload, ensure_ascii=False))
    except Exception as exc:
        try:
            write_dt_command_log(command_id, target, "ERROR", source, str(exc), mqtt_topic=topic)
            write_cmd_event(command_id, target, "ERROR", str(exc))
        except Exception:
            pass
        raise

    result = {
        "ok": True,
        "command_id": command_id,
        "target": target,
        "status": status,
        "mode": COMMAND_MODE,
        "mqtt_topic": topic,
        "payload": payload,
        "created_at": utc_now_iso(),
        "message": "Command published to MQTT immediately and logged to InfluxDB." if COMMAND_MODE == "mqtt_direct" else "Command queued in InfluxDB dt. Gateway will poll and publish MQTT.",
    }
    await broadcast_command({**payload, "status": status, "mqtt_topic": topic})
    return result

# =============================================================================
# STARTUP/SHUTDOWN
# =============================================================================

@app.on_event("startup")
def on_startup() -> None:
    init_mqtt()


@app.on_event("shutdown")
def on_shutdown() -> None:
    global mqtt_client
    if mqtt_client is not None:
        try:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
        except Exception:
            pass

# =============================================================================
# ROUTES
# =============================================================================

@app.get("/")
def root() -> dict[str, Any]:
    return {
        "name": "Plant Monitoring CPS Web API",
        "version": API_VERSION,
        "node_id": NODE_ID,
        "node_topic_id": NODE_TOPIC_ID,
        "plant": PLANT_NAME,
        "docs": "/docs",
        "websocket": "/ws/realtime",
    }


@app.get("/api/health")
def health() -> dict[str, Any]:
    check_env()
    influx_ok = False
    try:
        with get_client() as client:
            influx_ok = bool(client.ping())
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"InfluxDB error: {exc}") from exc
    return {
        "status": "OK" if influx_ok else "ERROR",
        "api_version": API_VERSION,
        "node_id": NODE_ID,
        "node_topic_id": NODE_TOPIC_ID,
        "plant": PLANT_NAME,
        "influx_url": INFLUX_URL,
        "org": INFLUX_ORG,
        "bucket": INFLUX_BUCKET,
        "command_mode": COMMAND_MODE,
        "mqtt": {"broker": MQTT_BROKER, "port": MQTT_PORT, "connected": mqtt_connected},
        "websocket_clients": 0,  # set by /api/realtime/latest for async count
        "measurements": [MEAS_SENSORS, MEAS_STATUS, MEAS_ACTUATOR, MEAS_CMD, MEAS_DT],
        "topics_v2": TOPICS,
        "timestamp": utc_now_iso(),
    }


@app.get("/api/dashboard/latest")
def dashboard_latest(
    minutes: int = Query(default=1440, ge=1, le=60 * 24 * 31),
    date: str | None = Query(default=None),
    prefer_realtime: bool = Query(default=True),
) -> dict[str, Any]:
    sensors_rows = query_measurement(MEAS_SENSORS, minutes=minutes, date=date, limit=500)
    actuator_rows = query_measurement(MEAS_ACTUATOR, minutes=minutes, date=date, limit=500)
    status_rows = query_measurement(MEAS_STATUS, minutes=minutes, date=date, limit=500)
    cmd_rows = query_measurement(MEAS_CMD, minutes=minutes, date=date, limit=200)
    dt_rows = query_measurement(MEAS_DT, minutes=minutes, date=date, limit=200)

    sensors = sensors_rows[-1] if sensors_rows else None
    actuator = actuator_rows[-1] if actuator_rows else None
    status = parse_status_record(status_rows[-1]) if status_rows else None
    command_event = cmd_rows[-1] if cmd_rows else None
    dt_command = dt_rows[-1] if dt_rows else None

    if prefer_realtime:
        sensors = LATEST.get("sensors") or sensors
        actuator = LATEST.get("actuator") or actuator
        status = LATEST.get("status") or status
        command_event = LATEST.get("command_event") or command_event
        dt_command = LATEST.get("dt_command") or dt_command

    return {
        "node_id": NODE_ID,
        "plant": PLANT_NAME,
        "api_version": API_VERSION,
        "realtime_updated_at": LATEST.get("updated_at"),
        "sensors": sensors,
        "actuator": actuator,
        "status": status,
        "command_event": command_event,
        "dt_command": dt_command,
        "counts": {
            "sensors": len(sensors_rows),
            "actuator": len(actuator_rows),
            "status": len(status_rows),
            "cmd": len(cmd_rows),
            "dt": len(dt_rows),
        },
        "timestamp": utc_now_iso(),
    }


@app.get("/api/realtime/latest")
async def realtime_latest() -> dict[str, Any]:
    return {
        "latest": LATEST,
        "buffer_count": len(EVENT_BUFFER),
        "websocket_clients": await manager.count(),
        "timestamp": utc_now_iso(),
    }


@app.post("/api/realtime/ingest")
async def realtime_ingest(event: RealtimeIngestEvent, x_realtime_token: str | None = Header(default=None)) -> dict[str, Any]:
    if REALTIME_INGEST_TOKEN and x_realtime_token != REALTIME_INGEST_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid realtime ingest token")
    event_dict = event.model_dump() if hasattr(event, "model_dump") else event.dict()
    latest_patch = apply_realtime_event(event_dict)
    out = {**event_dict, "latest": latest_patch}
    await manager.broadcast(out)
    return {"ok": True, "type": event.type, "latest_keys": list(latest_patch.keys()), "timestamp": utc_now_iso()}


@app.websocket("/ws/realtime")
async def websocket_realtime(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        await websocket.send_json({
            "type": "hello",
            "source": "backend",
            "api_version": API_VERSION,
            "latest": LATEST,
            "timestamp": utc_now_iso(),
        })
        while True:
            # Keep the connection alive and allow client ping messages.
            try:
                msg = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                if msg.strip().lower() == "ping":
                    await websocket.send_json({"type": "pong", "timestamp": utc_now_iso()})
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "heartbeat", "timestamp": utc_now_iso()})
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
    except Exception:
        await manager.disconnect(websocket)


@app.get("/api/history/{measurement}")
def history(
    measurement: Literal["sensors", "actuator", "status", "cmd", "dt"],
    minutes: int = Query(default=720, ge=1, le=60 * 24 * 31),
    date: str | None = Query(default=None),
    limit: int = Query(default=300, ge=1, le=5000),
) -> dict[str, Any]:
    rows = query_measurement(measurement, minutes=minutes, date=date, limit=limit)
    if measurement == MEAS_STATUS:
        rows = [parse_status_record(row) or row for row in rows]
    return {"measurement": measurement, "count": len(rows), "data": rows}


@app.get("/api/history/sensors")
def history_sensors(
    minutes: int = Query(default=720, ge=1, le=60 * 24 * 31),
    date: str | None = Query(default=None),
    limit: int = Query(default=300, ge=1, le=5000),
) -> dict[str, Any]:
    rows = query_measurement(MEAS_SENSORS, minutes=minutes, date=date, limit=limit)
    return {"measurement": MEAS_SENSORS, "count": len(rows), "data": rows}


@app.get("/api/history/actuator")
def history_actuator(
    minutes: int = Query(default=720, ge=1, le=60 * 24 * 31),
    date: str | None = Query(default=None),
    limit: int = Query(default=300, ge=1, le=5000),
) -> dict[str, Any]:
    rows = query_measurement(MEAS_ACTUATOR, minutes=minutes, date=date, limit=limit)
    return {"measurement": MEAS_ACTUATOR, "count": len(rows), "data": rows}


@app.get("/api/history/status")
def history_status(
    minutes: int = Query(default=720, ge=1, le=60 * 24 * 31),
    date: str | None = Query(default=None),
    limit: int = Query(default=300, ge=1, le=5000),
) -> dict[str, Any]:
    rows = query_measurement(MEAS_STATUS, minutes=minutes, date=date, limit=limit)
    rows = [parse_status_record(row) or row for row in rows]
    return {"measurement": MEAS_STATUS, "count": len(rows), "data": rows}


@app.post("/api/command/pump")
async def command_pump(cmd: PumpCommand) -> dict[str, Any]:
    duration = 0 if cmd.state == "OFF" else cmd.duration_s
    command_id = make_command_id(cmd.source)
    payload = {
        "id": command_id,
        "command_id": command_id,
        "source": cmd.source,
        "mode": "DIRECT",
        "target": "pump",
        "state": cmd.state,
        "duration_s": duration,
        "reason": cmd.reason,
        "sent_at": utc_now_iso(),
    }
    return await create_and_send_command("pump", payload, cmd.source, cmd.reason)


@app.post("/api/command/light")
async def command_light(cmd: LightCommand) -> dict[str, Any]:
    duration = 0 if cmd.state == "OFF" else cmd.duration_s
    command_id = make_command_id(cmd.source)
    payload = {
        "id": command_id,
        "command_id": command_id,
        "source": cmd.source,
        "mode": "DIRECT",
        "target": "light",
        "state": cmd.state,
        "duration_s": duration,
        "reason": cmd.reason,
        "sent_at": utc_now_iso(),
    }
    return await create_and_send_command("light", payload, cmd.source, cmd.reason)


@app.post("/api/command/planting-start")
async def command_planting_start(cmd: PlantingStartCommand) -> dict[str, Any]:
    command_id = make_command_id(cmd.source)
    action = cmd.action
    epoch = cmd.planting_start_epoch
    # Avoid retained SET_NOW replay later. Convert SET_NOW to a stable SET_EPOCH at backend time.
    if action == "SET_NOW":
        action = "SET_EPOCH"
        epoch = unix_now_s()
    if action == "SET_EPOCH" and not epoch:
        raise HTTPException(status_code=400, detail="SET_EPOCH requires planting_start_epoch")
    payload = {
        "id": command_id,
        "command_id": command_id,
        "source": cmd.source,
        "target": "planting_start",
        "action": action,
        "reason": cmd.reason,
        "sent_at": utc_now_iso(),
    }
    if action == "SET_EPOCH":
        payload["planting_start_epoch"] = int(epoch or 0)
    return await create_and_send_command("planting_start", payload, cmd.source, cmd.reason)


@app.get("/api/export/sensors.csv")
def export_sensors_csv(minutes: int = Query(default=720, ge=1, le=60 * 24 * 31), date: str | None = Query(default=None)) -> StreamingResponse:
    rows = query_measurement(MEAS_SENSORS, minutes=minutes, date=date, limit=5000)
    df = pd.DataFrame(rows)
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    filename = f"plant_sensors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return StreamingResponse(iter([buffer.getvalue()]), media_type="text/csv", headers={"Content-Disposition": f"attachment; filename={filename}"})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
