"""
Plant Monitoring CPS Backend
Version: v1.3.3-low-latency-db-async

Mục tiêu bản v1.3.0:
- Web chạy trước, Unity dùng lại cùng API/WebSocket qua Tailscale.
- Realtime không phụ thuộc InfluxDB: Gateway -> /api/realtime/ingest -> RAM cache -> WebSocket.
- Command Pump/Light chống spam bằng debounce + rate-limit + coalesce latest command.
- Command dùng MQTT QoS1 + command_id + seq + ACK/retry/timeout ở tầng ứng dụng.
- Planting Start dùng retained MQTT command cho đến khi ESP32 ACK, sau đó clear retained payload.

Flow:
ESP32 -> MQTT -> Gateway -> Backend ingest -> WebSocket -> Web/Unity
Web/Unity -> Backend command controller -> MQTT QoS1 -> ESP32 -> ACK/state -> Backend -> Web/Unity
InfluxDB chỉ dùng cho history/log/recovery, không làm realtime bus.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
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

API_VERSION = "1.3.5-int32-safe-epoch-seq"
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
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "plant_backend_v1_3_3")

INFLUX_URL = os.getenv("INFLUX_URL") or os.getenv("INFLUXDB_URL") or "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN") or os.getenv("INFLUXDB_TOKEN") or ""
INFLUX_ORG = os.getenv("INFLUX_ORG") or os.getenv("INFLUXDB_ORG") or "DEV_TEAM"
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET") or os.getenv("INFLUXDB_BUCKET") or "digital_twin_data"

REALTIME_INGEST_TOKEN = os.getenv("REALTIME_INGEST_TOKEN", "")

COMMAND_ACK_TIMEOUT_S = float(os.getenv("COMMAND_ACK_TIMEOUT_S", "1.8"))
COMMAND_MAX_RETRIES = int(os.getenv("COMMAND_MAX_RETRIES", "3"))
COMMAND_RETRY_SCAN_S = float(os.getenv("COMMAND_RETRY_SCAN_S", "0.20"))
COMMAND_TTL_S = float(os.getenv("COMMAND_TTL_S", "7.0"))
DIRECT_COMMAND_TTL_S = float(os.getenv("DIRECT_COMMAND_TTL_S", "15.0"))

PUMP_DEBOUNCE_MS = int(os.getenv("PUMP_DEBOUNCE_MS", "150"))
LIGHT_DEBOUNCE_MS = int(os.getenv("LIGHT_DEBOUNCE_MS", "100"))
PUMP_MIN_INTERVAL_S = float(os.getenv("PUMP_MIN_INTERVAL_S", "0.7"))
LIGHT_MIN_INTERVAL_S = float(os.getenv("LIGHT_MIN_INTERVAL_S", "0.25"))
PUMP_MAX_DURATION_S = int(os.getenv("PUMP_MAX_DURATION_S", "15"))
LIGHT_MAX_DURATION_S = int(os.getenv("LIGHT_MAX_DURATION_S", "1800"))

MEAS_SENSORS = "sensors"
MEAS_STATUS = "status"
MEAS_ACTUATOR = "actuator"
MEAS_CMD = "cmd"
MEAS_DT = "dt"
WRITE_PRECISION_SECONDS = getattr(WritePrecision, "S", None) or getattr(WritePrecision, "SECONDS")

# =============================================================================
# APP
# =============================================================================

app = FastAPI(
    title="Plant Monitoring CPS Web API",
    description="Web-first realtime MQTT/WebSocket backend with low-latency command controller.",
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
    duration_s: int = Field(default=10, ge=0, le=PUMP_MAX_DURATION_S)
    reason: str = "web_manual"
    source: str = "web"


class LightCommand(BaseModel):
    state: Literal["ON", "OFF"]
    duration_s: int = Field(default=300, ge=0, le=LIGHT_MAX_DURATION_S)
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
# BASIC HELPERS
# =============================================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def unix_now_s() -> int:
    return int(time.time())


def safe_upper(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip().upper() or default


def model_to_dict(model: BaseModel) -> dict[str, Any]:
    return model.model_dump() if hasattr(model, "model_dump") else model.dict()


def make_command_id(source: str, target: str) -> str:
    src = (source or "web").strip().lower().replace(" ", "_")[:12]
    tgt = (target or "cmd").strip().lower().replace(" ", "_")[:16]
    return f"{src}-{tgt}-{int(time.time() * 1000)}-{uuid.uuid4().hex[:6]}"


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
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def dataframe_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    for col in ("result", "table", "_start", "_stop"):
        if col in df.columns:
            df = df.drop(columns=[col])
    if "_time" in df.columns:
        df["_time"] = pd.to_datetime(df["_time"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df = df.replace({np.nan: None})
    return [{str(k): clean_value(v) for k, v in row.items()} for row in df.to_dict(orient="records")]

# =============================================================================
# INFLUXDB
# =============================================================================

def check_influx_env() -> None:
    if not INFLUX_TOKEN:
        raise HTTPException(status_code=500, detail="Missing INFLUX_TOKEN / INFLUXDB_TOKEN in backend .env")


def get_influx_client() -> InfluxDBClient:
    check_influx_env()
    return InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=30000)


def query_measurement(measurement: str, minutes: int = 30, date: str | None = None, limit: int = 300) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 5000))
    if date:
        try:
            start = datetime.fromisoformat(date).replace(tzinfo=timezone.utc)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD") from exc
        stop = start + timedelta(days=1)
        range_part = f"|> range(start: {start.isoformat()}, stop: {stop.isoformat()})"
    else:
        safe_minutes = max(1, min(int(minutes), 60 * 24 * 31))
        range_part = f"|> range(start: -{safe_minutes}m)"

    flux = f'''
from(bucket: "{INFLUX_BUCKET}")
  {range_part}
  |> filter(fn: (r) => r["_measurement"] == "{measurement}")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"])
  |> tail(n: {safe_limit})
'''
    try:
        with get_influx_client() as client:
            df = client.query_api().query_data_frame(org=INFLUX_ORG, query=flux)
        if isinstance(df, list):
            df = pd.concat(df, ignore_index=True) if df else pd.DataFrame()
        if df is None or df.empty:
            return []
        return dataframe_to_records(df)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"InfluxDB query error: {exc}") from exc


def write_influx_point(point: Point) -> None:
    try:
        with get_influx_client() as client:
            client.write_api(write_options=SYNCHRONOUS).write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
    except Exception as exc:
        # Command realtime must not crash only because Cloud history is slow/down.
        print(f"[WARN] Influx write failed: {exc}")


def write_cmd_event(command_id: str, target: str, status: str, message: str = "", seq: int = 0) -> None:
    point = (
        Point(MEAS_CMD)
        .tag("node_id", NODE_ID)
        .tag("command_id", command_id)
        .tag("target", target)
        .tag("status", status)
        .field("message", message or "")
        .field("seq", int(seq or 0))
        .time(utc_now(), WRITE_PRECISION_SECONDS)
    )
    write_influx_point(point)


def write_dt_command_log(command: dict[str, Any], status: str, message: str = "") -> None:
    target = str(command.get("target") or "unknown")
    command_id = str(command.get("command_id") or command.get("id") or "")
    point = (
        Point(MEAS_DT)
        .tag("node_id", NODE_ID)
        .tag("command_id", command_id)
        .tag("target", target)
        .tag("status", status)
        .field("source", str(command.get("source") or "web"))
        .field("reason", str(command.get("reason") or ""))
        .field("message", message or "")
        .field("seq", int(command.get("seq") or 0))
        .field("mqtt_topic", str(command.get("mqtt_topic") or ""))
        .time(utc_now(), WRITE_PRECISION_SECONDS)
    )
    if target in ("pump", "light"):
        point = point.field("state", str(command.get("state") or "")).field("duration_s", int(command.get("duration_s") or 0))
    if target == "planting_start":
        point = point.field("action", str(command.get("action") or ""))
        if command.get("planting_start_epoch") is not None:
            point = point.field("planting_start_epoch", int(command.get("planting_start_epoch") or 0))
    write_influx_point(point)



def schedule_background_call(func, *args, **kwargs) -> None:
    """Run slow side-effect work without blocking realtime MQTT command publish.

    InfluxDB Cloud writes can take 1-2 seconds on this Pi/network. In older
    versions, command endpoints wrote QUEUED/SENT to InfluxDB before the MQTT
    packet reached Mosquitto, which made broker-side latency look like packet
    loss. For realtime direct pump/light commands, MQTT publish is the first
    priority; DB logging is best-effort background work.
    """
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(asyncio.to_thread(func, *args, **kwargs))
    except RuntimeError:
        try:
            func(*args, **kwargs)
        except Exception:
            pass


def log_cmd_event(cmd_id: str, target: str, status: str, message: str = "", seq: int = 0, background: bool = False) -> None:
    if background:
        schedule_background_call(write_cmd_event, cmd_id, target, status, message, seq)
    else:
        write_cmd_event(cmd_id, target, status, message, seq)


def log_dt_command(command: dict[str, Any], status: str, message: str = "", background: bool = False) -> None:
    if background:
        schedule_background_call(write_dt_command_log, command, status, message)
    else:
        write_dt_command_log(command, status, message)


# =============================================================================
# WEBSOCKET + REALTIME CACHE
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
EVENT_BUFFER: deque[dict[str, Any]] = deque(maxlen=500)


def flatten_sensor_payload(payload: dict[str, Any]) -> dict[str, Any]:
    sensor = payload.get("sensor", {}) if isinstance(payload.get("sensor"), dict) else {}
    ai = payload.get("ai", {}) if isinstance(payload.get("ai"), dict) else {}
    control = payload.get("control", {}) if isinstance(payload.get("control"), dict) else {}
    pump = control.get("pump", {}) if isinstance(control.get("pump"), dict) else {}
    light = control.get("light", {}) if isinstance(control.get("light"), dict) else {}
    return {
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


def flatten_actuator_payload(payload: dict[str, Any]) -> dict[str, Any]:
    pump = payload.get("pump", {}) if isinstance(payload.get("pump"), dict) else {}
    light = payload.get("light", {}) if isinstance(payload.get("light"), dict) else {}
    return {
        "_time": payload.get("timestamp") or utc_now_iso(),
        "node_id": payload.get("node_id", NODE_ID),
        "event": payload.get("event"),
        "command_id": payload.get("command_id") or payload.get("id"),
        "seq": payload.get("seq"),
        "last_pump_command_id": payload.get("last_pump_command_id"),
        "last_pump_seq": payload.get("last_pump_seq"),
        "last_light_command_id": payload.get("last_light_command_id"),
        "last_light_seq": payload.get("last_light_seq"),
        "pump_state": pump.get("state"),
        "pump_mode": pump.get("mode"),
        "pump_reason": pump.get("reason"),
        "light_state": light.get("state"),
        "light_mode": light.get("mode"),
        "light_reason": light.get("reason"),
    }


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
# MQTT PUBLISHER
# =============================================================================

mqtt_client: mqtt.Client | None = None
mqtt_connected = False


def mqtt_on_connect(client, userdata, flags, rc):
    global mqtt_connected
    mqtt_connected = (rc == 0)
    print(f"[MQTT] connected={mqtt_connected} rc={rc}")


def mqtt_on_disconnect(client, userdata, rc):
    global mqtt_connected
    mqtt_connected = False
    print(f"[MQTT] disconnected rc={rc}")


def init_mqtt() -> None:
    global mqtt_client
    mqtt_client = mqtt.Client(client_id=MQTT_CLIENT_ID, clean_session=True, protocol=mqtt.MQTTv311)
    mqtt_client.on_connect = mqtt_on_connect
    mqtt_client.on_disconnect = mqtt_on_disconnect
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=20)
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    mqtt_client.loop_start()


def publish_mqtt(topic: str, payload: dict[str, Any], retain: bool = False) -> None:
    if mqtt_client is None:
        raise HTTPException(status_code=503, detail="MQTT client is not initialized")
    info = mqtt_client.publish(topic, json.dumps(payload, ensure_ascii=False), qos=MQTT_QOS, retain=retain)
    # Do not block the HTTP command path waiting for broker PUBACK.
    # Application-level ACK from ESP32/state is the real completion signal.
    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        raise HTTPException(status_code=503, detail=f"MQTT publish failed rc={info.rc}")


def clear_retained(topic: str) -> None:
    if mqtt_client is None:
        return
    try:
        info = mqtt_client.publish(topic, payload="", qos=MQTT_QOS, retain=True)
        info.wait_for_publish(timeout=1.0)
    except Exception as exc:
        print(f"[WARN] clear retained failed topic={topic}: {exc}")

# =============================================================================
# COMMAND CONTROLLER
# =============================================================================

@dataclass
class PendingCommand:
    command_id: str
    target: str
    desired_state: str = ""
    seq: int = 0
    payload: dict[str, Any] = field(default_factory=dict)
    topic: str = ""
    retain: bool = False
    source: str = "web"
    reason: str = ""
    created_monotonic: float = field(default_factory=time.monotonic)
    sent_monotonic: float = 0.0
    next_deadline: float = 0.0
    retries: int = 0
    status: str = "QUEUED"
    last_error: str = ""


COMMAND_LOCK = asyncio.Lock()
PENDING_COMMANDS: dict[str, PendingCommand] = {}
LATEST_DESIRED_BY_TARGET: dict[str, PendingCommand] = {}
TARGET_TASKS: dict[str, asyncio.Task] = {}
TARGET_LAST_SENT_AT: dict[str, float] = {"pump": 0.0, "light": 0.0, "planting_start": 0.0}
TARGET_SEQ: dict[str, int] = {"pump": 0, "light": 0, "planting_start": 0}
RETRY_TASK: asyncio.Task | None = None


def topic_for_target(target: str) -> tuple[str, bool]:
    if target == "pump":
        return TOPICS["cmd_direct_pump"], False
    if target == "light":
        return TOPICS["cmd_direct_light"], False
    if target == "planting_start":
        return TOPICS["cmd_config_planting_start"], True
    raise HTTPException(status_code=400, detail=f"Unsupported target={target}")


def controller_config(target: str) -> tuple[float, float]:
    if target == "pump":
        return PUMP_DEBOUNCE_MS / 1000.0, PUMP_MIN_INTERVAL_S
    if target == "light":
        return LIGHT_DEBOUNCE_MS / 1000.0, LIGHT_MIN_INTERVAL_S
    return 0.0, 0.0


def is_direct_target(target: str) -> bool:
    return target in ("pump", "light")


def next_seq_for_target(target: str) -> int:
    """Return a monotonic command sequence for the target.

    v1.3.5: direct actuator commands use an int32-safe epoch-seconds sequence
    instead of epoch-ms. ESP32 cJSON valueint is int32; epoch-ms overflows to
    2147483647, which can make later commands look stale. Epoch-seconds is
    monotonic across Backend restarts and still fits signed int32 until 2038.
    """
    if is_direct_target(target):
        now_seq = int(time.time())  # int32-safe epoch seconds
        seq = max(now_seq, TARGET_SEQ.get(target, 0) + 1)
    else:
        seq = TARGET_SEQ.get(target, 0) + 1
    TARGET_SEQ[target] = seq
    return seq


def enrich_direct_payload(payload: dict[str, Any], target: str) -> dict[str, Any]:
    """Add safety metadata for direct actuator commands.

    ESP32 accepts command_id/seq and newer firmware uses ttl_s/expires_at_ms to
    drop stale commands after reconnect. v1.3.5 keeps seq int32-safe while
    created_at_ms/expires_at_ms remain epoch-ms for TTL checks.
    """
    if not is_direct_target(target):
        return payload
    created_at_ms = int(payload.get("created_at_ms") or int(time.time() * 1000))
    ttl_s = float(payload.get("ttl_s") or DIRECT_COMMAND_TTL_S)
    expires_at_ms = int(payload.get("expires_at_ms") or (created_at_ms + int(ttl_s * 1000)))
    action = safe_upper(payload.get("action") or payload.get("state"))
    return {
        **payload,
        "action": action,
        "ttl_s": ttl_s,
        "created_at_ms": created_at_ms,
        "expires_at_ms": expires_at_ms,
    }


def command_is_latest_for_target(cmd: PendingCommand) -> bool:
    if not is_direct_target(cmd.target):
        return True
    return cmd.seq >= TARGET_SEQ.get(cmd.target, 0)


async def supersede_pending_target(target: str, new_command_id: str) -> list[PendingCommand]:
    """Remove older pending direct commands for the same actuator.

    This prevents an old retry from being published after a newer user command,
    which previously looked like packet loss or command ordering bugs.
    """
    superseded: list[PendingCommand] = []
    if not is_direct_target(target):
        return superseded
    async with COMMAND_LOCK:
        for cid, old in list(PENDING_COMMANDS.items()):
            if old.target == target and old.command_id != new_command_id:
                superseded.append(PENDING_COMMANDS.pop(cid))
    return superseded


async def broadcast_event(event: dict[str, Any]) -> None:
    EVENT_BUFFER.append(event)
    await manager.broadcast(event)


async def broadcast_command_status(cmd: PendingCommand, status: str, message: str = "") -> None:
    event = {
        "type": "command_event",
        "source": "backend",
        "timestamp": utc_now_iso(),
        "command_id": cmd.command_id,
        "target": cmd.target,
        "status": status,
        "message": message,
        "data": {
            **cmd.payload,
            "status": status,
            "message": message,
            "retry_count": cmd.retries,
            "mqtt_topic": cmd.topic,
        },
    }
    LATEST["command_event"] = {
        "_time": event["timestamp"],
        "command_id": cmd.command_id,
        "target": cmd.target,
        "status": status,
        "message": message,
        "retry_count": cmd.retries,
    }
    LATEST["dt_command"] = {**cmd.payload, "status": status, "mqtt_topic": cmd.topic, "retry_count": cmd.retries}
    LATEST["updated_at"] = event["timestamp"]
    await broadcast_event(event)


async def mark_command_done(cmd: PendingCommand, status: str = "DONE", message: str = "") -> None:
    async with COMMAND_LOCK:
        existing = PENDING_COMMANDS.pop(cmd.command_id, None)
        if existing is None:
            return
        cmd.status = status
    bg_log = is_direct_target(cmd.target)
    log_cmd_event(cmd.command_id, cmd.target, status, message, seq=cmd.seq, background=bg_log)
    log_dt_command({**cmd.payload, "mqtt_topic": cmd.topic}, status, message, background=bg_log)
    await broadcast_command_status(cmd, status, message)
    if cmd.target == "planting_start" and cmd.retain and status in ("DONE", "DUPLICATE", "ERROR"):
        clear_retained(cmd.topic)


async def publish_pending_command(cmd: PendingCommand, first_send: bool = False) -> None:
    try:
        publish_mqtt(cmd.topic, cmd.payload, retain=cmd.retain)
        cmd.sent_monotonic = time.monotonic()
        cmd.next_deadline = cmd.sent_monotonic + COMMAND_ACK_TIMEOUT_S
        cmd.status = "SENT" if first_send else "RETRY"
        bg_log = is_direct_target(cmd.target)
        log_cmd_event(cmd.command_id, cmd.target, cmd.status, json.dumps(cmd.payload, ensure_ascii=False), seq=cmd.seq, background=bg_log)
        log_dt_command({**cmd.payload, "mqtt_topic": cmd.topic}, cmd.status, background=bg_log)
        await broadcast_command_status(cmd, cmd.status, "published to MQTT QoS1")
    except Exception as exc:
        cmd.status = "ERROR"
        cmd.last_error = str(exc)
        bg_log = is_direct_target(cmd.target)
        log_cmd_event(cmd.command_id, cmd.target, "ERROR", str(exc), seq=cmd.seq, background=bg_log)
        log_dt_command({**cmd.payload, "mqtt_topic": cmd.topic}, "ERROR", str(exc), background=bg_log)
        await broadcast_command_status(cmd, "ERROR", str(exc))
        async with COMMAND_LOCK:
            PENDING_COMMANDS.pop(cmd.command_id, None)


async def delayed_send_for_target(target: str) -> None:
    debounce_s, min_interval_s = controller_config(target)
    if debounce_s > 0:
        await asyncio.sleep(debounce_s)

    async with COMMAND_LOCK:
        cmd = LATEST_DESIRED_BY_TARGET.pop(target, None)
    if cmd is None:
        return

    # Rate-limit actuator commands, but always re-check latest after waiting.
    wait_s = max(0.0, min_interval_s - (time.monotonic() - TARGET_LAST_SENT_AT.get(target, 0.0)))
    if wait_s > 0:
        await asyncio.sleep(wait_s)
        async with COMMAND_LOCK:
            newer = LATEST_DESIRED_BY_TARGET.pop(target, None)
        if newer is not None:
            await broadcast_command_status(cmd, "SUPERSEDED", f"replaced by newer {target} command before publish")
            cmd = newer

    # Do not publish stale queued commands if another command arrived while waiting.
    if not command_is_latest_for_target(cmd):
        await broadcast_command_status(cmd, "SUPERSEDED", f"stale {target} command seq={cmd.seq}; latest={TARGET_SEQ.get(target, 0)}")
        return

    for old in await supersede_pending_target(target, cmd.command_id):
        await broadcast_command_status(old, "SUPERSEDED", f"replaced by newer {target} command_id={cmd.command_id}")

    TARGET_LAST_SENT_AT[target] = time.monotonic()
    async with COMMAND_LOCK:
        PENDING_COMMANDS[cmd.command_id] = cmd
    await publish_pending_command(cmd, first_send=True)


async def submit_coalesced_command(target: str, payload: dict[str, Any], source: str, reason: str) -> dict[str, Any]:
    topic, retain = topic_for_target(target)
    seq = next_seq_for_target(target)
    command_id = payload.get("command_id") or make_command_id(source, target)
    payload = {
        **payload,
        "id": command_id,
        "command_id": command_id,
        "seq": seq,
        "target": target,
        "source": source,
        "reason": reason,
        "sent_at": utc_now_iso(),
    }
    if is_direct_target(target):
        payload["created_at_ms"] = int(time.time() * 1000)
    payload = enrich_direct_payload(payload, target)
    cmd = PendingCommand(
        command_id=command_id,
        target=target,
        desired_state=safe_upper(payload.get("state") or payload.get("action")),
        seq=seq,
        payload=payload,
        topic=topic,
        retain=retain,
        source=source,
        reason=reason,
        status="QUEUED",
    )

    bg_log = is_direct_target(target)
    log_cmd_event(command_id, target, "QUEUED", json.dumps(payload, ensure_ascii=False), seq=seq, background=bg_log)
    log_dt_command({**payload, "mqtt_topic": topic}, "QUEUED", background=bg_log)

    # Supersede older already-sent direct commands so their retry cannot arrive late.
    for old in await supersede_pending_target(target, command_id):
        await broadcast_command_status(old, "SUPERSEDED", f"replaced by newer {target} command_id={command_id}")

    debounce_s, min_interval_s = controller_config(target)
    elapsed_s = time.monotonic() - TARGET_LAST_SENT_AT.get(target, 0.0)
    can_fast_publish = is_direct_target(target) and elapsed_s >= min_interval_s

    async with COMMAND_LOCK:
        old = LATEST_DESIRED_BY_TARGET.get(target)
        if can_fast_publish:
            LATEST_DESIRED_BY_TARGET.pop(target, None)
            TARGET_LAST_SENT_AT[target] = time.monotonic()
            PENDING_COMMANDS[command_id] = cmd
        else:
            LATEST_DESIRED_BY_TARGET[target] = cmd
            task = TARGET_TASKS.get(target)
            if task is None or task.done():
                TARGET_TASKS[target] = asyncio.create_task(delayed_send_for_target(target))

    if old is not None:
        await broadcast_command_status(old, "SUPERSEDED", f"replaced by newer {target} command")

    if can_fast_publish:
        await broadcast_command_status(cmd, "QUEUED", "fast-path direct command queued")
        await publish_pending_command(cmd, first_send=True)
        status_msg = cmd.status
        message = "Fast path: published MQTT QoS1 immediately, waiting ESP32 ACK/state."
    else:
        await broadcast_command_status(cmd, "QUEUED", "queued by command controller")
        status_msg = "QUEUED"
        message = "Queued. Backend will debounce/rate-limit, publish MQTT QoS1, then wait ESP32 ACK/state."

    return {
        "ok": True,
        "api_version": API_VERSION,
        "command_id": command_id,
        "target": target,
        "seq": seq,
        "status": status_msg,
        "mqtt_topic": topic,
        "retain": retain,
        "payload": payload,
        "message": message,
    }


async def submit_immediate_command(target: str, payload: dict[str, Any], source: str, reason: str) -> dict[str, Any]:
    topic, retain = topic_for_target(target)
    seq = next_seq_for_target(target)
    command_id = payload.get("command_id") or make_command_id(source, target)
    payload = {
        **payload,
        "id": command_id,
        "command_id": command_id,
        "seq": seq,
        "target": target,
        "source": source,
        "reason": reason,
        "sent_at": utc_now_iso(),
    }
    if is_direct_target(target):
        payload["created_at_ms"] = int(time.time() * 1000)
    cmd = PendingCommand(
        command_id=command_id,
        target=target,
        desired_state=safe_upper(payload.get("state")),
        seq=seq,
        payload=payload,
        topic=topic,
        retain=retain,
        source=source,
        reason=reason,
    )
    async with COMMAND_LOCK:
        PENDING_COMMANDS[command_id] = cmd
    await broadcast_command_status(cmd, "QUEUED", "immediate command queued")
    await publish_pending_command(cmd, first_send=True)
    return {
        "ok": True,
        "api_version": API_VERSION,
        "command_id": command_id,
        "target": target,
        "seq": seq,
        "status": cmd.status,
        "mqtt_topic": topic,
        "retain": retain,
        "payload": payload,
    }


async def retry_worker() -> None:
    while True:
        await asyncio.sleep(COMMAND_RETRY_SCAN_S)
        now = time.monotonic()
        todo_retry: list[PendingCommand] = []
        todo_timeout: list[PendingCommand] = []
        async with COMMAND_LOCK:
            for cmd in list(PENDING_COMMANDS.values()):
                if cmd.status not in ("SENT", "RETRY"):
                    continue
                if now - cmd.created_monotonic > COMMAND_TTL_S:
                    todo_timeout.append(cmd)
                    continue
                if not command_is_latest_for_target(cmd):
                    todo_timeout.append(cmd)
                    continue
                if cmd.next_deadline and now >= cmd.next_deadline:
                    if cmd.retries < COMMAND_MAX_RETRIES:
                        cmd.retries += 1
                        todo_retry.append(cmd)
                    else:
                        todo_timeout.append(cmd)
        for cmd in todo_retry:
            await publish_pending_command(cmd, first_send=False)
        for cmd in todo_timeout:
            await mark_command_done(cmd, "TIMEOUT", f"no ACK after {cmd.retries} retries")


async def ack_direct_command_from_actuator(state: dict[str, Any]) -> None:
    # Nếu firmware đã include command_id trong actuator_state thì ACK chính xác.
    command_ids = [str(state.get("command_id") or "").strip()]
    command_ids.append(str(state.get("last_pump_command_id") or "").strip())
    command_ids.append(str(state.get("last_light_command_id") or "").strip())
    for command_id in [x for x in command_ids if x]:
        async with COMMAND_LOCK:
            cmd = PENDING_COMMANDS.get(command_id)
        if cmd and cmd.target in ("pump", "light"):
            await mark_command_done(cmd, "DONE", "ESP32 actuator_state ACK with command_id")
            return

    # Backward compatible: firmware v2.9.3 chưa có command_id cho pump/light state,
    # nên ACK theo state thật sau khi ESP32 publish actuator_state.
    for target in ("pump", "light"):
        actual = safe_upper(state.get(f"{target}_state"))
        if actual not in ("ON", "OFF"):
            continue
        async with COMMAND_LOCK:
            candidates = [c for c in PENDING_COMMANDS.values() if c.target == target and c.status in ("SENT", "RETRY")]
        if not candidates:
            continue
        # Chỉ ACK command mới nhất của target, tránh command cũ đè command mới.
        cmd = sorted(candidates, key=lambda c: c.seq)[-1]
        if cmd.desired_state == actual:
            await mark_command_done(cmd, "DONE", f"ESP32 actuator_state matches desired {target}={actual}")


async def ack_planting_start_from_status(raw: dict[str, Any]) -> None:
    command_id = str(raw.get("command_id") or raw.get("id") or "").strip()
    if not command_id:
        return
    target = str(raw.get("target") or "").strip()
    event = str(raw.get("event") or "").strip()
    is_planting = target == "planting_start" or event.startswith("planting_start_")
    if not is_planting:
        return
    status = safe_upper(raw.get("status"), "DONE")
    final_status = "DONE" if status in ("DONE", "OK") else ("DUPLICATE" if status == "DUPLICATE" else "ERROR")
    async with COMMAND_LOCK:
        cmd = PENDING_COMMANDS.get(command_id)
    if cmd:
        await mark_command_done(cmd, final_status, json.dumps(raw, ensure_ascii=False))
    else:
        # ACK tới trễ sau timeout hoặc command do Gateway gửi. Vẫn broadcast để Web/Unity biết.
        event_out = {
            "type": "command_event",
            "source": "backend",
            "timestamp": utc_now_iso(),
            "command_id": command_id,
            "target": "planting_start",
            "status": final_status,
            "message": "late/external planting_start ACK",
            "data": raw,
        }
        LATEST["command_event"] = {
            "_time": event_out["timestamp"],
            "command_id": command_id,
            "target": "planting_start",
            "status": final_status,
            "message": json.dumps(raw, ensure_ascii=False),
        }
        LATEST["updated_at"] = event_out["timestamp"]
        await broadcast_event(event_out)


def apply_realtime_event_sync(event: dict[str, Any]) -> dict[str, Any]:
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
        if etype == "planting_start_ack" or str(status.get("event") or "").startswith("planting_start_"):
            LATEST["command_event"] = {
                "_time": received_at,
                "command_id": event.get("command_id") or status.get("command_id"),
                "target": "planting_start",
                "status": event.get("status") or status.get("status"),
                "message": json.dumps(status, ensure_ascii=False),
            }
            latest_patch["command_event"] = LATEST["command_event"]
    elif etype in ("command_event", "command_sent"):
        LATEST["command_event"] = {
            "_time": received_at,
            "command_id": event.get("command_id") or data.get("command_id"),
            "target": event.get("target") or data.get("target"),
            "status": event.get("status") or data.get("status"),
            "message": event.get("message") or json.dumps(data, ensure_ascii=False),
        }
        LATEST["dt_command"] = data
        latest_patch["command_event"] = LATEST["command_event"]

    LATEST["updated_at"] = received_at
    EVENT_BUFFER.append(event)
    return latest_patch

# =============================================================================
# STARTUP/SHUTDOWN
# =============================================================================

@app.on_event("startup")
def on_startup() -> None:
    global RETRY_TASK
    init_mqtt()
    loop = asyncio.get_event_loop()
    RETRY_TASK = loop.create_task(retry_worker())
    print(f"[STARTUP] Plant backend {API_VERSION} started. MQTT={MQTT_BROKER}:{MQTT_PORT} QoS={MQTT_QOS}")


@app.on_event("shutdown")
def on_shutdown() -> None:
    global mqtt_client, RETRY_TASK
    if RETRY_TASK:
        RETRY_TASK.cancel()
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
    influx_ok = False
    influx_message = "disabled/missing token"
    if INFLUX_TOKEN:
        try:
            with get_influx_client() as client:
                influx_ok = bool(client.ping())
                influx_message = "OK" if influx_ok else "ping false"
        except Exception as exc:
            influx_message = str(exc)
    return {
        "status": "OK",
        "api_version": API_VERSION,
        "node_id": NODE_ID,
        "node_topic_id": NODE_TOPIC_ID,
        "plant": PLANT_NAME,
        "influx": {"url": INFLUX_URL, "org": INFLUX_ORG, "bucket": INFLUX_BUCKET, "ok": influx_ok, "message": influx_message},
        "mqtt": {"broker": MQTT_BROKER, "port": MQTT_PORT, "qos": MQTT_QOS, "connected": mqtt_connected},
        "topics": TOPICS,
        "command_controller": {
            "ack_timeout_s": COMMAND_ACK_TIMEOUT_S,
            "max_retries": COMMAND_MAX_RETRIES,
            "pump_debounce_ms": PUMP_DEBOUNCE_MS,
            "light_debounce_ms": LIGHT_DEBOUNCE_MS,
            "pump_min_interval_s": PUMP_MIN_INTERVAL_S,
            "light_min_interval_s": LIGHT_MIN_INTERVAL_S,
            "direct_command_ttl_s": DIRECT_COMMAND_TTL_S,
        },
        "timestamp": utc_now_iso(),
    }


@app.get("/api/realtime/latest")
async def realtime_latest() -> dict[str, Any]:
    async with COMMAND_LOCK:
        pending = [
            {
                "command_id": c.command_id,
                "target": c.target,
                "state": c.desired_state,
                "seq": c.seq,
                "status": c.status,
                "retry_count": c.retries,
            }
            for c in PENDING_COMMANDS.values()
        ]
        queued = [
            {"command_id": c.command_id, "target": c.target, "state": c.desired_state, "seq": c.seq, "status": c.status}
            for c in LATEST_DESIRED_BY_TARGET.values()
        ]
    return {
        "api_version": API_VERSION,
        "latest": LATEST,
        "pending_commands": pending,
        "queued_latest_commands": queued,
        "buffer_count": len(EVENT_BUFFER),
        "websocket_clients": await manager.count(),
        "timestamp": utc_now_iso(),
    }


@app.post("/api/realtime/ingest")
async def realtime_ingest(event: RealtimeIngestEvent, x_realtime_token: str | None = Header(default=None)) -> dict[str, Any]:
    if REALTIME_INGEST_TOKEN and x_realtime_token != REALTIME_INGEST_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid realtime ingest token")
    event_dict = model_to_dict(event)
    latest_patch = apply_realtime_event_sync(event_dict)

    # ACK handling phải chạy sau apply cache.
    if event.type == "actuator_state":
        await ack_direct_command_from_actuator(flatten_actuator_payload(event.data))
    if event.type in ("esp32_status", "planting_start_ack"):
        await ack_planting_start_from_status(event.data)

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


@app.get("/api/dashboard/latest")
def dashboard_latest(
    minutes: int = Query(default=30, ge=1, le=60 * 24 * 31),
    date: str | None = Query(default=None),
    prefer_realtime: bool = Query(default=True),
) -> dict[str, Any]:
    if prefer_realtime and date is None and any(LATEST.get(k) for k in ("sensors", "actuator", "status", "command_event", "dt_command")):
        return {
            "node_id": NODE_ID,
            "plant": PLANT_NAME,
            "api_version": API_VERSION,
            "realtime_only": True,
            "realtime_updated_at": LATEST.get("updated_at"),
            "sensors": LATEST.get("sensors"),
            "actuator": LATEST.get("actuator"),
            "status": LATEST.get("status"),
            "command_event": LATEST.get("command_event"),
            "dt_command": LATEST.get("dt_command"),
            "counts": {"sensors": 0, "actuator": 0, "status": 0, "cmd": 0, "dt": 0},
            "timestamp": utc_now_iso(),
        }

    sensors_rows = query_measurement(MEAS_SENSORS, minutes=minutes, date=date, limit=120)
    actuator_rows = query_measurement(MEAS_ACTUATOR, minutes=minutes, date=date, limit=50)
    status_rows = query_measurement(MEAS_STATUS, minutes=minutes, date=date, limit=50)
    cmd_rows = query_measurement(MEAS_CMD, minutes=minutes, date=date, limit=50)
    dt_rows = query_measurement(MEAS_DT, minutes=minutes, date=date, limit=50)
    return {
        "node_id": NODE_ID,
        "plant": PLANT_NAME,
        "api_version": API_VERSION,
        "realtime_only": False,
        "realtime_updated_at": LATEST.get("updated_at"),
        "sensors": LATEST.get("sensors") or (sensors_rows[-1] if sensors_rows else None),
        "actuator": LATEST.get("actuator") or (actuator_rows[-1] if actuator_rows else None),
        "status": LATEST.get("status") or (parse_status_record(status_rows[-1]) if status_rows else None),
        "command_event": LATEST.get("command_event") or (cmd_rows[-1] if cmd_rows else None),
        "dt_command": LATEST.get("dt_command") or (dt_rows[-1] if dt_rows else None),
        "counts": {"sensors": len(sensors_rows), "actuator": len(actuator_rows), "status": len(status_rows), "cmd": len(cmd_rows), "dt": len(dt_rows)},
        "timestamp": utc_now_iso(),
    }


@app.get("/api/history/{measurement}")
def history(
    measurement: Literal["sensors", "actuator", "status", "cmd", "dt"],
    minutes: int = Query(default=30, ge=1, le=60 * 24 * 31),
    date: str | None = Query(default=None),
    limit: int = Query(default=120, ge=1, le=5000),
) -> dict[str, Any]:
    rows = query_measurement(measurement, minutes=minutes, date=date, limit=limit)
    if measurement == MEAS_STATUS:
        rows = [parse_status_record(row) or row for row in rows]
    return {"measurement": measurement, "count": len(rows), "data": rows, "api_version": API_VERSION}


@app.get("/api/history/sensors")
def history_sensors(
    minutes: int = Query(default=30, ge=1, le=60 * 24 * 31),
    date: str | None = Query(default=None),
    limit: int = Query(default=120, ge=1, le=5000),
) -> dict[str, Any]:
    rows = query_measurement(MEAS_SENSORS, minutes=minutes, date=date, limit=limit)
    return {"measurement": MEAS_SENSORS, "count": len(rows), "data": rows, "api_version": API_VERSION}


@app.post("/api/command/pump")
async def command_pump(cmd: PumpCommand) -> dict[str, Any]:
    duration = 0 if cmd.state == "OFF" else min(cmd.duration_s, PUMP_MAX_DURATION_S)
    payload = {
        "mode": "DIRECT",
        "target": "pump",
        "state": cmd.state,
        "duration_s": duration,
    }
    return await submit_coalesced_command("pump", payload, cmd.source, cmd.reason)


@app.post("/api/command/light")
async def command_light(cmd: LightCommand) -> dict[str, Any]:
    duration = 0 if cmd.state == "OFF" else min(cmd.duration_s, LIGHT_MAX_DURATION_S)
    payload = {
        "mode": "DIRECT",
        "target": "light",
        "state": cmd.state,
        "duration_s": duration,
    }
    return await submit_coalesced_command("light", payload, cmd.source, cmd.reason)


@app.post("/api/command/planting-start")
async def command_planting_start(cmd: PlantingStartCommand) -> dict[str, Any]:
    action = cmd.action
    epoch = cmd.planting_start_epoch
    # Không retain SET_NOW vì replay sau này sẽ thành "now" sai thời điểm.
    # Backend đổi SET_NOW -> SET_EPOCH cố định tại thời điểm bấm.
    if action == "SET_NOW":
        action = "SET_EPOCH"
        epoch = unix_now_s()
    if action == "SET_EPOCH" and not epoch:
        raise HTTPException(status_code=400, detail="SET_EPOCH requires planting_start_epoch")
    payload: dict[str, Any] = {
        "target": "planting_start",
        "action": action,
    }
    if action == "SET_EPOCH":
        payload["planting_start_epoch"] = int(epoch or 0)
    return await submit_immediate_command("planting_start", payload, cmd.source, cmd.reason)


@app.get("/api/export/sensors.csv")
def export_sensors_csv(
    minutes: int = Query(default=720, ge=1, le=60 * 24 * 31),
    date: str | None = Query(default=None),
) -> StreamingResponse:
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
