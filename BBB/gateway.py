#!/usr/bin/env python3
"""
gateway.py — Raspberry Pi Edge AI Gateway + InfluxDB Digital Twin Bridge
========================================================================

Flow topic v2, không dùng SQL local:

TELEMETRY:
    ESP32 -> MQTT -> Raspberry Pi Gateway -> InfluxDB sensors/status

AUTO CONTROL:
    ESP32 sensor -> Raspberry Pi Gateway Random Forest -> cps/greenhouse/brassica_01/cmd/auto/pump -> ESP32

DIGITAL TWIN CONTROL:
    Digital Twin/Web/Unity -> InfluxDB measurement dt
    Raspberry Pi Gateway poll dt PENDING -> MQTT cmd/direct/pump|light hoặc cmd/config/planting_start -> ESP32
    ESP32 -> cps/greenhouse/brassica_01/state/actuator -> Gateway
    Gateway -> InfluxDB actuator + cmd/status

Cài đặt:
    pip install paho-mqtt influxdb-client joblib pandas scikit-learn

Chạy:
    python gateway.py --debug

Lưu ý:
    File này dùng biến môi trường InfluxDB để tránh hard-code token trong source code.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import signal
import sys
import threading
import time
import uuid
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import paho.mqtt.client as mqtt

try:
    import joblib
    import pandas as pd
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS
except ImportError as exc:
    print(f"[LỖI] Thiếu thư viện: {exc}")
    print("Chạy: pip install paho-mqtt influxdb-client joblib pandas scikit-learn")
    sys.exit(1)


# =============================================================================
# CONSTANTS
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent

NODE_ID = "BRASSICA_JUNCEA_01"
PLANT_NAME = "Rau Cải Mầm (Brassica juncea)"
GW_VERSION = "3.8.2-direct-guard-stable-command"

MODEL_PATH = BASE_DIR / "watering_random_forest_model.pkl"
FEATURES_PATH = BASE_DIR / "model_features.json"
CONFIG_PATH = BASE_DIR / "controller_config.json"

MQTT_BROKER = os.getenv("MQTT_BROKER", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_KEEPALIVE = 60
MQTT_QOS = 1
MQTT_CLIENT_ID = "rpi_gateway_brassica_topic_v2"

# MQTT topic tree v2: cps/greenhouse/<node>/...
TOPIC_ROOT = "cps/greenhouse"
NODE_TOPIC_ID = "brassica_01"

# ESP32 -> Gateway / Web / Unity
TOPIC_SENSOR = f"{TOPIC_ROOT}/{NODE_TOPIC_ID}/telemetry/sensors"
TOPIC_STATUS_ESP32 = f"{TOPIC_ROOT}/{NODE_TOPIC_ID}/status/esp32"
TOPIC_ACTUATOR_STATE = f"{TOPIC_ROOT}/{NODE_TOPIC_ID}/state/actuator"

# Gateway status
TOPIC_STATUS_GATEWAY = f"{TOPIC_ROOT}/gateway/status"

# Raspberry Pi Gateway AI AUTO -> ESP32
TOPIC_CMD_PUMP_AUTO = f"{TOPIC_ROOT}/{NODE_TOPIC_ID}/cmd/auto/pump"

# Unity/Web/Digital Twin DIRECT -> ESP32
TOPIC_CMD_PUMP_DIRECT = f"{TOPIC_ROOT}/{NODE_TOPIC_ID}/cmd/direct/pump"
TOPIC_CMD_LIGHT_DIRECT = f"{TOPIC_ROOT}/{NODE_TOPIC_ID}/cmd/direct/light"

# Config command
TOPIC_CMD_PLANTING_START = f"{TOPIC_ROOT}/{NODE_TOPIC_ID}/cmd/config/planting_start"

# ── InfluxDB Cloud ───────────────────────────────────────────────────────────
## Không hard-code token trong source code. Export biến môi trường trước khi chạy.
INFLUX_URL_DEFAULT = "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUX_TOKEN_DEFAULT = "6pSuWQaFLlWq6iRVfaRYEMwIO1DDEChBsG42HdDx5En6fuqpUx95j3xswbVNrcWxRrs_sizN6XXESjzNqcHzJA=="
INFLUX_ORG_DEFAULT = "DEV_TEAM"
INFLUX_BUCKET_DEFAULT = "digital_twin_data"

# InfluxDB measurements đúng theo thiết kế muốn thấy trong Data Explorer.
# MQTT topic KHÔNG tự tạo bảng InfluxDB; chỉ Point("<measurement>") mới tạo bảng.
MEAS_SENSORS = "sensors"      # telemetry/sensors + AI result
MEAS_STATUS = "status"        # latest JSON + esp32/gateway status + planting_start ACK
MEAS_ACTUATOR = "actuator"    # state/actuator
MEAS_CMD = "cmd"              # lệnh Gateway gửi xuống ESP32 + SENT/DONE/ERROR
MEAS_DT = "dt"                # queue/log lệnh Digital Twin/Web/Unity ghi vào InfluxDB

# Realtime bridge: Gateway nhận MQTT từ ESP32 thì vừa ghi InfluxDB, vừa đẩy ngay cho Backend.
REALTIME_ENABLED = os.getenv("REALTIME_ENABLED", "1").strip().lower() not in ("0", "false", "no", "off")
REALTIME_BACKEND_URL = os.getenv("REALTIME_BACKEND_URL", "http://127.0.0.1:8000").rstrip("/")
REALTIME_INGEST_PATH = os.getenv("REALTIME_INGEST_PATH", "/api/realtime/ingest")
REALTIME_INGEST_TOKEN = os.getenv("REALTIME_INGEST_TOKEN", "")
REALTIME_TIMEOUT_S = float(os.getenv("REALTIME_TIMEOUT_S", "0.7"))
REALTIME_QUEUE_MAX = int(os.getenv("REALTIME_QUEUE_MAX", "300"))


WRITE_PRECISION_SECONDS = getattr(WritePrecision, "S", None) or getattr(WritePrecision, "SECONDS")


# =============================================================================
# UTILS
# =============================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def setup_logging(debug: bool = False) -> logging.Logger:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger("GATEWAY")


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def normalize_state(value: Any) -> str:
    state = str(value or "").strip().upper()
    if state in ("1", "TRUE", "ON", "PUMP_ON", "LIGHT_ON"):
        return "ON"
    if state in ("0", "FALSE", "OFF", "PUMP_OFF", "LIGHT_OFF"):
        return "OFF"
    return state or "UNKNOWN"


def safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in ("1", "true", "yes", "y", "on"):
        return True
    if text in ("0", "false", "no", "n", "off"):
        return False
    return default


def clamp_duration(value: Any, default_s: int, max_s: int) -> int:
    duration = safe_int(value, default_s)
    if duration < 0:
        duration = 0
    return min(duration, max_s)


# =============================================================================
# CONFIG
# =============================================================================

@dataclass
class GatewayConfig:
    phase: int = 0
    soil_force_on: float = 25.0
    soil_force_off: float = 55.0
    dry_soil_threshold_phase1: float = 25.0
    dry_soil_threshold_phase2: float = 30.0
    min_on_steps: int = 1
    max_on_steps_normal: int = 3
    max_on_steps_dry_phase1: int = 6
    max_on_steps_dry_phase2: int = 5
    cooldown_steps: int = 2
    lux_phase2_threshold: float = 50.0

    # Alert thresholds for box/soil daylight-simulation model.
    temp_max: float = 32.0
    hum_p1_min: float = 65.0
    hum_p1_max: float = 88.0
    hum_p2_min: float = 50.0
    hum_p2_max: float = 80.0
    soil_min_alert: float = 35.0
    soil_max_alert: float = 80.0
    lux_leak_warning: float = 150.0
    lux_p2_min: float = 50.0

    @staticmethod
    def from_json(path: Path) -> "GatewayConfig":
        cfg = GatewayConfig()
        if not path.exists():
            return cfg

        raw = json.loads(path.read_text(encoding="utf-8"))

        for key in (
            "phase", "soil_force_on", "soil_force_off",
            "dry_soil_threshold_phase1", "dry_soil_threshold_phase2",
            "min_on_steps", "max_on_steps_normal",
            "max_on_steps_dry_phase1", "max_on_steps_dry_phase2",
            "cooldown_steps", "lux_phase2_threshold",
        ):
            if key in raw and hasattr(cfg, key):
                setattr(cfg, key, raw[key])

        # Optional future profile support.
        profiles = raw.get("phase_profiles", {})
        if isinstance(profiles, dict):
            p1 = profiles.get("phase1_low_light_germination", {})
            p2 = profiles.get("phase2_daylight_simulation", {})
            if isinstance(p1, dict):
                cfg.hum_p1_min = safe_float(p1.get("air_humidity_min", cfg.hum_p1_min), cfg.hum_p1_min)
                cfg.hum_p1_max = safe_float(p1.get("air_humidity_warning_high", cfg.hum_p1_max), cfg.hum_p1_max)
                cfg.lux_leak_warning = safe_float(p1.get("lux_warning_high", cfg.lux_leak_warning), cfg.lux_leak_warning)
            if isinstance(p2, dict):
                cfg.hum_p2_min = safe_float(p2.get("air_humidity_min", cfg.hum_p2_min), cfg.hum_p2_min)
                cfg.hum_p2_max = safe_float(p2.get("air_humidity_warning_high", cfg.hum_p2_max), cfg.hum_p2_max)
                cfg.lux_p2_min = safe_float(p2.get("lux_light_on_warning_low", cfg.lux_p2_min), cfg.lux_p2_min)

        return cfg


# =============================================================================
# INFLUXDB MANAGER
# =============================================================================

class InfluxManager:
    """
    InfluxDB usage:
      sensors            : telemetry time-series
      actuator           : physical relay feedback from ESP32
      status             : status/latest JSON snapshot for Digital Twin
      dt                 : command queue written by Digital Twin/Web, gồm pump/light/planting_start
      cmd                : SENT / DONE / ERROR event log written by Gateway

    Important:
      InfluxDB is append-only time-series. Gateway does not update old command rows.
      It records command progress by writing event points into dt_command_events.
    """

    def __init__(self, log: logging.Logger):
        self.log = log
        self.url = os.getenv("INFLUX_URL", INFLUX_URL_DEFAULT)
        self.token = os.getenv("INFLUX_TOKEN", INFLUX_TOKEN_DEFAULT)
        self.org = os.getenv("INFLUX_ORG", INFLUX_ORG_DEFAULT)
        self.bucket = os.getenv("INFLUX_BUCKET", INFLUX_BUCKET_DEFAULT)

        if not self.token:
            raise RuntimeError("Thiếu INFLUX_TOKEN: kiểm tra INFLUX_TOKEN_DEFAULT hoặc biến môi trường INFLUX_TOKEN.")

        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org, timeout=30000)
        self.client.ping()
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()

        self.telemetry_queue: queue.Queue[dict] = queue.Queue(maxsize=500)
        self.stop_event = threading.Event()

        self.log.info(f"✅ InfluxDB connected: {self.url} org={self.org} bucket={self.bucket}")

    def start(self) -> None:
        threading.Thread(target=self._telemetry_worker, daemon=True).start()

    def stop(self) -> None:
        self.stop_event.set()

    # -------------------------------------------------------------------------
    # Writes
    # -------------------------------------------------------------------------

    def enqueue_telemetry(self, payload: dict) -> None:
        try:
            self.telemetry_queue.put_nowait(payload)
        except queue.Full:
            self.log.warning("Influx telemetry queue full; drop point.")

    def _telemetry_worker(self) -> None:
        while not self.stop_event.is_set():
            try:
                payload = self.telemetry_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                self.write_telemetry(payload)
            except Exception as exc:
                self.log.error(f"[InfluxDB] telemetry write error: {exc}")
            finally:
                self.telemetry_queue.task_done()

    def write_telemetry(self, payload: dict) -> None:
        s = payload["sensor"]
        ai = payload["ai"]
        ctrl = payload["control"]

        point = (
            Point(MEAS_SENSORS)
            .tag("node_id", payload.get("node_id", NODE_ID))
            .tag("plant", PLANT_NAME)
            .tag("phase", str(payload.get("phase", 1)))
            .tag("phase_source", str(payload.get("phase_source", "unknown")))
            .tag("ai_source", ai.get("source", "unknown"))
            .tag("pump_state", ctrl["pump"]["state"])
            .field("temperature", float(s.get("temperature", 0)))
            .field("air_humidity", float(s.get("air_humidity", 0)))
            .field("lux", float(s.get("lux", 0)))
            .field("soil_moisture", float(s.get("soil_moisture", 0)))
            .field("soil_s1", float(s.get("soil_s1", 0)))
            .field("soil_s2", float(s.get("soil_s2", 0)))
            .field("soil_s3", float(s.get("soil_s3", 0)))
            .field("soil_s4", float(s.get("soil_s4", 0)))
            .field("soil_moisture_fused", float(s.get("soil_moisture_fused", s.get("soil_moisture", 0))))
            .field("soil_moisture_mean", float(s.get("soil_moisture_mean", s.get("soil_moisture", 0))))
            .field("soil_moisture_min", float(s.get("soil_moisture_min", s.get("soil_moisture", 0))))
            .field("soil_moisture_max", float(s.get("soil_moisture_max", s.get("soil_moisture", 0))))
            .field("soil_voltage_v1", float(s.get("soil_voltage_v1", 0)))
            .field("soil_voltage_v2", float(s.get("soil_voltage_v2", 0)))
            .field("soil_voltage_v3", float(s.get("soil_voltage_v3", 0)))
            .field("soil_voltage_v4", float(s.get("soil_voltage_v4", 0)))
            .field("soil_fusion_lo", float(s.get("soil_fusion_lo", 0)))
            .field("soil_fusion_hi", float(s.get("soil_fusion_hi", 0)))
            .field("soil_fusion_confidence", float(s.get("soil_fusion_confidence", 0)))
            .field("soil_fusion_valid_count", int(s.get("soil_fusion_valid_count", 0)))
            .field("soil_presence_state", str(s.get("soil_presence_state", "UNKNOWN")))
            .field("soil_fusion_method", str(s.get("soil_fusion_method", "")))
            .field("soil_control_reliable", 1 if s.get("soil_control_reliable") else 0)
            .field("soil_reliable", 1 if s.get("soil_reliable") else 0)
            .field("soil_sensor_fault", 1 if s.get("soil_sensor_fault") else 0)
            .field("soil_saturated_dry", 1 if s.get("soil_saturated_dry") else 0)
            .field("soil_stuck_zero", 1 if s.get("soil_stuck_zero") else 0)
            .field("soil_zero_streak", int(s.get("soil_zero_streak", 0)))
            .field("need_watering", int(ai.get("need_watering") or 0))
            .field("ai_confidence", float(ai.get("confidence", 0)))
            .field("prob_need_watering", float(ai.get("prob_need_watering", 0)))
            .field("pump", 1 if ctrl["pump"]["state"] == "ON" else 0)
            .field("light", 1 if ctrl["light"]["state"] == "ON" else 0)
            .field("step", int(payload.get("step") or 0))
            .field("gw_step", int(payload.get("gw_step") or 0))
            .field("uptime_s", int(payload.get("uptime_s") or 0))
            .field("wifi_rssi", int(payload.get("wifi_rssi") or 0))
            .field("days_after_planting", float(payload.get("days_after_planting", -1.0)))
            .field("planting_start_epoch", int(payload.get("planting_start_epoch") or 0))
            .field("planting_start_valid", 1 if payload.get("planting_start_valid") else 0)
            .field("alert", str(payload.get("alert") or ""))
            .time(datetime.now(timezone.utc), WRITE_PRECISION_SECONDS)
        )
        self.write_api.write(bucket=self.bucket, record=point)

    def write_actuator_state(self, state: dict) -> None:
        pump = state["pump"]
        light = state["light"]

        point = (
            Point(MEAS_ACTUATOR)
            .tag("node_id", state.get("node_id", NODE_ID))
            .tag("pump_state", pump.get("state", "UNKNOWN"))
            .tag("light_state", light.get("state", "UNKNOWN"))
            .field("pump", 1 if pump.get("state") == "ON" else 0)
            .field("light", 1 if light.get("state") == "ON" else 0)
            .field("pump_mode", str(pump.get("mode", "UNKNOWN")))
            .field("light_mode", str(light.get("mode", "UNKNOWN")))
            .field("pump_reason", str(pump.get("reason") or ""))
            .field("light_reason", str(light.get("reason") or ""))
            .time(datetime.now(timezone.utc), WRITE_PRECISION_SECONDS)
        )
        self.write_api.write(bucket=self.bucket, record=point)

    def write_latest_state(self, key: str, value: dict) -> None:
        point = (
            Point(MEAS_STATUS)
            .tag("node_id", NODE_ID)
            .tag("key", key)
            .field("value_json", json.dumps(value, ensure_ascii=False))
            .time(datetime.now(timezone.utc), WRITE_PRECISION_SECONDS)
        )
        self.write_api.write(bucket=self.bucket, record=point)

    def write_planting_start_state(self, raw: dict) -> None:
        """Persist the latest planting_start ACK/config returned by ESP32."""
        command_id = str(raw.get("command_id") or raw.get("id") or "")
        event = str(raw.get("event") or "")
        status = str(raw.get("status") or "")
        source = str(raw.get("source") or "ESP32")
        epoch = safe_int(raw.get("planting_start_epoch"), 0)
        start_time = str(raw.get("planting_start_time") or "")
        error = str(raw.get("error") or "")

        point = (
            Point(MEAS_STATUS)
            .tag("node_id", raw.get("node_id", NODE_ID))
            .tag("target", "planting_start")
            .tag("status", status or "UNKNOWN")
            .tag("event", event or "unknown")
            .field("command_id", command_id)
            .field("source", source)
            .field("planting_start_epoch", int(epoch))
            .field("planting_start_time", start_time)
            .field("error", error)
            .field("raw_json", json.dumps(raw, ensure_ascii=False))
            .time(datetime.now(timezone.utc), WRITE_PRECISION_SECONDS)
        )
        self.write_api.write(bucket=self.bucket, record=point)

    def write_command_event(self, command_id: str, target: str, status: str, message: str = "") -> None:
        point = (
            Point(MEAS_CMD)
            .tag("node_id", NODE_ID)
            .tag("command_id", command_id)
            .tag("target", target)
            .tag("status", status)
            .field("message", message)
            .time(datetime.now(timezone.utc), WRITE_PRECISION_SECONDS)
        )
        self.write_api.write(bucket=self.bucket, record=point)

    def write_command(
        self,
        command_id: str,
        target: str,
        state: str = "",
        duration_s: int = 0,
        reason: str = "",
        source: str = "digital_twin",
        action: str = "",
        planting_start_epoch: int = 0,
    ) -> None:
        """
        Digital Twin/Web/Unity writes this same shape to InfluxDB.

        Measurement: dt

        Common tags:
          node_id, command_id, target, status=PENDING

        Pump/light fields:
          state, duration_s, reason, source

        Planting-start fields:
          action = SET_NOW | SET_EPOCH | CLEAR | GET
          planting_start_epoch = unix epoch seconds, only needed for SET_EPOCH
          reason, source
        """
        target = str(target).strip().lower()

        point = (
            Point(MEAS_DT)
            .tag("node_id", NODE_ID)
            .tag("command_id", command_id)
            .tag("target", target)
            .tag("status", "PENDING")
            .field("reason", reason or "dt_command")
            .field("source", source or "digital_twin")
            .time(datetime.now(timezone.utc), WRITE_PRECISION_SECONDS)
        )

        if target == "planting_start":
            action = (action or "SET_NOW").strip().upper()
            point = point.field("action", action)
            if planting_start_epoch:
                point = point.field("planting_start_epoch", int(planting_start_epoch))
        else:
            point = (
                point
                .field("state", state.upper())
                .field("duration_s", int(duration_s))
            )

        self.write_api.write(bucket=self.bucket, record=point)

    def insert_test_command(
        self,
        target: str,
        state: str = "",
        duration_s: int = 0,
        reason: str = "cli_test",
        action: str = "",
        planting_start_epoch: int = 0,
    ) -> str:
        command_id = f"cmd-{uuid.uuid4().hex[:12]}"
        self.write_command(
            command_id=command_id,
            target=target,
            state=state,
            duration_s=duration_s,
            reason=reason,
            source="cli_test",
            action=action,
            planting_start_epoch=planting_start_epoch,
        )
        return command_id

    # -------------------------------------------------------------------------
    # Queries
    # -------------------------------------------------------------------------

    def query_processed_command_ids(self, lookback: str = "24h") -> set[str]:
        flux = f"""
from(bucket: "{self.bucket}")
  |> range(start: -{lookback})
  |> filter(fn: (r) => r._measurement == "{MEAS_CMD}")
  |> filter(fn: (r) => r.status == "DONE" or r.status == "ERROR")
  |> keep(columns: ["command_id"])
  |> group()
  |> distinct(column: "command_id")
"""
        ids: set[str] = set()
        tables = self.query_api.query(flux, org=self.org)
        for table in tables:
            for rec in table.records:
                cid = rec.values.get("command_id") or rec.get_value()
                if cid:
                    ids.add(str(cid))
        return ids

    def query_pending_commands(self, lookback: str = "24h", limit: int = 20) -> list[dict]:
        flux = f"""
from(bucket: "{self.bucket}")
  |> range(start: -{lookback})
  |> filter(fn: (r) => r._measurement == "{MEAS_DT}")
  |> filter(fn: (r) => r.status == "PENDING")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group()
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: {int(limit)})
"""
        tables = self.query_api.query(flux, org=self.org)
        commands: list[dict] = []

        for table in tables:
            for rec in table.records:
                v = rec.values
                command_id = v.get("command_id")
                target = v.get("target")
                if not command_id or not target:
                    continue

                commands.append({
                    "time": v.get("_time"),
                    "command_id": str(command_id),
                    "target": str(target).lower(),
                    "state": normalize_state(v.get("state")),
                    "duration_s": safe_int(v.get("duration_s"), 0),
                    "reason": str(v.get("reason") or "influx_command"),
                    "source": str(v.get("source") or "digital_twin"),
                    "action": str(v.get("action") or ""),
                    "planting_start_epoch": safe_int(v.get("planting_start_epoch"), 0),
                })

        return commands


    def query_latest_desired_planting_start(self, lookback: str = "30d") -> Optional[dict]:
        """
        Return newest DB/Unity desired planting_start epoch from measurement dt.
        DB/Unity/Web is the source of truth; ESP32 NVS is only a local cache.
        """
        flux = f"""
from(bucket: "{self.bucket}")
  |> range(start: -{lookback})
  |> filter(fn: (r) => r._measurement == "{MEAS_DT}")
  |> filter(fn: (r) => r.target == "planting_start")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group()
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: 50)
"""
        tables = self.query_api.query(flux, org=self.org)
        for table in tables:
            for rec in table.records:
                v = rec.values
                epoch = safe_int(v.get("planting_start_epoch"), 0)
                action = str(v.get("action") or "").upper()
                if action == "CLEAR":
                    # A CLEAR command means no active desired start from DB.
                    return {
                        "time": v.get("_time"),
                        "command_id": str(v.get("command_id") or ""),
                        "action": "CLEAR",
                        "planting_start_epoch": 0,
                        "reason": str(v.get("reason") or "clear_start"),
                        "source": str(v.get("source") or "digital_twin"),
                    }
                if epoch > 0:
                    return {
                        "time": v.get("_time"),
                        "command_id": str(v.get("command_id") or ""),
                        "action": action or "SET_EPOCH",
                        "planting_start_epoch": epoch,
                        "reason": str(v.get("reason") or "latest_db_start"),
                        "source": str(v.get("source") or "digital_twin"),
                    }
        return None



# =============================================================================
# REALTIME BRIDGE TO FASTAPI BACKEND
# =============================================================================

class RealtimeBridge:
    """Non-blocking HTTP bridge: Gateway -> FastAPI -> WebSocket clients.

    Realtime path should not wait for InfluxDB query. Gateway posts event snapshots
    to Backend; Backend broadcasts them to Web/Unity over WebSocket.
    If Backend is offline, Gateway only drops realtime events; InfluxDB logging still runs.
    """

    def __init__(self, log: logging.Logger):
        self.log = log
        self.enabled = REALTIME_ENABLED
        self.url = f"{REALTIME_BACKEND_URL}{REALTIME_INGEST_PATH}"
        self.token = REALTIME_INGEST_TOKEN
        self.queue: queue.Queue[dict] = queue.Queue(maxsize=REALTIME_QUEUE_MAX)
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if not self.enabled:
            self.log.warning("Realtime bridge disabled by REALTIME_ENABLED=0")
            return
        self.thread = threading.Thread(target=self._worker, daemon=True)
        self.thread.start()
        self.log.info(f"Realtime bridge enabled: POST {self.url}")

    def stop(self) -> None:
        self.stop_event.set()

    def publish(self, event_type: str, data: dict, topic: str = "", extra: Optional[dict] = None) -> None:
        if not self.enabled:
            return
        event = {
            "type": event_type,
            "source": "gateway",
            "gateway_version": GW_VERSION,
            "node_id": data.get("node_id", NODE_ID) if isinstance(data, dict) else NODE_ID,
            "topic": topic,
            "timestamp": utc_now_iso(),
            "data": data,
        }
        if extra:
            event.update(extra)
        try:
            self.queue.put_nowait(event)
        except queue.Full:
            self.log.warning("Realtime queue full; drop event type=%s", event_type)

    def _worker(self) -> None:
        while not self.stop_event.is_set():
            try:
                event = self.queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                body = json.dumps(event, ensure_ascii=False).encode("utf-8")
                headers = {"Content-Type": "application/json"}
                if self.token:
                    headers["X-Realtime-Token"] = self.token
                req = urllib.request.Request(self.url, data=body, headers=headers, method="POST")
                with urllib.request.urlopen(req, timeout=REALTIME_TIMEOUT_S) as resp:
                    if resp.status >= 300:
                        self.log.debug("Realtime POST status=%s", resp.status)
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                self.log.debug("Realtime POST failed: %s", exc)
            except Exception as exc:
                self.log.debug("Realtime worker error: %s", exc)
            finally:
                self.queue.task_done()

# =============================================================================
# AI CONTROLLER
# =============================================================================

class EdgeAIWateringController:
    def __init__(self, model: Any, features: list[str], cfg: GatewayConfig):
        self.model = model
        self.features = features
        self.cfg = cfg

        self.pump_state = "OFF"
        self.pump_on_counter = 0
        self.cooldown_counter = 0
        self.last_soil: Optional[float] = None

    def resolve_phase(self, esp_phase: Any = None) -> int:
        phase = safe_int(esp_phase, 0)
        if phase in (1, 2):
            return phase
        if int(self.cfg.phase) in (1, 2):
            return int(self.cfg.phase)
        return 1

    @staticmethod
    def validate(temperature: float, air_humidity: float, lux: float, soil_moisture: float) -> list[str]:
        errors = []
        if not (0 <= temperature <= 60):
            errors.append("temperature_out_of_range")
        if not (0 <= air_humidity <= 100):
            errors.append("air_humidity_out_of_range")
        if not (0 <= lux <= 120000):
            errors.append("lux_out_of_range")
        if not (0 <= soil_moisture <= 100):
            errors.append("soil_moisture_out_of_range")
        return errors

    def predict(self, temperature: float, air_humidity: float, lux: float, soil_moisture: float, phase: int) -> dict:
        lag1 = self.last_soil if self.last_soil is not None else soil_moisture
        delta = soil_moisture - lag1

        row = {
            "temperature": float(temperature),
            "air_humidity": float(air_humidity),
            "lux": float(lux),
            "soil_moisture": float(soil_moisture),
            "phase": int(phase),
            "soil_moisture_lag1": float(lag1),
            "soil_moisture_delta": float(delta),
        }
        df = pd.DataFrame([row])

        for feature in self.features:
            if feature not in df.columns:
                df[feature] = 0.0

        df = df[self.features]

        pred = int(self.model.predict(df)[0])
        proba = self.model.predict_proba(df)[0] if hasattr(self.model, "predict_proba") else [0.5, 0.5]
        classes = getattr(self.model, "classes_", [0, 1])
        pmap = {int(c): float(p) for c, p in zip(classes, proba)}

        return {
            "need_watering": pred,
            "confidence": max(pmap.get(0, 0.0), pmap.get(1, 0.0)),
            "prob_no_watering": pmap.get(0, 0.0),
            "prob_need_watering": pmap.get(1, 0.0),
            "action": "PUMP_ON" if pred == 1 else "PUMP_OFF",
        }

    def decide(self, sensor: dict) -> dict:
        temperature = sensor["temperature"]
        air_humidity = sensor["air_humidity"]
        lux = sensor["lux"]
        soil_moisture = sensor["soil_avg"]
        phase = self.resolve_phase(sensor.get("phase"))

        errors = self.validate(temperature, air_humidity, lux, soil_moisture)
        if errors:
            return {
                "status": "ERROR",
                "source": "validation",
                "errors": errors,
                "need_watering": None,
                "confidence": 0.0,
                "action": "NO_ACTION",
                "reason": "invalid_sensor_data",
                "phase": phase,
            }

        if soil_moisture <= float(self.cfg.soil_force_on):
            return self._ok("safety_rule", 1, 1.0, "PUMP_ON", "soil_moisture_very_low", phase)

        if soil_moisture >= float(self.cfg.soil_force_off):
            return self._ok("safety_rule", 0, 1.0, "PUMP_OFF", "soil_moisture_enough", phase)

        ai = self.predict(temperature, air_humidity, lux, soil_moisture, phase)
        return {
            "status": "OK",
            "source": "random_forest",
            "errors": [],
            "need_watering": ai["need_watering"],
            "confidence": ai["confidence"],
            "prob_no_watering": ai["prob_no_watering"],
            "prob_need_watering": ai["prob_need_watering"],
            "action": ai["action"],
            "reason": "model_prediction",
            "phase": phase,
        }

    @staticmethod
    def _ok(source: str, need: int, conf: float, action: str, reason: str, phase: int) -> dict:
        return {
            "status": "OK",
            "source": source,
            "errors": [],
            "need_watering": need,
            "confidence": conf,
            "prob_no_watering": 1.0 - float(need),
            "prob_need_watering": float(need),
            "action": action,
            "reason": reason,
            "phase": phase,
        }

    def update_pump_state(self, decision: dict, soil_moisture: float) -> tuple[str, str]:
        if decision["status"] == "ERROR":
            return self.pump_state, "NO_ACTION_ERROR"

        phase = int(decision.get("phase", 1))
        dry_thresh = (
            float(self.cfg.dry_soil_threshold_phase1)
            if phase == 1 else
            float(self.cfg.dry_soil_threshold_phase2)
        )
        max_dry = (
            int(self.cfg.max_on_steps_dry_phase1)
            if phase == 1 else
            int(self.cfg.max_on_steps_dry_phase2)
        )

        if self.pump_state == "ON":
            self.pump_on_counter += 1

            if soil_moisture >= float(self.cfg.soil_force_off):
                self._turn_off(cooldown=int(self.cfg.cooldown_steps))
                return self.pump_state, "TURN_OFF_SOIL_OK"

            if soil_moisture < dry_thresh:
                if self.pump_on_counter >= max_dry:
                    self._turn_off(cooldown=1)
                    return self.pump_state, "TURN_OFF_DRY_MAX_RUNTIME"
                return self.pump_state, "KEEP_ON_DRY_SOIL"

            if self.pump_on_counter >= int(self.cfg.max_on_steps_normal):
                self._turn_off(cooldown=int(self.cfg.cooldown_steps))
                return self.pump_state, "TURN_OFF_MAX_RUNTIME"

            return self.pump_state, "KEEP_ON_SOIL_NOT_ENOUGH"

        if self.cooldown_counter > 0:
            self.cooldown_counter -= 1
            return self.pump_state, "KEEP_OFF_COOLDOWN"

        if soil_moisture < dry_thresh and decision["action"] == "PUMP_ON":
            self._turn_on()
            return self.pump_state, "TURN_ON_AI_OR_DRY"

        return self.pump_state, "KEEP_OFF"

    def _turn_on(self) -> None:
        self.pump_state = "ON"
        self.pump_on_counter = 1
        self.cooldown_counter = 0

    def _turn_off(self, cooldown: int) -> None:
        self.pump_state = "OFF"
        self.pump_on_counter = 0
        self.cooldown_counter = cooldown

    def create_payload(self, sensor: dict, step: int) -> dict:
        decision = self.decide(sensor)
        pump_state, ctrl_reason = self.update_pump_state(decision, sensor["soil_avg"])
        self.last_soil = float(sensor["soil_avg"])

        phase = int(decision.get("phase", self.resolve_phase(sensor.get("phase"))))

        return {
            "node_id": NODE_ID,
            "plant": PLANT_NAME,
            "timestamp": utc_now_iso(),
            "step": step,
            "phase": phase,
            "phase_source": sensor.get("phase_source", "MISSING"),
            "days_after_planting": sensor.get("days_after_planting", -1.0),
            "planting_start_epoch": int(sensor.get("planting_start_epoch", 0)),
            "planting_start_valid": bool(sensor.get("planting_start_valid", False)),
            "uptime_s": sensor.get("uptime_s", 0),
            "wifi_rssi": sensor.get("wifi_rssi", 0),
            "sensor": {
                "temperature": float(sensor["temperature"]),
                "air_humidity": float(sensor["air_humidity"]),
                "lux": float(sensor["lux"]),
                "soil_moisture": float(sensor["soil_avg"]),
                "soil_moisture_fused": float(sensor.get("soil_fused", sensor["soil_avg"])),
                "soil_moisture_mean": float(sensor.get("soil_mean", sensor["soil_avg"])),
                "soil_moisture_min": float(sensor.get("soil_min", sensor["soil_avg"])),
                "soil_moisture_max": float(sensor.get("soil_max", sensor["soil_avg"])),
                "soil_s1": float(sensor["soil_s1"]),
                "soil_s2": float(sensor["soil_s2"]),
                "soil_s3": float(sensor["soil_s3"]),
                "soil_s4": float(sensor["soil_s4"]),
                "soil_voltage_v1": float(sensor.get("soil_voltage_v1", 0.0)),
                "soil_voltage_v2": float(sensor.get("soil_voltage_v2", 0.0)),
                "soil_voltage_v3": float(sensor.get("soil_voltage_v3", 0.0)),
                "soil_voltage_v4": float(sensor.get("soil_voltage_v4", 0.0)),
                "soil_fusion_method": str(sensor.get("soil_fusion_method", "")),
                "soil_fusion_lo": float(sensor.get("soil_fusion_lo", 0.0)),
                "soil_fusion_hi": float(sensor.get("soil_fusion_hi", 0.0)),
                "soil_fusion_confidence": float(sensor.get("soil_fusion_confidence", 0.0)),
                "soil_fusion_valid_count": int(sensor.get("soil_fusion_valid_count", 0)),
                "soil_presence_state": str(sensor.get("soil_presence_state", "UNKNOWN")),
                "soil_control_reliable": bool(sensor.get("soil_control_reliable", False)),
                "soil_reliable": bool(sensor.get("soil_reliable", False)),
                "soil_stuck_zero": bool(sensor.get("soil_stuck_zero", False)),
                "soil_saturated_dry": bool(sensor.get("soil_saturated_dry", False)),
                "soil_sensor_fault": bool(sensor.get("soil_sensor_fault", False)),
                "soil_zero_streak": int(sensor.get("soil_zero_streak", 0)),
            },
            "ai": {
                "status": decision["status"],
                "source": decision["source"],
                "need_watering": decision["need_watering"],
                "confidence": round(float(decision["confidence"]), 4),
                "prob_no_watering": round(float(decision.get("prob_no_watering", 0)), 4),
                "prob_need_watering": round(float(decision.get("prob_need_watering", 0)), 4),
                "action": decision["action"],
                "reason": decision["reason"],
                "errors": decision.get("errors", []),
            },
            "control": {
                "pump": {
                    "state": pump_state,
                    "mode": "AI_AUTO",
                    "reason": ctrl_reason,
                },
                "light": {
                    "state": sensor.get("light_state", "OFF"),
                    "mode": sensor.get("light_mode", "AUTO_RTC"),
                    "reason": sensor.get("light_reason"),
                },
            },
        }


def load_model_and_features(model_path: Path, features_path: Path) -> tuple[Any, list[str]]:
    model = joblib.load(model_path)

    features_raw = json.loads(features_path.read_text(encoding="utf-8"))
    if not isinstance(features_raw, list) or not all(isinstance(x, str) for x in features_raw):
        raise ValueError("model_features.json phải là list[str].")

    n_model = getattr(model, "n_features_in_", None)
    if n_model is not None and len(features_raw) != int(n_model):
        raise ValueError(
            f"model_features.json có {len(features_raw)} feature {features_raw}, "
            f"nhưng model cần {n_model} feature."
        )

    return model, features_raw


# =============================================================================
# PARSERS
# =============================================================================

def parse_sensor(raw: dict) -> dict:
    s = raw.get("sensor", raw)

    temperature = safe_float(s.get("temperature", s.get("temp", 0)))
    air_humidity = safe_float(s.get("air_humidity", s.get("hum", 0)))
    lux = safe_float(s.get("lux", 0))

    # Firmware v2.9.x keeps soil_moisture_avg as backward-compatible fused value.
    soil_fused = safe_float(s.get("soil_moisture_fused", s.get("soil_moisture_avg", s.get("soil_moisture", 0))))
    soil_avg = safe_float(s.get("soil_moisture_avg", soil_fused))
    soil_mean = safe_float(s.get("soil_moisture_mean", soil_avg))
    soil_min = safe_float(s.get("soil_moisture_min", soil_avg))
    soil_max = safe_float(s.get("soil_moisture_max", soil_avg))

    raw_soil = s.get("soil_moisture_raw", {}) if isinstance(s.get("soil_moisture_raw", {}), dict) else {}
    raw_voltage = s.get("soil_voltage_raw", {}) if isinstance(s.get("soil_voltage_raw", {}), dict) else {}
    fusion = s.get("soil_fusion", {}) if isinstance(s.get("soil_fusion", {}), dict) else {}

    status = raw.get("status", {}) if isinstance(raw.get("status", {}), dict) else {}
    light_state = "ON" if safe_bool(status.get("light_on", False)) else "OFF"

    phase_raw = raw.get("phase", None)
    phase = safe_int(phase_raw, 0)
    if phase not in (1, 2):
        phase = None

    return {
        "temperature": temperature,
        "air_humidity": air_humidity,
        "lux": lux,
        "soil_avg": soil_avg,
        "soil_mean": soil_mean,
        "soil_fused": soil_fused,
        "soil_min": soil_min,
        "soil_max": soil_max,
        "soil_s1": safe_float(raw_soil.get("s1", s.get("s1", soil_avg))),
        "soil_s2": safe_float(raw_soil.get("s2", s.get("s2", soil_avg))),
        "soil_s3": safe_float(raw_soil.get("s3", s.get("s3", soil_avg))),
        "soil_s4": safe_float(raw_soil.get("s4", s.get("s4", soil_avg))),
        "soil_voltage_v1": safe_float(raw_voltage.get("v1", 0.0)),
        "soil_voltage_v2": safe_float(raw_voltage.get("v2", 0.0)),
        "soil_voltage_v3": safe_float(raw_voltage.get("v3", 0.0)),
        "soil_voltage_v4": safe_float(raw_voltage.get("v4", 0.0)),
        "soil_fusion_method": str(fusion.get("method") or ""),
        "soil_fusion_lo": safe_float(fusion.get("lo", 0.0)),
        "soil_fusion_hi": safe_float(fusion.get("hi", 0.0)),
        "soil_fusion_confidence": safe_float(fusion.get("confidence", 0.0)),
        "soil_fusion_valid_count": safe_int(fusion.get("valid_count", 0)),
        "soil_presence_state": str(fusion.get("presence_state") or "UNKNOWN"),
        "soil_control_reliable": safe_bool(fusion.get("control_reliable", False)),
        "soil_reliable": safe_bool(fusion.get("reliable", False)),
        "soil_stuck_zero": safe_bool(fusion.get("stuck_zero", False)),
        "soil_saturated_dry": safe_bool(fusion.get("saturated_dry", False)),
        "soil_sensor_fault": safe_bool(fusion.get("sensor_fault", False)),
        "soil_zero_streak": safe_int(fusion.get("zero_streak", 0)),
        "phase": phase,
        "phase_source": raw.get("phase_source", "MISSING"),
        "esp_step": safe_int(raw.get("step", 0)),
        "days_after_planting": safe_float(raw.get("days_after_planting", -1.0), -1.0),
        "planting_start_epoch": safe_int(raw.get("planting_start_epoch", 0)),
        "planting_start_valid": safe_bool(raw.get("planting_start_valid", False)),
        "light_state": light_state,
        "light_mode": status.get("light_mode", "AUTO_RTC"),
        "light_reason": status.get("light_reason"),
        "uptime_s": safe_int(raw.get("uptime_s", 0)),
        "wifi_rssi": safe_int(status.get("wifi_rssi", 0)),
    }


def parse_actuator_state(raw: dict) -> dict:
    pump = raw.get("pump", {})
    light = raw.get("light", {})

    if isinstance(pump, dict):
        pump_state = normalize_state(pump.get("state", raw.get("pump_state", raw.get("pump"))))
        pump_mode = pump.get("mode", raw.get("pump_mode", "UNKNOWN"))
        pump_reason = pump.get("reason", raw.get("pump_reason"))
    else:
        pump_state = normalize_state(pump)
        pump_mode = raw.get("pump_mode", "UNKNOWN")
        pump_reason = raw.get("pump_reason")

    if isinstance(light, dict):
        light_state = normalize_state(light.get("state", raw.get("light_state", raw.get("light"))))
        light_mode = light.get("mode", raw.get("light_mode", "UNKNOWN"))
        light_reason = light.get("reason", raw.get("light_reason"))
    else:
        light_state = normalize_state(light)
        light_mode = raw.get("light_mode", "UNKNOWN")
        light_reason = raw.get("light_reason")

    return {
        "node_id": raw.get("node_id", NODE_ID),
        "timestamp": raw.get("timestamp", utc_now_iso()),
        "pump": {"state": pump_state, "mode": pump_mode, "reason": pump_reason},
        "light": {"state": light_state, "mode": light_mode, "reason": light_reason},
        "raw": raw,
    }


# =============================================================================
# ALERTS
# =============================================================================

def build_alerts(sensor: dict, phase: int, cfg: GatewayConfig) -> list[str]:
    alerts = []
    temp = sensor["temperature"]
    hum = sensor["air_humidity"]
    lux = sensor["lux"]
    soil = sensor["soil_avg"]

    if temp > cfg.temp_max:
        alerts.append(f"Nhiệt độ cao {temp:.1f}°C (max {cfg.temp_max:.1f}°C)")

    if phase == 1:
        if hum < cfg.hum_p1_min:
            alerts.append(f"Độ ẩm KK thấp Phase 1 {hum:.1f}%")
        if hum > cfg.hum_p1_max:
            alerts.append(f"Độ ẩm KK cao Phase 1 {hum:.1f}% — nguy cơ nấm")
        if lux > cfg.lux_leak_warning:
            alerts.append(f"Lọt sáng Phase 1 {lux:.1f} lux")
    else:
        if hum < cfg.hum_p2_min:
            alerts.append(f"Độ ẩm KK thấp Phase 2 {hum:.1f}%")
        if hum > cfg.hum_p2_max:
            alerts.append(f"Độ ẩm KK cao Phase 2 {hum:.1f}% — nguy cơ nấm")
        if lux < cfg.lux_p2_min:
            alerts.append(f"Đèn yếu Phase 2 {lux:.1f} lux")

    if soil < cfg.soil_min_alert:
        alerts.append(f"Đất khô {soil:.1f}%")
    elif soil > cfg.soil_max_alert:
        alerts.append(f"Đất quá ẩm {soil:.1f}%")

    return alerts


# =============================================================================
# GATEWAY APP
# =============================================================================

class GatewayApp:
    def __init__(
        self,
        broker: str,
        port: int,
        model_path: Path,
        features_path: Path,
        config_path: Path,
        poll_commands_s: float,
        debug: bool = False,
    ):
        self.log = setup_logging(debug)
        self.cfg = GatewayConfig.from_json(config_path)
        self.influx = InfluxManager(self.log)
        self.realtime = RealtimeBridge(self.log)

        model, features = load_model_and_features(model_path, features_path)
        self.controller = EdgeAIWateringController(model, features, self.cfg)

        self.client = mqtt.Client(
            client_id=MQTT_CLIENT_ID,
            clean_session=True,
            protocol=mqtt.MQTTv311,
        )
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.reconnect_delay_set(min_delay=2, max_delay=30)

        self.broker = broker
        self.port = port
        self.poll_commands_s = poll_commands_s
        self.stop_event = threading.Event()

        self.gw_step = 0
        self.step_lock = threading.Lock()

        self.direct_until = {"pump": 0.0, "light": 0.0}
        self.direct_lock = threading.Lock()

        self.processed_command_ids: set[str] = set()
        self.sent_commands: dict[str, dict] = {}

        # Planting start sync: DB/Unity desired epoch vs ESP32 NVS actual epoch.
        self.desired_planting_start_epoch: int = 0
        self.desired_planting_start_command_id: str = ""
        self.actual_planting_start_epoch: int = 0
        self.actual_planting_start_valid: bool = False
        self.last_planting_reconcile_at: float = 0.0
        self.planting_reconcile_interval_s: float = 10.0
        self.planting_ack_timeout_s: float = 8.0

        # Chống spam cmd/pump: chỉ publish khi trạng thái đổi,
        # hoặc sau một khoảng heartbeat để ESP32 nhận lại nếu vừa reconnect.
        self.last_pump_sent: Optional[str] = None
        self.last_pump_sent_at: float = 0.0
        self.pump_heartbeat_s: float = 30.0

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def start(self) -> None:
        self.log.info("═" * 72)
        self.log.info(f"Raspberry Pi Gateway v{GW_VERSION}")
        self.log.info(f"MQTT  : {self.broker}:{self.port}")
        self.log.info(f"Influx: {self.influx.url} org={self.influx.org} bucket={self.influx.bucket}")
        self.log.info(f"Model features: {self.controller.features}")
        self.log.info("Flow realtime: ESP32 -> MQTT -> Gateway -> Backend/WebSocket; storage: Gateway -> InfluxDB")
        self.log.info("═" * 72)

        self.influx.start()
        self.realtime.start()
        self._connect_mqtt()

        try:
            self.processed_command_ids = self.influx.query_processed_command_ids(lookback="24h")
            self.log.info(f"Loaded processed command ids from InfluxDB: {len(self.processed_command_ids)}")
        except Exception as exc:
            self.log.warning(f"Cannot load processed command ids: {exc}")

        threading.Thread(target=self._influx_command_bridge_worker, daemon=True).start()

        self.client.loop_forever()

    def stop(self) -> None:
        self.stop_event.set()
        self.realtime.stop()
        self.influx.stop()
        try:
            self.client.publish(
                TOPIC_STATUS_GATEWAY,
                json.dumps({"node_id": NODE_ID, "gateway": "Raspberry Pi", "gw_version": GW_VERSION, "status": "offline", "timestamp": utc_now_iso()}),
                retain=True,
            )
            time.sleep(0.2)
            self.client.disconnect()
        except Exception:
            pass

    def _connect_mqtt(self) -> None:
        for attempt in range(1, 6):
            try:
                self.client.connect(self.broker, self.port, keepalive=MQTT_KEEPALIVE)
                return
            except Exception as exc:
                self.log.warning(f"MQTT connect attempt {attempt}/5: {exc}")
                if attempt == 5:
                    raise
                time.sleep(3)

    # -------------------------------------------------------------------------
    # MQTT callbacks
    # -------------------------------------------------------------------------

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.log.info("✅ MQTT connected.")
            client.subscribe(TOPIC_SENSOR, qos=MQTT_QOS)
            client.subscribe(TOPIC_STATUS_ESP32, qos=MQTT_QOS)
            client.subscribe(TOPIC_ACTUATOR_STATE, qos=MQTT_QOS)
            # v3.8.2: Gateway cũng nghe lệnh DIRECT do Backend/Web publish.
            # Mục tiêu không phải để chuyển tiếp lệnh, mà để bật "direct guard"
            # tránh AI AUTO pump gửi OFF/ON đè lên lệnh tay trong lúc ESP32 đang thực thi.
            client.subscribe(TOPIC_CMD_PUMP_DIRECT, qos=MQTT_QOS)
            client.subscribe(TOPIC_CMD_LIGHT_DIRECT, qos=MQTT_QOS)
            client.publish(
                TOPIC_STATUS_GATEWAY,
                json.dumps({
                    "node_id": NODE_ID,
                    "gateway": "Raspberry Pi",
                    "gw_version": GW_VERSION,
                    "status": "online",
                    "timestamp": utc_now_iso(),
                }, ensure_ascii=False),
                retain=True,
            )
            self.log.info(f"Subscribed: {TOPIC_SENSOR}, {TOPIC_STATUS_ESP32}, {TOPIC_ACTUATOR_STATE}")
            self.log.info(f"Subscribed direct guard: {TOPIC_CMD_PUMP_DIRECT}, {TOPIC_CMD_LIGHT_DIRECT}")
            self.realtime.publish("gateway_status", {"node_id": NODE_ID, "gateway": "Raspberry Pi", "gw_version": GW_VERSION, "status": "online", "timestamp": utc_now_iso()}, topic=TOPIC_STATUS_GATEWAY)
        else:
            self.log.error(f"MQTT connect failed rc={rc}")

    def _on_disconnect(self, client, userdata, rc):
        if rc == 0:
            self.log.info("MQTT disconnected cleanly.")
        else:
            self.log.warning(f"MQTT disconnected rc={rc}, auto reconnect.")

    def _on_message(self, client, userdata, msg):
        try:
            raw = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            self.log.error(f"[MQTT] Non-JSON topic={msg.topic}: {msg.payload[:100]}")
            return

        try:
            if msg.topic == TOPIC_SENSOR:
                self._handle_sensor(raw)
            elif msg.topic == TOPIC_STATUS_ESP32:
                self._handle_status(raw)
            elif msg.topic == TOPIC_ACTUATOR_STATE:
                self._handle_actuator_state(raw)
            elif msg.topic in (TOPIC_CMD_PUMP_DIRECT, TOPIC_CMD_LIGHT_DIRECT):
                # v3.8.2: Backend/Web đã publish trực tiếp xuống ESP32.
                # Gateway chỉ quan sát để tạm khóa AI AUTO tránh ghi đè lệnh tay.
                self._handle_direct_command_guard(raw, msg.topic)
            else:
                self.log.debug(f"Ignore topic={msg.topic}")
        except Exception as exc:
            self.log.error(f"[MQTT] handler error topic={msg.topic}: {exc}", exc_info=True)


    def _handle_direct_command_guard(self, raw: dict, topic: str) -> None:
        """Observe DIRECT pump/light commands and guard AI AUTO from overriding them.

        Backend v1.3.x publishes direct commands straight to ESP32. Without subscribing
        here, Gateway AI may still send AUTO pump command shortly after and make the
        Web/Unity operator feel the direct command was missed. This function does not
        forward the command; it only creates a short manual/direct window.
        """
        target = "pump" if topic == TOPIC_CMD_PUMP_DIRECT else "light"
        state = normalize_state(raw.get("state") or raw.get("action") or "")
        command_id = str(raw.get("command_id") or raw.get("id") or "").strip()
        seq = raw.get("seq")

        max_s = 15 if target == "pump" else 1800
        default_s = 10 if target == "pump" else 300
        duration_s = clamp_duration(raw.get("duration_s"), default_s=default_s, max_s=max_s)

        # ON: giữ guard theo duration để AI không gửi OFF đè.
        # OFF: guard ngắn để ESP32 xử lý xong rồi cho AI quay lại.
        if state == "ON":
            guard_s = max(duration_s + 3, 8 if target == "pump" else 3)
        elif state == "OFF":
            guard_s = 2
        else:
            guard_s = 2

        self._set_direct_active(target, guard_s)
        self.log.warning(
            f"[DIRECT_GUARD] target={target} state={state or 'UNKNOWN'} "
            f"command_id={command_id or '-'} seq={seq if seq is not None else '-'} "
            f"hold_auto={guard_s}s topic={topic}"
        )
        try:
            self.realtime.publish(
                "command_seen",
                raw,
                topic=topic,
                extra={
                    "command_id": command_id,
                    "target": target,
                    "status": "SEEN_BY_GATEWAY",
                    "direct_guard_s": guard_s,
                },
            )
        except Exception as exc:
            self.log.debug(f"[DIRECT_GUARD] realtime publish skipped: {exc}")

    # -------------------------------------------------------------------------
    # Sensor processing
    # -------------------------------------------------------------------------

    def _next_gw_step(self) -> int:
        with self.step_lock:
            self.gw_step += 1
            return self.gw_step

    def _handle_sensor(self, raw: dict) -> None:
        sensor = parse_sensor(raw)
        self._update_actual_planting_from_sensor(sensor)
        self._reconcile_planting_start_if_needed("telemetry")
        gw_step = self._next_gw_step()
        step = sensor["esp_step"] if sensor["esp_step"] > 0 else gw_step

        payload = self.controller.create_payload(sensor, step=step)
        payload["gw_step"] = gw_step

        phase = int(payload["phase"])
        alerts = build_alerts(sensor, phase, self.cfg)
        payload["alert"] = "; ".join(alerts) if alerts else None

        pump_state = payload["control"]["pump"]["state"]
        pump_reason = payload["control"]["pump"]["reason"]
        ai_source = payload["ai"]["source"]
        confidence = float(payload["ai"]["confidence"])

        self.log.info("─" * 72)
        self.log.info(
            f"Step {step:4d} | Phase {phase} ({payload['phase_source']}) | "
            f"days={payload['days_after_planting']:.2f}"
        )
        self.log.info(
            f"T={sensor['temperature']:.1f}°C RH={sensor['air_humidity']:.1f}% "
            f"Lux={sensor['lux']:.1f} Soil={sensor['soil_avg']:.1f}%"
        )
        self.log.info(f"AI [{ai_source}] -> Pump {pump_state} ({pump_reason}) conf={confidence:.0%}")
        for alert in alerts:
            self.log.warning(f"⚠️ {alert}")

        if self._is_direct_active("pump"):
            self.log.warning(f"SKIP AUTO {TOPIC_CMD_PUMP_AUTO}: Digital Twin direct pump active.")
        else:
            now = time.time()
            should_publish = (
                pump_state != self.last_pump_sent
                or (now - self.last_pump_sent_at) >= self.pump_heartbeat_s
            )

            if should_publish:
                result = self.client.publish(TOPIC_CMD_PUMP_AUTO, pump_state, qos=MQTT_QOS)
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    self.last_pump_sent = pump_state
                    self.last_pump_sent_at = now
                    self.log.info(f"→ {TOPIC_CMD_PUMP_AUTO}: {pump_state}")
                    try:
                        self.influx.write_command_event(
                            "auto_pump",
                            "pump",
                            "SENT",
                            f"topic={TOPIC_CMD_PUMP_AUTO}; state={pump_state}; reason={pump_reason}",
                        )
                    except Exception as exc:
                        self.log.debug(f"[InfluxDB] auto cmd event write skipped: {exc}")
                else:
                    self.log.error(f"→ {TOPIC_CMD_PUMP_AUTO} FAILED rc={result.rc}")
            else:
                self.log.debug(f"Skip duplicate {TOPIC_CMD_PUMP_AUTO}: {pump_state}")

        self.influx.enqueue_telemetry(payload)
        try:
            self.influx.write_latest_state("telemetry", payload)
        except Exception as exc:
            self.log.debug(f"latest_state telemetry write skipped: {exc}")

        # Realtime path: không chờ Unity/Web query lại InfluxDB.
        self.realtime.publish("sensor", payload, topic=TOPIC_SENSOR)

    def _handle_actuator_state(self, raw: dict) -> None:
        state = parse_actuator_state(raw)

        try:
            self.influx.write_actuator_state(state)
            self.influx.write_latest_state("actuator_state", state)
        except Exception as exc:
            self.log.error(f"[InfluxDB] actuator/latest_state write error: {exc}")

        pump_state = state["pump"]["state"]
        light_state = state["light"]["state"]

        for command_id, cmd in list(self.sent_commands.items()):
            target = cmd.get("target")
            if target not in ("pump", "light"):
                continue

            expected = cmd.get("state")
            actual = pump_state if target == "pump" else light_state
            if actual == expected:
                try:
                    self.influx.write_command_event(command_id, target, "DONE", f"actuator_state={actual}")
                except Exception as exc:
                    self.log.error(f"[InfluxDB] command DONE write error: {exc}")
                self.processed_command_ids.add(command_id)
                self.sent_commands.pop(command_id, None)

        self.log.info(
            f"[ACTUATOR_STATE] pump={pump_state}/{state['pump']['mode']} "
            f"light={light_state}/{state['light']['mode']}"
        )
        self.realtime.publish("actuator_state", state, topic=TOPIC_ACTUATOR_STATE)

    def _handle_status(self, raw: dict) -> None:
        """Handle ESP32 status ACKs, especially planting_start command acknowledgements."""
        try:
            self.influx.write_latest_state("status", raw)
        except Exception as exc:
            self.log.debug(f"latest_state status write skipped: {exc}")

        self.realtime.publish("esp32_status", raw, topic=TOPIC_STATUS_ESP32)

        command_id = str(raw.get("command_id") or raw.get("id") or "").strip()
        event = str(raw.get("event") or "").strip()
        target = str(raw.get("target") or "").strip().lower()
        status = str(raw.get("status") or "").strip().upper()

        is_planting_ack = (
            target == "planting_start"
            or event.startswith("planting_start")
            or event in ("planting_start_updated", "planting_start_current", "planting_start_cleared")
        )

        if not command_id or not is_planting_ack:
            return

        ack_epoch = safe_int(raw.get("planting_start_epoch"), 0)
        ack_valid = safe_bool(raw.get("planting_start_valid", False))
        self.actual_planting_start_epoch = ack_epoch
        self.actual_planting_start_valid = ack_valid

        try:
            self.influx.write_planting_start_state(raw)
            self.influx.write_latest_state("planting_start", raw)
        except Exception as exc:
            self.log.error(f"[InfluxDB] planting_start_state write error: {exc}")

        # ESP32 should normally respond with status=DONE for SET_NOW/SET_EPOCH/CLEAR/GET.
        done_events = {
            "planting_start_updated",
            "planting_start_current",
            "planting_start_cleared",
        }
        if status in ("ERROR", "FAILED", "FAIL"):
            event_status = "ERROR"
        elif status in ("DONE", "OK", "SUCCESS") or event in done_events:
            event_status = "DONE"
        else:
            event_status = "DONE"

        expected_epoch = safe_int(self.sent_commands.get(command_id, {}).get("planting_start_epoch"), 0)
        if event_status == "DONE" and expected_epoch > 0 and ack_epoch != expected_epoch:
            event_status = "ERROR"
            raw = dict(raw)
            raw["gateway_error"] = f"epoch_mismatch expected={expected_epoch} actual={ack_epoch}"

        try:
            self.influx.write_command_event(
                command_id,
                "planting_start",
                event_status,
                json.dumps(raw, ensure_ascii=False),
            )
        except Exception as exc:
            self.log.error(f"[InfluxDB] planting_start ACK event write error: {exc}")

        was_gateway_sent = command_id in self.sent_commands
        self.processed_command_ids.add(command_id)
        self.sent_commands.pop(command_id, None)

        # Planting_start command is published as retained config command.
        # After ESP32 ACKs for a command sent by this Gateway, clear retained payload so old config command won't replay forever.
        if event_status == "DONE" and was_gateway_sent:
            clear_ret = self.client.publish(TOPIC_CMD_PLANTING_START, payload=None, qos=MQTT_QOS, retain=True)
            if clear_ret.rc == mqtt.MQTT_ERR_SUCCESS:
                self.log.info(f"[PLANTING_ACK] cleared retained {TOPIC_CMD_PLANTING_START}")
            else:
                self.log.warning(f"[PLANTING_ACK] clear retained failed rc={clear_ret.rc}")

        self.log.info(
            f"[PLANTING_ACK] command_id={command_id} event={event} "
            f"status={event_status} start={raw.get('planting_start_epoch')}"
        )
        self.realtime.publish("planting_start_ack", raw, topic=TOPIC_STATUS_ESP32, extra={"command_id": command_id, "status": event_status})

    # -------------------------------------------------------------------------
    # InfluxDB dt_commands -> MQTT dt/cmd bridge
    # -------------------------------------------------------------------------

    def _influx_command_bridge_worker(self) -> None:
        self.log.info("InfluxDB command bridge started: polling measurement dt.")
        while not self.stop_event.is_set():
            try:
                self._refresh_desired_planting_start()
                self._retry_timed_out_planting_start_commands()

                commands = self.influx.query_pending_commands(lookback="24h", limit=20)
                now = time.time()
                for cmd in commands:
                    command_id = cmd["command_id"]
                    target = str(cmd.get("target") or "").lower()

                    # InfluxDB dt is append-only. Older planting_start rows may remain PENDING;
                    # do not replay them after a newer Unity/Web Start exists.
                    if self._planting_command_is_stale(cmd):
                        try:
                            self.influx.write_command_event(command_id, target, "SUPERSEDED", "stale planting_start ignored; newer desired command exists")
                        except Exception:
                            pass
                        self.processed_command_ids.add(command_id)
                        self.log.warning(f"[PLANTING_SYNC] skip stale command_id={command_id} epoch={cmd.get('planting_start_epoch')} desired={self.desired_planting_start_epoch}")
                        continue

                    if command_id in self.processed_command_ids:
                        continue

                    # Planting start must wait for ESP32 ACK. If already SENT, retry only after timeout.
                    if target == "planting_start" and command_id in self.sent_commands:
                        sent_at = float(self.sent_commands[command_id].get("sent_at", 0.0))
                        if (now - sent_at) < self.planting_ack_timeout_s:
                            continue

                    self._send_influx_command(cmd)
            except Exception as exc:
                self.log.error(f"[INFLUX_BRIDGE] query/send error: {exc}", exc_info=True)
            time.sleep(self.poll_commands_s)

    def _send_influx_command(self, cmd: dict) -> None:
        command_id = cmd["command_id"]
        target = str(cmd["target"]).strip().lower()

        if target == "planting_start":
            self._send_planting_start_command(cmd)
            return

        state = normalize_state(cmd["state"])

        if target not in ("pump", "light"):
            self.influx.write_command_event(command_id, target, "ERROR", f"invalid target={target}")
            self.processed_command_ids.add(command_id)
            return

        if state not in ("ON", "OFF"):
            self.influx.write_command_event(command_id, target, "ERROR", f"invalid state={state}")
            self.processed_command_ids.add(command_id)
            return

        max_s = 15 if target == "pump" else 1800
        default_s = 10 if target == "pump" else 300
        duration_s = clamp_duration(cmd.get("duration_s"), default_s=default_s, max_s=max_s)

        topic = TOPIC_CMD_PUMP_DIRECT if target == "pump" else TOPIC_CMD_LIGHT_DIRECT
        payload = {
            "id": command_id,
            "command_id": command_id,
            "source": cmd.get("source") or "digital_twin_influx",
            "mode": "DIRECT",
            "target": target,
            "state": state,
            "duration_s": duration_s,
            "reason": cmd.get("reason") or "influx_command",
            "sent_at": utc_now_iso(),
        }

        result = self.client.publish(topic, json.dumps(payload, ensure_ascii=False), qos=MQTT_QOS)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            self.influx.write_command_event(command_id, target, "SENT", json.dumps(payload, ensure_ascii=False))
            self.processed_command_ids.add(command_id)
            self.sent_commands[command_id] = {"target": target, "state": state, "duration_s": duration_s}
            self._set_direct_active(target, duration_s + 2)
            self.log.warning(f"[INFLUX_BRIDGE] SENT id={command_id} -> {topic}: {state} duration={duration_s}s")
            self.realtime.publish("command_sent", payload, topic=topic, extra={"command_id": command_id, "target": target, "status": "SENT"})
        else:
            self.influx.write_command_event(command_id, target, "ERROR", f"mqtt publish rc={result.rc}")

    def _send_planting_start_command(self, cmd: dict) -> None:
        """Bridge target=planting_start from InfluxDB to ESP32 via MQTT."""
        command_id = cmd["command_id"]
        action = str(cmd.get("action") or "SET_NOW").strip().upper()
        reason = cmd.get("reason") or "planting_start_command"

        allowed_actions = {"SET_NOW", "SET_EPOCH", "CLEAR", "GET"}
        if action not in allowed_actions:
            self.influx.write_command_event(command_id, "planting_start", "ERROR", f"invalid action={action}")
            self.processed_command_ids.add(command_id)
            return

        payload = {
            "id": command_id,
            "command_id": command_id,
            "source": cmd.get("source") or "digital_twin_influx",
            "target": "planting_start",
            "action": action,
            "reason": reason,
            "sent_at": utc_now_iso(),
        }

        if action == "SET_EPOCH":
            epoch = safe_int(cmd.get("planting_start_epoch"), 0)
            if epoch <= 0:
                self.influx.write_command_event(
                    command_id,
                    "planting_start",
                    "ERROR",
                    "SET_EPOCH requires planting_start_epoch > 0",
                )
                self.processed_command_ids.add(command_id)
                return
            payload["planting_start_epoch"] = epoch

        result = self.client.publish(
            TOPIC_CMD_PLANTING_START,
            json.dumps(payload, ensure_ascii=False),
            qos=MQTT_QOS,
            retain=True,
        )

        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            self.influx.write_command_event(command_id, "planting_start", "SENT", json.dumps(payload, ensure_ascii=False))
            # Do NOT mark processed on SENT. Only ESP32 ACK DONE/ERROR closes this command.
            self.sent_commands[command_id] = {
                "target": "planting_start",
                "action": action,
                "planting_start_epoch": int(payload.get("planting_start_epoch") or 0),
                "sent_at": time.time(),
                "payload": payload,
            }
            if action == "SET_EPOCH" and int(payload.get("planting_start_epoch") or 0) > 0:
                self.desired_planting_start_epoch = int(payload["planting_start_epoch"])
                self.desired_planting_start_command_id = command_id
            self.log.warning(f"[INFLUX_BRIDGE] SENT id={command_id} -> {TOPIC_CMD_PLANTING_START}: {action}")
            self.realtime.publish("command_sent", payload, topic=TOPIC_CMD_PLANTING_START, extra={"command_id": command_id, "target": "planting_start", "status": "SENT"})
        else:
            self.influx.write_command_event(command_id, "planting_start", "ERROR", f"mqtt publish rc={result.rc}")
            self.processed_command_ids.add(command_id)

    def _update_actual_planting_from_sensor(self, sensor: dict) -> None:
        epoch = safe_int(sensor.get("planting_start_epoch"), 0)
        valid = safe_bool(sensor.get("planting_start_valid", False))
        if epoch > 0 or valid != self.actual_planting_start_valid:
            self.actual_planting_start_epoch = epoch
            self.actual_planting_start_valid = valid

    def _refresh_desired_planting_start(self) -> None:
        desired = self.influx.query_latest_desired_planting_start(lookback="30d")
        if not desired:
            return

        action = str(desired.get("action") or "").upper()
        epoch = safe_int(desired.get("planting_start_epoch"), 0)
        command_id = str(desired.get("command_id") or "")

        if action == "CLEAR":
            if self.desired_planting_start_epoch != 0:
                self.log.warning("[PLANTING_SYNC] desired cleared by DB")
            self.desired_planting_start_epoch = 0
            self.desired_planting_start_command_id = command_id
            return

        if epoch <= 0:
            return

        if epoch != self.desired_planting_start_epoch:
            self.log.warning(f"[PLANTING_SYNC] desired epoch from DB={epoch} command_id={command_id}")
        self.desired_planting_start_epoch = epoch
        self.desired_planting_start_command_id = command_id
        self._reconcile_planting_start_if_needed("db_refresh")

    def _planting_command_is_stale(self, cmd: dict) -> bool:
        """Return True if a pending planting_start command is older than the latest DB desired state.

        InfluxDB is append-only, so older dt rows remain PENDING forever. Without this guard,
        Gateway can replay an old start_epoch and force ESP32 NVS backward.
        """
        target = str(cmd.get("target") or "").lower()
        if target != "planting_start":
            return False

        action = str(cmd.get("action") or "").upper()
        command_id = str(cmd.get("command_id") or "")

        # GET is harmless and should be allowed. CLEAR is only allowed when it is the latest desired command.
        if action == "GET":
            return False

        desired_cmd = str(self.desired_planting_start_command_id or "")
        desired_epoch = int(self.desired_planting_start_epoch or 0)
        epoch = safe_int(cmd.get("planting_start_epoch"), 0)

        if desired_cmd and command_id != desired_cmd:
            return True

        if action == "SET_EPOCH" and desired_epoch > 0 and epoch > 0 and epoch != desired_epoch:
            return True

        return False

    def _reconcile_planting_start_if_needed(self, source: str) -> None:
        desired = int(self.desired_planting_start_epoch or 0)
        if desired <= 0:
            return

        actual = int(self.actual_planting_start_epoch or 0)
        valid = bool(self.actual_planting_start_valid)
        if valid and actual == desired:
            return

        now = time.time()
        if (now - self.last_planting_reconcile_at) < self.planting_reconcile_interval_s:
            return

        command_id = f"reconcile-{desired}"
        cmd = {
            "command_id": command_id,
            "target": "planting_start",
            "action": "SET_EPOCH",
            "planting_start_epoch": desired,
            "reason": f"reconcile_db_vs_esp32_{source}",
            "source": "gateway_reconcile",
        }
        self.log.warning(
            f"[PLANTING_SYNC] mismatch desired={desired} actual={actual} valid={valid}; resend SET_EPOCH"
        )
        self._send_planting_start_command(cmd)
        self.last_planting_reconcile_at = now

    def _retry_timed_out_planting_start_commands(self) -> None:
        now = time.time()
        for command_id, cmd in list(self.sent_commands.items()):
            if cmd.get("target") != "planting_start":
                continue
            sent_at = float(cmd.get("sent_at", 0.0))
            if (now - sent_at) < self.planting_ack_timeout_s:
                continue
            payload = cmd.get("payload")
            if not isinstance(payload, dict):
                continue
            result = self.client.publish(
                TOPIC_CMD_PLANTING_START,
                json.dumps(payload, ensure_ascii=False),
                qos=MQTT_QOS,
                retain=True,
            )
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                cmd["sent_at"] = now
                self.influx.write_command_event(command_id, "planting_start", "SENT", json.dumps(payload, ensure_ascii=False))
                self.log.warning(f"[PLANTING_SYNC] retry SET_EPOCH command_id={command_id}")
            else:
                self.influx.write_command_event(command_id, "planting_start", "ERROR", f"retry mqtt publish rc={result.rc}")
                self.processed_command_ids.add(command_id)
                self.sent_commands.pop(command_id, None)

    def _set_direct_active(self, target: str, duration_s: int) -> None:
        with self.direct_lock:
            self.direct_until[target] = max(self.direct_until.get(target, 0.0), time.time() + duration_s)

    def _is_direct_active(self, target: str) -> bool:
        with self.direct_lock:
            return time.time() < self.direct_until.get(target, 0.0)


# =============================================================================
# CLI
# =============================================================================

_app: Optional[GatewayApp] = None


def handle_signal(sig, frame):
    if _app:
        _app.log.info("Shutdown gracefully...")
        _app.stop()
    sys.exit(0)


def main() -> None:
    global _app

    parser = argparse.ArgumentParser(description="Raspberry Pi Gateway + InfluxDB Digital Twin Bridge")
    parser.add_argument("--broker", default=MQTT_BROKER)
    parser.add_argument("--port", type=int, default=MQTT_PORT)
    parser.add_argument("--model", default=str(MODEL_PATH))
    parser.add_argument("--features", default=str(FEATURES_PATH))
    parser.add_argument("--config", default=str(CONFIG_PATH))
    parser.add_argument("--poll-commands-s", type=float, default=float(os.getenv("POLL_COMMANDS_S", "1.0")))
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--insert-test", choices=["pump_on", "pump_off", "light_on", "light_off", "planting_start_now", "planting_start_get", "planting_start_clear"])

    args = parser.parse_args()

    if args.insert_test:
        log = setup_logging(args.debug)
        influx = InfluxManager(log)
        mapping = {
            "pump_on": ("pump", "ON", 10, "", "cli_test"),
            "pump_off": ("pump", "OFF", 0, "", "cli_test"),
            "light_on": ("light", "ON", 300, "", "cli_test"),
            "light_off": ("light", "OFF", 0, "", "cli_test"),
            "planting_start_now": ("planting_start", "", 0, "SET_NOW", "cli_test_new_batch"),
            "planting_start_get": ("planting_start", "", 0, "GET", "cli_test_get_start"),
            "planting_start_clear": ("planting_start", "", 0, "CLEAR", "cli_test_clear_start"),
        }
        target, state, duration, action, reason = mapping[args.insert_test]
        command_id = influx.insert_test_command(
            target,
            state,
            duration_s=duration,
            reason=reason,
            action=action,
        )
        print(f"Inserted InfluxDB command_id={command_id}: target={target} state={state} action={action} duration={duration}")
        return

    _app = GatewayApp(
        broker=args.broker,
        port=args.port,
        model_path=Path(args.model),
        features_path=Path(args.features),
        config_path=Path(args.config),
        poll_commands_s=args.poll_commands_s,
        debug=args.debug,
    )

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    _app.start()


if __name__ == "__main__":
    main()



