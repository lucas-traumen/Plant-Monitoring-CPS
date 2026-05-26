#!/usr/bin/env python3
"""
gateway.py — BBB Edge AI Gateway + InfluxDB Digital Twin Bridge
===============================================================

Flow mới, không dùng SQL:

TELEMETRY:
    ESP32 -> MQTT -> BBB Gateway -> InfluxDB sensor_data/latest_state

AUTO CONTROL:
    ESP32 sensor -> BBB Gateway Random Forest -> cps/greenhouse/cmd/pump -> ESP32

DIGITAL TWIN CONTROL:
    Digital Twin/Web/Unity -> InfluxDB measurement dt_commands
    BBB Gateway poll dt_commands PENDING -> MQTT cps/greenhouse/dt/cmd/pump/light -> ESP32
    ESP32 -> cps/greenhouse/actuator/state -> BBB
    BBB -> InfluxDB actuator_state + dt_command_events

Cài đặt:
    pip install paho-mqtt influxdb-client joblib pandas scikit-learn

Chạy:
    python gateway.py --debug

Lưu ý:
    File này giữ fallback InfluxDB token theo yêu cầu để chạy ngay.
    Có thể override bằng biến môi trường nếu cần.
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
GW_VERSION = "3.3-influx-dt-bridge-token-fallback"

MODEL_PATH = BASE_DIR / "watering_random_forest_model.pkl"
FEATURES_PATH = BASE_DIR / "model_features.json"
CONFIG_PATH = BASE_DIR / "controller_config.json"

MQTT_BROKER = os.getenv("MQTT_BROKER", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_KEEPALIVE = 60
MQTT_QOS = 1
MQTT_CLIENT_ID = "bbb_gateway_brassica_influx_bridge"

# ESP32 -> BBB / Digital Twin
TOPIC_SENSOR = "cps/greenhouse/sensors"
TOPIC_STATUS = "cps/greenhouse/status"
TOPIC_ACTUATOR_STATE = "cps/greenhouse/actuator/state"

# BBB -> ESP32: AUTO control
TOPIC_CMD_PUMP = "cps/greenhouse/cmd/pump"
TOPIC_CMD_LIGHT = "cps/greenhouse/cmd/light"  # legacy/manual, không publish trong AUTO

# BBB Influx Bridge -> ESP32: Digital Twin direct command
TOPIC_DT_CMD_PUMP = "cps/greenhouse/dt/cmd/pump"
TOPIC_DT_CMD_LIGHT = "cps/greenhouse/dt/cmd/light"

# ── InfluxDB Cloud ───────────────────────────────────────────────────────────
# Giữ fallback hard-code theo file cũ để chạy ngay.
# Có thể override bằng biến môi trường nếu cần.
INFLUX_URL_DEFAULT = "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUX_TOKEN_DEFAULT = "6pSuWQaFLlWq6iRVfaRYEMwIO1DDEChBsG42HdDx5En6fuqpUx95j3xswbVNrcWxRrs_sizN6XXESjzNqcHzJA=="
INFLUX_ORG_DEFAULT = "DEV_TEAM"
INFLUX_BUCKET_DEFAULT = "digital_twin_data"


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
      sensor_data        : telemetry time-series
      actuator_state     : physical relay feedback from ESP32
      latest_state       : JSON snapshot for Digital Twin
      dt_commands        : command queue written by Digital Twin
      dt_command_events  : SENT / DONE / ERROR event log written by Gateway

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

        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
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
            Point("sensor_data")
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
            .field("alert", str(payload.get("alert") or ""))
            .time(datetime.now(timezone.utc), WRITE_PRECISION_SECONDS)
        )
        self.write_api.write(bucket=self.bucket, record=point)

    def write_actuator_state(self, state: dict) -> None:
        pump = state["pump"]
        light = state["light"]

        point = (
            Point("actuator_state")
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
            Point("latest_state")
            .tag("node_id", NODE_ID)
            .tag("key", key)
            .field("value_json", json.dumps(value, ensure_ascii=False))
            .time(datetime.now(timezone.utc), WRITE_PRECISION_SECONDS)
        )
        self.write_api.write(bucket=self.bucket, record=point)

    def write_command_event(self, command_id: str, target: str, status: str, message: str = "") -> None:
        point = (
            Point("dt_command_events")
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
        state: str,
        duration_s: int,
        reason: str,
        source: str = "digital_twin",
    ) -> None:
        """
        Digital Twin/Web/Unity should write this same shape.

        Measurement: dt_commands
        Tags       : node_id, command_id, target, status=PENDING
        Fields     : state, duration_s, reason, source
        """
        point = (
            Point("dt_commands")
            .tag("node_id", NODE_ID)
            .tag("command_id", command_id)
            .tag("target", target)
            .tag("status", "PENDING")
            .field("state", state.upper())
            .field("duration_s", int(duration_s))
            .field("reason", reason)
            .field("source", source)
            .time(datetime.now(timezone.utc), WRITE_PRECISION_SECONDS)
        )
        self.write_api.write(bucket=self.bucket, record=point)

    def insert_test_command(self, target: str, state: str, duration_s: int, reason: str) -> str:
        command_id = f"cmd-{uuid.uuid4().hex[:12]}"
        self.write_command(command_id, target, state, duration_s, reason, source="cli_test")
        return command_id

    # -------------------------------------------------------------------------
    # Queries
    # -------------------------------------------------------------------------

    def query_processed_command_ids(self, lookback: str = "24h") -> set[str]:
        flux = f"""
from(bucket: "{self.bucket}")
  |> range(start: -{lookback})
  |> filter(fn: (r) => r._measurement == "dt_command_events")
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
  |> filter(fn: (r) => r._measurement == "dt_commands")
  |> filter(fn: (r) => r.status == "PENDING")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"], desc: false)
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
                })

        return commands


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
            "uptime_s": sensor.get("uptime_s", 0),
            "wifi_rssi": sensor.get("wifi_rssi", 0),
            "sensor": {
                "temperature": float(sensor["temperature"]),
                "air_humidity": float(sensor["air_humidity"]),
                "lux": float(sensor["lux"]),
                "soil_moisture": float(sensor["soil_avg"]),
                "soil_s1": float(sensor["soil_s1"]),
                "soil_s2": float(sensor["soil_s2"]),
                "soil_s3": float(sensor["soil_s3"]),
                "soil_s4": float(sensor["soil_s4"]),
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

    if "soil_moisture_avg" in s:
        soil_avg = safe_float(s["soil_moisture_avg"])
    elif "soil_moisture" in s:
        soil_avg = safe_float(s["soil_moisture"])
    else:
        vals = [safe_float(s.get(k), None) for k in ("s1", "s2", "s3", "s4") if s.get(k) is not None]
        vals = [v for v in vals if v is not None]
        soil_avg = sum(vals) / len(vals) if vals else 0.0

    raw_soil = s.get("soil_moisture_raw", {}) if isinstance(s.get("soil_moisture_raw", {}), dict) else {}

    status = raw.get("status", {}) if isinstance(raw.get("status", {}), dict) else {}
    light_state = "ON" if bool(status.get("light_on", False)) else "OFF"

    phase_raw = raw.get("phase", None)
    phase = safe_int(phase_raw, 0)
    if phase not in (1, 2):
        phase = None

    return {
        "temperature": temperature,
        "air_humidity": air_humidity,
        "lux": lux,
        "soil_avg": soil_avg,
        "soil_s1": safe_float(raw_soil.get("s1", s.get("s1", soil_avg))),
        "soil_s2": safe_float(raw_soil.get("s2", s.get("s2", soil_avg))),
        "soil_s3": safe_float(raw_soil.get("s3", s.get("s3", soil_avg))),
        "soil_s4": safe_float(raw_soil.get("s4", s.get("s4", soil_avg))),
        "phase": phase,
        "phase_source": raw.get("phase_source", "MISSING"),
        "esp_step": safe_int(raw.get("step", 0)),
        "days_after_planting": safe_float(raw.get("days_after_planting", -1.0), -1.0),
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

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def start(self) -> None:
        self.log.info("═" * 72)
        self.log.info(f"BBB Gateway v{GW_VERSION}")
        self.log.info(f"MQTT  : {self.broker}:{self.port}")
        self.log.info(f"Influx: {self.influx.url} org={self.influx.org} bucket={self.influx.bucket}")
        self.log.info(f"Model features: {self.controller.features}")
        self.log.info("Flow: DT -> InfluxDB dt_commands -> Gateway -> MQTT -> ESP32")
        self.log.info("═" * 72)

        self.influx.start()
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
        self.influx.stop()
        try:
            self.client.publish(
                TOPIC_STATUS,
                json.dumps({"node_id": NODE_ID, "gateway": "BBB", "status": "offline", "timestamp": utc_now_iso()}),
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
            client.subscribe(TOPIC_ACTUATOR_STATE, qos=MQTT_QOS)
            client.publish(
                TOPIC_STATUS,
                json.dumps({
                    "node_id": NODE_ID,
                    "gateway": "BBB",
                    "gw_version": GW_VERSION,
                    "status": "online",
                    "timestamp": utc_now_iso(),
                }, ensure_ascii=False),
                retain=True,
            )
            self.log.info(f"Subscribed: {TOPIC_SENSOR}, {TOPIC_ACTUATOR_STATE}")
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
            elif msg.topic == TOPIC_ACTUATOR_STATE:
                self._handle_actuator_state(raw)
            else:
                self.log.debug(f"Ignore topic={msg.topic}")
        except Exception as exc:
            self.log.error(f"[MQTT] handler error topic={msg.topic}: {exc}", exc_info=True)

    # -------------------------------------------------------------------------
    # Sensor processing
    # -------------------------------------------------------------------------

    def _next_gw_step(self) -> int:
        with self.step_lock:
            self.gw_step += 1
            return self.gw_step

    def _handle_sensor(self, raw: dict) -> None:
        sensor = parse_sensor(raw)
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
            self.log.warning(f"SKIP AUTO {TOPIC_CMD_PUMP}: Digital Twin direct pump active.")
        else:
            self.client.publish(TOPIC_CMD_PUMP, pump_state, qos=MQTT_QOS)
            self.log.info(f"→ {TOPIC_CMD_PUMP}: {pump_state}")

        self.influx.enqueue_telemetry(payload)
        try:
            self.influx.write_latest_state("telemetry", payload)
        except Exception as exc:
            self.log.debug(f"latest_state telemetry write skipped: {exc}")

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
            target = cmd["target"]
            expected = cmd["state"]
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

    # -------------------------------------------------------------------------
    # InfluxDB dt_commands -> MQTT dt/cmd bridge
    # -------------------------------------------------------------------------

    def _influx_command_bridge_worker(self) -> None:
        self.log.info("InfluxDB command bridge started: polling measurement dt_commands.")
        while not self.stop_event.is_set():
            try:
                commands = self.influx.query_pending_commands(lookback="24h", limit=20)
                for cmd in commands:
                    if cmd["command_id"] in self.processed_command_ids:
                        continue
                    self._send_influx_command(cmd)
            except Exception as exc:
                self.log.error(f"[INFLUX_BRIDGE] query/send error: {exc}", exc_info=True)
            time.sleep(self.poll_commands_s)

    def _send_influx_command(self, cmd: dict) -> None:
        command_id = cmd["command_id"]
        target = cmd["target"]
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

        topic = TOPIC_DT_CMD_PUMP if target == "pump" else TOPIC_DT_CMD_LIGHT
        payload = {
            "id": command_id,
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
        else:
            self.influx.write_command_event(command_id, target, "ERROR", f"mqtt publish rc={result.rc}")

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

    parser = argparse.ArgumentParser(description="BBB Gateway + InfluxDB Digital Twin Bridge")
    parser.add_argument("--broker", default=MQTT_BROKER)
    parser.add_argument("--port", type=int, default=MQTT_PORT)
    parser.add_argument("--model", default=str(MODEL_PATH))
    parser.add_argument("--features", default=str(FEATURES_PATH))
    parser.add_argument("--config", default=str(CONFIG_PATH))
    parser.add_argument("--poll-commands-s", type=float, default=1.0)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--insert-test", choices=["pump_on", "pump_off", "light_on", "light_off"])

    args = parser.parse_args()

    if args.insert_test:
        log = setup_logging(args.debug)
        influx = InfluxManager(log)
        mapping = {
            "pump_on": ("pump", "ON", 10),
            "pump_off": ("pump", "OFF", 0),
            "light_on": ("light", "ON", 300),
            "light_off": ("light", "OFF", 0),
        }
        target, state, duration = mapping[args.insert_test]
        command_id = influx.insert_test_command(target, state, duration_s=duration, reason="cli_test")
        print(f"Inserted InfluxDB command_id={command_id}: {target} {state} duration={duration}")
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

