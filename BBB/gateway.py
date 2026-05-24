"""
gateway.py — BBB Edge AI Gateway v2.0
======================================
Rau Cải Mầm (Brassica juncea) | BRASSICA_JUNCEA_01

File duy nhất chạy trên BeagleBone Black — tích hợp đầy đủ:
  • EdgeAIWateringController (Phase 1/2 tự nhận diện qua lux)
  • MQTT subscriber (nhận sensor từ ESP32)
  • MQTT publisher  (gửi cmd/pump + cmd/light xuống ESP32)
  • InfluxDB Cloud writer
  • Alert engine theo ngưỡng Brassica juncea
  • Graceful shutdown + auto-reconnect + retry

Cài đặt:
    pip install paho-mqtt influxdb-client joblib pandas scikit-learn

Chạy thử:
    python gateway.py
    python gateway.py --debug
    python gateway.py --broker 192.168.2.15 --debug
"""

# ══════════════════════════════════════════════════════════════════════════════
# IMPORTS
# ══════════════════════════════════════════════════════════════════════════════

import json
import time
import logging
import argparse
import threading
import signal
import sys
import os
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

try:
    import joblib
    import pandas as pd
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS
except ImportError as e:
    print(f"[LỖI] Thiếu thư viện: {e}")
    print("Chạy: pip install paho-mqtt influxdb-client joblib pandas scikit-learn")
    sys.exit(1)


# ══════════════════════════════════════════════════════════════════════════════
# CẤU HÌNH — SỬA THEO MÔI TRƯỜNG THỰC TẾ
# ══════════════════════════════════════════════════════════════════════════════

NODE_ID    = "BRASSICA_JUNCEA_01"
PLANT_NAME = "Rau Cải Mầm (Brassica juncea)"
GW_VERSION = "2.1.0-phase-sync"

# ── MQTT (Mosquitto local trên BBB) ─────────────────────────────────────────
MQTT_BROKER    = "127.0.0.1"       # Mosquitto chạy ngay trên BBB
MQTT_PORT      = 1883
MQTT_CLIENT_ID = "bbb_gateway_brassica_01"
MQTT_KEEPALIVE = 60
MQTT_QOS       = 1

TOPIC_SENSOR    = "cps/greenhouse/sensors"
TOPIC_CMD_PUMP  = "cps/greenhouse/cmd/pump"
TOPIC_CMD_LIGHT = "cps/greenhouse/cmd/light"   # legacy/manual only
TOPIC_CMD_PHASE = "cps/greenhouse/cmd/phase"   # BBB -> ESP32: 1 hoặc 2
TOPIC_STATUS    = "cps/greenhouse/status"

# ── InfluxDB Cloud ───────────────────────────────────────────────────────────
INFLUX_URL    = "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUX_TOKEN  = "6pSuWQaFLlWq6iRVfaRYEMwIO1DDEChBsG42HdDx5En6fuqpUx95j3xswbVNrcWxRrs_sizN6XXESjzNqcHzJA=="
INFLUX_ORG    = "DEV_TEAM"
INFLUX_BUCKET = "digital_twin_data"

# ── Đường dẫn file ──────────────────────────────────────────────────────────
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH    = os.path.join(BASE_DIR, "watering_random_forest_model.pkl")
FEATURES_PATH = os.path.join(BASE_DIR, "model_features.json")
CONFIG_PATH   = os.path.join(BASE_DIR, "controller_config.json")

# ── Thông số sinh học Brassica juncea ───────────────────────────────────────
# Nguồn: NCBI PMC8073284, Johnny's Seeds microgreens guide
THRESHOLDS = {
    "temp_min":    18.0,   # °C tối thiểu Phase 2
    "temp_max":    24.0,   # °C tối đa cả 2 phase
    "hum_p1_min":  70.0,   # % RH Phase 1 nảy mầm
    "hum_p1_max":  85.0,
    "hum_p2_min":  50.0,   # % RH Phase 2 sinh trưởng
    "hum_p2_max":  65.0,
    "soil_min":    55.0,   # % đất khô — cần tưới
    "soil_max":    80.0,   # % đất ướt — nguy cơ úng rễ
    "lux_leak":     5.0,   # lux Phase 1 lọt sáng
    "lux_p2_min": 150.0,   # lux đèn Phase 2 tối thiểu
}


# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════

def setup_logging(debug: bool = False) -> logging.Logger:
    level   = logging.DEBUG if debug else logging.INFO
    fmt     = "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    handlers = [logging.StreamHandler(sys.stdout)]
    try:
        handlers.append(logging.FileHandler(
            os.path.join(BASE_DIR, "gateway.log"), encoding="utf-8"))
    except Exception:
        pass

    logging.basicConfig(level=level, format=fmt,
                        datefmt=datefmt, handlers=handlers)
    return logging.getLogger("GATEWAY")


# ══════════════════════════════════════════════════════════════════════════════
# EDGE AI WATERING CONTROLLER
# ══════════════════════════════════════════════════════════════════════════════

LUX_PHASE2_THRESHOLD = 50


class EdgeAIWateringController:
    """
    Unified Phase-Aware Controller.
    - phase=0 (auto): tự nhận diện qua lux >= lux_phase2_threshold
    - phase=1 (force): Phase 1 hộp kín
    - phase=2 (force): Phase 2 có đèn
    """

    def __init__(
        self,
        model,
        features,
        phase                   = 0,
        soil_force_on           = 25,
        soil_force_off          = 55,
        dry_soil_threshold_phase1 = 35,
        dry_soil_threshold_phase2 = 40,
        min_on_steps            = 1,
        max_on_steps_normal     = 3,
        max_on_steps_dry_phase1 = 6,
        max_on_steps_dry_phase2 = 5,
        cooldown_steps          = 2,
        lux_phase2_threshold    = LUX_PHASE2_THRESHOLD,
        light_schedule_phase2   = None,
        **kwargs   # bỏ qua _meta và các key không dùng
    ):
        self.model    = model
        self.features = features

        self._phase_override      = phase
        self.soil_force_on        = soil_force_on
        self.soil_force_off       = soil_force_off
        self.dry_soil_threshold   = {
            1: dry_soil_threshold_phase1,
            2: dry_soil_threshold_phase2,
        }
        self.max_on_steps_dry     = {
            1: max_on_steps_dry_phase1,
            2: max_on_steps_dry_phase2,
        }
        self.min_on_steps         = min_on_steps
        self.max_on_steps_normal  = max_on_steps_normal
        self.cooldown_steps       = cooldown_steps
        self.lux_phase2_threshold = lux_phase2_threshold
        self.light_schedule       = light_schedule_phase2 or {}

        # State machine
        self.pump_state       = "OFF"
        self.pump_on_counter  = 0
        self.cooldown_counter = 0
        self._last_soil       = None

    # ── Phase detection ───────────────────────────────────────────────────────

    def detect_phase(self, lux: float) -> int:
        if self._phase_override in (1, 2):
            return self._phase_override
        return 2 if (lux or 0) >= self.lux_phase2_threshold else 1

    # ── Validation ────────────────────────────────────────────────────────────

    def validate(self, temperature, air_humidity, lux, soil_moisture) -> list:
        errors = []
        if temperature is None or not (0 <= temperature <= 60):
            errors.append("temperature_out_of_range")
        if air_humidity is None or not (0 <= air_humidity <= 100):
            errors.append("air_humidity_out_of_range")
        if soil_moisture is None or not (0 <= soil_moisture <= 100):
            errors.append("soil_moisture_out_of_range")
        max_lux = 5000 if self.detect_phase(lux or 0) == 1 else 120000
        if lux is None or not (0 <= lux <= max_lux):
            errors.append("lux_out_of_range")
        return errors

    # ── AI Prediction ─────────────────────────────────────────────────────────

    def predict(self, temperature, air_humidity, lux, soil_moisture, phase) -> dict:
        lag1  = self._last_soil if self._last_soil is not None else soil_moisture
        delta = soil_moisture - lag1

        row = {
            "temperature":         float(temperature),
            "air_humidity":        float(air_humidity),
            "lux":                 float(lux),
            "soil_moisture":       float(soil_moisture),
            "phase":               int(phase),
            "soil_moisture_lag1":  float(lag1),
            "soil_moisture_delta": float(delta),
        }

        df = pd.DataFrame([row])

        # Điền feature thiếu một cách an toàn.
        # QUAN TRỌNG: không được fallback phase = soil_moisture.
        for f in self.features:
            if f not in df.columns:
                if f == "phase":
                    df[f] = int(phase)
                elif f == "soil_moisture_lag1":
                    df[f] = float(lag1)
                elif f == "soil_moisture_delta":
                    df[f] = float(delta)
                else:
                    df[f] = 0.0

        df = df[self.features]

        pred  = int(self.model.predict(df)[0])
        proba = self.model.predict_proba(df)[0]
        pmap  = {int(c): float(p)
                 for c, p in zip(self.model.classes_, proba)}

        return {
            "need_watering":      pred,
            "confidence":         max(pmap.get(0, 0.0), pmap.get(1, 0.0)),
            "prob_no_watering":   pmap.get(0, 0.0),
            "prob_need_watering": pmap.get(1, 0.0),
            "action": "PUMP_ON" if pred == 1 else "PUMP_OFF",
        }

    # ── Decision ─────────────────────────────────────────────────────────────

    def decide(self, temperature, air_humidity, lux, soil_moisture) -> dict:
        errors = self.validate(temperature, air_humidity, lux, soil_moisture)
        if errors:
            return {
                "status": "ERROR", "source": "validation",
                "errors": errors,  "need_watering": None,
                "confidence": 0.0, "action": "NO_ACTION",
                "reason": "invalid_sensor_data",
                "phase": self.detect_phase(lux or 0),
            }

        phase = self.detect_phase(lux)

        # Safety rule: đất cực khô
        if soil_moisture <= self.soil_force_on:
            return self._ok("safety_rule", 1, 1.0, "PUMP_ON",
                            "soil_moisture_very_low", phase)

        # Safety rule: đất đủ ẩm
        if soil_moisture >= self.soil_force_off:
            return self._ok("safety_rule", 0, 1.0, "PUMP_OFF",
                            "soil_moisture_enough", phase)

        # AI model
        ai     = self.predict(temperature, air_humidity, lux, soil_moisture, phase)
        reason = ("model_prediction_light_on"
                  if phase == 2 and lux >= self.lux_phase2_threshold
                  else "model_prediction")

        return {
            "status":             "OK",
            "source":             "random_forest",
            "errors":             [],
            "need_watering":      ai["need_watering"],
            "confidence":         ai["confidence"],
            "prob_no_watering":   ai["prob_no_watering"],
            "prob_need_watering": ai["prob_need_watering"],
            "action":             ai["action"],
            "reason":             reason,
            "phase":              phase,
        }

    def _ok(self, source, need, conf, action, reason, phase) -> dict:
        return {
            "status": "OK", "source": source, "errors": [],
            "need_watering": need, "confidence": conf,
            "prob_no_watering":   1.0 - float(need),
            "prob_need_watering": float(need),
            "action": action, "reason": reason, "phase": phase,
        }

    # ── State machine ─────────────────────────────────────────────────────────

    def update_pump_state(self, decision, soil_moisture) -> tuple:
        if decision["status"] == "ERROR":
            return self.pump_state, "NO_ACTION_ERROR"

        phase      = decision.get("phase", 1)
        dry_thresh = self.dry_soil_threshold[phase]
        max_dry    = self.max_on_steps_dry[phase]

        if self.pump_state == "ON":
            self.pump_on_counter += 1

            if soil_moisture >= self.soil_force_off:
                self._reset_pump(self.cooldown_steps)
                return self.pump_state, "TURN_OFF_SOIL_OK"

            if soil_moisture < dry_thresh:
                if self.pump_on_counter >= max_dry:
                    self._reset_pump(1)
                    return self.pump_state, "TURN_OFF_DRY_MAX_RUNTIME"
                return self.pump_state, "KEEP_ON_DRY_SOIL"

            if self.pump_on_counter >= self.max_on_steps_normal:
                self._reset_pump(self.cooldown_steps)
                return self.pump_state, "TURN_OFF_MAX_RUNTIME"

            return self.pump_state, "KEEP_ON_SOIL_NOT_ENOUGH"

        # pump OFF
        if soil_moisture < dry_thresh and decision["action"] == "PUMP_ON":
            self._turn_on()
            return self.pump_state, "TURN_ON_DRY_SOIL"

        if self.cooldown_counter > 0:
            self.cooldown_counter -= 1
            return self.pump_state, "KEEP_OFF_COOLDOWN"

        if (decision["action"] == "PUMP_ON"
                and soil_moisture < self.soil_force_off):
            self._turn_on()
            return self.pump_state, "TURN_ON_AI_REQUEST"

        return self.pump_state, "KEEP_OFF"

    def _turn_on(self):
        self.pump_state       = "ON"
        self.pump_on_counter  = 1
        self.cooldown_counter = 0

    def _reset_pump(self, cooldown):
        self.pump_state       = "OFF"
        self.pump_on_counter  = 0
        self.cooldown_counter = cooldown

    # ── Build payload ─────────────────────────────────────────────────────────

    def create_payload(
        self, temperature, air_humidity, lux, soil_moisture,
        step=None, node_id=NODE_ID,
        light_state="OFF", light_mode="AUTO_RTC", light_reason=None
    ) -> dict:
        decision = self.decide(temperature, air_humidity, lux, soil_moisture)
        pump_state, ctrl_reason = self.update_pump_state(decision, soil_moisture)
        self._last_soil = float(soil_moisture)
        phase = decision.get("phase", self.detect_phase(lux))

        return {
            "node_id":   node_id,
            "plant":     PLANT_NAME,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "step":      step,
            "phase":     phase,
            "sensor": {
                "temperature":   float(temperature),
                "air_humidity":  float(air_humidity),
                "lux":           float(lux),
                "soil_moisture": float(soil_moisture),
            },
            "ai": {
                "status":             decision["status"],
                "source":             decision["source"],
                "need_watering":      decision["need_watering"],
                "confidence":         round(float(decision["confidence"]), 4),
                "prob_no_watering":   round(float(decision.get("prob_no_watering",  0)), 4),
                "prob_need_watering": round(float(decision.get("prob_need_watering", 0)), 4),
                "action":             decision["action"],
                "reason":             decision["reason"],
                "errors":             decision.get("errors", []),
            },
            "control": {
                "pump": {
                    "state":  pump_state,
                    "mode":   "AI_AUTO",
                    "reason": ctrl_reason,
                },
                "light": {
                    "state":  light_state,
                    "mode":   light_mode,
                    "reason": light_reason or ("RTC_SCHEDULE" if light_mode == "AUTO_RTC" else "USER_OVERRIDE"),
                },
            },
        }

    def __repr__(self):
        return (f"EdgeAIWateringController("
                f"phase_override={self._phase_override}, "
                f"pump={self.pump_state}, "
                f"features={self.features})")


def load_controller(model_path, features_path,
                    config_path=None) -> EdgeAIWateringController:
    model = joblib.load(model_path)
    with open(features_path, "r", encoding="utf-8") as f:
        features = json.load(f)
    config = {}
    if config_path and os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        skip = {"_meta", "_comment", "_version", "_node"}
        config = {k: v for k, v in raw.items() if k not in skip}
    return EdgeAIWateringController(model=model, features=features, **config)


# ══════════════════════════════════════════════════════════════════════════════
# ALERT ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def build_alerts(temperature, air_humidity, lux,
                 soil_moisture, phase) -> list:
    T      = THRESHOLDS
    alerts = []

    if temperature > T["temp_max"]:
        alerts.append(
            f"Nhiệt độ cao {temperature:.1f}°C (max {T['temp_max']}°C)")
    elif temperature < T["temp_min"] and phase == 2:
        alerts.append(
            f"Nhiệt độ thấp {temperature:.1f}°C (min {T['temp_min']}°C)")

    hmin = T["hum_p1_min"] if phase == 1 else T["hum_p2_min"]
    hmax = T["hum_p1_max"] if phase == 1 else T["hum_p2_max"]
    if air_humidity < hmin:
        alerts.append(
            f"Độ ẩm KK thấp {air_humidity:.1f}% (min {hmin:.0f}%)")
    elif air_humidity > hmax:
        alerts.append(
            f"Độ ẩm KK cao {air_humidity:.1f}% — nguy cơ nấm (max {hmax:.0f}%)")

    if phase == 1 and lux > T["lux_leak"]:
        alerts.append(
            f"LỌT SÁNG Phase 1: {lux:.1f} lux (ngưỡng {T['lux_leak']:.0f})")
    elif phase == 2 and lux < T["lux_p2_min"]:
        alerts.append(
            f"Đèn yếu Phase 2: {lux:.1f} lux (cần >= {T['lux_p2_min']:.0f})")

    if soil_moisture < T["soil_min"]:
        alerts.append(
            f"Đất thiếu nước {soil_moisture:.1f}% (min {T['soil_min']:.0f}%)")
    elif soil_moisture > T["soil_max"]:
        alerts.append(
            f"Đất quá ẩm {soil_moisture:.1f}% — nguy cơ úng rễ "
            f"(max {T['soil_max']:.0f}%)")

    return alerts


# ══════════════════════════════════════════════════════════════════════════════
# SENSOR PARSER — tương thích ESP32 v1 (flat) và v2 (nested)
# ══════════════════════════════════════════════════════════════════════════════

def parse_sensor(raw: dict) -> dict:
    """
    ESP32 v1 (flat) : {"node":"..","temp":28.0,"hum":70.0,"s1":45.0,"lux":5.0}
    ESP32 v2 (nested): {"sensor":{"temperature":28.0,"air_humidity":70.0,
                         "soil_moisture_avg":45.0,"lux":5.0,
                         "soil_moisture_raw":{"s1":..,"s2":..,"s3":..,"s4":..}}}
    """
    s = raw.get("sensor", raw)

    temperature  = float(s.get("temperature",  s.get("temp", 0)))
    air_humidity = float(s.get("air_humidity", s.get("hum",  0)))
    lux          = float(s.get("lux", 0))

    if "soil_moisture_avg" in s:
        soil_avg = float(s["soil_moisture_avg"])
    elif "soil_moisture" in s:
        soil_avg = float(s["soil_moisture"])
    else:
        vals = [s[k] for k in ("s1","s2","s3","s4") if s.get(k) is not None]
        soil_avg = sum(vals) / len(vals) if vals else 0.0

    raw_soil = s.get("soil_moisture_raw", {})
    s1 = float(raw_soil.get("s1", s.get("s1", soil_avg)))
    s2 = float(raw_soil.get("s2", s.get("s2", soil_avg)))
    s3 = float(raw_soil.get("s3", s.get("s3", soil_avg)))
    s4 = float(raw_soil.get("s4", s.get("s4", soil_avg)))

    status      = raw.get("status", {})
    light_on    = status.get("light_on", False)
    light_state = "ON" if light_on else "OFF"
    light_mode  = status.get("light_mode", "AUTO_RTC")
    light_reason = status.get("light_reason", None)

    # ESP32 phase feedback sau khi nhận cmd/phase từ BBB.
    # Nếu không có hoặc sai, gateway vẫn tự tính phase bằng controller.
    try:
        esp_phase = int(raw.get("phase")) if raw.get("phase") is not None else None
        if esp_phase not in (1, 2):
            esp_phase = None
    except Exception:
        esp_phase = None

    return {
        "temperature":  temperature,
        "air_humidity": air_humidity,
        "lux":          lux,
        "soil_avg":     soil_avg,
        "soil_s1": s1, "soil_s2": s2, "soil_s3": s3, "soil_s4": s4,
        "esp_phase":    esp_phase,
        "light_state":  light_state,
        "light_mode":   light_mode,
        "light_reason": light_reason,
        "uptime_s":     int(raw.get("uptime_s", 0)),
        "wifi_rssi":    int(status.get("wifi_rssi", 0)),
    }


# ══════════════════════════════════════════════════════════════════════════════
# INFLUXDB WRITER
# ══════════════════════════════════════════════════════════════════════════════

def init_influx(log) -> object:
    try:
        client    = InfluxDBClient(url=INFLUX_URL,
                                   token=INFLUX_TOKEN, org=INFLUX_ORG)
        write_api = client.write_api(write_options=SYNCHRONOUS)
        client.ping()
        log.info(f"✅ InfluxDB: {INFLUX_URL}")
        return write_api
    except Exception as e:
        log.warning(f"⚠️  InfluxDB chưa kết nối ({e}) — retry khi ghi")
        return None


def push_influx(write_api, payload: dict, log):
    if write_api is None:
        return
    try:
        s    = payload["sensor"]
        ai   = payload["ai"]
        ctrl = payload["control"]

        point = (
            Point("sensor_data")
            .tag("node_id",    payload["node_id"])
            .tag("plant",      PLANT_NAME)
            .tag("phase",      str(payload.get("phase", 1)))
            .tag("ai_source",  ai.get("source", "unknown"))
            .tag("pump_state", ctrl["pump"]["state"])
            # Sensor
            .field("temperature",    float(s.get("temperature",   0)))
            .field("air_humidity",   float(s.get("air_humidity",  0)))
            .field("lux",            float(s.get("lux",           0)))
            .field("soil_moisture",  float(s.get("soil_moisture", 0)))
            .field("soil_s1",        float(s.get("soil_s1", s.get("soil_moisture", 0))))
            .field("soil_s2",        float(s.get("soil_s2", s.get("soil_moisture", 0))))
            .field("soil_s3",        float(s.get("soil_s3", s.get("soil_moisture", 0))))
            .field("soil_s4",        float(s.get("soil_s4", s.get("soil_moisture", 0))))
            # AI
            .field("need_watering",      int(ai.get("need_watering") or 0))
            .field("ai_confidence",      float(ai.get("confidence",          0)))
            .field("prob_need_watering", float(ai.get("prob_need_watering",  0)))
            # Control
            .field("pump",  1 if ctrl["pump"]["state"]  == "ON" else 0)
            .field("light", 1 if ctrl["light"]["state"] == "ON" else 0)
            # Meta
            .field("step",      int(payload.get("step") or 0))
            .field("uptime_s",  int(payload.get("uptime_s",  0)))
            .field("wifi_rssi", int(payload.get("wifi_rssi", 0)))
            .time(datetime.now(timezone.utc), WritePrecision.S)
        )
        write_api.write(bucket=INFLUX_BUCKET, record=point)
        log.debug(f"[InfluxDB] ✓ step={payload.get('step')}")

    except Exception as e:
        log.error(f"[InfluxDB] ✗ Lỗi ghi: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# CORE PROCESSOR
# ══════════════════════════════════════════════════════════════════════════════

_step_counter = 0
_step_lock    = threading.Lock()


def process(mqtt_client, controller, write_api, raw: dict, log):
    global _step_counter

    with _step_lock:
        _step_counter += 1
        step = _step_counter

    # ── Parse sensor ─────────────────────────────────────────────────────────
    try:
        s = parse_sensor(raw)
    except Exception as e:
        log.error(f"[PARSE] Lỗi: {e} | raw={str(raw)[:200]}")
        return

    temperature  = s["temperature"]
    air_humidity = s["air_humidity"]
    lux          = s["lux"]
    soil_avg     = s["soil_avg"]

    # ── AI decision ───────────────────────────────────────────────────────────
    try:
        payload = controller.create_payload(
            temperature   = temperature,
            air_humidity  = air_humidity,
            lux           = lux,
            soil_moisture = soil_avg,
            step          = step,
            node_id       = NODE_ID,
            light_state   = s["light_state"],
            light_mode    = s["light_mode"],
            light_reason  = s.get("light_reason"),
        )
    except Exception as e:
        log.error(f"[AI] Lỗi controller: {e}", exc_info=True)
        return

    phase       = payload["phase"]
    esp_phase   = s.get("esp_phase")
    pump_state  = payload["control"]["pump"]["state"]
    pump_reason = payload["control"]["pump"]["reason"]
    ai_source   = payload["ai"]["source"]
    confidence  = payload["ai"]["confidence"]

    # ── Alerts ────────────────────────────────────────────────────────────────
    alerts = build_alerts(temperature, air_humidity,
                          lux, soil_avg, phase)
    payload["alert"]     = "; ".join(alerts) if alerts else None
    payload["uptime_s"]  = s["uptime_s"]
    payload["wifi_rssi"] = s["wifi_rssi"]

    # Bổ sung soil channels cho InfluxDB
    payload["sensor"].update({
        "soil_s1": s["soil_s1"], "soil_s2": s["soil_s2"],
        "soil_s3": s["soil_s3"], "soil_s4": s["soil_s4"],
    })

    # ── Log ───────────────────────────────────────────────────────────────────
    log.info("─" * 62)
    log.info(f"  Step {step:4d} | Phase {phase} | "
             f"{payload['timestamp'][:19]}")
    log.info(f"  T={temperature:.1f}°C  RH={air_humidity:.1f}%  "
             f"Lux={lux:.1f}  Soil={soil_avg:.1f}%")
    log.info(f"  AI [{ai_source}] → Pump {pump_state} "
             f"({pump_reason}) conf={confidence:.0%}")
    if esp_phase is not None and esp_phase != phase:
        log.warning(f"  ⚠️  ESP32 phase={esp_phase} khác BBB phase={phase}; sẽ đồng bộ lại qua cmd/phase")
    for alert in alerts:
        log.warning(f"  ⚠️  {alert}")

    # ── Publish cmd/pump xuống ESP32 ──────────────────────────────────────────
    ret = mqtt_client.publish(TOPIC_CMD_PUMP, pump_state, qos=MQTT_QOS)
    if ret.rc == mqtt.MQTT_ERR_SUCCESS:
        log.info(f"  → {TOPIC_CMD_PUMP}: {pump_state}")
    else:
        log.error(f"  → cmd/pump FAILED rc={ret.rc}")

    # ── Publish cmd/phase xuống ESP32 ─────────────────────────────────────────
    # ESP32 dùng phase + RTC DS3231 để tự bật/tắt đèn.
    # Không publish cmd/light trong luồng AUTO để tránh BBB và ESP32 tranh quyền đèn.
    ret = mqtt_client.publish(TOPIC_CMD_PHASE, str(phase), qos=MQTT_QOS, retain=True)
    if ret.rc == mqtt.MQTT_ERR_SUCCESS:
        log.info(f"  → {TOPIC_CMD_PHASE}: {phase} (retain)")
    else:
        log.error(f"  → cmd/phase FAILED rc={ret.rc}")

    # ── Ghi InfluxDB (thread riêng, không block MQTT loop) ───────────────────
    threading.Thread(
        target=push_influx,
        args=(write_api, payload, log),
        daemon=True
    ).start()


# ══════════════════════════════════════════════════════════════════════════════
# MQTT CALLBACKS
# ══════════════════════════════════════════════════════════════════════════════

def make_callbacks(controller, write_api, log):

    def on_connect(client, userdata, flags, rc):
        codes = {0:"OK", 1:"Protocol", 2:"Client ID",
                 3:"Unavailable", 4:"Credentials", 5:"Unauthorized"}
        if rc == 0:
            log.info(f"✅ MQTT connected → subscribe {TOPIC_SENSOR}; publish phase on each sensor packet")
            client.subscribe(TOPIC_SENSOR, qos=MQTT_QOS)
            client.publish(TOPIC_STATUS, json.dumps({
                "node_id":    NODE_ID,
                "gateway":    "BBB",
                "gw_version": GW_VERSION,
                "status":     "online",
                "timestamp":  datetime.now(timezone.utc).isoformat(),
            }), retain=True)
        else:
            log.error(f"❌ MQTT connect failed: {codes.get(rc, rc)}")

    def on_disconnect(client, userdata, rc):
        if rc == 0:
            log.info("MQTT disconnected (clean)")
        else:
            log.warning(f"⚠️  MQTT disconnected rc={rc} — auto-reconnect...")

    def on_message(client, userdata, msg):
        try:
            raw = json.loads(msg.payload.decode("utf-8"))
            process(client, controller, write_api, raw, log)
        except json.JSONDecodeError:
            log.error(f"[MQTT] Payload không phải JSON: {msg.payload[:100]}")
        except Exception as e:
            log.error(f"[MQTT] on_message lỗi: {e}", exc_info=True)

    def on_publish(client, userdata, mid):
        log.debug(f"[MQTT] ACK mid={mid}")

    return on_connect, on_disconnect, on_message, on_publish


# ══════════════════════════════════════════════════════════════════════════════
# GRACEFUL SHUTDOWN
# ══════════════════════════════════════════════════════════════════════════════

_client_ref = None
_log_ref    = None


def handle_signal(sig, frame):
    if _log_ref:
        _log_ref.info("⛔ Shutdown gracefully...")
    if _client_ref:
        try:
            _client_ref.publish(TOPIC_STATUS, json.dumps({
                "node_id":   NODE_ID,
                "status":    "offline",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }), retain=True)
            time.sleep(0.5)
            _client_ref.loop_stop()
            _client_ref.disconnect()
        except Exception:
            pass
    sys.exit(0)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    global _client_ref, _log_ref

    parser = argparse.ArgumentParser(
        description=f"BBB Edge AI Gateway v{GW_VERSION} — {PLANT_NAME}")
    parser.add_argument("--broker",   default=MQTT_BROKER,
                        help=f"MQTT broker IP (default: {MQTT_BROKER})")
    parser.add_argument("--port",     default=MQTT_PORT, type=int,
                        help=f"MQTT port (default: {MQTT_PORT})")
    parser.add_argument("--model",    default=MODEL_PATH,
                        help="Path model .pkl")
    parser.add_argument("--features", default=FEATURES_PATH,
                        help="Path features .json")
    parser.add_argument("--config",   default=CONFIG_PATH,
                        help="Path controller config .json")
    parser.add_argument("--debug",    action="store_true",
                        help="Enable DEBUG logging")
    args = parser.parse_args()

    log = setup_logging(args.debug)
    _log_ref = log

    log.info("═" * 62)
    log.info(f"  BBB Gateway v{GW_VERSION}")
    log.info(f"  Plant   : {PLANT_NAME}")
    log.info(f"  Node    : {NODE_ID}")
    log.info(f"  Model   : {args.model}")
    log.info(f"  Config  : {args.config}")
    log.info(f"  MQTT    : {args.broker}:{args.port} → {TOPIC_SENSOR}")
    log.info(f"  InfluxDB: {INFLUX_URL}")
    log.info("═" * 62)

    signal.signal(signal.SIGINT,  handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Load AI controller
    try:
        controller = load_controller(args.model, args.features, args.config)
        log.info(f"✅ Controller: {controller}")
    except Exception as e:
        log.critical(f"❌ Load controller thất bại: {e}")
        sys.exit(1)

    # InfluxDB
    write_api = init_influx(log)

    # MQTT client
    client = mqtt.Client(
        client_id   = MQTT_CLIENT_ID,
        clean_session = True,
        protocol    = mqtt.MQTTv311,
    )
    _client_ref = client

    on_connect, on_disconnect, on_message, on_publish = make_callbacks(
        controller, write_api, log)
    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message
    client.on_publish    = on_publish
    client.reconnect_delay_set(min_delay=2, max_delay=30)

    # Connect với retry 5 lần
    for attempt in range(1, 6):
        try:
            client.connect(args.broker, args.port, keepalive=MQTT_KEEPALIVE)
            break
        except Exception as e:
            log.warning(f"MQTT connect attempt {attempt}/5: {e}")
            if attempt == 5:
                log.critical(
                    f"❌ Không kết nối được MQTT "
                    f"{args.broker}:{args.port} sau 5 lần thử")
                sys.exit(1)
            time.sleep(3)

    log.info("🚀 Gateway running — Ctrl+C hoặc 'systemctl stop gateway' để dừng")
    client.loop_forever()


if __name__ == "__main__":
    main()