/*
 * Plant Monitoring CPS Web UI
 * Version: v1.3.5-int32-safe-epoch-seq-ui
 *
 * Web chạy trước:
 * - GET /api/realtime/latest để hiện dữ liệu tức thời từ RAM cache.
 * - WebSocket /ws/realtime để nhận sensor/status/ACK realtime.
 * - History InfluxDB tải nền, không chặn UI.
 * - Pump/Light command gửi về Backend command controller, không spam thẳng ESP32.
 */

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import "./App.css";

const APP_VERSION = "1.3.5-int32-safe-epoch-seq-ui";

function makeEndpoints() {
  const protocol = window.location.protocol === "https:" ? "https:" : "http:";
  const wsProtocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const host = window.location.hostname || "127.0.0.1";
  return {
    apiBase: `${protocol}//${host}:8000`,
    wsUrl: `${wsProtocol}//${host}:8000/ws/realtime`,
    host,
  };
}

function fmt(value, digits = 1, fallback = "--") {
  if (value === null || value === undefined || value === "") return fallback;
  const n = Number(value);
  if (Number.isFinite(n)) return n.toFixed(digits);
  return String(value);
}

function fmtBool(value) {
  if (value === true || value === "true" || value === "TRUE") return "YES";
  if (value === false || value === "false" || value === "FALSE") return "NO";
  return "--";
}

function fmtTime(value) {
  if (!value) return "--";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toLocaleString();
}

function stateBadgeClass(state) {
  const s = String(state || "").toUpperCase();
  if (s === "ON" || s === "DONE" || s === "OK") return "badge good";
  if (s === "OFF") return "badge off";
  if (s === "ERROR" || s === "TIMEOUT") return "badge bad";
  if (s === "SENT" || s === "RETRY" || s === "QUEUED") return "badge warn";
  return "badge";
}

function MiniLineChart({ data, yKey, label, suffix = "" }) {
  const points = useMemo(() => {
    const rows = (data || []).filter((r) => Number.isFinite(Number(r[yKey]))).slice(-120);
    if (rows.length < 2) return [];
    const values = rows.map((r) => Number(r[yKey]));
    const min = Math.min(...values);
    const max = Math.max(...values);
    const span = max - min || 1;
    return values.map((v, i) => {
      const x = (i / Math.max(1, values.length - 1)) * 100;
      const y = 100 - ((v - min) / span) * 100;
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    });
  }, [data, yKey]);

  const latest = data?.length ? data[data.length - 1]?.[yKey] : null;

  return (
    <div className="chart-card">
      <div className="chart-title">
        <span>{label}</span>
        <strong>{fmt(latest, 1)}{latest !== null && latest !== undefined ? suffix : ""}</strong>
      </div>
      <svg viewBox="0 0 100 100" preserveAspectRatio="none" className="mini-chart">
        <polyline points={points.join(" ")} />
      </svg>
    </div>
  );
}

function Card({ title, children }) {
  return (
    <section className="card">
      <h2>{title}</h2>
      {children}
    </section>
  );
}

function Metric({ label, value, unit = "" }) {
  return (
    <div className="metric">
      <div className="metric-label">{label}</div>
      <div className="metric-value">{value}{unit}</div>
    </div>
  );
}

export default function App() {
  const endpoints = useMemo(makeEndpoints, []);
  const [health, setHealth] = useState(null);
  const [latest, setLatest] = useState({});
  const [history, setHistory] = useState([]);
  const [events, setEvents] = useState([]);
  const [wsState, setWsState] = useState("CONNECTING");
  const [apiError, setApiError] = useState("");
  const [commandBusy, setCommandBusy] = useState(false);
  const [lastCommand, setLastCommand] = useState(null);
  const [pumpDuration, setPumpDuration] = useState(10);
  const [lightDuration, setLightDuration] = useState(300);
  const chartAppendRef = useRef(0);
  const wsRef = useRef(null);

  const sensors = latest?.sensors || null;
  const actuator = latest?.actuator || null;
  const status = latest?.status || null;
  const commandEvent = latest?.command_event || lastCommand || null;

  const pushEvent = useCallback((event) => {
    setEvents((prev) => [event, ...prev].slice(0, 40));
  }, []);

  const applyLatestPatch = useCallback((patch) => {
    if (!patch || typeof patch !== "object") return;
    setLatest((prev) => ({ ...prev, ...patch }));
  }, []);

  const fetchJson = useCallback(async (path, options = {}) => {
    const res = await fetch(`${endpoints.apiBase}${path}`, {
      headers: { "Content-Type": "application/json", ...(options.headers || {}) },
      ...options,
    });
    const text = await res.text();
    let body = null;
    try { body = text ? JSON.parse(text) : null; } catch { body = text; }
    if (!res.ok) {
      const detail = body?.detail || body || res.statusText;
      throw new Error(`${res.status} ${detail}`);
    }
    return body;
  }, [endpoints.apiBase]);

  const loadFastLatest = useCallback(async () => {
    try {
      const data = await fetchJson("/api/realtime/latest");
      setLatest(data.latest || {});
      setApiError("");
    } catch (err) {
      setApiError(`Latest error: ${err.message}`);
    }
  }, [fetchJson]);

  const loadHealth = useCallback(async () => {
    try {
      const data = await fetchJson("/api/health");
      setHealth(data);
    } catch (err) {
      setApiError(`Health error: ${err.message}`);
    }
  }, [fetchJson]);

  const loadHistory = useCallback(async () => {
    try {
      const data = await fetchJson("/api/history/sensors?minutes=30&limit=120");
      setHistory(data.data || []);
    } catch (err) {
      // History chậm/lỗi không được làm chết realtime UI.
      console.warn("history load failed", err);
    }
  }, [fetchJson]);

  useEffect(() => {
    loadHealth();
    loadFastLatest();
    const historyTimer = setTimeout(loadHistory, 1500);
    const latestTimer = setInterval(loadFastLatest, 30000);
    const historyInterval = setInterval(loadHistory, 5 * 60 * 1000);
    return () => {
      clearTimeout(historyTimer);
      clearInterval(latestTimer);
      clearInterval(historyInterval);
    };
  }, [loadHealth, loadFastLatest, loadHistory]);

  useEffect(() => {
    let closed = false;
    let retryTimer = null;

    function connect() {
      if (closed) return;
      setWsState("CONNECTING");
      const ws = new WebSocket(endpoints.wsUrl);
      wsRef.current = ws;

      ws.onopen = () => {
        setWsState("OPEN");
        setApiError("");
        try { ws.send("ping"); } catch {}
      };

      ws.onmessage = (msg) => {
        try {
          const event = JSON.parse(msg.data);
          pushEvent(event);
          if (event.latest) applyLatestPatch(event.latest);
          if (event.type === "hello" && event.latest) setLatest(event.latest);
          if (event.type === "sensor" && event.latest?.sensors) {
            const now = Date.now();
            // Chart chỉ append tối đa 1 điểm/giây để tránh lag khi event dày.
            if (now - chartAppendRef.current > 1000) {
              chartAppendRef.current = now;
              setHistory((prev) => [...prev, event.latest.sensors].slice(-120));
            }
          }
          if (event.type === "command_event" || event.type === "command_sent") {
            setLastCommand(event);
          }
        } catch (err) {
          console.warn("ws parse failed", err, msg.data);
        }
      };

      ws.onerror = () => {
        setWsState("ERROR");
      };

      ws.onclose = () => {
        setWsState("CLOSED");
        if (!closed) retryTimer = setTimeout(connect, 1500);
      };
    }

    connect();
    return () => {
      closed = true;
      if (retryTimer) clearTimeout(retryTimer);
      if (wsRef.current) wsRef.current.close();
    };
  }, [endpoints.wsUrl, applyLatestPatch, pushEvent]);

  async function sendCommand(path, body) {
    if (commandBusy) return;
    setCommandBusy(true);
    try {
      const data = await fetchJson(path, { method: "POST", body: JSON.stringify(body) });
      setLastCommand({
        type: "command_event",
        command_id: data.command_id,
        target: data.target,
        status: data.status,
        message: data.message || "queued",
        data,
        timestamp: new Date().toISOString(),
      });
      setApiError("");
    } catch (err) {
      setApiError(`Command error: ${err.message}`);
    } finally {
      setTimeout(() => setCommandBusy(false), 400);
    }
  }

  return (
    <main className="page">
      <header className="hero">
        <div>
          <p className="eyebrow">Plant Monitoring CPS</p>
          <h1>Web realtime control</h1>
          <p className="sub">Web chạy trước, Unity dùng lại cùng Backend/WebSocket qua Tailscale.</p>
        </div>
        <div className="status-stack">
          <span className={wsState === "OPEN" ? "pill online" : "pill offline"}>WS {wsState}</span>
          <span className={health?.mqtt?.connected ? "pill online" : "pill warn"}>MQTT {health?.mqtt?.connected ? "ONLINE" : "CHECK"}</span>
          <span className="pill">v{APP_VERSION}</span>
        </div>
      </header>

      {apiError && <div className="alert">{apiError}</div>}

      <section className="grid two">
        <Card title="Kết nối">
          <div className="kv"><span>Frontend host</span><b>{endpoints.host}</b></div>
          <div className="kv"><span>Backend API</span><b>{endpoints.apiBase}</b></div>
          <div className="kv"><span>WebSocket</span><b>{endpoints.wsUrl}</b></div>
          <div className="kv"><span>Node</span><b>{health?.node_id || "--"}</b></div>
          <div className="kv"><span>InfluxDB</span><b>{health?.influx?.ok ? "OK" : health?.influx?.message || "--"}</b></div>
        </Card>

        <Card title="Command ACK">
          <div className="kv"><span>Command ID</span><b>{commandEvent?.command_id || "--"}</b></div>
          <div className="kv"><span>Target</span><b>{commandEvent?.target || commandEvent?.data?.target || "--"}</b></div>
          <div className="kv"><span>Status</span><b className={stateBadgeClass(commandEvent?.status)}>{commandEvent?.status || "--"}</b></div>
          <div className="kv"><span>Message</span><b>{commandEvent?.message || commandEvent?.data?.message || "--"}</b></div>
        </Card>
      </section>

      <section className="grid four">
        <Metric label="Temperature" value={fmt(sensors?.temperature)} unit=" °C" />
        <Metric label="Humidity" value={fmt(sensors?.air_humidity)} unit=" %" />
        <Metric label="Lux" value={fmt(sensors?.lux)} unit=" lx" />
        <Metric label="Soil fused" value={fmt(sensors?.soil_moisture_fused ?? sensors?.soil_moisture)} unit=" %" />
      </section>

      <section className="grid two">
        <Card title="Actuator state thật từ ESP32">
          <div className="actuator-row">
            <div>
              <div className="actuator-name">Pump</div>
              <span className={stateBadgeClass(actuator?.pump_state || sensors?.pump_state)}>{actuator?.pump_state || sensors?.pump_state || "--"}</span>
              <p>{actuator?.pump_mode || sensors?.pump_mode || "--"} / {actuator?.pump_reason || sensors?.pump_reason || "--"}</p>
            </div>
            <div>
              <div className="actuator-name">Light</div>
              <span className={stateBadgeClass(actuator?.light_state || sensors?.light_state)}>{actuator?.light_state || sensors?.light_state || "--"}</span>
              <p>{actuator?.light_mode || sensors?.light_mode || "--"} / {actuator?.light_reason || sensors?.light_reason || "--"}</p>
            </div>
          </div>
        </Card>

        <Card title="Planting start">
          <div className="kv"><span>Valid</span><b>{fmtBool(sensors?.planting_start_valid ?? status?.planting_start_valid)}</b></div>
          <div className="kv"><span>Epoch</span><b>{sensors?.planting_start_epoch || status?.planting_start_epoch || "--"}</b></div>
          <div className="kv"><span>Days</span><b>{fmt(sensors?.days_after_planting, 2)}</b></div>
          <div className="kv"><span>Phase</span><b>{sensors?.phase ?? "--"} / {sensors?.phase_source || "--"}</b></div>
        </Card>
      </section>

      <Card title="Điều khiển Web-first">
        <div className="controls">
          <div className="control-block">
            <h3>Pump direct</h3>
            <label>Duration ON (s)</label>
            <input type="number" min="1" max="15" value={pumpDuration} onChange={(e) => setPumpDuration(Number(e.target.value))} />
            <div className="button-row">
              <button disabled={commandBusy} onClick={() => sendCommand("/api/command/pump", { state: "ON", duration_s: pumpDuration, source: "web", reason: "web_pump_on" })}>Pump ON</button>
              <button disabled={commandBusy} className="secondary" onClick={() => sendCommand("/api/command/pump", { state: "OFF", duration_s: 0, source: "web", reason: "web_pump_off" })}>Pump OFF</button>
            </div>
          </div>

          <div className="control-block">
            <h3>Light direct</h3>
            <label>Duration ON (s)</label>
            <input type="number" min="1" max="1800" value={lightDuration} onChange={(e) => setLightDuration(Number(e.target.value))} />
            <div className="button-row">
              <button disabled={commandBusy} onClick={() => sendCommand("/api/command/light", { state: "ON", duration_s: lightDuration, source: "web", reason: "web_light_on" })}>Light ON</button>
              <button disabled={commandBusy} className="secondary" onClick={() => sendCommand("/api/command/light", { state: "OFF", duration_s: 0, source: "web", reason: "web_light_off" })}>Light OFF</button>
            </div>
          </div>

          <div className="control-block">
            <h3>Planting start</h3>
            <p className="hint">SET_NOW sẽ được Backend đổi thành SET_EPOCH cố định rồi gửi retained QoS1.</p>
            <div className="button-row">
              <button disabled={commandBusy} onClick={() => sendCommand("/api/command/planting-start", { action: "SET_NOW", source: "web", reason: "web_start_now" })}>Start now</button>
              <button disabled={commandBusy} className="secondary" onClick={() => sendCommand("/api/command/planting-start", { action: "GET", source: "web", reason: "web_get_start" })}>Get</button>
              <button disabled={commandBusy} className="danger" onClick={() => sendCommand("/api/command/planting-start", { action: "CLEAR", source: "web", reason: "web_clear_start" })}>Clear</button>
            </div>
          </div>
        </div>
      </Card>

      <section className="grid three">
        <MiniLineChart data={history} yKey="temperature" label="Temperature" suffix="°C" />
        <MiniLineChart data={history} yKey="soil_moisture_fused" label="Soil fused" suffix="%" />
        <MiniLineChart data={history} yKey="lux" label="Lux" suffix=" lx" />
      </section>

      <Card title="Realtime events">
        <div className="event-list">
          {events.map((ev, idx) => (
            <div className="event" key={`${ev.timestamp || ev.received_at || idx}-${idx}`}>
              <span className="event-type">{ev.type}</span>
              <span>{fmtTime(ev.timestamp || ev.received_at)}</span>
              <span>{ev.command_id || ev.target || ev.topic || ""}</span>
              <span>{ev.status || ""}</span>
            </div>
          ))}
          {!events.length && <p className="hint">Chưa có event. Kiểm tra Gateway và WebSocket.</p>}
        </div>
      </Card>
    </main>
  );
}
