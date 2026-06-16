import { useEffect, useMemo, useState } from "react";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import "./App.css";

const API_BASE =
  import.meta.env.VITE_API_BASE ||
  `${window.location.protocol}//${window.location.hostname}:8000`;

const WS_BASE =
  import.meta.env.VITE_WS_BASE ||
  `${window.location.protocol === "https:" ? "wss" : "ws"}://${window.location.hostname}:8000`;

function formatNumber(value, digits = 1) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "N/A";
  }
  return Number(value).toFixed(digits);
}

function formatTime(value) {
  if (!value) return "N/A";
  try {
    return new Date(value).toLocaleString();
  } catch {
    return value;
  }
}

function formatShortTime(value) {
  if (!value) return "";
  try {
    return new Date(value).toLocaleTimeString();
  } catch {
    return value;
  }
}

function getPumpState(actuator, sensors) {
  return actuator?.pump_state || actuator?.pump || sensors?.pump_state || sensors?.pump || "N/A";
}

function getLightState(actuator, sensors) {
  return actuator?.light_state || actuator?.light || sensors?.light_state || sensors?.light || "N/A";
}

function getSoilMoisture(sensors) {
  return sensors?.soil_moisture ?? sensors?.soil_avg ?? sensors?.soil ?? null;
}

function StatusBadge({ value }) {
  const text = String(value ?? "N/A").toUpperCase();
  let className = "status-badge status-unknown";
  if (["ON", "1", "TRUE", "PUMP_ON", "LIGHT_ON"].includes(text)) {
    className = "status-badge status-on";
  } else if (["OFF", "0", "FALSE", "PUMP_OFF", "LIGHT_OFF"].includes(text)) {
    className = "status-badge status-off";
  }
  return <span className={className}>{text}</span>;
}

function MetricCard({ icon, title, value, unit, hint, accent }) {
  return (
    <div className={`metric-card ${accent || ""}`}>
      <div className="metric-top">
        <div className="metric-icon">{icon}</div>
        <div>
          <div className="metric-title">{title}</div>
          {hint && <div className="metric-hint">{hint}</div>}
        </div>
      </div>
      <div className="metric-value">
        {value}
        {unit && <span className="metric-unit">{unit}</span>}
      </div>
    </div>
  );
}

function InfoBox({ label, value }) {
  return (
    <div className="info-box">
      <span>{label}</span>
      <strong>{value ?? "N/A"}</strong>
    </div>
  );
}

function ChartPanel({ title, subtitle, data, lines }) {
  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <h2>{title}</h2>
          {subtitle && <p>{subtitle}</p>}
        </div>
      </div>
      <div className="chart-box">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 10, right: 20, left: 0, bottom: 8 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="display_time" minTickGap={28} />
            <YAxis />
            <Tooltip />
            <Legend />
            {lines.map((line) => (
              <Line
                key={line.dataKey}
                type="monotone"
                dataKey={line.dataKey}
                name={line.name}
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </section>
  );
}

function CommandPanel({ onCommand, commandBusy }) {
  const [pumpDuration, setPumpDuration] = useState(10);
  const [lightDuration, setLightDuration] = useState(300);
  const [epoch, setEpoch] = useState("");

  return (
    <section className="panel command-panel">
      <div className="panel-header">
        <div>
          <h2>Điều khiển Web</h2>
          <p>Web gửi lệnh tới Backend. Backend publish MQTT xuống ESP32 ngay và đồng thời ghi log vào InfluxDB.</p>
        </div>
      </div>

      <div className="command-grid">
        <div className="command-card">
          <h3>Pump direct</h3>
          <label>Duration ON, giây</label>
          <input
            type="number"
            min="1"
            max="15"
            value={pumpDuration}
            onChange={(e) => setPumpDuration(Number(e.target.value))}
          />
          <div className="button-row">
            <button disabled={commandBusy} onClick={() => onCommand("pump", "ON", pumpDuration)}>
              Pump ON
            </button>
            <button disabled={commandBusy} onClick={() => onCommand("pump", "OFF", 0)} className="secondary">
              Pump OFF
            </button>
          </div>
        </div>

        <div className="command-card">
          <h3>Light direct</h3>
          <label>Duration ON, giây</label>
          <input
            type="number"
            min="1"
            max="1800"
            value={lightDuration}
            onChange={(e) => setLightDuration(Number(e.target.value))}
          />
          <div className="button-row">
            <button disabled={commandBusy} onClick={() => onCommand("light", "ON", lightDuration)}>
              Light ON
            </button>
            <button disabled={commandBusy} onClick={() => onCommand("light", "OFF", 0)} className="secondary">
              Light OFF
            </button>
          </div>
        </div>

        <div className="command-card">
          <h3>Planting start</h3>
          <label>Epoch cho SET_EPOCH</label>
          <input
            type="number"
            placeholder="Unix epoch seconds"
            value={epoch}
            onChange={(e) => setEpoch(e.target.value)}
          />
          <div className="button-row wrap">
            <button disabled={commandBusy} onClick={() => onCommand("planting", "SET_NOW")}>SET_NOW</button>
            <button disabled={commandBusy} onClick={() => onCommand("planting", "GET")} className="secondary">GET</button>
            <button disabled={commandBusy} onClick={() => onCommand("planting", "CLEAR")} className="secondary">CLEAR</button>
            <button
              disabled={commandBusy || !epoch}
              onClick={() => onCommand("planting", "SET_EPOCH", Number(epoch))}
              className="secondary"
            >
              SET_EPOCH
            </button>
          </div>
        </div>
      </div>
    </section>
  );
}

function App() {
  const [minutes, setMinutes] = useState(720);
  const [selectedDate, setSelectedDate] = useState("");
  const [health, setHealth] = useState(null);
  const [latest, setLatest] = useState(null);
  const [sensorHistory, setSensorHistory] = useState([]);
  const [error, setError] = useState("");
  const [commandMessage, setCommandMessage] = useState("");
  const [commandBusy, setCommandBusy] = useState(false);
  const [lastUpdate, setLastUpdate] = useState("");
  const [wsStatus, setWsStatus] = useState("CONNECTING");
  const [realtimeCount, setRealtimeCount] = useState(0);

  function buildQueryString() {
    if (selectedDate) return `date=${selectedDate}`;
    return `minutes=${minutes}`;
  }


  function applyLatestPatch(patch) {
    if (!patch || typeof patch !== "object") return;
    setLatest((prev) => ({ ...(prev || {}), ...patch }));

    if (patch.sensors) {
      setSensorHistory((prev) => {
        const next = [...prev, patch.sensors];
        return next.slice(-300);
      });
    }
    setRealtimeCount((v) => v + 1);
    setLastUpdate(new Date().toLocaleString());
  }

  async function loadDashboardData() {
    try {
      setError("");
      const queryString = buildQueryString();
      const [healthResponse, latestResponse, sensorsHistoryResponse] = await Promise.all([
        fetch(`${API_BASE}/api/health`),
        fetch(`${API_BASE}/api/dashboard/latest?${queryString}`),
        fetch(`${API_BASE}/api/history/sensors?${queryString}`),
      ]);

      if (!healthResponse.ok) throw new Error(`Health API error: ${healthResponse.status}`);
      if (!latestResponse.ok) throw new Error(`Latest API error: ${latestResponse.status}`);
      if (!sensorsHistoryResponse.ok) throw new Error(`Sensors history API error: ${sensorsHistoryResponse.status}`);

      const healthData = await healthResponse.json();
      const latestData = await latestResponse.json();
      const sensorsHistoryData = await sensorsHistoryResponse.json();

      setHealth(healthData);
      setLatest(latestData);
      setSensorHistory(sensorsHistoryData.data || []);
      setLastUpdate(new Date().toLocaleString());
    } catch (err) {
      setError(err.message);
    }
  }

  async function sendCommand(type, actionOrState, durationOrEpoch) {
    try {
      setCommandBusy(true);
      setCommandMessage("");
      let url = "";
      let payload = {};

      if (type === "pump") {
        url = `${API_BASE}/api/command/pump`;
        payload = {
          state: actionOrState,
          duration_s: durationOrEpoch,
          source: "web",
          reason: actionOrState === "ON" ? "web_pump_on" : "web_pump_off",
        };
      } else if (type === "light") {
        url = `${API_BASE}/api/command/light`;
        payload = {
          state: actionOrState,
          duration_s: durationOrEpoch,
          source: "web",
          reason: actionOrState === "ON" ? "web_light_on" : "web_light_off",
        };
      } else {
        url = `${API_BASE}/api/command/planting-start`;
        payload = {
          action: actionOrState,
          source: "web",
          reason: `web_${String(actionOrState).toLowerCase()}`,
        };
        if (actionOrState === "SET_EPOCH") {
          payload.planting_start_epoch = durationOrEpoch;
        }
      }

      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.detail || `Command API error: ${response.status}`);
      }

      setCommandMessage(`${data.status || "SENT"} ${data.target}: ${data.command_id}`);
      await loadDashboardData();
    } catch (err) {
      setCommandMessage(`Lỗi command: ${err.message}`);
    } finally {
      setCommandBusy(false);
    }
  }

  useEffect(() => {
    let ws = null;
    let stopped = false;
    let reconnectTimer = null;

    function connectWs() {
      if (stopped) return;
      setWsStatus("CONNECTING");
      ws = new WebSocket(`${WS_BASE}/ws/realtime`);

      ws.onopen = () => setWsStatus("ONLINE");
      ws.onclose = () => {
        setWsStatus("OFFLINE");
        if (!stopped) reconnectTimer = setTimeout(connectWs, 2000);
      };
      ws.onerror = () => setWsStatus("ERROR");
      ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          if (msg.latest) applyLatestPatch(msg.latest);
          if (msg.type === "planting_start_ack" || msg.type === "command_sent") {
            const cid = msg.command_id || msg.data?.command_id || "";
            const st = msg.status || msg.data?.status || msg.latest?.command_event?.status || "";
            if (cid) setCommandMessage(`${msg.type}: ${cid} ${st}`);
          }
        } catch (err) {
          console.warn("WebSocket parse error", err);
        }
      };
    }

    connectWs();
    return () => {
      stopped = true;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      if (ws) ws.close();
    };
  }, []);

  useEffect(() => {
    loadDashboardData();
    // WebSocket là realtime chính; polling chỉ là fallback nhẹ để khôi phục khi WS rớt.
    const timer = setInterval(loadDashboardData, 30000);
    return () => clearInterval(timer);
  }, [minutes, selectedDate]);

  const sensors = latest?.sensors;
  const actuator = latest?.actuator;
  const status = latest?.status;
  const commandEvent = latest?.command_event;
  const dtCommand = latest?.dt_command;

  const soilMoisture = getSoilMoisture(sensors);
  const pumpState = getPumpState(actuator, sensors);
  const lightState = getLightState(actuator, sensors);

  const chartData = useMemo(() => {
    return sensorHistory.map((row) => ({
      ...row,
      display_time: formatShortTime(row._time),
      soil_moisture_display: row.soil_moisture ?? row.soil_avg ?? row.soil ?? null,
    }));
  }, [sensorHistory]);

  const hasRealData = Boolean(sensors);

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">CPS</div>
          <div>
            <h1>Plant Care</h1>
            <p>Raspberry Pi Gateway</p>
          </div>
        </div>

        <div className="sidebar-section">
          <span className="sidebar-label">API</span>
          <div className="sidebar-value">{API_BASE}</div>
        </div>

        <div className="sidebar-section">
          <span className="sidebar-label">Bucket</span>
          <div className="sidebar-value">{health?.bucket || "N/A"}</div>
        </div>

        <div className="sidebar-section">
          <span className="sidebar-label">View mode</span>
          <select
            className="sidebar-select"
            value={selectedDate ? "date" : "recent"}
            onChange={(e) => {
              if (e.target.value === "recent") setSelectedDate("");
            }}
          >
            <option value="recent">Theo thời gian gần đây</option>
            <option value="date">Theo ngày cụ thể</option>
          </select>
        </div>

        {!selectedDate && (
          <div className="sidebar-section">
            <span className="sidebar-label">Recent range</span>
            <select
              className="sidebar-select"
              value={minutes}
              onChange={(e) => {
                setMinutes(Number(e.target.value));
                setSelectedDate("");
              }}
            >
              <option value={60}>1 giờ</option>
              <option value={180}>3 giờ</option>
              <option value={360}>6 giờ</option>
              <option value={720}>12 giờ</option>
              <option value={1440}>24 giờ</option>
            </select>
          </div>
        )}

        <div className="sidebar-section">
          <span className="sidebar-label">Select date</span>
          <input
            className="sidebar-input"
            type="date"
            value={selectedDate}
            onChange={(e) => setSelectedDate(e.target.value)}
          />
          {selectedDate && (
            <button className="clear-date-button" onClick={() => setSelectedDate("")}>Clear date</button>
          )}
        </div>

        <button className="sidebar-button" onClick={loadDashboardData}>Refresh Data</button>
        <a className="sidebar-download" href={`${API_BASE}/api/export/sensors.csv?${buildQueryString()}`}>
          Download Sensors CSV
        </a>

        <div className="sidebar-footer">
          <span className="live-dot" />
          <span>{selectedDate ? `Viewing: ${selectedDate}` : `WebSocket: ${wsStatus} • events ${realtimeCount}`}</span>
        </div>
      </aside>

      <main className="main-content">
        <header className="top-header">
          <div>
            <div className="eyebrow">ESP32 + MQTT realtime + Pi Gateway + WebSocket + InfluxDB history</div>
            <h1>Plant Monitoring CPS Dashboard</h1>
            <p>
              Dashboard lấy log cũ từ InfluxDB và nhận dữ liệu mới qua WebSocket. Các nút điều khiển gửi lệnh tới Backend để publish MQTT ngay xuống ESP32, đồng thời ghi log vào InfluxDB.
            </p>
          </div>
          <div className="header-status-card">
            <div className="header-status-label">Backend status</div>
            <div className="header-status-value">{health?.status === "OK" ? "Online" : "Checking"}</div>
          </div>
        </header>

        {error && <div className="error-box">Lỗi: {error}</div>}
        {commandMessage && <div className="command-message">{commandMessage}</div>}

        <section className="summary-grid">
          <div className="summary-card"><span>Last dashboard update</span><strong>{lastUpdate || "N/A"}</strong></div>
          <div className="summary-card"><span>Realtime WebSocket</span><strong>{wsStatus} ({realtimeCount})</strong></div>
          <div className="summary-card"><span>Latest sensor time</span><strong>{formatTime(sensors?._time)}</strong></div>
          <div className="summary-card"><span>Rows loaded</span><strong>{sensorHistory.length}</strong></div>
          <div className="summary-card"><span>View mode</span><strong>{selectedDate ? `Date: ${selectedDate}` : `Recent: ${minutes}m`}</strong></div>
        </section>

        {!hasRealData && (
          <div className="empty-box">
            Chưa có dữ liệu thật từ gateway.py trong InfluxDB. Khi ESP32 gửi dữ liệu MQTT và gateway ghi vào measurement sensors, dashboard sẽ tự hiển thị.
          </div>
        )}

        <CommandPanel onCommand={sendCommand} commandBusy={commandBusy} />

        <section className="panel">
          <div className="panel-header">
            <div>
              <h2>Dữ liệu cảm biến mới nhất</h2>
              <p>Dữ liệu realtime từ WebSocket; khi mở lại sẽ fallback/query từ InfluxDB.</p>
            </div>
          </div>
          <div className="metric-grid">
            <MetricCard icon="🌡️" title="Temperature" value={formatNumber(sensors?.temperature, 1)} unit="°C" hint="DHT11 moving average" accent="accent-temp" />
            <MetricCard icon="💧" title="Air Humidity" value={formatNumber(sensors?.air_humidity, 1)} unit="%" hint="Không khí trong hộp trồng" accent="accent-humidity" />
            <MetricCard icon="☀️" title="Light" value={formatNumber(sensors?.lux, 1)} unit="lux" hint="BH1750" accent="accent-light" />
            <MetricCard icon="🌱" title="Soil Moisture" value={formatNumber(soilMoisture, 1)} unit="%" hint="ADS1115 4 channels average" accent="accent-soil" />
          </div>
        </section>

        <section className="system-grid">
          <section className="panel">
            <div className="panel-header"><div><h2>Growth Phase</h2><p>Thông tin phase do ESP32 RTC/NVS gửi qua gateway.</p></div></div>
            <div className="info-grid">
              <InfoBox label="Phase" value={sensors?.phase} />
              <InfoBox label="Phase source" value={sensors?.phase_source} />
              <InfoBox label="Days after planting" value={formatNumber(sensors?.days_after_planting, 2)} />
              <InfoBox label="WiFi RSSI" value={sensors?.wifi_rssi} />
            </div>
          </section>

          <section className="panel">
            <div className="panel-header"><div><h2>Actuator State</h2><p>Trạng thái relay thật từ measurement actuator.</p></div></div>
            <div className="info-grid">
              <InfoBox label="Pump" value={<StatusBadge value={pumpState} />} />
              <InfoBox label="Light" value={<StatusBadge value={lightState} />} />
              <InfoBox label="Pump mode" value={actuator?.pump_mode || sensors?.pump_mode} />
              <InfoBox label="Light mode" value={actuator?.light_mode || sensors?.light_mode} />
            </div>
          </section>
        </section>

        <section className="panel">
          <div className="panel-header"><div><h2>Edge AI</h2><p>Kết quả AI và điều khiển từ gateway.</p></div></div>
          <div className="info-grid ai-info-grid">
            <InfoBox label="Need watering" value={sensors?.need_watering} />
            <InfoBox label="AI confidence" value={formatNumber(sensors?.ai_confidence ?? sensors?.confidence, 2)} />
            <InfoBox label="AI source" value={sensors?.ai_source} />
            <InfoBox label="Pump command event" value={commandEvent?.status} />
            <InfoBox label="Latest dt target" value={dtCommand?.target} />
            <InfoBox label="Latest dt status" value={dtCommand?.status} />
          </div>
        </section>

        {sensorHistory.length > 0 && (
          <>
            <ChartPanel
              title="Sensor trend"
              subtitle="Nhiệt độ, độ ẩm không khí và độ ẩm đất theo thời gian."
              data={chartData}
              lines={[
                { dataKey: "temperature", name: "Temperature °C" },
                { dataKey: "air_humidity", name: "Air humidity %" },
                { dataKey: "soil_moisture_display", name: "Soil moisture %" },
              ]}
            />

            <ChartPanel
              title="Light trend"
              subtitle="Cường độ sáng BH1750 theo thời gian."
              data={chartData}
              lines={[{ dataKey: "lux", name: "Lux" }]}
            />

            <section className="panel">
              <div className="panel-header"><div><h2>Bảng dữ liệu sensors</h2><p>30 dòng dữ liệu sensors mới nhất từ InfluxDB.</p></div></div>
              <div className="table-wrapper">
                <table>
                  <thead>
                    <tr>
                      <th>Time</th><th>Temp</th><th>Air Humidity</th><th>Lux</th><th>Soil</th><th>Phase</th><th>Need</th><th>Confidence</th><th>AI Source</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sensorHistory.slice().reverse().slice(0, 30).map((row, index) => (
                      <tr key={`${row._time}-${index}`}>
                        <td>{formatTime(row._time)}</td>
                        <td>{formatNumber(row.temperature, 1)}</td>
                        <td>{formatNumber(row.air_humidity, 1)}</td>
                        <td>{formatNumber(row.lux, 1)}</td>
                        <td>{formatNumber(row.soil_moisture ?? row.soil_avg ?? row.soil, 1)}</td>
                        <td>{row.phase ?? "N/A"}</td>
                        <td>{row.need_watering ?? "N/A"}</td>
                        <td>{formatNumber(row.ai_confidence ?? row.confidence, 2)}</td>
                        <td>{row.ai_source ?? "N/A"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          </>
        )}

        {status && (
          <section className="panel">
            <div className="panel-header"><div><h2>Status JSON</h2><p>Dữ liệu mới nhất từ measurement status.</p></div></div>
            <pre className="json-box">{JSON.stringify(status, null, 2)}</pre>
          </section>
        )}
      </main>
    </div>
  );
}

export default App;
