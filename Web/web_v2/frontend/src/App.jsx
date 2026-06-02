import { useEffect, useMemo, useState } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  CartesianGrid,
  ResponsiveContainer,
} from "recharts";
import "./App.css";

const API_BASE = "http://localhost:8000";

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
  return (
    actuator?.pump_state ||
    actuator?.pump ||
    sensors?.pump_state ||
    sensors?.pump ||
    "N/A"
  );
}

function getLightState(actuator, sensors) {
  return (
    actuator?.light_state ||
    actuator?.light ||
    sensors?.light_state ||
    sensors?.light ||
    "N/A"
  );
}

function getSoilMoisture(sensors) {
  return (
    sensors?.soil_moisture ??
    sensors?.soil_avg ??
    sensors?.soil ??
    null
  );
}

function StatusBadge({ value }) {
  const text = String(value ?? "N/A");

  let className = "status-badge";

  if (
    text === "ON" ||
    text === "1" ||
    text === "PUMP_ON" ||
    text === "LIGHT_ON"
  ) {
    className += " status-on";
  } else if (
    text === "OFF" ||
    text === "0" ||
    text === "PUMP_OFF" ||
    text === "LIGHT_OFF"
  ) {
    className += " status-off";
  } else {
    className += " status-unknown";
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
        {unit && <span className="metric-unit"> {unit}</span>}
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
    <section className="panel chart-panel">
      <div className="panel-header">
        <div>
          <h2>{title}</h2>
          {subtitle && <p>{subtitle}</p>}
        </div>
      </div>

      <div className="chart-box">
        <ResponsiveContainer width="100%" height={320}>
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="display_time" minTickGap={30} />
            <YAxis />
            <Tooltip />
            <Legend />
            {lines.map((line) => (
              <Line
                key={line.key}
                type="monotone"
                dataKey={line.key}
                name={line.name}
                stroke={line.stroke}
                dot={false}
                strokeWidth={2.8}
                connectNulls
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
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
  const [lastUpdate, setLastUpdate] = useState("");

  function buildQueryString() {
    if (selectedDate) {
      return `date=${selectedDate}`;
    }

    return `minutes=${minutes}`;
  }

  async function loadDashboardData() {
    try {
      setError("");

      const queryString = buildQueryString();

      const [healthResponse, latestResponse, sensorsHistoryResponse] =
        await Promise.all([
          fetch(`${API_BASE}/api/health`),
          fetch(`${API_BASE}/api/dashboard/latest?${queryString}`),
          fetch(`${API_BASE}/api/history/sensors?${queryString}`),
        ]);

      if (!healthResponse.ok) {
        throw new Error(`Health API error: ${healthResponse.status}`);
      }

      if (!latestResponse.ok) {
        throw new Error(`Latest API error: ${latestResponse.status}`);
      }

      if (!sensorsHistoryResponse.ok) {
        throw new Error(`Sensors history API error: ${sensorsHistoryResponse.status}`);
      }

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

  useEffect(() => {
    loadDashboardData();

    const timer = setInterval(() => {
      loadDashboardData();
    }, 5000);

    return () => clearInterval(timer);
  }, [minutes, selectedDate]);

  const sensors = latest?.sensors;
  const actuator = latest?.actuator;
  const status = latest?.status;

  const soilMoisture = getSoilMoisture(sensors);
  const pumpState = getPumpState(actuator, sensors);
  const lightState = getLightState(actuator, sensors);

  const chartData = useMemo(() => {
    return sensorHistory.map((row) => ({
      ...row,
      display_time: formatShortTime(row._time),
      soil_moisture_display:
        row.soil_moisture ?? row.soil_avg ?? row.soil ?? null,
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
            <p>Dashboard</p>
          </div>
        </div>

        <div className="sidebar-section">
          <div className="sidebar-label">Data source</div>
          <div className="sidebar-value">InfluxDB from BBB Gateway</div>
        </div>

        <div className="sidebar-section">
          <div className="sidebar-label">Bucket</div>
          <div className="sidebar-value">{health?.bucket || "N/A"}</div>
        </div>

        <div className="sidebar-section">
          <div className="sidebar-label">Measurements</div>
          <div className="sidebar-value">
            sensors / status / actuator
          </div>
        </div>

        <div className="sidebar-section">
          <label className="sidebar-label">View mode</label>

          <select
            className="sidebar-select"
            value={selectedDate ? "date" : "recent"}
            onChange={(e) => {
              if (e.target.value === "recent") {
                setSelectedDate("");
              }
            }}
          >
            <option value="recent">Theo thời gian gần đây</option>
            <option value="date">Theo ngày cụ thể</option>
          </select>
        </div>

        <div className="sidebar-section">
          <label className="sidebar-label">Recent range</label>

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

        <div className="sidebar-section">
          <label className="sidebar-label">Select date</label>

          <input
            className="sidebar-input"
            type="date"
            value={selectedDate}
            onChange={(e) => setSelectedDate(e.target.value)}
          />

          {selectedDate && (
            <button
              className="clear-date-button"
              onClick={() => setSelectedDate("")}
            >
              Clear date
            </button>
          )}
        </div>

        <button className="sidebar-button" onClick={loadDashboardData}>
          Refresh Data
        </button>

        <a
          className="sidebar-download"
          href={`${API_BASE}/api/export/sensors.csv?${buildQueryString()}`}
          target="_blank"
          rel="noreferrer"
        >
          Download Sensors CSV
        </a>

        <div className="sidebar-footer">
          <div className="live-dot"></div>
          <span>
            {selectedDate ? `Viewing: ${selectedDate}` : "Realtime polling: 5s"}
          </span>
        </div>
      </aside>

      <main className="main-content">
        <header className="top-header">
          <div>
            <div className="eyebrow">ESP32 + MQTT + BBB Gateway + InfluxDB</div>

            <h1>Plant Monitoring CPS Dashboard</h1>

            <p>
              Dashboard đọc dữ liệu thật do BBB/gateway.py ghi lên InfluxDB.
              Không sử dụng fake data hoặc dữ liệu test.
            </p>
          </div>

          <div className="header-status-card">
            <div className="header-status-label">Backend status</div>

            <div className="header-status-value">
              {health?.status === "OK" ? "Online" : "Checking"}
            </div>
          </div>
        </header>

        {error && <div className="error-box">Lỗi: {error}</div>}

        <section className="summary-grid">
          <div className="summary-card">
            <span>Last dashboard update</span>
            <strong>{lastUpdate || "N/A"}</strong>
          </div>

          <div className="summary-card">
            <span>Latest sensor time</span>
            <strong>{formatTime(sensors?._time)}</strong>
          </div>

          <div className="summary-card">
            <span>Rows loaded</span>
            <strong>{sensorHistory.length}</strong>
          </div>

          <div className="summary-card">
            <span>View mode</span>
            <strong>{selectedDate ? `Date: ${selectedDate}` : `Recent: ${minutes}m`}</strong>
          </div>
        </section>

        {!hasRealData && (
          <section className="panel">
            <div className="empty-box">
              Chưa có dữ liệu thật từ BBB/gateway.py trong InfluxDB. Khi ESP32 gửi
              dữ liệu MQTT và gateway ghi vào measurement <b>sensors</b>, dashboard
              sẽ tự hiển thị.
            </div>
          </section>
        )}

        <section className="panel">
          <div className="panel-header">
            <div>
              <h2>Dữ liệu cảm biến mới nhất</h2>
              <p>Dữ liệu lấy từ measurement sensors trong InfluxDB.</p>
            </div>
          </div>

          <div className="metric-grid">
            <MetricCard
              icon="🌡️"
              title="Nhiệt độ"
              value={formatNumber(sensors?.temperature, 1)}
              unit="°C"
              hint="DHT sensor"
              accent="accent-temp"
            />

            <MetricCard
              icon="💧"
              title="Độ ẩm không khí"
              value={formatNumber(sensors?.air_humidity, 1)}
              unit="%"
              hint="DHT sensor"
              accent="accent-humidity"
            />

            <MetricCard
              icon="☀️"
              title="Ánh sáng"
              value={formatNumber(sensors?.lux, 1)}
              unit="lux"
              hint="BH1750"
              accent="accent-light"
            />

            <MetricCard
              icon="🌱"
              title="Độ ẩm đất"
              value={formatNumber(soilMoisture, 1)}
              unit="%"
              hint="Soil sensors / ADS1115"
              accent="accent-soil"
            />
          </div>
        </section>

        <section className="system-grid">
          <div className="panel">
            <div className="panel-header">
              <div>
                <h2>Growth Phase</h2>
                <p>Thông tin phase do gateway xử lý và ghi lên InfluxDB.</p>
              </div>
            </div>

            <div className="info-grid">
              <InfoBox label="Phase" value={sensors?.phase} />
              <InfoBox label="Phase Source" value={sensors?.phase_source} />
              <InfoBox
                label="Days After Planting"
                value={sensors?.days_after_planting ?? sensors?.days_after_sowing}
              />
              <InfoBox label="Light Warning" value={sensors?.light_warning} />
            </div>
          </div>

          <div className="panel">
            <div className="panel-header">
              <div>
                <h2>Actuator State</h2>
                <p>Trạng thái bơm và đèn từ measurement actuator.</p>
              </div>
            </div>

            <div className="info-grid">
              <InfoBox label="Pump State" value={<StatusBadge value={pumpState} />} />
              <InfoBox label="Light State" value={<StatusBadge value={lightState} />} />
              <InfoBox label="Actuator Time" value={formatTime(actuator?._time)} />
              <InfoBox label="Control Reason" value={actuator?.reason || sensors?.control_reason} />
            </div>
          </div>
        </section>

        <section className="panel">
          <div className="panel-header">
            <div>
              <h2>Edge AI</h2>
              <p>Kết quả AI và điều khiển từ gateway.</p>
            </div>
          </div>

          <div className="info-grid ai-info-grid">
            <InfoBox label="Need Watering" value={sensors?.need_watering} />
            <InfoBox
              label="Confidence"
              value={formatNumber(
                sensors?.ai_confidence ?? sensors?.confidence,
                2
              )}
            />
            <InfoBox
              label="Prob Need Watering"
              value={formatNumber(sensors?.prob_need_watering, 2)}
            />
            <InfoBox label="AI Source" value={sensors?.ai_source} />
            <InfoBox label="AI Action" value={sensors?.ai_action} />
            <InfoBox label="Gateway Step" value={sensors?.gw_step ?? sensors?.step} />
          </div>
        </section>

        {sensorHistory.length > 0 && (
          <>
            <div className="chart-layout">
              <ChartPanel
                title="Environment Chart"
                subtitle="Temperature, air humidity and soil moisture."
                data={chartData}
                lines={[
                  {
                    key: "temperature",
                    name: "Temperature",
                    stroke: "#fb7185",
                  },
                  {
                    key: "air_humidity",
                    name: "Air Humidity",
                    stroke: "#38bdf8",
                  },
                  {
                    key: "soil_moisture_display",
                    name: "Soil Moisture",
                    stroke: "#4ade80",
                  },
                ]}
              />

              <ChartPanel
                title="Light Chart"
                subtitle="Light intensity from BH1750."
                data={chartData}
                lines={[
                  {
                    key: "lux",
                    name: "Lux",
                    stroke: "#facc15",
                  },
                ]}
              />

              <ChartPanel
                title="AI Chart"
                subtitle="Need watering, confidence and probability."
                data={chartData}
                lines={[
                  {
                    key: "need_watering",
                    name: "Need Watering",
                    stroke: "#a78bfa",
                  },
                  {
                    key: "ai_confidence",
                    name: "AI Confidence",
                    stroke: "#f8fafc",
                  },
                  {
                    key: "prob_need_watering",
                    name: "Prob Need Watering",
                    stroke: "#fb923c",
                  },
                ]}
              />
            </div>

            <section className="panel">
              <div className="panel-header">
                <div>
                  <h2>Bảng dữ liệu sensors</h2>
                  <p>30 dòng dữ liệu sensors mới nhất từ InfluxDB.</p>
                </div>
              </div>

              <div className="table-wrapper">
                <table>
                  <thead>
                    <tr>
                      <th>Time</th>
                      <th>Temp</th>
                      <th>Air Humidity</th>
                      <th>Lux</th>
                      <th>Soil</th>
                      <th>Phase</th>
                      <th>Need</th>
                      <th>Confidence</th>
                      <th>AI Source</th>
                    </tr>
                  </thead>

                  <tbody>
                    {sensorHistory
                      .slice()
                      .reverse()
                      .slice(0, 30)
                      .map((row, index) => (
                        <tr key={`${row._time}-${index}`}>
                          <td>{formatTime(row._time)}</td>
                          <td>{formatNumber(row.temperature, 1)}</td>
                          <td>{formatNumber(row.air_humidity, 1)}</td>
                          <td>{formatNumber(row.lux, 1)}</td>
                          <td>
                            {formatNumber(
                              row.soil_moisture ?? row.soil_avg ?? row.soil,
                              1
                            )}
                          </td>
                          <td>{row.phase ?? "N/A"}</td>
                          <td>{row.need_watering ?? "N/A"}</td>
                          <td>
                            {formatNumber(row.ai_confidence ?? row.confidence, 2)}
                          </td>
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
            <div className="panel-header">
              <div>
                <h2>Gateway Status</h2>
                <p>Dữ liệu mới nhất từ measurement status.</p>
              </div>
            </div>

            <pre className="json-box">{JSON.stringify(status, null, 2)}</pre>
          </section>
        )}
      </main>
    </div>
  );
}

export default App;