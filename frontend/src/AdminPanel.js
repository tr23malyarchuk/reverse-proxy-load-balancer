import React, { useState, useEffect, useCallback, useRef } from "react";
import "./AdminPanel.css";

const API = "";
const REFRESH_INTERVAL = 5000;

const ALGO_COLORS = {
  round_robin:       "#3b82f6",
  random:            "#22c55e",
  least_connections: "#f59e0b",
  ip_hash:           "#a78bfa",
  power_of_two:      "#34d399",
};

const SERVER_COLORS = ["#3b82f6", "#22c55e", "#f59e0b", "#ef4444", "#a78bfa"];

function useAdminData() {
  const [servers, setServers]   = useState({ healthy_count: 0, servers: [], algorithms: [] });
  const [stats, setStats]       = useState({ by_server: [] });
  const [recent, setRecent]     = useState({ requests: [] });
  const [connected, setConnected] = useState(null);
  const [lastUpdate, setLastUpdate] = useState(null);

  const fetchAll = useCallback(async () => {
    try {
      const [sRes, stRes, rRes] = await Promise.all([
        fetch(`${API}/servers`),
        fetch(`${API}/stats`),
        fetch(`${API}/recent?n=50`),
      ]);
      const [s, st, r] = await Promise.all([sRes.json(), stRes.json(), rRes.json()]);
      setServers(s);
      setStats(st);
      setRecent(r);
      setConnected(true);
      setLastUpdate(new Date());
    } catch {
      setConnected(false);
    }
  }, []);

  useEffect(() => {
    fetchAll();
    const id = setInterval(fetchAll, REFRESH_INTERVAL);
    return () => clearInterval(id);
  }, [fetchAll]);

  return { servers, stats, recent, connected, lastUpdate, fetchAll };
}

function BarChart({ data, colors }) {
  if (!data || !data.length) return <div className="ap-empty-chart">Немає даних</div>;

  const max = Math.max(...data.map(d => d.value), 1);

  // Fixed pixel dimensions — viewBox scales to container via CSS
  const W = 320;
  const H = 120;          // bar area height
  const LABEL_H = 72;     // space below bars for vertical labels
  const TOP_PAD = 18;     // space above bars for value labels
  const TOTAL_H = TOP_PAD + H + LABEL_H;

  const count = data.length;
  const gap = W / count;
  const barW = Math.max(6, Math.min(36, gap * 0.55));

  return (
    <svg
      viewBox={`0 0 ${W} ${TOTAL_H}`}
      className="ap-bar-svg"
      style={{ width: "100%", height: "auto" }}
    >
      {data.map((d, i) => {
        const barH = Math.max(3, (d.value / max) * H);
        const barX = i * gap + (gap - barW) / 2;
        const barY = TOP_PAD + H - barH;
        const centerX = barX + barW / 2;
        const color = colors ? colors[i % colors.length] : "#3b82f6";

        // Format value: if float keep 3 decimals, otherwise integer
        const displayVal = Number.isInteger(d.value)
          ? d.value
          : d.value.toFixed(3);

        return (
          <g key={i}>
            {/* Bar */}
            <rect
              x={barX} y={barY}
              width={barW} height={barH}
              rx="3" fill={color} opacity="0.85"
            />

            {/* Value label above bar */}
            <text
              x={centerX}
              y={barY - 4}
              textAnchor="middle"
              fontSize="9"
              fontFamily="'JetBrains Mono', monospace"
              fill={color}
              opacity="0.95"
            >
              {displayVal}
            </text>

            {/* Category label — vertical, centred under bar */}
            <text
              x={centerX}
              y={TOP_PAD + H + 6}
              textAnchor="end"
              dominantBaseline="middle"
              fontSize="9"
              fontFamily="'JetBrains Mono', monospace"
              fill="#9ca3af"
              transform={`rotate(-90, ${centerX}, ${TOP_PAD + H + 6})`}
            >
              {d.label}
            </text>
          </g>
        );
      })}

      {/* Baseline */}
      <line
        x1={0} y1={TOP_PAD + H}
        x2={W} y2={TOP_PAD + H}
        stroke="#374151" strokeWidth="1"
      />
    </svg>
  );
}

// Sparkline
function Sparkline({ values, color = "#3b82f6" }) {
  if (!values || values.length < 2) return null;
  const W = 80, H = 28;
  const max = Math.max(...values, 0.001);
  const pts = values.map((v, i) => {
    const x = (i / (values.length - 1)) * W;
    const y = H - (v / max) * H;
    return `${x},${y}`;
  }).join(" ");
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="ap-sparkline">
      <polyline points={pts} fill="none" stroke={color} strokeWidth="1.5" strokeLinejoin="round" />
    </svg>
  );
}

// Metric card
function MetricCard({ label, value, color, spark }) {
  return (
    <div className="ap-metric">
      <div className="ap-metric-label">{label}</div>
      <div className="ap-metric-row">
        <div className="ap-metric-value" style={{ color: color || "var(--ap-text)" }}>
          {value ?? "—"}
        </div>
        {spark && <Sparkline values={spark} color={color} />}
      </div>
    </div>
  );
}

// Server card 
function ServerCard({ server, statRow, colorIdx }) {
  const color = SERVER_COLORS[colorIdx % SERVER_COLORS.length];
  const rate = statRow?.total ? Math.round((statRow.success / statRow.total) * 100) : 100;

  return (
    <div className="ap-server-card" style={{ "--accent": color }}>
      <div className="ap-server-top">
        <div>
          <div className="ap-server-name">{server.name}</div>
          <div className="ap-server-url">{server.url}</div>
        </div>
        <span className="ap-badge ap-badge-online">online</span>
      </div>
      <div className="ap-server-stats">
        <div className="ap-server-stat">
          <span>Активних з'єднань</span>
          <span className="ap-mono">{server.active_connections}</span>
        </div>
        <div className="ap-server-stat">
          <span>Всього запитів</span>
          <span className="ap-mono">{statRow?.total ?? 0}</span>
        </div>
        <div className="ap-server-stat">
          <span>Успіх</span>
          <span className="ap-mono" style={{ color: rate >= 90 ? "#22c55e" : "#f59e0b" }}>
            {rate}%
          </span>
        </div>
        <div className="ap-server-stat">
          <span>Сер. час</span>
          <span className="ap-mono">{statRow?.avg_s != null ? `${statRow.avg_s} с` : "—"}</span>
        </div>
      </div>
    </div>
  );
}

// Requests log table
function LogTable({ requests }) {
  const [filter, setFilter] = useState("");
  const servers = [...new Set(requests.map(r => r.server))].sort();
  const filtered = filter ? requests.filter(r => r.server === filter) : requests;

  return (
    <div className="ap-card">
      <div className="ap-card-header">
        <span className="ap-section-title">Лог запитів</span>
        <select
          className="ap-select"
          value={filter}
          onChange={e => setFilter(e.target.value)}
        >
          <option value="">Всі сервери</option>
          {servers.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
      </div>
      {filtered.length === 0 ? (
        <div className="ap-empty">Запитів ще не було</div>
      ) : (
        <div className="ap-table-wrap">
          <table className="ap-table">
            <thead>
              <tr>
                <th>Час</th>
                <th>Сервер</th>
                <th>Алгоритм</th>
                <th>Endpoint</th>
                <th>с</th>
                <th>Статус</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((r, i) => (
                <tr key={i}>
                  <td className="ap-mono ap-muted ap-small">{r.timestamp}</td>
                  <td>
                    <span className="ap-badge ap-badge-server">{r.server}</span>
                  </td>
                  <td>
                    <span
                      className="ap-badge ap-badge-algo"
                      style={{ "--algo-color": ALGO_COLORS[r.algorithm] || "#6b7280" }}
                    >
                      {r.algorithm}
                    </span>
                  </td>
                  <td className="ap-mono ap-small">{r.endpoint}</td>
                  <td className="ap-mono">{r.total_time}</td>
                  <td>
                    <span className={`ap-badge ${r.success ? "ap-badge-ok" : "ap-badge-fail"}`}>
                      {r.success ? "OK" : "FAIL"}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// Algorithms reference
const ALGO_INFO = [
  { id: "round_robin",       label: "По черзі",           desc: "Однорідні запити, рівномірне навантаження" },
  { id: "random",            label: "Випадково",           desc: "Тестування, рівноцінні сервери" },
  { id: "least_connections", label: "Мінімум з'єднань",   desc: "Різні за тривалістю запити" },
  { id: "ip_hash",           label: "За IP клієнта",      desc: "Прив'язка сесії до сервера" },
  { id: "power_of_two",      label: "Краще з двох",       desc: "Гетерогенне навантаження" },
];

function AlgorithmsPanel({ algorithms, recentRequests }) {
  const algoMap = {};
  recentRequests.forEach(r => { algoMap[r.algorithm] = (algoMap[r.algorithm] || 0) + 1; });

  return (
    <div className="ap-section">
      <div className="ap-card">
        <div className="ap-card-header">
          <span className="ap-section-title">Алгоритми балансування</span>
        </div>
        <div className="ap-algo-grid">
          {ALGO_INFO.map(a => (
            <div
              key={a.id}
              className={`ap-algo-card ${algorithms.includes(a.id) ? "ap-algo-available" : ""}`}
              style={{ "--algo-color": ALGO_COLORS[a.id] || "#6b7280" }}
            >
              <div className="ap-algo-name">{a.id}</div>
              <div className="ap-algo-label">{a.label}</div>
              <div className="ap-algo-desc">{a.desc}</div>
              {algoMap[a.id] && (
                <div className="ap-algo-count">{algoMap[a.id]} запитів</div>
              )}
            </div>
          ))}
        </div>
        <div className="ap-code-block">
          <div className="ap-code-label">Приклад запиту з вибором алгоритму:</div>
          <pre className="ap-pre">{`curl -X POST http://localhost:8000/pdf2png \\
  -F "file=@doc.pdf" \\
  -F "algorithm=least_connections"`}</pre>
        </div>
      </div>
    </div>
  );
}

// Main AdminPanel
const TABS = [
  { id: "overview",   label: "Огляд" },
  { id: "servers",    label: "Сервери" },
  { id: "stats",      label: "Статистика" },
  { id: "log",        label: "Лог" },
  { id: "algorithms", label: "Алгоритми" },
];

export default function AdminPanel() {
  const [tab, setTab] = useState("overview");
  const { servers, stats, recent, connected, lastUpdate, fetchAll } = useAdminData();

  const byServer = stats.by_server || [];
  const requests = recent.requests || [];
  const liveServers = servers.servers || [];

  const statsMap = {};
  byServer.forEach(r => { statsMap[r.server] = r; });

  const totalReqs = byServer.reduce((s, r) => s + (r.total || 0), 0);
  const totalOk   = byServer.reduce((s, r) => s + (r.success || 0), 0);
  const totalFail = totalReqs - totalOk;
  const avgTime   = byServer.length
    ? (byServer.reduce((s, r) => s + (r.avg_s || 0), 0) / byServer.length).toFixed(3)
    : "—";
  const totalConns = liveServers.reduce((s, v) => s + (v.active_connections || 0), 0);

  // Sparkline data — last 20 requests' response times
  const sparkTimes = requests.slice(0, 20).map(r => r.total_time || 0).reverse();

  return (
    <div className="ap-root">

      {/* Header */}
      <div className="ap-header">
        <div className="ap-header-left">
          <div className={`ap-dot ${connected === false ? "ap-dot-red" : connected ? "ap-dot-green" : "ap-dot-amber"}`} />
          <span className="ap-header-status">
            {connected === null ? "підключення..." : connected ? "балансировщик online" : "балансировщик недоступний"}
          </span>
          {lastUpdate && (
            <span className="ap-header-time">
              оновлено {lastUpdate.toLocaleTimeString("uk-UA")}
            </span>
          )}
        </div>
        <button className="ap-refresh-btn" onClick={fetchAll}>↺ оновити</button>
      </div>

      {/* Error banner */}
      {connected === false && (
        <div className="ap-error-banner">
          ⚠ Балансировщик недоступний. Переконайтесь, що uvicorn запущено на порту 8000.
        </div>
      )}

      {/* Tabs */}
      <div className="ap-tabs">
        {TABS.map(t => (
          <button
            key={t.id}
            className={`ap-tab ${tab === t.id ? "ap-tab-active" : ""}`}
            onClick={() => setTab(t.id)}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Overview */}
      {tab === "overview" && (
        <div className="ap-section">
          <div className="ap-metrics-row">
            <MetricCard label="Живих серверів" value={servers.healthy_count} color="#22c55e" />
            <MetricCard label="Всього запитів" value={totalReqs} />
            <MetricCard label="Успішних" value={totalOk} color="#22c55e" />
            <MetricCard label="Помилок" value={totalFail} color={totalFail > 0 ? "#ef4444" : "#22c55e"} />
            <MetricCard label="Сер. час, с" value={avgTime} color="#f59e0b" spark={sparkTimes} />
            <MetricCard label="Активних з'єднань" value={totalConns} />
          </div>

          <div className="ap-charts-row">
            <div className="ap-card ap-chart-card">
              <div className="ap-card-header">
                <span className="ap-section-title">Запити по серверах</span>
              </div>
              <BarChart
                data={byServer.map(r => ({ label: r.server, value: r.total || 0 }))}
                colors={SERVER_COLORS}
              />
            </div>
            <div className="ap-card ap-chart-card">
              <div className="ap-card-header">
                <span className="ap-section-title">По алгоритмах</span>
              </div>
              {(() => {
                const algoMap = {};
                requests.forEach(r => { algoMap[r.algorithm] = (algoMap[r.algorithm] || 0) + 1; });
                const data = Object.entries(algoMap).map(([k, v]) => ({ label: k.replace("_", " "), value: v }));
                return <BarChart data={data} colors={Object.values(ALGO_COLORS)} />;
              })()}
            </div>
            <div className="ap-card ap-chart-card">
              <div className="ap-card-header">
                <span className="ap-section-title">Сер. час відповіді, с</span>
              </div>
              <BarChart
                data={byServer.map(r => ({ label: r.server, value: r.avg_s || 0 }))}
                colors={["#f59e0b"]}
              />
            </div>
          </div>
        </div>
      )}

      {/* Servers */}
      {tab === "servers" && (
        <div className="ap-section">
          {liveServers.length === 0 ? (
            <div className="ap-card">
              <div className="ap-empty">Немає живих серверів. Запустіть scriptA.sh</div>
            </div>
          ) : (
            <div className="ap-servers-grid">
              {liveServers.map((s, i) => (
                <ServerCard key={s.name} server={s} statRow={statsMap[s.name]} colorIdx={i} />
              ))}
            </div>
          )}
        </div>
      )}

      {/* Stats */}
      {tab === "stats" && (
        <div className="ap-section">
          <div className="ap-card">
            <div className="ap-card-header">
              <span className="ap-section-title">Статистика по серверах</span>
            </div>
            {byServer.length === 0 ? (
              <div className="ap-empty">Запитів ще не було</div>
            ) : (
              <div className="ap-table-wrap">
                <table className="ap-table">
                  <thead>
                    <tr>
                      <th>Сервер</th>
                      <th>Всього</th>
                      <th>OK</th>
                      <th>Помилок</th>
                      <th>Успіх %</th>
                      <th>Сер. с</th>
                      <th>Мін. с</th>
                      <th>Макс. с</th>
                    </tr>
                  </thead>
                  <tbody>
                    {byServer.map(r => {
                      const fail = (r.total || 0) - (r.success || 0);
                      const rate = r.total ? Math.round(r.success / r.total * 100) : 100;
                      return (
                        <tr key={r.server}>
                          <td><span className="ap-badge ap-badge-server">{r.server}</span></td>
                          <td className="ap-mono">{r.total}</td>
                          <td className="ap-mono" style={{ color: "#22c55e" }}>{r.success}</td>
                          <td className="ap-mono" style={{ color: fail > 0 ? "#ef4444" : "#6b7280" }}>{fail}</td>
                          <td className="ap-mono" style={{ color: rate >= 90 ? "#22c55e" : "#f59e0b" }}>{rate}%</td>
                          <td className="ap-mono">{r.avg_s ?? "—"}</td>
                          <td className="ap-mono">{r.min_s ?? "—"}</td>
                          <td className="ap-mono">{r.max_s ?? "—"}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Log */}
      {tab === "log" && <LogTable requests={requests} />}

      {/* Algorithms */}
      {tab === "algorithms" && (
        <AlgorithmsPanel
          algorithms={servers.algorithms || []}
          recentRequests={requests}
        />
      )}
    </div>
  );
}

