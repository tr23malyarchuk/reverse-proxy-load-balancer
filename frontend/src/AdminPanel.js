// AdminPanel.js
import React, { useState, useEffect, useCallback, useRef } from "react";
import "./AdminPanel.css";

const API = "http://localhost:8000";
const ALGORITHMS = ["round_robin","random","least_connections","ip_hash","power_of_two"];
const CPU_INTENSITIES = ["low","medium","high"];
const METRIC_TYPES = ["cpu_percent","active_connections","latency_ms"];
const SERVER_COLORS = ["#3b82f6","#22c55e","#f59e0b","#ef4444","#a78bfa","#34d399"];
const ALGO_COLORS = { round_robin:"#3b82f6", random:"#22c55e", least_connections:"#f59e0b",
                      ip_hash:"#a78bfa", power_of_two:"#34d399" };

const apiFetch = (url, opts = {}) =>
  fetch(API + url, { headers: { "Content-Type": "application/json" }, ...opts });

function usePoll(fetchFn, interval = 5000) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const refresh = useCallback(async () => {
    try {
      const d = await fetchFn();
      setData(d); setError(null);
    } catch (e) { setError(e.message); }
    finally { setLoading(false); }
  }, [fetchFn]);
  useEffect(() => { refresh(); const id = setInterval(refresh, interval); return () => clearInterval(id); }, [refresh, interval]);
  return { data, loading, error, refresh };
}

function BarChart({ data, colors, unit = "" }) {
  if (!data || !data.length) return <div className="ap-empty-chart">Немає даних</div>;
  const max = Math.max(...data.map(d => d.value), 0.001);
  const W = 320, H = 120, LABEL_H = 72, TOP_PAD = 22, TOTAL_H = TOP_PAD + H + LABEL_H;
  const count = data.length;
  const gap = W / count;
  const barW = Math.max(6, Math.min(40, gap * 0.6));
  return (
    <svg viewBox={`0 0 ${W} ${TOTAL_H}`} className="ap-bar-svg" style={{ width:"100%", height:"auto" }}>
      <line x1={0} y1={TOP_PAD+H} x2={W} y2={TOP_PAD+H} stroke="#374151" strokeWidth="0.8"/>
      {data.map((d, i) => {
        const barH = Math.max(3, (d.value / max) * H);
        const barX = i * gap + (gap - barW) / 2;
        const barY = TOP_PAD + H - barH;
        const cx = barX + barW / 2;
        const color = colors ? colors[i % colors.length] : "#3b82f6";
        const displayVal = typeof d.value === "number"
          ? (Number.isInteger(d.value) ? d.value : d.value.toFixed(1)) + unit
          : d.value;
        return (
          <g key={i}>
            <rect x={barX} y={barY} width={barW} height={barH} rx="3" fill={color} opacity="0.85"/>
            <text x={cx} y={barY - 4} textAnchor="middle" fontSize="9"
              fontFamily="'JetBrains Mono',monospace" fill={color} opacity="0.95">{displayVal}</text>
            <text x={cx} y={TOP_PAD+H+6} textAnchor="end" dominantBaseline="middle"
              fontSize="9" fontFamily="'JetBrains Mono',monospace" fill="#9ca3af"
              transform={`rotate(-90,${cx},${TOP_PAD+H+6})`}>{d.label}</text>
          </g>
        );
      })}
    </svg>
  );
}

function GaugeBar({ value, max = 100, color = "#3b82f6", label }) {
  const pct = Math.min(100, Math.max(0, (value / max) * 100));
  const c = pct > 80 ? "#ef4444" : pct > 50 ? "#f59e0b" : color;
  return (
    <div className="ap-gauge-wrap">
      {label && <span className="ap-gauge-label">{label}</span>}
      <div className="ap-gauge-track">
        <div className="ap-gauge-fill" style={{ width: `${pct}%`, background: c }} />
      </div>
      <span className="ap-gauge-val" style={{ color: c }}>{value.toFixed(1)}%</span>
    </div>
  );
}

function Modal({ title, onClose, children }) {
  return (
    <div className="ap-modal-overlay" onClick={onClose}>
      <div className="ap-modal" onClick={e => e.stopPropagation()}>
        <div className="ap-modal-header">
          <span className="ap-modal-title">{title}</span>
          <button className="ap-modal-close" onClick={onClose}>✕</button>
        </div>
        <div className="ap-modal-body">{children}</div>
      </div>
    </div>
  );
}

function Field({ label, children }) {
  return (
    <div className="ap-field">
      <label className="ap-field-label">{label}</label>
      {children}
    </div>
  );
}

function Input({ ...props }) {
  return <input className="ap-input" {...props} />;
}

function Select({ children, ...props }) {
  return <select className="ap-input" {...props}>{children}</select>;
}

function StatusBadge({ status }) {
  const map = { running: "ap-badge-ok", stopped: "ap-badge-fail", error: "ap-badge-fail",
                unknown: "ap-badge-muted" };
  return <span className={`ap-badge ${map[status] || "ap-badge-muted"}`}>{status}</span>;
}

function SectionHeader({ title, action }) {
  return (
    <div className="ap-section-hdr">
      <h2 className="ap-section-h2">{title}</h2>
      {action}
    </div>
  );
}

function Spinner() { return <div className="ap-spinner" />; }

// PANEL: Services & Images
function ServicesPanel() {
  const fetchFn = useCallback(() => apiFetch("/cfg/services").then(r => r.json()), []);
  const { data: services, loading, refresh } = usePoll(fetchFn, 10000);
  const [modal, setModal] = useState(null); // null | "add" | service object
  const [form, setForm] = useState({ name:"", description:"", base_path:"", cpu_intensity:"medium",
                                     docker_image:"", update_period:"" });

  const openAdd = () => {
    setForm({ name:"", description:"", base_path:"", cpu_intensity:"medium", docker_image:"", update_period:"" });
    setModal("add");
  };
  const openEdit = (svc) => {
    setForm({ ...svc, docker_image: svc.docker_image || "", update_period: svc.update_period || "" });
    setModal(svc);
  };

  const save = async () => {
    const method = modal === "add" ? "POST" : "PUT";
    const url = modal === "add" ? "/cfg/services" : `/cfg/services/${modal.idService}`;
    await apiFetch(url, { method, body: JSON.stringify(form) });
    setModal(null); refresh();
  };

  const del = async (id) => {
    if (!window.confirm("Видалити сервіс?")) return;
    await apiFetch(`/cfg/services/${id}`, { method: "DELETE" });
    refresh();
  };

  return (
    <div className="ap-panel">
      <SectionHeader
        title="Services & Images"
        action={<button className="ap-btn ap-btn-primary" onClick={openAdd}>+ Додати сервіс</button>}
      />
      <p className="ap-description">
        Реєстрація сервісів конвертації та відповідних Docker-образів.
        Кожен сервіс має базовий шлях, образ і опціональний період автооновлення контейнерів.
      </p>
      {loading ? <Spinner /> : (
        <div className="ap-table-card">
          <table className="ap-table">
            <thead><tr>
              <th>Назва</th><th>Опис</th><th>Шлях</th>
              <th>CPU</th><th>Образ</th><th>Оновлення</th><th></th>
            </tr></thead>
            <tbody>
              {(services || []).map(s => (
                <tr key={s.idService}>
                  <td className="ap-mono ap-fw">{s.name}</td>
                  <td className="ap-muted">{s.description}</td>
                  <td className="ap-mono">{s.base_path}</td>
                  <td><span className={`ap-badge ${s.cpu_intensity==="high"?"ap-badge-fail":s.cpu_intensity==="medium"?"ap-badge-algo":"ap-badge-ok"}`}>{s.cpu_intensity}</span></td>
                  <td className="ap-mono ap-small">{s.docker_image || <span className="ap-muted">—</span>}</td>
                  <td className="ap-mono ap-small">{s.update_period || <span className="ap-muted">None</span>}</td>
                  <td>
                    <button className="ap-btn-icon" onClick={() => openEdit(s)} title="Редагувати">✎</button>
                    <button className="ap-btn-icon ap-btn-icon-danger" onClick={() => del(s.idService)} title="Видалити">✕</button>
                  </td>
                </tr>
              ))}
              {!services?.length && <tr><td colSpan={7} className="ap-empty">Немає сервісів</td></tr>}
            </tbody>
          </table>
        </div>
      )}

      {modal && (
        <Modal title={modal === "add" ? "Новий сервіс" : "Редагувати сервіс"} onClose={() => setModal(null)}>
          <Field label="Назва сервісу *">
            <Input value={form.name} onChange={e => setForm({...form, name: e.target.value})} placeholder="wav2mp3" />
          </Field>
          <Field label="Опис">
            <Input value={form.description} onChange={e => setForm({...form, description: e.target.value})} placeholder="Конвертація WAV у MP3" />
          </Field>
          <Field label="Базовий шлях *">
            <Input value={form.base_path} onChange={e => setForm({...form, base_path: e.target.value})} placeholder="/wav2mp3" />
          </Field>
          <Field label="Docker-образ">
            <Input value={form.docker_image} onChange={e => setForm({...form, docker_image: e.target.value})} placeholder="tr23malyarchuk/pa-tr23malyarchuk:latest" />
          </Field>
          <Field label="Інтенсивність CPU">
            <Select value={form.cpu_intensity} onChange={e => setForm({...form, cpu_intensity: e.target.value})}>
              {CPU_INTENSITIES.map(v => <option key={v}>{v}</option>)}
            </Select>
          </Field>
          <Field label="Період автооновлення">
            <Input value={form.update_period} onChange={e => setForm({...form, update_period: e.target.value})} placeholder="None (або 1h, 24h...)" />
          </Field>
          <div className="ap-modal-footer">
            <button className="ap-btn" onClick={() => setModal(null)}>Скасувати</button>
            <button className="ap-btn ap-btn-primary" onClick={save}>Зберегти</button>
          </div>
        </Modal>
      )}
    </div>
  );
}

// PANEL: Machines
function MachinesPanel() {
  const fetchFn = useCallback(() => apiFetch("/cfg/machines").then(r => r.json()), []);
  const loadFn  = useCallback(() => apiFetch("/cfg/machine_load").then(r => r.json()), []);
  const { data: machines, loading, refresh } = usePoll(fetchFn, 10000);
  const { data: loads } = usePoll(loadFn, 5000);
  const [modal, setModal] = useState(false);
  const [form, setForm] = useState({ hostname:"", ip_address:"", ssh_port:"22", description:"" });

  const loadMap = {};
  (loads || []).forEach(l => { loadMap[l.name] = l; });

  const add = async () => {
    await apiFetch("/cfg/machines", { method:"POST", body: JSON.stringify({...form, ssh_port: Number(form.ssh_port)}) });
    setModal(false); refresh();
  };
  const del = async (id) => {
    if (!window.confirm("Видалити машину?")) return;
    await apiFetch(`/cfg/machines/${id}`, { method:"DELETE" });
    refresh();
  };

  return (
    <div className="ap-panel">
      <SectionHeader
        title="Machines"
        action={<button className="ap-btn ap-btn-primary" onClick={() => setModal(true)}>+ Додати машину</button>}
      />
      <p className="ap-description">
        Реєстрація IP-адрес комп'ютерів для запуску сервісів. Зліва — конфігурація, справа — поточне навантаження.
      </p>
      {loading ? <Spinner /> : (
        <div className="ap-table-card">
          <table className="ap-table">
            <thead><tr>
              <th>Hostname</th><th>IP</th><th>SSH</th><th>Опис</th>
              <th>Контейнери</th><th>CPU</th><th>RAM</th><th></th>
            </tr></thead>
            <tbody>
              {(machines || []).map(m => {
                const running = (loads || []).filter(l => l.status === "running");
                const cpuSum = running.reduce((s, l) => s + l.cpu_pct, 0);
                const memSum = running.reduce((s, l) => s + l.mem_mb, 0);
                return (
                  <tr key={m.idMachine}>
                    <td className="ap-mono ap-fw">{m.hostname}</td>
                    <td className="ap-mono">{m.ip_address}</td>
                    <td className="ap-mono">{m.ssh_port}</td>
                    <td className="ap-muted">{m.description}</td>
                    <td className="ap-mono">{running.length}</td>
                    <td style={{ minWidth: 120 }}>
                      <GaugeBar value={Math.min(cpuSum, 100)} color="#3b82f6" />
                    </td>
                    <td className="ap-mono">{memSum.toFixed(0)} MB</td>
                    <td>
                      <button className="ap-btn-icon ap-btn-icon-danger" onClick={() => del(m.idMachine)} title="Видалити">✕</button>
                    </td>
                  </tr>
                );
              })}
              {!machines?.length && <tr><td colSpan={8} className="ap-empty">Немає машин</td></tr>}
            </tbody>
          </table>
        </div>
      )}

      {modal && (
        <Modal title="Нова машина" onClose={() => setModal(false)}>
          <Field label="Hostname *"><Input value={form.hostname} onChange={e => setForm({...form, hostname:e.target.value})} placeholder="worker-01" /></Field>
          <Field label="IP-адреса *"><Input value={form.ip_address} onChange={e => setForm({...form, ip_address:e.target.value})} placeholder="192.168.1.10" /></Field>
          <Field label="SSH порт"><Input type="number" value={form.ssh_port} onChange={e => setForm({...form, ssh_port:e.target.value})} /></Field>
          <Field label="Опис"><Input value={form.description} onChange={e => setForm({...form, description:e.target.value})} /></Field>
          <div className="ap-modal-footer">
            <button className="ap-btn" onClick={() => setModal(false)}>Скасувати</button>
            <button className="ap-btn ap-btn-primary" onClick={add}>Додати</button>
          </div>
        </Modal>
      )}
    </div>
  );
}

// PANEL: Pools
function PoolsPanel() {
  const fetchPools    = useCallback(() => apiFetch("/cfg/pools").then(r => r.json()), []);
  const fetchServices = useCallback(() => apiFetch("/cfg/services").then(r => r.json()), []);
  const fetchInstances= useCallback(() => apiFetch("/cfg/instances").then(r => r.json()), []);
  const fetchServers  = useCallback(() => apiFetch("/servers").then(r => r.json()), []);
  
  const { data: pools, loading, refresh } = usePoll(fetchPools, 8000);
  const { data: services } = usePoll(fetchServices, 30000);
  const { data: instances } = usePoll(fetchInstances, 10000);
  const { data: serversData } = usePoll(fetchServers, 3000);
  const { data: loadsData } = usePoll(() => fetch('/cfg/machine_load').then(r => r.json()), 3000);
  const statusMap = {};
  (loadsData || []).forEach(l => {
    statusMap[l.name] = l.status;
  });

  // Active connections mapping by server names
  const activeConnectionsMap = {};
  (serversData?.servers || []).forEach(s => {
    activeConnectionsMap[s.name] = s.active_connections;
  });

  const [modal, setModal] = useState(null);
  const [testing, setTesting] = useState(null); // { poolId, service, inProgress }
  const [testResult, setTestResult] = useState(null);

  const [form, setForm] = useState({
    name:"", service_id:"", algorithm:"round_robin", description:"",
    metric_type:"cpu_percent", scale_out_threshold:"", cooldown_seconds:"60",
    min_instances:"1", max_instances:"4",
  });

  const openAdd = () => {
    setForm({ name:"", service_id: services?.[0]?.idService || "", algorithm:"round_robin",
              description:"", metric_type:"cpu_percent", scale_out_threshold:"",
              cooldown_seconds:"60", min_instances:"1", max_instances:"4" });
    setModal("add");
  };

  const save = async () => {
    const body = { ...form, service_id: Number(form.service_id),
      cooldown_seconds: Number(form.cooldown_seconds),
      min_instances: Number(form.min_instances), max_instances: Number(form.max_instances),
      scale_out_threshold: form.scale_out_threshold ? Number(form.scale_out_threshold) : undefined };
    await apiFetch("/cfg/pools", { method:"POST", body: JSON.stringify(body) });
    setModal(null); refresh();
  };

  const del = async (id) => {
    if (!window.confirm("Видалити пул?")) return;
    await apiFetch(`/cfg/pools/${id}`, { method:"DELETE" });
    refresh();
  };

  const addMember = async (pid, iid) => {
    await apiFetch(`/cfg/pools/${pid}/members`, { method:"POST", body: JSON.stringify({ instance_id: iid }) });
    refresh();
  };

  const removeMember = async (pid, iid) => {
    await apiFetch(`/cfg/pools/${pid}/members/${iid}`, { method:"DELETE" });
    refresh();
  };

  // Test pool with sending requests
  const testPool = async (pool, service) => {
    setTesting({ poolId: pool.idPool, inProgress: true });
    setTestResult(null);
    
    try {
      // Создаем тестовый файл динамически
      let testBlob;
      let fileName;
      let contentType;
      
      // Создаем минимальный тестовый файл в зависимости от сервиса
      switch (service.name) {
        case 'wav2mp3':
          // Создаем простой WAV-файл (44 байта заголовка + тишина)
          const wavHeader = new Uint8Array(44);
          // RIFF header
          const view = new DataView(wavHeader.buffer);
          view.setUint32(0, 0x52494646, true); // "RIFF"
          view.setUint32(4, 36, true); // file size - 8
          view.setUint32(8, 0x57415645, true); // "WAVE"
          view.setUint32(12, 0x666d7420, true); // "fmt "
          view.setUint32(16, 16, true); // chunk size
          view.setUint16(20, 1, true); // PCM
          view.setUint16(22, 1, true); // mono
          view.setUint32(24, 8000, true); // sample rate
          view.setUint32(28, 16000, true); // byte rate
          view.setUint16(32, 2, true); // block align
          view.setUint16(34, 16, true); // bits per sample
          view.setUint32(36, 0x64617461, true); // "data"
          view.setUint32(40, 0, true); // data size
          testBlob = new Blob([wavHeader], { type: 'audio/wav' });
          fileName = 'test.wav';
          contentType = 'audio/wav';
          break;
          
        case 'pdf2png':
          // Создаем минимальный PDF
          const pdfHeader = `%PDF-1.4
  1 0 obj
  << /Type /Catalog /Pages 2 0 R >>
  endobj
  2 0 obj
  << /Type /Pages /Kids [3 0 R] /Count 1 >>
  endobj
  3 0 obj
  << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R >>
  endobj
  4 0 obj
  << /Length 44 >>
  stream
  BT /F1 12 Tf 100 700 Td (Test) Tj ET
  endstream
  endobj
  xref
  0 5
  0000000000 65535 f
  0000000009 00000 n
  0000000058 00000 n
  0000000115 00000 n
  0000000205 00000 n
  trailer
  << /Size 5 /Root 1 0 R >>
  startxref
  289
  %%EOF`;
          testBlob = new Blob([pdfHeader], { type: 'application/pdf' });
          fileName = 'test.pdf';
          contentType = 'application/pdf';
          break;
          
        case 'webp2png':
          // Минимальный WEBP (1x1 пиксель)
          const webpBase64 = 'UklGRhoAAABXRUJQVlA4TA4AAAAvAAAAEAcQERGIiP4HAA==';
          const webpBytes = Uint8Array.from(atob(webpBase64), c => c.charCodeAt(0));
          testBlob = new Blob([webpBytes], { type: 'image/webp' });
          fileName = 'test.webp';
          contentType = 'image/webp';
          break;
          
        case 'rar2zip':
          // Пустой ZIP файл
          const zipHeader = new Uint8Array([0x50, 0x4B, 0x05, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
          testBlob = new Blob([zipHeader], { type: 'application/zip' });
          fileName = 'test.zip';
          contentType = 'application/zip';
          break;
          
        default:
          testBlob = new Blob(['test'], { type: 'text/plain' });
          fileName = 'test.txt';
          contentType = 'text/plain';
      }
      
      const formData = new FormData();
      formData.append('file', testBlob, fileName);
      formData.append('algorithm', pool.algorithm);
      
      const startTime = Date.now();
      const response = await fetch(`http://localhost:8000${service.base_path}`, {
        method: 'POST',
        body: formData,
      });
      const endTime = Date.now();
      
      const chosenServer = response.headers.get('x-chosen-server');
      const totalTime = ((endTime - startTime) / 1000).toFixed(2);
      
      setTestResult({
        success: response.ok,
        server: chosenServer || 'unknown',
        time: totalTime,
        algorithm: pool.algorithm,
        status: response.status,
      });
      
      refresh();
      
    } catch (err) {
      console.error('Test error:', err);
      setTestResult({
        success: false,
        error: err.message,
        algorithm: pool.algorithm,
      });
    } finally {
      setTesting(null);
      setTimeout(() => setTestResult(null), 5000);
    }
  };

  return (
    <div className="ap-panel">
      <SectionHeader
        title="Pools"
        action={<button className="ap-btn ap-btn-primary" onClick={openAdd}>+ Новий пул</button>}
      />
      <p className="ap-description">
        Пул об'єднує інстанси сервісу для балансування. Натисніть <strong>▶ Тест</strong> на картці пулу, 
        чтобы отправить реальный запрос и увидеть, как выбранный алгоритм распределяет нагрузку.
      </p>
      
      {/* Результат теста */}
      {testResult && (
        <div className={`ap-test-result ${testResult.success ? 'ap-test-success' : 'ap-test-fail'}`}>
          <strong>Результат теста:</strong><br/>
          {testResult.success ? (
            <>✅ Запрос успешно обработан сервером <strong>{testResult.server}</strong><br/>
            ⏱ Время: {testResult.time} сек | 🔄 Алгоритм: {testResult.algorithm}</>
          ) : (
            <>❌ Помилка: {testResult.error}</>
          )}
        </div>
      )}
      
      {loading ? <Spinner /> : (
        <div className="ap-pools-grid">
          {(pools || []).map(p => {
            // Find conv service for pool
            const service = (services || []).find(s => s.idService === p.service_id);
            // Count total number of active conns in pool
            const totalConnections = p.members.reduce((sum, m) => {
              return sum + (activeConnectionsMap[m.hostname] || 0);
            }, 0);
            
            return (
              <div key={p.idPool} className="ap-pool-card">
                <div className="ap-pool-header">
                  <div>
                    <div className="ap-pool-name">{p.name}</div>
                    <div className="ap-muted ap-small">{p.service_name}</div>
                  </div>
                  <div style={{ display:"flex", gap:6, alignItems:"center" }}>
                    <span className="ap-badge ap-badge-algo" style={{"--algo-color": ALGO_COLORS[p.algorithm]||"#6b7280"}}>
                      {p.algorithm}
                    </span>
                    <button 
                      className="ap-btn ap-btn-test" 
                      onClick={() => testPool(p, service)}
                      disabled={testing?.inProgress}
                      title="Отправить тестовый запрос"
                    >
                      {testing?.poolId === p.idPool ? "⏳..." : "▶ Тест"}
                    </button>
                    <button className="ap-btn-icon ap-btn-icon-danger" onClick={() => del(p.idPool)}>✕</button>
                  </div>
                </div>

                {/* Статистика пула */}
                <div className="ap-pool-stats">
                  <div className="ap-pool-stat">
                    <span className="ap-pool-stat-label">Активных соединений:</span>
                    <span className="ap-pool-stat-value">{totalConnections}</span>
                  </div>
                  <div className="ap-pool-stat">
                    <span className="ap-pool-stat-label">Алгоритм:</span>
                    <span className="ap-pool-stat-value">{p.algorithm}</span>
                  </div>
                </div>

                <div className="ap-pool-section-title">Учасники</div>
                <div className="ap-pool-members">
                  {p.members.map(m => (
                    <div key={m.idPoolMember} className="ap-pool-member">
                      <span className="ap-mono ap-small">{m.hostname}:{m.port}</span>
                      <StatusBadge status={statusMap[m.hostname] || m.status || 'unknown'} />
                      <span className="ap-pool-conn-count">
                        🖥 {activeConnectionsMap[m.hostname] || 0} активных
                      </span>
                      <button className="ap-btn-icon ap-btn-icon-danger" onClick={() => removeMember(p.idPool, m.idInstance)} title="Видалити">−</button>
                    </div>
                  ))}
                  {!p.members.length && <span className="ap-muted ap-small">Немає учасників</span>}
                </div>

                <select className="ap-input ap-small" style={{ marginTop:8 }}
                  onChange={e => { if(e.target.value) { addMember(p.idPool, Number(e.target.value)); e.target.value=""; }}}>
                  <option value="">+ Додати інстанс...</option>
                  {(instances||[]).filter(i => !p.members.find(m => m.idInstance===i.idInstance))
                    .map(i => <option key={i.idInstance} value={i.idInstance}>{i.hostname}:{i.port} ({i.service_name})</option>)}
                </select>

                {p.autoscaling_rules.length > 0 && (
                  <>
                    <div className="ap-pool-section-title" style={{ marginTop:10 }}>Автомасштабування</div>
                    {p.autoscaling_rules.map(r => (
                      <div key={r.idRule} className="ap-rule-row">
                        <span className="ap-badge ap-badge-muted">{r.metric_type}</span>
                        <span className="ap-muted ap-small">{r.action === "scale_out" ? "↑" : "↓"} при {r.threshold}%</span>
                        <span className="ap-muted ap-small">cooldown {r.cooldown_seconds}s</span>
                        <span className="ap-muted ap-small">{r.min_instances}–{r.max_instances} інст.</span>
                      </div>
                    ))}
                  </>
                )}
              </div>
            );
          })}
          {!pools?.length && <div className="ap-empty">Немає пулів</div>}
        </div>
      )}

      {modal && (
        <Modal title="Новий пул" onClose={() => setModal(null)}>
          <Field label="Назва *"><Input value={form.name} onChange={e => setForm({...form, name:e.target.value})} placeholder="converter_pool" /></Field>
          <Field label="Сервіс *">
            <Select value={form.service_id} onChange={e => setForm({...form, service_id:e.target.value})}>
              <option value="">Оберіть...</option>
              {(services||[]).map(s => <option key={s.idService} value={s.idService}>{s.name}</option>)}
            </Select>
          </Field>
          <Field label="Алгоритм балансування">
            <Select value={form.algorithm} onChange={e => setForm({...form, algorithm:e.target.value})}>
              {ALGORITHMS.map(a => <option key={a}>{a}</option>)}
            </Select>
          </Field>
          <Field label="Опис"><Input value={form.description} onChange={e => setForm({...form, description:e.target.value})} /></Field>
          <div className="ap-divider">Умови масштабування (scale_out)</div>
          <Field label="Метрика">
            <Select value={form.metric_type} onChange={e => setForm({...form, metric_type:e.target.value})}>
              {METRIC_TYPES.map(m => <option key={m}>{m}</option>)}
            </Select>
          </Field>
          <Field label="Поріг (%)"><Input type="number" value={form.scale_out_threshold} onChange={e => setForm({...form, scale_out_threshold:e.target.value})} placeholder="80" /></Field>
          <Field label="Cooldown (с)"><Input type="number" value={form.cooldown_seconds} onChange={e => setForm({...form, cooldown_seconds:e.target.value})} /></Field>
          <Field label="Min інстансів"><Input type="number" value={form.min_instances} onChange={e => setForm({...form, min_instances:e.target.value})} /></Field>
          <Field label="Max інстансів"><Input type="number" value={form.max_instances} onChange={e => setForm({...form, max_instances:e.target.value})} /></Field>
          <div className="ap-modal-footer">
            <button className="ap-btn" onClick={() => setModal(null)}>Скасувати</button>
            <button className="ap-btn ap-btn-primary" onClick={save}>Створити</button>
          </div>
        </Modal>
      )}
    </div>
  );
}

// PANEL: Machines Load
function MachineLoadPanel() {
  const [loads, setLoads] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('/cfg/machine_load')
      .then(res => res.json())
      .then(setLoads)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  return (
    <div className="ap-panel">
      <SectionHeader title="Machines Load" />
      <p className="ap-description">
        Поточне навантаження на CPU та RAM кожного керованого контейнера.
      </p>
      {loading ? <Spinner /> : (
        <>
          <div className="ap-load-grid">
            {(loads || []).map((l, i) => (
              <div key={l.name} className={`ap-load-card ${l.status !== "running" ? "ap-load-card-off" : ""}`}>
                <div className="ap-load-card-header">
                  <span className="ap-load-name">{l.name}</span>
                  <StatusBadge status={l.status} />
                </div>
                <div className="ap-load-port">:{l.port}</div>
                {l.container_id && <div className="ap-muted ap-small" style={{marginBottom:10}}>{l.container_id}</div>}
                {l.status === "running" ? (
                  <>
                    <GaugeBar value={l.cpu_pct} label="CPU" color={SERVER_COLORS[i % SERVER_COLORS.length]} />
                    <GaugeBar value={l.mem_pct} label="RAM" color="#a78bfa" />
                    <div className="ap-muted ap-small" style={{marginTop:4}}>{l.mem_mb} MB</div>
                  </>
                ) : (
                  <div className="ap-empty" style={{padding:"16px 0"}}>Не запущено</div>
                )}
              </div>
            ))}
          </div>

          <div className="ap-card" style={{marginTop:16}}>
            <div className="ap-card-header"><span className="ap-section-title">CPU% по контейнерах</span></div>
            <BarChart
              data={(loads||[]).map(l => ({ label: l.name, value: l.cpu_pct }))}
              colors={SERVER_COLORS}
              unit="%"
            />
          </div>
        </>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// PANEL: Running Containers
// ─────────────────────────────────────────────────────────────────────────────
function ContainersPanel() {
  const [loads, setLoads] = useState([]);
  const [instances, setInstances] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      fetch('/cfg/machine_load').then(r => r.json()),
      fetch('/cfg/instances').then(r => r.json())
    ]).then(([loadsData, instancesData]) => {
      setLoads(loadsData);
      setInstances(instancesData);
    }).catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  const instMap = {};
  (instances || []).forEach(i => { instMap[i.container_id] = i; });

  return (
    <div className="ap-panel">
      <SectionHeader title="Running Containers" />
      <p className="ap-description">
        Статус, версії та метадані всіх керованих Docker-контейнерів.
      </p>
      {loading ? <Spinner /> : (
        <div className="ap-table-card">
          <table className="ap-table">
            <thead>
              <tr>
                <th>Контейнер</th><th>Порт</th><th>Статус</th>
                <th>Container ID</th><th>Образ</th><th>CPU%</th><th>RAM</th>
              </tr>
            </thead>
            <tbody>
              {(loads || []).map(l => {
                const inst = instMap[l.name] || instMap[l.container_id] || {};
                return (
                  <tr key={l.name}>
                    <td className="ap-mono ap-fw">{l.name}</td>
                    <td className="ap-mono">{l.port}</td>
                    <td><StatusBadge status={l.status} /></td>
                    <td className="ap-mono ap-small ap-muted">{l.container_id || "—"}</td>
                    <td className="ap-mono ap-small">{inst.docker_image || <span className="ap-muted">tr23malyarchuk/pa-tr23malyarchuk:latest</span>}</td>
                    <td>
                      {l.status === "running" ? (
                        <span className={`ap-mono ${l.cpu_pct > 80 ? "ap-danger" : l.cpu_pct > 50 ? "ap-warn" : ""}`}>
                          {l.cpu_pct.toFixed(2)}%
                        </span>
                      ) : "—"}
                    </td>
                    <td className="ap-mono">{l.status === "running" ? `${l.mem_mb} MB` : "—"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// PANEL: Service Latency
function LatencyPanel() {
  const latFn   = useCallback(() => apiFetch("/cfg/latency").then(r => r.json()), []);
  const statsFn = useCallback(() => apiFetch("/stats").then(r => r.json()), []);
  const { data: latency, loading } = usePoll(latFn, 5000);
  const { data: statsData } = usePoll(statsFn, 5000);

  const getRequestCountByEndpoint = () => {
    return (latency || []).map(r => ({ 
      label: r.endpoint.replace("/", ""), 
      value: r.total || 0 
    }));
  };

  return (
    <div className="ap-panel">
      <SectionHeader title="Service Latency" />
      <p className="ap-description">
        Середній, мінімальний та максимальний час виконання запитів по endpoint'ах.
        Базується на останніх 200 записах у БД. Важливо для оцінки задоволеності користувача.
      </p>
      {loading ? <Spinner /> : (
        <>
          <div className="ap-latency-row">
            <div className="ap-card ap-chart-card" style={{ flex: 2, maxWidth: '55%' }}>
              <div className="ap-card-header">
                <span className="ap-section-title">📊 Avg latency (мс) по endpoint</span>
              </div>
              <BarChart
                data={(latency||[]).map(r => ({ label: r.endpoint.replace("/",""), value: r.avg_ms || 0 }))}
                colors={["#3b82f6","#22c55e","#f59e0b","#ef4444"]}
                unit="ms"
              />
            </div>
            <div className="ap-card ap-chart-card" style={{ flex: 1 }}>
              <div className="ap-card-header">
                <span className="ap-section-title">⏱ Avg latency по серверах (с)</span>
              </div>
              <BarChart
                data={(statsData?.by_server||[]).map(r => ({ label: r.server, value: r.avg_s || 0 }))}
                colors={["#f59e0b"]}
                unit="s"
              />
            </div>
          </div>

          <div className="ap-latency-row" style={{ marginTop: 16 }}>
            <div className="ap-card ap-chart-card" style={{ flex: 1 }}>
              <div className="ap-card-header">
                <span className="ap-section-title">📈 Кількість запитів по сервісах</span>
              </div>
              <BarChart
                data={(latency||[]).map(r => ({ label: r.endpoint.replace("/",""), value: r.total || 0 }))}
                colors={["#22c55e","#3b82f6","#f59e0b","#a78bfa"]}
                unit=""
              />
              <div className="ap-muted ap-small" style={{ textAlign: "center", marginTop: 8 }}>
                Найбільше запитів отримує PDF→PNG (найбільш вимогливий до CPU)
              </div>
            </div>
            <div className="ap-card ap-chart-card" style={{ flex: 1 }}>
              <div className="ap-card-header">
                <span className="ap-section-title">🎯 Успішність запитів (%)</span>
              </div>
              <BarChart
                data={(latency||[]).map(r => ({ label: r.endpoint.replace("/",""), value: r.success_rate || 0 }))}
                colors={["#22c55e","#3b82f6","#f59e0b","#ef4444"]}
                unit="%"
              />
            </div>
          </div>

          <div className="ap-table-card" style={{ marginTop: 16 }}>
            <table className="ap-table">
              <thead>
                <tr>
                  <th>Endpoint</th><th>Запитів</th>
                  <th>Avg, мс</th><th>Min, мс</th><th>Max, мс</th><th>Success%</th>
                </tr>
              </thead>
              <tbody>
                {(latency||[]).map((r, i) => (
                  <tr key={i}>
                    <td className="ap-mono">{r.endpoint}</td>
                    <td className="ap-mono">{r.total}</td>
                    <td className="ap-mono" style={{color: r.avg_ms > 5000 ? "#ef4444" : r.avg_ms > 1000 ? "#f59e0b" : "#22c55e"}}>{r.avg_ms}</td>
                    <td className="ap-mono">{r.min_ms}</td>
                    <td className="ap-mono">{r.max_ms}</td>
                    <td>
                      <span className={`ap-badge ${r.success_rate >= 95 ? "ap-badge-ok" : r.success_rate >= 80 ? "ap-badge-algo" : "ap-badge-fail"}`}>
                        {r.success_rate}%
                      </span>
                    </td>
                  </tr>
                ))}
                {!latency?.length && <tr><td colSpan={6} className="ap-empty">Немає даних. Зробіть кілька конвертацій.</td></tr>}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}

// PANEL: Balancing Strategies
function BalancingStrategies() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  
  useEffect(() => {
    fetch('http://localhost:8000/stats/by-algorithm')
      .then(res => res.json())
      .then(setData)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);
  
  if (loading) return <Spinner />;
  if (!data?.algorithms?.length) {
    return (
      <div className="ap-panel">
        <SectionHeader title="Balancing Strategies" />
        <p className="ap-description">
          Немає даних. Зробіть кілька запитів з різними алгоритмами:
          <code>LB_ALG=round_robin k6 run backend/load_test.js</code>
        </p>
      </div>
    );
  }
  
  const colors = ["#3b82f6", "#22c55e", "#f59e0b", "#a78bfa", "#ef4444"];
  
  return (
    <div className="ap-panel">
      <SectionHeader title="Balancing Strategies" />
      
      <div className="ap-latency-row">
        <div className="ap-card ap-chart-card" style={{ flex: 1 }}>
          <div className="ap-card-header">
            <span className="ap-section-title">📊 Кількість запитів по алгоритмах</span>
          </div>
          <BarChart
            data={data.algorithms.map(a => ({ label: a.name, value: a.total_requests }))}
            colors={colors}
            unit=""
          />
        </div>
        
        <div className="ap-card ap-chart-card" style={{ flex: 1 }}>
          <div className="ap-card-header">
            <span className="ap-section-title">⏱ Середній час відповіді (с)</span>
          </div>
          <BarChart
            data={data.algorithms.map(a => ({ label: a.name, value: a.avg_response_time }))}
            colors={colors}
            unit="s"
          />
        </div>
      </div>
      
      <div className="ap-table-card">
        <table className="ap-table">
          <thead>
            <tr><th>Алгоритм</th><th>Запитів</th><th>Ср. час (с)</th><th>Серверів</th></tr>
          </thead>
          <tbody>
            {data.algorithms.map(a => (
              <tr key={a.name}>
                <td className="ap-mono">{a.name}</td>
                <td>{a.total_requests}</td>
                <td>{a.avg_response_time}</td>
                <td>{a.servers_used}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ROOT: AdminPanel
const NAV_ITEMS = [
  { id: "services",   label: "Services & Images", icon: "⬡" },
  { id: "machines",   label: "Machines",           icon: "⬢" },
  { id: "pools",      label: "Pools",              icon: "◈" },
  { id: "strategies", label: "Balancing Strategies", icon: "⚖️" },  // ← НОВАЯ
  { id: "load",       label: "Machines Load",      icon: "◉" },
  { id: "containers", label: "Running Containers", icon: "◎" },
  { id: "latency",    label: "Service Latency",    icon: "◈" },
];

export default function AdminPanel() {
  const [tab, setTab] = useState("services");

  const serversFn = useCallback(() => apiFetch("/servers").then(r => r.json()), []);
  const { data: serversData, error: connError } = usePoll(serversFn, 5000);
  const connected = !connError && serversData !== null;

  return (
    <div className="ap-root">
      <div className="ap-header">
        <div className="ap-header-left">
          <div className={`ap-dot ${connected ? "ap-dot-green" : "ap-dot-red"}`} />
          <span className="ap-header-status">
            {connected
              ? `Configuration Server · ${serversData?.healthy_count ?? 0} live`
              : "Load Balancer unavailable"}
          </span>
        </div>
        <span className="ap-header-time" suppressHydrationWarning>
          {new Date().toLocaleTimeString("uk-UA")}
        </span>
      </div>

      <div className="ap-layout">
        <nav className="ap-sidebar">
          {NAV_ITEMS.map(n => (
            <button
              key={n.id}
              className={`ap-nav-item ${tab === n.id ? "ap-nav-active" : ""}`}
              onClick={() => setTab(n.id)}
            >
              <span className="ap-nav-icon">{n.icon}</span>
              <span className="ap-nav-label">{n.label}</span>
            </button>
          ))}
        </nav>

        <div className="ap-content">
          {tab === "services"   && <ServicesPanel />}
          {tab === "machines"   && <MachinesPanel />}
          {tab === "pools"      && <PoolsPanel />}
          {tab === "strategies" && <BalancingStrategies />}  {/* ← НОВАЯ */}
          {tab === "load"       && <MachineLoadPanel />}
          {tab === "containers" && <ContainersPanel />}
          {tab === "latency"    && <LatencyPanel />}
        </div>
      </div>
    </div>
  );
}
