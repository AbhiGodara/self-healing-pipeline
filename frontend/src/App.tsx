import { useState, useEffect, useCallback } from "react";
import axios from "axios";
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis,
  CartesianGrid, Tooltip, ResponsiveContainer, Legend
} from "recharts";
import {
  Activity, AlertTriangle, CheckCircle, Database,
  RefreshCw, Zap, Clock, Shield
} from "lucide-react";

const API = "http://localhost:8000";

// ── TYPES ─────────────────────────────────────────────────────
interface Health {
  status: string;
  total_rows: number;
  good_rows: number;
  bad_rows: number;
  schema_ok: boolean;
  latest_dag_run: string;
  timestamp: string;
}

interface AuditEntry {
  id: string;
  agent_name: string;
  action_taken: string;
  reasoning: string;
  success: boolean;
  created_at: string;
}

interface DagRun {
  dag_id: string;
  run_id: string;
  state: string;
  execution_date: string;
}

interface RowStat {
  date: string;
  total: number;
  good: number;
  bad: number;
}

interface HealStatus {
  running: boolean;
  last_result: {
    status: string;
    fix_action: string;
    bad_rows: number;
    good_rows: number;
    timestamp: string;
  } | null;
}

// ── HELPERS ───────────────────────────────────────────────────
const stateColor = (state: string) => {
  if (state === "success") return "#22c55e";
  if (state === "failed") return "#ef4444";
  if (state === "running") return "#3b82f6";
  return "#f59e0b";
};

const agentColor = (agent: string) => {
  if (agent === "fix_agent") return "#8b5cf6";
  if (agent === "escalation_agent") return "#ef4444";
  return "#3b82f6";
};

// ── COMPONENTS ────────────────────────────────────────────────

function StatCard({ icon, label, value, sub, color }: {
  icon: React.ReactNode;
  label: string;
  value: string | number;
  sub?: string;
  color: string;
}) {
  return (
    <div style={{
      background: "#1e293b", borderRadius: 12, padding: "20px 24px",
      borderLeft: `4px solid ${color}`, flex: 1, minWidth: 160
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
        <span style={{ color }}>{icon}</span>
        <span style={{ color: "#94a3b8", fontSize: 13 }}>{label}</span>
      </div>
      <div style={{ color: "#f1f5f9", fontSize: 28, fontWeight: 700 }}>{value}</div>
      {sub && <div style={{ color: "#64748b", fontSize: 12, marginTop: 4 }}>{sub}</div>}
    </div>
  );
}

function Badge({ state }: { state: string }) {
  return (
    <span style={{
      background: stateColor(state) + "22",
      color: stateColor(state),
      border: `1px solid ${stateColor(state)}44`,
      borderRadius: 6, padding: "2px 10px", fontSize: 12, fontWeight: 600
    }}>
      {state}
    </span>
  );
}

// ── MAIN APP ──────────────────────────────────────────────────
export default function App() {
  const [health, setHealth] = useState<Health | null>(null);
  const [audit, setAudit] = useState<AuditEntry[]>([]);
  const [dagRuns, setDagRuns] = useState<DagRun[]>([]);
  const [rowStats, setRowStats] = useState<RowStat[]>([]);
  const [healStatus, setHealStatus] = useState<HealStatus>({ running: false, last_result: null });
  const [loading, setLoading] = useState(false);
  const [lastRefresh, setLastRefresh] = useState<string>("");

  const fetchAll = useCallback(async () => {
    try {
      const [h, a, d, r, hs] = await Promise.all([
        axios.get(`${API}/health`),
        axios.get(`${API}/audit`),
        axios.get(`${API}/dag-runs`),
        axios.get(`${API}/rows`),
        axios.get(`${API}/heal/status`),
      ]);
      setHealth(h.data);
      setAudit(a.data.entries);
      setDagRuns(d.data.runs);
      setRowStats(r.data.rows.reverse());
      setHealStatus(hs.data);
      setLastRefresh(new Date().toLocaleTimeString());
    } catch (e) {
      console.error("Fetch error", e);
    }
  }, []);

  useEffect(() => {
    fetchAll();
    const interval = setInterval(fetchAll, 10000);
    return () => clearInterval(interval);
  }, [fetchAll]);

  const injectFailure = async () => {
    setLoading(true);
    await axios.post(`${API}/inject-failure`);
    setTimeout(() => { fetchAll(); setLoading(false); }, 2000);
  };

  const triggerHeal = async () => {
    setLoading(true);
    await axios.post(`${API}/heal`, { dag_id: "data_ingestion_pipeline" });
    setTimeout(() => { fetchAll(); setLoading(false); }, 2000);
  };

  const isHealthy = health?.status === "healthy";

  return (
    <div style={{
      minHeight: "100vh", background: "#0f172a",
      color: "#f1f5f9", fontFamily: "'Inter', sans-serif", padding: 24
    }}>

      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 28 }}>
        <div>
          <h1 style={{ margin: 0, fontSize: 24, fontWeight: 700, color: "#f1f5f9" }}>
            🛡 Self-Healing Pipeline
          </h1>
          <p style={{ margin: "4px 0 0", color: "#64748b", fontSize: 13 }}>
            Real-time monitoring · Auto-remediation · Audit trail
          </p>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <span style={{ color: "#475569", fontSize: 12 }}>
            <Clock size={12} style={{ marginRight: 4 }} />
            {lastRefresh}
          </span>
          <button onClick={fetchAll} style={{
            background: "#1e293b", border: "1px solid #334155",
            color: "#94a3b8", borderRadius: 8, padding: "8px 14px",
            cursor: "pointer", display: "flex", alignItems: "center", gap: 6, fontSize: 13
          }}>
            <RefreshCw size={14} /> Refresh
          </button>
        </div>
      </div>

      {/* Status Banner */}
      <div style={{
        background: isHealthy ? "#14532d33" : "#7f1d1d33",
        border: `1px solid ${isHealthy ? "#22c55e44" : "#ef444444"}`,
        borderRadius: 12, padding: "14px 20px", marginBottom: 24,
        display: "flex", alignItems: "center", justifyContent: "space-between"
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          {isHealthy
            ? <CheckCircle size={20} color="#22c55e" />
            : <AlertTriangle size={20} color="#ef4444" />}
          <span style={{ fontWeight: 600, color: isHealthy ? "#22c55e" : "#ef4444" }}>
            {isHealthy ? "Pipeline Healthy" : "Incident Detected"}
          </span>
          <span style={{ color: "#64748b", fontSize: 13 }}>
            {health?.bad_rows} bad rows · Latest run: {health?.latest_dag_run}
          </span>
        </div>
        <div style={{ display: "flex", gap: 10 }}>
          <button onClick={injectFailure} disabled={loading} style={{
            background: "#7f1d1d", border: "1px solid #ef444466",
            color: "#fca5a5", borderRadius: 8, padding: "8px 16px",
            cursor: "pointer", fontSize: 13, fontWeight: 600
          }}>
            💣 Inject Failure
          </button>
          <button onClick={triggerHeal} disabled={loading || healStatus.running} style={{
            background: healStatus.running ? "#1e3a5f" : "#14532d",
            border: `1px solid ${healStatus.running ? "#3b82f666" : "#22c55e66"}`,
            color: healStatus.running ? "#93c5fd" : "#86efac",
            borderRadius: 8, padding: "8px 16px",
            cursor: "pointer", fontSize: 13, fontWeight: 600
          }}>
            {healStatus.running ? "⚙ Healing..." : "⚡ Trigger Heal"}
          </button>
        </div>
      </div>

      {/* Stat Cards */}
      <div style={{ display: "flex", gap: 16, marginBottom: 24, flexWrap: "wrap" }}>
        <StatCard icon={<Database size={18} />} label="Total Rows" value={health?.total_rows ?? "-"} color="#3b82f6" />
        <StatCard icon={<CheckCircle size={18} />} label="Good Rows" value={health?.good_rows ?? "-"} color="#22c55e" />
        <StatCard icon={<AlertTriangle size={18} />} label="Bad Rows" value={health?.bad_rows ?? "-"} color="#ef4444" />
        <StatCard icon={<Shield size={18} />} label="Schema" value={health?.schema_ok ? "OK" : "Error"} color={health?.schema_ok ? "#22c55e" : "#ef4444"} />
        <StatCard icon={<Activity size={18} />} label="DAG State" value={health?.latest_dag_run ?? "-"} color={stateColor(health?.latest_dag_run ?? "")} />
      </div>

      {/* Charts Row */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 24 }}>

        {/* Row Stats Chart */}
        <div style={{ background: "#1e293b", borderRadius: 12, padding: 20 }}>
          <h3 style={{ margin: "0 0 16px", fontSize: 14, color: "#94a3b8", fontWeight: 600 }}>
            DAILY ROW QUALITY
          </h3>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={rowStats}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="date" tick={{ fill: "#64748b", fontSize: 11 }} />
              <YAxis tick={{ fill: "#64748b", fontSize: 11 }} />
              <Tooltip contentStyle={{ background: "#0f172a", border: "1px solid #334155", borderRadius: 8 }} />
              <Legend />
              <Bar dataKey="good" fill="#22c55e" radius={[4, 4, 0, 0]} />
              <Bar dataKey="bad" fill="#ef4444" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* DAG Runs */}
        <div style={{ background: "#1e293b", borderRadius: 12, padding: 20 }}>
          <h3 style={{ margin: "0 0 16px", fontSize: 14, color: "#94a3b8", fontWeight: 600 }}>
            RECENT DAG RUNS
          </h3>
          <div style={{ display: "flex", flexDirection: "column", gap: 8, maxHeight: 220, overflowY: "auto" }}>
            {dagRuns.map((run, i) => (
              <div key={i} style={{
                display: "flex", justifyContent: "space-between", alignItems: "center",
                background: "#0f172a", borderRadius: 8, padding: "10px 14px"
              }}>
                <span style={{ color: "#64748b", fontSize: 11, fontFamily: "monospace" }}>
                  {run.run_id.slice(0, 30)}...
                </span>
                <Badge state={run.state} />
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Audit Log */}
      <div style={{ background: "#1e293b", borderRadius: 12, padding: 20 }}>
        <h3 style={{ margin: "0 0 16px", fontSize: 14, color: "#94a3b8", fontWeight: 600 }}>
          AGENT AUDIT LOG
        </h3>
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {audit.length === 0 && (
            <div style={{ color: "#475569", fontSize: 13, textAlign: "center", padding: 20 }}>
              No audit entries yet. Trigger a heal cycle to see agent actions here.
            </div>
          )}
          {audit.map((entry, i) => (
            <div key={i} style={{
              display: "grid",
              gridTemplateColumns: "140px 120px 1fr 80px 160px",
              gap: 12, alignItems: "center",
              background: "#0f172a", borderRadius: 8, padding: "12px 16px",
              borderLeft: `3px solid ${agentColor(entry.agent_name)}`
            }}>
              <span style={{ color: agentColor(entry.agent_name), fontSize: 12, fontWeight: 600 }}>
                {entry.agent_name}
              </span>
              <span style={{ color: "#cbd5e1", fontSize: 12 }}>{entry.action_taken}</span>
              <span style={{ color: "#64748b", fontSize: 11 }}>{entry.reasoning}</span>
              <span style={{
                color: entry.success ? "#22c55e" : "#ef4444",
                fontSize: 12, fontWeight: 600
              }}>
                {entry.success ? "✓ success" : "✗ failed"}
              </span>
              <span style={{ color: "#475569", fontSize: 11 }}>{entry.created_at}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Last Heal Result */}
      {healStatus.last_result && (
        <div style={{
          background: "#1e293b", borderRadius: 12, padding: 20, marginTop: 16,
          borderLeft: "4px solid #8b5cf6"
        }}>
          <h3 style={{ margin: "0 0 12px", fontSize: 14, color: "#94a3b8", fontWeight: 600 }}>
            LAST HEAL RESULT
          </h3>
          <div style={{ display: "flex", gap: 24, flexWrap: "wrap" }}>
            {Object.entries(healStatus.last_result).map(([k, v]) => (
              <div key={k}>
                <div style={{ color: "#475569", fontSize: 11, marginBottom: 2 }}>{k}</div>
                <div style={{ color: "#e2e8f0", fontSize: 14, fontWeight: 600 }}>{String(v)}</div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}