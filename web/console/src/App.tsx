import { useCallback, useEffect, useMemo, useState } from "react";
import "./styles.css";

type Settings = { apiBase: string; tenant: string; token: string; workerMetrics: string; lokiBase: string; lokiQuery: string };
type WorkerSnapshot = { url: string; name: string; online: boolean; checkedAt: number; inFlight: number | null; activeKeys: number | null; shards: number | null; executions: Array<{ ruleSet: string; status: string; count: number }> ; error?: string };
type NavKey = "overview" | "api" | "codegen" | "workers" | "rules" | "logs";

const DEFAULT_SETTINGS: Settings = { apiBase: "/api", tenant: "development-tenant", token: "", workerMetrics: "", lokiBase: "", lokiQuery: '{service_name="flowrule-worker"}' };
const NAV: Array<{ id: NavKey; label: string; icon: string }> = [
  { id: "overview", label: "Overview", icon: "◫" }, { id: "api", label: "API explorer", icon: "⌘" },
  { id: "codegen", label: "Code generation", icon: "⟐" }, { id: "workers", label: "Workers & rules", icon: "▦" },
  { id: "rules", label: "Rule management", icon: "◎" }, { id: "logs", label: "Logs", icon: "≋" },
];
const ROUTES = [
  { method: "GET", path: "/health", description: "API liveness" },
  { method: "GET", path: "/v1/contracts", description: "List registered contracts" },
  { method: "POST", path: "/v1/contracts", description: "Register a contract" },
  { method: "GET", path: "/v1/rules/active", description: "List active rules for the tenant" },
  { method: "GET", path: "/v1/rules/{ruleSet}/revisions", description: "List revisions for a rule set" },
  { method: "POST", path: "/v1/rules/{ruleSet}/revisions", description: "Compile and activate a rule revision" },
  { method: "GET", path: "/v1/workflows", description: "List workflows" },
  { method: "GET", path: "/v1/workflows/definitions", description: "List workflow definitions" },
  { method: "GET", path: "/v1/batches", description: "List batches" },
  { method: "GET", path: "/v1/quarantine", description: "List quarantined work" },
  { method: "GET", path: "/v1/scheduled-events", description: "List scheduled events" },
] as const;

function readSettings(): Settings {
  try { return { ...DEFAULT_SETTINGS, ...JSON.parse(localStorage.getItem("flowrule-console-settings") || "{}") }; }
  catch { return DEFAULT_SETTINGS; }
}
function normalizeBase(value: string) { return value.trim().replace(/\/+$/, ""); }
function unwrap(value: any): any { return value && typeof value === "object" && "data" in value ? value.data : value; }
function asArray(value: any): any[] { const v = unwrap(value); if (Array.isArray(v)) return v; if (!v || typeof v !== "object") return []; for (const key of ["active_rules", "revisions", "contracts", "schemas", "workflows", "events", "batches", "items", "data"]) if (Array.isArray(v[key])) return v[key]; return []; }
function fmt(value: unknown) { return typeof value === "string" ? value : JSON.stringify(value, null, 2) ?? String(value); }
function metricValue(text: string, name: string): number | null { const line = text.split("\n").find((row) => row.startsWith(name + " ") || row.startsWith(name + "{")); if (!line) return null; const n = Number(line.trim().split(/\s+/).pop()); return Number.isFinite(n) ? n : null; }
function labeledMetrics(text: string, metric: string) {
  return text.split("\n").flatMap((line) => {
    if (!line.startsWith(metric + "{")) return [];
    const match = line.match(/^([^\s]+)\s+([\d.eE+-]+)$/); if (!match) return [];
    const labels: Record<string, string> = {}; for (const part of (line.match(/\{(.*?)\}/)?.[1] || "").matchAll(/([a-zA-Z_][\w]*)="((?:\\.|[^"])*)"/g)) labels[part[1]] = part[2];
    return [{ labels, count: Number(match[2]) }];
  });
}
async function readResponse(response: Response) {
  const text = await response.text(); let payload: any = text;
  try { payload = text ? JSON.parse(text) : null; } catch { /* text response */ }
  if (!response.ok) throw new Error(payload?.error?.message || payload?.message || payload?.error || `${response.status} ${response.statusText}: ${String(text).slice(0, 240)}`);
  return payload;
}
async function apiRequest(settings: Settings, path: string, init: RequestInit = {}) {
  const headers = new Headers(init.headers);
  headers.set("X-Tenant-ID", settings.tenant.trim());
  if (settings.token.trim()) headers.set("Authorization", `Bearer ${settings.token.trim()}`);
  if (init.body && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");
  return readResponse(await fetch(normalizeBase(settings.apiBase) + path, { ...init, headers }));
}
function Status({ value }: { value: string }) { const key = value.toLowerCase().replace(/[^a-z0-9]+/g, "-"); return <span className={`status status-${key}`}><i />{value}</span>; }
function MetricCard({ label, value, hint, icon }: { label: string; value: string | number; hint: string; icon: string }) { return <article className="metric-card"><div className="metric-top"><span>{label}</span><span className="metric-icon">{icon}</span></div><strong>{value}</strong><small>{hint}</small></article>; }
function Empty({ title, detail }: { title: string; detail: string }) { return <div className="empty-state"><div className="empty-icon">⌁</div><strong>{title}</strong><p>{detail}</p></div>; }

export default function App() {
  const [settings, setSettings] = useState<Settings>(readSettings);
  const [nav, setNav] = useState<NavKey>("overview");
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [health, setHealth] = useState<"checking" | "online" | "offline">("checking");
  const [activeRules, setActiveRules] = useState<any[]>([]);
  const [contracts, setContracts] = useState<any[]>([]);
  const [workers, setWorkers] = useState<WorkerSnapshot[]>([]);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);
  const [apiPath, setApiPath] = useState("/v1/rules/active");
  const [apiMethod, setApiMethod] = useState("GET");
  const [apiBody, setApiBody] = useState('{"rule_set":"transaction.initiated","revision":1}');
        const text = await response.text(); if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const execs = labeledMetrics(text, "flowrule_executions_total");
        const grouped = new Map<string, number>();
        execs.forEach((row) => { const key = row.labels.rule_set || "unknown"; grouped.set(key, (grouped.get(key) || 0) + row.count); });
        const shardRows = labeledMetrics(text, "flowrule_shard_ownership");
        return { url, name: new URL(url).hostname + (endpoints.length > 1 ? ` · worker ${index + 1}` : ""), online: true, checkedAt: Date.now(), inFlight: metricValue(text, "flowrule_global_in_flight"), activeKeys: metricValue(text, "flowrule_active_keys"), shards: shardRows.filter((row) => row.count > 0).length, executions: [...grouped].map(([ruleSet, count]) => ({ ruleSet, status: "observed", count })).sort((a,b) => b.count-a.count) };
      } catch (e) { return { url, name: url, online: false, checkedAt: Date.now(), inFlight: null, activeKeys: null, shards: null, executions: [], error: e instanceof Error ? e.message : String(e) }; }
    }));
    setWorkers(workerData); setLastRefresh(new Date()); setLoading(false);
  }, [settings]);
  useEffect(() => { void refresh(); const id = window.setInterval(() => void refresh(), 10000); return () => window.clearInterval(id); }, [refresh]);

  const ruleCount = activeRules.length;
  const executionTotal = workers.reduce((sum, worker) => sum + worker.executions.reduce((n, item) => n + item.count, 0), 0);
  const onlineWorkers = workers.filter((worker) => worker.online).length;
  const selectedContractParts = selectedContract.split("@");
  const selectedContractName = selectedContractParts[0] || "";
  const selectedContractVersion = selectedContractParts[1] || "";

  const runApi = async () => {
    setApiBusy(true); setApiResult("");
    try { const body = apiMethod === "GET" || apiMethod === "DELETE" ? undefined : JSON.stringify(JSON.parse(apiBody)); const result = await apiRequest(settings, apiPath.startsWith("/") ? apiPath : "/" + apiPath, { method: apiMethod, body }); setApiResult(JSON.stringify(result, null, 2)); }
    catch (e) { setApiResult(e instanceof Error ? e.message : String(e)); } finally { setApiBusy(false); }
  };
  const createContract = async () => {
    setCodegenBusy(true); setGenerated(null);
    try {
      const payload = JSON.parse(contractSchema);
      payload.name = contractName.trim() || payload.name; payload.version = contractVersion.trim() || payload.version;
      const result = await apiRequest(settings, "/v1/contracts", { method: "POST", body: JSON.stringify(payload) });
      setGenerated({ operation: "Contract registered", result }); await refresh();
    } catch (e) { setGenerated({ error: e instanceof Error ? e.message : String(e) }); } finally { setCodegenBusy(false); }
  };
  const generate = async () => {
    if (!selectedContractName || !selectedContractVersion) return;
    setCodegenBusy(true); setGenerated(null);
    try { const result = await apiRequest(settings, `/v1/contracts/${encodeURIComponent(selectedContractName)}/${encodeURIComponent(selectedContractVersion)}/generate`, { method: "POST", body: JSON.stringify({ language }) }); setGenerated(result); }
    catch (e) { setGenerated({ error: e instanceof Error ? e.message : String(e) }); } finally { setCodegenBusy(false); }
  };
  const deployRule = async () => {
    setLoading(true); setRuleResult("");
    try { if (!ruleSet.trim()) throw new Error("Enter the rule set used by the source document."); const result = await apiRequest(settings, `/v1/rules/${encodeURIComponent(ruleSet.trim())}/revisions`, { method: "POST", headers: { "Content-Type": "application/yaml" }, body: ruleSource }); setRuleResult(JSON.stringify(result, null, 2)); await refresh(); }
    catch (e) { setRuleResult(e instanceof Error ? e.message : String(e)); } finally { setLoading(false); }
  };
  const deactivateRule = async (item: any) => {
    const id = item.rule_id || item.rule_set || item.ruleSet || item.id;
    const revision = item.revision;
    if (!id) return;
    if (!window.confirm(`Deactivate rule set "${id}"?`)) return;
    try { await apiRequest(settings, `/v1/rules/${encodeURIComponent(id)}/revisions/${encodeURIComponent(String(revision || 1))}`, { method: "DELETE" }); await refresh(); }
    catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  const loadLogs = async () => {
    if (!settings.lokiBase.trim()) { setLogsError("Configure a Loki-compatible query URL in Settings to browse runtime logs. The API currently does not expose a log-tail endpoint."); setLogs([]); return; }
    setLogsBusy(true); setLogsError("");
    try {
      const end = Date.now() * 1_000_000; const start = end - 60 * 60 * 1_000_000_000;
      const url = new URL(normalizeBase(settings.lokiBase) + "/loki/api/v1/query_range");
      url.searchParams.set("query", settings.lokiQuery || '{service_name="flowrule-worker"}'); url.searchParams.set("start", String(start)); url.searchParams.set("end", String(end)); url.searchParams.set("limit", "200"); url.searchParams.set("direction", "backward");
      const result = await readResponse(await fetch(url.toString(), { headers: settings.token ? { Authorization: `Bearer ${settings.token}` } : {} }));
      const streams = result?.data?.result || [];
      setLogs(streams.flatMap((stream: any) => (stream.values || []).map((entry: any[]) => ({ timestamp: new Date(Number(entry[0]) / 1_000_000).toLocaleString(), labels: stream.stream, line: entry[1] }))).sort((a: any,b: any) => b.timestamp.localeCompare(a.timestamp)));
    } catch (e) { setLogsError(e instanceof Error ? e.message : String(e)); } finally { setLogsBusy(false); }
  };

  return <div className="app-shell">
    <aside className="sidebar">
      <div className="brand"><div className="brand-mark">F</div><div><strong>FlowRule</strong><small>CONTROL PLANE</small></div></div>
      <div className="workspace-label">WORKSPACE</div>
      <div className="workspace"><span className="workspace-dot" />{settings.tenant || "No tenant"}<span className="chevron">⌄</span></div>
      <nav>{NAV.map(item => <button key={item.id} className={nav === item.id ? "nav-item selected" : "nav-item"} onClick={() => setNav(item.id)}><span className="nav-icon">{item.icon}</span>{item.label}{item.id === "workers" && workers.length > 0 && <span className="nav-count">{onlineWorkers}/{workers.length}</span>}</button>)}</nav>
      <div className="sidebar-bottom"><div className="connection-line"><span className={health === "online" ? "dot green" : health === "offline" ? "dot red" : "dot amber"} />API {health}</div><button className="settings-link" onClick={() => setSettingsOpen(true)}>⚙ Settings</button><div className="version-label">FlowRule Console · React</div></div>
    </aside>
    <main className="main-area">
      <header className="topbar"><div><div className="breadcrumbs">FlowRule <span>/</span> {NAV.find(item => item.id === nav)?.label}</div><h1>{({ overview: "Runtime overview", api: "API explorer", codegen: "Contract code generation", workers: "Workers & rules", rules: "Rule management", logs: "Runtime logs" } as Record<NavKey,string>)[nav]}</h1></div><div className="top-actions"><span className="live-indicator"><i /> AUTO REFRESH · 10S</span><button className="icon-button" title="Refresh data" onClick={() => void refresh()} disabled={loading}>↻</button><button className="button secondary" onClick={() => setSettingsOpen(true)}>⚙ Settings</button><div className="avatar">FR</div></div></header>
      {error && <div className="alert error"><strong>API connection issue</strong><span>{error}</span><button onClick={() => setError("")}>×</button></div>}
      {nav === "overview" && <section className="page-content">
        <div className="welcome-row"><div><div className="eyebrow">SYSTEM SNAPSHOT</div><h2>Runtime at a glance</h2><p>Rules, workers, and contract generation in one place.</p></div><div className="updated">Last refreshed <strong>{lastRefresh?.toLocaleTimeString() || "—"}</strong></div></div>
        <div className="metric-grid"><MetricCard label="API status" value={health === "online" ? "Online" : health === "offline" ? "Offline" : "Checking"} hint={normalizeBase(settings.apiBase)} icon="⌘"/><MetricCard label="Active rules" value={ruleCount} hint="From the tenant-scoped rules API" icon="◎"/><MetricCard label="Workers online" value={workers.length ? `${onlineWorkers}/${workers.length}` : "Not configured"} hint="Worker Prometheus endpoints" icon="▦"/><MetricCard label="Rule executions" value={workers.length ? executionTotal.toLocaleString() : "—"} hint="Cumulative counters from configured workers" icon="↗"/></div>
        <div className="content-grid"><section className="panel"><div className="panel-heading"><div><h3>Worker fleet</h3><p>Process-level telemetry, refreshed every 10 seconds</p></div><button className="text-button" onClick={() => setNav("workers")}>View workers ↗</button></div>{workers.length === 0 ? <Empty title="No worker endpoints configured" detail="Open Settings and add one private /metrics URL per worker process. The UI does not invent worker health."/> : <div className="worker-list">{workers.slice(0,4).map(worker => <div className="worker-row" key={worker.url}><div className="worker-symbol">▦</div><div className="worker-main"><strong>{worker.name}</strong><small>{worker.url}</small></div><Status value={worker.online ? "Online" : "Unreachable"}/><div className="worker-stat"><strong>{worker.inFlight ?? "—"}</strong><small>in flight</small></div><div className="worker-stat"><strong>{worker.shards ?? "—"}</strong><small>shards owned</small></div></div>)}</div>}</section>
        <section className="panel"><div className="panel-heading"><div><h3>Active rule sets</h3><p>Current activation state from the API</p></div><button className="text-button" onClick={() => setNav("rules")}>Manage ↗</button></div>{activeRules.length === 0 ? <Empty title="No active rules returned" detail={health === "online" ? "No active rules were found for this tenant." : "Connect to the API to load active rules."}/> : <div className="rule-mini-list">{activeRules.slice(0,6).map((rule,i) => <div className="rule-mini" key={rule.rule_id || rule.rule_set || rule.id || i}><div className="rule-symbol">R</div><div className="rule-main"><strong>{rule.rule_id || rule.rule_set || rule.ruleSet || rule.id || `Rule set ${i+1}`}</strong><small>Revision {rule.revision ?? rule.active_revision ?? "—"}</small></div><Status value={rule.status || "Active"}/></div>)}</div>}</section></div>
        <section className="panel"><div className="panel-heading"><div><h3>Quick actions</h3><p>Jump directly to a common operation</p></div></div><div className="quick-grid"><button onClick={() => setNav("api")}><span>⌘</span><strong>Explore API</strong><small>Inspect routes and send requests</small><b>→</b></button><button onClick={() => setNav("codegen")}><span>⟐</span><strong>Generate a client</strong><small>Go, Java, Protobuf, JSON Schema</small><b>→</b></button><button onClick={() => setNav("rules")}><span>◎</span><strong>Deploy a rule</strong><small>Validate and activate a revision</small><b>→</b></button><button onClick={() => setNav("logs")}><span>≋</span><strong>Inspect logs</strong><small>Query your configured Loki source</small><b>→</b></button></div></section>
      </section>}
      {nav === "api" && <section className="page-content"><div className="page-intro"><h2>Explore the management API</h2><p>Send requests against the running FlowRule API. Mutating requests use your configured tenant and token.</p></div><div className="api-layout"><section className="panel endpoint-panel"><div className="panel-heading"><div><h3>Known endpoints</h3><p>Routes registered by the current API</p></div></div>{ROUTES.map(route => <button className={apiPath === route.path ? "endpoint selected-endpoint" : "endpoint"} key={route.method+route.path} onClick={() => {setApiPath(route.path);setApiMethod(route.method);}}><span className={`method method-${route.method.toLowerCase()}`}>{route.method}</span><span><strong>{route.path}</strong><small>{route.description}</small></span></button>)}</section><section className="panel request-panel"><div className="panel-heading"><div><h3>Request builder</h3><p>Base URL: {normalizeBase(settings.apiBase)}</p></div></div><div className="form-row"><label>Method<select value={apiMethod} onChange={e=>setApiMethod(e.target.value)}>{["GET","POST","DELETE","PUT","PATCH"].map(m=><option key={m}>{m}</option>)}</select></label><label className="grow">Path<input value={apiPath} onChange={e=>setApiPath(e.target.value)} placeholder="/v1/rules/active"/></label></div><label>JSON body {apiMethod === "GET" || apiMethod === "DELETE" ? <span className="muted">(not sent for this method)</span> : null}<textarea className="code-editor request-editor" value={apiBody} onChange={e=>setApiBody(e.target.value)} spellCheck={false}/></label><div className="request-actions"><button className="button primary" onClick={() => void runApi()} disabled={apiBusy}>{apiBusy ? "Sending…" : "▶ Send request"}</button><span className="muted">X-Tenant-ID is included automatically</span></div><div className="response-heading"><h4>Response</h4>{apiResult && <button className="text-button" onClick={() => navigator.clipboard?.writeText(apiResult)}>Copy</button>}</div><pre className="response-box">{apiResult || "Send a request to inspect its response."}</pre></section></div></section>}
      {nav === "codegen" && <section className="page-content"><div className="page-intro"><h2>Contracts → generated artifacts</h2><p>Register a schema, select a stored version, and invoke the existing server-side generator.</p></div><div className="content-grid codegen-grid"><section className="panel"><div className="panel-heading"><div><h3>Register contract</h3><p>Requires contracts:admin outside local development</p></div></div><label>Contract name<input value={contractName} onChange={e=>setContractName(e.target.value)} placeholder="transaction.initiated"/></label><label>Version<input value={contractVersion} onChange={e=>setContractVersion(e.target.value)} placeholder="v1"/></label><label>Schema payload (JSON)<textarea className="code-editor schema-editor" value={contractSchema} onChange={e=>setContractSchema(e.target.value)} spellCheck={false}/></label><button className="button primary" disabled={codegenBusy} onClick={() => void createContract()}>{codegenBusy ? "Working…" : "＋ Register contract"}</button></section><section className="panel"><div className="panel-heading"><div><h3>Generate code</h3><p>Generated artifacts are written on the API server</p></div></div><label>Registered contract<select value={selectedContract} onChange={e=>setSelectedContract(e.target.value)}><option value="">Select a contract version…</option>{contracts.map((c,i)=>{const name=c.name || c.contract_name || "unnamed"; const version=c.version || "v1";return <option key={name+version+i} value={name+"@"+version}>{name} · {version}</option>;})}</select></label><label>Target language<select value={language} onChange={e=>setLanguage(e.target.value)}>{[["go","Go"],["java","Java"],["protobuf","Protocol Buffers"],["jsonschema","JSON Schema"]].map(([v,l])=><option key={v} value={v}>{l}</option>)}</select></label><div className="info-callout"><strong>Server-side output</strong><p>For safety, output stays inside the API server's managed temporary directory. This API returns generated file paths, not file contents or a download archive.</p></div><button className="button primary" disabled={codegenBusy || !selectedContract} onClick={() => void generate()}>{codegenBusy ? "Generating…" : "⚡ Generate artifacts"}</button>{generated && <div className="result-panel"><div className="result-heading"><strong>{generated.error ? "Operation failed" : generated.operation || "Generation result"}</strong><Status value={generated.error ? "Error" : "Complete"}/></div><pre className="response-box">{JSON.stringify(generated.error ? {error:generated.error} : generated, null, 2)}</pre></div>}</section></div></section>}
      {nav === "workers" && <section className="page-content"><div className="page-intro page-intro-row"><div><h2>Worker fleet & rule throughput</h2><p>Each worker is scraped independently. Rule counts are cumulative process counters, not in-flight rule executions.</p></div><button className="button secondary" onClick={() => setSettingsOpen(true)}>Configure workers</button></div>{workers.length === 0 ? <section className="panel"><Empty title="Worker telemetry is not configured" detail="Add each worker's private Prometheus /metrics endpoint in Settings. This page intentionally does not infer worker health from API liveness."/><button className="button primary centered-button" onClick={() => setSettingsOpen(true)}>Configure metrics URLs</button></section> : <><div className="metric-grid"><MetricCard label="Workers online" value={`${onlineWorkers}/${workers.length}`} hint="Successful metrics scrapes" icon="▦"/><MetricCard label="In-flight events" value={workers.reduce((n,w)=>n+(w.inFlight||0),0)} hint="Sum of current worker gauges" icon="↻"/><MetricCard label="Active partition keys" value={workers.reduce((n,w)=>n+(w.activeKeys||0),0)} hint="Process-local queue gauges" icon="⌘"/><MetricCard label="Cumulative executions" value={executionTotal.toLocaleString()} hint="Summed execution counters" icon="↗"/></div><div className="worker-cards">{workers.map(worker=><section className="panel worker-card" key={worker.url}><div className="worker-card-header"><div className="worker-symbol large">▦</div><div className="grow"><h3>{worker.name}</h3><p>{worker.url}</p></div><Status value={worker.online ? "Online" : "Unreachable"}/></div>{worker.error ? <div className="alert error">{worker.error}</div> : <><div className="worker-kpis"><div><small>In flight</small><strong>{worker.inFlight ?? "—"}</strong></div><div><small>Active keys</small><strong>{worker.activeKeys ?? "—"}</strong></div><div><small>Owned shards</small><strong>{worker.shards ?? "—"}</strong></div></div><h4>Rule execution counters</h4>{worker.executions.length ? worker.executions.map(item=><div className="throughput-row" key={item.ruleSet}><span>{item.ruleSet}</span><div className="bar-track"><i style={{width:`${Math.max(2, item.count / Math.max(...worker.executions.map(x=>x.count),1)*100)}%`}}/></div><strong>{item.count.toLocaleString()}</strong></div>) : <p className="muted">No execution series exposed by this worker yet.</p>}</> }<div className="worker-footer">Last scrape: {new Date(worker.checkedAt).toLocaleTimeString()}</div></section>)}</div></>}</section>}
      {nav === "rules" && <section className="page-content"><div className="page-intro"><h2>Rule lifecycle</h2><p>Inspect active revisions and deploy a new rule source through the compile-and-activate endpoint.</p></div><div className="content-grid rules-layout"><section className="panel"><div className="panel-heading"><div><h3>Deploy revision</h3><p>JSON or YAML rule source, maximum 1 MiB</p></div></div><label>Rule set<input value={ruleSet} onChange={e=>setRuleSet(e.target.value)} placeholder="transaction.initiated"/></label><label>Rule source<textarea className="code-editor rule-editor" value={ruleSource} onChange={e=>setRuleSource(e.target.value)} spellCheck={false}/></label><button className="button primary" onClick={() => void deployRule()} disabled={loading}>{loading ? "Compiling…" : "▶ Compile & activate"}</button>{ruleResult && <pre className="response-box result-inline">{ruleResult}</pre>}</section><section className="panel"><div className="panel-heading"><div><h3>Active rules</h3><p>{activeRules.length} record(s) returned by the API</p></div><button className="text-button" onClick={() => void refresh()}>Refresh ↻</button></div>{activeRules.length === 0 ? <Empty title="Nothing active" detail="Check the tenant setting or deploy a rule revision."/> : <div className="table-wrap"><table><thead><tr><th>Rule set</th><th>Revision</th><th>Status</th><th>Action</th></tr></thead><tbody>{activeRules.map((item,i)=>{const id=item.rule_id || item.rule_set || item.ruleSet || item.id || "unknown";return <tr key={id+i}><td><strong>{id}</strong><small className="table-sub">{item.tenant_id || item.tenantID || ""}</small></td><td>{item.revision ?? item.active_revision ?? "—"}</td><td><Status value={item.status || "Active"}/></td><td><button className="text-button danger-text" onClick={()=>void deactivateRule(item)}>Deactivate</button></td></tr>;})}</tbody></table></div>}</section></div></section>}
      {nav === "logs" && <section className="page-content"><div className="page-intro page-intro-row"><div><h2>Runtime logs</h2><p>Query recent structured logs from your configured Loki-compatible log backend.</p></div><button className="button primary" onClick={() => void loadLogs()} disabled={logsBusy}>{logsBusy ? "Querying…" : "↻ Query last hour"}</button></div>{logsError && <div className="alert info"><strong>Log source</strong><span>{logsError}</span><button onClick={() => setSettingsOpen(true)}>Configure</button></div>}<section className="panel"><div className="panel-heading"><div><h3>Log stream</h3><p>Most recent 200 entries from the last hour</p></div><span className="muted">{logs.length} entries</span></div>{logs.length === 0 ? <Empty title="No log entries loaded" detail="Set the Loki base URL and query in Settings, then query the stream. The application does not currently provide a log-tail HTTP endpoint."/> : <div className="logs-list">{logs.map((log,i)=><div className="log-row" key={i}><time>{log.timestamp}</time><span className="log-level">{String(log.line).match(/\b(ERROR|WARN|INFO|DEBUG)\b/)?.[1] || "LOG"}</span><code>{log.line}</code><button className="copy-log" onClick={()=>navigator.clipboard?.writeText(log.line)}>Copy</button></div>)}</div>}</section></section>}
      <footer className="footer"><span>FlowRule Console</span><span>Data refreshes every 10 seconds · {lastRefresh ? `Updated ${lastRefresh.toLocaleTimeString()}` : "Waiting for first refresh"}</span></footer>
    </main>
    {settingsOpen && <div className="modal-backdrop" onMouseDown={e=>{if(e.target===e.currentTarget)setSettingsOpen(false);}}><section className="settings-modal"><div className="modal-heading"><div><h2>Console settings</h2><p>Connection details are stored in this browser only.</p></div><button className="icon-button" onClick={() => setSettingsOpen(false)}>×</button></div><label>API base URL<input value={settings.apiBase} onChange={e=>setSettings({...settings,apiBase:e.target.value})} placeholder="http://localhost:8080"/></label><label>Tenant ID<input value={settings.tenant} onChange={e=>setSettings({...settings,tenant:e.target.value})} placeholder="development-tenant"/></label><label>Bearer token <span className="muted">(optional)</span><input type="password" value={settings.token} onChange={e=>setSettings({...settings,token:e.target.value})} autoComplete="off"/></label><label>Worker metrics URLs <span className="muted">(one per line or comma-separated)</span><textarea value={settings.workerMetrics} onChange={e=>setSettings({...settings,workerMetrics:e.target.value})} rows={3} placeholder="http://worker-1:9090/metrics"/></label><label>Loki base URL <span className="muted">(optional)</span><input value={settings.lokiBase} onChange={e=>setSettings({...settings,lokiBase:e.target.value})} placeholder="http://localhost:3100"/></label><label>Loki query<input value={settings.lokiQuery} onChange={e=>setSettings({...settings,lokiQuery:e.target.value})} placeholder='{service_name="flowrule-worker"}'/></label><div className="info-callout"><strong>Security note</strong><p>Keep metrics and Loki private. Browser requests need correct CORS settings. Tokens are stored in localStorage; use a short-lived, least-privilege token and do not use a shared machine.</p></div><div className="modal-actions"><button className="button secondary" onClick={() => setSettings(DEFAULT_SETTINGS)}>Reset</button><button className="button primary" onClick={() => saveSettings(settings)}>Save settings</button></div></section></div>}
  </div>;
}
