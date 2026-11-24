// Interfaz de Distributed ML: navegación por hash, polling cada 3 s
// (README: actualización de estados por polling) y dibujo de cada vista.
import { getCluster, getHealth } from "./services/cluster.service.js";
import { isOnline, listWorkers } from "./services/nodes.service.js";
import { deleteDataset, getColumns, listDatasets, uploadDataset } from "./services/datasets.service.js";
import { getResult, getTasks, listJobs } from "./services/jobs.service.js";
import { DEFAULTS, estimatePlan, startTraining } from "./services/training.service.js";

const VIEWS = ["panel", "cluster", "entrenar", "resultados"];
const POLL_MS = 3000;
const WORKER_COLORS = ["#173FAD", "#B3263E", "#2F7A5B", "#8A5CC7", "#C77A1A", "#4E5D78"];
const STATUS_LABEL = {
  pending: "En espera",
  running: "Entrenando",
  completed: "Completado",
  failed: "Falló",
  cancelled: "Cancelado",
  assigned: "Asignada",
};
const NUMBER_WORDS = { 2: "dos", 3: "tres", 4: "cuatro", 5: "cinco", 6: "seis" };

const $ = (id) => document.getElementById(id);
const esc = (value) =>
  String(value ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
const short = (id) => String(id || "").slice(0, 8);
const settle = (promise) => promise.then((value) => ({ ok: true, value }), (error) => ({ ok: false, error }));

const state = {
  view: null,
  health: null,
  cluster: null,
  workers: [],
  jobs: [],
  datasets: [],
  results: new Map(),
  tasks: new Map(),
  error: null,
  loading: false,
  selectedJob: readStored("selectedJob"),
  train: { datasetId: null, columns: [], target: null, workers: null, confirmDelete: null, busy: false },
};

function readStored(key) {
  try { return sessionStorage.getItem(key); } catch { return null; }
}
function store(key, value) {
  try {
    if (value == null) sessionStorage.removeItem(key);
    else sessionStorage.setItem(key, value);
  } catch { /* el almacenamiento puede no estar disponible */ }
}

/* ---------- Formato ---------- */
function pill(status) {
  return `<span class="pill ${esc(status)}">${esc(STATUS_LABEL[status] || status)}</span>`;
}
function pct(value, digits = 1) {
  return value == null ? "—" : `${(value * 100).toFixed(digits)} %`;
}
function seconds(value) {
  if (value == null || Number.isNaN(value)) return "—";
  if (value < 60) return `${value.toFixed(value < 10 ? 1 : 0)} s`;
  const m = Math.floor(value / 60);
  return `${m} min ${Math.round(value - m * 60)} s`;
}
function jobDuration(job) {
  if (!job.started_at) return null;
  const end = job.completed_at ? new Date(job.completed_at) : new Date();
  return (end - new Date(job.started_at)) / 1000;
}
function datasetName(id) {
  const dataset = state.datasets.find((d) => d.dataset_id === id);
  return dataset ? dataset.name : `Dataset ${short(id)}`;
}
function onlineWorkers() {
  return state.workers.filter(isOnline);
}
function workerColor(nodeId) {
  const ids = [...new Set(state.workers.map((w) => w.node_id))].sort();
  const index = ids.indexOf(nodeId);
  return WORKER_COLORS[(index < 0 ? ids.length : index) % WORKER_COLORS.length];
}
function notice(kind, text) {
  return `<div class="notice ${kind}">${esc(text)}</div>`;
}

/* ---------- Navegación ---------- */
function currentView() {
  const view = (location.hash || "#panel").slice(1);
  return VIEWS.includes(view) ? view : "panel";
}
function showView() {
  const view = currentView();
  const changed = view !== state.view;
  state.view = view;
  VIEWS.forEach((name) => { $(`view-${name}`).hidden = name !== view; });
  document.querySelectorAll("nav.tabs a").forEach((a) => {
    if (a.dataset.view === view) a.setAttribute("aria-current", "page");
    else a.removeAttribute("aria-current");
  });
  if (changed) window.scrollTo(0, 0);
  refresh();
}
function openJob(jobId) {
  state.selectedJob = jobId;
  store("selectedJob", jobId);
  if (location.hash === "#resultados") refresh();
  else location.hash = "resultados";
}

/* ---------- Datos ---------- */
async function refresh() {
  if (state.loading) return;
  state.loading = true;
  try {
    state.health = await getHealth();
    renderHealth();
    if (state.health.apiDown) {
      state.error = state.health.message;
      render();
      return;
    }
    const view = state.view;
    const [jobs, datasets, workers, cluster] = await Promise.all([
      settle(listJobs()),
      settle(listDatasets()),
      settle(listWorkers()),
      view === "cluster" || view === "panel" ? settle(getCluster()) : Promise.resolve(null),
    ]);
    if (jobs.ok) state.jobs = jobs.value;
    if (datasets.ok) state.datasets = datasets.value;
    if (workers.ok) state.workers = workers.value;
    if (cluster && cluster.ok) state.cluster = cluster.value;
    const failed = [jobs, datasets, workers].find((r) => !r.ok);
    state.error = failed ? failed.error.message : null;
    await loadDetails(view);
    render();
  } finally {
    state.loading = false;
  }
}

async function loadTasks(job) {
  const finished = ["completed", "failed", "cancelled"].includes(job.status);
  if (finished && state.tasks.has(job.job_id)) return;
  const result = await settle(getTasks(job.job_id));
  if (result.ok) state.tasks.set(job.job_id, result.value);
}
async function loadResult(job) {
  if (job.status !== "completed" || state.results.has(job.job_id)) return;
  const result = await settle(getResult(job.job_id));
  if (result.ok && result.value) state.results.set(job.job_id, result.value);
}
function focusJob() {
  return (
    state.jobs.find((j) => j.status === "running") ||
    state.jobs.find((j) => j.status === "pending") ||
    state.jobs.find((j) => j.status === "completed") ||
    null
  );
}
async function loadDetails(view) {
  const work = [];
  if (view === "panel") {
    state.jobs.slice(0, 8).forEach((job) => work.push(loadResult(job)));
    const active = state.jobs.find((j) => j.status === "running" || j.status === "pending");
    if (active) work.push(loadTasks(active));
  } else if (view === "cluster") {
    const job = focusJob();
    if (job) work.push(loadTasks(job));
  } else if (view === "resultados") {
    state.jobs.slice(0, 12).forEach((job) => work.push(loadResult(job)));
    const job = selectedJob();
    if (job) { work.push(loadTasks(job)); work.push(loadResult(job)); }
  }
  await Promise.all(work);
}
function selectedJob() {
  return state.jobs.find((j) => j.job_id === state.selectedJob) || state.jobs[0] || null;
}

/* ---------- Render ---------- */
function render() {
  if (state.view === "panel") renderPanel();
  if (state.view === "cluster") renderCluster();
  if (state.view === "entrenar") renderTrain();
  if (state.view === "resultados") renderResults();
}

function renderHealth() {
  const h = state.health;
  let dot = "ok", text = "Clúster sano";
  if (h.apiDown) { dot = "bad"; text = "API sin conexión"; }
  else if (!h.healthy) { dot = "bad"; text = "Base de datos no disponible"; }
  $("health").innerHTML = `<span class="dot ${dot}"></span><span>${text}</span>`;
}

function errorBanner() {
  return state.error ? notice("bad", state.error) : "";
}

function renderPanel() {
  const online = onlineWorkers().length;
  $("panel-title").innerHTML = online === 0
    ? "Un modelo,<br><em>esperando Workers.</em>"
    : `Un modelo,<br><em>${online === 1 ? "un Worker" : `${NUMBER_WORDS[online] || online} Workers`}.</em>`;

  // Job activo
  const active = state.jobs.find((j) => j.status === "running") || state.jobs.find((j) => j.status === "pending");
  const box = $("active-job");
  if (!active) {
    box.innerHTML = `
      <span class="eyebrow">Job en curso</span>
      <h3 style="font-size:34px">Nada <em>entrenando</em></h3>
      <p class="sub" style="margin:0">Cuando lances un entrenamiento verás aquí su estado y el reparto de Tasks.</p>
      <a class="btn btn-ghost" href="#entrenar">Lanzar uno</a>`;
  } else {
    const tasks = state.tasks.get(active.job_id) || [];
    const holders = new Set(tasks.map((t) => t.node_id).filter(Boolean)).size;
    const elapsed = jobDuration(active);
    box.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;gap:12px"><span class="eyebrow">Job en curso</span>${pill(active.status)}</div>
      <div>
        <h3 style="font-size:36px">${esc(datasetName(active.dataset_id))}</h3>
        <div class="sub" style="margin-top:6px">Target «${esc(active.target_column)}» · ${tasks.length || "—"} Tasks en ${holders || "—"} Workers</div>
      </div>
      ${active.status === "running"
        ? `<div class="bar live" aria-label="Entrenando"><span></span></div>`
        : notice("warn", `Espera ${active.requested_workers} Workers elegibles para empezar.`)}
      <div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px">
        <div class="mini"><div class="eyebrow">Tiempo</div><div class="v">${seconds(elapsed)}</div></div>
        <div class="mini"><div class="eyebrow">Épocas</div><div class="v">${esc(active.parameters?.epochs ?? "—")}</div></div>
      </div>
      <button class="btn btn-ghost btn-small" type="button" data-open-job="${esc(active.job_id)}">Ver detalle</button>`;
  }

  // Indicadores
  const db = state.cluster?.database;
  const lead = state.cluster?.leadership;
  const lastResult = state.jobs.filter((j) => j.status === "completed").map((j) => state.results.get(j.job_id)).find(Boolean);
  const counts = state.jobs.reduce((acc, j) => ({ ...acc, [j.status]: (acc[j.status] || 0) + 1 }), {});
  $("kpis").innerHTML = `
    <div class="glass card"><span class="eyebrow">Base de datos</span>
      <div class="big">${db ? db.reachable : "—"}<small>/${db ? db.total : "—"}</small></div>
      <div class="sub">${db ? (db.quorum ? "Nodos que responden, con quórum Raft." : "Sin quórum: la base de datos no acepta escrituras.") : "Consultando…"}</div></div>
    <div class="glass card"><span class="eyebrow">Workers</span>
      <div class="big">${online}<small>/${state.workers.length}</small></div>
      <div class="sub">${lead?.leader_node_id ? `Coordinador ${esc(short(lead.leader_node_id))}, lease de la época ${esc(lead.epoch)}.` : "Sin coordinador con lease vigente."}</div></div>
    <div class="glass card"><span class="eyebrow">Último resultado</span>
      <div class="big">${lastResult ? (lastResult.accuracy * 100).toFixed(1) : "—"}<small style="color:var(--fg)">${lastResult ? "%" : ""}</small></div>
      <div class="sub">${lastResult ? `Accuracy en test · loss ${lastResult.loss.toFixed(4)}.` : "Todavía no hay Jobs completados."}</div></div>
    <div class="glass card"><span class="eyebrow">Jobs</span>
      <div class="big">${state.jobs.length}</div>
      <div class="sub">${counts.running || 0} entrenando · ${counts.pending || 0} en espera · ${counts.completed || 0} completados · ${counts.failed || 0} fallidos.</div></div>`;

  // Jobs recientes
  const recent = state.jobs.slice(0, 8);
  $("recent-jobs").innerHTML = errorBanner() + (recent.length === 0
    ? `<div class="empty">Todavía no hay Jobs. <a href="#entrenar">Lanza el primero</a>.</div>`
    : `<table style="min-width:680px">
        <thead><tr><th>Dataset</th><th>Estado</th><th>Workers</th><th>Accuracy</th><th>Duración</th></tr></thead>
        <tbody>${recent.map((job) => {
          const result = state.results.get(job.job_id);
          return `<tr class="clickable" data-open-job="${esc(job.job_id)}" tabindex="0">
            <td>${esc(datasetName(job.dataset_id))}<div class="sub">${esc(job.target_column)} · ${esc(job.parameters?.epochs ?? "—")} épocas</div></td>
            <td>${pill(job.status)}</td>
            <td>${esc(job.actual_workers ?? "—")} de ${esc(job.requested_workers)}</td>
            <td>${result ? pct(result.accuracy) : "—"}</td>
            <td>${seconds(jobDuration(job))}</td></tr>`;
        }).join("")}</tbody></table>`);
}

function renderCluster() {
  const c = state.cluster;
  const db = c?.database;
  $("db-summary").textContent = db
    ? `${db.reachable} de ${db.total} nodos responden · ${db.quorum ? "con quórum" : "sin quórum"}`
    : "";
  const replication = db?.replication;
  const allThree = replication && replication.ranges > 0 && replication.replica_counts["3"] === replication.ranges;
  $("db-nodes").innerHTML = !db ? `<div class="glass card">${errorBanner() || "Consultando la base de datos…"}</div>` :
    db.nodes.map((node) => `
      <div class="glass card">
        <div class="node-top"><h3>${esc(node.name)}</h3>${node.reachable ? `<span class="pill online">Responde</span>` : `<span class="pill offline">Sin respuesta</span>`}</div>
        <dl class="kv"><dt>Dirección</dt><dd class="mono nowrap">${esc(node.host)}:${esc(node.port)}</dd>
        <dt>Latencia</dt><dd>${node.reachable ? `${node.latency_ms} ms` : "—"}</dd>
        <dt>Estado</dt><dd>${node.reachable ? "Acepta SQL" : esc(node.error || "Caído")}</dd></dl>
      </div>`).join("") + `
      <div class="glass card">
        <div class="node-top"><h3>Replicación</h3>${replication ? `<span class="pill ${allThree ? "online" : "pending"}">${allThree ? "Completa" : "Parcial"}</span>` : `<span class="pill idle">Sin datos</span>`}</div>
        <dl class="kv"><dt>Rangos</dt><dd>${replication ? replication.ranges : "—"}</dd>
        <dt>Réplicas</dt><dd>${replication ? Object.entries(replication.replica_counts).map(([n, count]) => `${count} con ${n}`).join(" · ") : "—"}</dd>
        <dt>En los nodos</dt><dd class="nowrap">${replication ? `{${replication.replica_nodes.join(", ")}}` : "—"}</dd></dl>
      </div>`;

  const workers = [...state.workers].sort((a, b) => (b.is_leader - a.is_leader) || (isOnline(b) - isOnline(a)));
  $("workers").innerHTML = workers.length === 0
    ? `<div class="glass card empty">No hay Workers registrados. Arranca uno con <span class="mono">python -m workers.worker</span>.</div>`
    : workers.map((w, index) => {
      const online = isOnline(w);
      const cpuPct = w.cpu_cores && w.cpu_available_cores != null ? Math.min(100, (w.cpu_available_cores / w.cpu_cores) * 100) : 0;
      const badge = w.is_leader ? `<span class="pill leader">Coordinador</span>` : online ? `<span class="pill online">En línea</span>` : `<span class="pill offline">Sin señal</span>`;
      return `<div class="glass card" style="${online ? "" : "opacity:.7"}">
        <div class="node-top"><div><h3 class="mono" style="font-family:var(--display);font-size:24px">${esc(short(w.node_id))}</h3><div class="sub">${esc(w.hostname)}</div></div>${badge}</div>
        <div class="field"><span class="label"><span>CPU disponible</span><output>${w.cpu_available_cores != null ? w.cpu_available_cores.toFixed(1) : "—"} de ${esc(w.cpu_cores ?? "—")} núcleos</output></span>
          <div class="bar"><span style="width:${cpuPct.toFixed(0)}%;background:${workerColor(w.node_id)}"></span></div></div>
        <dl class="kv"><dt>RAM disponible</dt><dd>${w.ram_available_mb != null ? (w.ram_available_mb / 1024).toFixed(1) + " GB" : "—"}</dd>
        <dt>Tasks activas</dt><dd>${esc(w.active_tasks ?? 0)}</dd>
        <dt>Heartbeat</dt><dd>${w.heartbeat_age_seconds != null ? `hace ${seconds(w.heartbeat_age_seconds)}` : "nunca"}</dd>
        <dt>Estado</dt><dd>${esc(w.status)}</dd></dl>
      </div>`;
    }).join("");

  const job = focusJob();
  const tasks = job ? state.tasks.get(job.job_id) || [] : [];
  const holders = [...new Set(tasks.map((t) => t.node_id).filter(Boolean))];
  const trainRows = tasks.reduce((sum, t) => sum + (t.rows_assigned || 0), 0);
  $("partitions").innerHTML = !job || tasks.length === 0
    ? `<h2 id="part-title">Cómo se <em>reparte</em> un Job</h2><p class="sub" style="margin:0">Cuando haya un Job verás aquí sus particiones de train y qué Worker ejecuta cada una.</p>`
    : `<div style="display:flex;flex-wrap:wrap;justify-content:space-between;align-items:baseline;gap:12px">
        <h2 id="part-title">Cómo se <em>reparte</em> el Job</h2>
        <span class="sub">${esc(datasetName(job.dataset_id))} · ${trainRows} filas de train en ${tasks.length} particiones · ${pill(job.status)}</span></div>
      <div class="strip num">${tasks.map((t) => `<div class="seg-part" style="background:${t.node_id ? workerColor(t.node_id) : "#9AA0AC"}" title="Worker ${esc(short(t.node_id))}">p${t.partition_index}<br>${esc(t.rows_assigned ?? "—")}${t.attempts > 1 ? `<br>intento ${t.attempts}` : ""}</div>`).join("")}</div>
      <div class="legend">${holders.map((id) => `<span><i style="background:${workerColor(id)}"></i>${esc(short(id))}</span>`).join("")}</div>`;
}

/* ---------- Entrenar ---------- */
function renderTrain() {
  const online = onlineWorkers().length;
  const maxWorkers = Math.max(1, online);
  if (state.train.workers == null || state.train.workers > maxWorkers) state.train.workers = Math.min(3, maxWorkers);
  $("workers-seg").innerHTML = Array.from({ length: maxWorkers }, (_, i) => i + 1)
    .map((n) => `<button type="button" data-workers="${n}" aria-pressed="${n === state.train.workers}" ${online === 0 ? "disabled" : ""}>${n}</button>`)
    .join("");

  if (state.train.datasetId && !state.datasets.some((d) => d.dataset_id === state.train.datasetId)) {
    state.train.datasetId = null;
    state.train.columns = [];
    state.train.target = null;
  }
  $("dataset-list").innerHTML = errorBanner() + (state.datasets.length === 0
    ? `<div class="empty">Sube tu primer CSV para empezar.</div>`
    : [...state.datasets].reverse().map((d) => {
      const selected = d.dataset_id === state.train.datasetId;
      const confirming = state.train.confirmDelete === d.dataset_id;
      return `<div class="dataset-row">
        <button type="button" class="dataset" data-dataset="${esc(d.dataset_id)}" aria-pressed="${selected}">
          <span><strong>${esc(d.name)}</strong><br><span class="sub">${esc(d.column_count)} columnas · ${esc(d.row_count)} filas</span></span>
          <span class="pill ${selected ? "running" : "idle"}">${selected ? "Elegido" : "Usar"}</span>
        </button>
        ${confirming
          ? `<button type="button" class="btn btn-small" style="background:var(--bad-soft);color:var(--bad-text)" data-delete-confirm="${esc(d.dataset_id)}">Borrar</button>`
          : `<button type="button" class="icon-btn" data-delete="${esc(d.dataset_id)}" aria-label="Borrar ${esc(d.name)}">
              <svg aria-hidden="true" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 7h16M10 11v6M14 11v6M6 7l1 13h10l1-13M9 7V4h6v3"/></svg></button>`}
      </div>`;
    }).join(""));

  $("target-chips").innerHTML = state.train.columns.length === 0
    ? `<span class="sub">${state.train.datasetId ? "Cargando columnas…" : "Primero elige un dataset."}</span>`
    : state.train.columns.map((c) => `<button type="button" class="chip" data-target="${esc(c)}" aria-pressed="${c === state.train.target}">${esc(c)}</button>`).join("");

  renderSummary();
}

function readParams() {
  return {
    learningRate: parseFloat($("lr").value),
    epochs: parseInt($("epochs").value, 10),
    trainRatio: parseFloat($("ratio").value),
    seed: parseInt($("seed").value, 10),
  };
}

function renderSummary() {
  const p = readParams();
  $("lr-out").textContent = p.learningRate.toFixed(2);
  $("epochs-out").textContent = String(p.epochs);
  $("ratio-out").textContent = `${Math.round(p.trainRatio * 100)} %`;
  const dataset = state.datasets.find((d) => d.dataset_id === state.train.datasetId);
  const plan = dataset ? estimatePlan({ rows: dataset.row_count, trainRatio: p.trainRatio, workers: state.train.workers || 1 }) : null;
  $("sum-name").innerHTML = dataset ? `${esc(dataset.name)} <em>listo</em>` : "Elige un dataset";
  $("sum-train").textContent = plan ? plan.train : "—";
  $("sum-test").textContent = plan ? plan.test : "—";
  $("sum-parts").textContent = plan ? plan.partitions : "—";
  $("sum-rows").textContent = plan ? (plan.minRows === plan.maxRows ? plan.minRows : `${plan.minRows}–${plan.maxRows}`) : "—";
  $("sum-target").textContent = state.train.target || "—";
  $("sum-workers").textContent = `${state.train.workers || "—"} de ${onlineWorkers().length} en línea`;
  $("train-submit").disabled = !dataset || !state.train.target || onlineWorkers().length === 0 || state.train.busy;
}

async function selectDataset(datasetId) {
  state.train.datasetId = datasetId;
  state.train.columns = [];
  state.train.target = null;
  state.train.confirmDelete = null;
  hideMessage("train-msg");
  renderTrain();
  const result = await settle(getColumns(datasetId));
  if (state.train.datasetId !== datasetId) return;
  if (result.ok) {
    state.train.columns = result.value.columns;
    state.train.target = result.value.columns[result.value.columns.length - 1];
  } else {
    showMessage("train-msg", "bad", result.error.message);
  }
  renderTrain();
}

async function handleUpload(file) {
  if (!file) return;
  hideMessage("upload-msg");
  $("drop-text").textContent = `Subiendo ${file.name}…`;
  const result = await settle(uploadDataset(file, file.name.replace(/\.csv$/i, "")));
  $("drop-text").textContent = "Arrastra un CSV o haz clic para subirlo";
  $("csv-file").value = "";
  if (!result.ok) {
    showMessage("upload-msg", "bad", `No se pudo subir ${file.name}: ${result.error.message}`);
    return;
  }
  toast(`${result.value.name} subido · ${result.value.row_count} filas`);
  state.datasets.push(result.value);
  await selectDataset(result.value.dataset_id);
}

async function handleDelete(datasetId) {
  const result = await settle(deleteDataset(datasetId));
  state.train.confirmDelete = null;
  if (!result.ok) {
    showMessage("upload-msg", "bad", result.error.status === 409 ? "No se puede borrar: algún Job usa este dataset." : result.error.message);
    renderTrain();
    return;
  }
  state.datasets = state.datasets.filter((d) => d.dataset_id !== datasetId);
  if (state.train.datasetId === datasetId) {
    state.train.datasetId = null;
    state.train.columns = [];
    state.train.target = null;
  }
  toast("Dataset borrado");
  renderTrain();
}

async function handleSubmit(event) {
  event.preventDefault();
  const p = readParams();
  if (!state.train.datasetId || !state.train.target) return;
  if (!Number.isInteger(p.seed) || p.seed < 0) {
    showMessage("train-msg", "bad", "El seed debe ser un número entero positivo.");
    return;
  }
  hideMessage("train-msg");
  state.train.busy = true;
  $("train-submit").textContent = "Preparando el dataset…";
  renderSummary();
  const result = await settle(startTraining({
    datasetId: state.train.datasetId,
    target: state.train.target,
    workers: state.train.workers,
    ...p,
  }));
  state.train.busy = false;
  $("train-submit").textContent = "Iniciar entrenamiento";
  renderSummary();
  if (!result.ok) {
    showMessage("train-msg", "bad", result.error.message);
    return;
  }
  const prep = result.value.preparation;
  toast(`Job creado · ${prep.train_rows} filas de train y ${prep.test_rows} de test`);
  state.jobs.unshift(result.value);
  openJob(result.value.job_id);
}

/* ---------- Resultados ---------- */
function renderResults() {
  const job = selectedJob();
  $("job-list").innerHTML = state.jobs.length === 0
    ? `<div class="empty">Todavía no hay Jobs.</div>`
    : state.jobs.map((j) => {
      const result = state.results.get(j.job_id);
      return `<button type="button" class="job-item" data-open-job="${esc(j.job_id)}" aria-pressed="${job && j.job_id === job.job_id}">
        <span class="row"><strong style="font-weight:600">${esc(datasetName(j.dataset_id))}</strong>${pill(j.status)}</span>
        <span class="row sub"><span>${new Date(j.created_at).toLocaleString("es")}</span><span>${result ? pct(result.accuracy) : ""}</span></span>
      </button>`;
    }).join("");

  const detail = $("job-detail");
  if (!job) {
    detail.innerHTML = `<div class="glass card empty">${errorBanner() || "Lanza un entrenamiento para ver aquí su resultado."}</div>`;
    return;
  }
  const tasks = state.tasks.get(job.job_id) || [];
  const result = state.results.get(job.job_id);
  const params = job.parameters || {};
  const meta = `
    <div class="grid" style="grid-template-columns:repeat(auto-fit,minmax(140px,1fr))">
      <div class="mini"><div class="eyebrow">Target</div><div class="v">${esc(job.target_column)}</div></div>
      <div class="mini"><div class="eyebrow">Workers</div><div class="v">${esc(job.actual_workers ?? "—")} de ${esc(job.requested_workers)}</div></div>
      <div class="mini"><div class="eyebrow">Épocas</div><div class="v">${esc(params.epochs ?? "—")}</div></div>
      <div class="mini"><div class="eyebrow">Learning rate</div><div class="v">${esc(params.learning_rate ?? "—")}</div></div>
      <div class="mini"><div class="eyebrow">Train</div><div class="v">${job.train_ratio != null ? Math.round(job.train_ratio * 100) + " %" : "—"}</div></div>
      <div class="mini"><div class="eyebrow">Seed</div><div class="v">${esc(job.seed ?? "—")}</div></div>
    </div>`;

  let body = "";
  if (job.status === "pending") {
    body = notice("warn", `En espera: el Scheduler lo arranca cuando haya ${job.requested_workers} Workers elegibles.`);
  } else if (job.status === "running") {
    body = `<div class="glass card" style="gap:14px"><span class="eyebrow">Entrenando · ${seconds(jobDuration(job))}</span>
      <div class="bar live"><span></span></div>
      <p class="sub" style="margin:0">El coordinador reparte cada época entre las Tasks. El avance por época vive en su memoria; las métricas aparecen aquí al terminar.</p></div>`;
  } else if (job.status === "failed" || job.status === "cancelled") {
    body = notice("bad", job.error_message || "El Job terminó sin resultado.");
  } else if (job.status === "completed" && !result) {
    body = `<div class="glass card">Cargando el resultado…</div>`;
  }

  if (result) {
    const m = result.metrics || {};
    const test = m.test || {};
    const train = m.train || {};
    body = `
      <div class="grid">
        <div class="glass card"><span class="eyebrow">Accuracy test</span><div class="big">${(result.accuracy * 100).toFixed(1)}<small style="color:var(--fg)">%</small></div></div>
        <div class="glass card"><span class="eyebrow">Loss test</span><div class="big" style="font-size:44px">${result.loss.toFixed(4)}</div></div>
        <div class="glass card"><span class="eyebrow">Accuracy train</span><div class="big" style="font-size:44px">${pct(train.accuracy)}</div></div>
        <div class="glass card"><span class="eyebrow">Duración</span><div class="big" style="font-size:44px">${seconds(result.duration_seconds)}</div></div>
      </div>
      <div class="two">
        <section class="main glass card" style="padding:28px" aria-labelledby="curve-title">
          <div style="display:flex;flex-wrap:wrap;justify-content:space-between;align-items:baseline;gap:12px">
            <h2 id="curve-title">Pérdida por <em>época</em></h2><span class="sub">train · ${esc(m.training?.partitions ?? "—")} particiones</span></div>
          ${lossChart(m.train_loss_by_epoch || [])}
        </section>
        <section class="aside glass card" style="padding:28px" aria-labelledby="cm-title">
          <h2 id="cm-title">Matriz de <em>confusión</em></h2>
          <p class="sub" style="margin:0">Filas: clase real · columnas: predicha. ${test.sample_count ?? "—"} filas de test.</p>
          ${confusionMatrix(test.confusion_matrix || [], m.class_labels || [])}
        </section>
      </div>`;
  }

  detail.innerHTML = `
    <section class="hero" style="align-items:flex-start;gap:20px;flex-direction:column">
      <span class="eyebrow rule">Job ${esc(short(job.job_id))} · ${new Date(job.created_at).toLocaleString("es")}</span>
      <h1 style="font-size:clamp(40px,6vw,68px)">${esc(datasetName(job.dataset_id))}${result ? `,<br><em>${(result.accuracy * 100).toFixed(1)} % de aciertos.</em>` : ` <em>${esc((STATUS_LABEL[job.status] || job.status).toLowerCase())}</em>`}</h1>
    </section>
    ${meta}
    ${body}
    <section class="glass" aria-labelledby="tasks-title" style="padding-bottom:10px">
      <div class="section-head"><h2 id="tasks-title">Tasks <em>del Job</em></h2><span class="sub">Una por partición · intento vigente</span></div>
      <div class="tablebox">${tasks.length === 0 ? `<div class="empty">El Scheduler todavía no creó las Tasks.</div>` : `
        <table style="min-width:560px"><thead><tr><th>Partición</th><th>Filas</th><th>Worker</th><th>Intentos</th><th>Estado</th></tr></thead>
        <tbody>${tasks.map((t) => `<tr><td>p${t.partition_index}</td><td>${esc(t.rows_assigned ?? "—")}</td>
          <td><span style="display:inline-flex;align-items:center;gap:8px"><i style="width:10px;height:10px;border-radius:3px;display:inline-block;background:${t.node_id ? workerColor(t.node_id) : "#9AA0AC"}"></i><span class="mono">${esc(short(t.node_id) || "—")}</span></span></td>
          <td>${esc(t.attempts)}</td><td>${pill(t.status)}</td></tr>`).join("")}</tbody></table>`}</div>
    </section>`;
}

function lossChart(points) {
  if (points.length < 2) return `<div class="empty">Sin curva de pérdida.</div>`;
  const W = 560, H = 200;
  const epochs = points.map((p) => p[0]);
  const losses = points.map((p) => p[1]);
  const minE = Math.min(...epochs), maxE = Math.max(...epochs);
  let lo = Math.min(...losses), hi = Math.max(...losses);
  const pad = (hi - lo) * 0.06 || 0.01;
  lo -= pad; hi += pad;
  const x = (e) => ((e - minE) / Math.max(1, maxE - minE)) * W;
  const y = (l) => ((hi - l) / (hi - lo)) * H;
  const line = points.map((p) => `${x(p[0]).toFixed(1)},${y(p[1]).toFixed(1)}`).join(" ");
  const ticks = [hi - pad, (hi + lo) / 2, lo + pad];
  const xTicks = [minE, Math.round((minE + maxE) / 2), maxE];
  const last = points[points.length - 1];
  return `<svg viewBox="-56 -14 640 252" role="img" aria-label="La pérdida de entrenamiento pasa de ${losses[0].toFixed(3)} en la época ${minE} a ${last[1].toFixed(3)} en la época ${maxE}." style="width:100%;height:auto">
    <defs><linearGradient id="lossfill" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#173FAD" stop-opacity=".22"/><stop offset="1" stop-color="#173FAD" stop-opacity="0"/></linearGradient></defs>
    ${ticks.map((t) => `<line x1="0" x2="${W}" y1="${y(t).toFixed(1)}" y2="${y(t).toFixed(1)}" stroke="rgba(15,23,42,.08)"/><text x="-10" y="${(y(t) + 4).toFixed(1)}" text-anchor="end" font-size="12" fill="#6E7585">${t.toFixed(3)}</text>`).join("")}
    ${xTicks.map((e) => `<text x="${x(e).toFixed(1)}" y="${H + 26}" text-anchor="middle" font-size="12" fill="#6E7585">${e}</text>`).join("")}
    <path d="M${line.split(" ").join(" L")} L${W},${H} L0,${H} Z" fill="url(#lossfill)"/>
    <polyline points="${line}" fill="none" stroke="#173FAD" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>
    <circle cx="${x(last[0]).toFixed(1)}" cy="${y(last[1]).toFixed(1)}" r="5" fill="#173FAD" stroke="#fff" stroke-width="2"/>
    <text x="${(x(last[0]) - 8).toFixed(1)}" y="${(y(last[1]) - 12).toFixed(1)}" text-anchor="end" font-size="12" fill="#0B0D12">${last[1].toFixed(4)}</text>
  </svg>`;
}

function confusionMatrix(matrix, labels) {
  if (matrix.length === 0) return `<div class="empty">Sin matriz.</div>`;
  const n = matrix.length;
  const header = `<span></span>${labels.map((l) => `<span class="axis">pred. ${esc(l)}</span>`).join("")}`;
  const rows = matrix.map((row, i) => `<span class="axis">real ${esc(labels[i])}</span>${row.map((v, j) =>
    `<div class="cell num ${v === 0 ? "zero" : i === j ? "hit" : "miss"}" aria-label="${v} de clase ${esc(labels[i])} predichos como ${esc(labels[j])}">${v}</div>`).join("")}`).join("");
  return `<div class="matrix" style="grid-template-columns:auto repeat(${n},minmax(0,1fr))">${header}${rows}</div>`;
}

/* ---------- Mensajes ---------- */
let toastTimer;
function toast(text, kind = "ok") {
  $("toast-text").textContent = text;
  $("toast-dot").className = `dot ${kind}`;
  $("toast").hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { $("toast").hidden = true; }, 3600);
}
function showMessage(id, kind, text) {
  const box = $(id);
  box.innerHTML = notice(kind, text);
  box.hidden = false;
}
function hideMessage(id) {
  $(id).hidden = true;
}

/* ---------- Eventos ---------- */
document.addEventListener("click", (event) => {
  const target = event.target.closest("[data-open-job],[data-dataset],[data-target],[data-workers],[data-delete],[data-delete-confirm]");
  if (!target) return;
  if (target.dataset.openJob) openJob(target.dataset.openJob);
  else if (target.dataset.dataset) selectDataset(target.dataset.dataset);
  else if (target.dataset.target) { state.train.target = target.dataset.target; renderTrain(); }
  else if (target.dataset.workers) { state.train.workers = Number(target.dataset.workers); renderTrain(); }
  else if (target.dataset.delete) {
    state.train.confirmDelete = target.dataset.delete;
    renderTrain();
    setTimeout(() => { if (state.train.confirmDelete === target.dataset.delete) { state.train.confirmDelete = null; renderTrain(); } }, 4000);
  } else if (target.dataset.deleteConfirm) handleDelete(target.dataset.deleteConfirm);
});
document.addEventListener("keydown", (event) => {
  const row = event.target.closest && event.target.closest("tr[data-open-job]");
  if (row && (event.key === "Enter" || event.key === " ")) {
    event.preventDefault();
    openJob(row.dataset.openJob);
  }
});
["lr", "epochs", "ratio", "seed"].forEach((id) => $(id).addEventListener("input", renderSummary));
$("train-form").addEventListener("submit", handleSubmit);
$("csv-file").addEventListener("change", (e) => handleUpload(e.target.files && e.target.files[0]));
const drop = $("drop");
["dragenter", "dragover"].forEach((type) => drop.addEventListener(type, (e) => { e.preventDefault(); drop.classList.add("over"); }));
["dragleave", "drop"].forEach((type) => drop.addEventListener(type, () => drop.classList.remove("over")));
drop.addEventListener("drop", (e) => {
  e.preventDefault();
  handleUpload(e.dataTransfer.files && e.dataTransfer.files[0]);
});

window.addEventListener("hashchange", showView);
setInterval(() => { if (!document.hidden) refresh(); }, POLL_MS);
$("lr").value = DEFAULTS.learningRate;
$("epochs").value = DEFAULTS.epochs;
$("ratio").value = DEFAULTS.trainRatio;
$("seed").value = DEFAULTS.seed;
showView();
