const trainingView = document.getElementById("training-view");
const demoView = document.getElementById("demo-view");
const openDemoButton = document.getElementById("open-demo");
const backButton = document.getElementById("back-to-training");
const fileInput = document.getElementById("csv-file");
const fileStatus = document.getElementById("file-status");
const fileName = document.getElementById("file-name");
const fileSize = document.getElementById("file-size");
const datasetUploadStatus = document.getElementById("dataset-upload-status");
const targetField = document.getElementById("target-field");
const targetColumn = document.getElementById("target-column");
const targetHint = document.getElementById("target-hint");
const featureColumns = document.getElementById("feature-columns");
const modelSelect = document.getElementById("model");
const trainingForm = document.getElementById("training-form");
const saveJobButton = document.getElementById("save-job");
const configurationStatus = document.getElementById("configuration-status");
const technicalMessage = document.getElementById("technical-message");
const lastUpdated = document.getElementById("last-updated");
const overallState = document.getElementById("overall-state");
const overallDetail = document.getElementById("overall-detail");
const leadershipState = document.getElementById("leadership-state");
const leadershipDetail = document.getElementById("leadership-detail");
const databaseNodes = document.getElementById("database-nodes");
const workersState = document.getElementById("workers-state");
const workersList = document.getElementById("workers-list");
const replicationState = document.getElementById("replication-state");
const replicationList = document.getElementById("replication-list");
const replicationNote = document.getElementById("replication-note");
const jobsList = document.getElementById("jobs-list");
const tasksList = document.getElementById("tasks-list");
const datasetsList = document.getElementById("datasets-list");
const tasksNote = document.getElementById("tasks-note");
const eventsState = document.getElementById("events-state");
const eventsList = document.getElementById("events-list");

const stateLabels = {
  healthy: "SALUDABLE",
  degraded: "DEGRADADO",
  unavailable: "NO DISPONIBLE",
  error: "ERROR",
  available: "DISPONIBLE",
  ready: "LISTO",
  not_ready: "NO LISTO",
  member: "MIEMBRO",
  not_listed: "NO REGISTRADO",
  leader: "COORDINADOR",
  no_leader: "SIN COORDINADOR",
  online: "ONLINE",
  offline: "OFFLINE",
  busy: "BUSY",
  partial: "PARCIAL",
  loading: "CARGANDO",
};

let refreshTimer = null;
let refreshInProgress = false;
let lastSuccessfulUpdate = null;
let uploadedDataset = null;
let uploadController = null;
let uploadSequence = 0;
let uploadInProgress = false;
let jobSubmitting = false;

function formatFileSize(bytes) {
  if (bytes < 1024) {
    return `${bytes} bytes`;
  }

  const units = ["KB", "MB", "GB"];
  let size = bytes / 1024;
  let unitIndex = 0;

  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }

  return `${size.toFixed(1)} ${units[unitIndex]}`;
}

function setFormStatus(element, message, state) {
  element.dataset.state = state;
  element.textContent = message;
}

function getApiError(payload) {
  if (typeof payload.detail === "string") {
    return payload.detail;
  }
  if (Array.isArray(payload.detail)) {
    return payload.detail.map((issue) => issue.msg || "Solicitud inválida.").join(" ");
  }
  return "FastAPI devolvió una respuesta de error.";
}

function updateTargetHint() {
  const hints = {
    "logistic-regression": "La regresión logística requiere al menos dos clases distintas.",
    "linear-regression": "La regresión lineal requiere que el target contenga números finitos.",
  };
  targetHint.textContent = hints[modelSelect.value] || "";
}

function updateFeatureColumns() {
  if (!uploadedDataset || !targetColumn.value) {
    featureColumns.textContent = "Selecciona la columna objetivo.";
    return;
  }
  const features = uploadedDataset.headers.filter(
    (header) => header !== targetColumn.value,
  );
  featureColumns.textContent = features.join(", ");
}

function updateSaveJobButton() {
  saveJobButton.disabled = (
    jobSubmitting
    || uploadInProgress
    || !uploadedDataset
    || !targetColumn.value
  );
}

function clearUploadedDataset() {
  uploadedDataset = null;
  targetField.hidden = true;
  targetColumn.replaceChildren(new Option("Carga un CSV para ver sus columnas", ""));
  featureColumns.textContent = "Selecciona la columna objetivo.";
  updateSaveJobButton();
}

async function uploadSelectedFile(file) {
  if (uploadController) {
    uploadController.abort();
    uploadController = null;
  }
  const currentSequence = ++uploadSequence;
  clearUploadedDataset();
  setFormStatus(configurationStatus, "", "idle");

  if (!file) {
    fileStatus.hidden = true;
    setFormStatus(datasetUploadStatus, "", "idle");
    uploadInProgress = false;
    updateSaveJobButton();
    return;
  }

  if (!file.name.toLowerCase().endsWith(".csv")) {
    fileInput.value = "";
    fileStatus.hidden = true;
    setFormStatus(datasetUploadStatus, "Selecciona un archivo con extensión .csv.", "error");
    uploadInProgress = false;
    updateSaveJobButton();
    return;
  }

  fileName.textContent = file.name;
  fileSize.textContent = `Archivo CSV · ${formatFileSize(file.size)}`;
  fileStatus.hidden = false;
  uploadInProgress = true;
  updateSaveJobButton();
  setFormStatus(datasetUploadStatus, "Cargando y validando el archivo CSV…", "loading");

  const controller = new AbortController();
  uploadController = controller;
  try {
    const response = await fetch(
      `/datasets/upload?filename=${encodeURIComponent(file.name)}`,
      {
        method: "POST",
        headers: { "Content-Type": "text/csv", Accept: "application/json" },
        body: file,
        signal: controller.signal,
      },
    );
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(getApiError(payload));
    }
    if (currentSequence !== uploadSequence) {
      return;
    }
    if (!Array.isArray(payload.headers) || payload.headers.length < 2) {
      throw new Error("FastAPI no devolvió los encabezados del CSV.");
    }

    uploadedDataset = payload;
    targetColumn.replaceChildren();
    const placeholder = new Option("Selecciona la columna objetivo", "");
    placeholder.disabled = true;
    placeholder.selected = true;
    targetColumn.append(placeholder);
    for (const header of payload.headers) {
      targetColumn.append(new Option(header, header));
    }
    targetField.hidden = false;
    fileSize.textContent = `${formatFileSize(payload.file_size_bytes)} · ${payload.row_count} filas · ${payload.column_count} columnas`;
    updateTargetHint();
    updateFeatureColumns();
    setFormStatus(
      datasetUploadStatus,
      `CSV validado y guardado. Dataset ${payload.dataset_id}.`,
      "success",
    );
  } catch (error) {
    if (currentSequence === uploadSequence && error.name !== "AbortError") {
      clearUploadedDataset();
      setFormStatus(datasetUploadStatus, error.message, "error");
    }
  } finally {
    if (currentSequence === uploadSequence) {
      uploadController = null;
      uploadInProgress = false;
      updateSaveJobButton();
    }
  }
}

function formatTimestamp(value) {
  if (!value) {
    return "Sin dato";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "Fecha no válida";
  }
  return date.toLocaleString("es-ES", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function setStateTag(element, state, label) {
  element.dataset.state = state || "unavailable";
  element.textContent = label || stateLabels[state] || String(state || "NO DISPONIBLE").toUpperCase();
}

function setMessage(state, title, detail) {
  technicalMessage.dataset.state = state;
  const paragraph = technicalMessage.querySelector("p");
  const strong = document.createElement("strong");
  strong.textContent = title;
  paragraph.replaceChildren(strong, document.createTextNode(` ${detail}`));
}

function showEmpty(container, message) {
  const paragraph = document.createElement("p");
  paragraph.className = "empty-state";
  paragraph.textContent = message;
  container.replaceChildren(paragraph);
}

function createDetailLine(label, value, className) {
  const line = document.createElement("span");
  line.className = className;
  const strong = document.createElement("strong");
  strong.textContent = `${label}:`;
  line.append(strong, document.createTextNode(` ${value}`));
  return line;
}

function renderDatabaseNodes(section) {
  if (!section.items.length) {
    showEmpty(databaseNodes, section.message || "El backend no reportó endpoints SQL.");
    return;
  }

  const colors = ["node-blue", "node-mint", "node-peach"];
  const cards = section.items.map((node, index) => {
    const card = document.createElement("article");
    card.className = "topology-node";

    const icon = document.createElement("span");
    icon.className = `topology-node-icon ${colors[index % colors.length]}`;
    icon.setAttribute("aria-hidden", "true");
    icon.textContent = String(index + 1).padStart(2, "0");

    const heading = document.createElement("div");
    heading.className = "db-node-heading";
    const name = document.createElement("strong");
    name.textContent = node.name;
    const sqlChip = document.createElement("span");
    sqlChip.className = "node-state-chip";
    sqlChip.dataset.state = node.sql.state;
    sqlChip.textContent = node.sql.state === "available" ? "SQL disponible" : node.sql.state;
    heading.append(name, sqlChip);

    const ports = document.createElement("small");
    ports.className = "node-state";
    ports.textContent = `SQL ${node.sql_port} · Admin ${node.admin_port || "no configurado"}`;

    const membership = node.membership.state === "member"
      ? `Miembro del clúster · nodo ${node.membership.node_id}`
      : node.membership.state === "not_listed"
        ? "No aparece en la lista de nodos del clúster"
        : node.membership.message || "Membresía no consultable";

    const queryResult = node.sql.state === "available"
      ? `SELECT 1 → ${node.sql.result} · ${node.sql.latency_ms} ms`
      : node.sql.message || "Consulta SQL no disponible";
    const health = node.health.message
      ? `${stateLabels[node.health.state] || node.health.state}: ${node.health.message}`
      : stateLabels[node.health.state] || node.health.state;

    card.append(
      icon,
      heading,
      ports,
      createDetailLine("Clúster", membership, "node-state"),
      createDetailLine("Consulta", queryResult, "node-query"),
      createDetailLine("Salud Admin API", health, "node-health"),
    );
    return card;
  });
  databaseNodes.replaceChildren(...cards);
}

function renderLeadership(leadership) {
  setStateTag(leadershipState, leadership.state);
  if (leadership.state === "leader" && leadership.worker) {
    const worker = leadership.worker;
    leadershipDetail.replaceChildren(
      createDetailLine("Coordinador", worker.hostname || "Hostname no disponible", "status-detail"),
      createDetailLine("Worker ID", worker.node_id, "status-detail"),
      createDetailLine("Época", leadership.epoch, "status-detail"),
      createDetailLine("Lease hasta", formatTimestamp(leadership.lease_until), "status-detail"),
    );
    return;
  }

  if (leadership.state === "no_leader") {
    leadershipDetail.textContent = "La consulta confirmó que no hay un lease de liderazgo vigente.";
    return;
  }
  leadershipDetail.textContent = leadership.message || "No se pudo consultar el liderazgo.";
}

function renderWorkers(section) {
  if (section.state !== "available") {
    setStateTag(workersState, section.state);
    showEmpty(workersList, section.message || "Falló la consulta de Workers.");
    return;
  }

  const counts = section.items.reduce((totals, worker) => {
    totals[worker.status] = (totals[worker.status] || 0) + 1;
    return totals;
  }, {});
  const summary = `${section.items.length} · ${counts.online || 0} online · ${counts.busy || 0} busy · ${counts.offline || 0} offline`;
  setStateTag(workersState, "available", summary);

  if (!section.items.length) {
    showEmpty(workersList, "La consulta fue exitosa; no hay Workers registrados.");
    return;
  }

  const rows = section.items.map((worker) => {
    const row = document.createElement("div");
    row.className = "worker-row";

    const identity = document.createElement("span");
    identity.className = "worker-identity";
    const hostname = document.createElement("strong");
    hostname.textContent = worker.hostname || "Hostname no disponible";
    const id = document.createElement("small");
    id.textContent = worker.node_id;
    identity.append(hostname, id);

    const state = document.createElement("span");
    state.className = "record-status";
    state.dataset.state = worker.status;
    state.textContent = stateLabels[worker.status] || worker.status;

    const metadata = document.createElement("span");
    metadata.className = "worker-meta";
    const heartbeat = worker.last_heartbeat
      ? `Heartbeat ${formatTimestamp(worker.last_heartbeat)}`
      : "Sin heartbeat registrado";
    const activeTasks = worker.active_tasks === null || worker.active_tasks === undefined
      ? "tareas activas sin métrica"
      : `${worker.active_tasks} tareas activas`;
    metadata.textContent = `${heartbeat} · ${activeTasks}`;
    row.append(identity, state, metadata);
    return row;
  });
  workersList.replaceChildren(...rows);
}

function renderReplication(section) {
  setStateTag(replicationState, section.state);
  replicationNote.textContent = section.interpretation || "";

  if (!section.items.length) {
    showEmpty(replicationList, section.message || "No hay reportes de rangos disponibles.");
    return;
  }

  const rows = section.items.map((node) => {
    const row = document.createElement("div");
    row.className = "replication-row";
    const title = document.createElement("strong");
    title.textContent = `Nodo ${node.node_id}`;
    const state = document.createElement("span");
    state.className = "record-status";
    state.dataset.state = node.state;
    state.textContent = stateLabels[node.state] || node.state;
    row.append(title, state);

    const metrics = document.createElement("span");
    metrics.className = "replication-metrics";
    metrics.textContent = node.state === "available" || node.state === "partial"
      ? `${node.range_count} rangos · ${node.underreplicated_count} subreplicados · ${node.unavailable_count} no disponibles · ${node.overreplicated_count} sobrerreplicados`
      : node.message || "No se pudo consultar";
    row.append(metrics);
    return row;
  });
  replicationList.replaceChildren(...rows);
  replicationNote.textContent = section.updated_at
    ? `${section.interpretation || ""} Última lectura: ${formatTimestamp(section.updated_at)}.`
    : section.interpretation || "";
}

function renderRecordSection(container, section, kind) {
  if (section.state !== "available") {
    showEmpty(container, section.message || "Falló la consulta de registros.");
    return;
  }
  if (!section.items.length) {
    showEmpty(container, "Consulta exitosa; no hay registros.");
    return;
  }

  const rows = section.items.map((record) => {
    const row = document.createElement("div");
    row.className = "record-item";
    const primary = document.createElement("span");
    primary.className = "record-primary";
    const title = document.createElement("strong");
    const detail = document.createElement("small");

    if (kind === "jobs") {
      title.textContent = record.algorithm;
      detail.textContent = `${record.target_column} · ${formatTimestamp(record.created_at)}`;
    } else if (kind === "tasks") {
      title.textContent = `Partición ${record.partition_index}`;
      detail.textContent = `Job ${record.job_id} · ${formatTimestamp(record.activity_at)}`;
    } else {
      title.textContent = record.name;
      detail.textContent = `${record.row_count ?? "Filas sin dato"} filas · ${formatTimestamp(record.created_at)}`;
    }

    primary.append(title, detail);
    const state = document.createElement("span");
    state.className = "record-status";
    state.dataset.state = record.status || "available";
    state.textContent = record.status || "registrado";
    row.append(primary, state);
    return row;
  });
  container.replaceChildren(...rows);
}

function renderEvents(section) {
  setStateTag(
    eventsState,
    section.state,
    section.state === "available"
      ? `${section.items.length} EVENTOS · ${formatTimestamp(section.updated_at)}`
      : null,
  );
  if (section.state !== "available") {
    showEmpty(eventsList, section.message || "Falló la consulta de eventos.");
    return;
  }
  if (!section.items.length) {
    showEmpty(eventsList, "La consulta fue exitosa; no hay eventos registrados.");
    return;
  }

  const rows = section.items.map((event) => {
    const row = document.createElement("div");
    row.className = "event-item";
    const primary = document.createElement("span");
    primary.className = "event-primary";
    const title = document.createElement("strong");
    title.textContent = event.event_type || "Evento sin tipo";
    const detail = document.createElement("small");
    detail.textContent = `Nodo informante ${event.reporting_id ?? "sin dato"} · ${formatTimestamp(event.timestamp)}`;
    primary.append(title, detail);
    row.append(primary);
    return row;
  });
  eventsList.replaceChildren(...rows);
}

function renderTechnicalStatus(data) {
  lastSuccessfulUpdate = data.updated_at;
  lastUpdated.textContent = `Actualizado ${formatTimestamp(data.updated_at)}`;

  const overall = data.overall;
  setStateTag(overallState, overall.state);
  const connectedNodes = data.database_nodes.items.filter(
    (node) => node.sql.state === "available",
  ).length;
  const memberNodes = data.database_nodes.items.filter(
    (node) => node.membership.state === "member",
  ).length;
  const readyNodes = data.database_nodes.items.filter(
    (node) => node.health.state === "ready",
  ).length;
  overallDetail.textContent = `${connectedNodes}/${data.database_nodes.items.length} endpoints SQL · ${memberNodes} miembros · ${readyNodes} Admin API listos · pool ${stateLabels[overall.database_pool] || overall.database_pool}.`;

  renderLeadership(data.leadership);
  renderDatabaseNodes(data.database_nodes);
  renderWorkers(data.workers);
  renderReplication(data.replication);
  renderRecordSection(jobsList, data.jobs, "jobs");
  renderRecordSection(tasksList, data.tasks, "tasks");
  renderRecordSection(datasetsList, data.datasets, "datasets");
  tasksNote.textContent = data.tasks.timestamp_note || "";
  renderEvents(data.events);
  setMessage("success", "Datos consultados.", "Los estados se actualizan automáticamente cada 4 segundos.");
}

function renderRefreshError(error) {
  const isFilePage = window.location.protocol === "file:";
  const message = isFilePage
    ? "Abre la interfaz desde http://127.0.0.1:8000/ para consultar el backend."
    : `No se pudo consultar FastAPI (${error.message}).`;
  setMessage("error", "Error de actualización.", message);
  setStateTag(overallState, "error", "ERROR DE CONSULTA");
  lastUpdated.textContent = lastSuccessfulUpdate
    ? `Último éxito ${formatTimestamp(lastSuccessfulUpdate)}`
    : "Sin actualización";
  overallDetail.textContent = lastSuccessfulUpdate
    ? `Los datos visibles corresponden al último éxito: ${formatTimestamp(lastSuccessfulUpdate)}.`
    : "Aún no hay una consulta exitosa.";

  if (!lastSuccessfulUpdate) {
    setStateTag(leadershipState, "error");
    leadershipDetail.textContent = message;
    setStateTag(workersState, "error");
    setStateTag(replicationState, "error");
    setStateTag(eventsState, "error");
    showEmpty(databaseNodes, message);
    showEmpty(workersList, message);
    showEmpty(replicationList, message);
    showEmpty(jobsList, message);
    showEmpty(tasksList, message);
    showEmpty(datasetsList, message);
    showEmpty(eventsList, message);
  }
}

async function refreshTechnicalStatus() {
  if (refreshInProgress) {
    return;
  }
  refreshInProgress = true;
  try {
    const response = await fetch("/cluster/technical-status", {
      cache: "no-store",
      headers: { Accept: "application/json" },
    });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    renderTechnicalStatus(await response.json());
  } catch (error) {
    renderRefreshError(error);
  } finally {
    refreshInProgress = false;
  }
}

function startTechnicalMonitoring() {
  stopTechnicalMonitoring();
  refreshTechnicalStatus();
  refreshTimer = window.setInterval(refreshTechnicalStatus, 4000);
}

function stopTechnicalMonitoring() {
  if (refreshTimer !== null) {
    window.clearInterval(refreshTimer);
    refreshTimer = null;
  }
}

fileInput.addEventListener("change", () => {
  uploadSelectedFile(fileInput.files[0]);
});

targetColumn.addEventListener("change", () => {
  updateFeatureColumns();
  updateSaveJobButton();
});

modelSelect.addEventListener("change", updateTargetHint);

trainingForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (saveJobButton.disabled || !uploadedDataset) {
    return;
  }

  jobSubmitting = true;
  updateSaveJobButton();
  setFormStatus(configurationStatus, "Validando y guardando la configuración…", "loading");
  try {
    const response = await fetch("/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({
        dataset_id: uploadedDataset.dataset_id,
        algorithm: modelSelect.value,
        target_column: targetColumn.value,
      }),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(getApiError(payload));
    }
    setFormStatus(
      configurationStatus,
      `Configuración guardada como Job pendiente: ${payload.job_id}. No se inició el entrenamiento.`,
      "success",
    );
  } catch (error) {
    setFormStatus(configurationStatus, error.message, "error");
  } finally {
    jobSubmitting = false;
    updateSaveJobButton();
  }
});

updateTargetHint();

openDemoButton.addEventListener("click", () => {
  trainingView.hidden = true;
  demoView.hidden = false;
  backButton.focus();
  startTechnicalMonitoring();
});

backButton.addEventListener("click", () => {
  stopTechnicalMonitoring();
  demoView.hidden = true;
  trainingView.hidden = false;
  openDemoButton.focus();
});
