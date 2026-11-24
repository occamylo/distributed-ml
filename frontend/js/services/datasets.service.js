import { request } from "../api.js";

export function listDatasets() {
  return request("/datasets");
}

// POST /datasets recibe el archivo como multipart (campo "file") y un
// nombre opcional. La API valida el CSV y responde 422 con el motivo.
export function uploadDataset(file, name) {
  const form = new FormData();
  form.append("file", file);
  if (name) form.append("name", name);
  return request("/datasets", { method: "POST", body: form, timeoutMs: 120000 });
}

export function getColumns(datasetId) {
  return request(`/datasets/${datasetId}/columns`);
}

export function deleteDataset(datasetId) {
  return request(`/datasets/${datasetId}`, { method: "DELETE" });
}
