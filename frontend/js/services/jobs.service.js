import { ApiError, request } from "../api.js";

export function listJobs() {
  return request("/jobs");
}

export function getJob(jobId) {
  return request(`/jobs/${jobId}`);
}

export function getTasks(jobId) {
  return request(`/jobs/${jobId}/tasks`);
}

// Un Job sin resultado responde 404: se devuelve null.
export async function getResult(jobId) {
  try {
    return await request(`/jobs/${jobId}/result`);
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) return null;
    throw error;
  }
}

export function createJob(payload) {
  return request("/jobs", { method: "POST", json: payload, timeoutMs: 120000 });
}
