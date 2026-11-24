import { request } from "../api.js";

// Workers registrados con sus métricas, si su heartbeat es reciente
// (is_fresh) y si tienen el lease de coordinador (is_leader).
export function listWorkers() {
  return request("/nodes");
}

export function isOnline(worker) {
  return worker.is_fresh && (worker.status === "online" || worker.status === "busy");
}
