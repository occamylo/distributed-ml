import { ApiError, request } from "../api.js";

// GET /health responde 503 cuando la base de datos no está disponible:
// para la interfaz eso es un estado, no un error.
export async function getHealth() {
  try {
    const data = await request("/health", { timeoutMs: 8000 });
    return { healthy: true, database: data.database };
  } catch (error) {
    if (error instanceof ApiError && error.status === 503) {
      return { healthy: false, database: "unavailable" };
    }
    return { healthy: false, database: "unknown", apiDown: error.status === 0, message: error.message };
  }
}

export function getCluster() {
  return request("/cluster");
}

// Vista técnica: estado de CockroachDB según su API de administración, eventos
// y registros recientes. El backend lo cachea unos segundos.
export function getTechnicalStatus() {
  return request("/cluster/technical-status");
}
