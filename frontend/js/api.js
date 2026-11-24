// Cliente HTTP de la API. El frontend se sirve desde el mismo origen que
// FastAPI, así que las rutas son relativas y no hace falta CORS.

export class ApiError extends Error {
  constructor(status, detail) {
    super(detail);
    this.status = status;
    this.detail = detail;
  }
}

const DEFAULT_TIMEOUT_MS = 20000;

export async function request(path, { method = "GET", json, body, timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const headers = { Accept: "application/json" };
  let payload = body;
  if (json !== undefined) {
    headers["Content-Type"] = "application/json";
    payload = JSON.stringify(json);
  }

  let response;
  try {
    response = await fetch(path, { method, headers, body: payload, signal: controller.signal });
  } catch (error) {
    throw new ApiError(
      0,
      error.name === "AbortError"
        ? "La API tardó demasiado en responder."
        : "No se pudo conectar con la API. ¿Está corriendo FastAPI en este equipo?"
    );
  } finally {
    clearTimeout(timer);
  }

  if (response.status === 204) return null;
  let data = null;
  try {
    data = await response.json();
  } catch {
    data = null;
  }
  if (!response.ok) {
    throw new ApiError(response.status, describeError(data) || `La API respondió ${response.status}.`);
  }
  return data;
}

function describeError(data) {
  if (!data) return "";
  const detail = data.detail;
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) {
    return detail
      .map((item) => {
        const field = (item.loc || []).slice(1).join(".");
        return field ? `${field}: ${item.msg}` : item.msg;
      })
      .join(" · ");
  }
  return "";
}
