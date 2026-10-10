/** Thin fetch wrapper. Returns parsed JSON; throws ApiError on non-2xx or invalid JSON. */

const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";

export class ApiError extends Error {
  status: number;
  body: unknown;
  constructor(status: number, body: unknown, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.body = body;
  }
}

export async function apiGet<T>(path: string, params?: Record<string, unknown>): Promise<T> {
  const url = new URL(path, BASE_URL);
  if (params) {
    for (const [key, value] of Object.entries(params)) {
      if (value === undefined || value === null) continue;
      if (Array.isArray(value)) {
        for (const v of value) url.searchParams.append(key, String(v));
      } else {
        url.searchParams.set(key, String(value));
      }
    }
  }
  const response = await fetch(url.toString(), {
    method: "GET",
    headers: { Accept: "application/json" },
  });
  let body: unknown;
  try {
    body = await response.json();
  } catch {
    throw new ApiError(response.status, null, `${path}: invalid JSON response`);
  }
  if (!response.ok) {
    throw new ApiError(response.status, body, `${path}: HTTP ${response.status}`);
  }
  return body as T;
}
