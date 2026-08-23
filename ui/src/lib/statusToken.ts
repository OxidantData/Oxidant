/**
 * The driver status token, and what to say when a gated route refuses.
 *
 * `OXIDANT_STATUS_TOKEN` gates the driver's operational routes — `/api/status`, the pipeline
 * list, a connector log tail, and `/api/v1/logs`, which is the one this app reads (the Cluster
 * page's process-log pane). The buffer carries every logged field, hosts and query text
 * included, and the server is wrapped in a permissive CORS layer, so it is not a route that can
 * be left open.
 *
 * The token is kept in `localStorage` under the same key the embedded console uses, so a token
 * pasted into either console works in the other, and it is only ever sent to this driver, as an
 * `Authorization` header — never in a URL or a query string, where it would land in logs.
 */
export const STATUS_TOKEN_KEY = "oxidant.statusToken";

export function statusToken(): string {
  try {
    return localStorage.getItem(STATUS_TOKEN_KEY) ?? "";
  } catch {
    // Private-mode Safari and friends: no storage, no token, still a working page.
    return "";
  }
}

export function setStatusToken(token: string): void {
  try {
    const trimmed = token.trim();
    if (trimmed) localStorage.setItem(STATUS_TOKEN_KEY, trimmed);
    else localStorage.removeItem(STATUS_TOKEN_KEY);
  } catch {
    /* nothing to persist to; the caller's in-memory value still applies to this page */
  }
}

export function statusAuthHeaders(): Record<string, string> {
  const token = statusToken();
  return token ? { Authorization: `Bearer ${token}` } : {};
}

/**
 * Why the log pane is empty, in terms of the thing that made it empty.
 *
 * `readJson` throws `"<status> <path>"` for a response with no `{"error"}` body, which is what
 * every refusal here looks like. `null` means "nothing went wrong" — the pane is simply empty.
 */
export function logBufferNotice(error: string | null): {
  message: string;
  needsToken: boolean;
} | null {
  if (!error) return null;
  if (/\b40[13]\b/.test(error)) {
    return {
      message:
        "The driver rejected this token. GET /api/v1/logs is gated by OXIDANT_STATUS_TOKEN; check the value the server was started with.",
      needsToken: true,
    };
  }
  if (/\b404\b/.test(error)) {
    return {
      message:
        "No log buffer to read here. GET /api/v1/logs answers 404 when OXIDANT_STATUS_TOKEN is unset on the driver — the route is gated, because the buffer carries every logged field — and on a process that keeps no buffer at all, such as a standalone history server.",
      needsToken: true,
    };
  }
  return { message: `Could not read the log buffer: ${error}`, needsToken: false };
}
