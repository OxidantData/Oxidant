export const APP_ID = "oxidant-local";

export interface JobData {
  jobId: number;
  name: string;
  description?: string;
  submissionTime?: string;
  completionTime?: string;
  stageIds: number[];
  status: string;
  numTasks: number;
  numCompletedTasks: number;
}

export interface StageData {
  stageId: number;
  name: string;
  status: string;
  numTasks: number;
  numCompleteTasks: number;
  executorRunTime: number;
  shuffleReadBytes: number;
  shuffleWriteBytes: number;
  outputRecords: number;
  tasks?: TaskData[];
}

export interface TaskData {
  taskId: number;
  executorId: string;
  status: string;
  executorRunTime: number;
  outputRecords: number;
  shuffleReadBytes: number;
  shuffleWriteBytes: number;
}

export interface SqlExecution {
  id: number;
  description: string;
  status: string;
  duration?: number;
  physicalPlan: string;
  logicalPlan?: string;
}

export interface ExecutorSummary {
  id: string;
  hostPort: string;
  activeTasks: number;
  completedTasks: number;
  totalShuffleRead: number;
  totalShuffleWrite: number;
}

export interface StatementSchema {
  fields: { name: string; type: string }[];
}

/** Newest-first list entry from `GET /api/v1/statements`. */
export interface StatementSummary {
  statementId: string;
  sql: string;
  status: string;
  submittedAtMs: number;
  durationMs?: number;
}

/** Full status document from `GET /api/v1/statements/{id}` (and `?wait=true` submit). */
export interface StatementDoc extends StatementSummary {
  error?: string;
  rowCount?: number;
  schema?: StatementSchema;
}

/** Result document from `GET /api/v1/statements/{id}/result?format=json`. */
export interface StatementResult {
  schema: StatementSchema;
  rows: Record<string, unknown>[];
  rowCount: number;
  truncated: boolean;
}

export interface CatalogInfo {
  name: string;
  isCurrent: boolean;
}

export interface CatalogTable {
  name: string;
  type: string;
}

export interface CatalogColumn {
  name: string;
  type: string;
}

export interface AutocompleteSuggestion {
  kind: "catalog" | "namespace" | "table" | "column";
  name: string;
  qualified: string;
}

export interface ClusterStatus {
  mode: string;
  workers: string[];
  version: string;
  process: {
    memoryUsedMb: number;
    memoryTotalMb: number;
    cpuPercent: number;
  };
}

const base = `/api/v1/applications/${APP_ID}`;

export const DEFAULT_ROW_LIMIT = 100;
export const MAX_ROW_LIMIT = 10_000;

/** Read a JSON body, surfacing the API's `{"error": "..."}` message on non-2xx. */
async function readJson<T>(r: Response, path: string): Promise<T> {
  const data = (await r.json().catch(() => null)) as ({ error?: string } & T) | null;
  if (!r.ok) throw new Error(data?.error ?? `${r.status} ${path}`);
  return data as T;
}

/**
 * One JSON request against the server, with the `{"error": "..."}` convention applied. Bodies
 * are omitted entirely when `body` is undefined, so a bare POST/DELETE sends no content type.
 * A 204 (the dashboard delete) resolves to `null`.
 */
export async function apiJson<T>(
  method: string,
  path: string,
  body?: unknown
): Promise<T> {
  return readJson<T>(
    await fetch(path, {
      method,
      headers: body === undefined ? {} : { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    }),
    path
  );
}

async function get<T>(path: string): Promise<T> {
  return apiJson<T>("GET", path);
}

async function post<T>(path: string, body?: unknown): Promise<T> {
  return apiJson<T>("POST", path, body);
}

export const api = {
  applications: () => get<unknown[]>("/api/v1/applications"),
  jobs: () => get<JobData[]>(`${base}/jobs`),
  stages: (details = true) =>
    get<StageData[]>(`${base}/stages?details=${details}`),
  sql: () => get<SqlExecution[]>(`${base}/sql`),
  executors: () => get<ExecutorSummary[]>(`${base}/executors`),
  environment: () =>
    get<{ sparkProperties: Record<string, string> }>(`${base}/environment`),
  statements: {
    list: () => get<{ statements: StatementSummary[] }>("/api/v1/statements"),
    submit: (sql: string, waitTimeoutSecs = 60) =>
      post<StatementDoc>(`/api/v1/statements?wait=true&timeout=${waitTimeoutSecs}`, { sql }),
    get: (id: string) => get<StatementDoc>(`/api/v1/statements/${id}`),
    result: (id: string, limit = 500, format: "json" | "csv" = "json") =>
      get<StatementResult>(`/api/v1/statements/${id}/result?format=${format}&limit=${limit}`),
    csv: (id: string, limit = 10_000) =>
      fetch(`/api/v1/statements/${id}/result?format=csv&limit=${limit}`).then((r) => {
        if (!r.ok) throw new Error(`${r.status} csv download`);
        return r.text();
      }),
    json: (id: string, limit = 10_000) =>
      fetch(`/api/v1/statements/${id}/result?format=json&limit=${limit}`).then((r) => {
        if (!r.ok) throw new Error(`${r.status} json download`);
        return r.json() as Promise<StatementResult>;
      }),
    cancel: (id: string) =>
      post<{ statementId: string; status: string }>(`/api/v1/statements/${id}/cancel`),
  },
  catalogs: {
    list: () => get<{ catalogs: CatalogInfo[] }>("/api/v1/catalogs"),
    namespaces: (catalog: string) =>
      get<{ namespaces: string[] }>(`/api/v1/catalogs/${catalog}/namespaces`),
    tables: (catalog: string, namespace: string) =>
      get<{ tables: CatalogTable[] }>(
        `/api/v1/catalogs/${catalog}/tables?namespace=${encodeURIComponent(namespace)}`
      ),
    columns: (catalog: string, namespace: string, table: string) =>
      get<{ columns: CatalogColumn[] }>(
        `/api/v1/catalogs/${catalog}/tables/${table}/columns?namespace=${encodeURIComponent(
          namespace
        )}`
      ),
    autocomplete: (prefix: string) =>
      get<{ suggestions: AutocompleteSuggestion[] }>(
        `/api/v1/catalogs/autocomplete?prefix=${encodeURIComponent(prefix)}`
      ),
  },
  cluster: {
    status: () => get<ClusterStatus>("/api/v1/cluster/status"),
    logs: () => get<{ logs: string[] }>("/api/v1/logs"),
  },
};

const TERMINAL_STATUSES = new Set(["succeeded", "failed", "canceled"]);

/**
 * Submit SQL via `?wait=true&timeout=60` and keep polling `GET /{id}` every 1s
 * while it is still pending/running. `onUpdate` fires on each status snapshot.
 */
export async function runStatement(
  sql: string,
  onUpdate?: (doc: StatementDoc) => void
): Promise<StatementDoc> {
  let doc = await api.statements.submit(sql);
  while (!TERMINAL_STATUSES.has(doc.status)) {
    onUpdate?.(doc);
    await new Promise((r) => setTimeout(r, 1000));
    doc = await api.statements.get(doc.statementId);
  }
  onUpdate?.(doc);
  return doc;
}

export function fmtMs(ms?: number | null): string {
  if (ms == null) return "—";
  return ms < 1000 ? `${ms} ms` : `${(ms / 1000).toFixed(2)} s`;
}

export function fmtBytes(n?: number): string {
  if (n == null || n === 0) return "0";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  let value = n;
  let unitIdx = 0;
  while (value >= 1024 && unitIdx < units.length - 1) {
    value /= 1024;
    unitIdx++;
  }
  return `${value.toFixed(1)} ${units[unitIdx]}`;
}

export function downloadBlob(
  content: string | Blob,
  filename: string,
  contentType: string
) {
  const blob = content instanceof Blob ? content : new Blob([content], { type: contentType });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}

export function jobDuration(j: JobData): number | null {
  if (!j.submissionTime || !j.completionTime) return null;
  return new Date(j.completionTime).getTime() - new Date(j.submissionTime).getTime();
}
