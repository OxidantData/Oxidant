export const APP_ID = "weft-local";

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

const base = `/api/v1/applications/${APP_ID}`;

async function get<T>(path: string): Promise<T> {
  const r = await fetch(path);
  if (!r.ok) throw new Error(`${r.status} ${path}`);
  return r.json();
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
  sparkProxy: (url: string) =>
    get<unknown>(`/api/v1/spark-proxy?url=${encodeURIComponent(url)}`),
};

export function fmtMs(ms?: number | null): string {
  if (ms == null) return "—";
  return ms < 1000 ? `${ms} ms` : `${(ms / 1000).toFixed(2)} s`;
}

export function fmtBytes(n?: number): string {
  if (n == null || n === 0) return "0";
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

export function jobDuration(j: JobData): number | null {
  if (!j.submissionTime || !j.completionTime) return null;
  return new Date(j.completionTime).getTime() - new Date(j.submissionTime).getTime();
}
