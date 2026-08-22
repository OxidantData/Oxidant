/**
 * Dashboard documents — the browser half of `/api/dashboards` (see
 * `crates/oxidant-ui-server/src/dashboards.rs`).
 *
 * A dashboard is a react-grid-layout `layout` array plus a list of widget specs. Each widget
 * is one SQL statement; the server stores it and never runs it. Execution goes through the
 * same statement API the SQL Editor uses (`runStatement` in `lib/api.ts`), so a widget query
 * shows up on the Jobs/SQL monitoring pages like any other.
 */
import type { LayoutItem } from "react-grid-layout";
import { apiJson } from "@/lib/api";

/** The widget types dashboards v1 ships. The server rejects anything else with a 400. */
export const WIDGET_TYPES = [
  "bar",
  "line",
  "area",
  "pie",
  "scatter",
  "table",
  "kpi",
] as const;

export type WidgetType = (typeof WIDGET_TYPES)[number];

/** Human labels for the widget picker, in the order it offers them. */
export const WIDGET_TYPE_LABELS: Record<WidgetType, string> = {
  bar: "Bar",
  line: "Line",
  area: "Area",
  pie: "Pie",
  scatter: "Scatter",
  table: "Table",
  kpi: "KPI counter",
};

/**
 * Per-type render options. Everything here is optional and the server treats it as opaque
 * JSON — the mapping from SQL results to an ECharts option lives in `lib/widgetData.ts`.
 */
export interface WidgetOptions {
  /** bar / line / area: stack the series on top of each other. */
  stacked?: boolean;
  /** bar: swap the axes (categories down the left). */
  horizontal?: boolean;
  /** line / area: monotone interpolation instead of straight segments. */
  smooth?: boolean;
  /** All charts: show the series legend. Defaults to on when there is more than one series. */
  legend?: boolean;
  /** kpi: suffix appended to the formatted value (`%`, ` rows`, …). */
  unit?: string;
  /** kpi: fixed number of decimal places. */
  decimals?: number;
  /** table: rows per page. */
  pageSize?: number;
}

export interface WidgetSpec {
  id: string;
  type: WidgetType;
  title: string;
  sql: string;
  options: WidgetOptions;
}

/**
 * One grid cell. This is react-grid-layout's own item type: the server stores the array
 * verbatim (validating only `i`/`x`/`y`/`w`/`h`) so the grid stays the single source of truth
 * for what a layout means.
 */
export type DashboardLayoutItem = LayoutItem;

export interface Dashboard {
  id: string;
  name: string;
  layout: DashboardLayoutItem[];
  widgets: WidgetSpec[];
  /** View-mode auto-refresh period. Absent = manual refresh only. */
  refreshIntervalSecs?: number;
  createdAtMs: number;
  updatedAtMs: number;
}

/** What the list endpoint returns — no widget SQL, so the list page stays cheap. */
export interface DashboardSummary {
  id: string;
  name: string;
  widgetCount: number;
  refreshIntervalSecs: number | null;
  createdAtMs: number;
  updatedAtMs: number;
}

/** Fields a create/patch may carry. Absent keys are left alone by the server. */
export interface DashboardPatch {
  name?: string;
  layout?: DashboardLayoutItem[];
  widgets?: WidgetSpec[];
  /** `null` clears auto-refresh; `undefined` leaves it as it was. */
  refreshIntervalSecs?: number | null;
}

export const dashboardsApi = {
  list: () =>
    apiJson<{ dashboards: DashboardSummary[] }>("GET", "/api/dashboards").then(
      (d) => d.dashboards
    ),
  get: (id: string) => apiJson<Dashboard>("GET", `/api/dashboards/${id}`),
  create: (body: DashboardPatch & { name: string }) =>
    apiJson<Dashboard>("POST", "/api/dashboards", body),
  update: (id: string, patch: DashboardPatch) =>
    apiJson<Dashboard>("PATCH", `/api/dashboards/${id}`, patch),
  remove: (id: string) => apiJson<null>("DELETE", `/api/dashboards/${id}`),
};

/** The auto-refresh periods the toolbar offers. `null` is "off". */
export const REFRESH_CHOICES: { label: string; secs: number | null }[] = [
  { label: "Off", secs: null },
  { label: "5s", secs: 5 },
  { label: "15s", secs: 15 },
  { label: "30s", secs: 30 },
  { label: "1m", secs: 60 },
  { label: "5m", secs: 300 },
  { label: "15m", secs: 900 },
];

/** Default cell size for a freshly added widget, in grid units (the grid is 12 columns). */
export const DEFAULT_WIDGET_SIZE: Record<WidgetType, { w: number; h: number }> = {
  bar: { w: 6, h: 8 },
  line: { w: 6, h: 8 },
  area: { w: 6, h: 8 },
  pie: { w: 4, h: 8 },
  scatter: { w: 6, h: 8 },
  table: { w: 6, h: 8 },
  kpi: { w: 3, h: 4 },
};

/**
 * Place a new widget below everything already on the grid, so adding one never lands on top
 * of an existing card whatever the current arrangement is.
 */
export function appendToLayout(
  layout: DashboardLayoutItem[],
  widget: WidgetSpec
): DashboardLayoutItem {
  const size = DEFAULT_WIDGET_SIZE[widget.type];
  const bottom = layout.reduce((max, item) => Math.max(max, item.y + item.h), 0);
  return { i: widget.id, x: 0, y: bottom, ...size, minW: 2, minH: 3 };
}

/** Widget ids are also react keys and grid item ids; they only have to be unique per doc. */
export function newWidgetId(existing: WidgetSpec[]): string {
  const taken = new Set(existing.map((w) => w.id));
  for (let n = 1; ; n++) {
    const id = `w${n}`;
    if (!taken.has(id)) return id;
  }
}

/** `12345678` → `12,345,678`; used for widget counts and row counts in list/detail chrome. */
export function fmtCount(n: number): string {
  return n.toLocaleString();
}

/** Coarse "how long ago" for the list page's Updated column. */
export function fmtRelativeMs(ms: number, now = Date.now()): string {
  const delta = Math.max(0, now - ms);
  const mins = Math.floor(delta / 60_000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days}d ago`;
  return new Date(ms).toLocaleDateString();
}
