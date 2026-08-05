import clickbenchRaw from "../data/benchmarks.json";
import tpchRaw from "../data/tpch.json";
import tpcdsRaw from "../data/tpcds.json";

export interface Engine {
  key: string;
  name: string;
  highlight: boolean;
  /** Total over the common set (queries ALL engines completed); the fair headline number. */
  total: number | null;
  /** This engine's full total over every query it completed. */
  totalAll?: number | null;
  /** "measured (EC2 c6a.4xlarge <date>)" or "pending". */
  source: string;
  /** Per-query hot seconds (min of try2/try3); entries null for failed queries. */
  perQuery: (number | null)[];
  failures: number | null;
  /** Indices of queries this engine could not execute. */
  failedQueries?: number[];
}

export interface Benchmarks {
  dataset: string;
  machine: string;
  runDate: string | null;
  queryCount: number;
  /** Number of queries every measured engine completed (basis for the fair total). */
  commonCount?: number;
  method: string;
  engines: Engine[];
}

export const benchmarks = clickbenchRaw as Benchmarks;
export const tpchBenchmarks = tpchRaw as Benchmarks;
export const tpcdsBenchmarks = tpcdsRaw as Benchmarks;

/** One distinct, solid color per engine — shared by every chart so bars/legends stay consistent.
 *  Weft keeps the brand orange; the others get clearly distinguishable hues (not faint grey). */
export const ENGINE_COLORS: Record<string, string> = {
  weft: "var(--weft-accent)", // brand orange
  "weft-dist": "var(--weft-accent)", // brand orange (distributed TPC-H/DS suites)
  sail: "#2563eb", // blue
  spark: "#64748b", // slate
  "spark-emr": "#64748b", // slate (Spark on EMR, TPC-H/DS head-to-head)
  gluten: "#16a34a", // green
  duckdb: "#eab308", // gold
};

export function engineColor(key: string): string {
  return ENGINE_COLORS[key] ?? "var(--weft-text-muted)";
}

export function isMeasured(e: Engine): boolean {
  return e.total != null;
}

export const measuredEngines = benchmarks.engines.filter(isMeasured);

/** Weft's speedup vs another engine as a multiple (e.g. 1.24 = 24% faster), or null. */
export function speedupVs(otherKey: string, suite: Benchmarks = benchmarks): number | null {
  const weft = suite.engines.find((e) => e.key === "weft");
  const other = suite.engines.find((e) => e.key === otherKey);
  if (!weft?.total || !other?.total) return null;
  return other.total / weft.total;
}

/**
 * Weft's wall-clock time saved vs another engine, as a percentage
 * (e.g. 80 = Weft completes in 20% of the other engine's time), or null.
 * Negative when the other engine is faster. This is the site's headline
 * convention: "% faster" always means (other − weft) ÷ other.
 */
export function pctFasterVs(otherKey: string, suite: Benchmarks = benchmarks): number | null {
  const weft = suite.engines.find((e) => e.key === "weft");
  const other = suite.engines.find((e) => e.key === otherKey);
  if (!weft?.total || !other?.total) return null;
  return (1 - weft.total / other.total) * 100;
}

/** Like [`pctFasterVs`] but keyed on the highlighted engine rather than `weft`. */
export function pctFasterOfHighlight(other: Engine, suite: Benchmarks): number | null {
  const weft = suite.engines.find((e) => e.key === "weft") ?? suite.engines.find((e) => e.highlight);
  if (!weft?.total || other.total == null) return null;
  return (1 - weft.total / other.total) * 100;
}
