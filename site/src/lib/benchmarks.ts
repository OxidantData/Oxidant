import clickbenchRaw from "../data/benchmarks.json";
import tpchRaw from "../data/tpch.json";
import tpcdsRaw from "../data/tpcds.json";
import tpchNocacheRaw from "../data/tpch-nocache.json";
import tpcdsNocacheRaw from "../data/tpcds-nocache.json";

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
export const tpchNocacheBenchmarks = tpchNocacheRaw as Benchmarks;
export const tpcdsNocacheBenchmarks = tpcdsNocacheRaw as Benchmarks;

/** One distinct, solid color per engine — shared by every chart so bars/legends stay consistent.
 *  Oxidant keeps the brand orange; the others get clearly distinguishable hues (not faint grey). */
export const ENGINE_COLORS: Record<string, string> = {
  oxidant: "var(--oxidant-accent)", // brand orange
  "oxidant-dist": "var(--oxidant-accent)", // brand orange (distributed TPC-H/DS suites)
  sail: "#2563eb", // blue
  spark: "#64748b", // slate
  "spark-emr": "#64748b", // slate (Spark on EMR, TPC-H/DS head-to-head)
  gluten: "#16a34a", // green
  duckdb: "#eab308", // gold
};

export function engineColor(key: string): string {
  return ENGINE_COLORS[key] ?? "var(--oxidant-text-muted)";
}

export function isMeasured(e: Engine): boolean {
  return e.total != null;
}

export const measuredEngines = benchmarks.engines.filter(isMeasured);

/** Oxidant's speedup vs another engine as a multiple (e.g. 1.24 = 24% faster), or null. */
export function speedupVs(otherKey: string, suite: Benchmarks = benchmarks): number | null {
  const oxidant = suite.engines.find((e) => e.key === "oxidant");
  const other = suite.engines.find((e) => e.key === otherKey);
  if (!oxidant?.total || !other?.total) return null;
  return other.total / oxidant.total;
}

/**
 * Oxidant's wall-clock time saved vs another engine, as a percentage
 * (e.g. 80 = Oxidant completes in 20% of the other engine's time), or null.
 * Negative when the other engine is faster. This is the site's headline
 * convention: "% faster" always means (other − oxidant) ÷ other.
 */
export function pctFasterVs(otherKey: string, suite: Benchmarks = benchmarks): number | null {
  const oxidant = suite.engines.find((e) => e.key === "oxidant");
  const other = suite.engines.find((e) => e.key === otherKey);
  if (!oxidant?.total || !other?.total) return null;
  return (1 - oxidant.total / other.total) * 100;
}

/** Like [`pctFasterVs`] but keyed on the highlighted engine rather than `oxidant`. */
export function pctFasterOfHighlight(other: Engine, suite: Benchmarks): number | null {
  const oxidant = suite.engines.find((e) => e.key === "oxidant") ?? suite.engines.find((e) => e.highlight);
  if (!oxidant?.total || other.total == null) return null;
  return (1 - oxidant.total / other.total) * 100;
}
