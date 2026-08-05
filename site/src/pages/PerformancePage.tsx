import BenchmarkChart from "../components/BenchmarkChart";
import PerQueryChart from "../components/PerQueryChart";
import StatChips from "../components/StatChips";
import CodeBlock from "../components/CodeBlock";
import ThreadDivider from "../components/ThreadDivider";
import {
  benchmarks,
  tpchBenchmarks,
  tpcdsBenchmarks,
  tpchNocacheBenchmarks,
  tpcdsNocacheBenchmarks,
  type Benchmarks,
  type Engine,
} from "../lib/benchmarks";

const weft = benchmarks.engines.find((e) => e.key === "weft");

const REPO = "https://github.com/vamzi/weft";

function SourceTag({ source }: { source: string }) {
  const pending = source === "pending";
  return (
    <span
      className={`rounded-full px-2 py-0.5 text-[11px] font-medium ${
        pending ? "bg-bg-subtle text-muted" : "bg-success/10 text-success"
      }`}
    >
      {pending ? "pending" : "measured"}
    </span>
  );
}

function ProvenanceTable({ engines }: { engines: Engine[] }) {
  return (
    <div className="overflow-x-auto rounded-weft border border-hairline">
      <table className="w-full min-w-[460px] text-sm">
        <thead className="bg-bg-subtle text-left text-xs uppercase tracking-wide text-muted">
          <tr>
            <th className="px-4 py-2.5 font-medium">Engine</th>
            <th className="px-4 py-2.5 font-medium">Common-set total</th>
            <th className="px-4 py-2.5 font-medium">Full total</th>
            <th className="px-4 py-2.5 font-medium">Failed queries</th>
            <th className="px-4 py-2.5 font-medium">Status</th>
          </tr>
        </thead>
        <tbody>
          {engines.map((e) => (
            <tr key={e.key} className="border-t border-hairline">
              <td className="px-4 py-2.5 font-medium">
                {e.name}
                {e.highlight && <span className="ml-2 text-xs text-accent">ours</span>}
              </td>
              <td className="px-4 py-2.5 tabular-nums font-semibold">
                {e.total != null ? `${e.total.toFixed(2)}s` : "—"}
              </td>
              <td className="px-4 py-2.5 tabular-nums text-muted">
                {e.totalAll != null ? `${e.totalAll.toFixed(2)}s` : "—"}
              </td>
              <td className="px-4 py-2.5 tabular-nums">
                {e.failures ? (
                  <span title={`Q${(e.failedQueries ?? []).join(", Q")}`}>
                    {e.failures}{" "}
                    <span className="text-xs text-muted">
                      (Q{(e.failedQueries ?? []).join(", Q")})
                    </span>
                  </span>
                ) : e.failures === 0 ? (
                  "0"
                ) : (
                  "—"
                )}
              </td>
              <td className="px-4 py-2.5">
                <SourceTag source={e.source} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function SuiteSection({
  eyebrow,
  title,
  blurb,
  suite,
  showPerQuery = true,
  queryBase = 1,
}: {
  eyebrow: string;
  title: string;
  blurb: string;
  suite: Benchmarks;
  showPerQuery?: boolean;
  /** First query number for axis labels (TPC-H/DS are 1-based). */
  queryBase?: number;
}) {
  const anyMeasured = suite.engines.some((e) => e.total != null);
  return (
    <section className="mt-16">
      <div className="max-w-3xl">
        <span className="weft-eyebrow">{eyebrow}</span>
        <h2 className="mt-2 text-2xl font-bold tracking-tight">{title}</h2>
        <p className="mt-3 text-muted">{blurb}</p>
        <p className="mt-2 text-sm text-muted">
          {suite.dataset}
          {suite.runDate ? ` · last run ${suite.runDate}` : " · awaiting first published run"} ·{" "}
          {suite.machine}
        </p>
      </div>

      {!anyMeasured && (
        <div className="mt-6 rounded-weft border border-dashed border-hairline bg-bg-subtle p-4 text-sm text-muted">
          <strong className="text-body">Pending.</strong> Parquet lands in S3 under Glue; the
          harness fills these numbers when the distributed run completes.
        </div>
      )}

      <div className="mt-8">
        <BenchmarkChart
          engines={suite.engines}
          title={
            suite.commonCount
              ? `Total hot runtime — ${suite.commonCount}/${suite.queryCount} queries completed`
              : "Total hot runtime"
          }
        />
      </div>

      {showPerQuery && anyMeasured && suite.queryCount <= 43 && (
        <div className="mt-8">
          <PerQueryChart engines={suite.engines} queryCount={suite.queryCount} queryBase={queryBase} />
        </div>
      )}

      <div className="mt-8">
        <ProvenanceTable engines={suite.engines} />
      </div>
    </section>
  );
}

export default function PerformancePage() {
  const anyMeasured = benchmarks.engines.some((e) => e.total != null);
  return (
    <div className="weft-container py-14">
      <div className="max-w-3xl">
        <span className="weft-eyebrow">Benchmarks</span>
        <h1 className="mt-2 text-3xl font-bold tracking-tight sm:text-4xl">Performance</h1>
        <p className="mt-4 text-lg text-muted">
          ClickBench on a dedicated box, plus TPC-H / TPC-DS SF10 head-to-head: Weft distributed vs
          stock Apache Spark on EMR — same instance spec, same Parquet bytes in S3, same query text.
        </p>
      </div>

      {/* ClickBench */}
      <section className="mt-12">
        <div className="max-w-3xl">
          <span className="weft-eyebrow">ClickBench</span>
          <h2 className="mt-2 text-2xl font-bold tracking-tight">Four-engine head-to-head</h2>
          <p className="mt-3 text-muted">
            Weft against Sail, Apache Spark, and Spark with the Gluten/Velox native backend — same
            machine, same dataset, same Spark SQL. Harness:{" "}
            <a
              className="text-accent hover:underline"
              href={`${REPO}/tree/main/bench/clickbench/multi`}
            >
              bench/clickbench/multi
            </a>
            .
          </p>
        </div>

        {!anyMeasured && (
          <div className="mt-8 rounded-weft border border-dashed border-hairline bg-bg-subtle p-4 text-sm text-muted">
            <strong className="text-body">Run in progress.</strong> The fresh benchmark is being
            measured on a dedicated {benchmarks.machine} per engine.
          </div>
        )}

        <section className="mt-10">
          <h3 className="mb-1 text-lg font-semibold">Headline</h3>
          <p className="mb-4 max-w-2xl text-sm text-muted">
            Against Sail — the lean Rust peer and our honest north-star — Weft is the faster engine;
            Spark and Spark+Gluten are the broader context. And Weft runs the{" "}
            <strong className="text-body">full 43/43</strong>
            {weft?.totalAll ? ` (${weft.totalAll.toFixed(1)}s, zero failures)` : ""} — it wins{" "}
            <em>and</em> finishes everything.
          </p>
          <StatChips />
        </section>

        <section className="mt-10">
          <BenchmarkChart
            engines={benchmarks.engines}
            title={
              benchmarks.commonCount
                ? `Total hot runtime — ${benchmarks.commonCount} queries all engines completed`
                : "Total hot runtime"
            }
          />
        </section>

        <section className="mt-8">
          <PerQueryChart engines={benchmarks.engines} queryCount={benchmarks.queryCount} />
        </section>

        <section className="mt-12">
          <h3 className="mb-4 text-lg font-semibold">Engines & provenance</h3>
          <ProvenanceTable engines={benchmarks.engines} />
        </section>

        <div className="my-12">
          <ThreadDivider node={0.5} />
        </div>

        <section className="mt-2 grid gap-8 lg:grid-cols-2">
          <div className="min-w-0">
            <h3 className="mb-3 text-lg font-semibold">Methodology</h3>
            <ul className="space-y-2.5 text-sm text-muted">
              <li>
                <strong className="text-body">Dataset.</strong> {benchmarks.dataset}
              </li>
              <li>
                <strong className="text-body">Hardware.</strong> Dedicated {benchmarks.machine} (16
                vCPU / 32 GiB) per engine.
              </li>
              <li>
                <strong className="text-body">Transport.</strong> {benchmarks.method}.
              </li>
            </ul>
          </div>
          <div className="min-w-0">
            <h3 className="mb-3 text-lg font-semibold">Reproduce it</h3>
            <CodeBlock
              lines={[
                { text: "# on a fresh c6a.4xlarge (Ubuntu 24.04)", comment: true },
                { text: "git clone https://github.com/vamzi/weft && cd weft" },
                { text: "bash bench/clickbench/multi/bootstrap.sh" },
                { text: "bash bench/clickbench/multi/run-all.sh" },
                { text: "python3 bench/clickbench/multi/to-site.py" },
              ]}
            />
          </div>
        </section>
      </section>

      <div className="my-12">
        <ThreadDivider node={0.35} />
      </div>

      <SuiteSection
        eyebrow="TPC-H SF10"
        title="Decision-support, head-to-head with Spark"
        blurb="Official Q1–Q22 over the same SF10 Parquet bytes on S3, on the same 3-node spec (1x c6g.2xlarge + 2x m8g.2xlarge), re-run fresh the same day with identical temperature (2 runs/query, hot = run 2): Weft distributed in strict mode via Spark Connect — with the S3 disk cache (a Databricks/Snowflake-style local-NVMe object cache on every worker) — vs stock Apache Spark 3.5.6 on EMR 7.13.0/YARN, which re-reads S3 on every query. It is a sweep: Weft 15.3s vs Spark 87.3s hot — 82% faster, winning all 22 queries. The cache is not the win: strip it from both engines and Weft still takes 69.0s vs 87.3s (21% faster, 16 of 22) — the wins are the engine: real table statistics, per-query hash-join selection (auto, not per-deployment), driver-side predicate pushdown before the stage split, runtime-measured shuffle cardinalities, and a plan cache that plans each stage once per worker — leak-free. The one pass Spark still takes is the truly cold, cacheless stream (105.7s vs our 118.3s — its S3A prefetcher reads ahead, ours does not yet); with the cache on, even the first-touch pass is a dead heat (104.1s vs 105.7s). Correctness is table stakes and Weft meets it: 22/22 golden-clean against DuckDB SF10."
        suite={tpchBenchmarks}
      />

      <div className="my-12">
        <ThreadDivider node={0.65} />
      </div>

      <SuiteSection
        eyebrow="TPC-DS SF10"
        title="Retail warehouse, 99 queries each"
        blurb="Same hardware, same bytes, all 99 queries on both engines, same temperature. The SF10 marathon is a rout: Weft 56.4s vs Spark 282.8s hot — 80% faster, winning 98 of 99 outright with Q72 a 4.3s-vs-4.4s coin flip at the boundary. The last holdout fell this run: Q4, the UNION-shaped self-join that trailed Spark 1.3x, dropped 11.4s → 3.25s (Spark: 8.8s) when the driver began pruning contradictory union arms before the stage split — each of its six year_total occurrences collapses to the single fact slice its sale_type filter keeps (Q11 rode along, 4.5s → 1.6s). Underneath it, the S3 disk cache lands each table on local NVMe once per worker instead of re-downloading ~250MB/node from S3 on every query. The cache is not the win either: off on both engines, Weft still takes 254.0s vs 282.8s (10% faster, 58 of 99), and the cache-filling first pass is a 48% rout (168.9s vs 325.2s) — only the truly cold, cacheless stream goes to Spark (365.3s vs 325.2s, its S3A prefetch edge; the open I/O work). 99/99 end-to-end under strict mode, every query golden-validated against DuckDB SF10. Per-query chart omitted (99 bars); totals tell the story."
        suite={tpcdsBenchmarks}
        showPerQuery={false}
      />

      <div className="my-12">
        <ThreadDivider node={0.5} />
      </div>

      <section className="mt-16">
        <div className="max-w-3xl">
          <span className="weft-eyebrow">Cache off — the control</span>
          <h2 className="mt-2 text-2xl font-bold tracking-tight">
            Same fight, no cache on either side
          </h2>
          <p className="mt-3 text-muted">
            The sections above run Weft with its S3 disk cache on (the default, and the
            benchmarked configuration). These two run the identical suites with the cache
            disabled — both engines re-read S3 on every query, so what remains is pure
            engine: planning, join strategy, execution. Weft still wins both suites, and
            the only pass that flips is the truly cold stream, where Spark&apos;s S3A
            prefetcher reads ahead and Weft&apos;s first touch does not yet.
          </p>
        </div>
      </section>

      <SuiteSection
        eyebrow="TPC-H SF10 · cache off"
        title="Decision-support, cache disabled"
        blurb="The control for the headline: cache off on both engines, identical everything else. Weft 69.0s vs Spark 87.3s hot — 21% faster, winning 16 of 22. Compare the cache-on section: 15.3s vs 87.3s (82% faster) — the cache multiplies the win, it does not create it. Cold-vs-cold (first touches, nothing cached anywhere): Spark 105.7s vs Weft 118.3s — 11% to Spark, the S3A prefetch edge on the one streaming pass; every subsequent pass belongs to Weft."
        suite={tpchNocacheBenchmarks}
      />

      <div className="my-12">
        <ThreadDivider node={0.2} />
      </div>

      <SuiteSection
        eyebrow="TPC-DS SF10 · cache off"
        title="Retail warehouse, cache disabled"
        blurb="The 99-query control: Weft 254.0s vs Spark 282.8s hot — 10% faster, 58 of 99 wins with both engines re-reading S3 on every query. With the cache on it becomes 56.4s vs 282.8s (80% faster): each table downloads once per cluster instead of once per query. The cold, cacheless first pass goes to Spark (325.2s vs 365.3s, ~11%) — S3A prefetch again, the open I/O workstream; after one pass the cache makes the point moot. 99/99 golden-validated against DuckDB SF10 here too. Per-query chart omitted (99 bars)."
        suite={tpcdsNocacheBenchmarks}
        showPerQuery={false}
      />

      <div className="my-12">
        <ThreadDivider node={0.5} />
      </div>

      <section className="mt-2 grid gap-8 lg:grid-cols-2">
        <div className="min-w-0">
          <h3 className="mb-3 text-lg font-semibold">SF10 data path</h3>
          <ul className="space-y-2.5 text-sm text-muted">
            <li>
              <strong className="text-body">Object store.</strong>{" "}
              <code className="text-body">s3://weft-artifacts-…/&#123;tpch,tpcds&#125;-sf10/</code>
            </li>
            <li>
              <strong className="text-body">Catalog.</strong> AWS Glue databases{" "}
              <code className="text-body">tpch_sf10</code> /{" "}
              <code className="text-body">tpcds_sf10</code>, attached as the Spark catalog{" "}
              <code className="text-body">glue</code>.
            </li>
            <li>
              <strong className="text-body">Weft compute.</strong> Weft Spark Connect driver + 2
              workers on EC2 (AL2023 arm64, the <code className="text-body">weft-sf10</code> ASGs),
              strict distributed mode.
            </li>
            <li>
              <strong className="text-body">Spark compute.</strong> Stock EMR 7.13.0 (Spark
              3.5.6-amzn-2) on YARN — 1x c6g.2xlarge master + 2x m8g.2xlarge core, the same instance
              spec as Weft. Tables registered as temp views over the same S3 prefix (the Glue DBs
              are schema-less weft registrations, so Spark infers from the parquet footers — same
              bytes either way). Two Spark-only normalizations, both documented in the runner:
              interval field precision (TPC-H Q1) and backtick aliases (EMR rejects{" "}
              <code className="text-body">AS &quot;...&quot;</code> outside ANSI mode).
            </li>
          </ul>
        </div>
        <div className="min-w-0">
          <h3 className="mb-3 text-lg font-semibold">Reproduce SF10</h3>
          <CodeBlock
            lines={[
              { text: "# weft: driver up at sc://<driver>:50051 (scripts/sf10-start.sh)", comment: true },
              { text: "python3 bench/sf100/run-spark-connect.py \\" },
              { text: "  --endpoint sc://<driver>:50051 --suite tpch --sf 10 \\" },
              { text: "  --glue-db tpch_sf10 --strict --worker-count 2 \\" },
              { text: "  --skip-worker-preflight --query-timeout 900 \\" },
              { text: "  --json bench/sf100/results/tpch-sf10.jsonl" },
              { text: "# spark: on the EMR master (same instance spec)", comment: true },
              { text: "spark-submit --master yarn --deploy-mode client \\" },
              { text: "  bench/sf100/emr/run-emr-suite.py --suite tpch \\" },
              { text: "  --queries-dir bench/tpch/queries --runs 2 \\" },
              { text: "  --out /tmp/tpch-sf10-emr.jsonl" },
              { text: "# then regenerate this page's data", comment: true },
              { text: "python3 bench/sf100/results/to-site-sf10.py" },
            ]}
          />
          <p className="mt-3 text-xs text-muted">
            Scripts:{" "}
            <a className="text-accent hover:underline" href={`${REPO}/tree/main/bench/sf100`}>
              bench/sf100
            </a>
            .
          </p>
        </div>
      </section>
    </div>
  );
}
