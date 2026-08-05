import BenchmarkChart from "../components/BenchmarkChart";
import PerQueryChart from "../components/PerQueryChart";
import StatChips from "../components/StatChips";
import CodeBlock from "../components/CodeBlock";
import ThreadDivider from "../components/ThreadDivider";
import {
  benchmarks,
  tpchBenchmarks,
  tpcdsBenchmarks,
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
        blurb="Official Q1–Q22 over the same SF10 Parquet bytes on S3, on the same 3-node spec (1x c6g.2xlarge + 2x m8g.2xlarge), re-run fresh the same day with identical temperature (2 runs/query, hot = run 2): Weft distributed in strict mode via Spark Connect vs stock Apache Spark 3.5.6 on EMR 7.13.0/YARN. Weft takes this round: 85.5s vs 87.3s hot (0.98x), winning 11 of 22 queries outright — after starting the day at 6.7x Spark. The climb came from engineering, not hardware: real table statistics from parquet footers, hash joins chosen per query instead of forced sort-merge, runtime-measured shuffle cardinalities, a plan cache that plans each stage once per worker, and a leak fix in that cache that keeps the hundredth query as fast as the first. Correctness is table stakes and Weft meets it: 22/22 golden-clean against DuckDB SF10."
        suite={tpchBenchmarks}
      />

      <div className="my-12">
        <ThreadDivider node={0.65} />
      </div>

      <SuiteSection
        eyebrow="TPC-DS SF10"
        title="Retail warehouse, 99 queries each"
        blurb="Same hardware, same bytes, all 99 queries on both engines, same temperature. The SF10 marathon is now a photo finish: 295.5s vs 282.8s hot (1.045x) — the gap was 1.31x two days ago and 8.5x at the first publish — with Weft winning 47 of the 99 outright and a 1.15x median ratio. The last structural outliers collapsed: Q28's six COUNT(DISTINCT) branches now share one scan (7.9s→1.6s), Q72 14.2s→5.4s, Q44 8.2s→2.5s. What remains is raw per-row throughput, led by Q39 (8.8s vs 1.3s) — not plan shapes. 99/99 end-to-end under strict mode, every query golden-validated against DuckDB SF10. Per-query chart omitted (99 bars); totals tell the story."
        suite={tpcdsBenchmarks}
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
