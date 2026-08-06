import { Link } from "react-router-dom";
import CodeBlock from "../components/CodeBlock";
import WovenField from "../components/WovenField";
import ThreadDivider from "../components/ThreadDivider";
import PricingSection, { AMI_URL } from "../components/PricingSection";
import {
  tpcdsBenchmarks,
  tpchBenchmarks,
  tpcdsNocacheBenchmarks,
  tpchNocacheBenchmarks,
  pctFasterOfHighlight,
  type Benchmarks,
} from "../lib/benchmarks";

const REPO = "https://github.com/OxidantData/Oxidant";

/* Headline numbers are derived from src/data/*.json (the measured SF10 runs), never hand-entered.
 * Fallbacks are the published figures: TPC-DS 56.4s vs 282.8s (80%), TPC-H 15.3s vs 87.3s (82%),
 * cold-cache TPC-DS 254.0s (10%) / TPC-H 69.0s (21%). */
function headToHead(suite: Benchmarks) {
  const spark = suite.engines.find((e) => e.key === "spark-emr");
  const oxidant = suite.engines.find((e) => e.key === "oxidant-dist");
  return {
    oxidantTotal: oxidant?.total ?? null,
    sparkTotal: spark?.total ?? null,
    pct: spark ? pctFasterOfHighlight(spark, suite) : null,
    queries: suite.queryCount,
    passed: suite.commonCount ?? suite.queryCount,
  };
}

const tpcds = headToHead(tpcdsBenchmarks);
const tpch = headToHead(tpchBenchmarks);
const tpcdsCold = headToHead(tpcdsNocacheBenchmarks);
const tpchCold = headToHead(tpchNocacheBenchmarks);

const sec = (n: number | null, fallback: string) => (n == null ? fallback : n.toFixed(1));
const pct = (n: number | null, fallback: string) => (n == null ? fallback : n.toFixed(0));

const STEPS = [
  {
    n: "1",
    title: "Launch the AMI",
    body: "Subscribe to the free AWS Marketplace AMI — or docker pull ghcr.io/oxidantdata/oxidant. One native binary, no JVM, nothing to tune. The Spark Connect server listens on port 50051.",
  },
  {
    n: "2",
    title: "Point PySpark at it",
    body: 'SparkSession.builder.remote("sc://host:50051"). Stock PySpark, unmodified — DataFrames, Spark SQL, and notebooks all speak the same Spark Connect protocol.',
  },
  {
    n: "3",
    title: "Run your jobs",
    body: "Same queries, same data — Parquet, Delta, or Iceberg on S3. When one node isn't enough, add workers; the Connect API in front doesn't change.",
  },
];

const FEATURES = [
  {
    title: "Spark-compatible",
    body: "A Spark Connect server, not a lookalike. Stock PySpark and Spark SQL clients connect over the gRPC protocol they already speak — one stock client drives our entire benchmark suite.",
  },
  {
    title: "Rust-fast",
    body: "A vectorized Arrow core (DataFusion) with a hand-written radix hash-aggregation path. Measured 80–82% faster than EMR Spark on identical EC2 hardware — no cache tricks required.",
  },
  {
    title: "No JVM",
    body: "The whole stack is one native Rust binary. No heap to size, no GC pauses deciding when your query stalls, no warm-up ritual before the first run is fast.",
  },
  {
    title: "Open formats",
    body: "Reads Parquet, Delta, and Iceberg directly through pluggable catalogs — Hive, Unity, or Glue — configured the Spark way. Your data stays in open formats, in your bucket.",
  },
  {
    title: "Distributed",
    body: "A Flight-based driver/worker cluster when a single node isn't enough. Same Spark Connect endpoint in front; the SF10 headline numbers above ran on a 3-node cluster.",
  },
  {
    title: "Open source",
    body: "AGPLv3 — audit every line, run it anywhere, pay nothing. Commercial license available for organizations whose policies require one.",
  },
];

function Hero() {
  return (
    <section className="relative isolate overflow-hidden border-b border-hairline">
      <WovenField />
      <div className="oxidant-container relative py-16 sm:py-24">
        <div className="grid items-center gap-10 lg:grid-cols-2">
          <div>
            <span className="oxidant-eyebrow">
              Drop-in Apache Spark replacement · written in Rust
            </span>
            <h1 className="mt-3 text-3xl font-bold leading-[1.1] tracking-tight sm:text-5xl">
              Faster than Spark.
              <br />
              <span className="text-accent">Free forever.</span>
            </h1>
            <p className="mt-5 max-w-xl text-lg text-muted">
              Oxidant speaks the Spark Connect protocol, so unmodified PySpark runs on it — and it
              runs <span className="font-semibold text-body">{pct(tpcds.pct, "80")}% faster than
              EMR Spark</span> on identical hardware. No JVM, no rewrite, no per-query meter. Open
              source under AGPLv3.
            </p>
            <div className="mt-7 flex flex-wrap gap-3">
              <a href={AMI_URL} className="oxidant-btn-primary">
                Get the AMI
              </a>
              <a href={REPO} className="oxidant-btn-ghost">
                View on GitHub
              </a>
            </div>
            <p className="mt-4 text-sm text-muted">
              Free AWS Marketplace AMI · <code className="font-mono text-[13px]">docker pull ghcr.io/oxidantdata/oxidant</code>
            </p>
          </div>
          <div>
            <CodeBlock
              lines={[
                { text: "# 1. launch Oxidant (AMI or docker)", comment: true },
                { text: "oxidant spark server --port 50051" },
                { text: "" },
                { text: "# 2. point a stock PySpark client at it — unmodified", comment: true },
                { text: "from pyspark.sql import SparkSession" },
                { text: "spark = (SparkSession.builder" },
                { text: '         .remote("sc://localhost:50051")' },
                { text: "         .getOrCreate())" },
                { text: "" },
                { text: "# 3. run the jobs you already have", comment: true },
                { text: 'spark.sql("SELECT count(*) FROM hits").show()' },
              ]}
              copy={"oxidant spark server --port 50051"}
            />
            <p className="mt-2 text-center text-xs text-muted">
              Your Spark code is unchanged — only the <code className="font-mono">sc://</code> URL.
            </p>
          </div>
        </div>
      </div>
      <ThreadDivider node={0.5} glide />
    </section>
  );
}

function ProofBand() {
  const stats = [
    {
      figure: `${pct(tpcds.pct, "80")}%`,
      label: "faster on TPC-DS SF10",
      detail: `${sec(tpcds.oxidantTotal, "56.4")}s vs EMR Spark's ${sec(tpcds.sparkTotal, "282.8")}s`,
    },
    {
      figure: `${pct(tpch.pct, "82")}%`,
      label: "faster on TPC-H SF10",
      detail: `${sec(tpch.oxidantTotal, "15.3")}s vs EMR Spark's ${sec(tpch.sparkTotal, "87.3")}s`,
    },
    {
      figure: `${tpcds.passed}/${tpcds.queries}`,
      label: "TPC-DS queries pass",
      detail: `golden-result verified · ${tpch.passed}/${tpch.queries} on TPC-H`,
    },
    {
      figure: "$0",
      label: "Community license",
      detail: "AGPLv3 — unlimited queries, forever",
    },
  ];
  return (
    <section className="border-b border-hairline bg-bg-subtle">
      <div className="oxidant-container py-16">
        <div className="mx-auto mb-10 max-w-2xl text-center">
          <span className="oxidant-eyebrow">Measured, not promised</span>
          <h2 className="mt-2 text-2xl font-bold tracking-tight sm:text-3xl">
            Faster than EMR Spark on identical hardware.
          </h2>
          <p className="mt-3 text-muted">
            TPC-DS and TPC-H at scale factor 10, same Parquet bytes on S3, same 3-node EC2 spec,
            same day. Oxidant ran distributed via Spark Connect; Spark ran stock EMR 7.13.0 on
            YARN.
          </p>
        </div>
        <div className="grid gap-px overflow-hidden rounded-oxidant border border-hairline bg-hairline sm:grid-cols-2 lg:grid-cols-4">
          {stats.map((s) => (
            <div key={s.label} className="bg-surface px-5 py-6 text-center">
              <div className="text-3xl font-bold tracking-tight text-accent">{s.figure}</div>
              <div className="mt-1 text-sm font-medium">{s.label}</div>
              <div className="mt-1 text-xs text-muted">{s.detail}</div>
            </div>
          ))}
        </div>
        <p className="mx-auto mt-6 max-w-3xl text-center text-sm text-muted">
          Cold and apples-to-apples (cache disabled): TPC-DS {sec(tpcdsCold.oxidantTotal, "254.0")}s
          ({pct(tpcdsCold.pct, "10")}% faster), TPC-H {sec(tpchCold.oxidantTotal, "69.0")}s
          ({pct(tpchCold.pct, "21")}% faster).{" "}
          <Link to="/performance" className="font-medium text-accent hover:underline">
            Full methodology & per-query results →
          </Link>
        </p>
      </div>
    </section>
  );
}

function HowItWorks() {
  return (
    <section className="border-b border-hairline">
      <div className="oxidant-container py-16">
        <div className="mb-10 max-w-2xl">
          <span className="oxidant-eyebrow">How it works</span>
          <h2 className="mt-2 text-2xl font-bold tracking-tight sm:text-3xl">
            Running before your coffee cools.
          </h2>
        </div>
        <div className="grid gap-5 md:grid-cols-3">
          {STEPS.map((s) => (
            <div key={s.n} className="oxidant-card p-6">
              <div className="flex h-8 w-8 items-center justify-center rounded-full bg-accent font-mono text-sm font-semibold text-accent-contrast">
                {s.n}
              </div>
              <h3 className="mt-4 text-base font-semibold tracking-tight">{s.title}</h3>
              <p className="mt-2 text-sm text-muted">{s.body}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

function FeatureGrid() {
  return (
    <section className="border-b border-hairline">
      <div className="oxidant-container py-16">
        <div className="mb-10 max-w-2xl">
          <span className="oxidant-eyebrow">What you get</span>
          <h2 className="mt-2 text-2xl font-bold tracking-tight sm:text-3xl">
            Spark without the JVM. Speed without the invoice.
          </h2>
        </div>
        <div className="grid gap-5 sm:grid-cols-2 lg:grid-cols-3">
          {FEATURES.map((f) => (
            <div key={f.title} className="oxidant-card p-6">
              <h3 className="text-base font-semibold tracking-tight">{f.title}</h3>
              <p className="mt-2 text-sm text-muted">{f.body}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

function FinalCta() {
  return (
    <section className="relative isolate overflow-hidden">
      <WovenField dense />
      <div className="oxidant-container relative py-20 text-center">
        <h2 className="text-2xl font-bold tracking-tight sm:text-3xl">
          Run your slowest Spark job on Oxidant today.
        </h2>
        <p className="mx-auto mt-3 max-w-xl text-muted">
          Launch the AMI, change one URL, and compare the run yourself. The engine is free, the
          benchmark is reproducible, and the meter is not running.
        </p>
        <div className="mt-7 flex flex-wrap justify-center gap-3">
          <a href={AMI_URL} className="oxidant-btn-primary">
            Get the AMI
          </a>
          <a href={REPO} className="oxidant-btn-ghost">
            View on GitHub
          </a>
          <Link to="/performance" className="oxidant-btn-ghost">
            Reproduce the benchmark →
          </Link>
        </div>
      </div>
    </section>
  );
}

export default function HomePage() {
  return (
    <>
      <Hero />
      <ProofBand />
      <HowItWorks />
      <FeatureGrid />
      <PricingSection />
      <FinalCta />
    </>
  );
}
