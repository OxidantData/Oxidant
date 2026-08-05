import { Link } from "react-router-dom";
import CodeBlock from "../components/CodeBlock";
import ShuttlePass from "../components/ShuttlePass";
import StatChips from "../components/StatChips";
import LoomDiagram from "../components/LoomDiagram";
import WarpFeatureRail from "../components/WarpFeatureRail";
import DiffBlock from "../components/DiffBlock";
import WovenField from "../components/WovenField";
import ThreadDivider from "../components/ThreadDivider";
import { benchmarks, pctFasterVs, speedupVs } from "../lib/benchmarks";

const REPO = "https://github.com/vamzi/weft";

const FEATURES = [
  {
    title: "Drop-in Spark Connect",
    body: "Weft is a Spark Connect server. Stock PySpark, Spark SQL, and notebooks connect over the same gRPC they already speak — one stock client drives the whole benchmark.",
  },
  {
    title: "Rust, no JVM",
    body: "The entire stack is native Rust. No JVM to size, no GC pauses deciding when your query stalls, fast cold starts — none of the heap-tuning ritual before you can run.",
  },
  {
    title: "Vectorized Arrow core",
    body: "Everything between operators stays in Apache Arrow. Loom executes columnar plans on DataFusion 54 with a hand-written radix hash-aggregation path for high-cardinality GROUP BY.",
  },
  {
    title: "Open formats, your catalog",
    body: "Reads Parquet, Delta, and Iceberg directly through a pluggable catalog — Hive, Unity, or Glue — configured the Spark way and resolved lazily, only when a query first names a table.",
  },
];

function Hero() {
  const vsSpark = pctFasterVs("spark");
  return (
    <section className="relative isolate overflow-hidden border-b border-hairline">
      <WovenField />
      <div className="weft-container relative py-16 sm:py-24">
        <div className="grid items-center gap-10 lg:grid-cols-2">
          <div>
            <span className="weft-eyebrow">Drop-in Apache Spark replacement · written in Rust</span>
            <h1 className="mt-3 text-3xl font-bold leading-[1.1] tracking-tight sm:text-5xl">
              Keep your Spark code.
              <br />
              <span className="text-accent">Replace the engine.</span>
            </h1>
            <p className="mt-5 max-w-xl text-lg text-muted">
              Weft speaks Spark Connect. Point unmodified PySpark or Spark SQL at{" "}
              <code className="font-mono text-body">sc://</code>, change one line, and your jobs run
              on a vectorized Rust core — no JVM, no rewrite.
              {vsSpark && (
                <>
                  {" "}
                  Measured <span className="font-semibold text-body">{vsSpark.toFixed(0)}% faster
                  than Spark</span> on ClickBench.
                </>
              )}
            </p>
            <div className="mt-7 flex flex-wrap gap-3">
              <a href={REPO} className="weft-btn-primary">
                Get started on GitHub
              </a>
              <Link to="/performance" className="weft-btn-ghost">
                See the full benchmark →
              </Link>
            </div>
          </div>
          <div>
            <CodeBlock
              lines={[
                { text: "# 1. start the Weft Spark Connect server", comment: true },
                { text: "weft spark server --port 50051" },
                { text: "" },
                { text: "# 2. point a stock PySpark client at it", comment: true },
                { text: "from pyspark.sql import SparkSession" },
                { text: 'spark = (SparkSession.builder' },
                { text: '         .remote("sc://localhost:50051")' },
                { text: "         .getOrCreate())" },
                { text: 'spark.sql("SELECT count(*) FROM hits").show()' },
              ]}
              copy={"weft spark server --port 50051"}
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

export default function HomePage() {
  const vsSpark = speedupVs("spark");
  return (
    <>
      <Hero />

      {/* S2 — the shuttle pass (kinetic proof) */}
      <section className="border-b border-hairline bg-bg-subtle">
        <div className="weft-container py-16">
          <div className="mx-auto mb-8 max-w-2xl text-center">
            <span className="weft-eyebrow">The proof</span>
            <h2 className="mt-2 text-2xl font-bold tracking-tight sm:text-3xl">
              {vsSpark
                ? `41 seconds — the run that takes Spark ${benchmarks.engines.find((e) => e.key === "spark")?.total?.toFixed(0)}.`
                : "One run, four engines."}
            </h2>
            <p className="mt-3 text-muted">
              One ClickBench run, four engines, the same {benchmarks.dataset} on the same{" "}
              {benchmarks.machine}, the same Spark SQL driven by one stock PySpark client. We compare
              the queries every engine actually completed — no engine gets credit for skipping a
              hard one.
            </p>
          </div>
          <div className="mx-auto max-w-3xl">
            <ShuttlePass />
            <div className="mt-6 flex flex-col items-center gap-4">
              <StatChips />
              <Link to="/performance" className="weft-btn-ghost">
                Full methodology & per-query results →
              </Link>
            </div>
          </div>
        </div>
      </section>

      {/* S3 — the loom (architecture metaphor) */}
      <section className="border-b border-hairline">
        <div className="weft-container py-16">
          <div className="grid items-center gap-10 lg:grid-cols-[3fr_2fr]">
            <div className="order-2 lg:order-1">
              <LoomDiagram />
            </div>
            <div className="order-1 lg:order-2">
              <span className="weft-eyebrow">How it's woven</span>
              <h2 className="mt-2 text-2xl font-bold tracking-tight sm:text-3xl">
                Built like a loom, not a fork of Spark.
              </h2>
              <p className="mt-3 text-muted">
                Weft isn't Spark with the serial numbers filed off. Your plan enters as the{" "}
                <strong className="text-body">warp</strong>, is refined by{" "}
                <strong className="text-body">heddle</strong>, and is woven into Arrow batches by{" "}
                <strong className="text-body">Loom</strong> — a vectorized CPU engine on Arrow and
                DataFusion. No serialization tax between operators, no garbage collector deciding
                when your query pauses.
              </p>
              <Link to="/architecture" className="mt-4 inline-block text-sm font-medium text-accent hover:underline">
                See how Weft is woven →
              </Link>
            </div>
          </div>
        </div>
      </section>

      {/* S4 — warp-rail features */}
      <section className="border-b border-hairline bg-bg-subtle">
        <div className="weft-container py-16">
          <div className="mb-10 max-w-2xl">
            <span className="weft-eyebrow">What you get</span>
            <h2 className="mt-2 text-2xl font-bold tracking-tight sm:text-3xl">
              Native speed, picked up one thread at a time.
            </h2>
          </div>
          <WarpFeatureRail features={FEATURES} />
        </div>
      </section>

      {/* S5 — the one-line diff */}
      <section className="border-b border-hairline">
        <div className="weft-container py-16">
          <div className="mx-auto max-w-2xl">
            <div className="mb-6 text-center">
              <span className="weft-eyebrow">The migration</span>
              <h2 className="mt-2 text-2xl font-bold tracking-tight sm:text-3xl">
                One line changes. Nothing else does.
              </h2>
            </div>
            <DiffBlock />
          </div>
        </div>
      </section>

      {/* S6 — close */}
      <section className="relative isolate overflow-hidden">
        <WovenField dense />
        <div className="weft-container relative py-20 text-center">
          <h2 className="text-2xl font-bold tracking-tight sm:text-3xl">
            Change one URL. Keep the rest.
          </h2>
          <p className="mx-auto mt-3 max-w-xl text-muted">
            Spin up the Weft Spark Connect server, point a notebook at it, and run a query you
            already trust the answer to. If it's faster, keep going. If it's not, you've changed one
            line back.
          </p>
          <div className="mt-7 flex flex-wrap justify-center gap-3">
            <a href={REPO} className="weft-btn-primary">
              Get started on GitHub
            </a>
            <Link to="/performance" className="weft-btn-ghost">
              Reproduce the benchmark →
            </Link>
          </div>
        </div>
      </section>
    </>
  );
}
