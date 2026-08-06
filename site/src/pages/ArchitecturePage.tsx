import LoomDiagram from "../components/LoomDiagram";
import DiffBlock from "../components/DiffBlock";
import CodeBlock from "../components/CodeBlock";
import ThreadDivider from "../components/ThreadDivider";

const REPO = "https://github.com/OxidantData/Oxidant";

function FlowCard({
  label,
  title,
  body,
  tone = "solid",
}: {
  label: string;
  title: string;
  body: string;
  tone?: "solid" | "engine" | "scaffold" | "research";
}) {
  const ring =
    tone === "engine"
      ? "border-accent/60 bg-accent/5"
      : tone === "scaffold"
        ? "border-dashed border-hairline bg-bg-subtle"
        : tone === "research"
          ? "border-dashed border-hairline bg-bg-subtle"
          : "border-hairline bg-surface";
  return (
    <div className={`rounded-oxidant border p-4 ${ring}`}>
      <div className="flex items-center gap-2">
        <span className="font-mono text-[11px] uppercase tracking-wide text-muted">{label}</span>
        {tone === "engine" && (
          <span className="rounded-full bg-accent px-1.5 py-0.5 text-[10px] font-semibold text-accent-contrast">
            engine
          </span>
        )}
        {tone === "scaffold" && (
          <span className="rounded-full border border-hairline px-1.5 py-0.5 text-[10px] text-muted">
            roadmap
          </span>
        )}
        {tone === "research" && (
          <span className="rounded-full border border-hairline px-1.5 py-0.5 text-[10px] text-muted">
            research · gated
          </span>
        )}
      </div>
      <h3 className="mt-1 text-sm font-semibold">{title}</h3>
      <p className="mt-1 text-sm leading-relaxed text-muted">{body}</p>
    </div>
  );
}

export default function ArchitecturePage() {
  return (
    <div className="oxidant-container py-14">
      <div className="max-w-3xl">
        <span className="oxidant-eyebrow">How Oxidant is woven</span>
        <h1 className="mt-2 text-3xl font-bold tracking-tight sm:text-4xl">
          A loom, not a fork of Spark.
        </h1>
        <p className="mt-4 text-lg text-muted">
          Oxidant accepts the plans your Spark client already produces and weaves them into Arrow on a
          native Rust core. Here's the honest version — what's load-bearing today, and what's still
          on the frame.
        </p>
      </div>

      {/* The metaphor */}
      <section className="mt-12">
        <div className="oxidant-card p-6 sm:p-8">
          <LoomDiagram />
        </div>
        <p className="mt-3 text-sm text-muted">
          The metaphor maps to real names in the codebase: <strong className="text-body">warp</strong>{" "}
          is the plan IR, <strong className="text-body">heddle</strong> the optimizer, and{" "}
          <strong className="text-body">Loom</strong> the vectorized engine.
        </p>
      </section>

      <div className="my-12">
        <ThreadDivider node={0.62} />
      </div>

      {/* The honest request path */}
      <section>
        <h2 className="mb-2 text-lg font-semibold">The request path</h2>
        <p className="mb-6 max-w-2xl text-sm text-muted">
          A stock Spark Connect client to native execution, no JVM anywhere. Solid blocks are live
          today; dashed blocks are scaffolding or gated research — drawn honestly, not on the
          default path.
        </p>
        <div className="grid items-stretch gap-4 md:grid-cols-4">
          <FlowCard
            label="client"
            title="Unmodified PySpark / Spark SQL"
            body="Your code changes one line — the sc:// URL. DataFrame API and spark.sql() both work as-is."
          />
          <FlowCard
            label="front door"
            title="oxidant-connect"
            body="A native Spark Connect gRPC server. Lowers both SQL strings and DataFrame relation trees straight to DataFusion logical plans; streams Arrow back."
          />
          <FlowCard
            label="engine"
            tone="engine"
            title="Loom"
            body="The load-bearing piece: a vectorized core on DataFusion 54 + Arrow, with a native radix hash-aggregation fast path for high-cardinality GROUP BY."
          />
          <FlowCard
            label="storage"
            title="Open tables"
            body="Parquet, Delta, and Iceberg via oxidant-datasource, through a lazy pluggable catalog (Hive Metastore reference provider)."
          />
        </div>

        <div className="mt-4 grid gap-4 md:grid-cols-2">
          <FlowCard
            label="warp · heddle · physical"
            tone="scaffold"
            title="The named pipeline stages"
            body="warp (plan IR), heddle (optimizer), and the physical layer are forward-looking scaffolding today — the live path is Connect → Loom, with DataFusion's optimizer plus Loom's tuning doing the work."
          />
          <FlowCard
            label="oxidant-hvm"
            tone="research"
            title="Bend → HVM2 backend"
            body="An opt-in, feature-gated research bet for irregular/recursive workloads no columnar engine serves well. It wins zero ClickBench queries by design — that's not the benchmark it's for."
          />
        </div>
      </section>

      <div className="my-12">
        <ThreadDivider node={0.38} />
      </div>

      {/* Maturity */}
      <section className="grid gap-8 lg:grid-cols-2">
        <div>
          <h2 className="mb-3 text-lg font-semibold">What's solid today</h2>
          <ul className="space-y-2 text-sm text-muted">
            <li>• Spark Connect gRPC: <code className="font-mono text-body">spark.sql()</code> and the DataFrame API (project/filter/aggregate/sort/limit/joins/set-ops/window/<code className="font-mono">na</code>/<code className="font-mono">pivot</code>), against stock pyspark-connect.</li>
            <li>• Loom CPU engine on DataFusion 54 with a native hash-agg fast path.</li>
            <li>• All 43/43 ClickBench queries run; the published head-to-head beats Sail, Gluten, and Spark on the real 14.78 GB dataset.</li>
            <li>• Parquet / Delta / Iceberg reads; pluggable catalog with a real Hive Metastore provider.</li>
          </ul>
        </div>
        <div>
          <h2 className="mb-3 text-lg font-semibold">What's early, said plainly</h2>
          <ul className="space-y-2 text-sm text-muted">
            <li>• Some SQL surface is still arriving (Python UDFs, <code className="font-mono text-body">pivot</code> without explicit values, stat ops, streaming reattach). A query the young server can't handle is recorded as an honest gap, never hidden.</li>
            <li>• Distributed mode is an MVP: a tested 2-stage hash shuffle over Arrow Flight; auto-decomposition and spill are deferred.</li>
            <li>• Delta/Iceberg v1 limits (no deletion vectors / merge-on-read yet).</li>
            <li>• The HVM2 backend is gated research, not a product feature.</li>
          </ul>
        </div>
      </section>

      <div className="my-12">
        <ThreadDivider node={0.5} />
      </div>

      {/* Try it */}
      <section className="grid gap-8 lg:grid-cols-2">
        <div className="min-w-0">
          <h2 className="mb-3 text-lg font-semibold">One line of diff</h2>
          <DiffBlock />
        </div>
        <div className="min-w-0">
          <h2 className="mb-3 text-lg font-semibold">Run it</h2>
          <CodeBlock
            lines={[
              { text: "# native server, no JVM", comment: true },
              { text: "oxidant spark server --port 50051" },
              { text: "" },
              { text: "# optional: wire an external catalog, the Spark way", comment: true },
              { text: "oxidant spark server --port 50051 \\" },
              { text: "  --catalog-conf spark.sql.catalog.prod.type=hive" },
            ]}
          />
          <p className="mt-3 text-xs text-muted">
            Full crate-by-crate detail in{" "}
            <a className="text-accent hover:underline" href={`${REPO}/blob/main/docs/architecture.md`}>
              docs/architecture.md
            </a>
            .
          </p>
        </div>
      </section>
    </div>
  );
}
