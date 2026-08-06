/**
 * A before/after one-line diff in the dark terminal surface — the only place body content uses
 * red/green, to drive home "change one line, nothing else." Context lines are neutral.
 */
export default function DiffBlock() {
  return (
    <div className="overflow-hidden rounded-oxidant border border-hairline bg-code-bg">
      <div className="flex items-center gap-2 border-b border-white/10 px-4 py-2">
        <span className="h-2.5 w-2.5 rounded-full bg-white/15" />
        <span className="oxidant-code-dim font-mono text-xs">session.py</span>
      </div>
      <pre className="overflow-x-auto px-4 py-4 font-mono text-[13px] leading-relaxed">
        <div className="oxidant-code-dim">from pyspark.sql import SparkSession</div>
        <div className="oxidant-code-dim"> </div>
        <div className="oxidant-code-dim">spark = (SparkSession.builder</div>
        <div className="relative rounded-sm bg-danger/15 pl-3 text-danger before:absolute before:left-0 before:content-['-']">
          {"         .remote(\"sc://your-spark:15002\")"}
        </div>
        <div className="relative rounded-sm bg-success/15 pl-3 text-success before:absolute before:left-0 before:text-accent before:content-['+']">
          {"         .remote(\"sc://localhost:50051\")  # Oxidant"}
        </div>
        <div className="oxidant-code-dim">         .getOrCreate())</div>
      </pre>
    </div>
  );
}
