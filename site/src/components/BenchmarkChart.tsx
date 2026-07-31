import { engineColor, type Engine } from "../lib/benchmarks";

/**
 * Total-runtime horizontal bar chart (lower = faster). Measured engines draw a solid bar scaled
 * to the slowest measured total; engines still `pending` draw a muted striped "awaiting run" bar
 * so the layout is honest and final before any number exists.
 */
export default function BenchmarkChart({
  engines,
  title = "Total hot runtime",
}: {
  engines: Engine[];
  title?: string;
}) {
  const weftTotal =
    (engines.find((e) => e.key === "weft") ?? engines.find((e) => e.highlight))?.total ?? null;
  const measured = engines.filter((e) => e.total != null) as (Engine & { total: number })[];
  const max = measured.length ? Math.max(...measured.map((e) => e.total)) : 1;
  // Fastest first; pending (null) last — matches the ShuttlePass order.
  const ordered = [...engines].sort((a, b) => (a.total ?? Infinity) - (b.total ?? Infinity));

  return (
    <div className="weft-card p-5 sm:p-6">
      <div className="mb-5 flex items-baseline justify-between">
        <h3 className="text-sm font-semibold">{title}</h3>
        <span className="text-xs text-muted">seconds · lower is better</span>
      </div>
      <div className="space-y-3.5">
        {ordered.map((e) => {
          const pending = e.total == null;
          const pct = pending ? 100 : Math.max(4, (e.total! / max) * 100);
          return (
            <div key={e.key} className="grid grid-cols-[140px_1fr_64px] items-center gap-3">
              <div className="truncate text-sm font-medium" title={e.name}>
                {e.name}
                {e.highlight && (
                  <span className="ml-1.5 align-middle text-[10px] font-semibold uppercase tracking-wide text-accent">
                    ours
                  </span>
                )}
              </div>
              <div className="relative flex h-7 items-center overflow-hidden rounded-weft-sm bg-bg-subtle">
                <div
                  className={
                    pending
                      ? "h-full rounded-weft-sm border border-dashed border-hairline bg-[repeating-linear-gradient(45deg,transparent,transparent_6px,var(--weft-border)_6px,var(--weft-border)_7px)]"
                      : "h-full rounded-weft-sm"
                  }
                  style={
                    pending
                      ? { width: `${pct}%` }
                      : { width: `${pct}%`, backgroundColor: engineColor(e.key) }
                  }
                />
                {/* "×N slower than Weft" tick at the bar's end */}
                {!pending && !e.highlight && weftTotal && (
                  <span className="ml-2 whitespace-nowrap text-[11px] font-medium tabular-nums text-muted">
                    {(e.total! / weftTotal).toFixed(2)}× Weft
                  </span>
                )}
              </div>
              <div className="text-right text-sm tabular-nums">
                {pending ? (
                  <span className="text-xs text-muted">pending</span>
                ) : (
                  <span className="font-semibold">{e.total!.toFixed(1)}s</span>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
