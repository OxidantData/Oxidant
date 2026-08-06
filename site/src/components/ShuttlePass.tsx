import { benchmarks, engineColor, type Engine } from "../lib/benchmarks";
import { useInView, prefersReducedMotion } from "../lib/useInView";

/**
 * The Shuttle Pass — Oxidant's kinetic benchmark reveal (replaces a plain bar teaser). Each engine is
 * a warp lane; a shuttle travels a thread whose length is strictly linear in total seconds (no
 * truncated axis). All launch together at constant speed on scroll-in, so the fastest engine's
 * (shortest) thread completes first — the race *shows* the win instead of asserting it.
 * prefers-reduced-motion → final resting state immediately.
 */
export default function ShuttlePass() {
  const engines = benchmarks.engines
    .filter((e) => e.total != null)
    .slice()
    .sort((a, b) => (a.total ?? 0) - (b.total ?? 0)) as (Engine & { total: number })[];

  const { ref, inView } = useInView<HTMLDivElement>();
  const reduce = prefersReducedMotion();
  const launched = inView || reduce;
  const max = engines.length ? Math.max(...engines.map((e) => e.total)) : 1;
  const SLOWEST_MS = 1500; // the slowest engine takes this long to draw; others scale linearly

  if (engines.length === 0) {
    return (
      <div className="oxidant-card p-8 text-center text-sm text-muted">
        The benchmark race appears here once results are published.
      </div>
    );
  }

  return (
    <div ref={ref} className="oxidant-card overflow-hidden p-6 sm:p-8">
      <div className="mb-6 flex flex-wrap items-baseline justify-between gap-2">
        <h3 className="text-sm font-semibold uppercase tracking-wide">The shuttle pass</h3>
        <span className="text-xs text-muted">
          hot total · {benchmarks.commonCount ?? benchmarks.queryCount} shared queries · lower is
          better
        </span>
      </div>

      <div className="space-y-4">
        {engines.map((e, i) => {
          const pct = (e.total / max) * 100;
          const durMs = reduce ? 0 : Math.max(220, (e.total / max) * SLOWEST_MS);
          return (
            <div key={e.key} className="grid grid-cols-[88px_1fr] items-center gap-3 sm:grid-cols-[110px_1fr]">
              <div className="truncate text-sm font-medium" title={e.name}>
                {e.name}
                {e.highlight && <span className="ml-1.5 text-[10px] font-semibold uppercase text-accent">ours</span>}
              </div>
              <div className="relative h-7">
                {/* the full track (the run) */}
                <div className="absolute inset-0 rounded-oxidant-sm bg-bg-subtle" />
                {/* the thread the shuttle weaves */}
                <div
                  className="absolute inset-y-0 left-0 flex items-center justify-end rounded-oxidant-sm"
                  style={{
                    width: launched ? `${pct}%` : "0%",
                    backgroundColor: engineColor(e.key),
                    opacity: e.highlight ? 1 : 0.85,
                    transition: reduce ? "none" : `width ${durMs}ms cubic-bezier(.22,1,.36,1)`,
                    transitionDelay: reduce ? "0ms" : `${i * 60}ms`,
                  }}
                >
                  {/* the shuttle at the leading edge */}
                  <span
                    className="mr-[-5px] h-3 w-3 rotate-45 rounded-[2px] shadow-sm"
                    style={{ backgroundColor: e.highlight ? "var(--oxidant-accent)" : "var(--oxidant-text)" }}
                  />
                </div>
                {/* time label settles in when the thread lands */}
                <span
                  className="absolute right-2 top-1/2 -translate-y-1/2 text-xs font-semibold tabular-nums"
                  style={{
                    color: pct > 78 ? "var(--oxidant-accent-contrast)" : "var(--oxidant-text)",
                    opacity: launched ? 1 : 0,
                    transition: reduce ? "none" : "opacity .3s ease",
                    transitionDelay: reduce ? "0ms" : `${durMs + i * 60}ms`,
                  }}
                >
                  {e.total.toFixed(1)}s
                </span>
              </div>
            </div>
          );
        })}
      </div>

      <p className="mt-5 text-xs text-muted">
        Thread length is linear in seconds; all engines launch together, so the fastest finishes
        first. Numbers are the common-set totals every engine completed — full methodology on the{" "}
        <a className="text-accent hover:underline" href="#/performance">
          Benchmarks page
        </a>
        .
      </p>
    </div>
  );
}
