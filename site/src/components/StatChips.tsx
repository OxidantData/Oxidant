import { useEffect, useState } from "react";
import { benchmarks, pctFasterVs } from "../lib/benchmarks";
import { useInView, prefersReducedMotion } from "../lib/useInView";

/** "Weft is N% faster than X" chips that count up from 0 once in view. Honest: only renders
 *  engines with a measured total; falls back to a pending note. "% faster" = wall-clock time
 *  saved vs the other engine: (other − weft) ÷ other. */
export default function StatChips() {
  const cards = benchmarks.engines
    .filter((e) => e.key !== "weft")
    .map((e) => ({ name: e.name, pct: pctFasterVs(e.key) }))
    .filter((c): c is { name: string; pct: number } => c.pct != null);

  const { ref, inView } = useInView<HTMLDivElement>();

  if (cards.length === 0) {
    return (
      <p className="text-sm text-muted">
        Speedups publish here once the fresh benchmark run completes.
      </p>
    );
  }

  return (
    <div ref={ref} className="flex flex-wrap gap-3">
      {cards.map((c) => (
        <Chip key={c.name} name={c.name} pct={c.pct} go={inView} />
      ))}
    </div>
  );
}

function Chip({ name, pct, go }: { name: string; pct: number; go: boolean }) {
  const [val, setVal] = useState(prefersReducedMotion() ? pct : 0);
  useEffect(() => {
    if (!go || prefersReducedMotion()) {
      setVal(pct);
      return;
    }
    const start = performance.now();
    const dur = 900;
    let raf = 0;
    const tick = (t: number) => {
      const p = Math.min(1, (t - start) / dur);
      const eased = 1 - Math.pow(1 - p, 3);
      setVal(pct * eased);
      if (p < 1) raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [go, pct]);

  return (
    <div className="rounded-weft border border-hairline bg-surface px-4 py-2.5">
      <span className="text-xl font-bold tabular-nums text-accent">{val.toFixed(0)}%</span>
      <span className="ml-2 text-sm text-muted">faster than {name}</span>
    </div>
  );
}
