import { useState } from "react";
import { engineColor, type Engine } from "../lib/benchmarks";

/**
 * Per-query hot-time bars across all 43 queries, grouped by engine. SVG, no chart lib — themeable
 * and dependency-free. If no engine is measured yet, shows a placeholder so the section still
 * renders during the `pending` phase.
 */
export default function PerQueryChart({
  engines,
  queryCount,
  queryBase = 0,
}: {
  engines: Engine[];
  queryCount: number;
  /** First query number for axis labels (ClickBench is 0-based; TPC-H/DS are 1-based). */
  queryBase?: number;
}) {
  const measured = engines.filter((e) => e.perQuery.length > 0);
  const [logScale, setLogScale] = useState(true);

  if (measured.length === 0) {
    return (
      <div className="oxidant-card flex h-56 items-center justify-center p-6 text-sm text-muted">
        Per-query results appear here once the benchmark has run.
      </div>
    );
  }

  const W = 980;
  const H = 320;
  const padL = 40;
  const padB = 28;
  const padT = 12;
  const plotW = W - padL - 8;
  const plotH = H - padB - padT;

  const allVals = measured.flatMap((e) => e.perQuery.filter((v): v is number => v != null));
  const maxV = Math.max(...allVals, 0.001);
  const scale = (v: number) => {
    if (logScale) {
      const lo = Math.log10(0.001);
      const hi = Math.log10(maxV);
      return ((Math.log10(Math.max(v, 0.001)) - lo) / (hi - lo)) * plotH;
    }
    return (v / maxV) * plotH;
  };

  const n = queryCount;
  const groupW = plotW / n;
  const barW = Math.max(1.2, (groupW * 0.8) / measured.length);

  return (
    <div className="oxidant-card p-5 sm:p-6">
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <h3 className="text-sm font-semibold">
          Per-query hot time (Q{queryBase}–Q{queryBase + n - 1})
        </h3>
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-3 text-xs">
            {measured.map((e) => (
              <span key={e.key} className="flex items-center gap-1.5">
                <span
                  className="inline-block h-2.5 w-2.5 rounded-sm"
                  style={{ background: engineColor(e.key) }}
                />
                {e.name}
              </span>
            ))}
          </div>
          <button
            onClick={() => setLogScale((s) => !s)}
            className="rounded-oxidant-sm border border-hairline px-2 py-1 text-xs text-muted hover:text-body"
          >
            {logScale ? "log" : "linear"}
          </button>
        </div>
      </div>
      <div className="overflow-x-auto">
        <svg viewBox={`0 0 ${W} ${H}`} className="w-full min-w-[680px]" role="img">
          {/* y gridlines */}
          {[0.25, 0.5, 0.75, 1].map((f) => (
            <line
              key={f}
              x1={padL}
              x2={W - 8}
              y1={padT + plotH - f * plotH}
              y2={padT + plotH - f * plotH}
              stroke="var(--oxidant-border)"
              strokeWidth={1}
            />
          ))}
          {/* bars */}
          {Array.from({ length: n }).map((_, qi) => {
            const gx = padL + qi * groupW + groupW * 0.1;
            return measured.map((e, ei) => {
              const v = e.perQuery[qi];
              if (v == null) return null;
              const h = scale(v);
              return (
                <rect
                  key={`${e.key}-${qi}`}
                  x={gx + ei * barW}
                  y={padT + plotH - h}
                  width={barW}
                  height={Math.max(0.5, h)}
                  fill={engineColor(e.key)}
                >
                  <title>{`${e.name} · Q${qi + queryBase}: ${v.toFixed(3)}s`}</title>
                </rect>
              );
            });
          })}
          {/* x label ticks every 5 */}
          {Array.from({ length: n }).map((_, qi) =>
            qi % 5 === 0 ? (
              <text
                key={`t${qi}`}
                x={padL + qi * groupW + groupW / 2}
                y={H - 8}
                textAnchor="middle"
                className="fill-muted"
                fontSize={10}
              >
                {qi + queryBase}
              </text>
            ) : null,
          )}
        </svg>
      </div>
      <p className="mt-2 text-xs text-muted">
        Hover a bar for the exact time. {logScale ? "Log" : "Linear"} scale; hot = min(try2, try3).
      </p>
    </div>
  );
}
