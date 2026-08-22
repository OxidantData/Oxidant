import { usePolling } from "@/lib/usePolling";
import { api, fmtBytes, type ClusterStatus } from "@/lib/api";
import { useEffect, useRef, useState } from "react";

export default function ClusterPage() {
  const { data: status } = usePolling(() => api.cluster.status(), 3000);
  const { data: logsData } = usePolling(() => api.cluster.logs(), 3000);
  const logsEndRef = useRef<HTMLDivElement>(null);
  const [autoScroll, setAutoScroll] = useState(true);

  useEffect(() => {
    if (autoScroll) {
      logsEndRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [logsData, autoScroll]);

  return (
    <div className="flex h-full flex-col gap-4">
      <div className="grid gap-4 lg:grid-cols-4">
        <MetricCard label="Mode" value={status?.mode ?? "—"} />
        <MetricCard
          label="Workers"
          value={status ? `${status.workers.length}` : "—"}
        />
        <MetricCard
          label="Process memory"
          value={
            status?.process
              ? `${fmtBytes(status.process.memoryUsedMb * 1024 * 1024)} / ${fmtBytes(
                  status.process.memoryTotalMb * 1024 * 1024
                )}`
              : "—"
          }
        />
        <MetricCard
          label="Process CPU"
          value={
            status?.process && status.process.cpuPercent != null
              ? `${status.process.cpuPercent.toFixed(1)}%`
              : "—"
          }
        />
      </div>

      <div className="oxidant-card flex min-h-0 flex-1 flex-col overflow-hidden">
        <div className="mb-2 flex items-center justify-between">
          <span className="oxidant-eyebrow">Cluster topology</span>
          <span className="text-xs text-muted">version {status?.version}</span>
        </div>
        {status?.workers.length ? (
          <div className="overflow-auto">
            <table className="w-full border-collapse text-sm">
              <thead>
                <tr>
                  <th className="border-b border-hairline px-2 py-1 text-left text-muted">
                    Worker endpoint
                  </th>
                </tr>
              </thead>
              <tbody>
                {status.workers.map((w) => (
                  <tr key={w}>
                    <td className="border-b border-hairline px-2 py-1.5 font-mono text-xs">
                      {w}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="text-sm text-muted">Running in single-node mode.</div>
        )}
      </div>

      <div className="oxidant-card flex min-h-0 flex-1 flex-col overflow-hidden">
        <div className="mb-2 flex items-center justify-between">
          <span className="oxidant-eyebrow">Process logs</span>
          <label className="flex items-center gap-1.5 text-xs text-muted">
            <input
              type="checkbox"
              className="accent-solid"
              checked={autoScroll}
              onChange={(e) => setAutoScroll(e.target.checked)}
            />
            Auto-scroll
          </label>
        </div>
        <div className="oxidant-code min-h-0 flex-1 overflow-auto p-2">
          {(logsData?.logs.length ?? 0) === 0 && (
            <span className="text-muted">No logs captured yet.</span>
          )}
          {logsData?.logs.map((line, i) => (
            <div key={i} className="whitespace-pre-wrap py-0.5">
              {line}
            </div>
          ))}
          <div ref={logsEndRef} />
        </div>
      </div>
    </div>
  );
}

function MetricCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="oxidant-card">
      <div className="oxidant-eyebrow">{label}</div>
      <div className="mt-1.5 text-xl font-semibold tracking-display">{value}</div>
    </div>
  );
}
