import { api, fmtMs } from "@/lib/api";
import { usePolling } from "@/lib/usePolling";

export default function SqlPage() {
  const { data: sql, error } = usePolling(() => api.sql());

  if (error) return <div className="weft-card text-danger">{error}</div>;
  if (!sql?.length) return <div className="weft-card text-muted">No SQL executions yet.</div>;

  return (
    <div className="space-y-4">
      {sql.map((s) => (
        <div key={s.id} className="weft-card">
          <div className="mb-2 flex flex-wrap gap-2 text-sm">
            <strong>{s.description}</strong>
            <span className={`status-${s.status}`}>{s.status}</span>
            <span className="text-muted">{fmtMs(s.duration)}</span>
          </div>
          <pre className="max-h-96 overflow-auto rounded bg-[#0b0b0c] p-3 font-mono text-xs leading-relaxed">
            {s.physicalPlan || "(no plan captured)"}
          </pre>
        </div>
      ))}
    </div>
  );
}
