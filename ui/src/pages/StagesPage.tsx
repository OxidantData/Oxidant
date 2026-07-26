import { api, fmtBytes, fmtMs } from "@/lib/api";
import { usePolling } from "@/lib/usePolling";

export default function StagesPage() {
  const { data: stages, error } = usePolling(() => api.stages(true));
  const maxDur = Math.max(...(stages?.map((s) => s.executorRunTime) ?? [1]), 1);

  if (error) return <div className="weft-card text-danger">{error}</div>;
  if (!stages?.length) return <div className="weft-card text-muted">No stages yet.</div>;

  return (
    <div className="weft-card overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-muted">
            <th className="p-2 text-left">Stage</th>
            <th className="p-2 text-left">Description</th>
            <th className="p-2 text-left">Duration</th>
            <th className="p-2 text-left">Tasks</th>
            <th className="p-2 text-left">Shuffle Read</th>
            <th className="p-2 text-left">Shuffle Write</th>
            <th className="p-2 text-left">Status</th>
          </tr>
        </thead>
        <tbody>
          {stages.map((s) => (
            <tr key={s.stageId} className="border-t border-border">
              <td className="p-2">{s.stageId}</td>
              <td className="p-2">{s.name}</td>
              <td className="p-2">
                <div className="flex items-center gap-2">
                  <span>{fmtMs(s.executorRunTime)}</span>
                  <div className="h-2 flex-1 max-w-[120px] rounded bg-border">
                    <div
                      className="h-2 rounded bg-accent"
                      style={{ width: `${((s.executorRunTime || 0) / maxDur) * 100}%` }}
                    />
                  </div>
                </div>
              </td>
              <td className="p-2">
                {s.numCompleteTasks}/{s.numTasks}
              </td>
              <td className="p-2">{fmtBytes(s.shuffleReadBytes)}</td>
              <td className="p-2">{fmtBytes(s.shuffleWriteBytes)}</td>
              <td className={`p-2 status-${s.status}`}>{s.status}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
