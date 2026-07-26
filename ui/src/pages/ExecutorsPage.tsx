import { api, fmtBytes } from "@/lib/api";
import { usePolling } from "@/lib/usePolling";

export default function ExecutorsPage() {
  const { data: executors, error } = usePolling(() => api.executors());

  if (error) return <div className="weft-card text-danger">{error}</div>;
  if (!executors?.length)
    return (
      <div className="weft-card text-muted">
        No executors registered. Configure workers for distributed mode.
      </div>
    );

  return (
    <div className="weft-card overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-muted">
            <th className="p-2 text-left">ID</th>
            <th className="p-2 text-left">Host</th>
            <th className="p-2 text-left">Active</th>
            <th className="p-2 text-left">Completed</th>
            <th className="p-2 text-left">Shuffle Read</th>
            <th className="p-2 text-left">Shuffle Write</th>
          </tr>
        </thead>
        <tbody>
          {executors.map((e) => (
            <tr key={e.id} className="border-t border-border">
              <td className="p-2">{e.id}</td>
              <td className="p-2 font-mono text-xs">{e.hostPort}</td>
              <td className="p-2">{e.activeTasks}</td>
              <td className="p-2">{e.completedTasks}</td>
              <td className="p-2">{fmtBytes(e.totalShuffleRead)}</td>
              <td className="p-2">{fmtBytes(e.totalShuffleWrite)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
