import { api, fmtBytes, fmtMs, jobDuration } from "@/lib/api";
import { usePolling } from "@/lib/usePolling";

export default function JobsPage() {
  const { data: jobs, error } = usePolling(() => api.jobs());

  if (error) return <div className="weft-card text-danger">{error}</div>;
  if (!jobs?.length)
    return <div className="weft-card text-muted">No jobs yet. Run a query via PySpark.</div>;

  return (
    <div className="weft-card overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-muted">
            <th className="p-2 text-left">Job ID</th>
            <th className="p-2 text-left">Description</th>
            <th className="p-2 text-left">Submitted</th>
            <th className="p-2 text-left">Duration</th>
            <th className="p-2 text-left">Stages</th>
            <th className="p-2 text-left">Status</th>
          </tr>
        </thead>
        <tbody>
          {jobs.map((j) => (
            <tr key={j.jobId} className="border-t border-border">
              <td className="p-2">{j.jobId}</td>
              <td className="p-2 max-w-md truncate" title={j.description}>
                {j.name || j.description}
              </td>
              <td className="p-2 text-muted">{j.submissionTime ?? "—"}</td>
              <td className="p-2">{fmtMs(jobDuration(j))}</td>
              <td className="p-2">{j.stageIds?.join(", ") || "—"}</td>
              <td className={`p-2 status-${j.status}`}>{j.status}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
