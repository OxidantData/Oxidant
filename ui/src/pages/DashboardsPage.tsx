import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  dashboardsApi,
  fmtCount,
  fmtRelativeMs,
  type Dashboard,
} from "@/lib/dashboards";

/**
 * The dashboards list: name, widget count, when it last changed.
 *
 * Creating one takes a name and nothing else — the grid is where a dashboard is actually
 * built, so this page's job is to get out of the way and hand over to it.
 */
export default function DashboardsPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [name, setName] = useState("");
  const [error, setError] = useState<string | null>(null);

  const { data: dashboards, isPending, error: loadError } = useQuery({
    queryKey: ["dashboards"],
    queryFn: dashboardsApi.list,
  });

  const create = useMutation({
    mutationFn: (dashboardName: string) =>
      dashboardsApi.create({ name: dashboardName, widgets: [], layout: [] }),
    onSuccess: (created: Dashboard) => {
      queryClient.invalidateQueries({ queryKey: ["dashboards"] });
      setName("");
      setError(null);
      navigate(`/dashboards/${created.id}`);
    },
    onError: (e: Error) => setError(e.message),
  });

  const remove = useMutation({
    mutationFn: (id: string) => dashboardsApi.remove(id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["dashboards"] }),
    onError: (e: Error) => setError(e.message),
  });

  const trimmed = name.trim();

  return (
    <div className="mx-auto flex max-w-4xl flex-col gap-4">
      <div className="oxidant-card space-y-3">
        <div>
          <h1 className="text-base font-medium text-body">Dashboards</h1>
          <p className="mt-1 text-sm text-muted">
            A dashboard is a grid of SQL-backed widgets. Each widget runs its statement against
            this engine on demand, through the same API as the SQL Editor.
          </p>
        </div>
        <form
          className="flex flex-wrap items-center gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            if (trimmed) create.mutate(trimmed);
          }}
        >
          <input
            className="oxidant-input min-w-64 flex-1"
            placeholder="New dashboard name"
            aria-label="New dashboard name"
            value={name}
            onChange={(e) => setName(e.target.value)}
          />
          <button
            className="oxidant-btn-primary"
            type="submit"
            disabled={!trimmed || create.isPending}
          >
            {create.isPending ? "Creating…" : "Create"}
          </button>
        </form>
        {(error || loadError) && (
          <p className="rounded-oxidant-sm border border-danger-line bg-danger-tint p-2 text-xs text-danger">
            {error ?? (loadError as Error).message}
          </p>
        )}
      </div>

      <div className="oxidant-card">
        {isPending ? (
          <p className="text-sm text-muted">Loading…</p>
        ) : !dashboards?.length ? (
          <p className="text-sm text-muted">
            No dashboards yet. Name one above to get started.
          </p>
        ) : (
          <table className="w-full border-collapse text-sm">
            <thead>
              <tr className="text-left">
                <th className="border-b border-hairline px-2 py-2 font-medium text-body">
                  Name
                </th>
                <th className="border-b border-hairline px-2 py-2 font-medium text-body">
                  Widgets
                </th>
                <th className="border-b border-hairline px-2 py-2 font-medium text-body">
                  Auto-refresh
                </th>
                <th className="border-b border-hairline px-2 py-2 font-medium text-body">
                  Updated
                </th>
                <th className="border-b border-hairline px-2 py-2" />
              </tr>
            </thead>
            <tbody>
              {dashboards.map((d) => (
                <tr key={d.id}>
                  <td className="border-b border-hairline px-2 py-2">
                    <Link className="oxidant-link" to={`/dashboards/${d.id}`}>
                      {d.name}
                    </Link>
                  </td>
                  <td className="border-b border-hairline px-2 py-2 text-muted">
                    {fmtCount(d.widgetCount)}
                  </td>
                  <td className="border-b border-hairline px-2 py-2 text-muted">
                    {d.refreshIntervalSecs ? `${d.refreshIntervalSecs}s` : "—"}
                  </td>
                  <td className="border-b border-hairline px-2 py-2 text-muted">
                    {fmtRelativeMs(d.updatedAtMs)}
                  </td>
                  <td className="border-b border-hairline px-2 py-2 text-right">
                    <button
                      className="nb-btn"
                      aria-label={`Delete ${d.name}`}
                      onClick={() => {
                        if (window.confirm(`Delete dashboard “${d.name}”?`)) {
                          remove.mutate(d.id);
                        }
                      }}
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
