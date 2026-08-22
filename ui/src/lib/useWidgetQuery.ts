/**
 * Running a widget's SQL.
 *
 * A widget is not a special kind of query: it goes through `runStatement`, the same
 * submit-and-poll path the SQL Editor uses, so a widget refresh appears on the Jobs and SQL
 * monitoring pages exactly like a hand-typed statement. TanStack Query supplies the caching,
 * the de-duplication (two widgets with identical SQL run once) and the auto-refresh interval.
 */
import { useQuery, type UseQueryResult } from "@tanstack/react-query";
import { api, runStatement, type StatementResult } from "@/lib/api";

/**
 * Rows fetched per widget. A chart that needs more than this is a chart nobody can read —
 * aggregate in SQL. The table widget is bound by the same limit and says so when it hits it.
 */
export const WIDGET_ROW_LIMIT = 1000;

/** Query key prefix, so a "Refresh" button can invalidate every widget at once. */
export const WIDGET_QUERY_KEY = "widget-sql";

export interface WidgetQueryOptions {
  /** Off while a widget is being edited — no point querying a half-typed statement. */
  enabled?: boolean;
  /** Auto-refresh period in ms, from the dashboard's `refreshIntervalSecs`. */
  refetchIntervalMs?: number | false;
}

export function useWidgetQuery(
  sql: string,
  { enabled = true, refetchIntervalMs = false }: WidgetQueryOptions = {}
): UseQueryResult<StatementResult, Error> {
  return useQuery<StatementResult, Error>({
    queryKey: [WIDGET_QUERY_KEY, sql],
    enabled: enabled && sql.trim().length > 0,
    queryFn: async () => {
      const doc = await runStatement(sql);
      if (doc.status !== "succeeded") {
        throw new Error(doc.error ?? `statement ${doc.status}`);
      }
      return api.statements.result(doc.statementId, WIDGET_ROW_LIMIT);
    },
    refetchInterval: refetchIntervalMs,
    // A dashboard tab left open for an hour should show what the interval promised, not a
    // burst of refetches the moment it regains focus.
    refetchOnWindowFocus: false,
    staleTime: 30_000,
    retry: false,
  });
}
