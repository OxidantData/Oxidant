import { useCallback, useEffect, useState } from "react";
import { api } from "@/lib/api";

export function usePolling<T>(fetcher: () => Promise<T>, intervalMs = 2000) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      setData(await fetcher());
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [fetcher]);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, intervalMs);
    let es: EventSource | null = null;
    try {
      es = new EventSource("/api/v1/events/stream");
      es.onmessage = () => refresh();
    } catch {
      /* SSE optional */
    }
    return () => {
      clearInterval(id);
      es?.close();
    };
  }, [refresh, intervalMs]);

  return { data, error, refresh };
}

export function useAppMeta() {
  return usePolling(async () => {
    const apps = (await api.applications()) as { name?: string }[];
    const jobs = await api.jobs();
    return { name: apps[0]?.name ?? "Weft", jobCount: jobs.length };
  });
}
