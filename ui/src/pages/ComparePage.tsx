import { useState } from "react";
import { api, fmtBytes, fmtMs, jobDuration, type JobData, type StageData } from "@/lib/api";
import { usePolling } from "@/lib/usePolling";

export default function ComparePage() {
  const { data: weftJobs } = usePolling(() => api.jobs());
  const { data: weftStages } = usePolling(() => api.stages(false));
  const [sparkUrl, setSparkUrl] = useState(
    () => localStorage.getItem("weft.sparkUrl") || "http://localhost:4040"
  );
  const [result, setResult] = useState<React.ReactNode>(null);
  const [loading, setLoading] = useState(false);

  async function compare() {
    localStorage.setItem("weft.sparkUrl", sparkUrl);
    setLoading(true);
    try {
      const base = sparkUrl.replace(/\/$/, "");
      const apps = (await api.sparkProxy(`${base}/api/v1/applications`)) as {
        id: string;
      }[];
      const appId = apps[0]?.id;
      let sparkJobs: JobData[] = [];
      let sparkStages: StageData[] = [];
      if (appId) {
        sparkJobs = (await api.sparkProxy(
          `${base}/api/v1/applications/${appId}/jobs`
        )) as JobData[];
        sparkStages = (await api.sparkProxy(
          `${base}/api/v1/applications/${appId}/stages`
        )) as StageData[];
      }
      const weftDur = (weftJobs ?? []).reduce(
        (a, j) => a + (jobDuration(j) ?? 0),
        0
      );
      const sparkDur = sparkJobs.reduce((a, j) => a + (jobDuration(j) ?? 0), 0);
      const delta =
        weftDur && sparkDur
          ? (((sparkDur - weftDur) / sparkDur) * 100).toFixed(1)
          : null;
      const weftShuffle = (weftStages ?? []).reduce(
        (a, s) => a + (s.shuffleReadBytes || 0),
        0
      );
      const sparkShuffle = sparkStages.reduce(
        (a, s) => a + (s.shuffleReadBytes || 0),
        0
      );

      setResult(
        <div className="space-y-4">
          <div className="grid gap-4 md:grid-cols-2">
            <div className="weft-card">
              <h3 className="mb-2 font-semibold">Weft</h3>
              <p>Jobs: {(weftJobs ?? []).length}</p>
              <p>Total duration: {fmtMs(weftDur)}</p>
              <p>Shuffle read: {fmtBytes(weftShuffle)}</p>
            </div>
            <div className="weft-card">
              <h3 className="mb-2 font-semibold">Spark</h3>
              <p>Jobs: {sparkJobs.length}</p>
              <p>Total duration: {fmtMs(sparkDur)}</p>
              <p>Shuffle read: {fmtBytes(sparkShuffle)}</p>
            </div>
          </div>
          {delta != null && (
            <div className="weft-card">
              <p className={weftDur < sparkDur ? "text-success" : "text-danger"}>
                Weft is {Math.abs(Number(delta))}%{" "}
                {weftDur < sparkDur ? "faster" : "slower"} than Spark (job wall-clock sum)
              </p>
            </div>
          )}
          <div className="grid gap-4 md:grid-cols-2">
            <div className="weft-card">
              <h4 className="mb-2 font-medium">Weft stages</h4>
              <pre className="font-mono text-xs">
                {(weftStages ?? [])
                  .map((s) => `Stage ${s.stageId}: ${s.name} (${fmtMs(s.executorRunTime)})`)
                  .join("\n") || "—"}
              </pre>
            </div>
            <div className="weft-card">
              <h4 className="mb-2 font-medium">Spark stages</h4>
              <pre className="font-mono text-xs">
                {sparkStages
                  .map((s) => `Stage ${s.stageId}: ${s.name} (${fmtMs(s.executorRunTime)})`)
                  .join("\n") || "—"}
              </pre>
            </div>
          </div>
        </div>
      );
    } catch (e) {
      setResult(
        <div className="weft-card text-danger">
          Compare failed: {e instanceof Error ? e.message : String(e)}
        </div>
      );
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="space-y-4">
      <div className="weft-card space-y-2">
        <label className="text-sm text-muted">Spark UI / History Server base URL</label>
        <input
          className="w-full max-w-lg rounded-md border border-border bg-bg px-3 py-2 text-sm"
          value={sparkUrl}
          onChange={(e) => setSparkUrl(e.target.value)}
        />
        <button className="weft-btn" onClick={compare} disabled={loading}>
          {loading ? "Loading…" : "Compare"}
        </button>
      </div>
      {result}
    </div>
  );
}
