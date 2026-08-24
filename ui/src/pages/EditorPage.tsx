import { useCallback, useRef, useState } from "react";
import {
  api,
  DEFAULT_ROW_LIMIT,
  downloadBlob,
  fmtMs,
  MAX_ROW_LIMIT,
  runStatement,
  type StatementDoc,
  type StatementResult,
} from "@/lib/api";
import { usePolling } from "@/lib/usePolling";
import ResultTable from "@/components/ResultTable";
import CatalogSidebar from "@/components/CatalogSidebar";
import SqlAutocompleteTextarea, {
  type SqlAutocompleteTextareaHandle,
} from "@/components/SqlAutocompleteTextarea";

export default function EditorPage() {
  const [sql, setSql] = useState("SELECT 1 AS hello");
  const [doc, setDoc] = useState<StatementDoc | null>(null);
  const [result, setResult] = useState<StatementResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [runningId, setRunningId] = useState<string | null>(null);
  const [limit100, setLimit100] = useState(true);
  const textareaRef = useRef<SqlAutocompleteTextareaHandle>(null);

  const listStatements = useCallback(
    () => api.statements.list().then((d) => d.statements),
    []
  );
  const { data: statements, refresh: refreshStatements } =
    usePolling(listStatements);

  const running = runningId != null;

  async function showDoc(d: StatementDoc) {
    setDoc(d);
    if (d.status === "succeeded") {
      setResult(
        await api.statements.result(
          d.statementId,
          limit100 ? DEFAULT_ROW_LIMIT : MAX_ROW_LIMIT
        )
      );
    } else if (d.status === "failed") {
      setError(d.error ?? "statement failed");
    }
  }

  async function run() {
    const trimmed = sql.trim();
    if (!trimmed || running) return;
    setError(null);
    setResult(null);
    setDoc(null);
    try {
      const d = await runStatement(trimmed, (cur) => {
        setRunningId(cur.statementId);
        setDoc(cur);
      });
      await showDoc(d);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setRunningId(null);
      refreshStatements();
    }
  }

  async function cancel() {
    if (!runningId) return;
    try {
      await api.statements.cancel(runningId);
    } catch {
      /* already terminal */
    }
  }

  async function load(id: string) {
    setError(null);
    setResult(null);
    try {
      let d = await api.statements.get(id);
      setDoc(d);
      while (d.status === "pending" || d.status === "running") {
        await new Promise((r) => setTimeout(r, 1000));
        d = await api.statements.get(id);
        setDoc(d);
      }
      await showDoc(d);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  function insertName(name: string) {
    textareaRef.current?.insertText(name);
  }

  async function downloadCsv() {
    if (!doc?.statementId) return;
    try {
      const text = await api.statements.csv(doc.statementId, MAX_ROW_LIMIT);
      downloadBlob(text, `oxidant-${doc.statementId}.csv`, "text/csv");
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  async function downloadJson() {
    if (!doc?.statementId) return;
    try {
      const data = await api.statements.json(doc.statementId, MAX_ROW_LIMIT);
      downloadBlob(
        JSON.stringify(data, null, 2),
        `oxidant-${doc.statementId}.json`,
        "application/json"
      );
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  return (
    <div className="flex h-full">
      <CatalogSidebar onInsert={insertName} />
      <div className="flex min-w-0 flex-1 flex-col">
        <div className="flex flex-1 gap-4 overflow-hidden">
          <div className="flex min-w-0 flex-1 flex-col space-y-4 overflow-hidden">
            <div className="oxidant-card space-y-3">
              <label className="text-sm text-muted">
                SQL — Cmd/Ctrl+Enter to run
              </label>
              <SqlAutocompleteTextarea
                ref={textareaRef}
                className="oxidant-input h-44 w-full p-3 font-mono"
                value={sql}
                spellCheck={false}
                onChange={(e) => setSql(e.target.value)}
                onKeyDown={(e) => {
                  if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
                    e.preventDefault();
                    run();
                  }
                }}
              />
              <div className="flex flex-wrap items-center gap-3">
                <button
                  className="oxidant-btn-primary"
                  onClick={run}
                  disabled={running}
                >
                  {running ? "Running…" : "Run"}
                </button>
                <button
                  className="oxidant-btn-ghost"
                  onClick={cancel}
                  disabled={!running}
                >
                  Cancel
                </button>
                <label className="flex items-center gap-1.5 text-sm text-muted">
                  <input
                    type="checkbox"
                    className="accent-solid"
                    checked={limit100}
                    onChange={(e) => setLimit100(e.target.checked)}
                  />
                  Limit to {DEFAULT_ROW_LIMIT} rows
                </label>
                {doc && (
                  <span className="text-sm text-muted">
                    <span className={`stmt-${doc.status}`}>{doc.status}</span>
                    {doc.durationMs != null && ` · ${fmtMs(doc.durationMs)}`}
                    {doc.rowCount != null &&
                      ` · ${doc.rowCount.toLocaleString()} rows`}
                  </span>
                )}
              </div>
            </div>

            {error && (
              <div className="shrink-0 whitespace-pre-wrap rounded-oxidant-sm border border-danger-line bg-danger-tint p-3 font-mono text-xs text-danger">
                {error}
              </div>
            )}
            {doc?.status === "canceled" && (
              <div className="shrink-0 oxidant-card text-muted">
                Statement canceled.
              </div>
            )}
            {result && doc && (
              <div className="min-h-0 flex-1 oxidant-card flex flex-col overflow-hidden">
                <div className="mb-2 flex shrink-0 items-center gap-2">
                  <button
                    className="nb-btn"
                    onClick={downloadCsv}
                    disabled={running}
                  >
                    Download CSV
                  </button>
                  <button
                    className="nb-btn"
                    onClick={downloadJson}
                    disabled={running}
                  >
                    Download JSON
                  </button>
                </div>
                <div className="min-h-0 flex-1 overflow-auto">
                  <ResultTable
                    result={result}
                    fullRowCount={doc.rowCount}
                    enablePagination
                  />
                </div>
              </div>
            )}
          </div>

          <div className="w-72 shrink-0 oxidant-card overflow-hidden">
            <div className="mb-2 flex items-center justify-between">
              <span className="oxidant-eyebrow">Recent statements</span>
              <button
                className="text-xs text-muted hover:text-body"
                onClick={refreshStatements}
              >
                Refresh
              </button>
            </div>
            <div className="h-full overflow-y-auto">
              {!statements?.length ? (
                <span className="text-sm text-muted">No statements yet.</span>
              ) : (
                <div className="divide-y divide-hairline">
                  {statements.map((s) => (
                    <button
                      key={s.statementId}
                      className="block w-full py-2 text-left hover:opacity-80"
                      onClick={() => load(s.statementId)}
                    >
                      <div className="flex justify-between text-sm">
                        <span className="flex items-center gap-1.5">
                          <span className={`stmt-${s.status}`}>{s.status}</span>
                          {/* Connect (PySpark) statements share this rail with REST ones since
                              the durable history landed; the badge says which is which. */}
                          {s.source && s.source !== "rest" ? (
                            <span className="rounded bg-hairline px-1 text-[10px] uppercase tracking-wide text-muted">
                              {s.source}
                            </span>
                          ) : null}
                        </span>
                        <span className="text-muted">{fmtMs(s.durationMs)}</span>
                      </div>
                      <div
                        className="truncate font-mono text-xs text-muted"
                        title={s.sql}
                      >
                        {s.sql}
                      </div>
                    </button>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}


