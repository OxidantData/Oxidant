import { useEffect, useRef, useState } from "react";
import {
  api,
  fmtMs,
  runStatement,
  type StatementDoc,
  type StatementResult,
} from "@/lib/api";
import { renderMarkdown } from "@/lib/markdown";
import ResultTable from "@/components/ResultTable";

type CellType = "sql" | "md";

interface Cell {
  id: string;
  type: CellType;
  source: string;
}

interface CellOutput {
  running?: boolean;
  doc?: StatementDoc;
  result?: StatementResult;
  error?: string;
}

const STORAGE_KEY = "oxidant.notebook.v1";

function uid(): string {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
}

/** Parse a persisted/exported notebook JSON into cells (accepts `markdown` as an alias). */
function parseCells(parsed: { cells?: { type?: string; source?: unknown }[] }): Cell[] {
  return (parsed.cells ?? [])
    .filter((c) => c && (c.type === "sql" || c.type === "md" || c.type === "markdown"))
    .map((c) => ({
      id: uid(),
      type: (c.type === "markdown" ? "md" : c.type) as CellType,
      source: String(c.source ?? ""),
    }));
}

function loadCells(): Cell[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) {
      const cells = parseCells(JSON.parse(raw));
      if (cells.length) return cells;
    }
  } catch {
    /* corrupted storage — fall through to the default cell */
  }
  return [{ id: uid(), type: "sql", source: "SELECT 1 AS hello" }];
}

export default function NotebookPage() {
  const [cells, setCells] = useState<Cell[]>(loadCells);
  const [outputs, setOutputs] = useState<Record<string, CellOutput>>({});
  const [runningAll, setRunningAll] = useState(false);
  const fileRef = useRef<HTMLInputElement>(null);

  // Persist cell source (never outputs) on every change.
  useEffect(() => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        version: 1,
        cells: cells.map(({ type, source }) => ({ type, source })),
      })
    );
  }, [cells]);

  function add(type: CellType) {
    setCells((cs) => [...cs, { id: uid(), type, source: "" }]);
  }

  function update(id: string, source: string) {
    setCells((cs) => cs.map((c) => (c.id === id ? { ...c, source } : c)));
  }

  function remove(id: string) {
    setCells((cs) => cs.filter((c) => c.id !== id));
    setOutputs((o) => {
      const rest = { ...o };
      delete rest[id];
      return rest;
    });
  }

  function move(id: string, dir: -1 | 1) {
    setCells((cs) => {
      const i = cs.findIndex((c) => c.id === id);
      const j = i + dir;
      if (i < 0 || j < 0 || j >= cs.length) return cs;
      const next = [...cs];
      [next[i], next[j]] = [next[j], next[i]];
      return next;
    });
  }

  async function runCell(id: string) {
    const cell = cells.find((c) => c.id === id);
    if (!cell || cell.type !== "sql" || !cell.source.trim()) return;
    if (outputs[id]?.running) return;
    setOutputs((o) => ({ ...o, [id]: { running: true } }));
    try {
      const doc = await runStatement(cell.source, (d) =>
        setOutputs((o) => ({ ...o, [id]: { running: true, doc: d } }))
      );
      if (doc.status === "succeeded") {
        const result = await api.statements.result(doc.statementId, 500);
        setOutputs((o) => ({ ...o, [id]: { doc, result } }));
      } else {
        setOutputs((o) => ({ ...o, [id]: { doc } }));
      }
    } catch (e) {
      setOutputs((o) => ({
        ...o,
        [id]: { error: e instanceof Error ? e.message : String(e) },
      }));
    }
  }

  async function runAll() {
    if (runningAll) return;
    setRunningAll(true);
    try {
      for (const c of cells) {
        if (c.type === "sql") await runCell(c.id);
      }
    } finally {
      setRunningAll(false);
    }
  }

  function exportNotebook() {
    const data = JSON.stringify(
      { version: 1, cells: cells.map(({ type, source }) => ({ type, source })) },
      null,
      2
    );
    const blob = new Blob([data], { type: "application/json" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "oxidant-notebook.json";
    a.click();
    URL.revokeObjectURL(a.href);
  }

  function importNotebook(file: File) {
    const reader = new FileReader();
    reader.onload = () => {
      try {
        const imported = parseCells(JSON.parse(String(reader.result)));
        if (!imported.length) throw new Error("no cells in file");
        setCells(imported);
        setOutputs({});
      } catch (e) {
        window.alert(
          `Import failed: ${e instanceof Error ? e.message : String(e)}`
        );
      }
    };
    reader.readAsText(file);
  }

  return (
    <div className="space-y-4">
      <div className="oxidant-card flex flex-wrap items-center gap-2">
        <button className="oxidant-btn" onClick={() => add("sql")}>
          + SQL cell
        </button>
        <button className="nb-btn" onClick={() => add("md")}>
          + Markdown cell
        </button>
        <button className="nb-btn" onClick={runAll} disabled={runningAll}>
          {runningAll ? "Running…" : "Run all"}
        </button>
        <button className="nb-btn" onClick={exportNotebook}>
          Export
        </button>
        <button className="nb-btn" onClick={() => fileRef.current?.click()}>
          Import
        </button>
        <input
          ref={fileRef}
          type="file"
          accept="application/json"
          className="hidden"
          onChange={(e) => {
            const f = e.target.files?.[0];
            if (f) importNotebook(f);
            e.target.value = "";
          }}
        />
      </div>
      {!cells.length && (
        <div className="oxidant-card text-muted">
          Empty notebook. Add a cell to get started.
        </div>
      )}
      {cells.map((cell, i) => {
        const out = outputs[cell.id];
        return (
          <div key={cell.id} className="oxidant-card space-y-2">
            <div className="flex flex-wrap items-center gap-1.5">
              <span className="rounded border border-border px-2 py-0.5 text-xs text-muted">
                {cell.type === "sql" ? "SQL" : "Markdown"}
              </span>
              {cell.type === "sql" && (
                <button
                  className="nb-btn"
                  onClick={() => runCell(cell.id)}
                  disabled={out?.running}
                >
                  {out?.running ? "Running…" : "Run"}
                </button>
              )}
              <button
                className="nb-btn"
                onClick={() => move(cell.id, -1)}
                disabled={i === 0}
              >
                ↑
              </button>
              <button
                className="nb-btn"
                onClick={() => move(cell.id, 1)}
                disabled={i === cells.length - 1}
              >
                ↓
              </button>
              <button className="nb-btn" onClick={() => remove(cell.id)}>
                Delete
              </button>
            </div>
            <textarea
              className="w-full rounded-md border border-border bg-bg p-3 font-mono text-sm focus:border-accent focus:outline-none"
              rows={Math.min(12, Math.max(3, cell.source.split("\n").length + 1))}
              value={cell.source}
              spellCheck={false}
              onChange={(e) => update(cell.id, e.target.value)}
              onKeyDown={(e) => {
                if (
                  (e.metaKey || e.ctrlKey) &&
                  e.key === "Enter" &&
                  cell.type === "sql"
                ) {
                  e.preventDefault();
                  runCell(cell.id);
                }
              }}
            />
            {cell.type === "md" && (
              <div
                className="md-preview text-sm"
                dangerouslySetInnerHTML={{ __html: renderMarkdown(cell.source) }}
              />
            )}
            {cell.type === "sql" && out && (
              <div className="overflow-x-auto">
                {out.running && (
                  <div className="text-xs text-muted">
                    Running…{" "}
                    {out.doc && (
                      <span className={`stmt-${out.doc.status}`}>
                        {out.doc.status}
                      </span>
                    )}
                  </div>
                )}
                {!out.running && out.error && (
                  <div className="whitespace-pre-wrap rounded-md border border-danger bg-danger/10 p-3 font-mono text-xs text-danger">
                    {out.error}
                  </div>
                )}
                {!out.running && out.doc?.status === "failed" && (
                  <div className="whitespace-pre-wrap rounded-md border border-danger bg-danger/10 p-3 font-mono text-xs text-danger">
                    {out.doc.error ?? "statement failed"}
                  </div>
                )}
                {!out.running && out.doc?.status === "canceled" && (
                  <div className="text-xs text-muted">Canceled.</div>
                )}
                {!out.running && out.result && out.doc && (
                  <>
                    <div className="mb-1 text-xs text-muted">
                      succeeded · {fmtMs(out.doc.durationMs)}
                    </div>
                    <ResultTable
                      result={out.result}
                      fullRowCount={out.doc.rowCount}
                    />
                  </>
                )}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
