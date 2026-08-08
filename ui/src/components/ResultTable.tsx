import { useMemo, useState } from "react";
import type { StatementResult } from "@/lib/api";

interface ResultTableProps {
  result: StatementResult;
  fullRowCount?: number;
  enablePagination?: boolean;
}

const PAGE_SIZES = [25, 50, 100, 250];
const DEFAULT_PAGE_SIZE = 50;

function cellText(v: unknown): string | null {
  if (v == null) return null;
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

export default function ResultTable({
  result,
  fullRowCount,
  enablePagination = false,
}: ResultTableProps) {
  const fields = result.schema?.fields ?? [];
  const cols = fields.length
    ? fields.map((f) => f.name)
    : Object.keys(result.rows[0] ?? {});
  const typeOf = (c: string) => fields.find((f) => f.name === c)?.type ?? "";
  const total = fullRowCount ?? result.rowCount;

  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE);

  const { pageRows, pageCount } = useMemo(() => {
    if (!enablePagination) {
      return { pageRows: result.rows, pageCount: 1 };
    }
    const count = Math.max(1, Math.ceil(result.rows.length / pageSize));
    const start = page * pageSize;
    return {
      pageRows: result.rows.slice(start, start + pageSize),
      pageCount: count,
    };
  }, [result.rows, page, pageSize, enablePagination]);

  return (
    <div className="flex h-full flex-col">
      <div className="mb-2 flex flex-wrap items-center justify-between gap-2 text-xs text-muted">
        <div>
          {total > result.rows.length
            ? `Showing ${result.rows.length.toLocaleString()} of ${total.toLocaleString()} rows fetched.`
            : `${result.rows.length.toLocaleString()} row${
                result.rows.length === 1 ? "" : "s"
              }`}
          {result.truncated && (
            <span className="ml-2 text-warning">
              Result was truncated by server limit.
            </span>
          )}
        </div>
        {enablePagination && pageCount > 1 && (
          <div className="flex items-center gap-2">
            <select
              className="rounded border border-border bg-bg px-1.5 py-1 text-xs"
              value={pageSize}
              onChange={(e) => {
                setPageSize(Number(e.target.value));
                setPage(0);
              }}
            >
              {PAGE_SIZES.map((s) => (
                <option key={s} value={s}>
                  {s} / page
                </option>
              ))}
            </select>
            <button
              className="rounded border border-border px-2 py-1 hover:bg-surface disabled:opacity-40"
              disabled={page === 0}
              onClick={() => setPage((p) => p - 1)}
            >
              Prev
            </button>
            <span>
              {page + 1} / {pageCount}
            </span>
            <button
              className="rounded border border-border px-2 py-1 hover:bg-surface disabled:opacity-40"
              disabled={page >= pageCount - 1}
              onClick={() => setPage((p) => p + 1)}
            >
              Next
            </button>
          </div>
        )}
      </div>

      <div className="min-h-0 flex-1 overflow-auto">
        <table className="w-full border-collapse text-sm">
          <thead className="sticky top-0 z-10 bg-surface">
            <tr>
              {cols.map((c) => (
                <th
                  key={c}
                  className="border-b border-border px-2.5 py-2 text-left"
                >
                  <div className="flex items-center gap-2">
                    <TypeIcon type={typeOf(c)} />
                    <span className="font-medium text-text">{c}</span>
                  </div>
                  <div className="mt-0.5 pl-6 text-xs font-normal text-muted">
                    {typeOf(c)}
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {pageRows.map((r, i) => (
              <tr key={i}>
                {cols.map((c) => {
                  const t = cellText(r[c]);
                  return (
                    <td
                      key={c}
                      className={`border-b border-border px-2.5 py-2 whitespace-nowrap ${
                        t == null ? "text-muted" : ""
                      }`}
                    >
                      {t ?? "NULL"}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function TypeIcon({ type }: { type: string }) {
  const normalized = type.toLowerCase();
  if (
    normalized.includes("int") ||
    normalized.includes("float") ||
    normalized.includes("double") ||
    normalized.includes("decimal") ||
    normalized.includes("numeric")
  ) {
    return <NumberIcon />;
  }
  if (normalized.includes("bool")) {
    return <BooleanIcon />;
  }
  if (
    normalized.includes("timestamp") ||
    normalized.includes("date") ||
    normalized.includes("time")
  ) {
    return <DateIcon />;
  }
  if (normalized.includes("binary")) {
    return <BinaryIcon />;
  }
  if (normalized.includes("struct") || normalized.includes("map") || normalized.includes("list")) {
    return <ComplexIcon />;
  }
  return <StringIcon />;
}

function StringIcon() {
  return (
    <svg
      className="h-3.5 w-3.5 text-muted"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M4 7V4h16v3" />
      <path d="M9 20h6" />
      <path d="M12 4v16" />
    </svg>
  );
}

function NumberIcon() {
  return (
    <svg
      className="h-3.5 w-3.5 text-accent"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M4 20V10" />
      <path d="M4 10V4l6 6" />
      <path d="M10 20V10" />
      <path d="M10 4v6l6 6" />
      <path d="M20 20V10" />
      <path d="M20 10V4" />
    </svg>
  );
}

function BooleanIcon() {
  return (
    <svg
      className="h-3.5 w-3.5 text-success"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M12 22c5.523 0 10-4.477 10-10S17.523 2 12 2 2 6.477 2 12s4.477 10 10 10z" />
      <path d="m9 12 2 2 4-4" />
    </svg>
  );
}

function DateIcon() {
  return (
    <svg
      className="h-3.5 w-3.5 text-warning"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <rect x="3" y="4" width="18" height="18" rx="2" />
      <path d="M16 2v4" />
      <path d="M8 2v4" />
      <path d="M3 10h18" />
    </svg>
  );
}

function BinaryIcon() {
  return (
    <svg
      className="h-3.5 w-3.5 text-muted"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <rect x="2" y="2" width="20" height="20" rx="5" />
      <path d="M8 9h1v6H8z" />
      <path d="M15 9h1v6h-1z" />
    </svg>
  );
}

function ComplexIcon() {
  return (
    <svg
      className="h-3.5 w-3.5 text-muted"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M12 3l9 4.5v9L12 21l-9-4.5v-9L12 3z" />
      <path d="M12 12 3 7.5" />
      <path d="M12 12v9" />
      <path d="M12 12 21 7.5" />
    </svg>
  );
}
