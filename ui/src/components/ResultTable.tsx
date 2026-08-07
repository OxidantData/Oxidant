import type { StatementResult } from "@/lib/api";

function cellText(v: unknown): string | null {
  if (v == null) return null;
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

/**
 * Schema-aware result table for a succeeded statement. Rendered rows are capped
 * by the fetch limit; `fullRowCount` (the status doc's rowCount) drives the
 * "showing N of M" note.
 */
export default function ResultTable({
  result,
  fullRowCount,
}: {
  result: StatementResult;
  fullRowCount?: number;
}) {
  const fields = result.schema?.fields ?? [];
  const cols = fields.length
    ? fields.map((f) => f.name)
    : Object.keys(result.rows[0] ?? {});
  const typeOf = (c: string) => fields.find((f) => f.name === c)?.type ?? "";
  const total = fullRowCount ?? result.rowCount;

  return (
    <div>
      <div className="mb-2 text-xs text-muted">
        {total > result.rows.length
          ? `Showing ${result.rows.length.toLocaleString()} of ${total.toLocaleString()} rows.`
          : `${result.rows.length.toLocaleString()} row${
              result.rows.length === 1 ? "" : "s"
            }`}
      </div>
      <table className="w-full border-collapse text-sm">
        <thead>
          <tr>
            {cols.map((c) => (
              <th
                key={c}
                className="border-b border-border px-2.5 py-2 text-left font-medium text-muted"
              >
                {c} <span className="font-normal">{typeOf(c)}</span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {result.rows.map((r, i) => (
            <tr key={i}>
              {cols.map((c) => {
                const t = cellText(r[c]);
                return (
                  <td
                    key={c}
                    className={`border-b border-border px-2.5 py-2 ${
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
  );
}
