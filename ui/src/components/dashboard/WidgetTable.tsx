import { useMemo, useState } from "react";
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
  type SortingState,
} from "@tanstack/react-table";
import type { StatementResult } from "@/lib/api";
import type { WidgetOptions } from "@/lib/dashboards";
import { toTableColumns } from "@/lib/widgetData";
import { WIDGET_ROW_LIMIT } from "@/lib/useWidgetQuery";
import WidgetNotice from "@/components/dashboard/WidgetNotice";

type Row = Record<string, unknown>;

interface WidgetTableProps {
  result: StatementResult;
  options?: WidgetOptions;
}

const DEFAULT_PAGE_SIZE = 25;

function cellText(value: unknown): string | null {
  if (value == null) return null;
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

/**
 * The table widget: every column the query returned, sortable, paginated.
 *
 * This is TanStack Table rather than the monitoring pages' `ResultTable` because a widget
 * needs sorting inside a small box, and because the pivot widget the platform build adds sits
 * on the same table core.
 */
export default function WidgetTable({ result, options = {} }: WidgetTableProps) {
  const [sorting, setSorting] = useState<SortingState>([]);
  const schema = useMemo(() => toTableColumns(result), [result]);

  const columns = useMemo(() => {
    const helper = createColumnHelper<Row>();
    return schema.map((col) =>
      helper.accessor((row) => row[col.name] ?? undefined, {
        id: col.name,
        header: col.name,
        meta: { type: col.type, numeric: col.numeric },
        // NULL sorts last in *both* directions — it is absence, not a smallest value. Only
        // `undefined` gets that treatment from TanStack (`sortUndefined` is applied outside
        // the asc/desc inversion), so the accessor above narrows NULL to it; the renderer
        // still prints "NULL" because `cellText` treats both the same.
        sortUndefined: "last",
      })
    );
  }, [schema]);

  const pageSize = Math.max(1, options.pageSize ?? DEFAULT_PAGE_SIZE);
  const table = useReactTable({
    data: result.rows as Row[],
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    initialState: { pagination: { pageSize } },
  });

  if (!schema.length) return <WidgetNotice>Query returned no columns.</WidgetNotice>;
  if (!result.rows.length) return <WidgetNotice>Query returned no rows.</WidgetNotice>;

  const pageCount = table.getPageCount();
  const truncated = result.truncated || result.rows.length >= WIDGET_ROW_LIMIT;

  return (
    <div className="flex h-full flex-col">
      <div className="min-h-0 flex-1 overflow-auto">
        <table className="w-full border-collapse text-sm">
          <thead className="sticky top-0 z-10 bg-surface">
            {table.getHeaderGroups().map((group) => (
              <tr key={group.id}>
                {group.headers.map((header) => {
                  const sort = header.column.getIsSorted();
                  const meta = header.column.columnDef.meta as
                    | { type?: string; numeric?: boolean }
                    | undefined;
                  return (
                    <th
                      key={header.id}
                      onClick={header.column.getToggleSortingHandler()}
                      title={meta?.type ? `${header.id} · ${meta.type}` : header.id}
                      className={`cursor-pointer select-none whitespace-nowrap border-b border-hairline px-2.5 py-1.5 font-medium text-body hover:text-body ${
                        meta?.numeric ? "text-right" : "text-left"
                      }`}
                    >
                      {flexRender(header.column.columnDef.header, header.getContext())}
                      <span className="ml-1 text-muted">
                        {sort === "asc" ? "↑" : sort === "desc" ? "↓" : ""}
                      </span>
                    </th>
                  );
                })}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.map((row) => (
              <tr key={row.id}>
                {row.getVisibleCells().map((cell) => {
                  const meta = cell.column.columnDef.meta as
                    | { numeric?: boolean }
                    | undefined;
                  const text = cellText(cell.getValue());
                  return (
                    <td
                      key={cell.id}
                      className={`whitespace-nowrap border-b border-hairline px-2.5 py-1.5 ${
                        meta?.numeric ? "text-right tabular-nums" : "text-left"
                      } ${text == null ? "text-muted" : ""}`}
                    >
                      {text ?? "NULL"}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="flex shrink-0 items-center justify-between gap-2 pt-1.5 text-xs text-muted">
        <span>
          {result.rows.length.toLocaleString()} row
          {result.rows.length === 1 ? "" : "s"}
          {truncated && (
            <span className="ml-2 text-warning">
              Capped at {WIDGET_ROW_LIMIT.toLocaleString()} — aggregate in SQL.
            </span>
          )}
        </span>
        {pageCount > 1 && (
          <span className="flex items-center gap-1.5">
            <button
              className="nb-btn"
              disabled={!table.getCanPreviousPage()}
              onClick={() => table.previousPage()}
            >
              Prev
            </button>
            <span>
              {table.getState().pagination.pageIndex + 1} / {pageCount}
            </span>
            <button
              className="nb-btn"
              disabled={!table.getCanNextPage()}
              onClick={() => table.nextPage()}
            >
              Next
            </button>
          </span>
        )}
      </div>
    </div>
  );
}
