/**
 * Turning a SQL result into something a widget can draw.
 *
 * ## The convention (one rule, applied to every widget type)
 *
 * > **The first column labels the point. Every numeric column after it is a series.**
 *
 * - Column 1 is the category / x value: the bar's label, the point on the line, the pie
 *   slice's name. For `scatter` it is parsed as a number (see below).
 * - Every column after it whose *type* is numeric becomes one series, named after the column.
 *   Non-numeric trailing columns are ignored by charts (`table` shows everything).
 * - `pie` uses only the **first** numeric column — a pie has one dimension.
 * - `kpi` takes the first numeric cell of the first row. A single-column, single-row result
 *   is used whatever its type, so `SELECT 'green' AS status` is a legal KPI.
 * - `scatter` needs a numeric x. When column 1 is not numeric the row's ordinal position is
 *   used instead, so a stringly-typed first column still plots.
 *
 * ## NULLs
 *
 * - A NULL in a value column is a **gap**, not a zero: `null` reaches ECharts, which breaks
 *   the line and draws no bar. Summing or zero-filling would invent data.
 * - A NULL in the label column renders as `∅`.
 * - A column that is *entirely* NULL still counts as a series when the schema says it is
 *   numeric — an all-NULL Int64 is a numeric column with no data, not a string column.
 *
 * Whether a column is numeric is decided from the statement API's Arrow type name
 * (`Int32`, `Float64`, `Decimal128(10, 2)`, …) and only falls back to sniffing values when
 * the schema is missing or names a type this list does not know.
 */
import type { StatementResult } from "@/lib/api";

/** The label shown where the first column is NULL. */
export const NULL_LABEL = "∅";

/** Arrow type names (`DataType::to_string()`) that are numbers we can plot. */
const NUMERIC_ARROW_TYPE = /^(Int|UInt|Float|Decimal)/i;
/** Arrow type names that are definitively not plottable, so no value sniffing is needed. */
const NON_NUMERIC_ARROW_TYPE =
  /^(Utf8|LargeUtf8|Boolean|Binary|LargeBinary|Date|Time|Timestamp|Duration|Interval|List|LargeList|Struct|Map|Union|Dictionary|Null)/i;

export interface ChartSeries {
  name: string;
  /** One entry per row, aligned with `labels`. `null` is a gap. */
  data: (number | null)[];
}

export interface ChartData {
  /** Formatted first-column values, one per row. */
  labels: string[];
  /** Name of the first column, or `null` when the result has no columns. */
  labelColumn: string | null;
  series: ChartSeries[];
  /** Why there is nothing to draw, when there is nothing to draw. */
  notice: string | null;
}

/** Column names in result order — from the schema, falling back to the first row's keys. */
export function columnNames(result: StatementResult): string[] {
  const fields = result.schema?.fields ?? [];
  if (fields.length) return fields.map((f) => f.name);
  return Object.keys(result.rows[0] ?? {});
}

/** Arrow type name for a column, or `""` when the schema does not carry one. */
export function columnType(result: StatementResult, name: string): string {
  return result.schema?.fields?.find((f) => f.name === name)?.type ?? "";
}

/**
 * `null` for "the schema does not say" — the caller then sniffs values. Kept separate from
 * [`isNumericColumn`] so the "all NULLs but typed Int64" case stays expressible.
 */
export function numericByType(type: string): boolean | null {
  if (!type) return null;
  if (NUMERIC_ARROW_TYPE.test(type)) return true;
  if (NON_NUMERIC_ARROW_TYPE.test(type)) return false;
  return null;
}

/**
 * Coerce one cell to a plottable number. Numeric strings count — Arrow serializes Decimal and
 * Int64 as JSON strings — but booleans do not: `true` is a category, not a magnitude.
 */
export function toNumber(value: unknown): number | null {
  if (value == null || typeof value === "boolean") return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return null;
    const n = Number(trimmed);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

/** Is this column a series? Schema first, value sniffing only as a fallback. */
export function isNumericColumn(result: StatementResult, name: string): boolean {
  const byType = numericByType(columnType(result, name));
  if (byType !== null) return byType;
  let sawValue = false;
  for (const row of result.rows) {
    const cell = row[name];
    if (cell == null) continue;
    if (toNumber(cell) === null) return false;
    sawValue = true;
  }
  return sawValue;
}

/** Format a first-column value as an axis/slice label. */
export function toLabel(value: unknown): string {
  if (value == null) return NULL_LABEL;
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

/**
 * Apply the convention. Widget-type specifics (pie taking one series, scatter needing pairs)
 * are layered on top of this by `buildChartOption`.
 */
export function toChartData(result: StatementResult): ChartData {
  const cols = columnNames(result);
  if (!cols.length) {
    return { labels: [], labelColumn: null, series: [], notice: "Query returned no columns." };
  }
  const labelColumn = cols[0];
  const labels = result.rows.map((row) => toLabel(row[labelColumn]));

  const valueColumns = cols.slice(1).filter((c) => isNumericColumn(result, c));
  // A single-column result has no value column of its own; plot the one column it has, using
  // the row's ordinal as the label. `SELECT count(*) FROM t` should still draw something.
  if (!valueColumns.length && cols.length === 1 && isNumericColumn(result, labelColumn)) {
    return {
      labels: result.rows.map((_, i) => String(i + 1)),
      labelColumn,
      series: [
        {
          name: labelColumn,
          data: result.rows.map((row) => toNumber(row[labelColumn])),
        },
      ],
      notice: null,
    };
  }

  const series: ChartSeries[] = valueColumns.map((name) => ({
    name,
    data: result.rows.map((row) => toNumber(row[name])),
  }));

  let notice: string | null = null;
  if (!result.rows.length) {
    notice = "Query returned no rows.";
  } else if (!series.length) {
    notice =
      cols.length === 1
        ? `Only one column (\`${labelColumn}\`) and it is not numeric — a chart needs a label column and at least one numeric column.`
        : `No numeric column after \`${labelColumn}\` — a chart needs a label column and at least one numeric column.`;
  }
  return { labels, labelColumn, series, notice };
}

export interface KpiValue {
  /** Formatted for display, `"—"` when the query returned nothing. */
  text: string;
  /** The underlying number, when the cell was one. */
  value: number | null;
  /** The column the value came from — shown under the number. */
  column: string | null;
  notice: string | null;
}

export interface KpiFormat {
  unit?: string;
  decimals?: number;
}

/** Compact enough to fit a small card: `1,234`, `12.3M`, `-0.45`. */
export function formatKpiNumber(value: number, format: KpiFormat = {}): string {
  const { decimals } = format;
  if (decimals != null) {
    return value.toLocaleString(undefined, {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    });
  }
  const magnitude = Math.abs(value);
  if (magnitude >= 1_000_000) {
    return value.toLocaleString(undefined, {
      notation: "compact",
      maximumFractionDigits: 1,
    });
  }
  if (magnitude !== 0 && magnitude < 0.01) return value.toExponential(2);
  return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

/**
 * First numeric cell of the first row. A one-column, one-row result is taken as-is even when
 * it is text, because that is the shape people write for a status counter.
 */
export function toKpi(result: StatementResult, format: KpiFormat = {}): KpiValue {
  const cols = columnNames(result);
  const row = result.rows[0];
  if (!cols.length || !row) {
    return { text: "—", value: null, column: null, notice: "Query returned no rows." };
  }
  const suffix = format.unit ?? "";
  const numericColumn = cols.find((c) => isNumericColumn(result, c));
  if (numericColumn) {
    const value = toNumber(row[numericColumn]);
    return {
      text: value == null ? "—" : formatKpiNumber(value, format) + suffix,
      value,
      column: numericColumn,
      notice: value == null ? "Value is NULL." : null,
    };
  }
  const text = toLabel(row[cols[0]]);
  return {
    text: text + suffix,
    value: null,
    column: cols[0],
    notice: null,
  };
}

/** `[x, y]` pairs for one scatter series, dropping rows with no x or no y. */
export interface ScatterSeries {
  name: string;
  points: [number, number][];
}

export interface ScatterData {
  /** Name of the x axis: the first column, or `"row"` when it had to fall back to ordinals. */
  xName: string;
  series: ScatterSeries[];
  notice: string | null;
}

export function toScatterData(result: StatementResult): ScatterData {
  const cols = columnNames(result);
  const base = toChartData(result);
  if (base.notice && !base.series.length) {
    return { xName: cols[0] ?? "row", series: [], notice: base.notice };
  }
  const labelColumn = cols[0];
  const xIsNumeric = isNumericColumn(result, labelColumn);
  const xs = result.rows.map((row, i) =>
    xIsNumeric ? toNumber(row[labelColumn]) : i + 1
  );
  const series = base.series.map((s) => ({
    name: s.name,
    points: s.data
      .map((y, i) => [xs[i], y] as [number | null, number | null])
      .filter((p): p is [number, number] => p[0] != null && p[1] != null),
  }));
  return {
    xName: xIsNumeric ? labelColumn : "row",
    series,
    notice: xIsNumeric
      ? null
      : `\`${labelColumn}\` is not numeric — plotting against row number.`,
  };
}

/** Table columns in result order, with the Arrow type for the header's subtitle. */
export function toTableColumns(
  result: StatementResult
): { name: string; type: string; numeric: boolean }[] {
  return columnNames(result).map((name) => ({
    name,
    type: columnType(result, name),
    numeric: isNumericColumn(result, name),
  }));
}
