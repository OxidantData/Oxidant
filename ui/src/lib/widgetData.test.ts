import { describe, expect, it } from "vitest";
import type { StatementResult } from "@/lib/api";
import {
  NULL_LABEL,
  columnNames,
  formatKpiNumber,
  isNumericColumn,
  toChartData,
  toKpi,
  toNumber,
  toScatterData,
  toTableColumns,
} from "@/lib/widgetData";

/** Build a statement result the way `/api/v1/statements/{id}/result` returns one. */
function result(
  fields: [string, string][],
  rows: Record<string, unknown>[]
): StatementResult {
  return {
    schema: { fields: fields.map(([name, type]) => ({ name, type })) },
    rows,
    rowCount: rows.length,
    truncated: false,
  };
}

describe("the mapping convention", () => {
  it("takes the first column as the label and every numeric column after it as a series", () => {
    const r = result(
      [
        ["region", "Utf8"],
        ["revenue", "Float64"],
        ["orders", "Int64"],
      ],
      [
        { region: "west", revenue: 10.5, orders: 3 },
        { region: "east", revenue: 20.25, orders: 7 },
      ]
    );
    const data = toChartData(r);
    expect(data.labelColumn).toBe("region");
    expect(data.labels).toEqual(["west", "east"]);
    expect(data.series).toEqual([
      { name: "revenue", data: [10.5, 20.25] },
      { name: "orders", data: [3, 7] },
    ]);
    expect(data.notice).toBeNull();
  });

  it("ignores non-numeric columns after the first, and says so when none are left", () => {
    const withText = result(
      [
        ["region", "Utf8"],
        ["revenue", "Float64"],
        ["note", "Utf8"],
      ],
      [{ region: "west", revenue: 1, note: "hi" }]
    );
    expect(toChartData(withText).series.map((s) => s.name)).toEqual(["revenue"]);

    const noneNumeric = result(
      [
        ["region", "Utf8"],
        ["note", "Utf8"],
      ],
      [{ region: "west", note: "hi" }]
    );
    const data = toChartData(noneNumeric);
    expect(data.series).toEqual([]);
    expect(data.notice).toContain("No numeric column");
  });

  it("treats Arrow decimals and int64-as-string as numeric", () => {
    const r = result(
      [
        ["day", "Date32"],
        ["amount", "Decimal128(10, 2)"],
      ],
      [{ day: "2026-08-22", amount: "1234.56" }]
    );
    expect(isNumericColumn(r, "amount")).toBe(true);
    expect(isNumericColumn(r, "day")).toBe(false);
    expect(toChartData(r).series[0].data).toEqual([1234.56]);
  });

  it("falls back to sniffing values when the schema carries no type", () => {
    const r: StatementResult = {
      schema: { fields: [] },
      rows: [
        { label: "a", n: 1 },
        { label: "b", n: 2 },
      ],
      rowCount: 2,
      truncated: false,
    };
    expect(columnNames(r)).toEqual(["label", "n"]);
    expect(isNumericColumn(r, "n")).toBe(true);
    expect(isNumericColumn(r, "label")).toBe(false);
  });

  it("plots a lone numeric column against the row ordinal", () => {
    const r = result([["total", "Int64"]], [{ total: 5 }, { total: 8 }]);
    const data = toChartData(r);
    expect(data.labels).toEqual(["1", "2"]);
    expect(data.series).toEqual([{ name: "total", data: [5, 8] }]);
  });
});

describe("NULLs", () => {
  it("keeps a NULL value as a gap rather than a zero", () => {
    const r = result(
      [
        ["day", "Utf8"],
        ["hits", "Int64"],
      ],
      [
        { day: "mon", hits: 4 },
        { day: "tue", hits: null },
        { day: "wed", hits: 9 },
      ]
    );
    // `null`, not `0`: zero-filling would invent a day with no traffic.
    expect(toChartData(r).series[0].data).toEqual([4, null, 9]);
  });

  it("renders a NULL label as ∅", () => {
    const r = result(
      [
        ["region", "Utf8"],
        ["n", "Int64"],
      ],
      [{ region: null, n: 1 }]
    );
    expect(toChartData(r).labels).toEqual([NULL_LABEL]);
  });

  it("keeps an all-NULL numeric column as a series", () => {
    const r = result(
      [
        ["day", "Utf8"],
        ["hits", "Int64"],
      ],
      [
        { day: "mon", hits: null },
        { day: "tue", hits: null },
      ]
    );
    // The schema says Int64; an empty numeric column is not a string column.
    expect(toChartData(r).series).toEqual([{ name: "hits", data: [null, null] }]);
  });

  it("drops NULL points from a scatter rather than plotting them at the origin", () => {
    const r = result(
      [
        ["x", "Float64"],
        ["y", "Float64"],
      ],
      [
        { x: 1, y: 2 },
        { x: 2, y: null },
        { x: null, y: 4 },
        { x: 3, y: 6 },
      ]
    );
    const data = toScatterData(r);
    expect(data.xName).toBe("x");
    expect(data.series[0].points).toEqual([
      [1, 2],
      [3, 6],
    ]);
  });

  it("plots a scatter against row number when the first column is not numeric", () => {
    const r = result(
      [
        ["name", "Utf8"],
        ["score", "Int64"],
      ],
      [
        { name: "a", score: 10 },
        { name: "b", score: 20 },
      ]
    );
    const data = toScatterData(r);
    expect(data.xName).toBe("row");
    expect(data.series[0].points).toEqual([
      [1, 10],
      [2, 20],
    ]);
    expect(data.notice).toContain("not numeric");
  });
});

describe("toNumber", () => {
  it("accepts numbers and numeric strings, and refuses everything else", () => {
    expect(toNumber(3)).toBe(3);
    expect(toNumber("3.5")).toBe(3.5);
    expect(toNumber(" 42 ")).toBe(42);
    expect(toNumber(null)).toBeNull();
    expect(toNumber("")).toBeNull();
    expect(toNumber("west")).toBeNull();
    // A boolean is a category, not a magnitude.
    expect(toNumber(true)).toBeNull();
    expect(toNumber(NaN)).toBeNull();
    expect(toNumber(Infinity)).toBeNull();
  });
});

describe("the KPI counter", () => {
  it("takes the single cell of a single-column, single-row result", () => {
    const r = result([["total"]].map(([n]) => [n, "Int64"] as [string, string]), [
      { total: 1234 },
    ]);
    const kpi = toKpi(r);
    expect(kpi.value).toBe(1234);
    expect(kpi.column).toBe("total");
    expect(kpi.text).toBe("1,234");
  });

  it("uses a non-numeric single cell as-is", () => {
    const r = result([["state", "Utf8"]], [{ state: "healthy" }]);
    const kpi = toKpi(r);
    expect(kpi.text).toBe("healthy");
    expect(kpi.value).toBeNull();
  });

  it("prefers the first numeric column when the row has several", () => {
    const r = result(
      [
        ["label", "Utf8"],
        ["count", "Int64"],
      ],
      [{ label: "orders", count: 7 }]
    );
    expect(toKpi(r).column).toBe("count");
    expect(toKpi(r).value).toBe(7);
  });

  it("says so rather than showing 0 when there are no rows or the value is NULL", () => {
    expect(toKpi(result([["n", "Int64"]], [])).text).toBe("—");
    expect(toKpi(result([["n", "Int64"]], [])).notice).toContain("no rows");
    const nullValue = toKpi(result([["n", "Int64"]], [{ n: null }]));
    expect(nullValue.text).toBe("—");
    expect(nullValue.notice).toContain("NULL");
  });

  it("appends the configured unit and honours a fixed decimal count", () => {
    const r = result([["pct", "Float64"]], [{ pct: 12.3456 }]);
    expect(toKpi(r, { unit: "%" }).text).toBe("12.35%");
    expect(toKpi(r, { decimals: 1 }).text).toBe("12.3");
  });

  it("formats large and tiny magnitudes readably", () => {
    expect(formatKpiNumber(1234)).toBe("1,234");
    expect(formatKpiNumber(12_300_000)).toBe("12.3M");
    expect(formatKpiNumber(0.0001)).toBe("1.00e-4");
    expect(formatKpiNumber(0)).toBe("0");
  });
});

describe("the table widget's columns", () => {
  it("keeps every column in result order and marks which are numeric", () => {
    const r = result(
      [
        ["region", "Utf8"],
        ["revenue", "Float64"],
        ["ok", "Boolean"],
      ],
      [{ region: "west", revenue: 1, ok: true }]
    );
    expect(toTableColumns(r)).toEqual([
      { name: "region", type: "Utf8", numeric: false },
      { name: "revenue", type: "Float64", numeric: true },
      { name: "ok", type: "Boolean", numeric: false },
    ]);
  });
});
