/**
 * Chart options, and proof that ECharts can actually draw them.
 *
 * The render smoke test uses ECharts' server-side rendering mode
 * (`init(null, theme, { ssr: true, renderer: "svg" })`), which needs no DOM and no canvas —
 * so the assertion is that *real* ECharts, wearing the *real* Oxidant theme, turns each
 * widget's option into marks. A mocked chart component could not tell us that.
 */
import { describe, expect, it } from "vitest";
import * as core from "echarts/core";
import { SVGRenderer } from "echarts/renderers";
import type { StatementResult } from "@/lib/api";
import { buildChartOption, CHART_WIDGET_TYPES, isChartWidget } from "@/lib/chartOption";
import { OXIDANT_ECHARTS_THEME } from "@/lib/echarts";
import { WIDGET_TYPES } from "@/lib/dashboards";

// The app registers only the canvas renderer; SSR needs the SVG one.
core.use([SVGRenderer]);

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

const SALES = result(
  [
    ["region", "Utf8"],
    ["revenue", "Float64"],
    ["orders", "Int64"],
  ],
  [
    { region: "west", revenue: 120.5, orders: 12 },
    { region: "east", revenue: 80, orders: null },
    { region: "north", revenue: 45.25, orders: 4 },
  ]
);

/** Render an option through real ECharts and hand back the SVG it produced. */
function renderToSvg(option: unknown): string {
  const chart = core.init(null, OXIDANT_ECHARTS_THEME, {
    renderer: "svg",
    ssr: true,
    width: 600,
    height: 360,
  });
  chart.setOption(option as never);
  const svg = chart.renderToSVGString();
  chart.dispose();
  return svg;
}

describe("render smoke: every chart widget draws", () => {
  it.each(CHART_WIDGET_TYPES)("%s produces marks", (type) => {
    const { option } = buildChartOption(type, SALES);
    expect(option).not.toBeNull();
    const svg = renderToSvg(option);
    expect(svg).toContain("<svg");
    // Something was actually drawn — an empty chart is a `<svg>` with no geometry in it.
    expect(svg).toMatch(/<(path|rect|circle|polyline)\b/);
    expect(svg.length).toBeGreaterThan(500);
  });

  it("labels the categories the query returned", () => {
    const { option } = buildChartOption("bar", SALES);
    const svg = renderToSvg(option);
    for (const region of ["west", "east", "north"]) {
      expect(svg).toContain(region);
    }
  });
});

describe("bar", () => {
  it("puts the labels on the category axis and one series per numeric column", () => {
    const { option } = buildChartOption("bar", SALES);
    const o = option as Record<string, never>;
    expect((o.xAxis as { data: string[] }).data).toEqual(["west", "east", "north"]);
    const series = o.series as unknown as { name: string; type: string; data: unknown[] }[];
    expect(series.map((s) => s.name)).toEqual(["revenue", "orders"]);
    expect(series.every((s) => s.type === "bar")).toBe(true);
    // The NULL rides through to ECharts as a gap.
    expect(series[1].data).toEqual([12, null, 4]);
  });

  it("stacks when asked, and swaps the axes when horizontal", () => {
    const stacked = buildChartOption("bar", SALES, { stacked: true }).option as Record<
      string,
      never
    >;
    const series = stacked.series as unknown as { stack?: string }[];
    expect(series.every((s) => s.stack === "total")).toBe(true);

    const horizontal = buildChartOption("bar", SALES, { horizontal: true })
      .option as Record<string, never>;
    expect((horizontal.xAxis as { type: string }).type).toBe("value");
    expect((horizontal.yAxis as { type: string }).type).toBe("category");
  });
});

describe("line and area", () => {
  it("are the same series type, with area adding a fill and never joining across NULLs", () => {
    const line = buildChartOption("line", SALES).option as Record<string, never>;
    const area = buildChartOption("area", SALES).option as Record<string, never>;
    const lineSeries = line.series as unknown as {
      type: string;
      areaStyle?: unknown;
      connectNulls: boolean;
    }[];
    const areaSeries = area.series as unknown as { type: string; areaStyle?: unknown }[];
    expect(lineSeries[0].type).toBe("line");
    expect(lineSeries[0].areaStyle).toBeUndefined();
    expect(lineSeries[0].connectNulls).toBe(false);
    expect(areaSeries[0].type).toBe("line");
    expect(areaSeries[0].areaStyle).toBeDefined();
  });
});

describe("pie", () => {
  it("draws the first numeric column only, and says which one", () => {
    const built = buildChartOption("pie", SALES);
    const series = (built.option as Record<string, never>).series as unknown as {
      data: { name: string; value: number }[];
    }[];
    expect(series[0].data).toEqual([
      { name: "west", value: 120.5 },
      { name: "east", value: 80 },
      { name: "north", value: 45.25 },
    ]);
    expect(built.notice).toContain("revenue");
  });

  it("drops NULL slices instead of drawing an empty wedge", () => {
    const withNull = result(
      [
        ["region", "Utf8"],
        ["revenue", "Float64"],
      ],
      [
        { region: "west", revenue: 10 },
        { region: "east", revenue: null },
      ]
    );
    const series = (buildChartOption("pie", withNull).option as Record<string, never>)
      .series as unknown as { data: unknown[] }[];
    expect(series[0].data).toEqual([{ name: "west", value: 10 }]);
  });
});

describe("scatter", () => {
  it("plots [x, y] pairs against a value axis named after the first column", () => {
    const xy = result(
      [
        ["latency_ms", "Float64"],
        ["rows", "Int64"],
      ],
      [
        { latency_ms: 1.5, rows: 100 },
        { latency_ms: 2.5, rows: 250 },
      ]
    );
    const option = buildChartOption("scatter", xy).option as Record<string, never>;
    expect((option.xAxis as { name: string; type: string }).name).toBe("latency_ms");
    expect((option.xAxis as { type: string }).type).toBe("value");
    const series = option.series as unknown as { data: number[][] }[];
    expect(series[0].data).toEqual([
      [1.5, 100],
      [2.5, 250],
    ]);
  });
});

describe("nothing to draw", () => {
  it("returns no option and an explanation instead of an empty chart", () => {
    const textOnly = result(
      [
        ["a", "Utf8"],
        ["b", "Utf8"],
      ],
      [{ a: "x", b: "y" }]
    );
    for (const type of CHART_WIDGET_TYPES) {
      const built = buildChartOption(type, textOnly);
      expect(built.option, `${type} should not build an option`).toBeNull();
      expect(built.notice, `${type} should explain itself`).toBeTruthy();
    }
  });

  it("says so for an empty result set", () => {
    const empty = result(
      [
        ["region", "Utf8"],
        ["n", "Int64"],
      ],
      []
    );
    expect(buildChartOption("bar", empty).notice).toContain("no rows");
  });
});

describe("the chart/component split", () => {
  it("routes exactly the five plot types to ECharts", () => {
    expect(WIDGET_TYPES.filter(isChartWidget)).toEqual(CHART_WIDGET_TYPES);
    expect(isChartWidget("table")).toBe(false);
    expect(isChartWidget("kpi")).toBe(false);
  });
});
