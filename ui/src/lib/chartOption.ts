/**
 * ECharts options for the five chart widgets, built from the mapping in `lib/widgetData.ts`.
 *
 * Colour, fonts, grid lines and tooltips all come from the registered `oxidant` theme
 * (`lib/echarts.ts`) — nothing here names a colour except where a value must be derived from
 * the series colour itself (the area gradient). Keeping palette decisions in exactly one place
 * is what makes the light/dark toggle a re-register rather than an audit.
 */
import type { EChartsOption } from "echarts";
import type { StatementResult } from "@/lib/api";
import type { WidgetOptions, WidgetType } from "@/lib/dashboards";
import { toChartData, toScatterData, type ChartData } from "@/lib/widgetData";

/** Types this module can draw. `table` and `kpi` are React components, not ECharts. */
export type ChartWidgetType = Extract<
  WidgetType,
  "bar" | "line" | "area" | "pie" | "scatter"
>;

export const CHART_WIDGET_TYPES: ChartWidgetType[] = [
  "bar",
  "line",
  "area",
  "pie",
  "scatter",
];

export function isChartWidget(type: WidgetType): type is ChartWidgetType {
  return (CHART_WIDGET_TYPES as WidgetType[]).includes(type);
}

export interface BuiltOption {
  option: EChartsOption | null;
  /** Set when the result cannot be drawn, or was drawn with a caveat worth showing. */
  notice: string | null;
}

/** Legend defaults to on once there is more than one series; `options.legend` overrides. */
function wantsLegend(data: ChartData, options: WidgetOptions): boolean {
  return options.legend ?? data.series.length > 1;
}

function legendBlock(show: boolean) {
  return show
    ? { show: true, type: "scroll" as const, top: 0, right: 0, left: 0 }
    : { show: false };
}

/** Shared cartesian frame for bar / line / area. */
function cartesian(
  data: ChartData,
  options: WidgetOptions,
  series: Record<string, unknown>[]
): EChartsOption {
  const legend = wantsLegend(data, options);
  const category = { type: "category" as const, data: data.labels };
  const value = { type: "value" as const };
  const horizontal = options.horizontal === true;
  return {
    legend: legendBlock(legend),
    grid: { top: legend ? 32 : 12, left: 8, right: 12, bottom: 4, containLabel: true },
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    xAxis: horizontal ? { ...value } : { ...category, name: data.labelColumn ?? "" },
    yAxis: horizontal ? { ...category, name: data.labelColumn ?? "" } : { ...value },
    series: series as EChartsOption["series"],
  };
}

function barOption(data: ChartData, options: WidgetOptions): EChartsOption {
  return cartesian(
    data,
    options,
    data.series.map((s) => ({
      name: s.name,
      type: "bar",
      data: s.data,
      stack: options.stacked ? "total" : undefined,
      // A stack reads as one bar, so only the top segment should be rounded; letting every
      // segment round would draw seams through the middle of the bar.
      itemStyle: options.stacked ? { borderRadius: 0 } : undefined,
    }))
  );
}

function lineOption(
  data: ChartData,
  options: WidgetOptions,
  area: boolean
): EChartsOption {
  return cartesian(
    data,
    options,
    data.series.map((s) => ({
      name: s.name,
      type: "line",
      data: s.data,
      smooth: options.smooth === true,
      // `connectNulls: false` is the point of the NULL convention: a gap stays a gap.
      connectNulls: false,
      showSymbol: data.labels.length <= 40,
      stack: options.stacked ? "total" : undefined,
      areaStyle: area ? { opacity: options.stacked ? 0.9 : 0.18 } : undefined,
    }))
  );
}

/** A pie has one dimension, so only the first numeric column is used. */
function pieOption(data: ChartData, options: WidgetOptions): BuiltOption {
  const series = data.series[0];
  if (!series) return { option: null, notice: data.notice };
  // NULL is a gap everywhere else; a pie has no way to draw one, so the slice is dropped.
  const slices = data.labels
    .map((name, i) => ({ name, value: series.data[i] }))
    .filter((s): s is { name: string; value: number } => s.value != null);
  const legend = options.legend ?? data.labels.length <= 12;
  return {
    option: {
      legend: { ...legendBlock(legend), type: "scroll", orient: "horizontal", top: 0 },
      tooltip: { trigger: "item", formatter: "{b}: {c} ({d}%)" },
      series: [
        {
          name: series.name,
          type: "pie",
          // A donut: the hole is where the total goes in the premium build, and a ring reads
          // better than a disc against a near-black card.
          radius: ["45%", "72%"],
          center: ["50%", legend ? "58%" : "50%"],
          avoidLabelOverlap: true,
          label: { show: data.labels.length <= 8, formatter: "{b}" },
          data: slices,
        },
      ],
    },
    notice:
      data.series.length > 1
        ? `Showing \`${series.name}\` — a pie chart draws one numeric column.`
        : data.notice,
  };
}

function scatterOption(
  result: StatementResult,
  options: WidgetOptions
): BuiltOption {
  const data = toScatterData(result);
  if (!data.series.length) return { option: null, notice: data.notice };
  const legend = options.legend ?? data.series.length > 1;
  return {
    option: {
      legend: legendBlock(legend),
      grid: { top: legend ? 32 : 12, left: 8, right: 12, bottom: 4, containLabel: true },
      tooltip: { trigger: "item" },
      xAxis: { type: "value", name: data.xName, scale: true },
      yAxis: { type: "value", scale: true },
      series: data.series.map((s) => ({
        name: s.name,
        type: "scatter",
        data: s.points,
      })),
    },
    notice: data.notice,
  };
}

/**
 * The one entry point: a statement result plus a widget spec in, an ECharts option out.
 * `option: null` means "there is nothing to draw" and `notice` says why.
 */
export function buildChartOption(
  type: ChartWidgetType,
  result: StatementResult,
  options: WidgetOptions = {}
): BuiltOption {
  if (type === "scatter") return scatterOption(result, options);

  const data = toChartData(result);
  if (!data.series.length) return { option: null, notice: data.notice };

  switch (type) {
    case "bar":
      return { option: barOption(data, options), notice: data.notice };
    case "line":
      return { option: lineOption(data, options, false), notice: data.notice };
    case "area":
      return { option: lineOption(data, options, true), notice: data.notice };
    case "pie":
      return pieOption(data, options);
  }
}
