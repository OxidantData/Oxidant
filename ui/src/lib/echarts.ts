/**
 * ECharts, tree-shaken, wearing the Oxidant brand.
 *
 * Only what dashboards v1 draws is registered: four series types and four components. The
 * barrel `import * as echarts from "echarts"` would pull in all 50+ chart types — including
 * every widget the OSS engine deliberately does not ship — so it is never imported here.
 * Adding a widget type means adding its chart to the `echarts.use` call below and nowhere
 * else.
 *
 * The theme is built at runtime from the CSS custom properties in `styles/theme.css`, so the
 * charts follow the header's light/dark toggle without a second copy of the palette. The
 * fallbacks below are the dark values verbatim — they are what a test environment (no
 * computed styles) and a pre-paint first render get.
 */
import * as echarts from "echarts/core";
import { BarChart, LineChart, PieChart, ScatterChart } from "echarts/charts";
import {
  GridComponent,
  LegendComponent,
  TitleComponent,
  TooltipComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";

echarts.use([
  BarChart,
  // `area` is a line series with an areaStyle — no separate chart to register.
  LineChart,
  PieChart,
  ScatterChart,
  GridComponent,
  TooltipComponent,
  LegendComponent,
  TitleComponent,
  CanvasRenderer,
]);

export { echarts };

/** The registered theme's name, passed to `<ReactECharts theme=...>`. */
export const OXIDANT_ECHARTS_THEME = "oxidant";

export interface BrandTokens {
  text: string;
  textSecondary: string;
  textMuted: string;
  border: string;
  borderStrong: string;
  surface: string;
  /** `--oxidant-chart-1..5`: monochrome, brightest first. */
  ramp: string[];
  success: string;
  warning: string;
  danger: string;
  fontUi: string;
}

/** Dark-theme values, kept byte-identical to `styles/theme.css`. */
const DARK_FALLBACK: BrandTokens = {
  text: "#f5f5f5",
  textSecondary: "#a3a3a3",
  textMuted: "#737373",
  border: "rgba(255, 255, 255, 0.09)",
  borderStrong: "rgba(255, 255, 255, 0.16)",
  surface: "#171719",
  ramp: ["#fafafa", "#a3a3a3", "#737373", "#525252", "#3f3f46"],
  success: "#22c55e",
  warning: "#eab308",
  danger: "#ef4444",
  fontUi: "Geist, Inter, ui-sans-serif, system-ui, sans-serif",
};

function readToken(style: CSSStyleDeclaration, name: string, fallback: string): string {
  const value = style.getPropertyValue(name).trim();
  return value || fallback;
}

/** Current values of the brand tokens. Falls back to the dark palette outside a browser. */
export function readBrandTokens(): BrandTokens {
  if (typeof window === "undefined" || typeof getComputedStyle !== "function") {
    return DARK_FALLBACK;
  }
  const style = getComputedStyle(document.documentElement);
  return {
    text: readToken(style, "--oxidant-text", DARK_FALLBACK.text),
    textSecondary: readToken(style, "--oxidant-text-secondary", DARK_FALLBACK.textSecondary),
    textMuted: readToken(style, "--oxidant-text-muted", DARK_FALLBACK.textMuted),
    border: readToken(style, "--oxidant-border", DARK_FALLBACK.border),
    borderStrong: readToken(style, "--oxidant-border-strong", DARK_FALLBACK.borderStrong),
    surface: readToken(style, "--oxidant-surface", DARK_FALLBACK.surface),
    ramp: DARK_FALLBACK.ramp.map((fallback, i) =>
      readToken(style, `--oxidant-chart-${i + 1}`, fallback)
    ),
    success: readToken(style, "--oxidant-success", DARK_FALLBACK.success),
    warning: readToken(style, "--oxidant-warning", DARK_FALLBACK.warning),
    danger: readToken(style, "--oxidant-danger", DARK_FALLBACK.danger),
    fontUi: readToken(style, "--oxidant-font-ui", DARK_FALLBACK.fontUi),
  };
}

/**
 * The brand, expressed as an ECharts theme.
 *
 * The rules are the ones the rest of the UI follows: the series ramp is monochrome (no
 * decorative hue anywhere), grid lines are the hairline border token rather than a grey of
 * ECharts' choosing, tooltips sit on the surface colour, and the axis line is dropped in
 * favour of a split line — the same "contrast, not chrome" the tables use.
 */
export function oxidantEChartsTheme(tokens: BrandTokens): Record<string, unknown> {
  const axis = {
    axisLine: { show: false },
    axisTick: { show: false },
    axisLabel: { color: tokens.textMuted, fontSize: 11 },
    splitLine: { lineStyle: { color: tokens.border, width: 1 } },
    nameTextStyle: { color: tokens.textMuted, fontSize: 11 },
  };
  return {
    color: tokens.ramp,
    backgroundColor: "transparent",
    textStyle: { fontFamily: tokens.fontUi, color: tokens.textSecondary },
    title: {
      textStyle: { color: tokens.text, fontWeight: 500 },
      subtextStyle: { color: tokens.textMuted },
    },
    grid: { left: 8, right: 12, top: 28, bottom: 8, containLabel: true },
    // Category axes get no split lines: the bars already carry the horizontal rhythm.
    categoryAxis: { ...axis, splitLine: { show: false } },
    valueAxis: axis,
    logAxis: axis,
    timeAxis: axis,
    legend: {
      textStyle: { color: tokens.textSecondary, fontSize: 11 },
      inactiveColor: tokens.textMuted,
      icon: "roundRect",
      itemWidth: 10,
      itemHeight: 10,
    },
    tooltip: {
      backgroundColor: tokens.surface,
      borderColor: tokens.borderStrong,
      borderWidth: 1,
      textStyle: { color: tokens.text, fontSize: 12 },
      axisPointer: {
        lineStyle: { color: tokens.borderStrong },
        crossStyle: { color: tokens.borderStrong },
        shadowStyle: { color: tokens.border },
      },
    },
    bar: { itemStyle: { borderRadius: [3, 3, 0, 0] } },
    line: { symbol: "circle", symbolSize: 5, lineStyle: { width: 2 } },
    pie: {
      itemStyle: { borderColor: tokens.surface, borderWidth: 2 },
      label: { color: tokens.textSecondary },
      labelLine: { lineStyle: { color: tokens.border } },
    },
    scatter: { symbolSize: 8 },
  };
}

/**
 * (Re-)register the theme against the tokens in effect right now. ECharts bakes the theme in
 * at `init`, so charts have to be remounted after a theme switch — `useBrandThemeKey` below
 * is what makes that happen.
 */
export function registerOxidantTheme(): string {
  echarts.registerTheme(OXIDANT_ECHARTS_THEME, oxidantEChartsTheme(readBrandTokens()));
  return OXIDANT_ECHARTS_THEME;
}

registerOxidantTheme();
