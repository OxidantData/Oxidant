import { useMemo } from "react";
import ReactEChartsCore from "echarts-for-react/lib/core";
import type { StatementResult } from "@/lib/api";
import type { WidgetOptions } from "@/lib/dashboards";
import { buildChartOption, type ChartWidgetType } from "@/lib/chartOption";
import { echarts, registerOxidantTheme, OXIDANT_ECHARTS_THEME } from "@/lib/echarts";
import { useThemeMode } from "@/lib/theme";
import WidgetNotice from "@/components/dashboard/WidgetNotice";

interface WidgetChartProps {
  type: ChartWidgetType;
  result: StatementResult;
  options?: WidgetOptions;
}

/**
 * One ECharts canvas, themed from the brand tokens.
 *
 * ECharts resolves its theme at `init`, so a light/dark switch cannot be applied to a live
 * instance — the `key` below remounts the chart when the mode changes, after re-registering
 * the theme against the new token values.
 */
export default function WidgetChart({ type, result, options = {} }: WidgetChartProps) {
  const mode = useThemeMode();
  const { option, notice } = useMemo(
    () => buildChartOption(type, result, options),
    [type, result, options]
  );
  const theme = useMemo(() => {
    // `mode` is the dependency that matters: re-read the CSS variables after a switch.
    void mode;
    return registerOxidantTheme();
  }, [mode]);

  if (!option) return <WidgetNotice>{notice ?? "Nothing to plot."}</WidgetNotice>;

  return (
    <div className="flex h-full flex-col">
      <div className="min-h-0 flex-1">
        <ReactEChartsCore
          key={`${type}-${mode}`}
          echarts={echarts}
          option={option}
          theme={theme ?? OXIDANT_ECHARTS_THEME}
          notMerge
          lazyUpdate
          style={{ height: "100%", width: "100%" }}
          opts={{ renderer: "canvas" }}
        />
      </div>
      {notice && (
        <p className="shrink-0 pt-1 text-xs text-muted" title={notice}>
          {notice}
        </p>
      )}
    </div>
  );
}
