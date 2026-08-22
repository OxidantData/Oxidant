import type { StatementResult } from "@/lib/api";
import type { WidgetOptions, WidgetType } from "@/lib/dashboards";
import { isChartWidget } from "@/lib/chartOption";
import WidgetChart from "@/components/dashboard/WidgetChart";
import WidgetKpi from "@/components/dashboard/WidgetKpi";
import WidgetTable from "@/components/dashboard/WidgetTable";

interface WidgetBodyProps {
  type: WidgetType;
  result: StatementResult;
  options?: WidgetOptions;
}

/**
 * The one place a widget type turns into a renderer. Charts go to ECharts; `table` and `kpi`
 * are plain React, because neither is a plot and neither should drag a canvas into the page.
 */
export default function WidgetBody({ type, result, options }: WidgetBodyProps) {
  if (isChartWidget(type)) {
    return <WidgetChart type={type} result={result} options={options} />;
  }
  if (type === "kpi") return <WidgetKpi result={result} options={options} />;
  return <WidgetTable result={result} options={options} />;
}
