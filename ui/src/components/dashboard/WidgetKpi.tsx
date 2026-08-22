import { useMemo } from "react";
import type { StatementResult } from "@/lib/api";
import type { WidgetOptions } from "@/lib/dashboards";
import { toKpi } from "@/lib/widgetData";
import WidgetNotice from "@/components/dashboard/WidgetNotice";

interface WidgetKpiProps {
  result: StatementResult;
  options?: WidgetOptions;
}

/**
 * A single number, as large as the card allows.
 *
 * Monochrome by design: a KPI that turns green when it goes up is a judgement the engine has
 * no basis for making, and green already means "succeeded" everywhere else in this UI.
 */
export default function WidgetKpi({ result, options = {} }: WidgetKpiProps) {
  const kpi = useMemo(
    () => toKpi(result, { unit: options.unit, decimals: options.decimals }),
    [result, options.unit, options.decimals]
  );

  if (kpi.notice && kpi.value == null && kpi.text === "—") {
    return <WidgetNotice>{kpi.notice}</WidgetNotice>;
  }

  return (
    <div className="flex h-full flex-col items-center justify-center gap-1 p-2">
      <span
        className="max-w-full truncate text-4xl font-semibold tracking-display text-body"
        title={kpi.text}
      >
        {kpi.text}
      </span>
      {kpi.column && (
        <span className="oxidant-eyebrow max-w-full truncate">{kpi.column}</span>
      )}
    </div>
  );
}
