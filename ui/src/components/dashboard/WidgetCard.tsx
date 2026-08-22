import type { WidgetSpec } from "@/lib/dashboards";
import { WIDGET_TYPE_LABELS } from "@/lib/dashboards";
import { useWidgetQuery } from "@/lib/useWidgetQuery";
import WidgetBody from "@/components/dashboard/WidgetBody";
import WidgetNotice from "@/components/dashboard/WidgetNotice";

interface WidgetCardProps {
  widget: WidgetSpec;
  /** Auto-refresh period for this dashboard, in ms. `false` = manual refresh only. */
  refetchIntervalMs?: number | false;
  /** Edit mode adds the drag handle and the Edit/Remove actions. */
  editing?: boolean;
  onEdit?: (widget: WidgetSpec) => void;
  onRemove?: (widget: WidgetSpec) => void;
}

/**
 * One card on the grid: a header that doubles as the drag handle, and a body that is whatever
 * the widget's SQL currently evaluates to.
 *
 * The header is `.dash-drag-handle` because dragging from anywhere would make it impossible to
 * select text in a table cell or click a legend entry; the grid is configured to start a drag
 * only from here.
 */
export default function WidgetCard({
  widget,
  refetchIntervalMs = false,
  editing = false,
  onEdit,
  onRemove,
}: WidgetCardProps) {
  const query = useWidgetQuery(widget.sql, { refetchIntervalMs });
  const title = widget.title || WIDGET_TYPE_LABELS[widget.type];

  return (
    <div className="flex h-full flex-col overflow-hidden rounded-oxidant border border-hairline bg-surface">
      <div
        className={`dash-drag-handle flex shrink-0 items-center gap-2 border-b border-hairline px-3 py-2 ${
          editing ? "cursor-move" : ""
        }`}
      >
        <span className="truncate text-sm font-medium text-body" title={title}>
          {title}
        </span>
        {query.isFetching && (
          <span className="shrink-0 text-xs text-warning" role="status">
            ···
          </span>
        )}
        <span className="ml-auto flex shrink-0 items-center gap-1">
          {editing ? (
            <>
              <button
                className="nb-btn dash-no-drag"
                onClick={() => onEdit?.(widget)}
                aria-label={`Edit ${title}`}
              >
                Edit
              </button>
              <button
                className="nb-btn dash-no-drag"
                onClick={() => onRemove?.(widget)}
                aria-label={`Remove ${title}`}
              >
                Remove
              </button>
            </>
          ) : (
            <button
              className="nb-btn dash-no-drag"
              onClick={() => query.refetch()}
              disabled={query.isFetching}
              aria-label={`Refresh ${title}`}
            >
              Refresh
            </button>
          )}
        </span>
      </div>

      <div className="min-h-0 flex-1 p-2">
        {query.isPending ? (
          <WidgetNotice>Running…</WidgetNotice>
        ) : query.error ? (
          <div className="h-full overflow-auto whitespace-pre-wrap rounded-oxidant-sm border border-danger-line bg-danger-tint p-2 font-mono text-xs text-danger">
            {query.error.message}
          </div>
        ) : query.data ? (
          <WidgetBody type={widget.type} result={query.data} options={widget.options} />
        ) : (
          <WidgetNotice>No result.</WidgetNotice>
        )}
      </div>
    </div>
  );
}
