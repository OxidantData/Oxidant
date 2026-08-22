import React, { useCallback, useMemo, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { GridLayout, useContainerWidth, type Layout } from "react-grid-layout";
import "react-grid-layout/css/styles.css";
import {
  appendToLayout,
  dashboardsApi,
  newWidgetId,
  REFRESH_CHOICES,
  type Dashboard,
  type DashboardLayoutItem,
  type WidgetSpec,
} from "@/lib/dashboards";
import { WIDGET_QUERY_KEY } from "@/lib/useWidgetQuery";
import WidgetCard from "@/components/dashboard/WidgetCard";
import WidgetEditor from "@/components/dashboard/WidgetEditor";

/** 12 columns, a 40px row: fine enough to place a KPI strip above a full-width chart. */
const GRID_COLS = 12;
const GRID_ROW_HEIGHT = 40;
const GRID_MARGIN: [number, number] = [12, 12];

/** What edit mode holds while unsaved. Saving PATCHes exactly these three fields. */
interface Draft {
  name: string;
  widgets: WidgetSpec[];
  layout: DashboardLayoutItem[];
}

/**
 * Every widget gets a grid cell, even one added through the API without a layout entry —
 * otherwise the card would exist in the document and never appear on screen.
 */
function ensureLayout(
  widgets: WidgetSpec[],
  layout: DashboardLayoutItem[]
): DashboardLayoutItem[] {
  const placed = new Map(layout.map((item) => [item.i, item]));
  const out: DashboardLayoutItem[] = [];
  for (const widget of widgets) {
    out.push(placed.get(widget.id) ?? appendToLayout(out, widget));
  }
  return out;
}

export default function DashboardPage() {
  const { id = "" } = useParams();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [draft, setDraft] = useState<Draft | null>(null);
  const [editingWidget, setEditingWidget] = useState<WidgetSpec | null>(null);
  const [error, setError] = useState<string | null>(null);

  const { data: saved, isPending, error: loadError } = useQuery({
    queryKey: ["dashboard", id],
    queryFn: () => dashboardsApi.get(id),
  });

  const save = useMutation({
    mutationFn: (next: Draft) =>
      dashboardsApi.update(id, {
        name: next.name,
        widgets: next.widgets,
        layout: next.layout,
      }),
    onSuccess: (updated: Dashboard) => {
      queryClient.setQueryData(["dashboard", id], updated);
      queryClient.invalidateQueries({ queryKey: ["dashboards"] });
      setDraft(null);
      setEditingWidget(null);
      setError(null);
    },
    onError: (e: Error) => setError(e.message),
  });

  const setRefresh = useMutation({
    mutationFn: (secs: number | null) =>
      dashboardsApi.update(id, { refreshIntervalSecs: secs }),
    onSuccess: (updated: Dashboard) =>
      queryClient.setQueryData(["dashboard", id], updated),
    onError: (e: Error) => setError(e.message),
  });

  const remove = useMutation({
    mutationFn: () => dashboardsApi.remove(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["dashboards"] });
      navigate("/dashboards");
    },
    onError: (e: Error) => setError(e.message),
  });

  const editing = draft !== null;
  const widgets = draft?.widgets ?? saved?.widgets ?? [];
  const layout = useMemo(
    () => ensureLayout(widgets, draft?.layout ?? saved?.layout ?? []),
    [widgets, draft?.layout, saved?.layout]
  );

  const onLayoutChange = useCallback(
    (next: Layout) => {
      // The grid also emits on mount and on width changes; only a deliberate edit is a change
      // worth marking unsaved.
      setDraft((d) => (d ? { ...d, layout: next as DashboardLayoutItem[] } : d));
    },
    []
  );

  function startEditing() {
    if (!saved) return;
    setDraft({
      name: saved.name,
      widgets: saved.widgets,
      layout: ensureLayout(saved.widgets, saved.layout),
    });
  }

  function addWidget() {
    if (!draft) return;
    setEditingWidget({
      id: newWidgetId(draft.widgets),
      type: "bar",
      title: "",
      sql: "",
      options: {},
    });
  }

  function saveWidget(widget: WidgetSpec) {
    setDraft((d) => {
      if (!d) return d;
      const exists = d.widgets.some((w) => w.id === widget.id);
      const widgets = exists
        ? d.widgets.map((w) => (w.id === widget.id ? widget : w))
        : [...d.widgets, widget];
      const layout = exists ? d.layout : [...d.layout, appendToLayout(d.layout, widget)];
      return { ...d, widgets, layout };
    });
    setEditingWidget(null);
  }

  function removeWidget(widget: WidgetSpec) {
    setDraft((d) =>
      d
        ? {
            ...d,
            widgets: d.widgets.filter((w) => w.id !== widget.id),
            layout: d.layout.filter((item) => item.i !== widget.id),
          }
        : d
    );
  }

  const refreshSecs = saved?.refreshIntervalSecs ?? null;
  // Auto-refresh is a view-mode affordance: re-running queries under a half-edited layout
  // would fight the person editing it.
  const refetchIntervalMs = !editing && refreshSecs ? refreshSecs * 1000 : false;
  const { width, containerRef } = useContainerWidth();

  return (
    <div className="flex h-full flex-col gap-3">
      {/* The toolbar and the editor panel come and go; the grid container below must not.
          `useContainerWidth` attaches its ResizeObserver on mount, so a container that only
          appears once the fetch resolves would never get measured — the grid would sit at the
          hook's 1280px default forever, no matter how wide the window is. */}
      {saved && (
        <div className="flex flex-wrap items-center gap-2">
          <button
            className="oxidant-link text-sm"
            onClick={() => navigate("/dashboards")}
          >
            ← Dashboards
          </button>
          <span className="h-4 w-px bg-hairline-strong" aria-hidden="true" />
          {editing ? (
            <input
              className="oxidant-input min-w-64 text-base font-medium"
              value={draft.name}
              aria-label="Dashboard name"
              onChange={(e) => setDraft({ ...draft, name: e.target.value })}
            />
          ) : (
            <h1 className="truncate text-base font-medium text-body">{saved.name}</h1>
          )}

          <div className="ml-auto flex flex-wrap items-center gap-2">
            {editing ? (
              <>
                <button className="oxidant-btn-ghost" onClick={addWidget}>
                  Add widget
                </button>
                <button
                  className="oxidant-btn-primary"
                  onClick={() => save.mutate(draft)}
                  disabled={save.isPending || !draft.name.trim()}
                >
                  {save.isPending ? "Saving…" : "Save"}
                </button>
                <button
                  className="oxidant-btn-ghost"
                  onClick={() => {
                    setDraft(null);
                    setEditingWidget(null);
                    setError(null);
                  }}
                >
                  Cancel
                </button>
              </>
            ) : (
              <>
                <button
                  className="oxidant-btn-ghost"
                  onClick={() =>
                    queryClient.invalidateQueries({ queryKey: [WIDGET_QUERY_KEY] })
                  }
                >
                  Refresh
                </button>
                <label className="flex items-center gap-1.5 text-xs text-muted">
                  Auto
                  <select
                    className="oxidant-input py-1 text-xs"
                    aria-label="Auto-refresh interval"
                    value={refreshSecs ?? ""}
                    onChange={(e) =>
                      setRefresh.mutate(e.target.value ? Number(e.target.value) : null)
                    }
                  >
                    {REFRESH_CHOICES.map((choice) => (
                      <option key={choice.label} value={choice.secs ?? ""}>
                        {choice.label}
                      </option>
                    ))}
                  </select>
                </label>
                <button className="oxidant-btn-ghost" onClick={startEditing}>
                  Edit
                </button>
                <button
                  className="oxidant-btn-ghost"
                  onClick={() => {
                    if (window.confirm(`Delete dashboard “${saved.name}”?`)) remove.mutate();
                  }}
                >
                  Delete
                </button>
              </>
            )}
          </div>
        </div>
      )}

      {error && (
        <div className="rounded-oxidant-sm border border-danger-line bg-danger-tint p-2 text-xs text-danger">
          {error}
        </div>
      )}

      {editingWidget && (
        <WidgetEditor
          key={editingWidget.id}
          widget={editingWidget}
          onSave={saveWidget}
          onCancel={() => setEditingWidget(null)}
        />
      )}

      {/* react-grid-layout v2 types its ref against React 19's `RefObject<T | null>`; this
          app is on React 18, whose `ref` prop still wants `RefObject<T>`. Same object either
          way — the cast is the version gap, not a lie about nullability. */}
      <div
        ref={containerRef as React.RefObject<HTMLDivElement>}
        className="min-h-0 flex-1 overflow-auto"
      >
        {isPending ? (
          <p className="text-sm text-muted">Loading dashboard…</p>
        ) : loadError ? (
          <div className="oxidant-card text-sm text-danger">
            {(loadError as Error).message}
          </div>
        ) : !widgets.length ? (
          <div className="oxidant-card text-sm text-muted">
            No widgets yet.{" "}
            {editing
              ? "Use “Add widget” above."
              : "Choose “Edit”, then “Add widget”, to put a query on the grid."}
          </div>
        ) : (
          <GridLayout
            width={width}
            layout={layout}
            gridConfig={{
              cols: GRID_COLS,
              rowHeight: GRID_ROW_HEIGHT,
              margin: GRID_MARGIN,
              containerPadding: [0, 0],
            }}
            dragConfig={{
              enabled: editing,
              handle: ".dash-drag-handle",
              cancel: ".dash-no-drag,button,input,select,textarea",
            }}
            resizeConfig={{ enabled: editing }}
            onLayoutChange={onLayoutChange}
          >
            {widgets.map((widget) => (
              <div key={widget.id}>
                <WidgetCard
                  widget={widget}
                  editing={editing}
                  refetchIntervalMs={refetchIntervalMs}
                  onEdit={setEditingWidget}
                  onRemove={removeWidget}
                />
              </div>
            ))}
          </GridLayout>
        )}
      </div>
    </div>
  );
}
