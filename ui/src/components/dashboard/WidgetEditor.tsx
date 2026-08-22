import { useState } from "react";
import {
  WIDGET_TYPES,
  WIDGET_TYPE_LABELS,
  type WidgetOptions,
  type WidgetSpec,
  type WidgetType,
} from "@/lib/dashboards";
import SqlAutocompleteTextarea from "@/components/SqlAutocompleteTextarea";

interface WidgetEditorProps {
  widget: WidgetSpec;
  onSave: (widget: WidgetSpec) => void;
  onCancel: () => void;
}

/** Which per-type options the form offers. Everything else in `WidgetOptions` is API-only. */
const OPTION_FIELDS: Record<WidgetType, ("stacked" | "horizontal" | "smooth" | "unit")[]> = {
  bar: ["stacked", "horizontal"],
  line: ["smooth", "stacked"],
  area: ["smooth", "stacked"],
  pie: [],
  scatter: [],
  table: [],
  kpi: ["unit"],
};

const OPTION_LABELS = {
  stacked: "Stack series",
  horizontal: "Horizontal bars",
  smooth: "Smooth line",
  unit: "Unit suffix",
} as const;

/**
 * The add/edit widget panel: type, title, SQL, and the handful of options that type has.
 *
 * The SQL box is the editor page's autocompleting textarea, so catalog/table/column
 * completion works here exactly as it does under **Editor** — a widget is a saved query, and
 * writing one should not feel like a different product.
 */
export default function WidgetEditor({ widget, onSave, onCancel }: WidgetEditorProps) {
  const [draft, setDraft] = useState<WidgetSpec>(widget);
  const [error, setError] = useState<string | null>(null);

  function patch(next: Partial<WidgetSpec>) {
    setDraft((d) => ({ ...d, ...next }));
  }

  function patchOptions(next: Partial<WidgetOptions>) {
    setDraft((d) => ({ ...d, options: { ...d.options, ...next } }));
  }

  function save() {
    if (!draft.sql.trim()) {
      // The server rejects this too; catching it here keeps the panel open with the SQL intact.
      setError("A widget needs a SQL statement.");
      return;
    }
    onSave({ ...draft, title: draft.title.trim(), sql: draft.sql.trim() });
  }

  return (
    <div className="oxidant-card space-y-3">
      <div className="flex flex-wrap items-end gap-3">
        <label className="flex flex-col gap-1 text-xs text-muted">
          Type
          <select
            className="oxidant-input"
            value={draft.type}
            onChange={(e) => patch({ type: e.target.value as WidgetType })}
          >
            {WIDGET_TYPES.map((type) => (
              <option key={type} value={type}>
                {WIDGET_TYPE_LABELS[type]}
              </option>
            ))}
          </select>
        </label>
        <label className="flex min-w-52 flex-1 flex-col gap-1 text-xs text-muted">
          Title
          <input
            className="oxidant-input"
            value={draft.title}
            placeholder={WIDGET_TYPE_LABELS[draft.type]}
            onChange={(e) => patch({ title: e.target.value })}
          />
        </label>
        {OPTION_FIELDS[draft.type].map((field) =>
          field === "unit" ? (
            <label key={field} className="flex w-28 flex-col gap-1 text-xs text-muted">
              {OPTION_LABELS[field]}
              <input
                className="oxidant-input"
                value={draft.options.unit ?? ""}
                placeholder="%"
                onChange={(e) => patchOptions({ unit: e.target.value || undefined })}
              />
            </label>
          ) : (
            <label
              key={field}
              className="flex items-center gap-1.5 pb-2 text-sm text-muted"
            >
              <input
                type="checkbox"
                className="accent-solid"
                checked={draft.options[field] === true}
                onChange={(e) => patchOptions({ [field]: e.target.checked })}
              />
              {OPTION_LABELS[field]}
            </label>
          )
        )}
      </div>

      <label className="flex flex-col gap-1 text-xs text-muted">
        SQL
        <SqlAutocompleteTextarea
          className="oxidant-input h-32 w-full p-3 font-mono"
          value={draft.sql}
          spellCheck={false}
          aria-label="Widget SQL"
          onChange={(e) => patch({ sql: e.target.value })}
        />
      </label>

      {error && (
        <p className="rounded-oxidant-sm border border-danger-line bg-danger-tint p-2 text-xs text-danger">
          {error}
        </p>
      )}

      <div className="flex items-center gap-2">
        <button className="oxidant-btn-primary" onClick={save}>
          Save widget
        </button>
        <button className="oxidant-btn-ghost" onClick={onCancel}>
          Cancel
        </button>
        <span className="text-xs text-muted">
          First column labels the point; every numeric column after it is a series.
        </span>
      </div>
    </div>
  );
}
