import type { ReactNode } from "react";

/**
 * The "this widget has nothing to show" state — an empty result, a non-numeric column, a
 * caveat about which column got plotted. Muted body text, never an error colour: red is
 * reserved for a statement that actually failed.
 */
export default function WidgetNotice({ children }: { children: ReactNode }) {
  return (
    <div className="flex h-full items-center justify-center p-4 text-center text-sm text-muted">
      <span className="max-w-md">{children}</span>
    </div>
  );
}
