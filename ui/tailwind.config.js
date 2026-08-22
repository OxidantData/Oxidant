/** @type {import('tailwindcss').Config} */
// Mirrors the website's tailwind.config.js so www.oxidantdata.com and the engine UI share one
// theme. Dark is the default; light is the opt-in `data-theme="light"` variant.
export default {
  darkMode: ['selector', ':root:not([data-theme="light"])'],
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  // Status classes are applied as `status-${job.status}` / `stmt-${doc.status}` template
  // literals, which Tailwind's content scanner cannot see. Without this they get purged and
  // every status renders as plain body text.
  safelist: [
    "status-SUCCEEDED",
    "status-COMPLETE",
    "status-SUCCESS",
    "status-FAILED",
    "status-RUNNING",
    "status-ACTIVE",
    "stmt-succeeded",
    "stmt-failed",
    "stmt-running",
    "stmt-pending",
    "stmt-canceled",
  ],
  theme: {
    extend: {
      colors: {
        bg: "var(--oxidant-bg)",
        "bg-subtle": "var(--oxidant-bg-subtle)",
        surface: "var(--oxidant-surface)",
        raised: "var(--oxidant-raised)",
        hairline: "var(--oxidant-border)",
        "hairline-strong": "var(--oxidant-border-strong)",
        body: "var(--oxidant-text)",
        secondary: "var(--oxidant-text-secondary)",
        muted: "var(--oxidant-text-muted)",
        // The inverted slab that stands in for an accent fill (primary buttons, badges).
        solid: "var(--oxidant-solid)",
        "solid-hover": "var(--oxidant-solid-hover)",
        "on-solid": "var(--oxidant-on-solid)",
        success: "var(--oxidant-success)",
        "success-tint": "var(--oxidant-success-tint)",
        warning: "var(--oxidant-warning)",
        "warning-tint": "var(--oxidant-warning-tint)",
        "warning-line": "var(--oxidant-warning-line)",
        // Engine-only — the site has no failure states to render. See src/styles/theme.css.
        danger: "var(--oxidant-danger)",
        "danger-tint": "var(--oxidant-danger-tint)",
        "danger-line": "var(--oxidant-danger-line)",
        "code-bg": "var(--oxidant-code-bg)",
        "code-text": "var(--oxidant-code-text)",
        "code-text-dim": "var(--oxidant-code-text-dim)",
        // Monochrome chart ramp — magnitude bars, per-stage timings.
        "chart-1": "var(--oxidant-chart-1)",
        "chart-2": "var(--oxidant-chart-2)",
        "chart-3": "var(--oxidant-chart-3)",
        "chart-4": "var(--oxidant-chart-4)",
        "chart-5": "var(--oxidant-chart-5)",
      },
      fontFamily: {
        sans: "var(--oxidant-font-ui)",
        mono: "var(--oxidant-font-mono)",
      },
      letterSpacing: {
        display: "-0.02em",
      },
      borderRadius: {
        oxidant: "var(--oxidant-radius)",
        "oxidant-sm": "var(--oxidant-radius-sm)",
      },
    },
  },
  plugins: [],
};
