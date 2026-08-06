/** @type {import('tailwindcss').Config} */
// Mirrors web/tailwind.config.js so the showcase site and the control plane share one theme.
export default {
  darkMode: ['selector', ':root[data-theme="dark"]'],
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: "var(--oxidant-bg)",
        "bg-subtle": "var(--oxidant-bg-subtle)",
        surface: "var(--oxidant-surface)",
        hairline: "var(--oxidant-border)",
        body: "var(--oxidant-text)",
        muted: "var(--oxidant-text-muted)",
        accent: "var(--oxidant-accent)",
        "accent-hover": "var(--oxidant-accent-hover)",
        "accent-contrast": "var(--oxidant-accent-contrast)",
        success: "var(--oxidant-success)",
        warning: "var(--oxidant-warning)",
        danger: "var(--oxidant-danger)",
        "code-bg": "var(--oxidant-code-bg)",
        "code-text": "var(--oxidant-code-text)",
      },
      fontFamily: {
        sans: "var(--oxidant-font-ui)",
        mono: "var(--oxidant-font-mono)",
      },
      borderRadius: {
        oxidant: "var(--oxidant-radius)",
        "oxidant-sm": "var(--oxidant-radius-sm)",
      },
      maxWidth: { content: "1100px" },
    },
  },
  plugins: [],
};
