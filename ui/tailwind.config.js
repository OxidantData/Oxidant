/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: "var(--oxidant-bg)",
        surface: "var(--oxidant-surface)",
        border: "var(--oxidant-border)",
        text: "var(--oxidant-text)",
        muted: "var(--oxidant-text-muted)",
        accent: "var(--oxidant-accent)",
        success: "var(--oxidant-success)",
        danger: "var(--oxidant-danger)",
        warning: "var(--oxidant-warning)",
      },
    },
  },
  plugins: [],
};
