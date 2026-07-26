/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: "var(--weft-bg)",
        surface: "var(--weft-surface)",
        border: "var(--weft-border)",
        text: "var(--weft-text)",
        muted: "var(--weft-text-muted)",
        accent: "var(--weft-accent)",
        success: "var(--weft-success)",
        danger: "var(--weft-danger)",
        warning: "var(--weft-warning)",
      },
    },
  },
  plugins: [],
};
