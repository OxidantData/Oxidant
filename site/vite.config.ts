import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "node:path";

// Production site at https://oxidantdata.com/ (apex) → assets resolve under /.
// Override with VITE_BASE='/path/' for path-hosted deploys (e.g. GitHub Pages project pages).
const base = process.env.VITE_BASE ?? "/";

export default defineConfig({
  base,
  plugins: [react()],
  resolve: {
    alias: { "@": path.resolve(__dirname, "src") },
  },
  server: { port: 5174 },
});
