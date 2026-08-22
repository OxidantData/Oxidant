/// <reference types="vitest/config" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";

// Vitest picks React's build from NODE_ENV at import time. A shell that exports
// NODE_ENV=production (as this repo's does) would hand the tests React's production bundle,
// where `act()` throws and Testing Library cannot render anything. Tests always want dev.
if (process.env.VITEST) process.env.NODE_ENV = "test";

export default defineConfig({
  plugins: [react()],
  base: "/",
  resolve: {
    alias: { "@": path.resolve(__dirname, "src") },
  },
  server: { port: 4041, proxy: { "/api": "http://localhost:4040" } },
  test: {
    environment: "jsdom",
    setupFiles: ["./vitest.setup.ts"],
    include: ["src/**/*.test.{ts,tsx}"],
    globals: true,
    restoreMocks: true,
  },
});
