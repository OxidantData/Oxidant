import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
// Brand typography, self-hosted: Geist on the weight axis, JetBrains Mono for plans, logs and
// SQL. Bundled rather than pulled from a CDN so an air-gapped driver renders identically.
import "@fontsource-variable/geist/wght.css";
import "@fontsource/jetbrains-mono/latin-400.css";
import "@fontsource/jetbrains-mono/latin-500.css";
import App from "./App";
import "./styles/global.css";

// Dashboards fetch through TanStack Query; the monitoring pages keep their own SSE-driven
// `usePolling`. Widget queries hit the engine, so refetching on every window focus would run
// real SQL each time the tab is looked at — the dashboard's own interval is the only timer.
const queryClient = new QueryClient({
  defaultOptions: {
    queries: { refetchOnWindowFocus: false, retry: false },
  },
});

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <App />
      </BrowserRouter>
    </QueryClientProvider>
  </React.StrictMode>
);
