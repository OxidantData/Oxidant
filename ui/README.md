# Oxidant Monitoring UI

Live Spark-like dashboard served by `oxidant spark server` on port **4040** (default).

## Development

With the Oxidant server running (`cargo run -p oxidant-cli -- spark server --port 50051 --foreground`):

```bash
npm install
npm run dev   # http://localhost:4041, proxies /api to :4040
```

```bash
npm test        # vitest: mapping, chart options, widget render smoke
npm run build   # tsc + vite -> dist/
```

By default the binary serves the single-file page compiled into `oxidant-ui-server`, so no npm
is required at runtime. That page cannot import anything, which is fine for the monitoring
tables and impossible for **Dashboards** — to serve this app instead, point the server at a
build of it:

```bash
npm run build
OXIDANT_UI_DIR=$PWD/dist oxidant start --port 50051 --ui-port 4040
```

## Theme

Design tokens live in [`src/styles/theme.css`](src/styles/theme.css) — a verbatim copy of the
website's `src/styles/theme.css` (same `--oxidant-*` variable names, same values) plus one
engine-only addition: `--oxidant-danger`, because this UI has failed jobs to render and the
marketing site does not. `tailwind.config.js` mirrors the site's token-to-utility mapping.
Re-syncing the brand is a file copy of `theme.css` and re-checking that config.

Dark is the default; light is `:root[data-theme="light"]`, toggled from the header and
persisted under the `oxidant-theme` localStorage key.

The single-file fallback UI compiled into `oxidant-ui-server`
(`crates/oxidant-ui-server/src/embedded_ui.html`) carries a hand-kept copy of the same tokens —
it is served by the binary itself and cannot import anything.

## Tabs

- **Jobs** — query/action jobs with duration and status
- **Stages** — shuffle stage metrics and task progress
- **SQL** — physical execution plans
- **Executors** — Flight workers
- **Environment** — session config and `OXIDANT_*` env
- **Cluster** — mode, workers, process metrics, and the driver's log buffer. That last one
  (`GET /api/v1/logs`) is gated by `OXIDANT_STATUS_TOKEN`; the pane says so and takes the token,
  storing it under the same `oxidant.statusToken` key the embedded console uses
- **Dashboards** — grids of SQL-backed widgets (ECharts + react-grid-layout); see
  [docs/web-ui.md](../docs/web-ui.md#dashboards) for the SQL-to-chart convention

**Pipelines** and **Observability** are not here: they are pages of the embedded console, and
setting `OXIDANT_UI_DIR` to serve this app swaps that console out. See
[docs/web-ui.md](../docs/web-ui.md#two-consoles).

## History server

```bash
OXIDANT_EVENT_LOG_DIR=/tmp/oxidant-events cargo run -p oxidant-cli -- spark server --no-ui --foreground
cargo run -p oxidant-cli -- history-server --dir /tmp/oxidant-events --port 18080
```
