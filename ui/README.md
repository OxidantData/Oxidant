# Oxidant Monitoring UI

Live Spark-like dashboard served by `oxidant spark server` on port **4040** (default).

## Development

With the Oxidant server running (`cargo run -p oxidant-cli -- spark server --port 50051`):

```bash
npm install
npm run dev   # http://localhost:4041, proxies /api to :4040
```

Production builds use the embedded SPA in `oxidant-ui-server` (no npm required at runtime).

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
- **Compare** — side-by-side Oxidant vs Spark REST metrics

## History server

```bash
OXIDANT_EVENT_LOG_DIR=/tmp/oxidant-events cargo run -p oxidant-cli -- spark server --no-ui
cargo run -p oxidant-cli -- history-server --dir /tmp/oxidant-events --port 18080
```
