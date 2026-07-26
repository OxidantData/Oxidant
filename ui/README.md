# Weft Monitoring UI

Live Spark-like dashboard served by `weft spark server` on port **4040** (default).

## Development

With the Weft server running (`cargo run -p weft-cli -- spark server --port 50051`):

```bash
npm install
npm run dev   # http://localhost:4041, proxies /api to :4040
```

Production builds use the embedded SPA in `weft-ui-server` (no npm required at runtime).

## Tabs

- **Jobs** — query/action jobs with duration and status
- **Stages** — shuffle stage metrics and task progress
- **SQL** — physical execution plans
- **Executors** — Flight workers
- **Environment** — session config and `WEFT_*` env
- **Compare** — side-by-side Weft vs Spark REST metrics

## History server

```bash
WEFT_EVENT_LOG_DIR=/tmp/weft-events cargo run -p weft-cli -- spark server --no-ui
cargo run -p weft-cli -- history-server --dir /tmp/weft-events --port 18080
```
