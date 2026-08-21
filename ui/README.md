# Oxidant Monitoring UI

Live Spark-like dashboard served by `oxidant spark server` on port **4040** (default).

## Development

With the Oxidant server running (`cargo run -p oxidant-cli -- spark server --port 50051`):

```bash
npm install
npm run dev   # http://localhost:4041, proxies /api to :4040
```

Production builds use the embedded SPA in `oxidant-ui-server` (no npm required at runtime).

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
