# Web UI

When the server runs with the UI enabled (the default), open <http://localhost:4040>. The same
HTTP port serves the monitoring pages, the SQL editor, notebooks, and the
[REST API](api.md).

```sh
oxidant spark server --port 50051 --ui-port 4040
```

> **No auth.** The UI and REST API have no authentication or authorization. On shared or
> reachable hosts, bind loopback and tunnel instead:
>
> ```sh
> oxidant spark server --port 50051 --ui-bind 127.0.0.1
> ssh -L 4040:localhost:4040 user@host
> ```
>
> `--no-ui` disables the HTTP server entirely.
>
> The one authenticated route on this port is
> [`GET /api/status`](api.md#driver-status) — the control-plane status endpoint, which stays
> off unless `OXIDANT_STATUS_TOKEN` is set.

## Monitoring pages

The UI mirrors the Spark UI layout and is backed by the Spark-compatible
`/api/v1/applications/...` endpoints, so existing tooling against those routes works too.

| Page | What it shows |
|------|---------------|
| **Jobs** | One row per action/query execution: status, duration, stages |
| **Stages** | Stage detail — tasks, input/shuffle sizes, per-stage timing |
| **SQL** | Every SQL/DataFrame execution with its plan and duration |
| **Executors** | The driver plus any connected workers (see [workers.md](workers.md)) |
| **Environment** | Runtime info, Spark/Oxidant properties, catalog config |

## Theme

The UI carries the Oxidant brand theme from <https://www.oxidantdata.com>: dark-first
monochrome — layered near-blacks, hairline borders, Geist typography, and no decorative accent
colour. Emphasis comes from contrast and weight; the inverted white slab is what a primary
button gets instead of a coloured fill.

Colour is reserved for status, and only for status:

| Colour | Means |
|--------|-------|
| Green | Succeeded / completed, and "faster than Spark" on the Compare page |
| Amber | Running, pending, truncated results, "slower than Spark" |
| Red | Failed — a failed job, a rejected statement, a statement error pane |

A toggle in the header switches to the light theme. The choice persists in `localStorage`
under `oxidant-theme` and is applied before first paint, so there is no flash on reload.

Nothing is fetched from a CDN — no external fonts, no external assets — so the pages render
identically on a driver with no egress. The React build in [`ui/`](../ui) bundles Geist and
JetBrains Mono as self-hosted woff2 files; the single-file page compiled into the binary has no
asset pipeline, so it falls through to the system sans stack unless Geist is installed locally.

## SQL Editor

The **SQL Editor** page runs ad-hoc SQL over the REST statement API — no client install needed.

- Type SQL into the textarea, press **Cmd/Ctrl+Enter** (or the Run button).
- Results render as a table under the editor.
- A **recent statements** list (newest first) shows status — `pending`, `running`,
  `succeeded`, `failed`, `canceled` — with the error message for failures; click one to
  re-inspect its result.

## Notebook

The **Notebook** page is a lightweight SQL notebook that runs entirely in your browser against
the statement API.

- **SQL cells** execute against the server; **Markdown cells** render documentation between
  queries.
- Run cells individually, or **run all** top-to-bottom.
- The notebook persists to browser **localStorage** automatically.
- **Export/import** as JSON to share a notebook or move it between browsers/machines.

### Python notebooks (Jupyter)

No Python runs on the driver — Oxidant executes SQL and DataFrame plans, not Python code. For a
Python notebook experience, run your own Jupyter locally and connect with the stock PySpark
Connect client:

```sh
pip install "pyspark-client>=4.0" jupyterlab
jupyter lab
```

In a notebook cell:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
spark.sql("SELECT 1 AS hello").show()
df = spark.sql("SELECT * FROM VALUES (1,'a'),(2,'b') AS t(num, letter)")
df.filter("num > 1").show()
```

Every `spark.sql(...)` call here shows up on the **SQL** monitoring page and in the statement
list, so the UI remains useful as a query history/monitor alongside Jupyter.
