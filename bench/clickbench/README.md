# ClickBench harness for Oxidant

Oxidant's north-star benchmark. Mirrors the [ClickBench](https://github.com/ClickHouse/ClickBench)
entry contract so results are independently reproducible, and so we can submit an entry
under `ClickHouse/ClickBench/oxidant/`.

## Headline result (2026-06-24)

On AWS **c6a.4xlarge** (16 vCPU / 32 GiB), real **14.78 GB hits.parquet** (99,997,497 rows),
all 43 queries through the live `oxidant-connect` Spark Connect server, 3 tries, hot = min(try2,try3):

| Engine | Hot total | Notes |
|--------|----------:|-------|
| **Oxidant** | **45.51 s** | DataFusion 54 core, tuned (pushdown/reorder/StringView) |
| Sail (published 2026-05-11) | 56.3 s | baseline |

**Oxidant is ~19% faster than Sail** on identical hardware + dataset + methodology. (Caveat: the
current margin rides partly on a newer DataFusion + a warm reused server; native operators for
the durable lead are tracked in `docs/ISSUES.md` Phase 1.)

## Reproduce

On a fresh Linux box (or the `scratchpad/c6a.sh` AWS helper):
```sh
./bench/clickbench/install          # build prereqs + Rust 1.90
./bench/clickbench/benchmark.sh     # build, fetch 14.78 GB, run 43 queries → results/c6a.4xlarge.json
```
To submit upstream: copy `results/<date>/c6a.4xlarge.json` + `template.json` into a `oxidant/`
directory under `ClickHouse/ClickBench` alongside these `install`/`benchmark.sh` scripts.

## Two harnesses

1. **Official entry (this directory's shell scripts)** — `install` + `benchmark.sh` + `query`
   run the real 14 GB `hits.parquet` on a `c6a.4xlarge`, driving the **live Spark Connect
   server** via a stock PySpark client (`sc://`), 3 tries/query. This is what we publish and
   compare against Sail's 56.3 s. *Not yet wired to the live server.*
2. **Local coverage harness (`oxidant-bench`)** — `cargo run -p oxidant-bench -- clickbench` runs the
   same 43 (DataFusion-dialect) queries through `oxidant_loom` directly against a **synthetic
   `hits` table** built from `hits_schema.tsv`. It proves all 43 queries run to completion and
   emits `results/local-synthetic.json`. It is **for dev/CI coverage only** — synthetic data
   and (by default) debug builds, so its timings are *not* comparable to Sail's absolute
   numbers. Gated in CI as `clickbench-coverage`. A second mode, `clickbench-grpc`, runs the
   same 43 queries through the **live `oxidant-connect` server over gRPC** (writes synthetic
   `hits.parquet`, boots the server, `CREATE EXTERNAL TABLE` + queries → Arrow IPC), exercising
   the full production transport (gated as `clickbench-grpc-coverage`). Both **43/43 pass**.

## Contract (from ClickBench)

- Dataset: single `hits` table, 99,997,497 rows × 105 cols, `hits.parquet` ≈ 14.78 GB.
- Each of the 43 queries runs **3 times**; the **hot** number is `min(try2, try3)`; the
  **cold** number is try 1 (caches dropped before it).
- Default hardware: AWS `c6a.4xlarge` (16 vCPU), 500 GB gp2, Ubuntu.
- `query` contract: read SQL on stdin → result to stdout → fractional **seconds on the last
  line of stderr** → non-zero exit on error.

## Files

| file | role |
|------|------|
| `install` | install the `oxidant` server + a PySpark client into a venv |
| `benchmark.sh` | top-level driver: download → start server → run 43 queries ×3 → emit JSON |
| `query` | run one SQL statement against `sc://localhost:50051`, time it |
| `queries.sql` | the 43 queries (vendored from upstream; a few included here as anchors) |
| `assert-beats-sail.sh` | CI gate: fail if total hot runtime > Sail's baseline |
| `run-correctness.sh` | run the 43 queries and diff results vs a DuckDB/Spark oracle |
| `template.json` | static system metadata for the results file |

## Sail baseline to beat (c6a.4xlarge, 2026-05-11)

Total hot ≈ **56.3 s**; the heavy hitters are Q24 (~10.2 s), Q34/Q35 (~5 s each).
See `../../docs/architecture.md` for the per-query target table.
