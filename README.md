# Oxidant

**A drop-in Apache Spark replacement.** Oxidant speaks the [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html)
protocol, so unmodified PySpark and Spark SQL clients connect with a one-line URL change — no JVM.

> **Oxidant starts where Sail ends.** A lean vectorized CPU core (**Loom**) beats Sail on the
> queries that dominate ClickBench, and an opt-in HVM2/Bend backend (**Oxidant-HVM**) opens a
> second front for the embarrassingly-parallel, irregular workloads no columnar engine serves well.

## Status

Pre-alpha, and runnable: the engine executes SQL end-to-end over Spark Connect — point a
stock PySpark client at it, browse the built-in Web UI (monitoring, SQL editor, notebooks) on
port 4040, run SQL over the REST API, or use the `oxidant sql` CLI. **Quickstart:**
[`docs/getting-started.md`](docs/getting-started.md) · **All docs:**
[`docs/README.md`](docs/README.md). Deploy via the free
Community AMI on AWS Marketplace (listing in progress) or
`docker pull ghcr.io/oxidantdata/oxidant`; EC2 autoscaling via CloudFormation is
documented in [`docs/distributed-ec2.md`](docs/distributed-ec2.md).

## Architecture (one screen)

```
PySpark / Spark SQL  ──Spark Connect gRPC──▶  oxidant-connect
                                                  │
                              oxidant-plan (warp) ─ oxidant-analyzer ─ oxidant-optimizer (heddle) ─ oxidant-physical
                                                  │
                            ┌─────────────────────┴─────────────────────┐
                     oxidant-loom (CPU)                              oxidant-hvm (parallel/GPU)
              vectorized Arrow, DataFusion→native          Bend codegen → HVM2 (opt-in, gated)
                                                  │
                              oxidant-execution (local | driver/worker + Arrow Flight)
                                                  │
                              oxidant-datasource (Parquet/Delta/Iceberg) ─ oxidant-catalog (Unity/Glue/Hive)
```

Everything between operators is Apache Arrow. `oxidant-hvm` is the *only* place data leaves Arrow,
and only for coarse, routed fragments — never the columnar hot loop.

## Why not "just compile everything to Bend"?

Because HVM2 (Bend's runtime) has no data plane: 24-bit numerics (a `SUM`/`COUNT` overflows at
16.7M), no hash-table primitive, no columnar/SIMD type, a 4 GB heap, no I/O/FFI, and a CUDA-only,
RTX-4090-only GPU path the maintainers themselves call "less stable." On ClickBench — pure
columnar/SIMD work — HVM2 loses every query. So Loom (vectorized native) carries the benchmark;
HVM2 is a gated research bet for a *different* workload class. See `docs/architecture.md` §3, §6.

## North star

Beat [Sail's published ClickBench result](https://github.com/ClickHouse/ClickBench/tree/main/sail)
on `c6a.4xlarge`, CPU-only: total hot runtime ≤ ~56.3 s across all 43 queries, published as an
independent, reproducible ClickBench entry.

## Build

```sh
cargo build --workspace   # stub builds on Rust 1.72+
cargo test  --workspace
```

The runtime crates that will pull in DataFusion/Arrow/tonic require **Rust ≥ 1.80** and **protoc**;
those deps are stubbed out today (see each crate's `Cargo.toml` TODOs).

## Run

```sh
oxidant spark server --port 50051   # gRPC on 50051, Web UI + REST API on 4040
```
```python
from pyspark.sql import SparkSession  # pip install "pyspark-client>=4.0"
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
spark.sql("SELECT count(*) FROM parquet.`hits.parquet`").show()
```

Or open the SQL editor at http://localhost:4040, or run `oxidant sql -e "SELECT 1 AS hello"`.
Full walkthrough (Docker, all clients, smoke-test SQL quirks):
[`docs/getting-started.md`](docs/getting-started.md).

## License

GNU Affero General Public License v3.0 — see [`LICENSE`](LICENSE). Snapshots
at or before commit `b18eead` remain available under Apache-2.0. Commercial
licensing: [`COMMERCIAL.md`](COMMERCIAL.md). Trademark policy:
[`TRADEMARK.md`](TRADEMARK.md).
