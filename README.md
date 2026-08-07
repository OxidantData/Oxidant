# Oxidant

**A drop-in Apache Spark replacement.** Oxidant speaks the [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html)
protocol, so unmodified PySpark and Spark SQL clients connect with a one-line URL change — no JVM.

> **Oxidant starts where Sail ends.** A lean vectorized CPU core (**Loom**) — a single
> execution backend, no second runtime — beats Sail on the queries that dominate
> ClickBench.

## Status

Pre-alpha scaffold. The workspace compiles but does not yet execute queries. See
[`docs/architecture.md`](docs/architecture.md) for the full plan and
[`docs/CODEMAP.md`](docs/CODEMAP.md) for the ownership map. Deploy via the free
Community AMI on AWS Marketplace (listing in progress) or
`docker pull ghcr.io/oxidantdata/oxidant`; EC2 autoscaling via CloudFormation is
documented in [`docs/distributed-ec2.md`](docs/distributed-ec2.md).

## Architecture (one screen)

```
PySpark / Spark SQL  ──Spark Connect gRPC──▶  oxidant-connect
                                                  │
                              oxidant-plan (warp) ─ oxidant-analyzer ─ oxidant-optimizer (heddle) ─ oxidant-physical
                                                  │
                                            oxidant-loom (CPU)
                                   vectorized Arrow, DataFusion→native
                                                  │
                              oxidant-execution (local | driver/worker + Arrow Flight)
                                                  │
                              oxidant-datasource (Parquet/Delta/Iceberg) ─ oxidant-catalog (Unity/Glue/Hive)
```

Everything between operators is Apache Arrow — no operator, present or planned, leaves it.

## Why not "just compile everything to Bend"?

HVM2 (Bend's runtime) has no data plane — 24-bit numerics, no hash table, no
columnar/SIMD type, a 4 GB heap, no I/O/FFI, a CUDA-only GPU path — so it cannot run
Oxidant's workload class at all. The scaffold was removed; verdict and rationale:
[`docs/HVM_VERDICT.md`](docs/HVM_VERDICT.md).

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

## Run (target UX, not yet implemented)

```sh
oxidant spark server --port 50051
```
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
spark.sql("SELECT count(*) FROM parquet.`hits.parquet`").show()
```

## License

GNU Affero General Public License v3.0 — see [`LICENSE`](LICENSE). Snapshots
at or before commit `b18eead` remain available under Apache-2.0. Commercial
licensing: [`COMMERCIAL.md`](COMMERCIAL.md). Trademark policy:
[`TRADEMARK.md`](TRADEMARK.md).
