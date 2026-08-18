# Runnable example

[`oxidant.yaml`](oxidant.yaml) is a complete Oxidant configuration that needs **no broker, no
metastore, and no AWS account**. It reads the committed [`../sample-data`](../sample-data) tree
and a small spool of newline-delimited JSON standing in for a Kafka topic.

```sh
cargo build -p oxidant-cli

# Query the sample tables — Parquet, CSV, Delta, and Iceberg — with no server running
./target/debug/oxidant sql -c examples/oxidant.yaml \
  -e "SELECT count(*) FROM local.samples.nation_delta"

# Inspect the pipeline without building anything
./target/debug/oxidant pipeline validate -c examples/oxidant.yaml
./target/debug/oxidant pipeline show     -c examples/oxidant.yaml

# Build bronze -> silver -> gold, then read the result back
./target/debug/oxidant pipeline run -c examples/oxidant.yaml --once
./target/debug/oxidant sql -c examples/oxidant.yaml \
  -e "SELECT * FROM local.live.revenue_gold ORDER BY event_date, customer"
```

```
+------------+----------+---------+--------+
| event_date | customer | revenue | orders |
+------------+----------+---------+--------+
| 2025-08-12 | ada      | 100     | 1      |
| 2025-08-12 | bob      | 250     | 1      |
| 2025-08-13 | ada      | 300     | 1      |
| 2025-08-13 | cy       | 75      | 1      |
+------------+----------+---------+--------+
```

Two things to notice. `ada` totals `400` across the two days and not `395`: the `-5` order is
**excluded** by the `amount_positive` expectation rather than netted into the sum — check with
`SELECT customer, sum(revenue) FROM local.live.revenue_gold GROUP BY customer`. And `event_date`
comes from the payload's own `event_ts`, not from when the record was ingested, which is why the
five orders land on two dates instead of all on today.

The pipeline writes into `examples/warehouse/`, which is git-ignored. Delete it to start over;
delete `examples/warehouse/_checkpoints/` too, or the streaming table resumes where it left off.

`spool/orders/batch-*.json` is one file per micro-batch. Adding a `batch-2.json` and re-running
ingests it. This is a test fixture, not a broker: it has one partition and replays whole files
on restart, so a re-run against the same spool ingests the same rows again. A real Kafka source
resumes from its checkpointed offsets and does not. See [`../docs/pipelines.md`](../docs/pipelines.md).
