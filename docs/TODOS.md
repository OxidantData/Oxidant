# TODOS

Open work items and gates, grouped by area. Pick work here; keep entries short and
link to docs/issues for detail. (Fresh file — an earlier internal TODOS.md was
removed in the pre-launch cleanup; this list starts with the public repo's items.)

## Binary releases / packaging

Binary releases ship via cargo-dist on every `v*` tag (see
[.github/workflows/binaries.yml](../.github/workflows/binaries.yml)): curl|sh
installer, Homebrew tap, tarballs + checksums, and `.deb`/`.rpm` packages on the
GitHub Release. Future items:

- [ ] Hosted APT repo (Cloudsmith / Gemfury) so users can `apt install oxidant`
      with upgrades — today the `.deb` is a manual download + `dpkg -i` from
      GitHub Releases.
- [ ] Hosted RPM repo (yum/dnf) — `.rpm` artifacts already ship per release but
      there is no repo metadata to subscribe to.
- [ ] Submit `oxidant` to Homebrew core once the project meets homebrew-core's
      notability requirements — until then the tap is
      [OxidantData/homebrew-tap](https://github.com/OxidantData/homebrew-tap).
- [ ] curl|sh installs the binary only (cargo-dist ships no data files), so the
      sample tables need `sample-data.tar.gz` from the release + `--sample-data`
      there — documented in getting-started.md. Tarballs, Homebrew and deb/rpm
      all auto-discover bundled samples (verified v0.1.0). Revisit if cargo-dist
      adds data-file installs.
- [ ] musl static builds (`x86_64_`/`aarch64-unknown-linux-musl`) for Alpine and
      minimal containers — needs a native-dep audit (ring, zstd-sys) for musl
      safety; gnu targets ship today.

## Config-driven binary — deferred work

Follow-ups deliberately left out of the config-file / local-catalog / declarative-pipeline
work (plan: a standalone `oxidant` binary driven by `oxidant.yaml`). Each was a conscious
scope decision, not an oversight — the rationale matters as much as the item.

### Engine config is a front-end over env vars, not a plumbed config object

- [ ] Plumb a real config object through the engine instead of lowering `engine:` to
      `OXIDANT_*` variables with `set_var` before construction. ~120 variables are read via
      `std::env::var` across 51 files, which is why the config crate lowers to that contract
      rather than replacing it. Consequence today: `engine:` keys only take effect in a
      process the CLI starts — they cannot retune a running server, and a variable already
      set in the environment wins over the file.

### Streaming fault tolerance — what is done, and what is left

Landed: the write-ahead offset log (`offsets/<batchId>` before processing, `commits/<batchId>`
after the sink), so a replay covers the range it was recorded as reading; event-time watermarks
resumed from the checkpoint; checkpointed `dropDuplicates` state expired by the watermark; a
durable per-table epoch replacing the wall clock; idle passes skipped; a bounded `availableNow`
drain; and `warn`/`fail` expectations on streaming tables, counted per micro-batch. See
[streaming.md](streaming.md#exactly-once-and-what-makes-it-true) and
[pipelines.md](pipelines.md).

Left, in rough order of value:

- [ ] **Restart strategies.** `is_retryable` matches `Error::Io` and nothing else, so a Delta
      commit that exhausts its own attempts, a throttled Glue call surfaced as
      `Error::Execution`, or a transient DataFusion failure all kill the batch on first sight.
      Around the batch there is no strategy at all: the Connect path calls `fail()` and the query
      is dead until someone notices, while the pipeline runner retries every trigger forever with
      no backoff and no failure-rate ceiling. Flink's fixed-delay / exponential-delay /
      failure-rate strategies exist because both extremes are wrong.
- [ ] **Graceful shutdown.** No `SIGINT`/`SIGTERM` handling, and the `ProcessingTime` arm is a
      `loop` with no break. A rolling deploy therefore lands mid-batch every time. The offset log
      makes that recoverable rather than lossy, but finishing the in-flight batch and exiting 0
      is still the right behaviour — and `stop --savepoint` after it.
- [ ] **A commit protocol for the non-Delta sinks.** `FileSink` and the Parquet sink write in
      place, so a reader listing mid-write sees partial files and a failed batch leaves orphans.
      Flink's in-progress → pending → committed shape maps onto the batch boundary directly.
- [ ] **Dedup state is written whole with each batch**, which bounds how large a key set is
      practical. A real state backend (or at minimum an incremental encoding) is the fix; until
      then the lateness window is the lever.
- [ ] **Fault-tolerance metrics.** Per-partition lag (high watermark - committed offset),
      checkpoint write duration, restart count, records behind the watermark. Every failure in
      this area is currently invisible in production, which is what makes silent loss silent.
- [ ] **Watermark idleness.** The watermark is a single global maximum, not a per-partition
      minimum with an idle-partition timeout, so one stalled partition cannot hold it back —
      the safe direction for correctness, but state expires on schedule even when a partition
      is lagging.

### Derived tables are fully recomputed

- [ ] Incremental / append-only derived tables. A derived table is recomputed in full on
      every update: always correct, needs no cross-batch state, and O(full table) per update.
      A gold aggregate over a large bronze table will dominate the trigger interval. Real
      incrementalization needs cross-batch state, which the engine does not have (see
      [streaming.md](streaming.md) "no stateful aggregation across batches") — a separate
      project, not a follow-up commit.

### AUTO CDC rewrites the whole target per batch

- [ ] Give AUTO CDC a real Delta `MERGE`. There is no merge operator in the engine, so each
      micro-batch is a read-modify-write of the entire target: read the current snapshot, merge
      in SQL, commit a replacement version through the Delta sink. Correct and atomic, but
      O(target rows) per batch — not viable for a large target at a fast trigger.
- [ ] SCD Type 2 / `TRACK HISTORY`. Rejected outright today; only SCD Type 1 is implemented.
      Type 2 needs per-key validity ranges, which is a different merge, not a flag on this one.

### Derived-table writes are materialized in memory

- [ ] Stream a derived table's recompute to its sink instead of collecting the whole result
      into driver memory first. `recompute` calls `Engine::sql`, which returns
      `Vec<RecordBatch>`, and hands that to `LakeSink::replace_batch`. A gold aggregate is
      small enough for this to be fine; a silver table that is a near-copy of a large bronze
      table is not. The batch CTAS path already streams (`run_create_table_ctas`), so the shape
      to copy exists.

### Streaming runs on the driver

- [ ] Run micro-batches on the Flight worker cluster. The streaming path is single-process
      on the driver, so one process bounds throughput; `maxOffsetsPerTrigger` remains the
      only lever. Unchanged by the config work.

### Catalog write DDL gaps

- [ ] Write DDL for the Hive catalog: it has `create_table` but no `create_database`, so
      `LakeSink::open` fails against it and it cannot be a pipeline sink. Only `local` and
      `glue` can. Config validation rejects a Hive/REST pipeline sink at load rather than
      failing mid-run.
- [ ] Write DDL for the REST/Unity catalog: it implements no `create_table`,
      `create_database`, or `alter_table` at all.
- [ ] Replace `oxidant-catalog-rest`'s transport: it shells out to blocking `curl`
      subprocesses from inside `async fn` bodies, which stalls a Tokio worker under load.
      Pre-existing and unrelated to the config work.
- [ ] Route `DROP TABLE` / `DROP DATABASE` / `SHOW PARTITIONS` / `MSCK REPAIR TABLE` from SQL
      to the catalog SPI. Glue implements `drop_table`, `drop_database`, `list_partitions`,
      and `repair_table`, and the local catalog implements them for symmetry, but **no SQL
      path reaches any of them** — they are unwired, and should not be described as working.

### SQL batch writes — what the first cut does not do

Delta CTAS, `INSERT INTO`, and `INSERT OVERWRITE` through a catalog now work (see
[sql-writes.md](sql-writes.md)). Left out:

- [ ] Carry `OPTIONS(…)` / `COMMENT` / `TBLPROPERTIES(…)` through a catalog CTAS.
      `CatalogProvider::create_table` takes a schema, a format, a location, and partition
      columns — nothing else — so these are **refused** today rather than accepted and
      dropped. `OPTIONS` is the one that matters: it is how a CSV table declares `header` and
      `delimiter`, so a CSV CTAS into a catalog cannot yet be configured.
- [ ] Non-CTAS `CREATE TABLE <catalog>.<db>.<t> (cols) USING <fmt>`. DataFusion only calls
      `SchemaProvider::register_table` for CTAS, so there is no seam for a declare-only
      create; it would need its own path in `Engine::sql` calling the SPI directly.
- [ ] `INSERT` collects its rows in driver memory before committing, because one Delta commit
      is atomic over the file set it carries. Staging data files first and committing them
      together would let an arbitrarily large `INSERT` run — the same work as streaming a
      derived table's recompute, above.
- [ ] `INSERT OVERWRITE` on a plain Parquet catalog table. Refused, not missing: with no
      transaction log the replacement could not be atomic, and a reader listing the directory
      mid-overwrite would see a half-empty table.
- [ ] `ALTER TABLE` / schema evolution. An `INSERT` casts to the table's types where a cast is
      safe and errors otherwise; it never widens the table.
- [ ] Partitioned writes for the flat formats. `PARTITIONED BY` on a `parquet`/`csv`/`json`
      CTAS, and `INSERT` into an already-partitioned Parquet table, are **refused**: this writer
      emits one flat file, so the partition columns would never reach the directory path the
      reader derives them from. Delta partitions on write and is the documented alternative.
- [ ] Reserved words in namespace or table position. Oxidant quotes an `INSERT` target's
      leading segment when it names a registered catalog (`LOCAL` is a keyword in the `INSERT`
      grammar — Hive's `INSERT OVERWRITE LOCAL DIRECTORY`), but `INSERT INTO cat.order.t`
      still needs backticks from the user.

### Iceberg stays publish-only

The `iceberg_append` stub is **deleted**. It was `pub` with no callers, wrote through local
`std::fs` only, derived snapshot ids by counting files, and emitted a hand-rolled two-field Avro
manifest no real Iceberg reader accepts — an exported function that looked like a supported write
path.

- [ ] Decide whether Iceberg ever becomes a real sink format. Today it is deliberately rejected
      as one: a Delta sink publishes Iceberg metadata over the same Parquet files
      (`icebergCompat`), so one copy of the data is readable by both. That design is why the stub
      was not worth repairing.

### SDP Phase 4a follow-ups

The Python query-function signal stream (see
[pipelines.md](pipelines.md)) works against a client that
defines its empty-relation flows *before* opening the stream. Nothing below can produce a wrong
result silently — each one fails loudly, just later or with a worse message than it should.

- [ ] **Hold the signal stream open and push at `StartRun`.** `execute_plan` materializes the
      full response list before streaming, so the RPC emits whatever is pending at call time and
      completes. Upstream Spark pushes signals during graph resolution while the client holds the
      stream. A 4.2+ client following that order gets an empty, already-closed stream and fails at
      `StartRun`. This is the load-bearing constraint on whether the feature works against a real
      client, not a cosmetic one — no stock client calls the path today, which is the only reason
      it is deferred.
- [ ] **Harden the deferred SQL pass-through gate.** Forwarded text is only checked for
      parseability: `DFParser::parse_sql` returns multiple statements and the predicate is `any`,
      so `SELECT … FROM bronze; DROP TABLE x` is stored verbatim and blows up as a parse error
      once the runner wraps it as a subquery. DDL/DML resolves table references too, so
      `INSERT INTO bronze SELECT …` passes the same way. Require exactly one statement and that
      it be a `Statement::Query`, and reject with `invalid_argument` up front.
- [ ] **Keep parameterized `SQL` relations on the planning path.** `sc::Sql` carries
      `args` / `pos_args` / `named_arguments` / `pos_arguments`; the pass-through forwards
      `sql.query` alone, so `spark.sql("… WHERE d = :d", args={...})` ships raw `:d` to the
      runner. Not a regression (the planning path ignores them too), but restricting the
      pass-through to the no-arguments case moves the failure back to analysis time for free.
- [ ] **Leaf-name matching over-defers across catalogs.** `other_catalog.other_db.orders_bronze`
      matches a graph output named `orders_bronze`, so it both skips planning and gets a false
      dependency edge in the runner's DAG. `oxidant_pipelines::Graph::build` matches the same way,
      so the two are at least consistent; fixing it means fixing both together.

### Not doing (and why)

- [ ] Migrate the CLI to `clap` ([main.rs](../crates/oxidant-cli/src/main.rs) TODO). Tempting
      while adding subcommands, but it rewrites every existing flag's parsing — its own PR,
      with its own regression surface.
- [ ] A SQL REPL. `oxidant sql` is one-shot; there is no readline/interactive loop anywhere.
- [ ] Wire remaining `pipelines.proto` surface (SDP Phase 4): sinks / `ExecuteOutputFlows`
      ([#93](https://github.com/oxidantdata/oxidant/issues/93)). Core `PipelineCommand`
      dispatch, SQL graph parsing, and `StartRun` execution landed in SDP Phases 1–2; the
      query-function execution signal stream
      ([#91](https://github.com/oxidantdata/oxidant/issues/91)) landed in Phase 4a, with the
      follow-ups listed above; AUTO CDC / SCD Type 1 flows
      ([#92](https://github.com/oxidantdata/oxidant/issues/92)) landed in Phase 4b, with the
      Delta `MERGE` and SCD Type 2 gaps listed above.
- [ ] Kafka as a *sink*. The Kafka integration is source-only.

### Gates to keep honest

- Confirmed: `cargo deny check` reports **licenses ok, bans ok, sources ok**, and raises no
  advisory against `serde_norway` or `unsafe-libyaml-norway`. `advisories` does fail overall, but
  only on pre-existing transitive dependencies of the AWS SDK — `h2` (RUSTSEC-2026-0258), `paste`
  (RUSTSEC-2024-0436), `quick-xml` (RUSTSEC-2026-0194/0195) and `rustls-webpki`
  (RUSTSEC-2026-0098/0099/0104). Nothing to do with the YAML support.
- [ ] Clear those pre-existing advisories, or record explicit `deny.toml` exceptions with a
      rationale, so `cargo deny check` can be a gate rather than a thing everyone ignores.
- [ ] Keep the Spark-SQL parity ratchet green for the batch-write piece (Delta CTAS /
      `INSERT INTO`, the `catalog_bridge.rs` seams — now landed):
      `oxidant-parity ratchet --baseline parity/baseline.json`. That file is 5.4k lines on
      the table-resolution hot path with a TTL cache and Lake Formation credential routing
      layered on it — the widest blast radius in this whole effort.
