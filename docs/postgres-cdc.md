# Postgres CDC source — design

**Status:** implementation spec. Feature set is benchmarked against ClickHouse
ClickPipes' Postgres CDC connector (PeerDB) — the current best-in-class
managed Postgres CDC product — mapped row by row in §7.

**Goal:** a pipeline declares a `postgres_cdc` source; the engine snapshots
the source tables and then streams committed changes (insert/update/delete)
from the WAL into lakehouse (Delta) tables, merging by primary key (SCD
Type 1) through the existing AUTO CDC machinery. No Kafka, no Debezium, no
JVM — the engine speaks the Postgres wire protocol directly, consistent with
the platform's air-gap story.

```yaml
tables:
  - name: sales_suppliers
    source:
      format: postgres_cdc
      options:
        host: db.internal
        port: "5432"
        database: sales
        user: oxidant_cdc
        password_env: OXIDANT_PGPASSWORD        # never a literal secret in YAML
        tls: verify-full                         # disable | require | verify-ca | verify-full
        tls_ca: /etc/oxidant/pg-ca.pem           # optional CA bundle
        publication: oxidant_sales               # created if missing
        slot: oxidant_sales_suppliers            # created if missing
        tables: public.sales_suppliers, public.sales_customers
        publish_ops: insert,update,delete        # drop `delete` for append-only history
    auto_cdc:
      source: sales_suppliers_changes            # the change stream (below)
      keys: [supplierID]
      sequence_by: __oxidant_lsn
      apply_as_deletes: __oxidant_op = 'd'
```

## 1. Architecture

Two components, both in `crates/oxidant-streaming`:

- **`pg_replication.rs`** — a minimal Postgres logical-replication client over
  `tokio-postgres`: identifies, creates/reuses a replication slot, starts
  replication, decodes the `pgoutput` (v1) message stream, and sends standby
  status updates (feedback) only when told the batch is durable. Hand-rolled
  pgoutput decode (the message set is small: Relation, Insert, Update, Delete,
  Begin, Commit, Type, Truncate, Origin, Keepalive) — no new heavy dependency.
- **`postgres_cdc.rs`** — a `Source` trait implementation (the same trait the
  Kafka and file sources implement, `source.rs`):

| `Source` method | Postgres CDC behaviour |
|---|---|
| `plan_batch` | Read WAL up to the server's current flush LSN (bounded by `max_batch_bytes`); record `[start_lsn, end_lsn)`. Empty range when caught up. Consumes nothing. |
| `poll_range` | Re-read exactly `[start_lsn, end_lsn)` from the slot's retained WAL and emit the change events in order. Deterministic because the slot's `confirmed_flush_lsn` is only advanced on commit (below), so everything in the range is still retained. |
| `committed_offsets` | The end LSN of the last durable batch; persisted in the query checkpoint like any source offsets. |
| `restore_offsets` | Resume the slot from the checkpointed LSN. |
| feedback | Sent to Postgres **only after the sink commits** — this is what makes replay sound: unconfirmed WAL stays in the slot, so a crashed batch's range is still readable. |

### The snapshot ⇄ stream handoff

`CREATE_REPLICATION_SLOT ... SNAPSHOT 'use'` (init message `USE_SNAPSHOT`)
gives a transactionally consistent point: the snapshot sees the tables as of
the slot's `consistent_point` LSN, and the slot streams everything after it.
Sequence: create slot with `USE_SNAPSHOT` → `COPY (SELECT …) TO STDOUT` each
source table in the snapshot transaction → close snapshot → start streaming
from `consistent_point`. Snapshot rows are emitted as `__oxidant_op='s'`
(snapshot) records so AUTO CDC merges them like upserts.

### Emitted schema

Source columns, in order, plus three metadata columns:

| column | type | meaning |
|---|---|---|
| (source columns) | mapped per §3 | the row's new image (insert/update/snapshot) or old image (delete, per REPLICA IDENTITY) |
| `__oxidant_op` | Utf8 | `'s'` snapshot, `'i'` insert, `'u'` update, `'d'` delete, `'t'` truncate |
| `__oxidant_lsn` | Int64 | WAL LSN of the change (monotone — AUTO CDC `sequence_by`) |
| `__oxidant_ts` | Timestamp(us) | commit timestamp from the Commit message |

Delete rows carry NULL in non-key columns unless the table has
`REPLICA IDENTITY FULL` (see §5 setup validation — warned, not required).

## 2. YAML surface

`SourceConfig.format: "postgres_cdc"` with the options shown above. A
pipeline-local **changes table** is implicit: the `auto_cdc.source` name maps
to this source's stream (today AUTO CDC sources are streaming tables in the
same DAG; the postgres_cdc source declares one directly). Multiple PG tables
in one source union their changes into one stream **only if they share a
schema** — otherwise one source per source table (v1 keeps it one-to-one:
one `postgres_cdc` source = one source table; multi-table comes with
per-table `tables:` entries in v2, matching how ClickPipes lets you add
tables to a pipe).

Per-table knobs (v1, as source options):
`exclude_columns: a,b,c`, `rename: lake_name`, `keys: supplierID` (defaults
to the source table's PK), `partition_by` (existing pipeline-level field —
ClickPipes' custom PARTITION BY analog).

## 3. Type mapping (Postgres → Arrow)

bool→Boolean; int2/int4→Int32; int8/oid/xid8→Int64; float4/float8→Float32/64;
numeric→Decimal128(38, s) (source scale; arbitrary-precision numeric falls
back to Utf8 with a logged warning); text/varchar/name/char/bpchar/enum/uuid/
json/jsonb/xml/inet/cidr/macaddr→Utf8 (json/jsonb as raw JSON text);
bytea→Binary; date→Date32; time→Time64(us); timestamp→Timestamp(us, None);
timestamptz→Timestamp(us, "UTC"); interval→Utf8 (v1); arrays→Utf8
(text form, v1); unknown OIDs→Utf8 text form. Everything decodes from
pgoutput's text representation except bytea (`\x` hex).

## 4. Reconciliation (manual + scheduled)

Drift happens: slot dropped, WAL recycled past `restart_lsn`, source restored
from backup, a bug. Detection and repair are first-class:

- **`oxidant pipeline reconcile -c oxidant.yaml [--table t] [--repair]`** —
  for every postgres_cdc table: compare the source's `count(*)` and a
  key-ordered aggregate hash (`md5(string_agg(md5(row::text), '' ORDER BY
  key))` on the source, the same folded hash over the Delta target via the
  engine) and report `in_sync | row_count_drift | hash_drift`. `--repair`
  re-snapshots the table (drop + fresh `USE_SNAPSHOT` load, resuming the
  stream from the new consistent point) — the ClickPipes "table resync"
  analog.
- **Auto**: optional `reconcile:` block on the pipeline —
  ```yaml
  reconcile:
    cron: "0 */6 * * *"     # engine-side scheduler, same cron parser as triggers
    mode: check             # check | repair
  ```
  `check` logs and emits a metric/event; `repair` additionally re-snapshots
  drifting tables. Runs between micro-batch triggers so it never interleaves
  a batch.
- **WAL-growth self-defense**: if the slot's retained WAL (from
  `pg_replication_slots`) passes `max_slot_bytes` (default 10 GiB, option),
  the source pauses with a loud error rather than letting the slot fill the
  source's disk — the number-one operational incident of logical replication.

## 5. Setup validation (actionable errors, not silent wrongness)

At source construction, verify and fail with the exact remediation SQL:

- `wal_level = logical` (RDS: `rds.logical_replication=1`).
- The user can `CREATE_REPLICATION_SLOT` (needs `REPLICATION` role /
  `rds_replication`) and `SELECT` the tables.
- Publication covers the tables (create if missing and permitted:
  `CREATE PUBLICATION … FOR TABLE …`; `FOR TABLES IN SCHEMA` when the
  config names a whole schema; **never** auto-create `FOR ALL TABLES` —
  same guidance as ClickPipes).
- Each table has a PK or `REPLICA IDENTITY FULL`/`USING INDEX` — required
  for updates/deletes to be identifiable; the error names the table and the
  `ALTER TABLE … REPLICA IDENTITY FULL` fix.
- Version ≥ 12 (pgoutput v1 exists).
- **No proxies**: refuse known pooler ports only via documentation — a
  PgBouncer/RDS-Proxy endpoint cannot hold a replication session; the setup
  doc says to point at the real database (ClickPipes documents the same).

## 6. Connector logs & platform visibility

Every connector emits a structured JSONL log at
`<pipeline.checkpoints>/logs/<source-name>.jsonl` — one event per line:
`snapshot_start/done` (rows, duration), `batch` (range, rows, bytes,
duration), `commit` (LSN confirmed), `schema_change`, `reconcile` (verdict),
`error` (message, will_retry), `slot_metrics` (retained bytes, lag vs server
flush LSN). Rotation by size (10 MiB × 5) — this is an operator log, not an
audit log.

The driver surfaces them so the **Oxidant Platform console** can render a
connector detail page without shell access: extend the ui-server with
`GET /api/v1/pipelines/{name}/logs?tail=N` (token-guarded like `/api/status`)
returning the parsed JSONL tail, and fold per-connector lag/rows/slot-bytes
into `/api/status` so a list page can show health at a glance.

## 7. ClickPipes parity map

| ClickPipes (PeerDB) feature | Oxidant postgres_cdc |
|---|---|
| Logical replication, committed txns only | ✅ pgoutput v1, committed-only by construction |
| Initial snapshot | ✅ `USE_SNAPSHOT` consistent snapshot |
| Parallel snapshotting (threads/rows-per-partition/tables-in-parallel) | v2 — v1 is one micro-batch per table, chunked by `snapshot_batch_rows` |
| Table select / rename / exclude columns | ✅ v1 |
| Custom ordering keys | ✅ `keys:` (defaults to PK) |
| Custom destination PARTITION BY | ✅ existing `partition_by` |
| Schema changes: ADD COLUMN auto-propagate (with defaults) | ⚠️ v1: detected, logged (`schema_change`), propagated on the next pipeline **restart** — see §10 |
| DROP COLUMN detected, NULL-filled, not dropped | ✅ same behaviour |
| TOAST columns (unchanged-TOAST marker) | ⚠️ v1: emitted as NULL; `ignore_null_updates_*` on the AUTO CDC target keeps the target's value — see §10 |
| Partitioned source tables | ✅ publish on the parent; PK/RI required on parent + partitions |
| PK or REPLICA IDENTITY requirement | ✅ validated at setup with remediation SQL |
| Deletes | ✅ physical delete in SCD1 (AUTO CDC parity); `publish_ops` excludes them for append-only |
| Soft-delete/version-column model (`_peerdb_*`) | n/a — SCD1 merge instead of RMT dedup; no query-layer dedup needed |
| Sync interval / pull batch size | ✅ pipeline `trigger:` + `max_batch_bytes` |
| Slot size monitoring + alerts | ✅ slot metrics in logs + `/api/status`; `max_slot_bytes` self-defense |
| Add tables to a running pipe | v2 (v1: add to YAML, restart pipeline — state resumes from checkpoints) |
| Table resync | ✅ `pipeline reconcile --repair` (also cron) |
| TLS modes + custom CA | ✅ v1 |
| SSH tunnel / PrivateLink / static IPs | n/a — engine runs in the customer's VPC beside the database |
| RDS IAM auth | v2 |
| Multi-source instances | ✅ multiple pipelines/connectors |
| OpenAPI/Terraform | engine is config-as-code by construction (oxidant.yaml) |
| PG 12+ | ✅ (pgoutput v1) |
| PgBouncer/pooler unsupported | ✅ documented |

## 8. Testing

- **Unit**: pgoutput decoder against recorded WAL message bytes (a captured
  fixture of Begin/Relation/Insert/Update/Delete/Commit covering every §3
  type); type-mapping table tests; snapshot-handoff sequence test against a
  fake wire; reconcile-hash equivalence test.
- **Integration** (`#[ignore]` unless `OXIDANT_PG_TEST_DSN` is set): real
  Postgres — snapshot, insert/update/delete, truncate, additive schema
  change, kill-and-resume mid-batch (replayed range must reproduce),
  reconcile drift detection + repair. CI gets a service container later; the
  gate for this PR is the local e2e below.
- **E2E (this machine)**: Homebrew postgresql@17, `wal_level=logical` scratch
  cluster, `oxidant pipeline run` against `examples/postgres-cdc.yaml`,
  mutations applied live, Delta target verified with `oxidant sql`.

## 9. Explicit non-goals (v1)

Parallel snapshotting, MySQL, multi-table single source, SSH tunnels, RDS
IAM auth, DDL beyond additive columns, exactly-once *schema* evolution of
partition layouts.

## 10. v1 implementation notes (and where it departs from the sections above)

Everything in §1–§6 is implemented except where noted here. Each departure is a
place the design above described the destination and v1 stops short of it; none
of them is silent.

**The replication socket is hand-rolled.** `tokio-postgres` is the control
connection — validation, catalog introspection and slot metrics are ordinary
SQL — but no published version can open a replication session: 0.7.18 has no way
to put `replication=database` in the startup packet (it is a startup parameter,
not a GUC, so `options=-c …` cannot carry it) and no CopyBoth duplex. So
`pg_replication.rs` opens that one socket itself on top of `postgres-protocol`,
the message-framing and SCRAM crate `tokio-postgres` is already built on. That
crate's own backend parser is not usable either — it rejects `CopyBothResponse`
outright — so the frame reader is here too. No replication framework crate was
added, and the pgoutput decode is hand-rolled as specified.

**The snapshot reads through a server-side cursor, not `COPY … TO STDOUT`.** The
handoff is exactly as §1 describes — `CREATE_REPLICATION_SLOT … USE_SNAPSHOT`
inside a `REPEATABLE READ` transaction on the replication session, the tables read
in that transaction, then `START_REPLICATION` from the `consistent_point`. Each
table is read as `DECLARE … NO SCROLL CURSOR FOR SELECT …` followed by
`FETCH FORWARD snapshot_batch_rows`, over the **simple query protocol** rather
than through `COPY`, because a walsender parses `Query` messages and
`COPY TO STDOUT` needs a copy-out handshake the simple protocol does not carry.
The two produce the same values in the same text encoding, and the text form is
what the pgoutput decoder reads anyway, so the snapshot and the stream share one
conversion table.

**The snapshot is one micro-batch per source table.** `snapshot_batch_rows`
bounds the *read* — one `FETCH` and one Arrow batch per slice — but not the
**commit**: `Source::poll_range` hands the scheduler one micro-batch, so a source
table's Arrow batches are all in memory when it returns.

> **Memory requirement.** Plan for the largest source table's Arrow form to fit
> in the engine's heap, with headroom. The cursor keeps the Postgres text form
> down to one slice at a time — before it, a 50M-row table was materialised whole
> as one `String` per cell *and* converted to Arrow, which was an OOM at pipeline
> start rather than a slow snapshot — but the Arrow side is still whole-table.
> Splitting the snapshot across commits is v2; it needs a snapshot batch to be
> re-readable across a process restart, and a snapshot transaction cannot outlive
> the process.

That transaction is committed by `mark_durable`, not at the end of the last read,
so a snapshot batch whose sink write fails can be polled again — the
`Source::poll_range` determinism contract, and the difference between a retryable
blip and a permanently wedged query. An interrupted snapshot restarts from the
first table (logged as `snapshot_start` with a reason) against a freshly created
slot; because the scheduler replays a *recorded* range rather than replanning,
each snapshot range carries the `consistent_point` it was planned against, and a
range whose point is not the one the source would read against does not count
toward the pass. Re-emitting a table already loaded costs nothing but time —
snapshot rows are upserts merged by key. Parallel snapshotting (§7) is v2.

**One transaction is buffered whole, whatever `max_batch_bytes` says.** A
micro-batch must not end inside a transaction — the range would not be a commit
boundary and a replay could not reproduce it — so the byte budget is only
consulted *between* transactions. A single bulk `UPDATE`, a migration or a
nightly reload is therefore held in memory in full before any of it is written.
The source logs a `large_transaction` event and a line to stderr once the
buffered changes pass 1 GiB, naming the size and the remedy (smaller transactions
on the publisher), so this is discovered before the heap runs out rather than
after.

**`ADD COLUMN` propagates on restart, not mid-stream.** The publisher re-sends a
`Relation` message when a table's shape changes; v1 records it, writes a
`schema_change` event to the connector log and a line to stderr, and keeps
ingesting on the schema the streaming DataFrame was planned against — a column
the source has never seen is dropped, and one the publisher no longer sends is
NULL-filled (the `DROP COLUMN` behaviour §7 promises). Restarting the pipeline
re-introspects and picks the new column up. Propagating it *without* a restart
means evolving the micro-batch schema, the merge's projection and the Delta
target together mid-query, which is a change to the streaming engine rather than
to this connector.

**Unchanged TOAST is emitted as NULL.** The decoder keeps the distinction (`u`
is not `n` on the wire), but the emitted batch has one way to say "no value".
Postgres does not send the old value, and re-reading it from the source would
make `poll_range` non-deterministic — which is the property the whole replay
contract rests on. AUTO CDC already has the setting that turns NULL into "leave
the target alone": put the key columns in `ignore_null_updates_except` on the
merge target, as `examples/postgres-cdc.yaml` does. Note the trade: with it set,
an UPDATE that genuinely sets a column to NULL is also ignored. Without a
`REPLICA IDENTITY FULL` table there is no way to tell the two apart, and that is
what `REPLICA IDENTITY FULL` buys.

**A snapshot row's `__oxidant_ts` is NULL.** It has no commit, so it has no
commit timestamp. Its `__oxidant_lsn` is one byte *below* the slot's
`consistent_point`, so every change to the same key arrives with a strictly
larger value and wins — which is what `sequence_by: __oxidant_lsn` needs. Below
rather than on: the stream starts at the consistent point, and on a quiet server
the next WAL record begins exactly there, so a snapshot row stamped on it ties
with the first change after it and AUTO CDC's strict `>` discards the change.

**An UPDATE that moves the row's identity emits two rows.** pgoutput writes the
old image on an UPDATE only when a replica-identity column changed, so a `'d'`
carrying the old key goes out ahead of the `'u'` for the new one, at the same
LSN. They name different keys, so the tie cannot make them race. Without it AUTO
CDC would insert the new key and leave the old row in the target forever. Under
`REPLICA IDENTITY FULL` there is no such signal — the whole old row arrives on
every update — so the identity columns are compared instead.

**`__oxidant_lsn` is the change's own WAL position**, not its transaction's, so
two changes to one key inside one transaction still order against each other.

**Multiple tables in one source must share a schema.** §2 says so; v1 enforces
it at introspection with an error naming both tables. `tables: schema.*` expands
to every ordinary table and every partitioned **parent** in the schema —
`AND NOT c.relispartition`, so leaf partitions are not counted a second time —
and creates the publication as `FOR TABLES IN SCHEMA` (PG 15+); a whole schema
whose tables differ therefore fails the same check.

**The partitioned parent is the replication unit.** Naming the parent (or letting
`schema.*` resolve it) snapshots the whole table once and publishes on the
parent, which is what §7's parity row means. Leaf partitions are deliberately not
resolved: a partition shares its parent's columns, so nothing would complain, and
the snapshot would read the parent — which returns every row across every
partition — and then read each partition again, emitting every row twice at 2x
the memory and 2x the time. With `publish_via_partition_root` at its default
`false` a *change* arrives attributed to the partition, and is matched to the
parent by the publication rather than by a second `tables:` entry.

**Reconciliation (§4) is not in this PR** beyond the WAL-growth self-defense,
which is: `max_slot_bytes` (default 10 GiB) is checked on every plan, logged as
`slot_metrics`, and stops the pipeline with the `pg_drop_replication_slot` SQL
when it is exceeded. The `reconcile` subcommand and its cron are PR2.

**The ui-server endpoint (§6) is not in this PR.** The connector log is written
in the documented place and format; serving it is a separate change.

### Runtime knobs

| Knob | Default | What it does |
|---|---|---|
| `OXIDANT_PG_CDC_IDLE_MS` | `500` | How long planning waits for more WAL before deciding the publisher is caught up. Only paid when the batch has not yet reached the flush LSN it was aiming at, so an idle stream does not pay it. |
| `OXIDANT_PG_CDC_STATUS_MS` | `20000` | How often an otherwise silent source sends a standby status update. Postgres' walsender asks for a reply after `wal_sender_timeout / 2` and hangs up at `wal_sender_timeout` (60 s by default), so a pipeline over a quiet table has to speak. The message carries the position already committed, so it moves the slot nowhere. A `trigger:` longer than `wal_sender_timeout` will still lose the socket between triggers; the source re-dials and re-issues `START_REPLICATION` from the checkpointed LSN, so it costs a handshake, not the pipeline. |

### AUTO CDC wiring

A `postgres_cdc` source *is* a change stream, so the changes table needs no
second entry in `tables:`. Naming it `<table>_changes` in `auto_cdc.source` asks
for the implicit stream; any other unmatched name is still an error, so a typo
cannot turn into a merge over nothing. Declaring a real `<table>_changes` table
also works and keeps the existing mirroring rules (identical `source:`, `sql:`,
`expect:`, `dedup_columns:`).
