# `oxidant.yaml` — the configuration file

One file configures the whole binary: which catalogs exist, how the engine is tuned, and what
the declarative pipeline builds. It replaces the three ad-hoc surfaces that came before it —
scattered `OXIDANT_*` environment variables, per-subcommand flags, and `--catalog-conf key=value`
pairs — without removing any of them.

```sh
oxidant sql -c oxidant.yaml -e "SELECT count(*) FROM local.live.orders"
oxidant start -c oxidant.yaml --port 50051
oxidant pipeline run -c oxidant.yaml
```

A complete, runnable example that needs no broker and no AWS:
[`examples/oxidant.yaml`](../examples/oxidant.yaml).

## Where the file comes from

In order, first hit wins:

1. `--config <PATH>` (or `-c <PATH>`)
2. `$OXIDANT_CONFIG`
3. `./oxidant.yaml` in the working directory
4. nothing — every subcommand still works, just with no declared catalogs

An **explicit** path that does not exist is an error. The implicit sources are skipped when
absent. That asymmetry is deliberate: a typo in `--config` must not silently fall through to a
default and run your statement against the wrong catalogs.

Unknown keys are rejected rather than ignored — a misspelled `warehosue` is a startup error
naming the key, not a table silently written somewhere else.

## Paths must be absolute

Every local filesystem path in this file — `warehouse`, a table `location`, a `discover` path,
`pipeline.storage`, `pipeline.checkpoints`, `oxidant.spool.dir` — must be absolute. A relative
one is a startup error naming the key:

```text
error: `catalogs.local.warehouse` must be an absolute path (got `./warehouse`)
```

This is a deliberate refusal to guess. "Relative to what" has two defensible answers — the
process working directory and the config file's own directory — they disagree, and picking one
silently means `oxidant pipeline show` reports a different location than the one your data
actually goes to, depending on where you ran it. The rule is also enforced one level down, in
the catalog itself, so a path arriving through `--catalog-conf`, `OXIDANT_CATALOG_CONF`, or a
PySpark client's `Config` RPC is rejected the same way.

Anything with a URI scheme (`s3://`, `file://`) is already absolute and passes through
untouched.

## `vars:` and `${NAME}` — absolute without being unportable

Hard-coding `/home/you/...` makes a file nobody else can use. Interpolation is how a config stays
portable *and* absolute: the variable supplies the absolute prefix.

```yaml
vars:
  DATA: /srv/oxidant           # a default; the environment overrides it

catalogs:
  local:
    type: local
    warehouse: ${DATA}/warehouse
    tables:
      samples.nation:
        format: delta
        location: ${CONFIG_DIR}/../sample-data/delta/tpch_nation
```

Names resolve in this order:

| Source | Notes |
|---|---|
| **Built-ins** | `${CONFIG_DIR}` — absolute directory holding this file. `${PWD}` — the working directory. Cannot be shadowed. |
| **Environment** | `DATA=/mnt/fast oxidant …` overrides the file, so a container needs no edit |
| **`vars:`** | The checked-in default |

`${CONFIG_DIR}` is what makes a committed example runnable: it means the same thing from any
directory and on any machine. That is exactly how
[`examples/oxidant.yaml`](../examples/oxidant.yaml) is written.

Three things worth knowing:

- **An undefined name is an error, not an empty string.** A silently-empty `${DATA}` would turn
  `${DATA}/warehouse` into `/warehouse` — absolute, so it passes every check, and pointed at the
  root of your disk.
- **Inside a YAML flow mapping (`{ a: b }`), quote the value.** `{` and `}` are structural there,
  so write `{ path: "${DATA}/x" }`. In block style (one key per line) no quoting is needed.
- **A bare `$` is left alone**, so Spark JSON paths like `get_json_object(v, '$.order_id')`
  survive untouched. Write `$${` for a literal `${`.

## `engine:` — tuning

Lowered to the `OXIDANT_*` environment contract the engine reads **once, at construction**.

| Key | Variable |
|---|---|
| `memory_limit_bytes` | `OXIDANT_MEMORY_LIMIT_BYTES` |
| `memory_pool_fraction` | `OXIDANT_MEMORY_POOL_FRACTION` |
| `shuffle_partitions` | `OXIDANT_SHUFFLE_PARTITIONS` |
| `target_partitions` | `OXIDANT_TARGET_PARTITIONS` |
| `batch_size` | `OXIDANT_BATCH_SIZE` |
| `shuffle_spill_bytes` | `OXIDANT_SHUFFLE_SPILL_BYTES` |
| `shuffle_spill_dir` | `OXIDANT_SHUFFLE_SPILL_DIR` |
| `broadcast_join_threshold_bytes` | `OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES` |
| `s3_cache_dir` | `OXIDANT_S3_CACHE_DIR` |
| `s3_cache_max_bytes` | `OXIDANT_S3_CACHE_MAX_BYTES` |
| `workers` (list) | `OXIDANT_WORKERS` (comma-joined) |
| `env` (map) | anything else, spelled verbatim |

Two consequences worth knowing before you rely on this:

- **A variable already set in the environment wins over the file.** `OXIDANT_SHUFFLE_PARTITIONS=400
  oxidant …` overrides `shuffle_partitions: 200`, the same direction a CLI flag beats a config value.
- **These only affect a process this CLI starts.** They cannot retune a server that is already
  running. `engine:` is a typed, validated front-end over the environment contract, not a
  replacement for it.

Leave `memory_limit_bytes` unset for auto-sizing from cgroup/host RAM. Never set it to `0`.
Note it sizes both the DataFusion pool *and* the shuffle cache, so a worker's real ceiling is
above the number you write.

## `catalogs:` — where tables come from

Each entry becomes `spark.sql.catalog.<name>.*`, which is the same flat namespace
`--catalog-conf` and a PySpark client's `Config` RPC use. A catalog declared here behaves
identically to one declared any other way — there is one bootstrap path, not two.

```yaml
catalogs:
  glue:
    type: glue
    region: us-east-1
    warehouse: s3://my-bucket/warehouse
default_catalog: glue
```

| Key | Meaning |
|---|---|
| `type` | `local` · `glue` · `hive` · `rest` / `unity` / `iceberg` |
| `warehouse` | Root new tables are created under (`{warehouse}/{db}.db/{table}/`) |
| `region` | AWS region (Glue). Resolved from the ambient AWS chain when unset |
| `uri` | Metastore endpoint (Hive `thrift://…`, REST `https://…`) |
| `token` | Bearer token for a REST catalog |
| `lakeformation` | Enable Lake Formation enforcement on a Glue catalog |
| `options` | Escape hatch for any other `spark.sql.catalog.<name>.<key>` |

AWS credentials are **not** configured here. They resolve through the standard chain, in the
order the AWS CLI uses: environment variables, web identity (IRSA), the shared profile in
`~/.aws` (including SSO and `credential_process`), the ECS container endpoint, then instance
metadata. So `AWS_PROFILE=myprofile` works on a laptop and an instance role works on EC2 with
nothing extra. A table can still pin its own identity through `options` — see below.

### `type: local` — a catalog over directories

The catalog to reach for when you want to query files without standing up a metastore. It also
has real write DDL, which matters: `local` and `glue` are the only two catalogs that can create
databases and tables, so they are the only two a pipeline can materialize into.

```yaml
vars:
  ROOT: /srv/oxidant

catalogs:
  local:
    type: local
    warehouse: ${ROOT}/warehouse
    tables:
      raw.events:  { format: parquet, location: "${ROOT}/data/events/" }
      raw.orders:  { format: delta,   location: "${ROOT}/data/orders/" }
      raw.clicks:  { format: iceberg, location: s3://bucket/clicks/ }
      raw.lookup:  { format: csv,     location: "${ROOT}/data/lookup.csv", options: { header: "true" } }
      raw.audit:   { format: json,    location: "${ROOT}/data/audit/" }
    discover:
      - { namespace: bronze, path: "${ROOT}/data/bronze" }
```

(The quotes are the YAML flow-mapping rule from above — inside `{ … }`, a `${…}` value must be
quoted. `warehouse:` is block style and needs none.)

`tables:` entries are keyed `namespace.table`. `format` is one of `parquet`, `delta`,
`iceberg`, `csv`, `json` — `orc` and `avro` are rejected with a clear error rather than
mis-read. `options` carries reader settings (CSV `header`, `delimiter`) and storage credentials
(`s3.access-key-id` / `fs.s3a.access.key`, `s3.endpoint`, `fs.s3a.assumed.role.arn`,
`s3.skip-signature`) — the same vocabulary the engine's own S3 registration accepts.

A `discover:` entry may carry its own `options:` for storage credentials; without one it
inherits the catalog's. This matters for an `s3://` root — without credentials reaching it, the
scan falls back to the ambient AWS chain and the whole catalog fails to build on a host that
has none.

`discover:` scans a tree and registers what it recognizes. Two layouts:

- **table per subdirectory** — `bronze/orders/_delta_log/…`
- **table per file** — `parquet/tpch_nation.parquet`, named after the file stem

Format is inferred from what a directory actually contains: a `_delta_log/` means Delta, a
`metadata/` or `*.metadata.json` means Iceberg, otherwise the file extensions decide — at any
depth, so a Hive-partitioned tree (`orders/dt=2026-01-01/part-0.parquet`) is recognized and its
partition columns (`dt`) are picked up from the path. A directory Oxidant cannot identify is
**skipped**, not guessed at — a stray `README.md` should not become a table that fails at query
time.

A table carrying *both* a `_delta_log/` and a `metadata/` — Oxidant's own `iceberg_compat`
output — registers as **Delta**. The Delta log is always current; the published Iceberg tree
trails it by up to `checkpoint_interval` commits, so reading it as Iceberg would silently serve
stale data.

Declared tables and discovered tables are read-only pointers to data the catalog does not own.
`create_table`, `drop_table`, and `alter_table` refuse to touch them — config is your statement
of intent, and a pipeline must not quietly redefine it. Everything the catalog *does* own lives
in a manifest under `{warehouse}/_oxidant_catalog/`, written as a versioned log so two writers
sharing a warehouse conflict-and-retry instead of losing each other's tables.

## `pipeline:` and `tables:` — the declarative DAG

See [`pipelines.md`](pipelines.md).

## What is not configured here

- **Ports and bind addresses** stay on the subcommand (`--port`, `--ui-port`, `--ui-bind`).
  They are per-process, and a config file shared across a cluster should not pin them.
- **Credentials**, as above — the AWS chain owns those.
