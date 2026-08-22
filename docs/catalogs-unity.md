# Unity Catalog

Oxidant reaches Unity Catalog through its **Iceberg REST** surface — the same provider that
backs `type = rest` / `type = iceberg` (`crates/oxidant-catalog-rest`). `type = unity` is an
alias for it, not a separate client: there is no code path that speaks Unity Catalog's *native*
`/api/2.1/unity-catalog/{catalogs,schemas,tables}` API.

Everything below was verified against a real **Unity Catalog OSS 0.6.0** server
(`docker run unitycatalog/unitycatalog:latest`). Where something does not work, the actual
failing output is quoted rather than paraphrased.

## Configuration

Catalog options are the flat `spark.sql.catalog.<name>.*` namespace, so the same keys work from
`--catalog-conf`, `OXIDANT_CATALOG_CONF`, a PySpark client's `spark.conf.set`, and the
`catalogs:` block of `oxidant.yaml` — see [config.md](config.md) and [catalogs.md](catalogs.md).

| Key | Required | Meaning |
|---|---|---|
| `spark.sql.catalog.<name>.type` | yes | `unity` (alias of `rest` / `iceberg`) |
| `spark.sql.catalog.<name>.uri` | yes | Base of the Iceberg REST API — **not** the native UC API. For UC OSS: `http://<host>:<port>/api/2.1/unity-catalog/iceberg` |
| `spark.sql.catalog.<name>.warehouse` | for UC | The UC **catalog** name (e.g. `unity`). Used to discover the REST *prefix* — see below |
| `spark.sql.catalog.<name>.prefix` | no | Sets the REST prefix explicitly, skipping discovery. Escape hatch when a server does not implement `/v1/config` |
| `spark.sql.catalog.<name>.token` | for hosted UC | Bearer token sent as `Authorization: Bearer <token>` on every request |
| `spark.sql.defaultCatalog` | no | The catalog unqualified names resolve against |
| `spark.sql.defaultDatabase` | no | The namespace unqualified names resolve against |

### The `warehouse` key is not optional in practice

Unity Catalog serves every Iceberg REST resource under a **prefix**, which the client is
expected to discover. Asking for the config without a warehouse is an error, and the unprefixed
resource paths do not exist:

```console
$ curl -s http://localhost:18080/api/2.1/unity-catalog/iceberg/v1/config
{"error":{"message":"Must supply a proper catalog in warehouse property.","type":"BadRequestException","code":400}}

$ curl -s -o /dev/null -w '%{http_code}\n' \
    http://localhost:18080/api/2.1/unity-catalog/iceberg/v1/namespaces
404
```

With a warehouse, the server names the prefix it requires:

```console
$ curl -s 'http://localhost:18080/api/2.1/unity-catalog/iceberg/v1/config?warehouse=unity'
{"defaults":{},"overrides":{"prefix":"catalogs/unity"},"endpoints":[...]}

$ curl -s http://localhost:18080/api/2.1/unity-catalog/iceberg/v1/catalogs/unity/namespaces
{"namespaces":[["commerce"],["consumer"],["default"]],"next-page-token":null}
```

Oxidant performs that `GET /v1/config?warehouse=…` once, on first use, and applies
`overrides.prefix` to every later request. Setting `prefix` explicitly skips the round trip. A
catalog with neither key issues exactly the unprefixed requests it always did, so a plain
Iceberg REST server (Polaris, Nessie, Tabular, …) is unaffected.

## Working configuration (UC OSS, no auth)

```console
$ docker run -d --name uc-oss -p 18080:8080 unitycatalog/unitycatalog:latest

$ ./target/debug/oxidant spark server --no-ui --port 50077 \
    --catalog-conf spark.sql.catalog.uc.type=unity \
    --catalog-conf spark.sql.catalog.uc.uri=http://localhost:18080/api/2.1/unity-catalog/iceberg \
    --catalog-conf spark.sql.catalog.uc.warehouse=unity \
    --catalog-conf spark.sql.defaultCatalog=uc \
    --catalog-conf spark.sql.defaultDatabase=default
```

The equivalent `oxidant.yaml`:

```yaml
catalogs:
  uc:
    type: unity
    uri: http://localhost:18080/api/2.1/unity-catalog/iceberg
    warehouse: unity
default_catalog: uc
```

### Verified: metadata resolves, with and without a catalog prefix

Driven by a stock `pyspark-client` 4.2.0 session against the server above:

```
>>> SHOW SCHEMAS IN uc
+-----------+
| namespace |
+-----------+
|  commerce |
|  consumer |
|   default |
+-----------+

>>> SHOW TABLES IN uc.default
+-----------+-------------------+-------------+
| namespace |         tableName | isTemporary |
+-----------+-------------------+-------------+
|   default | marksheet_uniform |       false |
+-----------+-------------------+-------------+
```

`spark.sql.defaultCatalog=uc` makes a **2-part** `default.marksheet_uniform` and a **bare**
`marksheet_uniform` resolve into `uc` — both reach the table and read its schema from Unity
Catalog. (Before this, DataFusion's process-wide default catalog claimed every unqualified name
and external catalogs were reachable only fully qualified; `docs/catalogs.md` still described
that limitation.)

### Verified: an undeclared default catalog is refused at startup

```console
$ ./target/debug/oxidant spark server --no-ui --port 50078 \
    --catalog-conf spark.sql.catalog.uc.type=unity \
    --catalog-conf spark.sql.catalog.uc.uri=http://x \
    --catalog-conf spark.sql.defaultCatalog=ucc
Declared 3 catalog config entrie(s)
oxidant: plan error: spark.sql.defaultCatalog=`ucc` names a catalog that is not declared — add
`spark.sql.catalog.ucc.type=<local|hive|glue|rest|unity|iceberg>` (and its `uri`/`warehouse`) or
drop the setting (declared: uc)
$ echo $?
1
```

Without this the typo is silent: the server boots and every unqualified name goes on resolving
against the builtin `spark_catalog`, which reads as a missing table. Note the check is
startup-only — over the `Config` RPC a client sets one key per call, so a default catalog
legitimately arrives before the catalog it names and is retried instead of refused.

## Limitations found

### Reading table data does not work for UC's sample tables

Listing and schema resolution work; **reading rows does not**, for every table UC OSS exposes
over Iceberg REST today. Against the working configuration above:

```
>>> SELECT * FROM uc.default.marksheet_uniform ORDER BY id LIMIT 5
plan error: Schema error: No field named id. Valid fields are
uc.default.marksheet_uniform."col-b1510e05-9617-48b4-ab74-f870af2c3e2f",
uc.default.marksheet_uniform."col-f95db0ce-61b0-4efa-a036-487777249590",
uc.default.marksheet_uniform."col-349e49ef-9c01-4851-a878-73c9c430077c"

>>> SELECT count(*) AS n FROM marksheet_uniform
execution error: Internal error: Physical input schema should be the same as the one converted
from logical input schema. Differences: - schema metadata differs: (physical) {} vs (logical)
{"org.apache.spark.version": "3.5.1", "org.apache.spark.sql.parquet.row.metadata": "{…
\"delta.columnMapping.physicalName\":\"col-b1510e05-9617-48b4-ab74-f870af2c3e2f\"…"}
```

The cause is **Delta column mapping**, not the Unity Catalog wiring. `marksheet_uniform` is a
Delta UniForm table (`delta.columnMapping.mode=name`,
`delta.universalFormat.enabledFormats=iceberg`), so its Parquet files carry physical
`col-<uuid>` names and the logical names live only in the table metadata. Oxidant does not yet
map them.

Oxidant's **Delta** reader already refuses this case explicitly; its **Iceberg** reader does
not, and leaks the physical names into the query schema instead. Both are reproducible with no
Unity Catalog in the picture, pointing a `local` catalog at the same directory:

```console
$ oxidant sql --config uniform.yaml -e 'SELECT * FROM lc.d.uniform_delta LIMIT 3'
oxidant: plan error: This feature is not implemented: lakehouse column mapping is not yet
supported for `lc.d.uniform_delta` (logical `id` is stored as
`col-b1510e05-9617-48b4-ab74-f870af2c3e2f`); refusing to return null or misnamed columns

$ oxidant sql --config uniform.yaml -e 'SELECT * FROM lc.d.uniform_iceberg LIMIT 3'
oxidant: plan error: Schema error: No field named id. Valid fields are
lc.d.uniform_iceberg."col-b1510e05-9617-48b4-ab74-f870af2c3e2f", …
```

Two separate gaps, neither in this crate:

1. **Column mapping is unimplemented** for both lakehouse readers
   (`crates/oxidant-datasource`). This is what blocks reading UC's tables.
2. **The Iceberg reader misses the refusal** the Delta reader has, so a column-mapped table
   surfaces physical names rather than a clear error. `docs/catalogs.md` claims column-mapping
   tables are "refused with an explicit error rather than misread" — that is true of Delta, not
   of Iceberg.

Because `type = unity` classifies a table as Iceberg whenever its location does not contain
`_delta_log` (which a UC table location never does), UC tables always take the leaking path.

### Only UniForm tables are visible at all

UC OSS's Iceberg REST surface exposes only tables that carry Iceberg metadata. Of the five
sample tables in `unity.default`, four are plain Delta and are invisible to Oxidant:

```console
$ curl -s 'http://localhost:18080/api/2.1/unity-catalog/tables?catalog_name=unity&schema_name=default' \
  | jq -c '.tables[] | {name, table_type, data_source_format}'
{"name":"marksheet","table_type":"MANAGED","data_source_format":"DELTA"}
{"name":"marksheet_uniform","table_type":"EXTERNAL","data_source_format":"DELTA"}
{"name":"mytable","table_type":"EXTERNAL","data_source_format":"DELTA"}
{"name":"numbers","table_type":"EXTERNAL","data_source_format":"DELTA"}
{"name":"user_countries","table_type":"EXTERNAL","data_source_format":"DELTA"}

$ curl -s http://localhost:18080/api/2.1/unity-catalog/iceberg/v1/catalogs/unity/namespaces/default/tables
{"identifiers":[{"namespace":["default"],"name":"marksheet_uniform"}],"next-page-token":null}
```

Reaching plain Delta tables would require a native Unity Catalog API client. There is none.

### `current_catalog()` does not report the default catalog

```
>>> SELECT current_catalog() AS c, current_database() AS d
+---------------+---------+
|             c |       d |
+---------------+---------+
| spark_catalog | default |
+---------------+---------+
```

Name *resolution* honors the default catalog (2-part and bare names resolve into `uc`), but the
`current_catalog()` / `current_database()` SQL functions are not wired to the session's
catalog pointers and still report the builtin. `SHOW`/`DESCRIBE` and `USE` do read the session
state; only these two functions are wrong.

### Not covered

- **Credential vending.** Oxidant does not call UC's `/credentials` endpoint. Only table
  locations reachable with ambient credentials (a local path, or an `s3://` bucket the process
  can already read via the standard AWS chain) work. UC OSS's sample tables are `file://` paths
  **inside the container**, so the same paths must exist on the Oxidant host.
- **Writes.** `catalogs.md` already lists write DDL for REST/Unity as unimplemented; nothing
  here changes that.
- **Multi-level namespaces.** UC's Iceberg REST namespaces are single-segment under the
  per-catalog prefix, which is what Oxidant handles. A server that returns genuinely nested
  namespaces would only have its first segment used.
- **Databricks-hosted Unity Catalog is untested.** The configuration below is derived from the
  key wiring, not from a verified run.

## Databricks-hosted Unity Catalog

Databricks exposes a Unity Catalog Iceberg REST endpoint per workspace. The token is a
workspace personal access token or an OAuth token, and it is sent as a bearer token:

```yaml
catalogs:
  uc:
    type: unity
    uri: https://<workspace>.cloud.databricks.com/api/2.1/unity-catalog/iceberg
    warehouse: <uc-catalog-name>
    token: ${DATABRICKS_TOKEN}
default_catalog: uc
```

Unverified against a real workspace. The same two blockers above apply and are likely worse
there: Databricks UC tables are Delta with column mapping by default, and their storage
locations require credential vending, which Oxidant does not do.

## Verification

| Claim | Test |
|---|---|
| A declared `unity` catalog is a valid default | `oxidant-connect` `catalog::default_catalog_tests::a_declared_unity_catalog_is_a_valid_default` |
| An undeclared default catalog is refused, by name | `…::an_undeclared_default_catalog_is_refused_by_name` |
| `spark_catalog` needs no declaration | `…::the_builtin_catalog_is_always_a_valid_default` |
| `spark.sql.catalog.<n>=<impl>` also declares a catalog | `…::the_bare_implementation_class_spelling_declares_a_catalog` |
| The UC prefix is discovered from `warehouse` | `oxidant-catalog-rest` `prefix_tests::a_warehouse_discovers_the_iceberg_rest_prefix` |
| An explicit `prefix` skips discovery | `…::an_explicit_prefix_is_used_without_discovery` |
| A prefix-less Iceberg REST server is unchanged | `…::a_prefixless_catalog_is_unchanged` |
| Blank `token`/`warehouse`/`prefix` are unset | `…::blank_options_are_treated_as_unset` |
| A default external catalog resolves 2-part names | `oxidant-loom` `tests::a_default_external_catalog_resolves_two_part_names` |
| `defaultDatabase` makes bare names resolve | `…::a_default_namespace_makes_bare_names_resolve` |
| The builtin-catalog path and temp views are unchanged | `…::the_builtin_catalog_path_is_unchanged` |

The end-to-end run against a live `unitycatalog/unitycatalog:latest` container is manual — it is
not in CI and there is no committed harness for it. Reproduce it with the commands in this
document.
