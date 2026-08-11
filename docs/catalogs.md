# Catalogs: bring your own metastore

Oxidant resolves table names through Apache DataFusion's catalog API, which already supports
three-part names (`catalog.namespace.table`) and **lazy, asynchronous** table loading. An external
metastore plugs into that path so a query hits the catalog only when it first references one of its
tables — no eager registration of every table.

```
spark.sql.catalog.prod.type = hive          ┐ config (Spark-compatible)
spark.sql.catalog.prod.uri  = thrift://…     ┘
        │
        ▼
CatalogRegistry ── register ─▶ DataFusion catalog bridge ── lazy load_table ─▶ oxidant CatalogProvider
   (oxidant-connect)                (oxidant-loom::catalog_bridge)                     (HiveCatalog / yours)
```

## Configure an external catalog (zero code)

Use Spark's standard catalog-plugin config keys. Set them however you set any Spark conf — at
server start or from the client:

```python
spark.conf.set("spark.sql.catalog.prod.type", "hive")
spark.conf.set("spark.sql.catalog.prod.uri", "thrift://hms.internal:9083")

spark.sql("SELECT count(*) FROM prod.sales.orders").show()   # prod = catalog, sales = database
spark.read.table("prod.sales.orders").filter("amount > 100").show()
spark.catalog.listDatabases()        # lists prod's databases
spark.catalog.listTables("sales")    # lists tables in prod.sales
spark.catalog.tableExists("prod.sales.orders")
```

At server start instead:

```
oxidant spark server --port 50051 \
  --catalog-conf spark.sql.catalog.prod.type=hive \
  --catalog-conf spark.sql.catalog.prod.uri=thrift://hms.internal:9083
# or: OXIDANT_CATALOG_CONF="spark.sql.catalog.prod.type=hive;spark.sql.catalog.prod.uri=thrift://hms:9083"
```

Supported `type` values today: **`hive`** (Hive Metastore over Thrift), **`glue`**
(AWS Glue Data Catalog via `aws-sdk-glue` in-process, with the standard AWS credential
chain — env, shared config, instance role / IRSA), and **`rest`** / `unity` / `iceberg`
(Iceberg REST). Glue options: `region`, optional `warehouse`
(`s3://bucket/prefix` for CTAS without `LOCATION`).

EC2 ASG walkthrough (create Glue DB/table, IAM, `CatalogConf`):
[`distributed-ec2.md`](distributed-ec2.md).

## Bring your own catalog (Rust)

Implement the async [`CatalogProvider`](../crates/oxidant-catalog/src/lib.rs) trait and register it.
The trait is small: list namespaces/tables and resolve one table to a `TableMetadata`
(location + format + optional schema/credentials). The engine turns that metadata into a reader via
its shared Parquet/Delta/Iceberg path.

```rust
use oxidant_catalog::{CatalogProvider, Result, TableFormat, TableMetadata};

#[async_trait::async_trait]
impl CatalogProvider for MyCatalog {
    fn name(&self) -> &str { &self.name }
    async fn list_namespaces(&self, parent: &[String]) -> Result<Vec<Vec<String>>> { /* … */ }
    async fn list_tables(&self, namespace: &[String]) -> Result<Vec<String>> { /* … */ }
    async fn load_table(&self, namespace: &[String], table: &str) -> Result<TableMetadata> {
        Ok(TableMetadata::new("my.ns.t", "s3://bucket/path", TableFormat::Parquet))
    }
}

// engine.register_catalog("my", Arc::new(MyCatalog::new()));
```

`oxidant-catalog-hive` is the reference implementation; mirror its structure for a new provider crate,
then wire its `type` string into `oxidant-connect`'s `build_provider` factory
(`crates/oxidant-connect/src/catalog.rs`).

## What works / what's next (v1)

- **Works:** three-part-qualified queries (`cat.db.tbl`) and `spark.read.table("cat.db.tbl")`
  resolve lazily; `spark.catalog.listCatalogs/listDatabases/listTables/tableExists/databaseExists`
  and `currentCatalog`/`setCurrentCatalog`/`currentDatabase`/`setCurrentDatabase`;
  `refreshTable` (evicts the driver-side cached table provider and invalidates cached stage
  plans — workers converge via `OXIDANT_CATALOG_CACHE_TTL_MS`, see
  [runtime-contract.md](runtime-contract.md)); Hive/Glue tables in
  Parquet, Delta, and Iceberg — on the local filesystem and over `s3://`. The metastore table's
  format is auto-detected from its parameters (`table_type=ICEBERG`, `classification=delta`,
  `spark.sql.sources.provider`, …); Delta is read via delta-kernel and Iceberg via its
  metadata/manifest chain, both with snapshot pinning, over whatever `object_store` the table
  location resolves to (S3 buckets are registered automatically, honoring
  `fs.s3a.*` storage options). Validated end-to-end against AWS Glue + S3 at TPC-H SF10
  (all 8 tables, `count(*)` + Q1/Q6 identical to the Parquet baseline).
- **Not yet:** DDL through the catalog (read-only); `hdfs://` locations; Delta/Iceberg
  **column-mapping** tables (refused with an explicit error rather than misread);
  `USE <catalog>` / current-database affects the `spark.catalog.*` listing context but not yet the
  resolution of *unqualified* table names in queries — use fully-qualified names with external
  catalogs for now; ORC/Avro tables.
