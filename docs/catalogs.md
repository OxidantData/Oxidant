# Catalogs: bring your own metastore

Weft resolves table names through Apache DataFusion's catalog API, which already supports
three-part names (`catalog.namespace.table`) and **lazy, asynchronous** table loading. An external
metastore plugs into that path so a query hits the catalog only when it first references one of its
tables — no eager registration of every table.

```
spark.sql.catalog.prod.type = hive          ┐ config (Spark-compatible)
spark.sql.catalog.prod.uri  = thrift://…     ┘
        │
        ▼
CatalogRegistry ── register ─▶ DataFusion catalog bridge ── lazy load_table ─▶ weft CatalogProvider
   (weft-connect)                (weft-loom::catalog_bridge)                     (HiveCatalog / yours)
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
weft spark server --port 50051 \
  --catalog-conf spark.sql.catalog.prod.type=hive \
  --catalog-conf spark.sql.catalog.prod.uri=thrift://hms.internal:9083
# or: WEFT_CATALOG_CONF="spark.sql.catalog.prod.type=hive;spark.sql.catalog.prod.uri=thrift://hms:9083"
```

Supported `type` values today: **`hive`** (Hive Metastore over Thrift), **`glue`**
(AWS Glue Data Catalog via the AWS CLI + instance role / IRSA), and **`rest`** /
`unity` / `iceberg` (Iceberg REST). Glue options: `region`, optional `warehouse`
(`s3://bucket/prefix` for CTAS without `LOCATION`). The AWS CLI path is taken only
from `WEFT_AWS_BIN` (never from catalog options).

EC2 ASG walkthrough (create Glue DB/table, IAM, `CatalogConf`):
[`distributed-ec2.md`](distributed-ec2.md). K8s/IRSA notes:
[`distributed-k8s.md`](distributed-k8s.md).

## Bring your own catalog (Rust)

Implement the async [`CatalogProvider`](../crates/weft-catalog/src/lib.rs) trait and register it.
The trait is small: list namespaces/tables and resolve one table to a `TableMetadata`
(location + format + optional schema/credentials). The engine turns that metadata into a reader via
its shared Parquet/Delta/Iceberg path.

```rust
use weft_catalog::{CatalogProvider, Result, TableFormat, TableMetadata};

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

`weft-catalog-hive` is the reference implementation; mirror its structure for a new provider crate,
then wire its `type` string into `weft-connect`'s `build_provider` factory
(`crates/weft-connect/src/catalog.rs`).

## What works / what's next (v1)

- **Works:** three-part-qualified queries (`cat.db.tbl`) and `spark.read.table("cat.db.tbl")`
  resolve lazily; `spark.catalog.listCatalogs/listDatabases/listTables/tableExists/databaseExists`
  and `currentCatalog`/`setCurrentCatalog`/`currentDatabase`/`setCurrentDatabase`; Hive tables in
  Parquet, plus Delta/Iceberg when the Hive table points at a local-filesystem location.
- **Not yet:** DDL through the catalog (read-only); remote object stores (`s3://`, `hdfs://`) for
  **Delta/Iceberg** tables (Parquet over remote stores works once an `object_store` is registered);
  `USE <catalog>` / current-database affects the `spark.catalog.*` listing context but not yet the
  resolution of *unqualified* table names in queries — use fully-qualified names with external
  catalogs for now; ORC/Avro tables.
