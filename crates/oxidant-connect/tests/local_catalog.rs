//! The `local` catalog resolving real tables through the engine.
//!
//! Covers the claim the config file makes: point a catalog at directories of data files and
//! query them by name, in every format Oxidant reads, with no metastore and no AWS. The
//! fixtures are the committed `sample-data/` tree, which carries the same TPC-H tables as
//! Parquet, CSV, Delta, and Iceberg — so the counts must agree across all four, which is a
//! much stronger check than each one merely parsing.

use std::collections::HashMap;

use oxidant_connect::OxidantService;

/// Repo-root-relative path to the committed sample tables.
fn sample_data() -> std::path::PathBuf {
    // `CARGO_MANIFEST_DIR` is `crates/oxidant-connect`.
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../sample-data")
        .canonicalize()
        .expect("sample-data tree is committed at the repo root")
}

/// Flat `spark.sql.catalog.*` config for a local catalog over the sample tree.
///
/// Deliberately built as the flat map rather than through `oxidant-config`: this is the
/// interface the engine actually consumes, and a catalog declared by `--catalog-conf` must
/// behave identically to one declared in YAML.
fn local_catalog_conf(warehouse: &std::path::Path) -> HashMap<String, String> {
    let root = sample_data();
    let tables = serde_json::json!({
        "samples.nation_parquet": {
            "format": "parquet",
            "location": root.join("parquet/tpch_nation.parquet").to_string_lossy(),
        },
        "samples.nation_csv": {
            "format": "csv",
            "location": root.join("csv/tpch_nation.csv").to_string_lossy(),
            "options": { "header": "true" },
        },
        "samples.nation_delta": {
            "format": "delta",
            "location": root.join("delta/tpch_nation").to_string_lossy(),
        },
        "samples.nation_iceberg": {
            "format": "iceberg",
            "location": root.join("iceberg/tpch_nation").to_string_lossy(),
        },
    });
    HashMap::from([
        (
            "spark.sql.catalog.local.type".to_string(),
            "local".to_string(),
        ),
        (
            "spark.sql.catalog.local.warehouse".to_string(),
            warehouse.to_string_lossy().to_string(),
        ),
        (
            "spark.sql.catalog.local.tables".to_string(),
            tables.to_string(),
        ),
    ])
}

/// Run one scalar-count query and return the count.
async fn count(engine: &oxidant_loom::Engine, sql: &str) -> i64 {
    let batches = engine
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));
    let batch = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .unwrap_or_else(|| panic!("no rows returned by: {sql}"));
    let column = batch.column(0);
    let values = column
        .as_any()
        .downcast_ref::<oxidant_loom::arrow::array::Int64Array>()
        .unwrap_or_else(|| panic!("count column is not Int64 for: {sql}"));
    values.value(0)
}

#[tokio::test]
async fn a_config_declared_local_catalog_reads_all_four_formats() {
    let warehouse = tempfile::tempdir().expect("tempdir");
    let service = OxidantService::with_catalogs(local_catalog_conf(warehouse.path())).await;
    let engine = service.engine();

    let parquet = count(&engine, "SELECT count(*) FROM local.samples.nation_parquet").await;
    assert!(parquet > 0, "the parquet fixture should not be empty");

    for table in ["nation_csv", "nation_delta", "nation_iceberg"] {
        let n = count(
            &engine,
            &format!("SELECT count(*) FROM local.samples.{table}"),
        )
        .await;
        assert_eq!(
            n, parquet,
            "`{table}` holds the same TPC-H table as the parquet copy, so the counts must agree"
        );
    }
}

#[tokio::test]
async fn a_local_catalog_table_is_queryable_not_merely_countable() {
    // A count can pass while every column reads back null — the exact failure mode an Iceberg
    // table without a field-id name mapping has. Project and filter real columns instead.
    let warehouse = tempfile::tempdir().expect("tempdir");
    let service = OxidantService::with_catalogs(local_catalog_conf(warehouse.path())).await;
    let engine = service.engine();

    for table in ["nation_parquet", "nation_delta", "nation_iceberg"] {
        let n = count(
            &engine,
            &format!(
                "SELECT count(*) FROM local.samples.{table} WHERE n_name IS NOT NULL \
                 AND n_nationkey >= 0"
            ),
        )
        .await;
        assert!(
            n > 0,
            "`{table}` returned no non-null rows — the table resolved but its columns did not"
        );
    }
}

#[tokio::test]
async fn discovery_registers_the_sample_tree_without_declaring_each_table() {
    // `sample-data/` uses both layouts this has to handle: `parquet/` is a directory of files
    // (one table each), `delta/` is a directory of table directories.
    let warehouse = tempfile::tempdir().expect("tempdir");
    let root = sample_data();
    let discover = serde_json::json!([
        { "namespace": "files", "path": root.join("parquet").to_string_lossy() },
        { "namespace": "dirs", "path": root.join("delta").to_string_lossy() },
    ]);
    let conf = HashMap::from([
        (
            "spark.sql.catalog.local.type".to_string(),
            "local".to_string(),
        ),
        (
            "spark.sql.catalog.local.warehouse".to_string(),
            warehouse.path().to_string_lossy().to_string(),
        ),
        (
            "spark.sql.catalog.local.discover".to_string(),
            discover.to_string(),
        ),
    ]);
    let service = OxidantService::with_catalogs(conf).await;
    let engine = service.engine();

    let from_files = count(&engine, "SELECT count(*) FROM local.files.tpch_nation").await;
    let from_dirs = count(&engine, "SELECT count(*) FROM local.dirs.tpch_nation").await;
    assert!(from_files > 0, "the table-per-file layout found nothing");
    assert_eq!(
        from_files, from_dirs,
        "the same table discovered through both layouts must have the same row count"
    );
}

#[tokio::test]
async fn a_local_catalog_creates_a_database_and_table_the_way_a_pipeline_sink_needs() {
    // This is the capability the whole local-catalog crate exists for: before it, only Glue
    // implemented the write DDL `LakeSink::open` calls, so a live table could only ever be
    // materialized into AWS.
    use oxidant_catalog::TableFormat;
    use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    let warehouse = tempfile::tempdir().expect("tempdir");
    let service = OxidantService::with_catalogs(local_catalog_conf(warehouse.path())).await;
    let catalog = service
        .engine()
        .external_catalog("local")
        .expect("the local catalog is registered");

    catalog
        .create_database("live", true, Some("streaming".into()), None)
        .await
        .expect("create_database");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("amount", DataType::Int64, true),
    ]));
    let created = catalog
        .create_table(
            &["live".into()],
            "orders",
            schema,
            TableFormat::Delta,
            None,
            &[],
        )
        .await
        .expect("create_table");
    assert!(
        created
            .location
            .starts_with(&warehouse.path().to_string_lossy().to_string()),
        "a created table must land under the warehouse, got {}",
        created.location
    );

    // And it must be visible to a *separate* reader of the same warehouse — the manifest is
    // the shared state, so a fresh catalog instance has to see it.
    let reader = OxidantService::with_catalogs(local_catalog_conf(warehouse.path())).await;
    let loaded = reader
        .engine()
        .external_catalog("local")
        .expect("registered")
        .load_table(&["live".into()], "orders")
        .await
        .expect("a table created by one instance is visible to another");
    assert_eq!(loaded.format, TableFormat::Delta);
}
