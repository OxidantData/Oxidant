//! Batch writes to a catalog table from SQL: `CREATE TABLE … USING delta AS SELECT`,
//! `INSERT INTO`, and `INSERT OVERWRITE`.
//!
//! These run against the `local` catalog on a temp directory, so they exercise the same code an
//! `s3://` Glue table would take — `CatalogProvider::create_table`, the Delta writer, and the
//! transaction log — with no AWS and no metastore.

use std::collections::HashMap;
use std::path::Path;

use oxidant_connect::OxidantService;

/// A local-type catalog named `lake` whose warehouse is `warehouse`, with no pre-declared
/// tables. Named `lake` rather than `local` because most of these statements are `INSERT`s and
/// `local` is a sqlparser keyword there — `insert_works_for_a_catalog_named_local` covers that.
fn conf(warehouse: &Path) -> HashMap<String, String> {
    HashMap::from([
        (
            "spark.sql.catalog.lake.type".to_string(),
            "local".to_string(),
        ),
        (
            "spark.sql.catalog.lake.warehouse".to_string(),
            warehouse.to_string_lossy().to_string(),
        ),
    ])
}

/// Build an engine over a fresh warehouse with the `live` database already created.
async fn engine_with_live_db(warehouse: &Path) -> std::sync::Arc<oxidant_loom::Engine> {
    let service = OxidantService::with_catalogs(conf(warehouse)).await;
    let engine = service.engine();
    engine
        .external_catalog("lake")
        .expect("the local catalog is registered")
        .create_database("live", true, None, None)
        .await
        .expect("create_database");
    engine
}

async fn count(engine: &oxidant_loom::Engine, sql: &str) -> i64 {
    let batches = engine
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));
    let batch = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .unwrap_or_else(|| panic!("no rows returned by: {sql}"));
    batch
        .column(0)
        .as_any()
        .downcast_ref::<oxidant_loom::arrow::array::Int64Array>()
        .unwrap_or_else(|| panic!("count column is not Int64 for: {sql}"))
        .value(0)
}

/// Every `_delta_log/*.json` commit body, oldest first.
fn commits(table_dir: &Path) -> Vec<String> {
    let log = table_dir.join("_delta_log");
    let mut files: Vec<_> = std::fs::read_dir(&log)
        .unwrap_or_else(|e| panic!("no transaction log at {}: {e}", log.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    files.sort();
    files
        .iter()
        .map(|p| std::fs::read_to_string(p).expect("read commit"))
        .collect()
}

#[tokio::test]
async fn ctas_using_delta_writes_a_real_delta_table_into_the_catalog() {
    let warehouse = tempfile::tempdir().expect("tempdir");
    let engine = engine_with_live_db(warehouse.path()).await;

    engine
        .sql(
            "CREATE TABLE lake.live.ctas_orders USING delta AS \
             SELECT 1 AS id, 100 AS amount UNION ALL SELECT 2, 250",
        )
        .await
        .expect("CTAS into the local catalog");

    // A transaction log, not merely a directory of Parquet: this is the difference between a
    // Delta table and a Parquet one that happens to say `delta` in the metastore.
    let table_dir = warehouse.path().join("live.db").join("ctas_orders");
    let log = commits(&table_dir);
    assert_eq!(log.len(), 1, "one commit for one CTAS, got {}", log.len());
    assert!(
        log[0].contains("\"metaData\"") && log[0].contains("\"add\""),
        "the first commit must declare the table and add its files: {}",
        log[0]
    );

    assert_eq!(
        count(&engine, "SELECT count(*) FROM lake.live.ctas_orders").await,
        2
    );
    assert_eq!(
        count(
            &engine,
            "SELECT sum(amount) FROM lake.live.ctas_orders WHERE id > 0"
        )
        .await,
        350,
        "the columns must read back with their values, not merely count"
    );
}

#[tokio::test]
async fn insert_into_a_delta_table_appends_and_is_visible_immediately() {
    let warehouse = tempfile::tempdir().expect("tempdir");
    let engine = engine_with_live_db(warehouse.path()).await;

    engine
        .sql("CREATE TABLE lake.live.appended_orders USING delta AS SELECT 1 AS id, 100 AS amount")
        .await
        .expect("CTAS");
    engine
        .sql("INSERT INTO lake.live.appended_orders SELECT 2 AS id, 250 AS amount")
        .await
        .expect("INSERT INTO");

    // Same session, same engine: the catalog bridge caches a table's resolved file list, so this
    // is the check that an insert is not invisible until the cache expires.
    assert_eq!(
        count(&engine, "SELECT count(*) FROM lake.live.appended_orders").await,
        2
    );
    assert_eq!(
        count(&engine, "SELECT sum(amount) FROM lake.live.appended_orders").await,
        350
    );

    let log = commits(&warehouse.path().join("live.db").join("appended_orders"));
    assert_eq!(log.len(), 2, "CTAS then INSERT is two commits");
    assert!(
        !log[1].contains("\"remove\""),
        "an append must not retire any file: {}",
        log[1]
    );
}

#[tokio::test]
async fn insert_overwrite_replaces_rather_than_accumulates() {
    let warehouse = tempfile::tempdir().expect("tempdir");
    let engine = engine_with_live_db(warehouse.path()).await;

    engine
        .sql(
            "CREATE TABLE lake.live.overwritten_orders USING delta AS \
             SELECT 1 AS id, 100 AS amount UNION ALL SELECT 2, 250",
        )
        .await
        .expect("CTAS");
    engine
        .sql("INSERT OVERWRITE lake.live.overwritten_orders SELECT 9 AS id, 5 AS amount")
        .await
        .expect("INSERT OVERWRITE");

    assert_eq!(
        count(&engine, "SELECT count(*) FROM lake.live.overwritten_orders").await,
        1,
        "an overwrite replaces the table's contents; 3 would mean it appended"
    );
    assert_eq!(
        count(
            &engine,
            "SELECT sum(amount) FROM lake.live.overwritten_orders"
        )
        .await,
        5
    );

    // The old files must be retired in the same commit that adds the new ones — otherwise a
    // reader replaying the log still sees them and the count silently doubles.
    let log = commits(&warehouse.path().join("live.db").join("overwritten_orders"));
    assert_eq!(log.len(), 2);
    assert!(
        log[1].contains("\"remove\"") && log[1].contains("\"add\""),
        "the overwrite commit must carry both removes and adds: {}",
        log[1]
    );
}

#[tokio::test]
async fn ctas_honors_location_and_partitioned_by() {
    let warehouse = tempfile::tempdir().expect("tempdir");
    let elsewhere = tempfile::tempdir().expect("tempdir");
    let engine = engine_with_live_db(warehouse.path()).await;

    let location = elsewhere.path().join("orders_by_day");
    engine
        .sql(&format!(
            "CREATE TABLE lake.live.located_orders USING delta \
             LOCATION '{}' PARTITIONED BY (event_date) AS \
             SELECT 1 AS id, 100 AS amount, '2026-01-01' AS event_date \
             UNION ALL SELECT 2, 250, '2026-01-02'",
            location.display()
        ))
        .await
        .expect("CTAS with LOCATION and PARTITIONED BY");

    assert!(
        location.join("_delta_log").is_dir(),
        "LOCATION was ignored — nothing was written to {}",
        location.display()
    );
    assert!(
        !warehouse
            .path()
            .join("live.db")
            .join("located_orders")
            .exists(),
        "an explicit LOCATION must not also create the default warehouse directory"
    );
    // Hive-style partition directories, which is what makes the partitioning real rather than a
    // column that happens to be listed in the metadata.
    assert!(
        location.join("event_date=2026-01-01").is_dir(),
        "no partition directory was written under {}",
        location.display()
    );
    assert_eq!(
        count(&engine, "SELECT count(*) FROM lake.live.located_orders").await,
        2
    );
    assert_eq!(
        count(
            &engine,
            "SELECT count(*) FROM lake.live.located_orders WHERE event_date = '2026-01-02'"
        )
        .await,
        1,
        "the partition column must read back from the path"
    );
}

#[tokio::test]
async fn a_clause_that_would_be_silently_dropped_is_refused() {
    let warehouse = tempfile::tempdir().expect("tempdir");
    let engine = engine_with_live_db(warehouse.path()).await;

    // `OPTIONS` has nowhere to go in `CatalogProvider::create_table`, and for CSV it decides how
    // the table is even read. Accepting and discarding it would produce a table that does not
    // match its own DDL.
    let err = engine
        .sql(
            "CREATE TABLE lake.live.refused_orders USING delta OPTIONS ('mergeSchema' 'true') AS \
             SELECT 1 AS id",
        )
        .await
        .expect_err("a dropped clause must not pass silently");
    assert!(
        err.to_string().contains("OPTIONS"),
        "the error should name the clause it cannot carry: {err}"
    );
}

#[tokio::test]
async fn iceberg_is_refused_as_a_write_target_with_a_reason() {
    let warehouse = tempfile::tempdir().expect("tempdir");
    let engine = engine_with_live_db(warehouse.path()).await;

    let err = engine
        .sql("CREATE TABLE lake.live.iceberg_orders USING iceberg AS SELECT 1 AS id")
        .await
        .expect_err("Iceberg is publish-only");
    assert!(
        err.to_string().contains("icebergCompat"),
        "the refusal should point at the supported way to get an Iceberg-readable table: {err}"
    );
}

#[tokio::test]
async fn insert_works_for_a_catalog_named_local() {
    // sqlparser consumes `LOCAL` as Hive's `INSERT OVERWRITE LOCAL DIRECTORY` keyword, so
    // `INSERT INTO local.live.t` failed at parse — for the exact catalog name `docs/config.md`'s
    // example config uses. Every other test here names its catalog `lake`, so without this one
    // the whole suite would pass while the documented setup did not work.
    let warehouse = tempfile::tempdir().expect("tempdir");
    let conf = HashMap::from([
        (
            "spark.sql.catalog.local.type".to_string(),
            "local".to_string(),
        ),
        (
            "spark.sql.catalog.local.warehouse".to_string(),
            warehouse.path().to_string_lossy().to_string(),
        ),
    ]);
    let engine = OxidantService::with_catalogs(conf).await.engine();
    engine
        .external_catalog("local")
        .expect("the local catalog is registered")
        .create_database("live", true, None, None)
        .await
        .expect("create_database");

    engine
        .sql("CREATE TABLE local.live.reserved USING delta AS SELECT 1 AS id")
        .await
        .expect("CTAS");
    engine
        .sql("INSERT INTO local.live.reserved SELECT 2 AS id")
        .await
        .expect("INSERT INTO a catalog named `local`");
    engine
        .sql("INSERT OVERWRITE local.live.reserved SELECT 3 AS id")
        .await
        .expect("INSERT OVERWRITE a catalog named `local`");

    assert_eq!(
        count(&engine, "SELECT count(*) FROM local.live.reserved").await,
        1,
        "the overwrite should have left exactly its own row"
    );
}

#[tokio::test]
async fn partitioning_a_non_delta_table_is_refused_rather_than_written_flat() {
    // Only Delta partitions on write here. Accepting `PARTITIONED BY` for Parquet would register
    // a table whose catalog metadata says "partitioned by d" while the data sits in one flat file
    // with `d` still inside it — a table unreadable through its own metadata, because the reader
    // looks for those columns in the directory path.
    let warehouse = tempfile::tempdir().expect("tempdir");
    let engine = engine_with_live_db(warehouse.path()).await;

    let err = engine
        .sql(
            "CREATE TABLE lake.live.flat_parquet USING parquet PARTITIONED BY (d) AS \
             SELECT 1 AS id, 'x' AS d",
        )
        .await
        .expect_err("a partitioned Parquet CTAS must not silently write flat");
    assert!(
        err.to_string().contains("PARTITIONED BY") && err.to_string().contains("delta"),
        "the error should name the clause and the format that does work: {err}"
    );

    // The equivalent Delta statement is what the error points at, and it works.
    engine
        .sql(
            "CREATE TABLE lake.live.partitioned_delta USING delta PARTITIONED BY (d) AS \
             SELECT 1 AS id, 'x' AS d",
        )
        .await
        .expect("Delta partitions on write");
    assert!(warehouse
        .path()
        .join("live.db/partitioned_delta/d=x")
        .is_dir());
}

#[tokio::test]
async fn a_ctas_whose_write_fails_does_not_leave_a_table_behind() {
    // The catalog entry is created before the data is written. If the write fails and the entry
    // stayed, the statement would be un-retryable in a way the user cannot clear: `CREATE TABLE`
    // reports the table already exists, and no SQL path reaches `DROP TABLE` to remove it.
    let warehouse = tempfile::TempDir::new().expect("tempdir");
    let engine = engine_with_live_db(warehouse.path()).await;

    // A location that passes every pre-flight check — it exists, and it is a directory — but
    // that the writer cannot actually write into. The failure has to land *after* the catalog
    // entry is created, which is the window this test is about.
    let blocked = warehouse.path().join("blocked");
    std::fs::create_dir_all(&blocked).expect("mkdir");
    let mut perms = std::fs::metadata(&blocked).expect("metadata").permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o555);
    std::fs::set_permissions(&blocked, perms).expect("chmod");

    let write_error = engine
        .sql(&format!(
            "CREATE TABLE lake.live.blocked_ctas USING delta LOCATION '{}/' AS SELECT 1 AS id",
            blocked.display()
        ))
        .await
        .expect_err("the write cannot succeed")
        .to_string();

    // Asked of the catalog directly, not through a `SELECT`: a query against the half-created
    // table fails either way — because it is absent, or because it is registered and its data is
    // not there — so a failing `SELECT` proves nothing about which happened.
    let registered = engine
        .external_catalog("lake")
        .expect("the local catalog is registered")
        .table_exists(&["live".to_string()], "blocked_ctas")
        .await
        .expect("catalog lookup");
    assert!(
        !registered,
        "the failed CTAS left `blocked_ctas` in the catalog with no data behind it, and no SQL \
         path can drop it (write error was: {write_error})"
    );
}

#[tokio::test]
async fn insert_into_a_partitioned_parquet_table_is_refused() {
    // The reader derives partition values from the directory path, so a flat single-file write
    // would come back with wrong or missing partition columns on every row.
    let warehouse = tempfile::tempdir().expect("tempdir");
    let service = OxidantService::with_catalogs(conf(warehouse.path())).await;
    let engine = service.engine();
    let catalog = engine
        .external_catalog("lake")
        .expect("the local catalog is registered");
    catalog
        .create_database("live", true, None, None)
        .await
        .expect("create_database");

    // Declared through the SPI so the table is partitioned Parquet without needing a CTAS path
    // that now refuses exactly this shape.
    use oxidant_catalog::TableFormat;
    use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("d", DataType::Utf8, true),
    ]));
    let created = catalog
        .create_table(
            &["live".into()],
            "hive_parquet",
            schema,
            TableFormat::Parquet,
            None,
            &["d".to_string()],
        )
        .await
        .expect("create_table");

    // One partition directory with a data file, so the table resolves at all.
    let dir = std::path::Path::new(created.location.trim_start_matches("file://")).join("d=x");
    std::fs::create_dir_all(&dir).expect("mkdir");
    engine
        .sql(&format!(
            "COPY (SELECT 1 AS id) TO '{}/part-0.parquet' STORED AS PARQUET",
            dir.display()
        ))
        .await
        .expect("seed one partition file");

    let err = engine
        .sql("INSERT INTO lake.live.hive_parquet SELECT 2 AS id, 'y' AS d")
        .await
        .expect_err("a partitioned Parquet insert must not write a flat file at the root");
    // Specific on purpose: a test that only checks for the word "partitioned" would also pass if
    // the table failed to resolve for some unrelated reason.
    let message = err.to_string();
    assert!(
        message.contains("`INSERT` into the partitioned Parquet table")
            && message.contains("Use a Delta table"),
        "this must be the partition refusal, not some other failure: {message}"
    );
}

/// `docs/sql-writes.md` promises that a value an `INSERT` cannot store in the column's type is
/// an error, not a `NULL` quietly committed in its place. Arrow's *default* cast is the safe one
/// — it substitutes `NULL` for anything that does not fit — so a write path that reaches for
/// `cast` rather than `cast_with_options(safe: false)` silently corrupts the table instead of
/// refusing the statement. These are the shapes that would land as `NULL` under a safe cast.
///
/// The sources are real columns rather than literals so the values survive constant folding and
/// the cast happens on the way to the file, which is where the damage would be done.
#[tokio::test]
async fn insert_of_a_value_too_big_for_the_column_errors_rather_than_writing_null() {
    let warehouse = tempfile::tempdir().expect("tempdir");
    let engine = engine_with_live_db(warehouse.path()).await;

    engine
        .sql(
            "CREATE TABLE lake.live.typed USING delta AS \
             SELECT CAST(1 AS INT) AS id, CAST(1.50 AS DECIMAL(5,2)) AS amount",
        )
        .await
        .expect("CTAS");
    engine
        .sql(
            "CREATE TABLE lake.live.oversized USING delta AS \
             SELECT '2147483648' AS id_text, CAST(2147483648 AS BIGINT) AS id_big, \
                    '100000.00' AS amount_text, CAST(100000.00 AS DECIMAL(12,2)) AS amount_wide, \
                    'not-a-number' AS junk",
        )
        .await
        .expect("CTAS oversized");

    for (sql, why) in [
        (
            "INSERT INTO lake.live.typed SELECT id_text, amount FROM lake.live.oversized, \
             lake.live.typed",
            "a string past INT range",
        ),
        (
            "INSERT INTO lake.live.typed SELECT id_big, amount FROM lake.live.oversized, \
             lake.live.typed",
            "a BIGINT past INT range",
        ),
        (
            "INSERT INTO lake.live.typed SELECT id, amount_text FROM lake.live.oversized, \
             lake.live.typed",
            "a string past DECIMAL(5,2) range",
        ),
        (
            "INSERT INTO lake.live.typed SELECT id, amount_wide FROM lake.live.oversized, \
             lake.live.typed",
            "a DECIMAL(12,2) past DECIMAL(5,2) range",
        ),
        (
            "INSERT INTO lake.live.typed SELECT junk, amount FROM lake.live.oversized, \
             lake.live.typed",
            "a string that is not a number at all",
        ),
    ] {
        let err =
            engine.sql(sql).await.err().unwrap_or_else(|| {
                panic!("{why} must fail the INSERT, not be stored as NULL: {sql}")
            });
        // Whichever layer catches it, the message has to be about the value not fitting — an
        // unrelated failure would make this test pass for the wrong reason.
        let text = err.to_string();
        assert!(
            text.contains("Cast error")
                || text.contains("too large")
                || text.contains("cannot be written to"),
            "{why}: expected a cast/range failure, got: {text}"
        );
    }

    assert_eq!(
        count(&engine, "SELECT count(*) FROM lake.live.typed").await,
        1,
        "only the CTAS row may be in the table; a refused INSERT must commit nothing"
    );
    assert_eq!(
        count(
            &engine,
            "SELECT count(*) FROM lake.live.typed WHERE id IS NULL OR amount IS NULL"
        )
        .await,
        0,
        "a refused INSERT must not have left a NULL behind"
    );
    assert_eq!(
        commits(&warehouse.path().join("live.db").join("typed")).len(),
        1,
        "a refused INSERT must not produce a transaction-log commit"
    );
}

/// The same contract for a timestamp, which lives on a Parquet table because Delta has no
/// mapping for the nanosecond timestamp `CAST(… AS TIMESTAMP)` produces.
#[tokio::test]
async fn insert_of_an_unparseable_timestamp_errors_rather_than_writing_null() {
    let warehouse = tempfile::tempdir().expect("tempdir");
    let engine = engine_with_live_db(warehouse.path()).await;

    engine
        .sql(
            "CREATE TABLE lake.live.events USING parquet AS \
             SELECT CAST('2026-01-01 00:00:00' AS TIMESTAMP) AS ts",
        )
        .await
        .expect("CTAS");
    engine
        .sql("CREATE TABLE lake.live.raw_events USING parquet AS SELECT 'not-a-timestamp' AS ts")
        .await
        .expect("CTAS raw");

    let err = engine
        .sql("INSERT INTO lake.live.events SELECT ts FROM lake.live.raw_events")
        .await
        .expect_err("an unparseable timestamp must fail the INSERT, not be stored as NULL")
        .to_string();
    assert!(
        err.contains("not-a-timestamp") || err.contains("cannot be written to"),
        "expected a timestamp-parse failure, got: {err}"
    );

    assert_eq!(
        count(
            &engine,
            "SELECT count(*) FROM lake.live.events WHERE ts IS NULL"
        )
        .await,
        0,
        "a refused INSERT must not have left a NULL timestamp behind"
    );
}

/// The cast is cast-or-*fail*, not cast-or-reject-nulls: a value that was already absent stays
/// absent. Rejecting these would make every nullable column unwritable, which is the opposite
/// over-correction.
#[tokio::test]
async fn insert_of_a_null_stays_null_and_still_commits() {
    let warehouse = tempfile::tempdir().expect("tempdir");
    let engine = engine_with_live_db(warehouse.path()).await;

    engine
        .sql(
            "CREATE TABLE lake.live.nullable_orders USING delta AS \
             SELECT CAST(1 AS INT) AS id, CAST(NULL AS DECIMAL(5,2)) AS amount",
        )
        .await
        .expect("CTAS");
    engine
        .sql(
            "INSERT INTO lake.live.nullable_orders \
             SELECT CAST(2 AS INT), CAST(NULL AS DECIMAL(5,2))",
        )
        .await
        .expect("a NULL into a nullable column is a legal INSERT");

    assert_eq!(
        count(&engine, "SELECT count(*) FROM lake.live.nullable_orders").await,
        2
    );
    assert_eq!(
        count(
            &engine,
            "SELECT count(*) FROM lake.live.nullable_orders WHERE amount IS NULL"
        )
        .await,
        2,
        "a NULL must read back as NULL, not as a substituted value"
    );
}
