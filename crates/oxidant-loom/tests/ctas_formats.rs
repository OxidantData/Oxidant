//! `CREATE TABLE ... USING <fmt> AS SELECT ...` writes bytes in the format it declares.
//!
//! This guards a bug that a round-trip test cannot see. The CTAS path used to pick the file
//! *extension* from the requested format while always writing with Parquet's `ArrowWriter`, so
//! `USING csv` produced Parquet bytes in a file named `part-00000.csv` and then registered the
//! table as `STORED AS CSV`. Nothing errored at write time.
//!
//! So these tests assert on the **leading bytes of the file on disk**, not merely that the data
//! reads back — because for the broken version, reading back is exactly what failed, and a test
//! that only checked "did the write succeed" would have passed throughout.

use oxidant_loom::Engine;

/// Parquet files begin with the four-byte magic `PAR1`.
const PARQUET_MAGIC: &[u8] = b"PAR1";

/// Locate the single data file a CTAS wrote for `table`.
///
/// Managed `CREATE TABLE ... USING <fmt>` tables land under
/// `{temp_dir}/oxidant-warehouse/{engine-id}/{table}/`, and the engine-id is unique per
/// `Engine`, so searching for the table name is unambiguous as long as each test uses its own.
fn written_file(table: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join("oxidant-warehouse");
    let engines = std::fs::read_dir(&root)
        .unwrap_or_else(|e| panic!("read {}: {e}", root.display()))
        .filter_map(std::result::Result::ok);
    for engine in engines {
        let dir = engine.path().join(table);
        if !dir.is_dir() {
            continue;
        }
        let mut files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
            .filter_map(std::result::Result::ok)
            .map(|e| e.path())
            .filter(|p| p.is_file())
            .collect();
        files.sort();
        assert_eq!(
            files.len(),
            1,
            "expected one data file in {}, got {files:?}",
            dir.display()
        );
        return files.remove(0);
    }
    panic!(
        "no table directory named `{table}` under {}",
        root.display()
    );
}

#[tokio::test]
async fn ctas_writes_each_format_as_itself_not_as_parquet() {
    let engine = Engine::new();

    let cases = [
        ("t_parquet", "parquet", "parquet"),
        ("t_csv", "csv", "csv"),
        ("t_json", "json", "json"),
    ];

    for (table, format, ext) in cases {
        engine
            .sql(&format!(
                "CREATE TABLE {table} USING {format} AS \
                 SELECT * FROM VALUES (1, 'a'), (2, 'b') AS v(num, letter)"
            ))
            .await
            .unwrap_or_else(|e| panic!("CTAS with USING {format} failed: {e}"));

        let file = written_file(table);
        assert_eq!(
            file.extension().and_then(|e| e.to_str()),
            Some(ext),
            "the file for USING {format} should end in .{ext}"
        );

        let bytes = std::fs::read(&file).expect("read the written file");
        assert!(!bytes.is_empty(), "USING {format} wrote an empty file");
        let starts_with_parquet = bytes.starts_with(PARQUET_MAGIC);
        if format == "parquet" {
            assert!(
                starts_with_parquet,
                "a parquet table should start with the PAR1 magic"
            );
        } else {
            assert!(
                !starts_with_parquet,
                "USING {format} wrote PARQUET bytes into a .{ext} file — the exact bug this \
                 test exists for. First bytes: {:?}",
                &bytes[..bytes.len().min(16)]
            );
            // And it must be readable text, since both CSV and JSON are.
            let text = String::from_utf8(bytes)
                .unwrap_or_else(|_| panic!("USING {format} did not write valid UTF-8 text"));
            assert!(
                text.contains('a') && text.contains('b'),
                "USING {format} did not contain the projected values: {text}"
            );
        }
    }
}

#[tokio::test]
async fn a_csv_table_written_by_ctas_reads_back_its_rows() {
    // The consequence of the byte-level bug: the table registered fine and then could not be
    // scanned. Reading it back is the user-visible half of the guarantee.
    let engine = Engine::new();
    engine
        .sql(
            "CREATE TABLE csv_rt USING csv AS \
             SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS v(num, letter)",
        )
        .await
        .expect("CTAS");
    let batches = engine
        .sql("SELECT count(*) AS n FROM csv_rt")
        .await
        .expect("read the csv table back");
    let n = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<oxidant_loom::arrow::array::Int64Array>()
                .expect("count is Int64")
                .value(0)
        })
        .expect("a count row");
    assert_eq!(n, 3, "every row written must read back");
}

#[tokio::test]
async fn ctas_into_an_unwritable_format_is_refused_rather_than_silently_substituted() {
    // `orc` parses as a Spark format but Oxidant can neither write nor read it. Quietly writing
    // Parquet under an `.orc` name is how the CSV bug started, so this must be an error.
    let engine = Engine::new();
    let result = engine
        .sql("CREATE TABLE t_orc USING orc AS SELECT * FROM VALUES (1) AS v(num)")
        .await;
    assert!(
        result.is_err(),
        "USING orc must be refused, not silently written as something else"
    );
}

/// `LOCATION`, `PARTITIONED BY`, and `OPTIONS(…)` reach real files, not just a lowered string.
///
/// The unit tests in `spark_create_table` assert on the DDL this produces; these assert that
/// DataFusion accepts it. That distinction matters: `Engine::sql` falls back to the normal path
/// whenever the lowered DDL fails to plan, so a wrong rewrite would show up as an unhelpful parse
/// error rather than a test failure anywhere else.
#[tokio::test]
async fn create_table_honors_location_partitioned_by_and_csv_options() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = Engine::new();

    let location = dir.path().join("events");
    engine
        .sql(&format!(
            "CREATE TABLE located_events (id INT, region STRING) USING csv \
             PARTITIONED BY (region) LOCATION '{}' OPTIONS (header 'true')",
            location.display()
        ))
        .await
        .expect("CREATE TABLE with LOCATION / PARTITIONED BY / OPTIONS");

    engine
        .sql("INSERT INTO located_events VALUES (1, 'eu'), (2, 'us'), (3, 'eu')")
        .await
        .expect("INSERT");

    // Written where the DDL asked, partitioned the way it asked.
    assert!(
        location.join("region=eu").is_dir() && location.join("region=us").is_dir(),
        "no Hive partition directories under {}",
        location.display()
    );

    let batches = engine
        .sql("SELECT count(*) FROM located_events WHERE region = 'eu'")
        .await
        .expect("SELECT");
    let count = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("a row")
        .column(0)
        .as_any()
        .downcast_ref::<oxidant_loom::arrow::array::Int64Array>()
        .expect("Int64 count")
        .value(0);
    assert_eq!(
        count, 2,
        "the partition column must filter from the path, not read back null"
    );

    // `header 'true'` must have reached DataFusion as `format.has_header`. If the option had been
    // dropped, the written header line would read back as a data row.
    let total = engine
        .sql("SELECT count(*) FROM located_events")
        .await
        .expect("SELECT");
    let total = total
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("a row")
        .column(0)
        .as_any()
        .downcast_ref::<oxidant_loom::arrow::array::Int64Array>()
        .expect("Int64 count")
        .value(0);
    assert_eq!(total, 3, "the CSV header row was read as data");
}
