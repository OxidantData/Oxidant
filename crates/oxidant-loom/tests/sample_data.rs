//! Integration tests for the committed `sample-data/` tree + `Engine::register_sample_tables`
//! (what `oxidant spark server --sample-data <DIR>` / `OXIDANT_SAMPLE_DATA_DIR` drives at boot).
//!
//! The tree is TPC-H SF 0.01 regenerated with `cargo run -p oxidant-bench -- sample-data`
//! (see `sample-data/README.md`). It is relocatable by construction — the Delta log and every
//! Iceberg path are table-root-relative — so the same bytes must read identically from any
//! checkout or from the Docker image. The cross-format test is the guard that the
//! delta_kernel / iceberg-rust readers stay compatible with the generator's output; if a
//! reader chokes on the generated files, fix the generator, not this test.

use std::path::PathBuf;

use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::Engine;

/// The committed tree at the workspace root (`crates/oxidant-loom` → `../../sample-data`).
fn sample_data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../sample-data")
}

/// Exact SF 0.01 row counts, verified against the generator's output.
const EXPECTED: [(&str, i64); 8] = [
    ("nation", 25),
    ("region", 5),
    ("supplier", 100),
    ("customer", 1500),
    ("part", 2000),
    ("partsupp", 8000),
    ("orders", 15000),
    ("lineitem", 60175),
];

/// The four tables that also have Delta + Iceberg variants.
const HEADLINE: [&str; 4] = ["nation", "customer", "orders", "lineitem"];

/// `SELECT count(*) FROM samples.<table>` as an i64.
async fn samples_count(engine: &Engine, table: &str) -> i64 {
    let batches = engine
        .sql(&format!("SELECT count(*) FROM samples.{table}"))
        .await
        .unwrap_or_else(|e| panic!("count samples.{table}: {e}"));
    batches
        .iter()
        .map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("count(*) is Int64")
                .value(0)
        })
        .sum()
}

/// Every committed table registers and carries its SF 0.01 row count (parquet + csv).
#[tokio::test(flavor = "multi_thread")]
async fn registers_all_committed_sample_tables() {
    let dir = sample_data_dir();
    assert!(
        dir.is_dir(),
        "sample-data tree missing at {}",
        dir.display()
    );
    let engine = Engine::new();
    let registered = engine.register_sample_tables(&dir).await;
    // 8 parquet + 8 csv + 4 delta + 4 iceberg.
    assert_eq!(registered, 24);
    for (table, expected) in EXPECTED {
        assert_eq!(
            samples_count(&engine, &format!("tpch_{table}")).await,
            expected,
            "samples.tpch_{table} row count"
        );
        assert_eq!(
            samples_count(&engine, &format!("tpch_{table}_csv")).await,
            expected,
            "samples.tpch_{table}_csv row count"
        );
    }
}

/// The 4 headline tables must read back the same row count from all 4 physical formats —
/// this catches delta_kernel / iceberg-rust vs. generator incompatibilities.
#[tokio::test(flavor = "multi_thread")]
async fn cross_format_counts_match_for_headline_tables() {
    let engine = Engine::new();
    engine.register_sample_tables(sample_data_dir()).await;
    for table in HEADLINE {
        let expected = EXPECTED
            .into_iter()
            .find(|(t, _)| *t == table)
            .unwrap_or_else(|| panic!("no expected count for {table}"))
            .1;
        for suffix in ["", "_csv", "_delta", "_iceberg"] {
            assert_eq!(
                samples_count(&engine, &format!("tpch_{table}{suffix}")).await,
                expected,
                "samples.tpch_{table}{suffix} row count"
            );
        }
    }
}

/// Without `--sample-data` there is no `samples` schema and queries against it fail (no
/// behavior change for existing deployments).
#[tokio::test(flavor = "multi_thread")]
async fn engine_without_sample_data_has_no_samples_schema() {
    let engine = Engine::new();
    let catalog = engine
        .ctx()
        .catalog("spark_catalog")
        .expect("built-in catalog");
    assert!(catalog.schema("samples").is_none());
    assert!(
        engine
            .sql("SELECT count(*) FROM samples.tpch_nation")
            .await
            .is_err(),
        "samples.tpch_nation must not resolve without registration"
    );
}

/// A missing directory registers nothing and never fails (boot must not depend on sample data).
#[tokio::test(flavor = "multi_thread")]
async fn missing_sample_data_dir_registers_nothing() {
    let engine = Engine::new();
    let registered = engine
        .register_sample_tables("/definitely/not/a/sample-data/dir")
        .await;
    assert_eq!(registered, 0);
    let catalog = engine
        .ctx()
        .catalog("spark_catalog")
        .expect("built-in catalog");
    assert!(catalog.schema("samples").is_none());
}
