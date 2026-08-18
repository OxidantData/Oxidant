//! `oxidant sql` running statements **in-process**, with no server anywhere.
//!
//! This is the behaviour the config file exists to enable: declare a catalog over directories
//! of data files and query them by name from the binary alone. Every test here deliberately
//! avoids starting a server, and one asserts that `--url` still routes to the REST path so the
//! two cannot quietly collapse into one.
//!
//! Lives in `oxidant-cli` so Cargo sets `CARGO_BIN_EXE_oxidant` when the test binary is built.

use std::process::{Command, Output};

fn oxidant_bin() -> std::path::PathBuf {
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_oxidant") {
        return std::path::PathBuf::from(p);
    }
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "debug".into());
    let target = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target");
    let path = target.join(&profile).join("oxidant");
    assert!(
        path.exists(),
        "oxidant binary not found at {} — run `cargo build -p oxidant-cli` first",
        path.display()
    );
    path
}

fn sample_data() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../sample-data")
        .canonicalize()
        .expect("sample-data tree is committed at the repo root")
}

/// Write a config declaring the sample tables in every format, and return its path.
fn write_config(dir: &std::path::Path) -> std::path::PathBuf {
    let root = sample_data();
    let root = root.display();
    let warehouse = dir.join("warehouse");
    let config = format!(
        "catalogs:
  local:
    type: local
    warehouse: {warehouse}
    tables:
      samples.nation_parquet: {{ format: parquet, location: {root}/parquet/tpch_nation.parquet }}
      samples.nation_csv:     {{ format: csv,     location: {root}/csv/tpch_nation.csv, options: {{ header: \"true\" }} }}
      samples.nation_delta:   {{ format: delta,   location: {root}/delta/tpch_nation }}
      samples.nation_iceberg: {{ format: iceberg, location: {root}/iceberg/tpch_nation }}
    discover:
      - {{ namespace: bronze, path: {root}/parquet }}
default_catalog: local
",
        warehouse = warehouse.display()
    );
    let path = dir.join("oxidant.yaml");
    std::fs::write(&path, config).expect("write config");
    path
}

/// Run `oxidant sql` with no server, returning the raw output.
fn run_sql(args: &[&str]) -> Output {
    Command::new(oxidant_bin())
        .arg("sql")
        .args(args)
        // A stray `OXIDANT_URL` in the developer's shell would silently send these to a server
        // and make the test assert nothing about the embedded path.
        .env_remove("OXIDANT_URL")
        .output()
        .expect("run oxidant sql")
}

fn stdout_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn assert_ok(output: &Output, what: &str) {
    assert!(
        output.status.success(),
        "{what} failed:\nstdout: {}\nstderr: {}",
        stdout_of(output),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn sql_runs_with_no_server_and_no_config_at_all() {
    // The zero-config case: the binary must answer a self-contained query on its own.
    let output = run_sql(&["-e", "SELECT 1 AS hello"]);
    assert_ok(&output, "bare embedded query");
    assert!(
        stdout_of(&output).contains("hello"),
        "expected the column header, got: {}",
        stdout_of(&output)
    );
}

#[test]
fn every_declared_format_reads_back_the_same_row_count() {
    // The four copies are the same TPC-H table, so agreeing counts prove each format really
    // resolved — far stronger than each one merely parsing.
    let dir = tempfile::tempdir().expect("tempdir");
    let config = write_config(dir.path());
    let config = config.to_string_lossy().to_string();

    let mut counts = Vec::new();
    for table in [
        "nation_parquet",
        "nation_csv",
        "nation_delta",
        "nation_iceberg",
    ] {
        let output = run_sql(&[
            "-c",
            &config,
            "--format",
            "csv",
            "-e",
            &format!("SELECT count(*) AS n FROM local.samples.{table}"),
        ]);
        assert_ok(&output, table);
        let count: i64 = stdout_of(&output)
            .lines()
            .last()
            .and_then(|line| line.trim().parse().ok())
            .unwrap_or_else(|| panic!("no count in output for {table}: {}", stdout_of(&output)));
        assert!(count > 0, "`{table}` returned no rows");
        counts.push((table, count));
    }
    let first = counts[0].1;
    for (table, count) in &counts {
        assert_eq!(
            *count, first,
            "`{table}` disagrees with `{}` on row count: {counts:?}",
            counts[0].0
        );
    }
}

#[test]
fn columns_read_back_real_values_not_nulls() {
    // A count passes even when every column reads as null — the exact failure mode an Iceberg
    // table without a field-id name mapping has. Assert on actual data.
    let dir = tempfile::tempdir().expect("tempdir");
    let config = write_config(dir.path());
    let config = config.to_string_lossy().to_string();

    for table in ["nation_parquet", "nation_delta", "nation_iceberg"] {
        let output = run_sql(&[
            "-c",
            &config,
            "--format",
            "csv",
            "-e",
            &format!("SELECT n_name FROM local.samples.{table} WHERE n_nationkey = 0"),
        ]);
        assert_ok(&output, table);
        assert!(
            stdout_of(&output).contains("ALGERIA"),
            "`{table}` did not return the expected value; got: {}",
            stdout_of(&output)
        );
    }
}

#[test]
fn discovered_tables_are_queryable_without_being_declared() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = write_config(dir.path());
    let output = run_sql(&[
        "-c",
        &config.to_string_lossy(),
        "--format",
        "csv",
        "-e",
        "SELECT count(*) AS n FROM local.bronze.tpch_lineitem",
    ]);
    assert_ok(&output, "discovered table");
    let count: i64 = stdout_of(&output)
        .lines()
        .last()
        .and_then(|line| line.trim().parse().ok())
        .expect("a count");
    assert!(count > 0, "discovery registered nothing");
}

#[test]
fn a_missing_table_exits_non_zero_so_scripts_can_chain() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = write_config(dir.path());
    let output = run_sql(&[
        "-c",
        &config.to_string_lossy(),
        "-e",
        "SELECT * FROM local.samples.no_such_table",
    ]);
    assert!(
        !output.status.success(),
        "a failed statement must exit non-zero, or `oxidant sql ... && next` runs anyway"
    );
}

#[test]
fn an_explicit_config_path_that_does_not_exist_is_an_error() {
    // A typo in `--config` must not silently fall through to the defaults and run against the
    // wrong catalogs.
    let output = run_sql(&["-c", "/nonexistent/oxidant.yaml", "-e", "SELECT 1"]);
    assert!(!output.status.success(), "a missing --config must fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("nonexistent"),
        "the error should name the path, got: {stderr}"
    );
}

#[test]
fn an_invalid_config_names_the_offending_key() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bad.yaml");
    std::fs::write(
        &path,
        "catalogs:\n  local:\n    type: local\n    warehosue: /tmp/w\n",
    )
    .expect("write");
    let output = run_sql(&["-c", &path.to_string_lossy(), "-e", "SELECT 1"]);
    assert!(!output.status.success(), "an invalid config must fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("warehosue"),
        "the error should name the misspelled key, got: {stderr}"
    );
}

#[test]
fn an_explicit_url_still_routes_to_the_rest_api() {
    // The two paths must not collapse into one: with `--url` given, the statement has to go to
    // that server, and failing to reach it is the correct outcome — not a local result.
    let output = run_sql(&["--url", "http://127.0.0.1:1", "-e", "SELECT 1 AS hello"]);
    assert!(
        !output.status.success(),
        "an unreachable --url must fail rather than silently running the query locally"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("127.0.0.1:1"),
        "the error should name the server that was tried, got: {stderr}"
    );
    assert!(
        !stdout_of(&output).contains("hello"),
        "the query must NOT have been answered locally"
    );
}
