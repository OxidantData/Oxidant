//! `oxidant pipeline` building a Kafka → Delta → SQL DAG end to end, offline.
//!
//! Uses the Kafka source's spool mode (`oxidant.spool.dir`), so this needs no broker, no
//! metastore, and no AWS — the same path CI can run. What it asserts is the whole product
//! claim: a config file describes bronze/silver/gold, the binary builds them, and the results
//! are queryable by name afterwards with `oxidant sql`.
//!
//! Lives in `oxidant-cli` so Cargo sets `CARGO_BIN_EXE_oxidant` when the test binary is built.

use std::process::{Command, Output};

fn oxidant_bin() -> std::path::PathBuf {
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_oxidant") {
        return std::path::PathBuf::from(p);
    }
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "debug".into());
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target")
        .join(&profile)
        .join("oxidant");
    assert!(
        path.exists(),
        "oxidant binary not found at {} — run `cargo build -p oxidant-cli` first",
        path.display()
    );
    path
}

/// A pipeline fixture: a spool of two micro-batches plus a three-table config.
struct Fixture {
    dir: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let spool = dir.path().join("spool/orders");
        std::fs::create_dir_all(&spool).expect("mkdir spool");
        // One file per micro-batch, newline-delimited JSON. The `-5` row is what the `drop`
        // expectation has to remove.
        std::fs::write(
            spool.join("batch-0.json"),
            "{\"order_id\":1,\"customer\":\"ada\",\"amount\":100}\n\
             {\"order_id\":2,\"customer\":\"bob\",\"amount\":250}\n\
             {\"order_id\":3,\"customer\":\"ada\",\"amount\":-5}\n",
        )
        .expect("batch-0");
        std::fs::write(
            spool.join("batch-1.json"),
            "{\"order_id\":4,\"customer\":\"cy\",\"amount\":75}\n\
             {\"order_id\":5,\"customer\":\"ada\",\"amount\":300}\n",
        )
        .expect("batch-1");

        let config = format!(
            r#"catalogs:
  local:
    type: local
    warehouse: {warehouse}
default_catalog: local
pipeline:
  name: sales
  catalog: local
  schema: live
  storage: {warehouse}/live
  checkpoints: {warehouse}/_checkpoints
  trigger: once
tables:
  - name: orders_bronze
    source:
      format: kafka
      options:
        subscribe: orders
        oxidant.spool.dir: {spool}
        startingOffsets: earliest
    sql: |
      SELECT
        CAST(get_json_object(CAST(value AS STRING), '$.order_id') AS BIGINT) AS order_id,
        get_json_object(CAST(value AS STRING), '$.customer')                 AS customer,
        CAST(get_json_object(CAST(value AS STRING), '$.amount') AS BIGINT)   AS amount
      FROM stream
  - name: orders_silver
    sql: SELECT * FROM orders_bronze
    expect:
      amount_positive:
        check: amount > 0
        action: drop
  - name: revenue_gold
    sql: SELECT customer, sum(amount) AS revenue, count(*) AS orders FROM orders_silver GROUP BY customer
"#,
            warehouse = dir.path().join("warehouse").display(),
            spool = spool.display(),
        );
        std::fs::write(dir.path().join("oxidant.yaml"), config).expect("write config");
        Self { dir }
    }

    fn config(&self) -> String {
        self.dir.path().join("oxidant.yaml").display().to_string()
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(oxidant_bin())
            .args(args)
            .arg("--config")
            .arg(self.config())
            .current_dir(self.dir.path())
            .env_remove("OXIDANT_URL")
            .output()
            .expect("run oxidant")
    }

    /// Run a scalar query through the embedded `oxidant sql` path.
    fn count(&self, sql: &str) -> i64 {
        let output = self.run(&["sql", "--format", "csv", "-e", sql]);
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "query failed: {sql}\nstdout: {stdout}\nstderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        stdout
            .lines()
            .last()
            .and_then(|line| line.trim().parse().ok())
            .unwrap_or_else(|| panic!("no scalar in output for `{sql}`: {stdout}"))
    }
}

#[test]
fn validate_reports_the_update_order_without_running_anything() {
    let fixture = Fixture::new();
    let output = fixture.run(&["pipeline", "validate"]);
    assert!(output.status.success(), "validate failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("orders_bronze -> orders_silver -> revenue_gold"),
        "expected the topological order, got: {stdout}"
    );
    // Nothing may have been built.
    assert!(
        !fixture.dir.path().join("warehouse/live").exists(),
        "validate must not write any tables"
    );
}

#[test]
fn a_run_builds_every_table_and_the_results_are_queryable_by_name() {
    let fixture = Fixture::new();
    let output = fixture.run(&["pipeline", "run"]);
    assert!(
        output.status.success(),
        "pipeline run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Bronze holds every source row.
    assert_eq!(
        fixture.count("SELECT count(*) FROM local.live.orders_bronze"),
        5
    );
    // Silver drops the one row failing the expectation.
    assert_eq!(
        fixture.count("SELECT count(*) FROM local.live.orders_silver"),
        4
    );
    // Gold aggregates the survivors: three distinct customers.
    assert_eq!(
        fixture.count("SELECT count(*) FROM local.live.revenue_gold"),
        3
    );
    // And the arithmetic is right, not merely the row count: ada is 100 + 300, with the -5
    // excluded by the expectation rather than netted into the sum.
    assert_eq!(
        fixture.count("SELECT revenue FROM local.live.revenue_gold WHERE customer = 'ada'"),
        400
    );
}

#[test]
fn a_derived_table_is_replaced_by_a_rerun_rather_than_appended_to() {
    // Full recompute means the second pass must swap the contents, not accumulate them. If
    // `replace` had appended, gold would double.
    let fixture = Fixture::new();
    assert!(fixture.run(&["pipeline", "run"]).status.success());
    let first = fixture.count("SELECT count(*) FROM local.live.revenue_gold");
    assert!(fixture.run(&["pipeline", "run"]).status.success());
    let second = fixture.count("SELECT count(*) FROM local.live.revenue_gold");
    assert_eq!(
        first, second,
        "a recomputed table must be replaced, not appended to"
    );
}

#[test]
fn a_failing_expectation_aborts_the_update_and_leaves_the_last_good_version() {
    let fixture = Fixture::new();
    assert!(fixture.run(&["pipeline", "run"]).status.success());
    let before = fixture.count("SELECT count(*) FROM local.live.revenue_gold");

    // Switch gold to an expectation its own output cannot satisfy.
    let config = std::fs::read_to_string(fixture.config()).expect("read");
    let broken = config.replace(
        "  - name: revenue_gold\n    sql: SELECT customer, sum(amount) AS revenue, count(*) AS orders FROM orders_silver GROUP BY customer\n",
        "  - name: revenue_gold\n    sql: SELECT customer, sum(amount) AS revenue, count(*) AS orders FROM orders_silver GROUP BY customer\n    expect:\n      impossible:\n        check: revenue < 0\n        action: fail\n",
    );
    assert_ne!(config, broken, "the config edit must have applied");
    std::fs::write(fixture.config(), broken).expect("write");

    let output = fixture.run(&["pipeline", "run"]);
    assert!(
        !output.status.success(),
        "a failed expectation must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("impossible"),
        "the error should name the expectation: {stderr}"
    );
    assert_eq!(
        fixture.count("SELECT count(*) FROM local.live.revenue_gold"),
        before,
        "the table must be left at its last good version"
    );
}

#[test]
fn a_streamed_delta_table_is_also_readable_as_iceberg() {
    // One copy of the data, two metadata trees. The Iceberg side trails the Delta side by up to
    // `checkpoint_interval` commits, so the invariant is a non-empty snapshot no larger than
    // Delta's — not equality.
    let fixture = Fixture::new();
    assert!(fixture.run(&["pipeline", "run"]).status.success());
    let delta = fixture.count("SELECT count(*) FROM local.live.orders_bronze");
    let iceberg = fixture.count("SELECT count(*) FROM local.live.orders_bronze_iceberg");
    assert!(
        iceberg > 0 && iceberg <= delta,
        "expected a non-empty Iceberg snapshot no larger than Delta's ({delta}), got {iceberg}"
    );
}

#[test]
fn restricting_to_one_table_still_builds_its_ancestors() {
    // Refreshing gold from stale silver would report success over old numbers.
    let fixture = Fixture::new();
    let output = fixture.run(&["pipeline", "run", "--table", "revenue_gold"]);
    assert!(
        output.status.success(),
        "targeted run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fixture.count("SELECT count(*) FROM local.live.orders_bronze"),
        5
    );
    assert_eq!(
        fixture.count("SELECT count(*) FROM local.live.revenue_gold"),
        3
    );
}
