//! `oxidant pipeline` running an AUTO CDC (SCD Type 1) table declared in YAML, offline.
//!
//! The Connect/SDP-SQL path is covered in `oxidant-connect`; this covers the other surface that
//! reaches the same merge — a `auto_cdc:` block on a table in `oxidant.yaml`. Uses the Kafka
//! source's spool mode, so it needs no broker.
//!
//! Lives in `oxidant-cli` so Cargo sets `CARGO_BIN_EXE_oxidant` when the test binary is built.

use std::process::{Command, Output};

mod common;
use common::oxidant_bin;

/// A CDC spool (insert, out-of-order update, delete, truncate, tied keys) plus a bronze/SCD1
/// config over it.
struct Fixture {
    dir: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let spool = dir.path().join("spool/customers");
        std::fs::create_dir_all(&spool).expect("mkdir spool");
        // `seq` is the event order, `op` the change type. The `seq: 0` row for id 1 arrives
        // after `seq: 2` and must lose; id 2 is inserted and then deleted.
        std::fs::write(
            spool.join("batch-0.json"),
            "{\"id\":1,\"name\":\"ada\",\"seq\":1,\"op\":\"I\"}\n\
             {\"id\":2,\"name\":\"bob\",\"seq\":1,\"op\":\"I\"}\n\
             {\"id\":1,\"name\":\"ada_renamed\",\"seq\":2,\"op\":\"U\"}\n\
             {\"id\":1,\"name\":\"stale\",\"seq\":0,\"op\":\"U\"}\n\
             {\"id\":2,\"name\":\"bob\",\"seq\":2,\"op\":\"D\"}\n\
             {\"id\":3,\"name\":\"cy\",\"seq\":1,\"op\":\"I\"}\n",
        )
        .expect("batch-0");

        // A truncate whose sequence is *older* than everything committed: it must remove
        // nothing. The whole-target wipe this used to do is invisible unless live rows sit
        // above the truncate's sequence.
        std::fs::write(
            spool.join("batch-1.json"),
            "{\"id\":9,\"seq\":0,\"op\":\"T\"}\n\
             {\"id\":5,\"name\":\"eve\",\"seq\":3,\"op\":\"I\"}\n",
        )
        .expect("batch-1");

        // Two rows for one key at the same sequence: the winner has to be the same on every
        // run, or a replay recomputes a different table than the one that was committed.
        std::fs::write(
            spool.join("batch-2.json"),
            "{\"id\":4,\"name\":\"aaa\",\"seq\":5,\"op\":\"I\"}\n\
             {\"id\":4,\"name\":\"zzz\",\"seq\":5,\"op\":\"I\"}\n",
        )
        .expect("batch-2");

        // A truncate at seq 1: rows committed above it survive, `cy` (seq 1) does not.
        std::fs::write(
            spool.join("batch-3.json"),
            "{\"id\":9,\"seq\":1,\"op\":\"T\"}\n\
             {\"id\":6,\"name\":\"six\",\"seq\":6,\"op\":\"I\"}\n",
        )
        .expect("batch-3");

        let source = format!(
            r#"    source:
      format: kafka
      options:
        subscribe: customers
        oxidant.spool.dir: {spool}
        startingOffsets: earliest
    sql: |
      SELECT
        CAST(get_json_object(CAST(value AS STRING), '$.id') AS BIGINT)  AS id,
        get_json_object(CAST(value AS STRING), '$.name')                AS name,
        CAST(get_json_object(CAST(value AS STRING), '$.seq') AS BIGINT) AS seq,
        get_json_object(CAST(value AS STRING), '$.op')                  AS op
      FROM stream
"#,
            spool = spool.display(),
        );
        let config = format!(
            r#"catalogs:
  local:
    type: local
    warehouse: {warehouse}
default_catalog: local
pipeline:
  name: cdc
  catalog: local
  schema: live
  storage: {warehouse}/live
  checkpoints: {warehouse}/_checkpoints
  trigger: once
tables:
  - name: customers_bronze
{source}
  - name: customers_scd1
{source}
    auto_cdc:
      source: customers_bronze
      keys: [id]
      sequence_by: seq
      apply_as_deletes: "op = 'D'"
      apply_as_truncates: "op = 'T'"
      except_column_list: [op]
"#,
            warehouse = dir.path().join("warehouse").display(),
            source = source,
        );
        std::fs::write(dir.path().join("oxidant.yaml"), config).expect("write config");
        Self { dir }
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(oxidant_bin())
            .args(args)
            .arg("--config")
            .arg(self.dir.path().join("oxidant.yaml"))
            .current_dir(self.dir.path())
            .env_remove("OXIDANT_URL")
            .output()
            .expect("run oxidant")
    }

    /// Run a query through the embedded `oxidant sql` path and return its data rows.
    fn query(&self, sql: &str) -> Vec<String> {
        let output = self.run(&["sql", "--format", "csv", "-e", sql]);
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "query failed: {sql}\nstdout: {stdout}\nstderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        stdout
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .skip(1) // header
            .map(str::to_string)
            .collect()
    }
}

#[test]
fn show_reports_the_cdc_table_as_its_own_kind() {
    let fixture = Fixture::new();
    let output = fixture.run(&["pipeline", "show"]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "show failed: {stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("customers_scd1 (auto_cdc"),
        "no auto_cdc kind in: {stdout}"
    );
    // The `auto_cdc.source` is what puts the edge in the graph — the target's own `sql:` never
    // names the bronze table.
    assert!(
        stdout.contains("reads: customers_bronze"),
        "no cdc source edge in: {stdout}"
    );
}

#[test]
fn a_yaml_auto_cdc_table_merges_by_key_instead_of_appending() {
    let fixture = Fixture::new();
    let output = fixture.run(&["pipeline", "run"]);
    assert!(
        output.status.success(),
        "run failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // The bronze table appends every change event; the SCD1 target keeps one row per key.
    assert_eq!(
        fixture.query("SELECT count(*) FROM local.live.customers_bronze"),
        vec!["12"]
    );
    // id 1 keeps its highest-sequence value (not the `seq: 0` row that arrived later) and
    // survives both truncates, which are older than it. id 2 is deleted by `op = 'D'`. id 3 is
    // inserted at seq 1 and then removed by the seq-1 truncate — the one truncate that reaches
    // it. id 4 resolves its tied sequence deterministically (`zzz`, the larger remaining
    // column). ids 5 and 6 arrive above every truncate. `op` is dropped by
    // `except_column_list`.
    assert_eq!(
        fixture.query("SELECT id, name, seq FROM local.live.customers_scd1 ORDER BY id"),
        vec!["1,ada_renamed,2", "4,zzz,5", "5,eve,3", "6,six,6"]
    );
}
