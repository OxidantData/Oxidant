//! Smoke test for the `oxidant worker` / `oxidant driver` CLI subprocess path: spawn two workers,
//! register in-memory data via Parquet files, and assert the distributed GROUP BY matches
//! single-node execution.
//!
//! Lives in `oxidant-cli` so Cargo sets `CARGO_BIN_EXE_oxidant` when the test binary is built.

use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;
use tempfile::TempDir;

mod common;
use common::oxidant_bin;

fn pick_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local_addr")
        .port()
}

fn make_batch(start: i64, end: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let ks: Vec<i64> = (start..end).map(|i| i % 5).collect();
    let vs: Vec<i64> = (start..end).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ks)),
            Arc::new(Int64Array::from(vs)),
        ],
    )
    .unwrap()
}

fn write_parquet(path: &std::path::Path, batch: &RecordBatch) {
    use datafusion::parquet::arrow::ArrowWriter;
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

#[tokio::test]
async fn cli_driver_worker_matches_single_node() {
    const N: i64 = 50;
    let query = "SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k";

    let single = Engine::new();
    single
        .register_batches("t", vec![make_batch(0, N)])
        .unwrap();
    let expected_rows: usize = single
        .sql(query)
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();

    let dir = TempDir::new().unwrap();
    let p0_path = dir.path().join("half0.parquet");
    let p1_path = dir.path().join("half1.parquet");
    write_parquet(&p0_path, &make_batch(0, N / 2));
    write_parquet(&p1_path, &make_batch(N / 2, N));

    let oxidant = oxidant_bin();
    assert!(
        oxidant.exists(),
        "oxidant binary not found at {}; run `cargo build -p oxidant-cli` first",
        oxidant.display()
    );

    let p0 = pick_port();
    let p1 = pick_port();

    let mut w0 = Command::new(&oxidant)
        .args([
            "worker",
            "--foreground",
            "--port",
            &p0.to_string(),
            "--data",
            p0_path.to_str().unwrap(),
            "--table",
            "t",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn worker 0");
    let mut w1 = Command::new(&oxidant)
        .args([
            "worker",
            "--foreground",
            "--port",
            &p1.to_string(),
            "--data",
            p1_path.to_str().unwrap(),
            "--table",
            "t",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn worker 1");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut driver_ok = false;
    for _ in 0..30 {
        let out = Command::new(&oxidant)
            .args([
                "driver",
                "--workers",
                &format!("127.0.0.1:{p0},127.0.0.1:{p1}"),
                "--partial-sql",
                "SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k",
                "--final-sql",
                "SELECT k, SUM(c) AS c, SUM(s) AS s FROM shuffle_input GROUP BY k",
                "--hash-keys",
                "0",
            ])
            .output()
            .expect("run driver");
        if out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            if stderr.contains(&format!("distributed result: {expected_rows} rows")) {
                driver_ok = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let _ = w0.kill();
    let _ = w1.kill();
    let _ = w0.wait();
    let _ = w1.wait();

    assert!(
        driver_ok,
        "oxidant driver subprocess must return {expected_rows} rows matching single-node"
    );
}
