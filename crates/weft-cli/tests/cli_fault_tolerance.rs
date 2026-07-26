//! Fault-tolerance smoke tests: kill a worker mid-stage and assert the driver still returns
//! single-node-correct results via retry, worker restart, and lineage recompute.
//!
//! Lives in `weft-cli` so Cargo sets `CARGO_BIN_EXE_weft` when the test binary is built.
//! Run `cargo build -p weft-cli` before `cargo test -p weft-cli --test cli_fault_tolerance`.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use weft_loom::arrow::array::Int64Array;
use weft_loom::arrow::datatypes::{DataType, Field, Schema};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;

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

fn write_parquet(path: &Path, batch: &RecordBatch) {
    use datafusion::parquet::arrow::ArrowWriter;
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

fn weft_bin() -> PathBuf {
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_weft") {
        return PathBuf::from(p);
    }
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "debug".into());
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Ok(td) = std::env::var("CARGO_TARGET_DIR") {
        candidates.push(PathBuf::from(td).join(&profile).join("weft"));
    }
    let workspace_target = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target");
    candidates.push(workspace_target.join(&profile).join("weft"));
    candidates.push(
        workspace_target
            .join("llvm-cov-target")
            .join(&profile)
            .join("weft"),
    );
    for c in &candidates {
        if c.exists() {
            return c.clone();
        }
    }
    candidates
        .into_iter()
        .next()
        .unwrap_or_else(|| workspace_target.join(&profile).join("weft"))
}

struct WorkerHandle {
    child: Child,
    port: u16,
    data: PathBuf,
    fault_env: Vec<(String, String)>,
}

impl WorkerHandle {
    fn spawn(weft: &Path, port: u16, data: &Path, fault_env: &[(&str, &str)]) -> Self {
        let mut cmd = Command::new(weft);
        cmd.args([
            "worker",
            "--port",
            &port.to_string(),
            "--data",
            data.to_str().unwrap(),
            "--table",
            "t",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
        for (k, v) in fault_env {
            cmd.env(k, v);
        }
        let child = cmd.spawn().expect("spawn worker");
        Self {
            child,
            port,
            data: data.to_path_buf(),
            fault_env: fault_env
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn try_wait(&mut self) -> Option<ExitStatus> {
        self.child.try_wait().expect("poll worker").map(|s| {
            if s.success() {
                panic!(
                    "worker on port {} exited cleanly (expected fault injection)",
                    self.port
                );
            }
            s
        })
    }

    fn respawn(&mut self, weft: &Path) {
        let _ = self.child.wait();
        let mut cmd = Command::new(weft);
        cmd.args([
            "worker",
            "--port",
            &self.port.to_string(),
            "--data",
            self.data.to_str().unwrap(),
            "--table",
            "t",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
        for (k, v) in &self.fault_env {
            cmd.env(k, v);
        }
        // Only the first matching task should fault; clear injection on restart.
        cmd.env_remove("WEFT_FAULT_EXIT_ON_TASK");
        cmd.env_remove("WEFT_FAULT_EXIT_STAGE");
        self.child = cmd.spawn().expect("respawn worker");
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct DistQuerySetup {
    _dir: TempDir,
    weft: PathBuf,
    p0: u16,
    p1: u16,
    w0: WorkerHandle,
    w1: WorkerHandle,
    expected_rows: usize,
}

impl DistQuerySetup {
    async fn new(n: i64, fault_w0: &[(&str, &str)]) -> Self {
        const QUERY: &str = "SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k";

        let single = Engine::new();
        single
            .register_batches("t", vec![make_batch(0, n)])
            .unwrap();
        let expected_rows: usize = single
            .sql(QUERY)
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();

        let dir = TempDir::new().unwrap();
        let p0_path = dir.path().join("half0.parquet");
        let p1_path = dir.path().join("half1.parquet");
        write_parquet(&p0_path, &make_batch(0, n / 2));
        write_parquet(&p1_path, &make_batch(n / 2, n));

        let weft = weft_bin();
        assert!(
            weft.exists(),
            "weft binary not found at {}; run `cargo build -p weft-cli` first",
            weft.display()
        );

        let p0 = pick_port();
        let p1 = pick_port();
        let w0 = WorkerHandle::spawn(&weft, p0, &p0_path, fault_w0);
        let w1 = WorkerHandle::spawn(&weft, p1, &p1_path, &[]);

        Self {
            _dir: dir,
            weft,
            p0,
            p1,
            w0,
            w1,
            expected_rows,
        }
    }
}

async fn run_with_worker_restart(fault_w0: &[(&str, &str)]) -> bool {
    const N: i64 = 80;
    let mut setup = DistQuerySetup::new(N, fault_w0).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let workers = format!("127.0.0.1:{},127.0.0.1:{}", setup.p0, setup.p1);
    let expected = setup.expected_rows;
    let weft = setup.weft.clone();

    let driver = tokio::task::spawn_blocking(move || {
        for _ in 0..120 {
            let out = Command::new(&weft)
                .args([
                    "driver",
                    "--workers",
                    &workers,
                    "--partial-sql",
                    "SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k",
                    "--final-sql",
                    "SELECT k, SUM(c) AS c, SUM(s) AS s FROM shuffle_input GROUP BY k",
                    "--hash-keys",
                    "0",
                ])
                .env("WEFT_TASK_MAX_RETRIES", "12")
                .output()
                .expect("run driver");
            if out.status.success() {
                let stderr = String::from_utf8_lossy(&out.stderr);
                if stderr.contains(&format!("distributed result: {expected} rows")) {
                    return true;
                }
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        false
    });

    let mut driver = Some(driver);
    let mut driver_ok = false;
    for _ in 0..240 {
        if setup.w0.try_wait().is_some() {
            setup.w0.respawn(&setup.weft);
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        if driver.as_ref().expect("driver handle").is_finished() {
            driver_ok = driver
                .take()
                .expect("driver handle")
                .await
                .expect("driver task join");
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if let Some(h) = driver {
        h.abort();
    }

    setup.w0.kill();
    setup.w1.kill();

    driver_ok
}

#[tokio::test]
async fn cli_producer_worker_kill_restart_matches_single_node() {
    let ok = run_with_worker_restart(&[
        ("WEFT_FAULT_EXIT_ON_TASK", "1"),
        ("WEFT_FAULT_EXIT_STAGE", "producer"),
    ])
    .await;
    assert!(
        ok,
        "driver must succeed after producer worker kill + restart (retry path)"
    );
}

#[tokio::test]
async fn cli_consumer_worker_kill_recompute_matches_single_node() {
    let ok = run_with_worker_restart(&[
        ("WEFT_FAULT_EXIT_ON_TASK", "1"),
        ("WEFT_FAULT_EXIT_STAGE", "consumer"),
    ])
    .await;
    assert!(
        ok,
        "driver must succeed after consumer worker kill + restart (lineage recompute path)"
    );
}
