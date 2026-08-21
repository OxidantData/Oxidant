//! The auto-splitter: `plan_distributed` derives the partial/final stage SQL from a query, and the
//! distributed result (gathered + optional global finalize) must equal single-node, for a range of
//! grouped-aggregation shapes (SUM/COUNT/MIN/MAX/AVG, COUNT(DISTINCT), ORDER BY/LIMIT).

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use oxidant_execution::driver::{run_stages, Cluster};
use oxidant_execution::flight::serve_worker;
use oxidant_execution::plan::{plan_distributed, plan_distributed_logical};
use oxidant_loom::arrow::array::{Int64Array, RecordBatch};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::util::pretty::pretty_format_batches;
use oxidant_loom::Engine;

/// Serialize multi-worker outer-join tests and give each worker a fresh port. All port
/// allocation in this file goes through `unique_worker_port`: the old grab-release-rebind
/// `ephemeral_port()` raced under parallel tests (the kernel can hand the same number to
/// another socket between drop and rebind), and the previous 41000+ seed sat INSIDE the
/// Linux ephemeral source range (32768..=60999), so the harness's own outbound connections
/// could steal a worker's port — `serve_worker` swallows the EADDRINUSE and the test then
/// panicked with "wN did not bind" (~40% flake on loaded CI runners).
static OUTER_JOIN_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static OUTER_JOIN_PORT: std::sync::OnceLock<AtomicU16> = std::sync::OnceLock::new();

fn unique_worker_port() -> u16 {
    // Seed the counter exactly once, atomically (`get_or_init` blocks racing callers; the
    // earlier fetch_add-then-store init could hand out tiny pre-seed values). The base stays
    // BELOW the ephemeral floor: 10000..=24999 is clear of the ephemeral range above and the
    // fixed test ports other suites use (50571+). Seeding from the pid keeps two concurrent
    // cargo-test processes from sharing a base and reusing each other's leftover listeners.
    OUTER_JOIN_PORT
        .get_or_init(|| AtomicU16::new(10000 + (std::process::id() as u16 % 15000)))
        .fetch_add(1, Ordering::Relaxed)
}

/// rows(k, v, w) where k = i % `groups`, v = i, w = i % 7 — for grouping/aggregation.
fn batch(start: i64, end: i64, groups: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("w", DataType::Int64, false),
    ]));
    let k: Vec<i64> = (start..end).map(|i| i % groups).collect();
    let v: Vec<i64> = (start..end).collect();
    let w: Vec<i64> = (start..end).map(|i| i % 7).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(k)),
            Arc::new(Int64Array::from(v)),
            Arc::new(Int64Array::from(w)),
        ],
    )
    .unwrap()
}

/// Pretty-print batches as a stable string for comparison (handles arbitrary schemas/types).
fn show(batches: &[RecordBatch]) -> String {
    pretty_format_batches(batches).unwrap().to_string()
}

/// Sort batch rows textually for order-insensitive comparison (grouped results have no inherent
/// order); the per-line sort makes the comparison independent of worker concatenation order.
fn sorted_lines(batches: &[RecordBatch]) -> Vec<String> {
    // Zero-row batches carry no row content — since KAN-28 the distributed path returns them
    // *typed* (a schema-carrying empty batch) where single-node returns an empty vec, so
    // compare row content only.
    let batches: Vec<RecordBatch> = batches
        .iter()
        .filter(|b| b.num_rows() > 0)
        .cloned()
        .collect();
    let mut lines: Vec<String> = show(&batches).lines().map(|s| s.to_string()).collect();
    lines.sort();
    lines
}

struct Cluster2 {
    cluster: Cluster,
}

/// Block until each worker accepts TCP. `serve_worker` swallows bind errors, so without this a
/// stolen port turns into a retry loop that only ever reports "never succeeded".
async fn await_worker_bind(workers: &[(&str, u16)]) {
    for &(label, port) in workers {
        let mut ok = false;
        // 10s ceiling: loaded 4-vCPU CI runners can take far longer than the old 2s to
        // schedule the spawned worker task.
        for _ in 0..250 {
            if tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok()
            {
                ok = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        }
        assert!(ok, "{label} did not bind on port {port}");
    }
}

async fn two_workers(_base: u16) -> Cluster2 {
    const N: i64 = 300;
    const G: i64 = 12;
    // Ephemeral ports avoid cross-crate collisions under `cargo test --workspace`
    // (fixed ports previously hung for minutes on TCP connect when another suite stole them).
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    let e0 = Arc::new(Engine::new());
    e0.register_batches("t", vec![batch(0, N / 2, G)]).unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("t", vec![batch(N / 2, N, G)]).unwrap();
    tokio::spawn(async move {
        let _ = serve_worker(p0, e0).await;
    });
    tokio::spawn(async move {
        let _ = serve_worker(p1, e1).await;
    });
    Cluster2 {
        cluster: Cluster::new(vec![
            format!("http://127.0.0.1:{p0}"),
            format!("http://127.0.0.1:{p1}"),
        ]),
    }
}

/// Run `sql` distributed via the auto-splitter and return the gathered (+finalized) batches.
async fn run_auto(c2: &Cluster2, planner: &Engine, sql: &str) -> Vec<RecordBatch> {
    let dq = plan_distributed(planner, sql, &[])
        .await
        .expect("plan_distributed");
    let mut out = None;
    // Up to 15s: CI runners boot the two workers and run the multi-stage shuffle under heavy
    // parallel-test load far slower than a dev box (where this succeeds on the first try), so a
    // 5s budget flaked intermittently. Bumping the retry window keeps the gate reliable.
    for _ in 0..150 {
        match run_stages(&c2.cluster, &dq.stages).await {
            Ok(b) => {
                out = Some(b);
                break;
            }
            Err(e) => {
                eprintln!("run_stages err: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await
            }
        }
    }
    let gathered = out.expect("distributed run never succeeded");
    match &dq.finalize_sql {
        None => gathered,
        Some(fsql) => {
            // Apply the global ORDER BY / LIMIT on the driver over the gathered result.
            let fin = Engine::new();
            fin.register_batches("result", gathered).unwrap();
            fin.sql(fsql).await.expect("finalize")
        }
    }
}

async fn assert_matches(base: u16, sql: &str, ordered: bool) {
    // Single-node ground truth + a planner engine that knows the schema.
    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 300, 12)])
        .unwrap();
    let expected = single.sql(sql).await.unwrap();

    let c2 = two_workers(base).await;
    let actual = run_auto(&c2, &single, sql).await;

    if ordered {
        assert_eq!(
            show(&actual),
            show(&expected),
            "ordered distributed result must equal single-node for: {sql}"
        );
    } else {
        assert_eq!(
            sorted_lines(&actual),
            sorted_lines(&expected),
            "distributed result must equal single-node for: {sql}"
        );
    }
}

#[tokio::test]
async fn recombinable_aggregates() {
    assert_matches(
        50611,
        "SELECT k, SUM(v) AS sv, COUNT(*) AS c, MIN(v) AS mn, MAX(v) AS mx FROM t GROUP BY k",
        false,
    )
    .await;
}

#[tokio::test]
async fn avg_is_decomposed() {
    assert_matches(
        50613,
        "SELECT k, AVG(v) AS av, COUNT(*) AS c FROM t GROUP BY k",
        false,
    )
    .await;
}

#[tokio::test]
async fn count_distinct_via_raw_shuffle() {
    assert_matches(
        50615,
        "SELECT k, COUNT(DISTINCT w) AS d, COUNT(*) AS c FROM t GROUP BY k",
        false,
    )
    .await;
}

#[tokio::test]
async fn filter_then_group() {
    assert_matches(
        50617,
        "SELECT k, SUM(v) AS sv FROM t WHERE v > 50 GROUP BY k",
        false,
    )
    .await;
}

#[tokio::test]
async fn order_by_limit_is_global() {
    // The global ORDER BY + LIMIT must pick the top groups across ALL workers, not per worker.
    assert_matches(
        50619,
        "SELECT k, SUM(v) AS sv FROM t GROUP BY k ORDER BY sv DESC LIMIT 3",
        true,
    )
    .await;
}

/// dim(d_key, d_name): a small dimension table, replicated in full on every worker.
fn dim(groups: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("d_key", DataType::Int64, false),
        Field::new("d_name", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(0..groups)),
            Arc::new(Int64Array::from_iter_values((0..groups).map(|i| i * 100))),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn auto_derived_broadcast_join() {
    // A star join: sharded fact `t` (join key = its group col `k`) ⋈ replicated dim, grouped by a
    // DIMENSION column. The auto-splitter must fold the join into the partial stage (broadcast) and
    // recombine, matching single-node.
    const G: i64 = 12;
    let sql = "SELECT d.d_name AS name, SUM(t.v) AS sv, COUNT(*) AS c \
               FROM t JOIN dim d ON t.k = d.d_key GROUP BY d.d_name";

    // Ground truth.
    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 300, G)])
        .unwrap();
    single.register_batches("dim", vec![dim(G)]).unwrap();
    let expected = single.sql(sql).await.unwrap();

    // Two workers: `t` sharded, `dim` replicated in full on each.
    let p0 = unique_worker_port();
    let p1 = unique_worker_port();
    let e0 = Arc::new(Engine::new());
    e0.register_batches("t", vec![batch(0, 150, G)]).unwrap();
    e0.register_batches("dim", vec![dim(G)]).unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("t", vec![batch(150, 300, G)]).unwrap();
    e1.register_batches("dim", vec![dim(G)]).unwrap();
    tokio::spawn(async move {
        let _ = serve_worker(p0, e0).await;
    });
    tokio::spawn(async move {
        let _ = serve_worker(p1, e1).await;
    });
    await_worker_bind(&[("w0", p0), ("w1", p1)]).await;
    let cluster = Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ]);

    let dq = plan_distributed(&single, sql, &["dim"])
        .await
        .expect("plan_distributed should auto-derive the broadcast join");
    let mut gathered = None;
    // Up to 15s — see `run_auto`; the broadcast-join cluster is just as sensitive to CI load.
    for _ in 0..150 {
        if let Ok(b) = run_stages(&cluster, &dq.stages).await {
            gathered = Some(b);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let actual = gathered.expect("distributed broadcast join never succeeded");
    assert_eq!(
        sorted_lines(&actual),
        sorted_lines(&expected),
        "auto-derived broadcast join must equal single-node"
    );
}

#[tokio::test]
async fn two_sharded_tables_auto_shuffle_join() {
    // Two sharded tables → auto-derived shuffle join + aggregation must match single-node.
    const G: i64 = 12;
    let sql = "SELECT d.d_name AS name, COUNT(*) AS c, SUM(t.v) AS sv                FROM t JOIN dim d ON t.k = d.d_key GROUP BY d.d_name";

    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 200, G)])
        .unwrap();
    single.register_batches("dim", vec![dim(G)]).unwrap();
    let expected = single.sql(sql).await.unwrap();

    fn dim_shard(groups: i64, start: i64, end: i64) -> RecordBatch {
        let full = dim(groups);
        let schema = full.schema();
        let k = full
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let n = full
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut ks = Vec::new();
        let mut ns = Vec::new();
        for i in 0..full.num_rows() {
            let kv = k.value(i);
            if kv >= start && kv < end {
                ks.push(kv);
                ns.push(n.value(i));
            }
        }
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ks)),
                Arc::new(Int64Array::from(ns)),
            ],
        )
        .unwrap()
    }

    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    let e0 = Arc::new(Engine::new());
    e0.register_batches("t", vec![batch(0, 100, G)]).unwrap();
    e0.register_batches("dim", vec![dim_shard(G, 0, G / 2)])
        .unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("t", vec![batch(100, 200, G)]).unwrap();
    e1.register_batches("dim", vec![dim_shard(G, G / 2, G)])
        .unwrap();
    tokio::spawn(async move {
        let _ = serve_worker(p0, e0).await;
    });
    tokio::spawn(async move {
        let _ = serve_worker(p1, e1).await;
    });
    await_worker_bind(&[("w0", p0), ("w1", p1)]).await;
    let cluster = Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ]);

    let dq = plan_distributed(&single, sql, &[])
        .await
        .expect("plan_distributed should auto-derive the shuffle join");
    assert!(
        dq.stages.len() >= 3,
        "shuffle join should produce multi-stage DAG, got {}",
        dq.stages.len()
    );
    let mut gathered = None;
    let mut last_err = None;
    for _ in 0..150 {
        match run_stages(&cluster, &dq.stages).await {
            Ok(b) => {
                gathered = Some(b);
                break;
            }
            Err(e) => last_err = Some(e.to_string()),
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let actual = gathered.unwrap_or_else(|| {
        panic!(
            "distributed shuffle join never succeeded; last error: {}",
            last_err.as_deref().unwrap_or("<none>")
        )
    });
    assert_eq!(
        sorted_lines(&actual),
        sorted_lines(&expected),
        "auto-derived shuffle join must equal single-node"
    );
}

fn dim2(groups: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("d2_key", DataType::Int64, false),
        Field::new("d2_name", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(0..groups)),
            Arc::new(Int64Array::from_iter_values((0..groups).map(|i| i * 10))),
        ],
    )
    .unwrap()
}

fn dim2_shard(groups: i64, start: i64, end: i64) -> RecordBatch {
    let full = dim2(groups);
    let schema = full.schema();
    let k = full
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let n = full
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut ks = Vec::new();
    let mut ns = Vec::new();
    for i in 0..full.num_rows() {
        let kv = k.value(i);
        if kv >= start && kv < end {
            ks.push(kv);
            ns.push(n.value(i));
        }
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ks)),
            Arc::new(Int64Array::from(ns)),
        ],
    )
    .unwrap()
}

fn dim_shard(groups: i64, start: i64, end: i64) -> RecordBatch {
    let full = dim(groups);
    let schema = full.schema();
    let k = full
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let n = full
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut ks = Vec::new();
    let mut ns = Vec::new();
    for i in 0..full.num_rows() {
        let kv = k.value(i);
        if kv >= start && kv < end {
            ks.push(kv);
            ns.push(n.value(i));
        }
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ks)),
            Arc::new(Int64Array::from(ns)),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn multi_dim_broadcast_star_matches_single_node() {
    // One sharded fact + two replicated dims (ANSI multi-join star) folds into the partial.
    const G: i64 = 12;
    let sql = "SELECT d.d_name AS name, SUM(t.v) AS sv, COUNT(*) AS c                FROM t JOIN dim d ON t.k = d.d_key JOIN dim2 d2 ON t.k = d2.d2_key                GROUP BY d.d_name";

    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 200, G)])
        .unwrap();
    single.register_batches("dim", vec![dim(G)]).unwrap();
    single.register_batches("dim2", vec![dim2(G)]).unwrap();
    let expected = single.sql(sql).await.unwrap();

    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    let e0 = Arc::new(Engine::new());
    e0.register_batches("t", vec![batch(0, 100, G)]).unwrap();
    e0.register_batches("dim", vec![dim(G)]).unwrap();
    e0.register_batches("dim2", vec![dim2(G)]).unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("t", vec![batch(100, 200, G)]).unwrap();
    e1.register_batches("dim", vec![dim(G)]).unwrap();
    e1.register_batches("dim2", vec![dim2(G)]).unwrap();
    tokio::spawn(async move {
        let _ = serve_worker(p0, e0).await;
    });
    tokio::spawn(async move {
        let _ = serve_worker(p1, e1).await;
    });
    await_worker_bind(&[("w0", p0), ("w1", p1)]).await;
    let cluster = Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ]);

    let dq = plan_distributed(&single, sql, &["dim", "dim2"])
        .await
        .expect("multi-dim broadcast should plan");
    assert_eq!(dq.stages.len(), 2, "broadcast star is two stages");
    let mut gathered = None;
    for _ in 0..150 {
        if let Ok(b) = run_stages(&cluster, &dq.stages).await {
            gathered = Some(b);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let actual = gathered.expect("distributed multi-dim broadcast never succeeded");
    assert_eq!(
        sorted_lines(&actual),
        sorted_lines(&expected),
        "multi-dim broadcast must equal single-node"
    );
}

#[tokio::test]
async fn three_sharded_tables_left_deep_shuffle_chain() {
    // Three sharded tables → left-deep pairwise shuffle joins + agg must match single-node.
    const G: i64 = 12;
    let sql = "SELECT d.d_name AS name, COUNT(*) AS c, SUM(t.v) AS sv                FROM t JOIN dim d ON t.k = d.d_key JOIN dim2 d2 ON t.k = d2.d2_key                GROUP BY d.d_name";

    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 200, G)])
        .unwrap();
    single.register_batches("dim", vec![dim(G)]).unwrap();
    single.register_batches("dim2", vec![dim2(G)]).unwrap();
    let expected = single.sql(sql).await.unwrap();

    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    let e0 = Arc::new(Engine::new());
    e0.register_batches("t", vec![batch(0, 100, G)]).unwrap();
    e0.register_batches("dim", vec![dim_shard(G, 0, G / 2)])
        .unwrap();
    e0.register_batches("dim2", vec![dim2_shard(G, 0, G / 2)])
        .unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("t", vec![batch(100, 200, G)]).unwrap();
    e1.register_batches("dim", vec![dim_shard(G, G / 2, G)])
        .unwrap();
    e1.register_batches("dim2", vec![dim2_shard(G, G / 2, G)])
        .unwrap();
    tokio::spawn(async move {
        let _ = serve_worker(p0, e0).await;
    });
    tokio::spawn(async move {
        let _ = serve_worker(p1, e1).await;
    });
    await_worker_bind(&[("w0", p0), ("w1", p1)]).await;
    let cluster = Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ]);

    let dq = plan_distributed(&single, sql, &[])
        .await
        .expect("three-table shuffle chain should plan");
    assert!(
        dq.stages.len() >= 5,
        "left-deep chain needs intermediate stages, got {}",
        dq.stages.len()
    );
    let mut gathered = None;
    for _ in 0..150 {
        if let Ok(b) = run_stages(&cluster, &dq.stages).await {
            gathered = Some(b);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let actual = gathered.expect("distributed join chain never succeeded");
    assert_eq!(
        sorted_lines(&actual),
        sorted_lines(&expected),
        "left-deep shuffle chain must equal single-node"
    );
}

#[tokio::test]
async fn global_aggregation_auto_distributes() {
    let sql = "SELECT SUM(v) AS sv, COUNT(*) AS c FROM t";
    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 300, 12)])
        .unwrap();
    let expected = single.sql(sql).await.unwrap();

    let c2 = two_workers(50641).await;
    let actual = run_auto(&c2, &single, sql).await;
    assert_eq!(
        sorted_lines(&actual),
        sorted_lines(&expected),
        "global aggregation must equal single-node"
    );
}

#[tokio::test]
async fn having_auto_distributes() {
    assert_matches(
        50651,
        "SELECT k, SUM(v) AS sv FROM t GROUP BY k HAVING SUM(v) > 1000",
        false,
    )
    .await;
}

/// Helper: two workers with sharded `t` + full replicated `dim`.
///
/// Ephemeral ports, for the same reason [`two_workers`] uses them: a fixed port that another suite
/// on the runner already holds leaves the workers unbound, and every stage then burns the TCP
/// connect timeout instead of failing.
async fn two_workers_with_dim() -> (Cluster, Engine) {
    const N: i64 = 300;
    const G: i64 = 12;
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    let e0 = Arc::new(Engine::new());
    e0.register_batches("t", vec![batch(0, N / 2, G)]).unwrap();
    e0.register_batches("dim", vec![dim(G)]).unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("t", vec![batch(N / 2, N, G)]).unwrap();
    e1.register_batches("dim", vec![dim(G)]).unwrap();
    tokio::spawn(async move {
        let _ = serve_worker(p0, e0).await;
    });
    tokio::spawn(async move {
        let _ = serve_worker(p1, e1).await;
    });
    await_worker_bind(&[("w0", p0), ("w1", p1)]).await;
    let cluster = Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ]);
    let planner = Engine::new();
    planner.register_batches("t", vec![batch(0, N, G)]).unwrap();
    planner.register_batches("dim", vec![dim(G)]).unwrap();
    (cluster, planner)
}

async fn run_auto_replicated(
    cluster: &Cluster,
    planner: &Engine,
    sql: &str,
    replicated: &[&str],
) -> Vec<RecordBatch> {
    let dq = plan_distributed(planner, sql, replicated)
        .await
        .expect("plan_distributed");
    // The workers are already bound by the time we get here, so this only rides out the brief
    // window before they accept. Keep the last error: swallowing it turned one CI failure into
    // thirty minutes of retries reporting nothing but "never succeeded".
    let mut out = None;
    let mut last_err = None;
    for _ in 0..20 {
        match run_stages(cluster, &dq.stages).await {
            Ok(b) => {
                out = Some(b);
                break;
            }
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
    let gathered = out.unwrap_or_else(|| {
        panic!("distributed run never succeeded; last error: {last_err:?}");
    });
    match &dq.finalize_sql {
        None => gathered,
        Some(fsql) => {
            let fin = Engine::new();
            fin.register_batches("result", gathered).unwrap();
            fin.sql(fsql).await.expect("finalize")
        }
    }
}

#[tokio::test]
async fn in_subquery_over_replicated_dim() {
    let sql = "SELECT k, SUM(v) AS sv FROM t WHERE k IN (SELECT d_key FROM dim WHERE d_name >= 0) GROUP BY k";
    let (cluster, planner) = two_workers_with_dim().await;
    let expected = planner.sql(sql).await.unwrap();
    let actual = run_auto_replicated(&cluster, &planner, sql, &["dim"]).await;
    assert_eq!(
        sorted_lines(&actual),
        sorted_lines(&expected),
        "IN subquery over replicated dim must equal single-node"
    );
}

#[tokio::test]
async fn exists_subquery_over_replicated_dim() {
    let sql = "SELECT k, SUM(v) AS sv FROM t \
               WHERE EXISTS (SELECT 1 FROM dim d WHERE d.d_key = t.k AND d.d_name >= 0) \
               GROUP BY k";
    let (cluster, planner) = two_workers_with_dim().await;
    let expected = planner.sql(sql).await.unwrap();
    let actual = run_auto_replicated(&cluster, &planner, sql, &["dim"]).await;
    assert_eq!(
        sorted_lines(&actual),
        sorted_lines(&expected),
        "EXISTS over replicated dim must equal single-node"
    );
}

#[tokio::test]
async fn scalar_subquery_over_replicated_dim() {
    let sql = "SELECT k, SUM(v) AS sv FROM t \
               WHERE v > (SELECT AVG(d_name) FROM dim) GROUP BY k";
    let (cluster, planner) = two_workers_with_dim().await;
    let expected = planner.sql(sql).await.unwrap();
    let actual = run_auto_replicated(&cluster, &planner, sql, &["dim"]).await;
    assert_eq!(
        sorted_lines(&actual),
        sorted_lines(&expected),
        "scalar subquery over replicated dim must equal single-node"
    );
}

#[tokio::test]
async fn subquery_over_unreplicated_table_materializes_dim() {
    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 60, 12)])
        .unwrap();
    single.register_batches("dim", vec![dim(12)]).unwrap();
    let lp = single
        .logical_plan("SELECT k, SUM(v) AS sv FROM t WHERE k IN (SELECT d_key FROM dim) GROUP BY k")
        .await
        .unwrap();
    // Dim is not in the replicated set, so the planner gathers it into a stage and rewrites the
    // IN-subquery against `shuffle_input_N` rather than declining.
    let dq = plan_distributed_logical(&lp, &[] /* dim not replicated */)
        .expect("unreplicated dim subquery should materialize");
    assert!(
        dq.stages
            .iter()
            .any(|s| s.sql.contains("__oxidant_subquery_gate") || s.sql.contains("FROM dim")),
        "expected a dim gather / subquery-gate stage, got: {dq:?}"
    );
    assert!(
        dq.stages.iter().any(|s| s.sql.contains("shuffle_input_")),
        "final stage must read the materialized dim via shuffle_input: {dq:?}"
    );
}

#[tokio::test]
async fn two_sharded_tables_shuffle_join_plans() {
    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 60, 12)])
        .unwrap();
    single.register_batches("dim", vec![dim(12)]).unwrap();
    let plan = plan_distributed(
        &single,
        "SELECT d.d_key AS name, COUNT(*) AS c FROM t JOIN dim d ON t.k = d.d_key GROUP BY d.d_key",
        &[],
    )
    .await;
    assert!(
        plan.is_ok(),
        "two-sharded shuffle join should auto-derive: {plan:?}"
    );
    assert_eq!(plan.unwrap().stages.len(), 4);
}

#[tokio::test]
async fn shuffle_join_conjunction_keeps_residual_in_stage_sql() {
    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 60, 12)])
        .unwrap();
    single.register_batches("dim", vec![dim(12)]).unwrap();
    let lp = single
        .logical_plan(
            "SELECT d.d_key, COUNT(*) \
             FROM t JOIN dim d ON t.k = d.d_key AND t.v > d.d_name \
             GROUP BY d.d_key",
        )
        .await
        .unwrap();

    let dq = plan_distributed_logical(&lp, &[])
        .expect("equality plus residual predicate should plan as a shuffle join");
    let join_stage = dq
        .stages
        .iter()
        .find(|stage| stage.upstream_stage_ids.len() == 2)
        .expect("shuffle plan should contain a two-input join stage");

    assert!(
        join_stage.sql.contains(" WHERE "),
        "residual predicate must remain as a post-join filter: {}",
        join_stage.sql
    );
}

#[tokio::test]
async fn shuffle_join_multi_key_hashes_composite() {
    // KAN-10 / D-2.7: composite equijoin keys must plan (hash both columns).
    let single = Engine::new();
    // Build a fact with (k, k2) so both join keys are real columns.
    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("k2", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let fact = RecordBatch::try_new(
        fact_schema,
        vec![
            Arc::new(Int64Array::from(
                (0..60).map(|i| i % 12).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (0..60).map(|i| (i % 12) * 10).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from((0..60).collect::<Vec<_>>())),
        ],
    )
    .unwrap();
    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("d_key", DataType::Int64, false),
        Field::new("d_key2", DataType::Int64, false),
    ]));
    let dim_batch = RecordBatch::try_new(
        dim_schema,
        vec![
            Arc::new(Int64Array::from((0..12).collect::<Vec<_>>())),
            Arc::new(Int64Array::from(
                (0..12).map(|i| i * 10).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    single.register_batches("t", vec![fact]).unwrap();
    single.register_batches("dim2", vec![dim_batch]).unwrap();
    let plan = plan_distributed(
        &single,
        "SELECT d.d_key, COUNT(*) AS c \
         FROM t JOIN dim2 d ON t.k = d.d_key AND t.k2 = d.d_key2 \
         GROUP BY d.d_key",
        &[],
    )
    .await;
    let dq = plan.expect("multi-key shuffle join should plan");
    let producers: Vec<_> = dq
        .stages
        .iter()
        .filter(|s| s.upstream_stage_ids.is_empty() && !s.hash_key_cols.is_empty())
        .collect();
    assert!(
        producers.iter().any(|s| s.hash_key_cols.len() >= 2),
        "leaf producers must hash the composite key: {:?}",
        producers
            .iter()
            .map(|s| &s.hash_key_cols)
            .collect::<Vec<_>>()
    );
    assert!(
        dq.stages
            .iter()
            .any(|s| s.sql.contains(" AND ") && s.upstream_stage_ids.len() == 2),
        "join stage ON must AND both keys: {:?}",
        dq.stages.iter().map(|s| &s.sql).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn subquery_alias_around_join_chain_is_peeled() {
    // KAN-11: SubqueryAlias wrapping a left-deep join must not reject the chain.
    let single = Engine::new();
    single.register_batches("t", vec![batch(0, 40, 8)]).unwrap();
    single.register_batches("dim", vec![dim(8)]).unwrap();
    let plan = plan_distributed(
        &single,
        "SELECT name, SUM(c) FROM (\
            SELECT d.d_key AS name, COUNT(*) AS c \
            FROM t JOIN dim d ON t.k = d.d_key \
            GROUP BY d.d_key\
         ) x GROUP BY name",
        &[],
    )
    .await;
    // Outer aggregate-over-aggregate may gather or reject stacked aggs; the inner aliased
    // join itself must not fail solely for SubqueryAlias.
    if let Err(e) = &plan {
        let msg = e.to_string();
        assert!(
            !msg.contains("SubqueryAlias"),
            "must not reject solely for SubqueryAlias: {msg}"
        );
    }
}

/// Flattened leaf schemas matching `leaf_stage_sql` output (`alias__col`).
fn flat_shuffle_inputs(nullable_keys: bool) -> (RecordBatch, RecordBatch) {
    let left = {
        let schema = Arc::new(Schema::new(vec![
            Field::new("t__k", DataType::Int64, nullable_keys),
            Field::new("t__v", DataType::Int64, false),
            Field::new("t__w", DataType::Int64, false),
        ]));
        let k = if nullable_keys {
            Int64Array::from(vec![Some(0i64), None])
        } else {
            Int64Array::from(vec![0i64, 1])
        };
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(k),
                Arc::new(Int64Array::from(vec![10i64, 12])),
                Arc::new(Int64Array::from(vec![0i64, 1])),
            ],
        )
        .unwrap()
    };
    let right = {
        let schema = Arc::new(Schema::new(vec![
            Field::new("d__d_key", DataType::Int64, nullable_keys),
            Field::new("d__d_name", DataType::Int64, false),
        ]));
        let k = if nullable_keys {
            Int64Array::from(vec![Some(1i64), None])
        } else {
            Int64Array::from(vec![0i64, 1])
        };
        RecordBatch::try_new(
            schema,
            vec![Arc::new(k), Arc::new(Int64Array::from(vec![100i64, 200]))],
        )
        .unwrap()
    };
    (left, right)
}

async fn assert_join_stages_reparse(dq: &oxidant_execution::plan::DistributedQuery, label: &str) {
    let (left, right) = flat_shuffle_inputs(true);
    let probe = Engine::new();
    probe
        .register_batches("shuffle_input_0", vec![left])
        .unwrap();
    probe
        .register_batches("shuffle_input_1", vec![right])
        .unwrap();
    for s in &dq.stages {
        if s.upstream_stage_ids.len() == 2 {
            probe
                .sql(&s.sql)
                .await
                .unwrap_or_else(|e| panic!("{label} stage SQL re-parse failed: {e}\n{}", s.sql));
        }
    }
}

/// LEFT / FULL / ANTI shuffle joins: one cluster, equality vs single-node.
/// Uses pid-seeded ports (not ephemeral bind/drop) — TOCTOU otherwise connects to leftover
/// listeners and silently drops a shuffle partition (odd keys missing).
/// NULL-key co-location is covered by `null_join_keys_are_hashed_not_dropped`.
#[tokio::test]
async fn two_sharded_tables_left_outer_shuffle_join() {
    let _guard = OUTER_JOIN_TEST_LOCK.lock().await;
    const G: i64 = 12;
    let dim_all = vec![
        dim_shard(G, 0, G / 2),
        dim_shard(G + G / 4, G, G + G / 4), // right-only keys for FULL
    ];

    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 200, G)])
        .unwrap();
    single.register_batches("dim", dim_all).unwrap();

    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    let e0 = Arc::new(Engine::new());
    e0.register_batches("t", vec![batch(0, 100, G)]).unwrap();
    e0.register_batches("dim", vec![dim_shard(G, 0, G / 4)])
        .unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("t", vec![batch(100, 200, G)]).unwrap();
    e1.register_batches(
        "dim",
        vec![
            dim_shard(G, G / 4, G / 2),
            dim_shard(G + G / 4, G, G + G / 4),
        ],
    )
    .unwrap();
    tokio::spawn(async move {
        let _ = serve_worker(p0, e0).await;
    });
    tokio::spawn(async move {
        let _ = serve_worker(p1, e1).await;
    });
    await_worker_bind(&[("w0", p0), ("w1", p1)]).await;
    let cluster = Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ]);

    for (sql, stage_kw) in [
        (
            "SELECT t.k AS k, COUNT(*) AS c, SUM(COALESCE(d.d_name, 0)) AS sv \
             FROM t LEFT JOIN dim d ON t.k = d.d_key GROUP BY t.k",
            "LEFT JOIN",
        ),
        (
            "SELECT t.k AS k, COUNT(*) AS c, SUM(COALESCE(d.d_name, 0)) AS sv \
             FROM t FULL OUTER JOIN dim d ON t.k = d.d_key GROUP BY t.k",
            "FULL OUTER JOIN",
        ),
        (
            "SELECT t.k AS k, COUNT(*) AS c, SUM(t.v) AS sv \
             FROM t LEFT ANTI JOIN dim d ON t.k = d.d_key GROUP BY t.k",
            "LEFT ANTI JOIN",
        ),
    ] {
        let expected = single.sql(sql).await.unwrap();
        let dq = plan_distributed_logical(&single.logical_plan(sql).await.unwrap(), &[])
            .unwrap_or_else(|e| panic!("{stage_kw} should plan: {e}"));
        assert!(
            dq.stages.iter().any(|s| s.sql.contains(stage_kw)),
            "expected {stage_kw} in stage SQL, got: {:?}",
            dq.stages.iter().map(|s| &s.sql).collect::<Vec<_>>()
        );
        assert_join_stages_reparse(&dq, stage_kw).await;

        let mut last = Vec::new();
        let mut ok = false;
        for _ in 0..150 {
            if let Ok(b) = run_stages(&cluster, &dq.stages).await {
                last = sorted_lines(&b);
                if last == sorted_lines(&expected) {
                    ok = true;
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(
            ok,
            "{stage_kw} shuffle must equal single-node\n got: {last:?}\n exp: {:?}",
            sorted_lines(&expected)
        );
    }
}

#[tokio::test]
async fn full_then_inner_chain_coalesces_carried_join_key() {
    // Blocker 1: after FULL, intermediate projection must COALESCE the carried key so the next
    // shuffle still colocates unmatched-right rows.
    let single = Engine::new();
    single.register_batches("t", vec![batch(0, 40, 8)]).unwrap();
    single.register_batches("dim", vec![dim(8)]).unwrap();
    single.register_batches("dim2", vec![dim2(8)]).unwrap();
    let sql = "SELECT t.k AS k, COUNT(*) AS c \
               FROM t FULL OUTER JOIN dim d ON t.k = d.d_key \
               JOIN dim2 d2 ON t.k = d2.d2_key \
               GROUP BY t.k";
    let dq = plan_distributed_logical(&single.logical_plan(sql).await.unwrap(), &[])
        .expect("FULL then JOIN chain should plan");
    let intermediate = dq
        .stages
        .iter()
        .find(|s| {
            s.upstream_stage_ids.len() == 2
                && s.hash_key_cols.len() == 1
                && !s.sql.contains("GROUP BY")
        })
        .unwrap_or_else(|| {
            panic!(
                "expected intermediate join stage, stages={:?}",
                dq.stages.iter().map(|s| &s.sql).collect::<Vec<_>>()
            )
        });
    assert!(
        intermediate.sql.to_uppercase().contains("COALESCE"),
        "FULL intermediate must COALESCE carried key, got:\n{}",
        intermediate.sql
    );
    assert!(
        intermediate.sql.contains("FULL OUTER JOIN"),
        "expected FULL OUTER JOIN in intermediate, got:\n{}",
        intermediate.sql
    );
}

#[tokio::test]
async fn subquery_over_sharded_table_plans_semi_shuffle() {
    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 60, 12)])
        .unwrap();
    let lp = single
        .logical_plan(
            "SELECT k, SUM(v) AS sv FROM t WHERE k IN (SELECT k FROM t WHERE v > 10) GROUP BY k",
        )
        .await
        .unwrap();
    // KAN-29: an uncorrelated self-IN over the sharded fact no longer gathers the whole fact to
    // partition 0 — the IN list is a key producer hash-shuffled by `k`, co-located with the
    // outer scan, and the semi join feeds the ordinary partial/combine aggregation.
    let dq = plan_distributed_logical(&lp, &[]).expect("sharded self-IN should plan semi shuffle");
    assert_eq!(
        dq.stages.len(),
        4,
        "key producer -> outer scan -> semi+partial -> combine: {dq:?}"
    );
    assert_eq!(dq.stages[0].hash_key_cols, vec![0]);
    assert!(
        dq.stages[0]
            .sql
            .contains("SELECT DISTINCT t.k AS k0 FROM t WHERE"),
        "{}",
        dq.stages[0].sql
    );
    assert!(
        dq.stages[2]
            .sql
            .contains("o.ok0 IN (SELECT k0 FROM shuffle_input_1)"),
        "semi stage must evaluate the IN against the co-located key stream: {}",
        dq.stages[2].sql
    );
}

#[tokio::test]
async fn union_all_of_two_aggs() {
    let sql = "SELECT k, SUM(v) AS sv FROM t GROUP BY k \
               UNION ALL \
               SELECT k, SUM(v) AS sv FROM t WHERE v > 50 GROUP BY k";
    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 300, 12)])
        .unwrap();
    let expected = single.sql(sql).await.unwrap();

    let c2 = two_workers(50691).await;
    let actual = run_auto(&c2, &single, sql).await;
    assert_eq!(
        sorted_lines(&actual),
        sorted_lines(&expected),
        "UNION ALL of two aggs must equal single-node"
    );
}

#[tokio::test]
async fn union_distinct_of_two_aggs() {
    let sql = "SELECT k, SUM(v) AS sv FROM t GROUP BY k \
               UNION \
               SELECT k, SUM(v) AS sv FROM t WHERE v > 50 GROUP BY k";
    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 300, 12)])
        .unwrap();
    let expected = single.sql(sql).await.unwrap();

    let c2 = two_workers(50692).await;
    let actual = run_auto(&c2, &single, sql).await;
    assert_eq!(
        sorted_lines(&actual),
        sorted_lines(&expected),
        "UNION distinct of two aggs must equal single-node"
    );
}

#[tokio::test]
async fn except_of_two_aggs() {
    let sql = "SELECT k FROM t GROUP BY k \
               EXCEPT \
               SELECT k FROM t WHERE v > 100 GROUP BY k";
    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 300, 12)])
        .unwrap();
    let expected = single.sql(sql).await.unwrap();

    let c2 = two_workers(50693).await;
    let actual = run_auto(&c2, &single, sql).await;
    assert_eq!(
        sorted_lines(&actual),
        sorted_lines(&expected),
        "EXCEPT of two aggs must equal single-node"
    );
}

#[tokio::test]
async fn partition_by_sum_window_auto_distributes() {
    let sql = "SELECT k, SUM(v) OVER (PARTITION BY k) AS sv FROM t";
    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 300, 12)])
        .unwrap();
    let dq = plan_distributed(&single, sql, &[])
        .await
        .expect("window should plan");
    assert_eq!(dq.stages.len(), 2);
    // Catch relation-qualified stage SQL before paying for a 2-worker cluster.
    let local = Engine::new();
    local
        .register_batches("shuffle_input", vec![batch(0, 50, 12)])
        .unwrap();
    local.sql(&dq.stages[1].sql).await.unwrap_or_else(|e| {
        panic!(
            "stage1 SQL invalid: {e}
{}",
            dq.stages[1].sql
        )
    });
    assert_matches(50695, sql, false).await;
}

#[tokio::test]
async fn window_without_partition_by_is_rejected() {
    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 60, 12)])
        .unwrap();
    let lp = single
        .logical_plan("SELECT SUM(v) OVER () AS sv FROM t")
        .await
        .unwrap();
    let err = plan_distributed_logical(&lp, &[]);
    let msg = format!("{}", err.expect_err("global window must be rejected"));
    assert!(
        msg.contains("PARTITION BY") || msg.contains("window"),
        "expected PARTITION BY / window rejection, got: {msg}"
    );
}

#[tokio::test]
async fn row_number_window_gathers_then_ranks() {
    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 60, 12)])
        .unwrap();
    let lp = single
        .logical_plan("SELECT k, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rn FROM t")
        .await
        .unwrap();
    // Ranking windows need global order within each partition, so the planner gathers the
    // sharded fact and applies ROW_NUMBER on the combined input.
    let dq = plan_distributed_logical(&lp, &[]).expect("ROW_NUMBER should gather then rank");
    assert!(
        dq.stages.iter().any(|s| {
            s.sql.to_lowercase().contains("row_number()") && s.sql.contains("shuffle_input_")
        }),
        "ranking stage must run ROW_NUMBER over gathered shuffle input: {dq:?}"
    );
}
