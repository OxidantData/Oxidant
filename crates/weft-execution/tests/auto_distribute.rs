//! The auto-splitter: `plan_distributed` derives the partial/final stage SQL from a query, and the
//! distributed result (gathered + optional global finalize) must equal single-node, for a range of
//! grouped-aggregation shapes (SUM/COUNT/MIN/MAX/AVG, COUNT(DISTINCT), ORDER BY/LIMIT).

use std::sync::Arc;

use weft_execution::driver::{run_stages, Cluster};
use weft_execution::flight::serve_worker;
use weft_execution::plan::plan_distributed;
use weft_loom::arrow::array::{Int64Array, RecordBatch};
use weft_loom::arrow::datatypes::{DataType, Field, Schema};
use weft_loom::arrow::util::pretty::pretty_format_batches;
use weft_loom::Engine;

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
    let mut lines: Vec<String> = show(batches).lines().map(|s| s.to_string()).collect();
    lines.sort();
    lines
}

struct Cluster2 {
    cluster: Cluster,
}

async fn two_workers(base: u16) -> Cluster2 {
    const N: i64 = 300;
    const G: i64 = 12;
    let (p0, p1) = (base, base + 1);
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
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
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
    let (p0, p1) = (50621u16, 50622u16);
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
        let k = full.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let n = full.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
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

    let (p0, p1) = (50631u16, 50632u16);
    let e0 = Arc::new(Engine::new());
    e0.register_batches("t", vec![batch(0, 100, G)]).unwrap();
    e0.register_batches("dim", vec![dim_shard(G, 0, G / 2)]).unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("t", vec![batch(100, 200, G)]).unwrap();
    e1.register_batches("dim", vec![dim_shard(G, G / 2, G)]).unwrap();
    tokio::spawn(async move {
        let _ = serve_worker(p0, e0).await;
    });
    tokio::spawn(async move {
        let _ = serve_worker(p1, e1).await;
    });
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
    for _ in 0..150 {
        if let Ok(b) = run_stages(&cluster, &dq.stages).await {
            gathered = Some(b);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let actual = gathered.expect("distributed shuffle join never succeeded");
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
    let k = full.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let n = full.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
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
    let k = full.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let n = full.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
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
    single.register_batches("t", vec![batch(0, 200, G)]).unwrap();
    single.register_batches("dim", vec![dim(G)]).unwrap();
    single.register_batches("dim2", vec![dim2(G)]).unwrap();
    let expected = single.sql(sql).await.unwrap();

    let (p0, p1) = (50701u16, 50702u16);
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
    single.register_batches("t", vec![batch(0, 200, G)]).unwrap();
    single.register_batches("dim", vec![dim(G)]).unwrap();
    single.register_batches("dim2", vec![dim2(G)]).unwrap();
    let expected = single.sql(sql).await.unwrap();

    let (p0, p1) = (50711u16, 50712u16);
    let e0 = Arc::new(Engine::new());
    e0.register_batches("t", vec![batch(0, 100, G)]).unwrap();
    e0.register_batches("dim", vec![dim_shard(G, 0, G / 2)]).unwrap();
    e0.register_batches("dim2", vec![dim2_shard(G, 0, G / 2)]).unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("t", vec![batch(100, 200, G)]).unwrap();
    e1.register_batches("dim", vec![dim_shard(G, G / 2, G)]).unwrap();
    e1.register_batches("dim2", vec![dim2_shard(G, G / 2, G)]).unwrap();
    tokio::spawn(async move {
        let _ = serve_worker(p0, e0).await;
    });
    tokio::spawn(async move {
        let _ = serve_worker(p1, e1).await;
    });
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
async fn two_workers_with_dim(base: u16) -> (Cluster, Engine) {
    const N: i64 = 300;
    const G: i64 = 12;
    let (p0, p1) = (base, base + 1);
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
    let cluster = Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ]);
    let planner = Engine::new();
    planner
        .register_batches("t", vec![batch(0, N, G)])
        .unwrap();
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
    let mut out = None;
    for _ in 0..150 {
        match run_stages(cluster, &dq.stages).await {
            Ok(b) => {
                out = Some(b);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
    let gathered = out.expect("distributed run never succeeded");
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
    let (cluster, planner) = two_workers_with_dim(50661).await;
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
    let (cluster, planner) = two_workers_with_dim(50671).await;
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
    let (cluster, planner) = two_workers_with_dim(50681).await;
    let expected = planner.sql(sql).await.unwrap();
    let actual = run_auto_replicated(&cluster, &planner, sql, &["dim"]).await;
    assert_eq!(
        sorted_lines(&actual),
        sorted_lines(&expected),
        "scalar subquery over replicated dim must equal single-node"
    );
}

#[tokio::test]
async fn subquery_over_unreplicated_table_is_rejected() {
    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 60, 12)])
        .unwrap();
    single.register_batches("dim", vec![dim(12)]).unwrap();
    let err = plan_distributed(
        &single,
        "SELECT k, SUM(v) AS sv FROM t WHERE k IN (SELECT d_key FROM dim) GROUP BY k",
        &[], // dim not replicated
    )
    .await;
    let msg = format!("{}", err.expect_err("must reject unreplicated subquery"));
    assert!(
        msg.contains("replicated") || msg.contains("subquery"),
        "expected subquery/replicated rejection, got: {msg}"
    );
}

#[tokio::test]
async fn subquery_over_sharded_table_is_rejected() {
    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 60, 12)])
        .unwrap();
    let err = plan_distributed(
        &single,
        "SELECT k, SUM(v) AS sv FROM t WHERE k IN (SELECT k FROM t WHERE v > 10) GROUP BY k",
        &[],
    )
    .await;
    let msg = format!("{}", err.expect_err("must reject sharded self-subquery"));
    assert!(
        msg.contains("scanned") || msg.contains("broadcast-safe") || msg.contains("subquery"),
        "expected sharded-subquery rejection, got: {msg}"
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
async fn union_distinct_is_rejected() {
    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 60, 12)])
        .unwrap();
    let err = plan_distributed(
        &single,
        "SELECT k, SUM(v) AS sv FROM t GROUP BY k \
         UNION \
         SELECT k, SUM(v) AS sv FROM t WHERE v > 10 GROUP BY k",
        &[],
    )
    .await;
    let msg = format!("{}", err.expect_err("UNION distinct must be rejected"));
    assert!(
        msg.contains("UNION") || msg.contains("distinct"),
        "expected UNION distinct message, got: {msg}"
    );
}

#[tokio::test]
async fn partition_by_sum_window_auto_distributes() {
    assert_matches(
        50695,
        "SELECT k, SUM(v) OVER (PARTITION BY k) AS sv FROM t",
        false,
    )
    .await;
}

#[tokio::test]
async fn window_without_partition_by_is_rejected() {
    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 60, 12)])
        .unwrap();
    let err = plan_distributed(
        &single,
        "SELECT SUM(v) OVER () AS sv FROM t",
        &[],
    )
    .await;
    let msg = format!("{}", err.expect_err("global window must be rejected"));
    assert!(
        msg.contains("PARTITION BY") || msg.contains("window"),
        "expected PARTITION BY / window rejection, got: {msg}"
    );
}

#[tokio::test]
async fn row_number_window_is_rejected() {
    let single = Engine::new();
    single
        .register_batches("t", vec![batch(0, 60, 12)])
        .unwrap();
    let err = plan_distributed(
        &single,
        "SELECT k, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rn FROM t",
        &[],
    )
    .await;
    let msg = format!("{}", err.expect_err("ROW_NUMBER must be rejected"));
    assert!(
        msg.contains("window") || msg.contains("ORDER BY") || msg.contains("ROW_NUMBER"),
        "expected ranking window rejection, got: {msg}"
    );
}
