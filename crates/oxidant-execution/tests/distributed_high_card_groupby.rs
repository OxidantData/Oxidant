//! KAN-32: a high-cardinality GROUP BY in the TPC-H Q18 stage shape (equijoin + partial
//! aggregate + HAVING-filtered semi-shuffle + combine aggregate) must complete and match
//! single-node exactly, with `OXIDANT_SHUFFLE_PARTITIONS` *greater* than the worker count.
//!
//! This exercises three KAN-32 fixes at once:
//! - intermediate (consume + produce) stages dispatch one task per shuffle partition, so
//!   upstream buckets `workers..np-1` are not silently dropped (pre-fix this test's result
//!   was a fraction of the truth);
//! - producer stages stream their output into the spill-aware bucket cache instead of
//!   materializing two full copies in worker memory;
//! - the spilled cache still serves exact reads, proven by forcing an 8 MiB spill threshold.

use std::sync::Arc;
use std::time::Duration;

use oxidant_execution::driver::{run_stages, Cluster, StageDef};
use oxidant_execution::flight::{serve_worker, serve_worker_with_spill};
use oxidant_execution::shuffle::spill::SpillStore;
use oxidant_loom::arrow::array::{Float64Array, Int64Array};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;

const BATCH: i64 = 8192;

fn ephemeral_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_totalprice", DataType::Float64, false),
    ]))
}

fn lineitem_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_quantity", DataType::Int64, false),
    ]))
}

/// `orders`: keys 1..=n (unique per order, the high-cardinality group key).
fn orders_batches(start: i64, end: i64) -> Vec<RecordBatch> {
    let schema = orders_schema();
    let mut out = Vec::new();
    let mut s = start;
    while s < end {
        let e = (s + BATCH).min(end);
        let keys: Vec<i64> = (s..e).collect();
        let prices: Vec<f64> = (s..e).map(|k| (k % 1000) as f64 + 0.5).collect();
        out.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(keys)),
                    Arc::new(Float64Array::from(prices)),
                ],
            )
            .unwrap(),
        );
        s = e;
    }
    out
}

/// `lineitem`: 4 rows per order; quantity varies so `HAVING sum(l_quantity) > 300` keeps a
/// subset of orders.
fn lineitem_batches(start: i64, end: i64) -> Vec<RecordBatch> {
    let schema = lineitem_schema();
    let mut out = Vec::new();
    let mut keys: Vec<i64> = Vec::new();
    let mut qtys: Vec<i64> = Vec::new();
    for k in start..end {
        for j in 0..4 {
            keys.push(k);
            qtys.push((k + j) % 150);
            if keys.len() as i64 == BATCH {
                out.push(
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(Int64Array::from(std::mem::take(&mut keys))),
                            Arc::new(Int64Array::from(std::mem::take(&mut qtys))),
                        ],
                    )
                    .unwrap(),
                );
            }
        }
    }
    if !keys.is_empty() {
        out.push(
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(keys)),
                    Arc::new(Int64Array::from(qtys)),
                ],
            )
            .unwrap(),
        );
    }
    out
}

/// (o_orderkey, o_totalprice, sum_qty) rows, sorted for order-insensitive comparison.
fn result_rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let p = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let s = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            // Prices are exact k%1000+0.5 fractions — bit-exact comparison is safe.
            out.push((k.value(i), p.value(i).to_bits() as i64, s.value(i)));
        }
    }
    out.sort();
    out
}

/// The Q18 stage shape over `orders`/`lineitem` (see `OXIDANT_TPCH_DEBUG=1` output of
/// `oxidant-bench tpch-distributed`): leaf partial-agg → HAVING semi-filter → leaf join →
/// partial group-by with IN semi-join → combine. Two intermediate stages, so a partition
/// count above the worker count only stays exact with per-partition dispatch.
fn q18_shape_stages() -> Vec<StageDef> {
    vec![
        StageDef::new(
            0,
            "SELECT lineitem.l_orderkey AS k0, sum(lineitem.l_quantity) AS a0 \
             FROM lineitem GROUP BY lineitem.l_orderkey",
            vec![],
            vec![0],
        ),
        StageDef::new(
            1,
            "SELECT k0 FROM (SELECT k0, sum(a0) AS r0 FROM shuffle_input GROUP BY k0) \
             AS combined WHERE (r0 > 300)",
            vec![0],
            vec![0],
        ),
        StageDef::new(
            2,
            "SELECT orders.o_orderkey AS ok0, orders.o_totalprice AS oc3, \
             lineitem.l_quantity AS oc4 FROM orders JOIN lineitem \
             ON orders.o_orderkey = lineitem.l_orderkey",
            vec![],
            vec![0],
        ),
        StageDef::new(
            3,
            "SELECT ok0 AS g2, oc3 AS g4, sum(oc4) AS a0 FROM shuffle_input_0 AS o \
             WHERE o.ok0 IN (SELECT k0 FROM shuffle_input_1) GROUP BY ok0, oc3",
            vec![2, 1],
            vec![0, 1],
        ),
        StageDef::new(
            4,
            "SELECT g2 AS \"o_orderkey\", g4 AS \"o_totalprice\", r0 AS \"sum_qty\" FROM \
             (SELECT g2, g4, sum(a0) AS r0 FROM shuffle_input GROUP BY g2, g4) AS combined",
            vec![3],
            vec![],
        ),
    ]
}

const GROUND_TRUTH_SQL: &str = "SELECT o_orderkey, o_totalprice, sum(l_quantity) AS sum_qty \
     FROM orders JOIN lineitem ON o_orderkey = l_orderkey \
     WHERE o_orderkey IN ( \
         SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300 \
     ) \
     GROUP BY o_orderkey, o_totalprice";

async fn wait_for_workers(endpoints: &[String]) {
    for ep in endpoints {
        for _ in 0..50 {
            if oxidant_execution::flight::health_check_worker(ep.clone())
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// Tiny deterministic regression lock for the per-partition intermediate dispatch: with 16
/// partitions on 2 workers the pre-KAN-32 driver consumed only buckets 0..1 of every
/// intermediate stage's upstream and returned ~1/8 of the groups.
#[tokio::test]
async fn intermediate_stage_consumes_all_partitions() {
    const N: i64 = 64;
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");

    let single = Engine::new();
    single
        .register_batches("orders", orders_batches(1, N + 1))
        .unwrap();
    single
        .register_batches("lineitem", lineitem_batches(1, N + 1))
        .unwrap();
    let expected = result_rows(&single.sql(GROUND_TRUTH_SQL).await.unwrap());

    let (p0, p1) = (ephemeral_port(), ephemeral_port());
    let e0 = Arc::new(Engine::new());
    e0.register_batches("orders", orders_batches(1, N + 1))
        .unwrap();
    e0.register_batches("lineitem", lineitem_batches(1, N / 2 + 1))
        .unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("orders", orders_batches(1, N + 1))
        .unwrap();
    e1.register_batches("lineitem", lineitem_batches(N / 2 + 1, N + 1))
        .unwrap();
    tokio::spawn(async move {
        let _ = serve_worker(p0, e0).await;
    });
    tokio::spawn(async move {
        let _ = serve_worker(p1, e1).await;
    });

    let endpoints = vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ];
    wait_for_workers(&endpoints).await;
    let cluster = Cluster::new(endpoints);
    assert_eq!(cluster.num_partitions, 16);

    let stages = q18_shape_stages();
    let gathered = run_stages(&cluster, &stages)
        .await
        .expect("distributed Q18 shape failed");
    assert_eq!(
        result_rows(&gathered),
        expected,
        "every group must survive 16 shuffle partitions on 2 workers"
    );
}

/// The full KAN-32 bottleneck at meaningful scale: ~1.5M rows with all-unique group keys
/// (worst case for partial/combine aggregation — the partial stage reduces nothing),
/// 16 shuffle partitions on 2 workers, and an 8 MiB shuffle-spill threshold so the join
/// stage's cache spills to disk. Must complete within a bounded wall time and match
/// single-node exactly.
#[tokio::test]
async fn high_cardinality_groupby_q18_shape_at_scale() {
    const N: i64 = 300_000; // orders; lineitem = 4 × N rows
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");

    let single = Engine::new();
    single
        .register_batches("orders", orders_batches(1, N + 1))
        .unwrap();
    single
        .register_batches("lineitem", lineitem_batches(1, N + 1))
        .unwrap();
    let expected = result_rows(&single.sql(GROUND_TRUTH_SQL).await.unwrap());

    let spill_root =
        std::env::temp_dir().join(format!("oxidant-kan32-spill-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&spill_root);

    let (p0, p1) = (ephemeral_port(), ephemeral_port());
    let e0 = Arc::new(Engine::new());
    e0.register_batches("orders", orders_batches(1, N + 1))
        .unwrap();
    e0.register_batches("lineitem", lineitem_batches(1, N / 2 + 1))
        .unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("orders", orders_batches(1, N + 1))
        .unwrap();
    e1.register_batches("lineitem", lineitem_batches(N / 2 + 1, N + 1))
        .unwrap();
    let s0 = SpillStore::with_memory_limit(spill_root.join("w0"), 8 * 1024 * 1024).unwrap();
    let s1 = SpillStore::with_memory_limit(spill_root.join("w1"), 8 * 1024 * 1024).unwrap();
    tokio::spawn(async move {
        let _ = serve_worker_with_spill(p0, e0, s0, true).await;
    });
    tokio::spawn(async move {
        let _ = serve_worker_with_spill(p1, e1, s1, true).await;
    });

    let endpoints = vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ];
    wait_for_workers(&endpoints).await;
    let cluster = Cluster::new(endpoints);
    assert_eq!(cluster.num_partitions, 16);

    let stages = q18_shape_stages();
    let gathered = tokio::time::timeout(Duration::from_secs(240), run_stages(&cluster, &stages))
        .await
        .expect("Q18-shape stage DAG exceeded its 240s budget")
        .expect("distributed Q18 shape failed");
    assert_eq!(
        result_rows(&gathered),
        expected,
        "spilled high-cardinality group-by must equal single-node"
    );

    // The 8 MiB threshold is far below the join stage's output — spill must have engaged.
    let spill_files = walk_arrow_files(&spill_root);
    assert!(
        spill_files > 0,
        "expected shuffle spill files under {}",
        spill_root.display()
    );

    let _ = std::fs::remove_dir_all(&spill_root);
}

fn walk_arrow_files(dir: &std::path::Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut n = 0;
    for ent in entries.flatten() {
        let path = ent.path();
        if path.is_dir() {
            n += walk_arrow_files(&path);
        } else if path
            .file_name()
            .and_then(|s| s.to_str())
            .is_some_and(|s| s.ends_with(".arrow"))
        {
            n += 1;
        }
    }
    n
}
