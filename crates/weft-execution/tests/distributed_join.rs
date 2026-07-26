//! A distributed **shuffle hash join**: two tables, each sharded across two workers and
//! hash-partitioned on the join key, joined locally per worker, must match the single-node join
//! row-for-row. This exercises the multi-upstream consumer (`shuffle_input_0` / `shuffle_input_1`)
//! that the stage DAG generalization added.

use std::sync::Arc;

use weft_execution::driver::{run_stages, Cluster, StageDef};
use weft_execution::flight::serve_worker;
use weft_loom::arrow::array::Int64Array;
use weft_loom::arrow::datatypes::{DataType, Field, Schema};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;

/// orders(o_orderkey, o_custkey) for orderkeys in `[start, end)`, custkey = orderkey % `custs`.
fn orders(start: i64, end: i64, custs: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
    ]));
    let ok: Vec<i64> = (start..end).collect();
    let ck: Vec<i64> = (start..end).map(|i| i % custs).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ok)),
            Arc::new(Int64Array::from(ck)),
        ],
    )
    .unwrap()
}

/// customer(c_custkey, c_val) for custkeys in `[start, end)`, c_val = custkey * 10.
fn customer(start: i64, end: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_val", DataType::Int64, false),
    ]));
    let ck: Vec<i64> = (start..end).collect();
    let cv: Vec<i64> = (start..end).map(|i| i * 10).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ck)),
            Arc::new(Int64Array::from(cv)),
        ],
    )
    .unwrap()
}

/// (k, n, s) rows sorted by k, for order-insensitive comparison.
fn rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let n = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let s = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((k.value(i), n.value(i), s.value(i)));
        }
    }
    out.sort();
    out
}

// Single-node ground truth and distributed share this join+aggregate (table names match the
// per-worker base tables single-node, and the registered shuffle inputs distributed).
const SINGLE_SQL: &str = "SELECT o.o_custkey AS k, COUNT(*) AS n, SUM(c.c_val) AS s \
     FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey GROUP BY o.o_custkey";

#[tokio::test]
async fn two_worker_shuffle_join_matches_single_node() {
    const CUSTS: i64 = 20;
    const ORDERS: i64 = 200;

    // Single-node ground truth over the whole dataset.
    let single = Engine::new();
    single
        .register_batches("orders", vec![orders(0, ORDERS, CUSTS)])
        .unwrap();
    single
        .register_batches("customer", vec![customer(0, CUSTS)])
        .unwrap();
    let expected = rows(&single.sql(SINGLE_SQL).await.unwrap());

    // Two workers; each holds half of orders AND half of customer under the base table names.
    let p0 = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let p1 = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let e0 = Arc::new(Engine::new());
    e0.register_batches("orders", vec![orders(0, ORDERS / 2, CUSTS)])
        .unwrap();
    e0.register_batches("customer", vec![customer(0, CUSTS / 2)])
        .unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("orders", vec![orders(ORDERS / 2, ORDERS, CUSTS)])
        .unwrap();
    e1.register_batches("customer", vec![customer(CUSTS / 2, CUSTS)])
        .unwrap();
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

    // Stage 0: shuffle orders by o_custkey (output col index 1).
    // Stage 1: shuffle customer by c_custkey (output col index 0).
    // Stage 2: join the co-located buckets and aggregate. Same key value ⇒ same bucket on both
    // sides ⇒ every match is local to one worker.
    let stages = vec![
        StageDef {
            stage_id: 0,
            sql: "SELECT o_orderkey, o_custkey FROM orders".into(),
            upstream_stage_ids: vec![],
            hash_key_cols: vec![1],
            ..StageDef::default()
        },
        StageDef {
            stage_id: 1,
            sql: "SELECT c_custkey, c_val FROM customer".into(),
            upstream_stage_ids: vec![],
            hash_key_cols: vec![0],
            ..StageDef::default()
        },
        StageDef {
            stage_id: 2,
            sql: "SELECT o.o_custkey AS k, COUNT(*) AS n, SUM(c.c_val) AS s \
                  FROM shuffle_input_0 o JOIN shuffle_input_1 c ON o.o_custkey = c.c_custkey \
                  GROUP BY o.o_custkey"
                .into(),
            upstream_stage_ids: vec![0, 1],
            hash_key_cols: vec![],
            ..StageDef::default()
        },
    ];

    // Retry until both workers are up and the distributed query returns.
    let mut actual = None;
    for _ in 0..50 {
        match run_stages(&cluster, &stages).await {
            Ok(b) => {
                actual = Some(rows(&b));
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
    let actual = actual.expect("distributed join never succeeded");

    assert_eq!(
        actual, expected,
        "distributed shuffle-join result must equal single-node"
    );
}
