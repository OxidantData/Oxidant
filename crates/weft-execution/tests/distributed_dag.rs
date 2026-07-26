//! Two W1b capabilities, each verified distributed == single-node:
//!
//! 1. `multi_shuffle_dag_with_intermediate_stage` — a join keyed on one column whose result is
//!    re-shuffled on a *different* group key before a final aggregate. This needs an intermediate
//!    stage that both consumes (the join inputs) and produces (the re-shuffled join output).
//! 2. `replicated_small_table_join` — the small dimension table is replicated in full on every
//!    worker, so the join runs locally with no shuffle of either base table; only the partial
//!    aggregate is shuffled.

use std::sync::Arc;

use weft_execution::driver::{run_stages, Cluster, StageDef};
use weft_execution::flight::serve_worker;
use weft_loom::arrow::array::{Int64Array, RecordBatch};
use weft_loom::arrow::datatypes::{DataType, Field, Schema};
use weft_loom::arrow::util::pretty::pretty_format_batches;
use weft_loom::Engine;

/// orders(o_orderkey, o_custkey, o_region): custkey = orderkey % `custs`, region = orderkey % 3.
fn orders(start: i64, end: i64, custs: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_region", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(start..end)),
            Arc::new(Int64Array::from_iter_values(
                (start..end).map(|i| i % custs),
            )),
            Arc::new(Int64Array::from_iter_values((start..end).map(|i| i % 3))),
        ],
    )
    .unwrap()
}

/// customer(c_custkey, c_val): val = custkey * 10.
fn customer(start: i64, end: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_val", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(start..end)),
            Arc::new(Int64Array::from_iter_values((start..end).map(|i| i * 10))),
        ],
    )
    .unwrap()
}

fn sorted_lines(batches: &[RecordBatch]) -> Vec<String> {
    let mut lines: Vec<String> = pretty_format_batches(batches)
        .unwrap()
        .to_string()
        .lines()
        .map(|s| s.to_string())
        .collect();
    lines.sort();
    lines
}

async fn run(cluster: &Cluster, stages: &[StageDef]) -> Vec<RecordBatch> {
    for _ in 0..50 {
        if let Ok(b) = run_stages(cluster, stages).await {
            return b;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("distributed run never succeeded");
}

const CUSTS: i64 = 16;
const NORD: i64 = 160;

#[tokio::test]
async fn multi_shuffle_dag_with_intermediate_stage() {
    // Ground truth: join on custkey, group by region.
    let single = Engine::new();
    single
        .register_batches("orders", vec![orders(0, NORD, CUSTS)])
        .unwrap();
    single
        .register_batches("customer", vec![customer(0, CUSTS)])
        .unwrap();
    let expected = single
        .sql(
            "SELECT o.o_region AS region, SUM(c.c_val) AS s, COUNT(*) AS n \
             FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey GROUP BY o.o_region",
        )
        .await
        .unwrap();

    // Ephemeral ports for workspace-parallel CI.
    let p0 = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let p1 = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let e0 = Arc::new(Engine::new());
    e0.register_batches("orders", vec![orders(0, NORD / 2, CUSTS)])
        .unwrap();
    e0.register_batches("customer", vec![customer(0, CUSTS / 2)])
        .unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("orders", vec![orders(NORD / 2, NORD, CUSTS)])
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

    // 0,1: shuffle each side by custkey. 2 (intermediate): join, re-shuffle by region.
    // 3 (output): aggregate by region. Stage 2 both consumes (0,1) and produces (for 3).
    let stages = vec![
        StageDef {
            stage_id: 0,
            sql: "SELECT o_orderkey, o_custkey, o_region FROM orders".into(),
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
            sql: "SELECT o.o_region AS region, c.c_val AS val \
                  FROM shuffle_input_0 o JOIN shuffle_input_1 c ON o.o_custkey = c.c_custkey"
                .into(),
            upstream_stage_ids: vec![0, 1],
            hash_key_cols: vec![0], // re-shuffle the join output by region
            ..StageDef::default()
        },
        StageDef {
            stage_id: 3,
            sql: "SELECT region, SUM(val) AS s, COUNT(*) AS n FROM shuffle_input GROUP BY region"
                .into(),
            upstream_stage_ids: vec![2],
            hash_key_cols: vec![],
            ..StageDef::default()
        },
    ];

    let actual = run(&cluster, &stages).await;
    assert_eq!(
        sorted_lines(&actual),
        sorted_lines(&expected),
        "multi-shuffle DAG must match single-node"
    );
}

#[tokio::test]
async fn replicated_small_table_join() {
    // Ground truth: join on custkey, group by custkey.
    let single = Engine::new();
    single
        .register_batches("orders", vec![orders(0, NORD, CUSTS)])
        .unwrap();
    single
        .register_batches("customer", vec![customer(0, CUSTS)])
        .unwrap();
    let expected = single
        .sql(
            "SELECT o.o_custkey AS k, SUM(c.c_val) AS s, COUNT(*) AS n \
             FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey GROUP BY o.o_custkey",
        )
        .await
        .unwrap();

    // customer is REPLICATED: the full table on every worker. orders is sharded.
    let p0 = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let p1 = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let e0 = Arc::new(Engine::new());
    e0.register_batches("orders", vec![orders(0, NORD / 2, CUSTS)])
        .unwrap();
    e0.register_batches("customer", vec![customer(0, CUSTS)])
        .unwrap(); // full copy
    let e1 = Arc::new(Engine::new());
    e1.register_batches("orders", vec![orders(NORD / 2, NORD, CUSTS)])
        .unwrap();
    e1.register_batches("customer", vec![customer(0, CUSTS)])
        .unwrap(); // full copy
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

    // Stage 0 joins the local orders shard against the full (replicated) customer — no shuffle of
    // either base table — and partial-aggregates by custkey; stage 1 recombines.
    let stages = vec![
        StageDef {
            stage_id: 0,
            sql: "SELECT o.o_custkey AS k, SUM(c.c_val) AS s, COUNT(*) AS n \
                  FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey GROUP BY o.o_custkey"
                .into(),
            upstream_stage_ids: vec![],
            hash_key_cols: vec![0],
            ..StageDef::default()
        },
        StageDef {
            stage_id: 1,
            sql: "SELECT k, SUM(s) AS s, SUM(n) AS n FROM shuffle_input GROUP BY k".into(),
            upstream_stage_ids: vec![0],
            hash_key_cols: vec![],
            ..StageDef::default()
        },
    ];

    let actual = run(&cluster, &stages).await;
    assert_eq!(
        sorted_lines(&actual),
        sorted_lines(&expected),
        "replicated-join must match single-node"
    );
}
