//! The headline 1.5b test: a 2-worker GROUP BY through a real hash shuffle must match the
//! single-node result row-for-row.

use std::sync::Arc;

use weft_execution::driver::{run_distributed, Cluster, DistributedPlan};
use weft_execution::flight::serve_worker;
use weft_loom::arrow::array::Int64Array;
use weft_loom::arrow::datatypes::{DataType, Field, Schema};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;

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

/// Extract (k, c, s) rows and sort by k for order-insensitive comparison.
fn rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let c = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let s = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((k.value(i), c.value(i), s.value(i)));
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn two_worker_groupby_matches_single_node() {
    const N: i64 = 100;
    let query = "SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k";

    // Single-node ground truth over the whole dataset.
    let single = Engine::new();
    single
        .register_batches("t", vec![make_batch(0, N)])
        .unwrap();
    let expected = rows(&single.sql(query).await.unwrap());

    // Ephemeral ports avoid collisions with weft-connect tests under workspace CI.
    let p0 = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let p1 = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let e0 = Arc::new(Engine::new());
    e0.register_batches("t", vec![make_batch(0, N / 2)])
        .unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("t", vec![make_batch(N / 2, N)])
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
    let plan = DistributedPlan {
        partial_sql: "SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k".into(),
        final_sql: "SELECT k, SUM(c) AS c, SUM(s) AS s FROM shuffle_input GROUP BY k".into(),
        hash_key_cols: vec![0],
    };

    // Retry until both workers are up and the distributed query returns.
    let mut actual = None;
    for _ in 0..50 {
        match run_distributed(&cluster, &plan).await {
            Ok(b) => {
                actual = Some(rows(&b));
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
    let actual = actual.expect("distributed query never succeeded");

    assert_eq!(
        actual, expected,
        "distributed result must equal single-node"
    );
}
