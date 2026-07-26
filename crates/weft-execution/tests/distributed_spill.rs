//! Force shuffle spill via `WEFT_SHUFFLE_SPILL_DIR` and assert the distributed GROUP BY still
//! matches the single-node result (gap item 10).

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

fn spill_file_count(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .map(|entries| entries.filter_map(Result::ok).count())
        .unwrap_or(0)
}

#[tokio::test]
async fn two_worker_groupby_with_forced_spill() {
    const N: i64 = 100;
    let query = "SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k";

    let single = Engine::new();
    single
        .register_batches("t", vec![make_batch(0, N)])
        .unwrap();
    let expected = rows(&single.sql(query).await.unwrap());

    let spill0 = std::env::temp_dir().join(format!("weft-spill-test-0-{}", std::process::id()));
    let spill1 = std::env::temp_dir().join(format!("weft-spill-test-1-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&spill0);
    let _ = std::fs::remove_dir_all(&spill1);
    std::fs::create_dir_all(&spill0).unwrap();
    std::fs::create_dir_all(&spill1).unwrap();

    let (p0, p1) = (50581u16, 50582u16);
    let e0 = Arc::new(Engine::new());
    e0.register_batches("t", vec![make_batch(0, N / 2)])
        .unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("t", vec![make_batch(N / 2, N)])
        .unwrap();

    let spill0_spawn = spill0.clone();
    let spill1_spawn = spill1.clone();
    tokio::spawn(async move {
        std::env::set_var("WEFT_SHUFFLE_SPILL_DIR", spill0_spawn);
        let _ = serve_worker(p0, e0).await;
    });
    tokio::spawn(async move {
        std::env::set_var("WEFT_SHUFFLE_SPILL_DIR", spill1_spawn);
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
    let actual = actual.expect("distributed query with spill never succeeded");

    assert!(
        spill_file_count(&spill0) > 0 || spill_file_count(&spill1) > 0,
        "expected at least one spill file under worker spill dirs"
    );
    assert_eq!(
        actual, expected,
        "spilled distributed result must equal single-node"
    );

    let _ = std::fs::remove_dir_all(&spill0);
    let _ = std::fs::remove_dir_all(&spill1);
}
