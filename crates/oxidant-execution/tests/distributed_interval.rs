//! TPC-DS Q12-shaped date window with Spark's `interval '30 days'` spelling.
//!
//! The Databricks dialect oxidant plans on rejects that form at parse time unless
//! `normalize_spark_sql` rewrites it. DataFusion's stage Unparser can also emit the
//! Postgres-verbose form into stage SQL; workers re-parse that under the same dialect.
//! This test locks the e2e path: a 2-worker distributed plan over a sharded fact with a
//! replicated date_dim must match single-node when the filter uses the Spark spelling.

use std::sync::Arc;

use oxidant_execution::driver::{run_stages, Cluster};
use oxidant_execution::flight::{health_check_worker, serve_worker};
use oxidant_execution::plan::plan_distributed;
use oxidant_loom::arrow::array::{Date32Array, Float64Array, Int64Array, StringArray};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;

/// 2001-01-12 as Date32 epoch days.
const D_START: i32 = 11334;
/// Window length matching `interval '30 days'` (inclusive BETWEEN → 31 calendar days of keys).
const WINDOW_DAYS: i32 = 30;

const QUERY: &str = "\
SELECT i_class, sum(ws_ext_sales_price) AS itemrevenue \
FROM web_sales, item, date_dim \
WHERE ws_item_sk = i_item_sk \
  AND ws_sold_date_sk = d_date_sk \
  AND i_category IN ('Sports', 'Books', 'Home') \
  AND d_date BETWEEN cast('2001-01-12' AS date) \
                 AND (cast('2001-01-12' AS date) + interval '30 days') \
GROUP BY i_class \
ORDER BY i_class";

async fn start_worker(engine: Arc<Engine>) -> String {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    tokio::spawn(async move {
        let _ = serve_worker(port, engine).await;
    });
    let endpoint = format!("http://127.0.0.1:{port}");
    for _ in 0..50 {
        if health_check_worker(endpoint.clone()).await.is_ok() {
            return endpoint;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("worker did not become ready at {endpoint}");
}

fn item_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i_item_sk", DataType::Int64, false),
        Field::new("i_category", DataType::Utf8, false),
        Field::new("i_class", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec![
                "Sports", "Books", "Home", "Jewelry",
            ])),
            Arc::new(StringArray::from(vec![
                "athletic", "fiction", "kitchen", "rings",
            ])),
        ],
    )
    .unwrap()
}

fn date_dim_batch() -> RecordBatch {
    // One row per day from 2001-01-01 through 2001-03-15 so the 30-day window is interior.
    let n = 75i32;
    let schema = Arc::new(Schema::new(vec![
        Field::new("d_date_sk", DataType::Int64, false),
        Field::new("d_date", DataType::Date32, false),
    ]));
    let sks: Vec<i64> = (0..n).map(|i| i as i64 + 1000).collect();
    // 2001-01-01 = 11323.
    let dates: Vec<i32> = (0..n).map(|i| 11323 + i).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(sks)),
            Arc::new(Date32Array::from(dates)),
        ],
    )
    .unwrap()
}

/// web_sales shard: rows for `item_sk` in `[item_lo, item_hi)`.
fn sales_shard(item_lo: i64, item_hi: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ws_item_sk", DataType::Int64, false),
        Field::new("ws_sold_date_sk", DataType::Int64, false),
        Field::new("ws_ext_sales_price", DataType::Float64, false),
    ]));
    let mut items = Vec::new();
    let mut dates = Vec::new();
    let mut prices = Vec::new();
    // Spread sales across a 60-day span so some fall inside the interval window and some outside.
    for item in item_lo..item_hi {
        for day_off in 0i32..60 {
            let d = D_START - 10 + day_off; // starts 10 days before the window
            let date_sk = 1000 + (d - 11323) as i64;
            items.push(item);
            dates.push(date_sk);
            prices.push(10.0 + item as f64 + day_off as f64 * 0.01);
        }
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(items)),
            Arc::new(Int64Array::from(dates)),
            Arc::new(Float64Array::from(prices)),
        ],
    )
    .unwrap()
}

fn revenue_rows(batches: &[RecordBatch]) -> Vec<(String, f64)> {
    let mut out = Vec::new();
    for b in batches {
        let classes = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let rev = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((classes.value(i).to_string(), rev.value(i)));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

#[tokio::test]
async fn spark_interval_date_window_matches_single_node_distributed() {
    let _ = WINDOW_DAYS; // documents the BETWEEN window; used by the SQL literal below.
    let item = item_batch();
    let dates = date_dim_batch();
    let left = sales_shard(1, 3); // items 1,2
    let right = sales_shard(3, 5); // items 3,4

    let single = Engine::new();
    single
        .register_batches("web_sales", vec![left.clone(), right.clone()])
        .unwrap();
    single.register_batches("item", vec![item.clone()]).unwrap();
    single
        .register_batches("date_dim", vec![dates.clone()])
        .unwrap();
    let expected = revenue_rows(&single.sql(QUERY).await.expect("single-node Q12-shaped"));

    let e0 = Arc::new(Engine::new());
    e0.register_batches("web_sales", vec![left]).unwrap();
    e0.register_batches("item", vec![item.clone()]).unwrap();
    e0.register_batches("date_dim", vec![dates.clone()])
        .unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("web_sales", vec![right]).unwrap();
    e1.register_batches("item", vec![item.clone()]).unwrap();
    e1.register_batches("date_dim", vec![dates.clone()])
        .unwrap();

    let endpoints = vec![start_worker(e0).await, start_worker(e1).await];

    // Planner only needs schemas; a one-row probe of each table is enough.
    let planner = Engine::new();
    planner
        .register_batches("web_sales", vec![sales_shard(1, 2).slice(0, 1)])
        .unwrap();
    planner
        .register_batches("item", vec![item.slice(0, 1)])
        .unwrap();
    planner
        .register_batches("date_dim", vec![dates.slice(0, 1)])
        .unwrap();
    // item + date_dim are replicated (broadcast/replicate dims); web_sales is the sharded fact.
    let dq = plan_distributed(&planner, QUERY, &["item", "date_dim"])
        .await
        .expect("Q12-shaped interval query must distribute");
    assert!(
        dq.stages.iter().any(|s| {
            let u = s.sql.to_ascii_uppercase();
            u.contains("INTERVAL") || u.contains("2001-01-12") || u.contains("D_DATE")
        }),
        "stage SQL should carry the date window (possibly unparsed): {:?}",
        dq.stages.iter().map(|s| &s.sql).collect::<Vec<_>>()
    );

    let cluster = Cluster::new(endpoints);
    let mut actual = None;
    for _ in 0..50 {
        match run_stages(&cluster, &dq.stages).await {
            Ok(b) => {
                actual = Some(b);
                break;
            }
            Err(e) => {
                // Transient connect races during worker boot; retry.
                if e.to_string().contains("connect") {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
                panic!("distributed Q12-shaped interval query failed: {e}");
            }
        }
    }
    let actual = revenue_rows(&actual.expect("distributed query never succeeded"));
    assert_eq!(
        expected, actual,
        "distributed Spark-interval date window must match single-node"
    );
    // Jewelry is filtered out by category; Sports/Books/Home remain.
    assert!(
        expected.iter().any(|(c, _)| c == "athletic")
            && expected.iter().any(|(c, _)| c == "fiction")
            && expected.iter().any(|(c, _)| c == "kitchen")
            && !expected.iter().any(|(c, _)| c == "rings"),
        "expected in-window classes only: {expected:?}"
    );
}
