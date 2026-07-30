//! KAN-40: the distributed AVG partial/final merge must preserve the single-node result
//! exactly — no f32/decimal-scale truncation in the SUM/COUNT recombine.
//!
//! Background: TPC-H Q1 at SF10 showed AVG columns at ~6 decimal places (e.g. `25.500975`)
//! against a DuckDB f64 golden (`25.500975103`). That scale is DataFusion's (and Spark's)
//! decimal AVG typing — `AVG(DECIMAL(15,2))` returns `DECIMAL(19,6)` — and is identical
//! single-node; the recombine `sum(a_s) / NULLIF(sum(a_c), 0)` keeps the same value. The
//! earlier `CAST(... AS DOUBLE)` recombine was removed (b1f332a) precisely because it made
//! distributed diverge from single-node (TPC-DS Q7/Q26). This test locks the invariant:
//! a 2-worker distributed AVG equals the single-node result bit-for-bit, for both decimal
//! and double inputs. Own integration binary so ports / engine state don't race other tests.

use std::sync::Arc;

use weft_execution::driver::{run_stages, Cluster};
use weft_execution::flight::{health_check_worker, serve_worker};
use weft_execution::plan::plan_distributed;
use weft_loom::arrow::array::{Decimal128Array, Float64Array, Int64Array};
use weft_loom::arrow::datatypes::{DataType, Field, Schema};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;

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

/// Run `query` single-node over the full dataset and distributed over two workers holding
/// `shards`, returning (single, distributed) batches.
async fn single_vs_distributed(
    query: &str,
    shards: [Vec<RecordBatch>; 2],
) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
    let full: Vec<RecordBatch> = shards.iter().flatten().cloned().collect();
    let single = Engine::new();
    single.register_batches("t", full).unwrap();
    let expected = single.sql(query).await.unwrap();

    // The planner needs the table schema only; a one-row slice of the data is enough.
    let probe = shards[0][0].slice(0, 1);
    let mut endpoints = Vec::new();
    for shard in shards {
        let engine = Arc::new(Engine::new());
        engine.register_batches("t", shard).unwrap();
        endpoints.push(start_worker(engine).await);
    }

    let planner = Engine::new();
    planner.register_batches("t", vec![probe]).unwrap();
    let dq = plan_distributed(&planner, query, &[])
        .await
        .expect("avg group-by must distribute");
    let cluster = Cluster::new(endpoints);
    let mut actual = None;
    for _ in 0..50 {
        match run_stages(&cluster, &dq.stages).await {
            Ok(b) => {
                actual = Some(b);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
    (expected, actual.expect("distributed query never succeeded"))
}

/// (k, avg-as-f64) rows keyed by group, for order-insensitive exact comparison.
fn avg_rows(batches: &[RecordBatch]) -> Vec<(i64, f64)> {
    let mut out = Vec::new();
    for b in batches {
        let ks = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let col = b.column(1);
        let scale = match b.schema().field(1).data_type() {
            DataType::Decimal128(_, s) => Some(*s),
            _ => None,
        };
        for i in 0..b.num_rows() {
            let v = if let Some(scale) = scale {
                let a = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
                a.value(i) as f64 / 10f64.powi(scale.into())
            } else {
                col.as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(i)
            };
            out.push((ks.value(i), v));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

#[tokio::test]
async fn two_worker_avg_decimal_matches_single_node() {
    // TPC-H-like decimal(15,2) measures with non-terminating averages.
    let mk = |start: i64, end: i64| {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Decimal128(15, 2), true),
        ]));
        let ks: Vec<i64> = (start..end).map(|i| i % 4).collect();
        let vs: Vec<i128> = (start..end)
            .map(|i| 2500 + (i * 37) as i128 % 991)
            .collect();
        let varr = Decimal128Array::from(vs)
            .with_precision_and_scale(15, 2)
            .unwrap();
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ks)), Arc::new(varr)]).unwrap()
    };
    let (single, dist) = single_vs_distributed(
        "SELECT k, AVG(v) AS a FROM t GROUP BY k",
        [vec![mk(0, 500)], vec![mk(500, 1000)]],
    )
    .await;
    assert_eq!(
        avg_rows(&single),
        avg_rows(&dist),
        "distributed AVG over DECIMAL must equal single-node bit-for-bit"
    );
}

#[tokio::test]
async fn two_worker_avg_double_matches_single_node() {
    // Doubles with enough significant digits that any f32-ish truncation in the merge shows.
    let mk = |start: i64, end: i64| {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Float64, false),
        ]));
        let ks: Vec<i64> = (start..end).map(|i| i % 4).collect();
        let vs: Vec<f64> = (start..end)
            .map(|i| 25.500975103 + (i as f64) * 0.000000001)
            .collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ks)),
                Arc::new(Float64Array::from(vs)),
            ],
        )
        .unwrap()
    };
    let (single, dist) = single_vs_distributed(
        "SELECT k, AVG(v) AS a FROM t GROUP BY k",
        [vec![mk(0, 500)], vec![mk(500, 1000)]],
    )
    .await;
    let (s, d) = (avg_rows(&single), avg_rows(&dist));
    assert_eq!(s.len(), d.len());
    for ((sk, sv), (dk, dv)) in s.iter().zip(d.iter()) {
        assert_eq!(sk, dk);
        // Full f64 precision: float addition is not associative, so partial-sum order can
        // move the last ulp — but an f32/decimal(…,6) truncation (~1e-7 relative) must fail.
        assert!(
            (sv - dv).abs() <= 1e-15 * sv.abs().max(1.0),
            "key {sk}: distributed AVG lost precision ({sv} vs {dv})"
        );
    }
}
