//! Microbenchmark: native `group_aggregate` vs DataFusion on a high-cardinality GROUP BY —
//! the Q31–Q35 shape. Both aggregate the *same in-memory batches* (a `MemTable` for DataFusion),
//! so this isolates the aggregation operator from Parquet IO.
//!
//! Run release (debug numbers are meaningless):
//!   cargo run --release -p oxidant-loom --example bench_hashagg -- [rows] [groups] [iters]

use std::sync::Arc;
use std::time::Instant;

use oxidant_loom::arrow::array::{Int64Array, RecordBatch};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use oxidant_loom::ops::hash_agg::{group_aggregate, AggKind, AggSpec};
use oxidant_loom::Engine;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

/// `rows` rows scattered across `groups` distinct keys (Knuth multiplicative hash, no rng dep),
/// chunked into 8192-row batches like a real scan.
fn make_data(rows: usize, groups: i64) -> Vec<RecordBatch> {
    let schema = schema();
    let chunk = 8192;
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < rows {
        let n = chunk.min(rows - i);
        let mut ks = Vec::with_capacity(n);
        let mut vs = Vec::with_capacity(n);
        for j in 0..n {
            let idx = (i + j) as u64;
            let k = (idx.wrapping_mul(2654435761) % groups as u64) as i64;
            ks.push(k);
            vs.push(idx as i64);
        }
        out.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ks)),
                    Arc::new(Int64Array::from(vs)),
                ],
            )
            .unwrap(),
        );
        i += n;
    }
    out
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let mut args = std::env::args().skip(1);
    let rows: usize = args
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4_000_000);
    let groups: i64 = args
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let iters: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(5);

    let batches = make_data(rows, groups);
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!(
        "rows={total_rows} groups={groups} iters={iters} cores={}",
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0),
    );

    let aggs = vec![
        AggSpec {
            kind: AggKind::Sum,
            input: Some(1),
            name: "s".into(),
        },
        AggSpec {
            kind: AggKind::Count,
            input: None,
            name: "c".into(),
        },
    ];
    let sch = schema();

    // --- Native kernel ---
    let mut native_groups = 0;
    let mut native_best = f64::MAX;
    for _ in 0..iters {
        let t = Instant::now();
        let out = group_aggregate(&sch, &batches, &[0], &aggs).unwrap();
        let dt = t.elapsed().as_secs_f64();
        native_groups = out.num_rows();
        native_best = native_best.min(dt);
    }

    // --- DataFusion ---
    let engine = Engine::new();
    engine.register_batches("t", batches.clone()).unwrap();
    let sql = "SELECT k, SUM(v) s, COUNT(*) c FROM t GROUP BY k";
    let mut df_groups = 0;
    let mut df_best = f64::MAX;
    for _ in 0..iters {
        let t = Instant::now();
        let out = engine.sql(sql).await.unwrap();
        let dt = t.elapsed().as_secs_f64();
        df_groups = out.iter().map(|b| b.num_rows()).sum();
        df_best = df_best.min(dt);
    }

    let mrows = total_rows as f64 / 1e6;
    println!(
        "native : {native_best:.4}s  ({:.1} Mrows/s)  groups={native_groups}",
        mrows / native_best
    );
    println!(
        "datafusion: {df_best:.4}s  ({:.1} Mrows/s)  groups={df_groups}",
        mrows / df_best
    );
    println!("speedup (df/native): {:.2}x", df_best / native_best);
    assert_eq!(native_groups, df_groups, "group counts must match");
}
