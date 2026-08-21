//! The enabled side of the KAN-70 GPU offload gate: with `OXIDANT_GPU_OFFLOAD=1`
//! the engine's physical optimizer pipeline carries the `gpu_offload` rule. A
//! non-matching plan (here: an in-memory scan) must still plan and execute
//! unchanged — the rule is a no-op outside its one shape.
//!
//! This lives in its own test binary so mutating the process env cannot race the
//! default-off guard in `gpu_offload_default.rs`.

use std::sync::Arc;

use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::arrow::util::pretty::pretty_format_batches;
use oxidant_loom::Engine;

#[tokio::test(flavor = "multi_thread")]
async fn enabled_offload_rule_leaves_non_matching_plans_alone() {
    std::env::set_var("OXIDANT_GPU_OFFLOAD", "1");

    let engine = Engine::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
        ],
    )
    .unwrap();
    engine.ctx().register_batch("t", batch).unwrap();

    // A MemTable scan is not offloadable: the plan keeps its CPU aggregate…
    let batches = engine
        .ctx()
        .sql("EXPLAIN SELECT k, sum(v) FROM t GROUP BY k")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let plan = pretty_format_batches(&batches).unwrap().to_string();
    assert!(
        !plan.contains("GpuScanAggExec"),
        "MemTable plan must stay on CPU even with offload enabled:\n{plan}"
    );
    assert!(plan.contains("AggregateExec"), "plan:\n{plan}");

    // …and the query still executes correctly through the pipeline that now
    // carries the rule.
    let batches = engine
        .sql("SELECT k, sum(v) FROM t GROUP BY k")
        .await
        .unwrap();
    let total: i64 = batches
        .iter()
        .map(|b| {
            b.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .flatten()
                .sum::<i64>()
        })
        .sum();
    assert_eq!(total, 100);

    std::env::remove_var("OXIDANT_GPU_OFFLOAD");
}
