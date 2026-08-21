//! Guard against accidental default enablement of the KAN-70 GPU offload spike:
//! with `OXIDANT_GPU_OFFLOAD` unset, a group-by query plan must contain NO GPU
//! node (the rule registers only via `oxidant_gpu::register_if_enabled`, which is
//! env-gated). If someone flips the default, this test fails.

use std::sync::Arc;

use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::arrow::util::pretty::pretty_format_batches;
use oxidant_loom::Engine;

#[tokio::test(flavor = "multi_thread")]
async fn group_by_plan_has_no_gpu_node_when_offload_env_unset() {
    // Pin the precondition regardless of the outer shell's environment.
    std::env::remove_var("OXIDANT_GPU_OFFLOAD");

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
        "GPU offload must be opt-in via OXIDANT_GPU_OFFLOAD=1, plan:\n{plan}"
    );
    assert!(
        plan.contains("AggregateExec"),
        "expected a plain CPU aggregate plan:\n{plan}"
    );
}
