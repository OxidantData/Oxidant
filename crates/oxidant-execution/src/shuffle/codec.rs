//! Serialize/deserialize DataFusion physical-plan *fragments* for the Flight ticket, via
//! `datafusion-proto`.
//!
//! This lets the driver ship a stage's compiled plan (not just SQL) to a worker. It is the
//! preferred path when it round-trips cleanly; the `stage_sql` path in [`crate::shuffle`] is
//! the permanent fallback for plans whose leaves `datafusion-proto` cannot encode (e.g. an
//! in-memory source). See `fragment_round_trips` for the gate.

use std::sync::Arc;

use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::{AsExecutionPlan, DefaultPhysicalExtensionCodec};
use datafusion_proto::protobuf::PhysicalPlanNode;
use oxidant_common::{Error, Result};
use prost::Message;

/// Serialize a physical plan to `datafusion-proto` bytes (rides in `StageTicket.plan_fragment`).
pub fn serialize_plan(plan: &Arc<dyn ExecutionPlan>) -> Result<Vec<u8>> {
    let codec = DefaultPhysicalExtensionCodec {};
    let node = PhysicalPlanNode::try_from_physical_plan(plan.clone(), &codec)
        .map_err(|e| Error::Execution(format!("serialize physical plan: {e}")))?;
    Ok(node.encode_to_vec())
}

/// Deserialize a physical plan against a worker's session state (function registry + runtime).
pub fn deserialize_plan(bytes: &[u8], state: &SessionState) -> Result<Arc<dyn ExecutionPlan>> {
    let node = PhysicalPlanNode::decode(bytes)
        .map_err(|e| Error::Execution(format!("decode physical plan node: {e}")))?;
    let codec = DefaultPhysicalExtensionCodec {};
    let task_ctx = TaskContext::from(state);
    node.try_into_physical_plan(&task_ctx, &codec)
        .map_err(|e| Error::Execution(format!("deserialize physical plan: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidant_loom::arrow::array::{Int64Array, RecordBatch};
    use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
    use oxidant_loom::Engine;

    // Gate: confirm a GROUP BY plan over a *Parquet* leaf round-trips through datafusion-proto.
    // Parquet sources serialize; an in-memory source may not — which is exactly why the
    // distributed path keeps `stage_sql` as its primary execution route.
    #[tokio::test]
    async fn fragment_round_trips_over_parquet_leaf() {
        use datafusion::parquet::arrow::ArrowWriter;

        let dir = std::env::temp_dir().join(format!("oxidant-codec-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("part.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();
        {
            let f = std::fs::File::create(&path).unwrap();
            let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }

        let engine = Engine::new();
        engine
            .ctx()
            .register_parquet("t", path.to_str().unwrap(), Default::default())
            .await
            .unwrap();

        let plan = engine
            .physical_plan("SELECT k, SUM(v) AS s FROM t GROUP BY k")
            .await
            .unwrap();

        let bytes = serialize_plan(&plan).expect("serialize");
        let state = engine.session_state();
        let restored = deserialize_plan(&bytes, &state).expect("deserialize");

        let direct = engine.execute_plan(plan).await.unwrap();
        let round = engine.execute_plan(restored).await.unwrap();
        let direct_rows: usize = direct.iter().map(|b| b.num_rows()).sum();
        let round_rows: usize = round.iter().map(|b| b.num_rows()).sum();
        assert_eq!(direct_rows, round_rows);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // Why the distributed path ships *SQL* per stage rather than serialized consumer plans:
    // a MemTable (in-memory `shuffle_input`) leaf *does* serialize, but datafusion-proto BAKES THE
    // DATA into the bytes — it does not late-bind the table by name on deserialize. So a consumer
    // fragment cannot be shipped from the driver and rebound to a worker's freshly-pulled shuffle
    // bucket; the worker must re-plan its stage SQL against its own locally-registered input. This
    // test pins that behavior so the “named-table placeholder + worker rebind” idea isn't retried.
    #[tokio::test]
    async fn memtable_leaf_bakes_in_data_not_late_bound() {
        use oxidant_loom::arrow::array::Int64Array;
        use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};

        fn b(ks: Vec<i64>, vs: Vec<i64>) -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("s", DataType::Int64, false),
            ]));
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(ks)),
                    Arc::new(Int64Array::from(vs)),
                ],
            )
            .unwrap()
        }

        let driver = Engine::new();
        driver
            .register_batches("shuffle_input", vec![b(vec![1, 2, 1], vec![10, 20, 30])])
            .unwrap();
        let bytes = serialize_plan(
            &driver
                .physical_plan("SELECT SUM(s) AS s FROM shuffle_input")
                .await
                .unwrap(),
        )
        .expect("a MemTable-leaf plan still serializes");

        // A *different* worker engine with different data under the same name.
        let worker = Engine::new();
        worker
            .register_batches("shuffle_input", vec![b(vec![1, 1, 1], vec![100, 100, 100])])
            .unwrap();
        let restored = deserialize_plan(&bytes, &worker.session_state()).unwrap();
        let out = worker.execute_plan(restored).await.unwrap();
        let sum = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        // 60 == the driver's baked-in data; 300 would be late-binding to the worker's data.
        assert_eq!(
            sum, 60,
            "data is baked into the fragment, not late-bound by name"
        );
    }
}
