//! The streaming DataFrame's input table.
//!
//! A Structured Streaming query is a *batch* plan re-executed once per micro-batch: Spark swaps
//! the source's newest rows in at the leaf and runs the same plan again. Oxidant does the same —
//! [`MicroBatchInput`] is a `TableProvider` whose contents are replaced between batches, so
//! `readStream…select…filter…writeStream` translates to one DataFusion `LogicalPlan` built once
//! at query start and executed per batch.
//!
//! The plan captures the provider by `Arc` at translation time, so the query manager has to swap
//! the *same* provider instance rather than re-register a name. Getting that handle from the
//! translator to the query manager is what [`capture`] is for: the Connect layer translates the
//! streaming relation inside a capture scope, and every streaming read encountered during that
//! translation reports its input back.
//!
//! Each captured query gets its **own** input, so two queries reading the same Kafka topic with
//! identical options do not overwrite each other's micro-batch between the poll and the plan
//! execution. Translations outside a capture scope — `AnalyzePlan`'s `isStreaming`/schema
//! queries, which never execute — share one input per (format, options) instead, so repeatedly
//! analyzing a streaming DataFrame does not accumulate tables in the session.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use oxidant_common::{Error, Result};
use oxidant_loom::arrow::datatypes::SchemaRef;
use oxidant_loom::arrow::record_batch::RecordBatch;

/// A table whose rows are the current micro-batch.
pub struct MicroBatchInput {
    name: String,
    schema: SchemaRef,
    table: Arc<MemTable>,
}

impl MicroBatchInput {
    /// Build an empty input with a fixed schema.
    pub fn new(name: impl Into<String>, schema: SchemaRef) -> Result<Self> {
        let table = MemTable::try_new(schema.clone(), vec![vec![]])
            .map_err(|e| Error::Execution(format!("streaming input table: {e}")))?;
        Ok(Self {
            name: name.into(),
            schema,
            table: Arc::new(table),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn provider(&self) -> Arc<dyn TableProvider> {
        self.table.clone()
    }

    /// Replace the input's rows with this micro-batch.
    ///
    /// Batches whose schema does not match the declared one are rejected: a plan built against
    /// the declared schema would either error deep in execution or, worse, read the wrong column
    /// by position.
    pub async fn set_batches(&self, batches: Vec<RecordBatch>) -> Result<()> {
        for b in &batches {
            if b.schema() != self.schema {
                return Err(Error::Execution(format!(
                    "streaming input `{}`: batch schema {:?} does not match the source schema {:?}",
                    self.name,
                    b.schema(),
                    self.schema
                )));
            }
        }
        let partition = self
            .table
            .batches
            .first()
            .ok_or_else(|| Error::Execution("streaming input has no partition".into()))?;
        *partition.write().await = batches;
        Ok(())
    }
}

tokio::task_local! {
    /// Inputs created by streaming reads during the current translation, in encounter order.
    static CAPTURED: RefCell<Vec<Arc<MicroBatchInput>>>;
}

/// Translate `fut` while collecting every streaming input its reads create.
///
/// Returns the future's value alongside the inputs, so the caller can hand the query manager the
/// exact provider instances the plan captured.
pub async fn capture<F, T>(fut: F) -> (T, Vec<Arc<MicroBatchInput>>)
where
    F: std::future::Future<Output = T>,
{
    let cell = RefCell::new(Vec::new());
    CAPTURED
        .scope(cell, async move {
            let value = fut.await;
            let inputs = CAPTURED.with(|c| c.borrow().clone());
            (value, inputs)
        })
        .await
}

/// Shared inputs for translations that happen outside a [`capture`] scope, keyed by
/// [`stream_input_name`].
fn shared() -> &'static Mutex<HashMap<String, Arc<MicroBatchInput>>> {
    static SHARED: OnceLock<Mutex<HashMap<String, Arc<MicroBatchInput>>>> = OnceLock::new();
    SHARED.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The table name a *shared* (analysis-only) streaming input resolves to.
///
/// Derived from the read's format and options so repeated analysis of the same streaming
/// DataFrame reuses one table rather than registering a new one per call.
pub fn stream_input_name(format: &str, options: &BTreeMap<String, String>) -> String {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    format.to_ascii_lowercase().hash(&mut hasher);
    for (k, v) in options {
        k.hash(&mut hasher);
        v.hash(&mut hasher);
    }
    format!("_oxidant_stream_{:016x}", hasher.finish())
}

/// Build the input a streaming read should scan.
///
/// Inside a [`capture`] scope this mints a fresh, uniquely named input and records it for the
/// caller; outside one it returns the shared per-(format, options) input. Callers register the
/// returned input's provider in the session context under [`MicroBatchInput::name`].
pub fn stream_input(
    format: &str,
    options: &BTreeMap<String, String>,
    schema: SchemaRef,
) -> Result<Arc<MicroBatchInput>> {
    static SEQ: AtomicU64 = AtomicU64::new(0);

    let captured = CAPTURED.try_with(|_| ()).is_ok();
    if captured {
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let input = Arc::new(MicroBatchInput::new(
            format!("_oxidant_stream_q{n}"),
            schema,
        )?);
        CAPTURED.with(|c| c.borrow_mut().push(input.clone()));
        return Ok(input);
    }

    let name = stream_input_name(format, options);
    let mut guard = shared().lock().unwrap_or_else(|e| e.into_inner());
    if let Some(existing) = guard.get(&name) {
        return Ok(existing.clone());
    }
    let input = Arc::new(MicroBatchInput::new(name.clone(), schema)?);
    guard.insert(name, input.clone());
    Ok(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidant_loom::arrow::array::Int64Array;
    use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]))
    }

    fn batch(vals: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vals))]).unwrap()
    }

    fn opts(topic: &str) -> BTreeMap<String, String> {
        [("subscribe".to_string(), topic.to_string())]
            .into_iter()
            .collect()
    }

    #[test]
    fn the_shared_name_is_stable_for_the_same_read_and_differs_across_reads() {
        assert_eq!(
            stream_input_name("kafka", &opts("events")),
            stream_input_name("kafka", &opts("events"))
        );
        assert_ne!(
            stream_input_name("kafka", &opts("events")),
            stream_input_name("kafka", &opts("audit"))
        );
        assert_ne!(
            stream_input_name("kafka", &opts("events")),
            stream_input_name("rate", &opts("events"))
        );
    }

    #[tokio::test]
    async fn swapping_batches_replaces_rather_than_appends() {
        let input = MicroBatchInput::new("t", schema()).unwrap();
        input.set_batches(vec![batch(vec![1, 2, 3])]).await.unwrap();
        input.set_batches(vec![batch(vec![4])]).await.unwrap();

        let held = input.table.batches[0].read().await;
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].num_rows(), 1, "batch 1 must not still be present");
    }

    #[tokio::test]
    async fn a_mismatched_batch_schema_is_rejected() {
        let input = MicroBatchInput::new("t", schema()).unwrap();
        let other: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            false,
        )]));
        let wrong =
            RecordBatch::try_new(other, vec![Arc::new(Int64Array::from(vec![1i64]))]).unwrap();
        let err = input.set_batches(vec![wrong]).await.unwrap_err();
        assert!(err.to_string().contains("does not match"), "{err}");
    }

    #[tokio::test]
    async fn capture_hands_back_the_inputs_a_translation_created() {
        let (plan_schema, inputs) = capture(async {
            let a = stream_input("kafka", &opts("events"), schema()).unwrap();
            let b = stream_input("kafka", &opts("audit"), schema()).unwrap();
            (a.schema(), b.name().to_string())
        })
        .await;
        assert_eq!(plan_schema.0.fields().len(), 1);
        assert_eq!(inputs.len(), 2, "both reads must be reported");
        assert_eq!(inputs[1].name(), plan_schema.1);
    }

    #[tokio::test]
    async fn two_captured_queries_on_the_same_topic_get_separate_inputs() {
        // The hazard this guards: two live queries reading one topic would otherwise share a
        // buffer, and one query's poll would overwrite the batch the other was about to execute.
        let (_, first) = capture(async { stream_input("kafka", &opts("shared"), schema()) }).await;
        let (_, second) = capture(async { stream_input("kafka", &opts("shared"), schema()) }).await;

        assert_ne!(first[0].name(), second[0].name());
        first[0].set_batches(vec![batch(vec![1, 2])]).await.unwrap();
        second[0].set_batches(vec![batch(vec![9])]).await.unwrap();
        assert_eq!(first[0].table.batches[0].read().await[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn outside_a_capture_the_same_read_reuses_one_input() {
        // The analysis path: `isStreaming` / schema queries never execute, so sharing keeps
        // repeated analysis from accumulating tables in the session.
        let a = stream_input("kafka", &opts("analysis-only"), schema()).unwrap();
        let b = stream_input("kafka", &opts("analysis-only"), schema()).unwrap();
        assert!(Arc::ptr_eq(&a, &b));
    }
}
