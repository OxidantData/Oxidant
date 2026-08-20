//! Streaming sink that merges each micro-batch into a Delta table (AUTO CDC / SCD Type 1).

use oxidant_common::{Error, Result};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;
use oxidant_streaming::{LakeSink, Sink};

use crate::auto_cdc::CdcMerge;

/// A sink that read-modify-writes the whole target table on every micro-batch.
///
/// Without a Delta `MERGE` the only atomic update available is the sink's `replace` commit, so
/// each batch costs one full rewrite of the target. The current contents are cached between
/// batches so a run of N batches reads the table once rather than N times.
pub struct CdcMergeSink {
    engine: Engine,
    merge: CdcMerge,
    target_table: String,
    /// Current target contents, or `None` before the first read / after a failed batch.
    state: Option<Vec<RecordBatch>>,
    inner: LakeSink,
}

impl CdcMergeSink {
    pub fn new(engine: Engine, merge: CdcMerge, target_table: String, inner: LakeSink) -> Self {
        Self {
            engine,
            merge,
            target_table,
            state: None,
            inner,
        }
    }

    /// Read the target as it stands. A table that does not exist yet is an empty target; any
    /// other read failure is a real error — swallowing it would silently overwrite live data.
    async fn read_target(&self) -> Result<Vec<RecordBatch>> {
        match self
            .engine
            .sql(&format!("SELECT * FROM {}", self.target_table))
            .await
        {
            Ok(batches) => Ok(batches),
            Err(e) if is_missing_table(&e) => Ok(Vec::new()),
            Err(e) => Err(Error::Execution(format!(
                "AUTO CDC target `{}`: {e}",
                self.target_table
            ))),
        }
    }
}

fn is_missing_table(err: &Error) -> bool {
    let msg = err.to_string().to_ascii_lowercase();
    msg.contains("not found")
        || msg.contains("does not exist")
        || msg.contains("no table")
        || msg.contains("table or view not found")
}

#[async_trait::async_trait]
impl Sink for CdcMergeSink {
    async fn write_batch(&mut self, batches: &[RecordBatch], batch_id: u64) -> Result<u64> {
        if batches.is_empty() {
            return Ok(0);
        }
        let target = match self.state.take() {
            Some(state) => state,
            None => self.read_target().await?,
        };
        let merged = self.merge.apply(&self.engine, batches, &target).await?;
        let rows = self.inner.replace_batch(&merged, batch_id).await?;
        self.state = Some(merged);
        Ok(rows)
    }

    fn description(&self) -> String {
        format!("CdcMergeSink[SCD1 -> {}]", self.target_table)
    }
}
