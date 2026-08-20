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

    /// Read the target as it stands.
    ///
    /// A target that has never been committed to is an empty target; every *other* read failure
    /// is a real error, because swallowing it would make the next `replace_batch` write the
    /// whole table as just this micro-batch. The two cases are told apart by asking the sink
    /// whether it has ever committed — not by matching on the text of the error, which cannot
    /// tell "no such table" from an object-store 404 on a log file or a catalog blip.
    async fn read_target(&self) -> Result<Vec<RecordBatch>> {
        if self.inner.next_commit_version() == 0 {
            return Ok(Vec::new());
        }
        self.engine
            .sql(&format!("SELECT * FROM {}", self.target_table))
            .await
            .map_err(|e| Error::Execution(format!("AUTO CDC target `{}`: {e}", self.target_table)))
    }
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
        let before = self.inner.next_commit_version();
        let rows = self.inner.replace_batch(&merged, batch_id).await?;
        if self.inner.next_commit_version() == before {
            // Nothing was committed: this `batch_id` had already been applied, so the table holds
            // what *that* commit wrote, which is not guaranteed to equal what we just recomputed.
            // Caching the recomputed value would leave every later batch merging against a base
            // the table does not have, so drop it and re-read instead.
            self.state = None;
        } else {
            self.state = Some(merged);
        }
        Ok(rows)
    }

    fn description(&self) -> String {
        format!("CdcMergeSink[SCD1 -> {}]", self.target_table)
    }
}
