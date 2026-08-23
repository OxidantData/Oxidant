//! Streaming data sources.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use oxidant_loom::arrow::datatypes::SchemaRef;
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;
use serde::{Deserialize, Serialize};

/// A source's replay position, persisted in the query checkpoint after the sink commits.
///
/// The shape is deliberately source-agnostic — `source` names the implementation so a checkpoint
/// written by one source is never silently interpreted by another (a Kafka offset restored into a
/// file source would read as a file name), and `entries` is an ordered map so the serialized JSON
/// is byte-stable across runs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceOffsets {
    pub source: String,
    pub entries: BTreeMap<String, i64>,
}

/// The exact span of source positions one micro-batch will read, decided *before* it reads
/// anything and written to the checkpoint's offset log.
///
/// This is what makes a replay sound. Sink-side idempotency stamps the batch id into the commit
/// and drops a batch the log already carries — which is only correct if the replay covers the
/// same records the first attempt did. Without a recorded range, a batch replayed after a crash
/// reads *whatever is available now*, the sink recognizes the id and discards the whole thing,
/// and every record that arrived in between is lost with it. Recording the range first turns
/// "read the next batch" into "read batch 7", which is answerable identically at any later time.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchRange {
    /// The implementation that planned this range, so a range written by one source is never
    /// interpreted by another — the same guard [`SourceOffsets`] carries.
    pub source: String,
    /// Where each key begins, inclusive.
    pub start: BTreeMap<String, i64>,
    /// Where each key ends, exclusive.
    pub end: BTreeMap<String, i64>,
    /// Discrete items the batch covers, for a source whose position is a set rather than a span
    /// — the file source's paths. Empty for offset-based sources.
    #[serde(default)]
    pub items: Vec<String>,
}

impl BatchRange {
    /// True when the range names no records, which is what an idle trigger produces.
    ///
    /// An empty range is never written to the log and never advances the batch id: an idle
    /// trigger has to leave the table, and its version history, completely alone.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
            && self
                .end
                .iter()
                .all(|(key, end)| *end <= self.start.get(key).copied().unwrap_or(0))
    }
}

/// A micro-batch data source.
///
/// Reading is split in two on purpose. [`Source::plan_batch`] decides what the batch covers and
/// commits nothing; [`Source::poll_range`] reads exactly that and is required to be
/// **deterministic** — called twice with the same range it must produce the same records. The
/// scheduler writes the range to the offset log between the two calls, so a batch interrupted
/// anywhere after that point is replayed from the record of what it was reading rather than from
/// whatever the source happens to hold now.
#[async_trait::async_trait]
pub trait Source: Send + Sync {
    /// Decide what the next micro-batch will read, without reading it.
    ///
    /// Returns an empty range when no new data is available. Must not advance any position the
    /// source would report as committed *over anything it did not report*: a planned batch that
    /// is never written has to be re-plannable, and a plan that consumed something would lose it.
    /// A source that has to read to discover the extent of a range — the Postgres CDC source
    /// decodes WAL to find the next commit boundary — may move its committed position over a
    /// stretch it has established holds no records, because re-planning from either end of that
    /// stretch yields the same batch. It must not acknowledge anything to a server from here:
    /// only [`Source::mark_durable`] knows the batch survived. The one carve-out is a
    /// protocol-level keepalive: a source whose server demands periodic liveness replies (the
    /// Postgres walsender's `wal_sender_timeout`) may answer with the position it has *already*
    /// reported as committed — never with a position past it — because that acknowledges
    /// nothing the checkpoint does not already say.
    async fn plan_batch(&mut self, engine: &Engine) -> oxidant_common::Result<BatchRange>;

    /// Read exactly the records `range` names.
    ///
    /// **Deterministic**: the same range must yield the same records however many times it is
    /// read, and whichever process reads it. This is the property the offset log converts into
    /// exactly-once — a replay that returned a wider batch would be discarded whole by the
    /// sink's idempotency stamp, taking the extra records with it.
    async fn poll_range(
        &mut self,
        engine: &Engine,
        range: &BatchRange,
    ) -> oxidant_common::Result<Vec<RecordBatch>>;

    /// The schema this source emits, known before the first batch arrives.
    ///
    /// The streaming DataFrame is planned against this, so a source whose schema depends on the
    /// data (a file source over an unknown directory) returns an empty schema and only supports
    /// pass-through queries.
    fn schema(&self) -> SchemaRef;

    /// Spark-style source description for progress reporting (`KafkaV2[Subscribe[topic]]`).
    fn description(&self) -> String;

    /// Position to persist once the current batch is durably in the sink. `None` means the source
    /// has no replayable position (a rate/memory source).
    fn committed_offsets(&self) -> Option<SourceOffsets> {
        None
    }

    /// Resume from a checkpointed position. Called once, before the first poll.
    fn restore_offsets(&mut self, _offsets: &SourceOffsets) {}

    /// Called once the batch just committed is durable — its rows in the sink *and* its position
    /// in the checkpoint.
    ///
    /// Most sources need nothing here: their position lives entirely in the checkpoint, so there
    /// is no one to tell. A source reading from a server that retains data on the consumer's
    /// behalf — a Postgres replication slot, which keeps WAL until it is confirmed — has to
    /// acknowledge somewhere, and this is the only point at which acknowledging is safe.
    /// Acknowledging at read time would let the server discard exactly the records a replay of a
    /// failed batch needs; acknowledging after the sink write but before the checkpoint would
    /// discard the records a restart from the *previous* checkpoint would ask for.
    async fn mark_durable(&mut self, _engine: &Engine) -> oxidant_common::Result<()> {
        Ok(())
    }

    /// The furthest position available *right now*, ignoring any per-batch budget.
    ///
    /// This is what bounds an `availableNow` / `once` drain. It is deliberately not the planned
    /// range's end: with `maxOffsetsPerTrigger` set, one batch covers a slice of what is
    /// available, so stopping at it would drain a single batch and call the topic exhausted.
    ///
    /// `None` means the source cannot state an end — a directory of files can always gain
    /// another — and the drain falls back to stopping when a batch comes back empty.
    async fn available_end(
        &mut self,
        _engine: &Engine,
    ) -> oxidant_common::Result<Option<BTreeMap<String, i64>>> {
        Ok(None)
    }
}

/// File-directory source: reads new Parquet/JSON/CSV files not yet in the offset set.
pub struct FileSource {
    path: PathBuf,
    format: String,
    /// Paths already covered by a committed batch. A planned-but-uncommitted batch is *not*
    /// recorded here — the offset log names its files, and that is what a replay reads.
    seen: HashSet<String>,
    schema: SchemaRef,
}

impl FileSource {
    pub fn new(path: impl AsRef<Path>, format: &str) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            format: format.to_ascii_lowercase(),
            seen: HashSet::new(),
            // A directory of not-yet-existing files has no schema until the first file lands.
            schema: std::sync::Arc::new(oxidant_loom::arrow::datatypes::Schema::empty()),
        }
    }

    pub fn new_files(&self) -> Vec<PathBuf> {
        let Ok(entries) = std::fs::read_dir(&self.path) else {
            return vec![];
        };
        let mut files: Vec<PathBuf> = entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.is_file() && !self.seen.contains(&p.to_string_lossy().to_string()))
            .collect();
        // Deterministic order so a restart replays files the same way it first read them.
        files.sort();
        files
    }
}

#[async_trait::async_trait]
impl Source for FileSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn description(&self) -> String {
        format!("FileStreamSource[{}]", self.path.display())
    }

    /// The batch is the set of files not yet consumed, named explicitly.
    ///
    /// A directory listing is not a stable position the way an offset is — a file can appear at
    /// any moment — so the plan records the paths themselves. A replay then reads exactly those,
    /// even though the directory has moved on.
    async fn plan_batch(&mut self, _engine: &Engine) -> oxidant_common::Result<BatchRange> {
        Ok(BatchRange {
            source: "file".into(),
            items: self
                .new_files()
                .iter()
                .map(|p| p.to_string_lossy().into_owned())
                .collect(),
            ..Default::default()
        })
    }

    async fn poll_range(
        &mut self,
        engine: &Engine,
        range: &BatchRange,
    ) -> oxidant_common::Result<Vec<RecordBatch>> {
        use datafusion::prelude::{CsvReadOptions, JsonReadOptions, ParquetReadOptions};

        if range.source != "file" || range.items.is_empty() {
            return Ok(vec![]);
        }
        // Read through DataFusion's readers rather than a SQL string. `parquet_scan()` and
        // friends are DuckDB table functions that this engine has never registered, so the SQL
        // form this used to build could not resolve for any format.
        let ctx = engine.ctx();
        let mut all = Vec::new();
        for path in &range.items {
            let df = match self.format.as_str() {
                "parquet" => ctx.read_parquet(path, ParquetReadOptions::default()).await,
                "json" => ctx.read_json(path, JsonReadOptions::default()).await,
                "csv" => ctx.read_csv(path, CsvReadOptions::default()).await,
                other => {
                    return Err(oxidant_common::Error::Unsupported(format!(
                        "readStream.format(`{other}`) is not a file source"
                    )))
                }
            };
            let batches = df
                .map_err(|e| oxidant_common::Error::Execution(format!("read `{path}`: {e}")))?
                .collect()
                .await
                .map_err(|e| oxidant_common::Error::Execution(format!("read `{path}`: {e}")))?;
            if let Some(b) = batches.first() {
                self.schema = b.schema();
            }
            all.extend(batches);
        }
        // Marked consumed only once every file in the range has been read. Marking them one at a
        // time would strand the rows already collected when a later file fails: the error
        // discards them, and the files they came from would stay consumed.
        self.seen.extend(range.items.iter().cloned());
        Ok(all)
    }

    fn committed_offsets(&self) -> Option<SourceOffsets> {
        // A file source's position is the set of consumed paths; the value is unused.
        Some(SourceOffsets {
            source: "file".into(),
            entries: self.seen.iter().map(|p| (p.clone(), 1i64)).collect(),
        })
    }

    fn restore_offsets(&mut self, offsets: &SourceOffsets) {
        if offsets.source != "file" {
            return;
        }
        self.seen.extend(offsets.entries.keys().cloned());
    }
}

/// Rate source for tests: emits N rows per batch.
pub struct MemoryRateSource {
    rows_per_batch: u64,
    batch_count: u64,
    max_batches: u64,
}

impl MemoryRateSource {
    pub fn new(rows_per_batch: u64, max_batches: u64) -> Self {
        Self {
            rows_per_batch,
            batch_count: 0,
            max_batches,
        }
    }
}

#[async_trait::async_trait]
impl Source for MemoryRateSource {
    fn schema(&self) -> SchemaRef {
        use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
        std::sync::Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn description(&self) -> String {
        format!("RateStreamSource[rowsPerBatch={}]", self.rows_per_batch)
    }

    async fn plan_batch(&mut self, _engine: &Engine) -> oxidant_common::Result<BatchRange> {
        if self.batch_count >= self.max_batches {
            return Ok(BatchRange::default());
        }
        // The position is the batch counter: batch N is always the same generated rows, so the
        // range is a one-wide span and a replay of it reproduces them exactly.
        Ok(BatchRange {
            source: "rate".into(),
            start: [("batch".to_string(), self.batch_count as i64)].into(),
            end: [("batch".to_string(), self.batch_count as i64 + 1)].into(),
            items: vec![],
        })
    }

    async fn poll_range(
        &mut self,
        engine: &Engine,
        range: &BatchRange,
    ) -> oxidant_common::Result<Vec<RecordBatch>> {
        if range.source != "rate" || range.is_empty() {
            return Ok(vec![]);
        }
        self.batch_count = range.end.get("batch").copied().unwrap_or(0) as u64;
        let sql = format!(
            "SELECT id FROM range(0, {}, 1) AS t(id)",
            self.rows_per_batch
        );
        engine.sql(&sql).await
    }

    fn committed_offsets(&self) -> Option<SourceOffsets> {
        Some(SourceOffsets {
            source: "rate".into(),
            entries: [("batch".to_string(), self.batch_count as i64)].into(),
        })
    }

    fn restore_offsets(&mut self, offsets: &SourceOffsets) {
        if offsets.source != "rate" {
            return;
        }
        self.batch_count = offsets.entries.get("batch").copied().unwrap_or(0) as u64;
    }

    async fn available_end(
        &mut self,
        _engine: &Engine,
    ) -> oxidant_common::Result<Option<BTreeMap<String, i64>>> {
        Ok(Some(
            [(
                "batch".to_string(),
                self.max_batches.min(i64::MAX as u64) as i64,
            )]
            .into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn drain(src: &mut FileSource, engine: &Engine) -> usize {
        let range = src.plan_batch(engine).await.unwrap();
        src.poll_range(engine, &range)
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum()
    }

    #[tokio::test]
    async fn a_file_source_reads_new_files_and_skips_ones_it_has_seen() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("a.json"), "{\"n\":1}\n{\"n\":2}\n").unwrap();
        let engine = Engine::new();
        let mut src = FileSource::new(dir.path(), "json");

        assert_eq!(drain(&mut src, &engine).await, 2);
        assert_eq!(src.schema().fields().len(), 1);

        // Already consumed.
        assert_eq!(drain(&mut src, &engine).await, 0);

        std::fs::write(dir.path().join("b.json"), "{\"n\":3}\n").unwrap();
        assert_eq!(drain(&mut src, &engine).await, 1);
    }

    #[tokio::test]
    async fn a_recorded_range_reads_the_same_files_however_often_it_is_replayed() {
        // The property the offset log converts into exactly-once. A batch that failed keeps its
        // recorded range, and re-reading that range must produce what the first attempt saw —
        // not "whatever has landed in the directory since".
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("a.json"), "{\"n\":1}\n{\"n\":2}\n").unwrap();
        let engine = Engine::new();
        let mut src = FileSource::new(dir.path(), "json");

        let range = src.plan_batch(&engine).await.unwrap();
        assert_eq!(range.items.len(), 1);

        let first: usize = src
            .poll_range(&engine, &range)
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(first, 2);

        // A file lands between the two attempts. The replay must not pick it up: the sink would
        // recognize the batch id, discard the whole thing, and `b.json` would be lost with it.
        std::fs::write(dir.path().join("b.json"), "{\"n\":3}\n").unwrap();
        let replay: usize = src
            .poll_range(&engine, &range)
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(replay, 2, "the replay covers exactly the recorded range");

        // And the newcomer is still there for the next batch.
        assert_eq!(drain(&mut src, &engine).await, 1);
    }

    #[tokio::test]
    async fn a_planned_batch_that_is_never_read_can_be_planned_again() {
        // `plan_batch` must consume nothing: a plan that advanced the position would lose the
        // batch whenever recording it failed.
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("a.json"), "{\"n\":1}\n").unwrap();
        let engine = Engine::new();
        let mut src = FileSource::new(dir.path(), "json");

        let first = src.plan_batch(&engine).await.unwrap();
        let second = src.plan_batch(&engine).await.unwrap();
        assert_eq!(first, second, "planning twice describes the same batch");
        assert_eq!(drain(&mut src, &engine).await, 1);
    }

    #[test]
    fn an_empty_range_is_what_an_idle_trigger_produces() {
        assert!(BatchRange::default().is_empty());
        assert!(BatchRange {
            source: "kafka".into(),
            start: [("t-0".to_string(), 7)].into(),
            end: [("t-0".to_string(), 7)].into(),
            items: vec![],
        }
        .is_empty());
        assert!(!BatchRange {
            source: "kafka".into(),
            start: [("t-0".to_string(), 7)].into(),
            end: [("t-0".to_string(), 8)].into(),
            items: vec![],
        }
        .is_empty());
    }

    #[test]
    fn file_offsets_do_not_cross_source_types() {
        let mut src = FileSource::new("/tmp/does-not-matter", "json");
        src.restore_offsets(&SourceOffsets {
            source: "kafka".into(),
            entries: [("events-0".to_string(), 7i64)].into_iter().collect(),
        });
        assert!(src.seen.is_empty(), "a kafka checkpoint is not a file list");

        src.restore_offsets(&SourceOffsets {
            source: "file".into(),
            entries: [("/data/a.json".to_string(), 1i64)].into_iter().collect(),
        });
        assert!(src.seen.contains("/data/a.json"));
    }
}
