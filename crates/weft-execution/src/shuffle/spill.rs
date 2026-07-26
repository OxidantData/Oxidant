//! Disk spill for cached shuffle partition buckets.
//!
//! When a producer stage's in-memory bucket cache would exceed
//! [`SpillConfig::threshold_bytes`], each non-empty bucket is written as an Arrow IPC
//! stream under [`SpillConfig::spill_dir`] and the in-memory batches are dropped. Consumers
//! (`pull_bucket` / `ShuffleReadTicket`) transparently reload spilled buckets.
//!
//! Env knobs (see `docs/DISTRIBUTED_PARITY.md` item 10):
//! - `WEFT_SHUFFLE_SPILL_BYTES` — soft in-memory budget for one stage's buckets (default 256 MiB)
//! - `WEFT_SPILL_DIR` — root directory for spill files (fallback: process temp dir)

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use weft_common::{Error, Result};
use weft_loom::arrow::datatypes::SchemaRef;
use weft_loom::arrow::ipc::reader::StreamReader;
use weft_loom::arrow::ipc::writer::StreamWriter;
use weft_loom::arrow::record_batch::RecordBatch;

/// Default soft budget for keeping one stage's shuffle buckets in memory (256 MiB).
pub const DEFAULT_SHUFFLE_SPILL_BYTES: u64 = 256 * 1024 * 1024;

/// Process-wide counter of buckets spilled to disk (tests assert spill actually happened).
static SPILL_BUCKET_COUNT: AtomicU64 = AtomicU64::new(0);

/// Spill policy for the Flight worker's stage-output cache.
#[derive(Debug, Clone)]
pub struct SpillConfig {
    /// Soft max bytes of RecordBatch payload kept in memory for one cached stage. At or above
    /// this size, buckets are written to disk.
    pub threshold_bytes: u64,
    /// Root directory; stage spill files live under `{spill_dir}/shuffle/{pid}-{stage_id}/`.
    pub spill_dir: PathBuf,
}

impl SpillConfig {
    /// Read `WEFT_SHUFFLE_SPILL_BYTES` / `WEFT_SPILL_DIR` (with defaults).
    pub fn from_env() -> Self {
        let threshold_bytes = std::env::var("WEFT_SHUFFLE_SPILL_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_SHUFFLE_SPILL_BYTES);
        let spill_dir = std::env::var("WEFT_SPILL_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir());
        Self {
            threshold_bytes,
            spill_dir,
        }
    }

    /// Explicit config (integration tests force a tiny threshold).
    pub fn new(threshold_bytes: u64, spill_dir: PathBuf) -> Self {
        Self {
            threshold_bytes,
            spill_dir,
        }
    }
}

/// One partition bucket: resident in memory or spilled to an IPC file.
#[derive(Debug)]
pub enum Bucket {
    /// Batches still held in the process.
    Memory(Vec<RecordBatch>),
    /// Arrow IPC stream on disk (schema + zero or more batches).
    Spilled(PathBuf),
}

/// Cached output of one producer stage: schema (for empty-bucket padding) + per-partition buckets.
#[derive(Debug)]
pub struct CachedStage {
    /// Output schema so an empty / missing bucket can still be served typed.
    pub schema: SchemaRef,
    /// One entry per downstream partition.
    pub buckets: Vec<Bucket>,
    /// Directory holding this stage's spill files; removed on [`Drop`].
    spill_root: Option<PathBuf>,
}

impl Drop for CachedStage {
    fn drop(&mut self) {
        if let Some(root) = self.spill_root.take() {
            let _ = fs::remove_dir_all(&root);
        }
    }
}

impl CachedStage {
    /// Build a cache entry from hash-partitioned buckets, spilling when over threshold.
    pub fn from_buckets(
        schema: SchemaRef,
        buckets: Vec<Vec<RecordBatch>>,
        stage_id: u32,
        cfg: &SpillConfig,
    ) -> Result<Self> {
        let total = buckets
            .iter()
            .flat_map(|b| b.iter())
            .map(batch_memory_bytes)
            .sum::<usize>() as u64;

        if total < cfg.threshold_bytes {
            return Ok(Self {
                schema,
                buckets: buckets.into_iter().map(Bucket::Memory).collect(),
                spill_root: None,
            });
        }

        let spill_root = cfg
            .spill_dir
            .join("shuffle")
            .join(format!("{}-{}", std::process::id(), stage_id));
        fs::create_dir_all(&spill_root).map_err(|e| {
            Error::Io(format!(
                "create shuffle spill dir {}: {e}",
                spill_root.display()
            ))
        })?;

        let mut out = Vec::with_capacity(buckets.len());
        for (i, batches) in buckets.into_iter().enumerate() {
            if batches.is_empty() {
                // Empty buckets are cheap; keep them in memory (schema padding on read).
                out.push(Bucket::Memory(batches));
                continue;
            }
            let path = spill_root.join(format!("part-{i}.arrow"));
            write_ipc(&path, &schema, &batches)?;
            SPILL_BUCKET_COUNT.fetch_add(1, Ordering::Relaxed);
            out.push(Bucket::Spilled(path));
        }

        Ok(Self {
            schema,
            buckets: out,
            spill_root: Some(spill_root),
        })
    }

    /// Load one partition for a shuffle read (clones memory buckets / reads spill files).
    pub fn read_partition(&self, partition: u32) -> Result<Vec<RecordBatch>> {
        match self.buckets.get(partition as usize) {
            Some(Bucket::Memory(batches)) if !batches.is_empty() => Ok(batches.clone()),
            Some(Bucket::Spilled(path)) => load_ipc(path),
            // Missing or empty: caller pads with a schema-carrying empty batch.
            _ => Ok(Vec::new()),
        }
    }
}

/// Approximate heap footprint of a batch's arrays (used for the spill threshold).
pub fn batch_memory_bytes(batch: &RecordBatch) -> usize {
    batch.get_array_memory_size()
}

/// Number of buckets written to disk since process start (test helper).
pub fn spilled_bucket_count() -> u64 {
    SPILL_BUCKET_COUNT.load(Ordering::Relaxed)
}

/// Reset the spill counter (test helper).
pub fn reset_spilled_bucket_count() {
    SPILL_BUCKET_COUNT.store(0, Ordering::Relaxed);
}

fn write_ipc(path: &Path, schema: &SchemaRef, batches: &[RecordBatch]) -> Result<()> {
    let file = File::create(path)
        .map_err(|e| Error::Io(format!("create spill {}: {e}", path.display())))?;
    let mut writer = StreamWriter::try_new(file, schema.as_ref())
        .map_err(|e| Error::Execution(format!("spill ipc writer: {e}")))?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| Error::Execution(format!("spill ipc write: {e}")))?;
    }
    writer
        .finish()
        .map_err(|e| Error::Execution(format!("spill ipc finish: {e}")))?;
    Ok(())
}

fn load_ipc(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)
        .map_err(|e| Error::Io(format!("open spill {}: {e}", path.display())))?;
    let reader = StreamReader::try_new(file, None)
        .map_err(|e| Error::Execution(format!("spill ipc reader: {e}")))?;
    let mut out = Vec::new();
    for batch in reader {
        out.push(batch.map_err(|e| Error::Execution(format!("spill ipc batch: {e}")))?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use weft_loom::arrow::array::Int64Array;
    use weft_loom::arrow::datatypes::{DataType, Field, Schema};

    fn sample() -> (SchemaRef, Vec<RecordBatch>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        (schema, vec![batch])
    }

    #[test]
    fn below_threshold_stays_in_memory() {
        let (schema, batches) = sample();
        let dir = std::env::temp_dir().join(format!("weft-spill-mem-{}", std::process::id()));
        let cfg = SpillConfig::new(u64::MAX, dir);
        let cached = CachedStage::from_buckets(schema, vec![batches], 0, &cfg).unwrap();
        assert!(matches!(cached.buckets[0], Bucket::Memory(_)));
        assert!(cached.spill_root.is_none());
        let got = cached.read_partition(0).unwrap();
        assert_eq!(got[0].num_rows(), 3);
    }

    #[test]
    fn over_threshold_spills_and_round_trips() {
        let (schema, batches) = sample();
        let dir = std::env::temp_dir().join(format!("weft-spill-disk-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let cfg = SpillConfig::new(1, dir.clone()); // force spill
        let before = spilled_bucket_count();
        let cached = CachedStage::from_buckets(schema.clone(), vec![batches, vec![]], 7, &cfg)
            .unwrap();
        assert!(matches!(cached.buckets[0], Bucket::Spilled(_)));
        assert!(matches!(cached.buckets[1], Bucket::Memory(_)));
        assert!(spilled_bucket_count() > before);
        let got = cached.read_partition(0).unwrap();
        assert_eq!(got[0].num_rows(), 3);
        let empty = cached.read_partition(1).unwrap();
        assert!(empty.is_empty());
        let root = cached.spill_root.clone().expect("spill root");
        assert!(root.exists());
        drop(cached);
        assert!(!root.exists(), "Drop must remove spill files");
        let _ = fs::remove_dir_all(&dir);
    }
}
