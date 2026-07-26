//! Spill hash-shuffle buckets to local disk when in-memory caching would OOM.
//!
//! Activated when `WEFT_SHUFFLE_SPILL_DIR` is set, or when `WEFT_MEMORY_LIMIT_BYTES` is set and
//! cached shuffle data reaches that threshold. Buckets are written as Arrow IPC stream files keyed
//! by `(stage_id, partition_id)`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use weft_common::{Error, Result};
use weft_loom::arrow::datatypes::SchemaRef;
use weft_loom::arrow::ipc::reader::StreamReader;
use weft_loom::arrow::ipc::writer::StreamWriter;
use weft_loom::arrow::record_batch::RecordBatch;

static SPILL_STORE_SEQ: AtomicU64 = AtomicU64::new(0);

/// On-disk spill store for one worker process.
#[derive(Debug, Clone)]
pub struct SpillStore {
    root: PathBuf,
    force_spill: bool,
    memory_limit_bytes: Option<usize>,
}

impl SpillStore {
    /// Open a spill directory when shuffle spilling is configured.
    ///
    /// `WEFT_SHUFFLE_SPILL_DIR` forces every cached shuffle bucket to disk. When only
    /// `WEFT_MEMORY_LIMIT_BYTES` is set, a per-worker temporary directory is created and buckets
    /// spill once their estimated Arrow memory footprint reaches that limit.
    pub fn from_env() -> Option<Self> {
        let configured_root = non_empty_env("WEFT_SHUFFLE_SPILL_DIR").map(PathBuf::from);
        let memory_limit_bytes = non_empty_env("WEFT_MEMORY_LIMIT_BYTES")
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0);

        if configured_root.is_none() && memory_limit_bytes.is_none() {
            return None;
        }

        let force_spill = configured_root.is_some();
        let root = configured_root.unwrap_or_else(default_spill_root);
        let store = Self {
            root,
            force_spill,
            memory_limit_bytes,
        };
        std::fs::create_dir_all(&store.root).ok()?;
        Some(store)
    }

    fn path(&self, stage_id: u32, partition: u32) -> PathBuf {
        self.root
            .join(format!("stage_{stage_id}_part_{partition}.arrow"))
    }

    /// Whether a bucket set should be spilled now.
    pub fn should_spill(&self, buckets: &[Vec<RecordBatch>]) -> bool {
        self.force_spill
            || self
                .memory_limit_bytes
                .is_some_and(|limit| estimated_bucket_bytes(buckets) >= limit)
    }

    /// Write `batches` for one bucket; returns the file path.
    pub fn write_bucket(
        &self,
        stage_id: u32,
        partition: u32,
        schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<PathBuf> {
        let path = self.path(stage_id, partition);
        let file = std::fs::File::create(&path)
            .map_err(|e| Error::Io(format!("spill create {}: {e}", path.display())))?;
        let mut writer = StreamWriter::try_new(file, &schema)
            .map_err(|e| Error::Io(format!("spill writer: {e}")))?;
        for b in batches {
            writer
                .write(b)
                .map_err(|e| Error::Io(format!("spill write: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| Error::Io(format!("spill finish: {e}")))?;
        Ok(path)
    }

    /// Read a spilled bucket back into memory.
    pub fn read_bucket(&self, stage_id: u32, partition: u32) -> Result<Vec<RecordBatch>> {
        let path = self.path(stage_id, partition);
        if !path.exists() {
            return Ok(Vec::new());
        }
        read_ipc_file(&path)
    }

    /// Remove all spill files for a stage.
    pub fn clear_stage(&self, stage_id: u32) {
        if let Ok(entries) = std::fs::read_dir(&self.root) {
            let prefix = format!("stage_{stage_id}_");
            for ent in entries.flatten() {
                if ent.file_name().to_string_lossy().starts_with(&prefix) {
                    let _ = std::fs::remove_file(ent.path());
                }
            }
        }
    }
}

fn read_ipc_file(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = std::fs::File::open(path)
        .map_err(|e| Error::Io(format!("spill open {}: {e}", path.display())))?;
    let reader =
        StreamReader::try_new(file, None).map_err(|e| Error::Io(format!("spill read: {e}")))?;
    reader
        .map(|b| b.map_err(|e| Error::Io(format!("spill batch: {e}"))))
        .collect()
}

/// In-memory bucket set, optionally spilled to disk.
#[derive(Debug)]
pub enum BucketCache {
    Memory(Vec<Vec<RecordBatch>>),
    Spilled {
        schema: SchemaRef,
        spill: Arc<SpillStore>,
        stage_id: u32,
    },
}

impl BucketCache {
    pub fn from_memory(buckets: Vec<Vec<RecordBatch>>) -> Self {
        Self::Memory(buckets)
    }

    pub fn maybe_spill(
        schema: SchemaRef,
        buckets: Vec<Vec<RecordBatch>>,
        stage_id: u32,
        spill: Option<&SpillStore>,
    ) -> Result<Self> {
        if let Some(store) = spill {
            if store.should_spill(&buckets) {
                return Self::spill_buckets(schema, buckets, stage_id, store);
            }
        }
        Ok(Self::Memory(buckets))
    }

    /// Cache a single pushed partition, spilling immediately if policy requires it.
    pub fn from_partition(
        schema: SchemaRef,
        stage_id: u32,
        partition: u32,
        batches: Vec<RecordBatch>,
        spill: Option<&SpillStore>,
    ) -> Result<Self> {
        let mut buckets = vec![Vec::new(); partition as usize + 1];
        buckets[partition as usize] = batches;
        Self::maybe_spill(schema, buckets, stage_id, spill)
    }

    /// Append batches to one partition, converting an in-memory cache to spilled if the configured
    /// memory threshold is reached.
    pub fn append_partition(
        &mut self,
        schema: SchemaRef,
        stage_id: u32,
        partition: u32,
        batches: Vec<RecordBatch>,
        spill: Option<&SpillStore>,
    ) -> Result<()> {
        match self {
            Self::Memory(buckets) => {
                let idx = partition as usize;
                if buckets.len() <= idx {
                    buckets.resize_with(idx + 1, Vec::new);
                }
                buckets[idx].extend(batches);

                if let Some(store) = spill {
                    if store.should_spill(buckets) {
                        let owned = std::mem::take(buckets);
                        *self = Self::spill_buckets(schema, owned, stage_id, store)?;
                    }
                }
                Ok(())
            }
            Self::Spilled {
                schema: spilled_schema,
                spill,
                stage_id,
            } => {
                let mut merged = spill.read_bucket(*stage_id, partition).unwrap_or_default();
                merged.extend(batches);
                spill.write_bucket(*stage_id, partition, spilled_schema.clone(), &merged)?;
                Ok(())
            }
        }
    }

    fn spill_buckets(
        schema: SchemaRef,
        buckets: Vec<Vec<RecordBatch>>,
        stage_id: u32,
        store: &SpillStore,
    ) -> Result<Self> {
        for (i, bucket) in buckets.iter().enumerate() {
            store.write_bucket(stage_id, i as u32, schema.clone(), bucket)?;
        }
        Ok(Self::Spilled {
            schema,
            spill: Arc::new(store.clone()),
            stage_id,
        })
    }

    pub fn read_partition(&self, partition: usize) -> Vec<RecordBatch> {
        match self {
            Self::Memory(buckets) => buckets.get(partition).cloned().unwrap_or_default(),
            Self::Spilled {
                schema,
                spill,
                stage_id,
            } => spill
                .read_bucket(*stage_id, partition as u32)
                .unwrap_or_default()
                .into_iter()
                .filter(|b| b.num_rows() > 0)
                .collect::<Vec<_>>()
                .pipe(|data| {
                    if data.is_empty() {
                        vec![RecordBatch::new_empty(schema.clone())]
                    } else {
                        data
                    }
                }),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Memory(buckets) => buckets
                .iter()
                .find_map(|b| b.first())
                .map(|b| b.schema())
                .unwrap_or_else(|| Arc::new(weft_loom::arrow::datatypes::Schema::empty())),
            Self::Spilled { schema, .. } => schema.clone(),
        }
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

fn default_spill_root() -> PathBuf {
    let seq = SPILL_STORE_SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join("weft-shuffle-spill")
        .join(format!("{}-{seq}", std::process::id()))
}

fn estimated_bucket_bytes(buckets: &[Vec<RecordBatch>]) -> usize {
    buckets
        .iter()
        .flatten()
        .map(RecordBatch::get_array_memory_size)
        .sum()
}

trait Pipe: Sized {
    fn pipe<F, R>(self, f: F) -> R
    where
        F: FnOnce(Self) -> R,
    {
        f(self)
    }
}
impl<T> Pipe for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_loom::arrow::array::Int64Array;
    use weft_loom::arrow::datatypes::{DataType, Field, Schema};

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap()
    }

    #[test]
    fn memory_limit_policy_spills_when_threshold_reached() {
        let root = default_spill_root();
        let store = SpillStore {
            root: root.clone(),
            force_spill: false,
            memory_limit_bytes: Some(1),
        };
        std::fs::create_dir_all(&root).unwrap();

        let b = batch();
        let schema = b.schema();
        let cache =
            BucketCache::maybe_spill(schema, vec![vec![b]], 11, Some(&store)).expect("spill");
        assert!(matches!(cache, BucketCache::Spilled { .. }));
        assert!(!store.read_bucket(11, 0).unwrap().is_empty());

        let _ = std::fs::remove_dir_all(root);
    }
}
