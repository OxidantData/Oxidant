//! Spill hash-shuffle buckets to local disk when in-memory caching would OOM.
//!
//! Activated when `WEFT_SHUFFLE_SPILL_DIR` is set, or when `WEFT_SHUFFLE_SPILL_BYTES` /
//! `WEFT_MEMORY_LIMIT_BYTES` is set and cached shuffle data reaches that threshold. Buckets are
//! written as Arrow IPC stream files keyed by `(stage_id, partition_id)`.

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
    /// `WEFT_SHUFFLE_SPILL_BYTES` or `WEFT_MEMORY_LIMIT_BYTES` is set, a per-worker temporary
    /// directory is created and buckets spill once their estimated Arrow memory footprint reaches
    /// that limit (`WEFT_SHUFFLE_SPILL_BYTES` takes precedence).
    pub fn from_env() -> Option<Self> {
        let configured_root = non_empty_env("WEFT_SHUFFLE_SPILL_DIR").map(PathBuf::from);
        let memory_limit_bytes = non_empty_env("WEFT_SHUFFLE_SPILL_BYTES")
            .or_else(|| non_empty_env("WEFT_MEMORY_LIMIT_BYTES"))
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0);

        if configured_root.is_none() && memory_limit_bytes.is_none() {
            return None;
        }

        let force_spill = configured_root.is_some();
        // Always nest under a unique per-Worker subdirectory so concurrent in-process
        // workers (tests) — or multiple SpillStore::from_env calls — never clobber the
        // same `stage_*_part_*.arrow` paths.
        let root = match configured_root {
            Some(base) => unique_spill_subdir(base),
            None => default_spill_root(),
        };
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
        self.force_spill || self.should_spill_bytes(estimated_bucket_bytes(buckets))
    }

    /// Whether an estimated in-memory footprint should spill now.
    pub fn should_spill_bytes(&self, bytes: usize) -> bool {
        self.force_spill || self.memory_limit_bytes.is_some_and(|limit| bytes >= limit)
    }

    /// Append one batch to a spilled partition (read–extend–write).
    pub fn append_batch_to_bucket(
        &self,
        stage_id: u32,
        partition: u32,
        schema: SchemaRef,
        batch: &RecordBatch,
    ) -> Result<()> {
        let mut merged = self.read_bucket(stage_id, partition).unwrap_or_default();
        merged.push(batch.clone());
        self.write_bucket(stage_id, partition, schema, &merged)?;
        Ok(())
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

    /// Append one batch to a partition, spilling when the configured threshold is reached.
    pub fn append_batch(
        &mut self,
        schema: SchemaRef,
        stage_id: u32,
        partition: u32,
        batch: RecordBatch,
        spill: Option<&SpillStore>,
    ) -> Result<()> {
        match self {
            Self::Memory(buckets) => {
                let idx = partition as usize;
                if buckets.len() <= idx {
                    buckets.resize_with(idx + 1, Vec::new);
                }
                buckets[idx].push(batch);

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
            } => spill.append_batch_to_bucket(*stage_id, partition, spilled_schema.clone(), &batch),
        }
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

fn unique_spill_subdir(base: PathBuf) -> PathBuf {
    let seq = SPILL_STORE_SEQ.fetch_add(1, Ordering::Relaxed);
    base.join(format!("{}-{seq}", std::process::id()))
}

fn default_spill_root() -> PathBuf {
    unique_spill_subdir(std::env::temp_dir().join("weft-shuffle-spill"))
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

    fn batch_with(values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    fn store_at(root: PathBuf, force_spill: bool, memory_limit_bytes: Option<usize>) -> SpillStore {
        std::fs::create_dir_all(&root).unwrap();
        SpillStore {
            root,
            force_spill,
            memory_limit_bytes,
        }
    }

    #[test]
    fn append_batch_spills_when_threshold_reached() {
        let root = default_spill_root();
        let store = SpillStore {
            root: root.clone(),
            force_spill: false,
            memory_limit_bytes: Some(1),
        };
        std::fs::create_dir_all(&root).unwrap();

        let b = batch();
        let schema = b.schema();
        let mut cache = BucketCache::from_memory(vec![Vec::new()]);
        cache
            .append_batch(schema.clone(), 12, 0, b, Some(&store))
            .expect("append");
        assert!(matches!(cache, BucketCache::Spilled { .. }));
        assert!(!store.read_bucket(12, 0).unwrap().is_empty());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn memory_limit_policy_spills_when_threshold_reached() {
        let root = default_spill_root();
        let store = store_at(root.clone(), false, Some(1));

        let b = batch();
        let schema = b.schema();
        let cache =
            BucketCache::maybe_spill(schema, vec![vec![b]], 11, Some(&store)).expect("spill");
        assert!(matches!(cache, BucketCache::Spilled { .. }));
        assert!(!store.read_bucket(11, 0).unwrap().is_empty());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn below_memory_limit_stays_in_memory() {
        let root = default_spill_root();
        // Large limit: a tiny Int64 batch must not trigger spill.
        let store = store_at(root.clone(), false, Some(usize::MAX));

        let b = batch();
        let schema = b.schema();
        let cache =
            BucketCache::maybe_spill(schema, vec![vec![b]], 1, Some(&store)).expect("cache");
        assert!(matches!(cache, BucketCache::Memory(_)));
        assert!(!store.path(1, 0).exists());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn force_spill_writes_even_without_memory_limit() {
        let root = default_spill_root();
        let store = store_at(root.clone(), true, None);

        let b = batch();
        let schema = b.schema();
        let cache = BucketCache::from_partition(schema, 7, 0, vec![b.clone()], Some(&store))
            .expect("force spill");
        assert!(matches!(cache, BucketCache::Spilled { .. }));

        let got = cache.read_partition(0);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].num_rows(), b.num_rows());
        assert_eq!(
            got[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn append_partition_crosses_threshold_and_spills() {
        let root = default_spill_root();
        // Threshold just above one batch so the first insert stays Memory, second append spills.
        let one = batch();
        let one_bytes = estimated_bucket_bytes(&[vec![one.clone()]]);
        let store = store_at(root.clone(), false, Some(one_bytes + 1));

        let schema = one.schema();
        let mut cache =
            BucketCache::maybe_spill(schema.clone(), vec![vec![one]], 3, Some(&store)).unwrap();
        assert!(matches!(cache, BucketCache::Memory(_)));

        cache
            .append_partition(schema, 3, 0, vec![batch_with(vec![4, 5])], Some(&store))
            .unwrap();
        assert!(matches!(cache, BucketCache::Spilled { .. }));

        let rows: Vec<i64> = cache
            .read_partition(0)
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(rows, vec![1, 2, 3, 4, 5]);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn append_onto_spilled_partition_merges_on_disk() {
        let root = default_spill_root();
        let store = store_at(root.clone(), true, None);
        let first = batch_with(vec![10, 20]);
        let schema = first.schema();
        let mut cache =
            BucketCache::from_partition(schema.clone(), 9, 1, vec![first], Some(&store)).unwrap();
        assert!(matches!(cache, BucketCache::Spilled { .. }));

        cache
            .append_partition(schema, 9, 1, vec![batch_with(vec![30])], Some(&store))
            .unwrap();

        let rows: Vec<i64> = cache
            .read_partition(1)
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(rows, vec![10, 20, 30]);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn clear_stage_removes_spill_files_and_empty_read_is_empty() {
        let root = default_spill_root();
        let store = store_at(root.clone(), true, None);
        let b = batch();
        store.write_bucket(42, 0, b.schema(), &[b]).expect("write");
        assert!(!store.read_bucket(42, 0).unwrap().is_empty());

        store.clear_stage(42);
        assert!(store.read_bucket(42, 0).unwrap().is_empty());
        // Missing partition with no prior write also returns empty (not an error).
        assert!(store.read_bucket(42, 99).unwrap().is_empty());

        let _ = std::fs::remove_dir_all(root);
    }
}
