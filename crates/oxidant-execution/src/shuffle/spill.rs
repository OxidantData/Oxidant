//! Spill hash-shuffle buckets to local disk when in-memory caching would OOM.
//!
//! Activated when `OXIDANT_SHUFFLE_SPILL_DIR` is set, or when `OXIDANT_SHUFFLE_SPILL_BYTES` /
//! `OXIDANT_MEMORY_LIMIT_BYTES` is set and cached shuffle data reaches that threshold. Buckets are
//! written as Arrow IPC stream files keyed by `(stage_id, partition_id)`; appends after the
//! initial spill land in per-bucket segment files so they never rewrite the whole bucket.
//!
//! Two budgets apply: a per-stage limit (`memory_limit_bytes`) and a worker-wide limit
//! (`total_limit_bytes`, from `OXIDANT_SHUFFLE_TOTAL_SPILL_BYTES`, defaulting to the per-stage
//! limit) enforced across all cached stages by [`enforce_total_budget`].

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use oxidant_common::{Error, Result};
use oxidant_loom::arrow::datatypes::SchemaRef;
use oxidant_loom::arrow::ipc::reader::StreamReader;
use oxidant_loom::arrow::ipc::writer::StreamWriter;
use oxidant_loom::arrow::record_batch::RecordBatch;

static SPILL_STORE_SEQ: AtomicU64 = AtomicU64::new(0);

/// On-disk spill store for one worker process.
#[derive(Debug, Clone)]
pub struct SpillStore {
    root: PathBuf,
    force_spill: bool,
    memory_limit_bytes: Option<usize>,
    total_limit_bytes: Option<usize>,
}

impl SpillStore {
    /// Open a spill directory when shuffle spilling is configured.
    ///
    /// `OXIDANT_SHUFFLE_SPILL_DIR` forces every cached shuffle bucket to disk. When only
    /// `OXIDANT_SHUFFLE_SPILL_BYTES` or `OXIDANT_MEMORY_LIMIT_BYTES` is set, a per-worker temporary
    /// directory is created and buckets spill once their estimated Arrow memory footprint reaches
    /// that limit (`OXIDANT_SHUFFLE_SPILL_BYTES` takes precedence). `OXIDANT_SHUFFLE_TOTAL_SPILL_BYTES`
    /// caps the summed in-memory bytes across all cached stages; it defaults to the per-stage
    /// limit so several under-limit stages cannot accumulate past the budget.
    pub fn from_env() -> Option<Self> {
        let configured_root = non_empty_env("OXIDANT_SHUFFLE_SPILL_DIR").map(PathBuf::from);
        // Explicit shuffle env wins; otherwise use [`resolve_shuffle_spill_bytes`] so an
        // auto-sized FairSpillPool and the in-memory shuffle cache do not both claim ~70%
        // of RAM (shuffle takes ¼ of the auto-sized pool). `OXIDANT_MEMORY_LIMIT_BYTES=0`
        // opts out of both — only a configured spill dir (force-spill) still enables the store.
        let memory_limit_bytes = non_empty_env("OXIDANT_SHUFFLE_SPILL_BYTES")
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .or_else(oxidant_loom::resolve_shuffle_spill_bytes);
        let total_limit_bytes = non_empty_env("OXIDANT_SHUFFLE_TOTAL_SPILL_BYTES")
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .or(memory_limit_bytes);

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
            total_limit_bytes,
        };
        std::fs::create_dir_all(&store.root).ok()?;
        Some(store)
    }

    /// Spill when the in-memory bucket footprint reaches `memory_limit_bytes` (threshold policy,
    /// not force-all). Writes under `root` (created if missing). Prefer this in tests over
    /// process-global `OXIDANT_*` env so concurrent suites cannot poison each other.
    pub fn with_memory_limit(root: impl Into<PathBuf>, memory_limit_bytes: usize) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)
            .map_err(|e| Error::Io(format!("spill create {}: {e}", root.display())))?;
        Ok(Self {
            root,
            force_spill: false,
            memory_limit_bytes: Some(memory_limit_bytes.max(1)),
            // Opt-in here (unlike `from_env`) so existing threshold tests keep per-stage semantics.
            total_limit_bytes: None,
        })
    }

    /// Cap the summed in-memory bytes across all cached stages (see [`enforce_total_budget`]).
    pub fn with_total_limit(mut self, total_limit_bytes: usize) -> Self {
        self.total_limit_bytes = Some(total_limit_bytes.max(1));
        self
    }

    /// Worker-wide in-memory shuffle budget across all stages, if configured.
    pub fn total_limit_bytes(&self) -> Option<usize> {
        self.total_limit_bytes
    }

    /// Directory where this store writes `stage_*_part_*.arrow` files.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Spill file for one bucket of one producer task's cache. The `src` scope (the producing
    /// task's partition id, or [`crate::shuffle::PUSH_SRC`] for `do_exchange` pushes) keeps
    /// per-producer cache entries — one stage can be produced by several tasks on the same
    /// worker (KAN-32 per-partition intermediate dispatch) — from clobbering each other's
    /// files.
    fn path(&self, stage_id: u32, src: u32, partition: u32) -> PathBuf {
        self.root
            .join(format!("stage_{stage_id}_src{src}_part_{partition}.arrow"))
    }

    /// Whether a bucket set should be spilled now.
    pub fn should_spill(&self, buckets: &[Vec<RecordBatch>]) -> bool {
        self.force_spill || self.should_spill_bytes(estimated_bucket_bytes(buckets))
    }

    /// Whether an estimated in-memory footprint should spill now.
    pub fn should_spill_bytes(&self, bytes: usize) -> bool {
        self.force_spill || self.memory_limit_bytes.is_some_and(|limit| bytes >= limit)
    }

    /// Append one batch to a spilled partition as a new segment file (O(new data), no
    /// read–modify–write of the whole bucket).
    pub fn append_batch_to_bucket(
        &self,
        stage_id: u32,
        src: u32,
        partition: u32,
        schema: SchemaRef,
        batch: &RecordBatch,
    ) -> Result<()> {
        self.append_batches_to_bucket(
            stage_id,
            src,
            partition,
            schema,
            std::slice::from_ref(batch),
        )
    }

    /// Append batches to a spilled partition as one new segment file. The base
    /// `stage_*_part_*.arrow` file is never rewritten after the initial spill.
    pub fn append_batches_to_bucket(
        &self,
        stage_id: u32,
        src: u32,
        partition: u32,
        schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        if !self.path(stage_id, src, partition).exists() {
            // First write for this bucket: create the base file.
            self.write_bucket(stage_id, src, partition, schema, batches)?;
            return Ok(());
        }
        // Segments are only ever created here as `existing count`, so numbering is contiguous.
        let seq = self.segment_paths(stage_id, src, partition).len() as u64;
        let path = self.segment_path(stage_id, src, partition, seq);
        write_ipc_file(&path, schema, batches)
    }

    /// Write `batches` for one bucket; returns the file path.
    pub fn write_bucket(
        &self,
        stage_id: u32,
        src: u32,
        partition: u32,
        schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<PathBuf> {
        let path = self.path(stage_id, src, partition);
        write_ipc_file(&path, schema, batches)?;
        Ok(path)
    }

    /// Read a spilled bucket back into memory (base file followed by segments in append order).
    pub fn read_bucket(&self, stage_id: u32, src: u32, partition: u32) -> Result<Vec<RecordBatch>> {
        let path = self.path(stage_id, src, partition);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let mut batches = read_ipc_file(&path)?;
        for segment in self.segment_paths(stage_id, src, partition) {
            batches.extend(read_ipc_file(&segment)?);
        }
        Ok(batches)
    }

    fn segment_path(&self, stage_id: u32, src: u32, partition: u32, seq: u64) -> PathBuf {
        self.root.join(format!(
            "stage_{stage_id}_src{src}_part_{partition}.seg{seq}.arrow"
        ))
    }

    /// Existing segment files for one bucket, in append order.
    fn segment_paths(&self, stage_id: u32, src: u32, partition: u32) -> Vec<PathBuf> {
        let prefix = format!("stage_{stage_id}_src{src}_part_{partition}.seg");
        let mut segs: Vec<(u64, PathBuf)> = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.root) {
            for ent in entries.flatten() {
                let name = ent.file_name();
                let Some(name) = name.to_str() else { continue };
                let Some(rest) = name.strip_prefix(&prefix) else {
                    continue;
                };
                let Some(seq) = rest
                    .strip_suffix(".arrow")
                    .and_then(|s| s.parse::<u64>().ok())
                else {
                    continue;
                };
                segs.push((seq, ent.path()));
            }
        }
        segs.sort_by_key(|(seq, _)| *seq);
        segs.into_iter().map(|(_, path)| path).collect()
    }

    /// Remove all spill files for a stage.
    pub fn clear_stage(&self, stage_id: u32) {
        self.clear_by_prefix(&format!("stage_{stage_id}_"));
    }

    /// Remove one producer scope's spill files for a stage — a failed task discards its
    /// partial output without touching sibling tasks' buckets (KAN-32).
    pub fn clear_scoped_stage(&self, stage_id: u32, src: u32) {
        self.clear_by_prefix(&format!("stage_{stage_id}_src{src}_"));
    }

    fn clear_by_prefix(&self, prefix: &str) {
        if let Ok(entries) = std::fs::read_dir(&self.root) {
            for ent in entries.flatten() {
                if ent.file_name().to_string_lossy().starts_with(prefix) {
                    let _ = std::fs::remove_file(ent.path());
                }
            }
        }
    }
}

fn write_ipc_file(path: &Path, schema: SchemaRef, batches: &[RecordBatch]) -> Result<()> {
    let file = std::fs::File::create(path)
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
    Ok(())
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
        /// Producer scope of this cache entry (see [`SpillStore::path`]).
        src: u32,
        /// Per-partition row counts at spill time, incremented on append — lets a worker
        /// answer AQE row-count probes without re-reading spilled files (KAN-32).
        row_counts: Vec<usize>,
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
        src: u32,
        spill: Option<&SpillStore>,
    ) -> Result<Self> {
        if let Some(store) = spill {
            if store.should_spill(&buckets) {
                return Self::spill_buckets(schema, buckets, stage_id, src, store);
            }
        }
        Ok(Self::Memory(buckets))
    }

    /// Cache a single pushed partition, spilling immediately if policy requires it.
    pub fn from_partition(
        schema: SchemaRef,
        stage_id: u32,
        src: u32,
        partition: u32,
        batches: Vec<RecordBatch>,
        spill: Option<&SpillStore>,
    ) -> Result<Self> {
        let mut buckets = vec![Vec::new(); partition as usize + 1];
        buckets[partition as usize] = batches;
        Self::maybe_spill(schema, buckets, stage_id, src, spill)
    }

    /// Append one batch to a partition, spilling when the configured threshold is reached.
    pub fn append_batch(
        &mut self,
        schema: SchemaRef,
        stage_id: u32,
        src: u32,
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
                        *self = Self::spill_buckets(schema, owned, stage_id, src, store)?;
                    }
                }
                Ok(())
            }
            Self::Spilled {
                schema: spilled_schema,
                spill,
                stage_id,
                src,
                row_counts,
            } => {
                let rows = batch.num_rows();
                spill.append_batch_to_bucket(
                    *stage_id,
                    *src,
                    partition,
                    spilled_schema.clone(),
                    &batch,
                )?;
                bump_row_count(row_counts, partition, rows);
                Ok(())
            }
        }
    }

    /// Append batches to one partition, converting an in-memory cache to spilled if the configured
    /// memory threshold is reached.
    pub fn append_partition(
        &mut self,
        schema: SchemaRef,
        stage_id: u32,
        src: u32,
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
                        *self = Self::spill_buckets(schema, owned, stage_id, src, store)?;
                    }
                }
                Ok(())
            }
            Self::Spilled {
                schema: spilled_schema,
                spill,
                stage_id,
                src,
                row_counts,
            } => {
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                spill.append_batches_to_bucket(
                    *stage_id,
                    *src,
                    partition,
                    spilled_schema.clone(),
                    &batches,
                )?;
                bump_row_count(row_counts, partition, rows);
                Ok(())
            }
        }
    }

    fn spill_buckets(
        schema: SchemaRef,
        buckets: Vec<Vec<RecordBatch>>,
        stage_id: u32,
        src: u32,
        store: &SpillStore,
    ) -> Result<Self> {
        let row_counts = buckets
            .iter()
            .map(|bucket| bucket.iter().map(|b| b.num_rows()).sum())
            .collect();
        for (i, bucket) in buckets.iter().enumerate() {
            store.write_bucket(stage_id, src, i as u32, schema.clone(), bucket)?;
        }
        Ok(Self::Spilled {
            schema,
            spill: Arc::new(store.clone()),
            stage_id,
            src,
            row_counts,
        })
    }

    /// Estimated in-memory footprint of this cache (0 once spilled).
    pub fn memory_bytes(&self) -> usize {
        match self {
            Self::Memory(buckets) => estimated_bucket_bytes(buckets),
            Self::Spilled { .. } => 0,
        }
    }

    /// Per-partition row counts (in-memory from the batches, tracked across spill) — cheap
    /// enough for the driver's AQE sampling probes (KAN-32).
    pub fn partition_row_counts(&self) -> Vec<usize> {
        match self {
            Self::Memory(buckets) => buckets
                .iter()
                .map(|bucket| bucket.iter().map(|b| b.num_rows()).sum())
                .collect(),
            Self::Spilled { row_counts, .. } => row_counts.clone(),
        }
    }

    /// Spill an in-memory cache to disk in place (no-op when already spilled).
    pub fn spill_now(
        &mut self,
        schema: SchemaRef,
        stage_id: u32,
        src: u32,
        store: &SpillStore,
    ) -> Result<()> {
        if let Self::Memory(buckets) = self {
            let owned = std::mem::take(buckets);
            *self = Self::spill_buckets(schema, owned, stage_id, src, store)?;
        }
        Ok(())
    }

    pub fn read_partition(&self, partition: usize) -> Result<Vec<RecordBatch>> {
        match self {
            Self::Memory(buckets) => Ok(buckets.get(partition).cloned().unwrap_or_default()),
            Self::Spilled {
                schema,
                spill,
                stage_id,
                src,
                ..
            } => {
                // A missing base file is a legitimately empty bucket (producers only write
                // files for buckets they appended to), but a file that exists and fails to
                // read is corruption or a lifecycle race — surface it. Swallowing it here
                // (`unwrap_or_default`) silently served partial shuffle data downstream:
                // SF10 TPC-H Q16 returned the right row count with wrong aggregate values.
                let data = spill
                    .read_bucket(*stage_id, *src, partition as u32)?
                    .into_iter()
                    .filter(|b| b.num_rows() > 0)
                    .collect::<Vec<_>>();
                Ok(if data.is_empty() {
                    vec![RecordBatch::new_empty(schema.clone())]
                } else {
                    data
                })
            }
        }
    }

    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Memory(buckets) => buckets
                .iter()
                .find_map(|b| b.first())
                .map(|b| b.schema())
                .unwrap_or_else(|| Arc::new(oxidant_loom::arrow::datatypes::Schema::empty())),
            Self::Spilled { schema, .. } => schema.clone(),
        }
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

/// Enforce the worker-wide in-memory shuffle budget (`OXIDANT_SHUFFLE_TOTAL_SPILL_BYTES`,
/// defaulting to the per-stage limit) across all cached stages: while the summed
/// [`BucketCache::memory_bytes`] exceeds the budget, spill the largest in-memory stage.
/// Call after inserting/appending into the worker's stage cache. No-op without a total limit.
/// Cache entries are keyed by `(stage_id, src)` — one per producing task (KAN-32).
pub fn enforce_total_budget(
    stages: &mut HashMap<(u32, u32), (SchemaRef, BucketCache)>,
    store: &SpillStore,
) -> Result<()> {
    let Some(limit) = store.total_limit_bytes else {
        return Ok(());
    };
    loop {
        let total: usize = stages.values().map(|(_, cache)| cache.memory_bytes()).sum();
        if total <= limit {
            return Ok(());
        }
        // Some stage must hold the excess (total > limit >= 1), so a victim always exists and
        // each iteration strictly reduces the total.
        let victim = stages
            .iter()
            .max_by_key(|(_, (_, cache))| cache.memory_bytes())
            .map(|(&key, _)| key)
            .expect("total above limit implies an in-memory stage");
        let (schema, cache) = stages.get_mut(&victim).expect("victim stage");
        cache.spill_now(schema.clone(), victim.0, victim.1, store)?;
    }
}

fn bump_row_count(row_counts: &mut Vec<usize>, partition: u32, rows: usize) {
    let idx = partition as usize;
    if row_counts.len() <= idx {
        row_counts.resize(idx + 1, 0);
    }
    row_counts[idx] += rows;
}

fn unique_spill_subdir(base: PathBuf) -> PathBuf {
    let seq = SPILL_STORE_SEQ.fetch_add(1, Ordering::Relaxed);
    base.join(format!("{}-{seq}", std::process::id()))
}

fn default_spill_root() -> PathBuf {
    unique_spill_subdir(std::env::temp_dir().join("oxidant-shuffle-spill"))
}

/// Estimated Arrow footprint of record batches (same metric [`SpillStore`] thresholds use).
pub fn estimated_batch_bytes(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::get_array_memory_size).sum()
}

fn estimated_bucket_bytes(buckets: &[Vec<RecordBatch>]) -> usize {
    buckets
        .iter()
        .flatten()
        .map(RecordBatch::get_array_memory_size)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidant_loom::arrow::array::Int64Array;
    use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};

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
            total_limit_bytes: None,
        }
    }

    fn int64_values(batches: &[RecordBatch]) -> Vec<i64> {
        batches
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
            .collect()
    }

    #[test]
    fn append_batch_spills_when_threshold_reached() {
        let root = default_spill_root();
        let store = SpillStore {
            root: root.clone(),
            force_spill: false,
            memory_limit_bytes: Some(1),
            total_limit_bytes: None,
        };
        std::fs::create_dir_all(&root).unwrap();

        let b = batch();
        let schema = b.schema();
        let mut cache = BucketCache::from_memory(vec![Vec::new()]);
        cache
            .append_batch(schema.clone(), 12, 0, 0, b, Some(&store))
            .expect("append");
        assert!(matches!(cache, BucketCache::Spilled { .. }));
        assert!(!store.read_bucket(12, 0, 0).unwrap().is_empty());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn memory_limit_policy_spills_when_threshold_reached() {
        let root = default_spill_root();
        let store = store_at(root.clone(), false, Some(1));

        let b = batch();
        let schema = b.schema();
        let cache =
            BucketCache::maybe_spill(schema, vec![vec![b]], 11, 0, Some(&store)).expect("spill");
        assert!(matches!(cache, BucketCache::Spilled { .. }));
        assert!(!store.read_bucket(11, 0, 0).unwrap().is_empty());

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
            BucketCache::maybe_spill(schema, vec![vec![b]], 1, 0, Some(&store)).expect("cache");
        assert!(matches!(cache, BucketCache::Memory(_)));
        assert!(!store.path(1, 0, 0).exists());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn force_spill_writes_even_without_memory_limit() {
        let root = default_spill_root();
        let store = store_at(root.clone(), true, None);

        let b = batch();
        let schema = b.schema();
        let cache = BucketCache::from_partition(schema, 7, 0, 0, vec![b.clone()], Some(&store))
            .expect("force spill");
        assert!(matches!(cache, BucketCache::Spilled { .. }));

        let got = cache.read_partition(0).unwrap();
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
            BucketCache::maybe_spill(schema.clone(), vec![vec![one]], 3, 0, Some(&store)).unwrap();
        assert!(matches!(cache, BucketCache::Memory(_)));

        cache
            .append_partition(schema, 3, 0, 0, vec![batch_with(vec![4, 5])], Some(&store))
            .unwrap();
        assert!(matches!(cache, BucketCache::Spilled { .. }));

        let rows: Vec<i64> = cache
            .read_partition(0)
            .unwrap()
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
            BucketCache::from_partition(schema.clone(), 9, 0, 1, vec![first], Some(&store))
                .unwrap();
        assert!(matches!(cache, BucketCache::Spilled { .. }));

        cache
            .append_partition(schema, 9, 0, 1, vec![batch_with(vec![30])], Some(&store))
            .unwrap();

        let rows: Vec<i64> = cache
            .read_partition(1)
            .unwrap()
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
        store
            .write_bucket(42, 0, 0, b.schema(), &[b])
            .expect("write");
        assert!(!store.read_bucket(42, 0, 0).unwrap().is_empty());

        store.clear_stage(42);
        assert!(store.read_bucket(42, 0, 0).unwrap().is_empty());
        // Missing partition with no prior write also returns empty (not an error).
        assert!(store.read_bucket(42, 0, 99).unwrap().is_empty());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn global_budget_spills_largest_stage_across_stages() {
        let root = default_spill_root();
        // Per-stage limit above each stage alone, but the worker-wide budget below their sum.
        let big = batch_with(vec![1, 2, 3, 4, 5, 6]);
        let small = batch_with(vec![7, 8]);
        let big_bytes = estimated_batch_bytes(std::slice::from_ref(&big));
        let small_bytes = estimated_batch_bytes(std::slice::from_ref(&small));
        assert!(big_bytes > small_bytes);
        let store = SpillStore::with_memory_limit(root.clone(), big_bytes + 1)
            .unwrap()
            .with_total_limit(big_bytes + 1);

        let schema = big.schema();
        let mut stages: HashMap<(u32, u32), (SchemaRef, BucketCache)> = HashMap::new();
        for (stage_id, b) in [(1u32, big), (2u32, small)] {
            let cache =
                BucketCache::maybe_spill(schema.clone(), vec![vec![b]], stage_id, 0, Some(&store))
                    .unwrap();
            // Each stage alone is under the per-stage limit: stays in memory.
            assert!(matches!(cache, BucketCache::Memory(_)));
            stages.insert((stage_id, 0), (schema.clone(), cache));
        }

        enforce_total_budget(&mut stages, &store).unwrap();

        // The largest stage spilled; the small one stays cached in memory.
        assert!(matches!(stages[&(1, 0)].1, BucketCache::Spilled { .. }));
        assert!(matches!(stages[&(2, 0)].1, BucketCache::Memory(_)));
        let total: usize = stages.values().map(|(_, c)| c.memory_bytes()).sum();
        assert!(
            total <= big_bytes + 1,
            "worker total {total} must be bounded"
        );

        // Evicted stage data still round-trips from disk.
        assert_eq!(
            int64_values(&stages[&(1, 0)].1.read_partition(0).unwrap()),
            vec![1, 2, 3, 4, 5, 6]
        );
        assert_eq!(
            int64_values(&stages[&(2, 0)].1.read_partition(0).unwrap()),
            vec![7, 8]
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn spilled_bucket_appends_are_incremental_segments() {
        let root = default_spill_root();
        let store = store_at(root.clone(), true, None);
        let first = batch_with(vec![1, 2]);
        let schema = first.schema();
        let mut cache =
            BucketCache::from_partition(schema.clone(), 5, 0, 0, vec![first], Some(&store))
                .unwrap();
        assert!(matches!(cache, BucketCache::Spilled { .. }));

        let base = store.path(5, 0, 0);
        let base_len = std::fs::metadata(&base).unwrap().len();

        for i in 0..4 {
            cache
                .append_batch(
                    schema.clone(),
                    5,
                    0,
                    0,
                    batch_with(vec![10 + i]),
                    Some(&store),
                )
                .unwrap();
        }

        // No full-file rewrite: the base file is untouched and each append added one segment.
        assert_eq!(std::fs::metadata(&base).unwrap().len(), base_len);
        assert_eq!(store.segment_paths(5, 0, 0).len(), 4);

        // Round-trip returns every batch in append order.
        assert_eq!(
            int64_values(&cache.read_partition(0).unwrap()),
            vec![1, 2, 10, 11, 12, 13]
        );

        let _ = std::fs::remove_dir_all(root);
    }
}
