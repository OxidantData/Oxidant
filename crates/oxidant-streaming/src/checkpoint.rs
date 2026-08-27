//! Checkpoint metadata persistence (offsets, batch id, sink commits).

use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use oxidant_common::{Error, Result};
use oxidant_loom::Engine;
use serde::{Deserialize, Serialize};

use crate::query::StreamingQueryId;
use crate::source::BatchRange;

const OFFSETS_FILE: &str = "offsets.json";
/// Ranges a batch was recorded as *about to* read, written before it reads them.
const OFFSET_LOG_DIR: &str = "offsets";
/// Markers written once a batch's rows are durably in the sink.
const COMMIT_LOG_DIR: &str = "commits";

/// Persisted checkpoint state for a streaming query.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CheckpointState {
    pub query_id: String,
    pub run_id: String,
    pub batch_id: u64,
    /// The source's replay position as of `committed_batch_id`. Written only after the sink
    /// commit succeeds, so a crash mid-batch replays that batch rather than skipping it.
    #[serde(default)]
    pub source_offsets: Option<crate::source::SourceOffsets>,
    /// Last batch id successfully committed to the sink (exactly-once semantics).
    pub committed_batch_id: u64,
    /// Event-time watermark in microseconds, as of the last committed batch.
    ///
    /// Derived, not authoritative — `max_event_time_micros` less the configured lateness. Kept
    /// because it is what an operator reads when asking how far behind the stream is.
    pub watermark_micros: i64,
    /// The greatest event time ever observed, which is what the watermark is computed from.
    ///
    /// Persisted so a restart resumes the watermark rather than starting it over: a watermark
    /// that reset would make already-forgotten dedup keys necessary again.
    #[serde(default)]
    pub max_event_time_micros: Option<i64>,
    /// Cross-batch operator state — today, the `dropDuplicates` key set.
    ///
    /// Carried in the checkpoint rather than held only in memory, so a restart resumes with the
    /// keys it had. Bounded by the watermark: keys expire once no record old enough to match
    /// them is expected.
    #[serde(default)]
    pub dedup_state: Option<crate::state::DedupState>,
    /// Highest batch id whose log entries have been deleted, so pruning never re-issues a delete
    /// for an object it already removed. Without it, each commit re-deletes the same window and
    /// a fast trigger spends most of its object-store calls on no-ops.
    #[serde(default)]
    pub pruned_through: u64,
}

/// What a batch did, written to the commit log once its rows are durably in the sink.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BatchCommit {
    pub batch_id: u64,
    pub num_output_rows: u64,
    /// Where the source stood once this batch was read — the position a restart resumes from.
    ///
    /// Taken from the source rather than derived from the batch's range, because a range is not
    /// always a whole position: the file source's range names the files *this* batch covered,
    /// while its position is every file consumed so far. Reconstructing one from the other would
    /// silently forget the earlier batches and re-read the entire directory.
    #[serde(default)]
    pub resume_position: Option<crate::source::SourceOffsets>,
}

/// Object-store-backed checkpoint store.
///
/// Backed by an [`ObjectStore`] rather than `std::fs` because `checkpointLocation` is an
/// **object-store URL** in every deployment this targets. Writing it through the filesystem does
/// not fail on an `s3://` location — it creates a local directory literally named `s3:/bucket/...`
/// under the process's working directory. The query then runs perfectly, and its restart-resume
/// story is quietly fiction: a driver that restarts anywhere else finds no checkpoint, replays
/// from `startingOffsets`, and duplicates (or with `latest`, skips) everything.
#[derive(Debug, Clone)]
pub struct CheckpointStore {
    store: Arc<dyn ObjectStore>,
    root: ObjectPath,
    /// The location as the user wrote it, for error messages.
    location: String,
}

impl CheckpointStore {
    pub fn new(store: Arc<dyn ObjectStore>, root: ObjectPath, location: impl Into<String>) -> Self {
        Self {
            store,
            root,
            location: location.into(),
        }
    }

    /// The location as configured, for display.
    pub fn location(&self) -> &str {
        &self.location
    }

    fn offsets_path(&self) -> ObjectPath {
        self.root.clone().join(OFFSETS_FILE)
    }

    pub async fn load(&self) -> std::io::Result<CheckpointState> {
        let path = self.offsets_path();
        let bytes = match self.store.get(&path).await {
            Ok(result) => result.bytes().await.map_err(io_err)?,
            Err(object_store::Error::NotFound { .. }) => return Ok(CheckpointState::default()),
            Err(e) => return Err(io_err(e)),
        };
        serde_json::from_slice(&bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    /// Write the checkpoint so a reader never observes a partial one.
    ///
    /// A truncated checkpoint is worse than a missing one: `load` fails to parse it, every
    /// caller's `unwrap_or_default()` quietly turns that into "no committed offsets", and with
    /// `startingOffsets=latest` the query silently skips everything produced since. Object stores
    /// make a `PUT` atomic — a reader sees either the whole old object or the whole new one — so
    /// unlike the filesystem this needs no staged temporary file.
    pub async fn save(&self, state: &CheckpointState) -> std::io::Result<()> {
        let text = serde_json::to_vec_pretty(state)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        self.store
            .put(&self.offsets_path(), text.into())
            .await
            .map_err(io_err)?;
        Ok(())
    }

    /// Stamp a new *run* onto this checkpoint.
    ///
    /// Progress is deliberately preserved: a restart is a new `run_id` over the same committed
    /// batch id, offsets, and watermark — that is the whole point of pointing a query at an
    /// existing `checkpointLocation`. Wiping them here would silently re-ingest (or, with
    /// `startingOffsets=latest`, silently skip) everything the previous run had already
    /// committed. The `query_id` from the existing checkpoint wins for the same reason Spark
    /// persists it: the query's identity outlives any one run.
    pub async fn init_for_query(&self, id: &StreamingQueryId) -> std::io::Result<()> {
        let mut state = self.load().await.unwrap_or_default();
        if state.query_id.is_empty() {
            state.query_id = id.id.clone();
        }
        state.run_id = id.run_id.clone();
        self.save(&state).await
    }
}

impl CheckpointStore {
    fn planned_path(&self, batch_id: u64) -> ObjectPath {
        self.root
            .clone()
            .join(OFFSET_LOG_DIR)
            .join(batch_id.to_string())
    }

    fn commit_path(&self, batch_id: u64) -> ObjectPath {
        self.root
            .clone()
            .join(COMMIT_LOG_DIR)
            .join(batch_id.to_string())
    }

    /// Record what batch `batch_id` is about to read, before it reads any of it.
    ///
    /// This write is the ordering constraint the whole design rests on. Once it lands, the batch
    /// has a fixed extent that any later attempt — in this process or in one started tomorrow —
    /// resolves to the same records. Recording it *after* the read instead would leave a
    /// replay free to cover a wider range, which the sink's idempotency stamp then discards
    /// whole, silently taking the newly arrived records with it.
    pub async fn save_planned(&self, batch_id: u64, range: &BatchRange) -> std::io::Result<()> {
        let text = serde_json::to_vec_pretty(range)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        self.store
            .put(&self.planned_path(batch_id), text.into())
            .await
            .map_err(io_err)?;
        Ok(())
    }

    /// The recorded extent of `batch_id`, or `None` if it was never planned.
    pub async fn load_planned(&self, batch_id: u64) -> std::io::Result<Option<BatchRange>> {
        match self.store.get(&self.planned_path(batch_id)).await {
            Ok(result) => {
                let bytes = result.bytes().await.map_err(io_err)?;
                serde_json::from_slice(&bytes)
                    .map(Some)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(io_err(e)),
        }
    }

    /// Mark `batch_id` as durably written to the sink.
    ///
    /// Ordered after the sink write and before [`CheckpointStore::save`], so the three states a
    /// crash can leave behind are all distinguishable: planned only (replay it), planned and
    /// committed (its rows are in the table — recover the resume point from the range), or fully
    /// recorded (carry on).
    pub async fn save_commit(&self, commit: &BatchCommit) -> std::io::Result<()> {
        let text = serde_json::to_vec_pretty(commit)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        self.store
            .put(&self.commit_path(commit.batch_id), text.into())
            .await
            .map_err(io_err)?;
        Ok(())
    }

    /// Whether `batch_id` reached the sink.
    pub async fn is_committed(&self, batch_id: u64) -> std::io::Result<bool> {
        Ok(self.load_commit(batch_id).await?.is_some())
    }

    /// What `batch_id` committed, or `None` if it never reached the sink.
    pub async fn load_commit(&self, batch_id: u64) -> std::io::Result<Option<BatchCommit>> {
        match self.store.get(&self.commit_path(batch_id)).await {
            Ok(result) => {
                let bytes = result.bytes().await.map_err(io_err)?;
                serde_json::from_slice(&bytes)
                    .map(Some)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(io_err(e)),
        }
    }

    /// Drop log entries for batches that are fully accounted for, returning the new watermark.
    ///
    /// Both logs would otherwise grow one object per micro-batch forever — on a 500ms trigger
    /// that is 172,800 objects a day, and listing the checkpoint becomes the slowest thing about
    /// starting a query. Only entries **strictly older** than the last committed batch go, so
    /// the recovery cases above always still have the record they need.
    pub async fn prune_log(&self, pruned_through: u64, committed_batch_id: u64) -> u64 {
        let mut pruned = pruned_through;
        // `committed_batch_id` itself is retained: a crash before the resume record is written
        // recovers its position from that entry.
        while pruned + 1 < committed_batch_id && pruned < pruned_through + PRUNE_BATCH as u64 {
            pruned += 1;
            let _ = self.store.delete(&self.planned_path(pruned)).await;
            let _ = self.store.delete(&self.commit_path(pruned)).await;
        }
        pruned
    }
}

/// How many superseded log entries one commit cleans up. Bounded so pruning costs a predictable
/// amount per batch rather than stalling one trigger with a very long backlog — a query resumed
/// from an old watermark catches up over the batches that follow.
const PRUNE_BATCH: usize = 8;

/// One object under a checkpoint root, as the log-serving endpoints see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointObject {
    /// Path relative to the root the listing was rooted at, `/`-separated.
    pub name: String,
    pub size_bytes: u64,
    /// Last-write time in epoch milliseconds, or `None` where the store reports none.
    pub modified_ms: Option<u64>,
}

/// Generic object access under the checkpoint root.
///
/// Everything a pipeline keeps beside its offsets — the pipeline's per-table epochs, the
/// `reconcile.json` schedule, the connector logs the console tails — used to go through
/// `std::fs` while the offsets went through this store. That split is invisible until the root
/// is an `s3://` URL, at which point the offsets land in the bucket and everything else lands
/// in a local directory named `s3:` under the driver's working directory. The driver then looks
/// healthy right up to the moment it is replaced, and the replacement re-snapshots.
impl CheckpointStore {
    /// A store rooted at `segment` (which may contain `/`) under this one.
    pub fn child(&self, segment: &str) -> Self {
        Self {
            store: self.store.clone(),
            root: join_rel(&self.root, segment),
            location: format!("{}/{segment}", self.location.trim_end_matches('/')),
        }
    }

    /// The object-store path of `rel` under this root.
    pub fn object_path(&self, rel: &str) -> ObjectPath {
        join_rel(&self.root, rel)
    }

    /// `rel` as an operator would type it — the configured location plus the relative key.
    pub fn uri(&self, rel: &str) -> String {
        if rel.is_empty() {
            self.location.clone()
        } else {
            format!("{}/{rel}", self.location.trim_end_matches('/'))
        }
    }

    /// The bytes of `rel`, or `None` when there is no such object.
    pub async fn read(&self, rel: &str) -> std::io::Result<Option<Vec<u8>>> {
        match self.store.get(&self.object_path(rel)).await {
            Ok(result) => Ok(Some(result.bytes().await.map_err(io_err)?.to_vec())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(io_err(e)),
        }
    }

    /// Replace `rel` with `bytes`. A `PUT` is atomic, so no staged temporary file is needed —
    /// see [`CheckpointStore::save`].
    pub async fn write(&self, rel: &str, bytes: Vec<u8>) -> std::io::Result<()> {
        self.store
            .put(&self.object_path(rel), bytes.into())
            .await
            .map_err(io_err)?;
        Ok(())
    }

    /// Delete `rel`, reporting whether there was anything there.
    pub async fn remove(&self, rel: &str) -> std::io::Result<bool> {
        match self.store.delete(&self.object_path(rel)).await {
            Ok(()) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(io_err(e)),
        }
    }

    /// Move `from` to `to`, both relative to this root.
    ///
    /// Copy-then-delete, not `ObjectStore::rename`: S3 has no rename, and the trait's default
    /// implementation is exactly this pair under a name that reads as if it were atomic. A
    /// missing source is not an error — the callers are rotating a generation that may never
    /// have been written.
    pub async fn rename(&self, from: &str, to: &str) -> std::io::Result<()> {
        let (from, to) = (self.object_path(from), self.object_path(to));
        match self.store.copy(&from, &to).await {
            Ok(()) => {}
            Err(object_store::Error::NotFound { .. }) => return Ok(()),
            Err(e) => return Err(io_err(e)),
        }
        match self.store.delete(&from).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(io_err(e)),
        }
    }

    /// Every object directly or transitively under `rel`, named relative to `rel`.
    ///
    /// An empty listing is not an error: an object store has no directories, so "the prefix does
    /// not exist" and "nothing has been written under it yet" are the same fact.
    pub async fn list(&self, rel: &str) -> std::io::Result<Vec<CheckpointObject>> {
        let prefix = self.object_path(rel);
        let mut stream = self.store.list(Some(&prefix));
        let mut out = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(io_err)?;
            let Some(rest) = meta.location.prefix_match(&prefix) else {
                continue;
            };
            let name = rest
                .map(|part| part.as_ref().to_string())
                .collect::<Vec<_>>()
                .join("/");
            if name.is_empty() {
                continue;
            }
            out.push(CheckpointObject {
                name,
                size_bytes: meta.size,
                modified_ms: u64::try_from(meta.last_modified.timestamp_millis()).ok(),
            });
        }
        Ok(out)
    }

    /// Fail now if this root cannot be reached, naming the location.
    ///
    /// Called once at pipeline start. Without it a bogus bucket surfaces as whatever the first
    /// checkpoint write happens to be — an `error` line inside the connector log the same bucket
    /// was supposed to hold, which nobody can read. A single `list` is enough: S3 answers
    /// `NoSuchBucket` for a bucket that is not there and `403` for one this process cannot see,
    /// and both are things an operator must hear at boot rather than an hour in.
    pub async fn probe(&self) -> Result<()> {
        self.store
            .list(Some(&self.root))
            .next()
            .await
            .transpose()
            .map(|_| ())
            .map_err(|e| {
                Error::Io(format!(
                    "checkpoint root `{}` is not reachable: {e}. Checkpoints are the pipeline's \
                     replay position — a root that cannot be written is a pipeline that \
                     re-snapshots on every restart, so this is refused at start rather than \
                     discovered later.",
                    self.location
                ))
            })
    }
}

/// Split a `/`-separated relative key into object-store path parts.
///
/// `ObjectPath::join` takes one part and percent-encodes a `/` inside it, so a key like
/// `logs/orders.jsonl` handed over whole becomes a single object literally named
/// `logs%2Forders.jsonl` — which lists, reads and writes perfectly and is invisible to anything
/// looking for `logs/`.
fn join_rel(root: &ObjectPath, rel: &str) -> ObjectPath {
    let mut path = root.clone();
    for part in rel.split('/').filter(|p| !p.is_empty() && *p != ".") {
        path = path.join(part);
    }
    path
}

/// Resolve a checkpoint root to a store and a prefix within it.
///
/// The **one** place a checkpoint location becomes an object store. Goes through the engine's
/// own resolver, so a checkpoint on S3 uses exactly the credentials, endpoint, and assumed role
/// the table write uses — one auth path, not two. A bare filesystem path is normalized to a
/// `file://` URL first; without that, `s3://bucket/x` and `/tmp/x` are indistinguishable to the
/// URL parser and the object store, and the S3 one silently lands in a local directory named
/// `s3:`.
///
/// Callers with no engine of their own (the one-shot CLI paths — `pipeline show`,
/// `reconcile --cron`) pass a default [`Engine`], which is a cheap in-process constructor and
/// resolves `s3://` from the ambient AWS environment. That is deliberately the same function
/// rather than a second resolver: a second one is how the CLI and the runner end up disagreeing
/// about which bucket the schedule is in.
pub fn checkpoint_store(engine: &Engine, location: &str) -> Result<CheckpointStore> {
    let trimmed = location.trim();
    if trimmed.is_empty() {
        return Err(Error::Plan(
            "checkpoint location is empty — it is the source of truth for replay".into(),
        ));
    }
    let url = if trimmed.contains("://") {
        trimmed.to_string()
    } else {
        let absolute = std::path::Path::new(trimmed);
        let absolute = if absolute.is_absolute() {
            absolute.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|e| Error::Io(format!("resolving `{trimmed}`: {e}")))?
                .join(absolute)
        };
        // The directory has to exist before it can be addressed as a store prefix. Only for a
        // filesystem root: an object store has no directories to create.
        std::fs::create_dir_all(&absolute)
            .map_err(|e| Error::Io(format!("creating checkpoint `{}`: {e}", absolute.display())))?;
        format!("file://{}", absolute.display())
    };

    let store = engine.object_store_for(&url, &HashMap::new())?;
    let parsed = url::Url::parse(&url)
        .map_err(|e| Error::Plan(format!("bad checkpoint location `{location}`: {e}")))?;
    let root = ObjectPath::from(percent_decode(parsed.path()).trim_start_matches('/'));
    Ok(CheckpointStore::new(store, root, trimmed))
}

/// Undo the percent-encoding `Url::parse` applies to a path, so a prefix with a space in it
/// addresses the same object the operator wrote rather than one named `%20`.
fn percent_decode(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(&path[i + 1..i + 3], 16) {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| path.to_string())
}

fn io_err(e: object_store::Error) -> std::io::Error {
    // Not `Error::other`, which is stable since 1.74 and this crate's MSRV is 1.72.
    std::io::Error::new(std::io::ErrorKind::Other, e)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn local_store(dir: &TempDir) -> CheckpointStore {
        let store = object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        CheckpointStore::new(
            Arc::new(store),
            ObjectPath::from(""),
            dir.path().to_string_lossy().into_owned(),
        )
    }

    #[tokio::test]
    async fn a_partial_write_can_never_be_observed() {
        // The failure this rules out: a half-written `offsets.json` parses as nothing, every
        // caller defaults it, and the query silently restarts from `startingOffsets`.
        let dir = TempDir::new().unwrap();
        let store = local_store(&dir);
        let mut state = CheckpointState {
            batch_id: 7,
            committed_batch_id: 7,
            ..Default::default()
        };
        store.save(&state).await.unwrap();
        state.batch_id = 8;
        store.save(&state).await.unwrap();

        // The visible checkpoint always parses, and no staging artifact is left behind.
        assert_eq!(store.load().await.unwrap().batch_id, 8);
        assert!(!dir.path().join("offsets.json.tmp").exists());
    }

    #[tokio::test]
    async fn the_log_distinguishes_the_three_states_a_crash_can_leave() {
        let dir = TempDir::new().unwrap();
        let store = local_store(&dir);
        let range = crate::source::BatchRange {
            source: "kafka".into(),
            start: [("orders-0".to_string(), 100i64)].into_iter().collect(),
            end: [("orders-0".to_string(), 200i64)].into_iter().collect(),
            items: vec![],
        };

        // Nothing recorded: the batch was never planned.
        assert!(store.load_planned(7).await.unwrap().is_none());
        assert!(!store.is_committed(7).await.unwrap());

        // Planned but not committed: replay this exact extent.
        store.save_planned(7, &range).await.unwrap();
        assert_eq!(store.load_planned(7).await.unwrap().as_ref(), Some(&range));
        assert!(!store.is_committed(7).await.unwrap());

        // Committed: the rows are in the sink, and the resume position came from the source.
        store
            .save_commit(&BatchCommit {
                batch_id: 7,
                num_output_rows: 100,
                resume_position: Some(crate::source::SourceOffsets {
                    source: "kafka".into(),
                    entries: [("orders-0".to_string(), 200i64)].into_iter().collect(),
                }),
            })
            .await
            .unwrap();
        let commit = store.load_commit(7).await.unwrap().expect("committed");
        assert_eq!(commit.num_output_rows, 100);
        assert_eq!(
            commit.resume_position.unwrap().entries.get("orders-0"),
            Some(&200)
        );
    }

    #[tokio::test]
    async fn pruning_keeps_the_last_committed_batch_and_never_repeats_a_delete() {
        // The entry for the committed batch is what recovery reads when the resume record did
        // not land, so it has to survive its own commit. And pruning must advance: re-deleting
        // the same window every batch is most of a fast trigger's object-store traffic.
        let dir = TempDir::new().unwrap();
        let store = local_store(&dir);
        let range = crate::source::BatchRange::default();
        for batch_id in 1..=4 {
            store.save_planned(batch_id, &range).await.unwrap();
            store
                .save_commit(&BatchCommit {
                    batch_id,
                    ..Default::default()
                })
                .await
                .unwrap();
        }

        let pruned = store.prune_log(0, 4).await;
        assert_eq!(pruned, 3, "everything below the committed batch");
        assert!(
            store.load_planned(4).await.unwrap().is_some(),
            "the committed batch keeps its range for recovery"
        );
        assert!(store.load_planned(1).await.unwrap().is_none());
        assert!(!store.is_committed(2).await.unwrap());

        // Nothing new to do, and nothing re-deleted.
        assert_eq!(store.prune_log(3, 4).await, 3);
    }

    #[tokio::test]
    async fn checkpoint_round_trip() {
        let dir = TempDir::new().unwrap();
        let store = local_store(&dir);
        let id = StreamingQueryId::new();
        store.init_for_query(&id).await.unwrap();
        let mut state = store.load().await.unwrap();
        state.batch_id = 3;
        state.source_offsets = Some(crate::source::SourceOffsets {
            source: "kafka".into(),
            entries: [("events-0".to_string(), 42i64)].into_iter().collect(),
        });
        store.save(&state).await.unwrap();
        let loaded = store.load().await.unwrap();
        assert_eq!(loaded.batch_id, 3);
        assert_eq!(loaded.source_offsets, state.source_offsets);
    }

    #[tokio::test]
    async fn an_s3_location_never_becomes_a_local_directory_named_s3() {
        // The trap this whole module exists to avoid: `Path::new("s3://bucket/ckpt")` is a
        // *relative* path whose first component is `s3:`, so a checkpoint root resolved through
        // the filesystem lands under the driver's working directory. The query then runs, and
        // its restart-resume story is fiction.
        let engine = Engine::new();
        let store =
            checkpoint_store(&engine, "s3://oxidant-test/pipelines/orders").expect("resolves");
        assert_eq!(store.location(), "s3://oxidant-test/pipelines/orders");
        assert_eq!(
            store.object_path("offsets.json").to_string(),
            "pipelines/orders/offsets.json",
            "the bucket is the store, and the key is the path inside it"
        );
        assert_eq!(
            store.uri("logs/orders.jsonl"),
            "s3://oxidant-test/pipelines/orders/logs/orders.jsonl"
        );
        assert!(
            !std::path::Path::new("s3:").exists(),
            "resolving an s3:// root must not create a local `s3:` directory"
        );
    }

    #[tokio::test]
    async fn a_relative_key_addresses_a_prefix_and_not_an_escaped_name() {
        // `ObjectPath::join` percent-encodes a `/` inside one part, so a key handed over whole
        // becomes a single object named `logs%2Forders.jsonl` — which reads and writes perfectly
        // and is invisible to anything listing `logs/`.
        let dir = TempDir::new().unwrap();
        let store = local_store(&dir);
        store
            .write("logs/orders.jsonl", b"{}\n".to_vec())
            .await
            .unwrap();
        assert!(dir.path().join("logs").join("orders.jsonl").is_file());

        let listed = store.list("logs").await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "orders.jsonl");
        assert_eq!(listed[0].size_bytes, 3);

        assert_eq!(
            store.read("logs/orders.jsonl").await.unwrap().as_deref(),
            Some(&b"{}\n"[..])
        );
        assert!(store.remove("logs/orders.jsonl").await.unwrap());
        assert!(!store.remove("logs/orders.jsonl").await.unwrap());
        assert!(store.list("logs").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_child_store_is_the_same_store_one_prefix_down() {
        let dir = TempDir::new().unwrap();
        let logs = local_store(&dir).child("logs");
        logs.write("orders.jsonl", b"{}\n".to_vec()).await.unwrap();
        assert!(dir.path().join("logs").join("orders.jsonl").is_file());
        assert!(logs.location().ends_with("/logs"));
    }

    #[tokio::test]
    async fn an_empty_location_is_refused_rather_than_resolved_to_the_working_directory() {
        // The unreachable-bucket case needs a real endpoint to be honest about, and lives in
        // `tests/minio_checkpoints.rs` against MinIO. This is the half that needs no network.
        let engine = Engine::new();
        let err = checkpoint_store(&engine, "   ").expect_err("refused");
        assert!(
            err.to_string().contains("source of truth for replay"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn a_missing_checkpoint_reads_as_a_fresh_query() {
        let dir = TempDir::new().unwrap();
        let state = local_store(&dir).load().await.unwrap();
        assert_eq!(state.batch_id, 0);
        assert!(state.query_id.is_empty());
    }
}
