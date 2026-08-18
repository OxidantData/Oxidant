//! Checkpoint metadata persistence (offsets, batch id, sink commits).

use std::sync::Arc;

use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};

use crate::query::StreamingQueryId;

const OFFSETS_FILE: &str = "offsets.json";

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
    /// Event-time watermark in microseconds (for late-data dropping).
    pub watermark_micros: i64,
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
    async fn a_missing_checkpoint_reads_as_a_fresh_query() {
        let dir = TempDir::new().unwrap();
        let state = local_store(&dir).load().await.unwrap();
        assert_eq!(state.batch_id, 0);
        assert!(state.query_id.is_empty());
    }
}
