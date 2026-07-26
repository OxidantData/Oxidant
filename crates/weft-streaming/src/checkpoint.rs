//! Checkpoint metadata persistence (offsets, batch id, sink commits).

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::query::StreamingQueryId;

const OFFSETS_FILE: &str = "offsets.json";
const METADATA_FILE: &str = "metadata";

/// Persisted checkpoint state for a streaming query.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CheckpointState {
    pub query_id: String,
    pub run_id: String,
    pub batch_id: u64,
    pub source_offsets: Vec<String>,
    /// Last batch id successfully committed to the sink (exactly-once semantics).
    pub committed_batch_id: u64,
    /// Event-time watermark in microseconds (for late-data dropping).
    pub watermark_micros: i64,
}

/// Filesystem-backed checkpoint store.
#[derive(Debug, Clone)]
pub struct CheckpointStore {
    root: PathBuf,
}

impl CheckpointStore {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.root
    }

    pub fn load(&self) -> std::io::Result<CheckpointState> {
        let p = self.root.join(OFFSETS_FILE);
        if !p.exists() {
            return Ok(CheckpointState::default());
        }
        let text = std::fs::read_to_string(p)?;
        serde_json::from_str(&text)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    pub fn save(&self, state: &CheckpointState) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.root)?;
        std::fs::create_dir_all(self.root.join(METADATA_FILE))?;
        let text = serde_json::to_string_pretty(state)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        std::fs::write(self.root.join(OFFSETS_FILE), text)
    }

    pub fn init_for_query(&self, id: &StreamingQueryId) -> std::io::Result<()> {
        let state = CheckpointState {
            query_id: id.id.clone(),
            run_id: id.run_id.clone(),
            batch_id: 0,
            source_offsets: vec![],
            committed_batch_id: 0,
            watermark_micros: 0,
        };
        self.save(&state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn checkpoint_round_trip() {
        let dir = TempDir::new().unwrap();
        let store = CheckpointStore::new(dir.path());
        let id = StreamingQueryId::new();
        store.init_for_query(&id).unwrap();
        let mut state = store.load().unwrap();
        state.batch_id = 3;
        state.source_offsets = vec!["file1.parquet".into()];
        store.save(&state).unwrap();
        let loaded = store.load().unwrap();
        assert_eq!(loaded.batch_id, 3);
        assert_eq!(loaded.source_offsets, vec!["file1.parquet"]);
    }
}
