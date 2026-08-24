//! Durable query history: the statement journal, its replay, and the knobs that bound both.
//!
//! Design: `docs/query-history-durability.md`. This module is PR1 of that plan — the journal
//! (self-contained snapshot records, engine-minted ids, the data-dir lock, the fsync discipline),
//! replay at boot, and the two-tier read model the REST statement store reads through. Result
//! spill, rolling exec logs and the log browser are later PRs and are deliberately absent.
//!
//! What lives where:
//!
//! - [`config`] — `OXIDANT_DATA_DIR` and friends, resolved once at boot;
//! - [`record`] — the JSONL record shapes and the seq-monotone fold;
//! - [`journal`] — the writer thread, segments, compaction, replay;
//! - [`lock`] — one process per data dir;
//! - [`fs_util`] — 0600/0700 at create time, and the directory fsync every rename needs.

mod config;
mod fs_util;
mod journal;
mod lock;
mod record;

use std::sync::Arc;

pub(crate) use config::{HistoryConfig, SqlMode};
pub(crate) use journal::Journal;
use record::Fold;
pub(crate) use record::{
    now_rfc3339, rfc3339_from_ms, FoldedStatement, JournalRecord, RecordKind, Source,
    StatementStatus, RECORD_VERSION,
};

/// The history side of the statement store: the locked data dir and the journal writing into it.
pub(crate) struct HistoryRuntime {
    pub(crate) cfg: HistoryConfig,
    pub(crate) journal: Arc<Journal>,
    /// Held for the process's lifetime; dropping it releases the data dir.
    _lock: lock::DataDirLock,
}

impl std::fmt::Debug for HistoryRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoryRuntime")
            .field("root", &self.cfg.root)
            .finish()
    }
}

impl HistoryRuntime {
    /// Lock the data dir, replay the journal, and correct whatever a crash left mid-flight.
    ///
    /// `Err` is a boot failure and is meant to be: a second process on one data dir, or a root
    /// that names an object store, are both misconfigurations that would otherwise corrupt or
    /// silently discard the history an operator asked for.
    pub(crate) fn boot(cfg: HistoryConfig) -> Result<(Self, Fold), String> {
        let data_dir_lock = lock::acquire(&cfg)?;
        let (journal, mut fold) = Journal::open(&cfg).map_err(|e| {
            format!(
                "oxidant: cannot open the statement journal at {}: {e}",
                cfg.statements_dir.display()
            )
        })?;

        // A statement that was pending or running at shutdown is `failed` with an explicit
        // error, never a sixth status value. The correction is written back so the next boot
        // reads it as fact and compaction cannot resurrect `running`.
        let interrupted = fold.mark_interrupted();
        if !interrupted.is_empty() {
            tracing::info!(
                count = interrupted.len(),
                "statement journal: marking statements interrupted by restart"
            );
            for id in &interrupted {
                if let Some(st) = fold.statements.get_mut(id) {
                    st.last_seq = journal.next_seq();
                    st.rank = RecordKind::Snapshot.rank();
                    journal.append_retained(st.to_snapshot());
                }
            }
            journal.sync_blocking();
        }

        Ok((
            Self {
                cfg,
                journal,
                _lock: data_dir_lock,
            },
            fold,
        ))
    }
}
