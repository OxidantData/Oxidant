//! Durable query history: the statement journal, its replay, and the knobs that bound both.
//!
//! Design: `docs/query-history-durability.md`. PR1 built the journal (self-contained snapshot
//! records, engine-minted ids, the data-dir lock, the fsync discipline), replay at boot, and the
//! two-tier read model the REST statement store reads through. PR2 adds result spill
//! (`results/<id>.arrow`), the disk read-back `/result` falls through to, result GC tied to the
//! journal, and the disk guards `/api/status` reports. Rolling exec logs and the log browser are
//! PR3/PR4 and are deliberately absent.
//!
//! What lives where:
//!
//! - [`config`] — `OXIDANT_DATA_DIR` and friends, resolved once at boot;
//! - [`record`] — the JSONL record shapes and the seq-monotone fold;
//! - [`journal`] — the writer thread, segments, compaction, replay;
//! - [`lock`] — one process per data dir;
//! - [`results`] — the spill writer thread, the read-back, and result GC;
//! - [`disk`] — the byte budget, the free-space floor, and the prune order;
//! - [`fs_util`] — 0600/0700 at create time, and the directory fsync every rename needs.

mod config;
pub(crate) mod disk;
mod fs_util;
mod journal;
mod lock;
mod record;
mod results;

use std::sync::Arc;

pub(crate) use config::{HistoryConfig, ResultPersist, SqlMode};
pub(crate) use journal::Journal;
use record::Fold;
pub(crate) use record::{
    now_rfc3339, rfc3339_from_ms, FoldedStatement, JournalRecord, RecordKind, ResultPointer,
    Source, StatementStatus, RECORD_VERSION, RESULT_TOO_LARGE,
};
#[cfg(test)]
pub(crate) use results::SPILL_QUEUE;
pub(crate) use results::{ResultStore, SpillJob, SpillOutcome};

/// The history side of the statement store: the locked data dir and the journal writing into it.
pub(crate) struct HistoryRuntime {
    pub(crate) cfg: HistoryConfig,
    pub(crate) journal: Arc<Journal>,
    /// The spilled-result tier. Present whenever history is, including under
    /// `OXIDANT_RESULT_PERSIST=never` — the directory and the counters exist, the writes do not.
    pub(crate) results: Arc<ResultStore>,
    /// Held for the process's lifetime; dropping it releases the journal dir.
    _lock: lock::JournalDirLock,
}

impl std::fmt::Debug for HistoryRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoryRuntime")
            .field("root", &self.cfg.root)
            .field("statements_dir", &self.cfg.statements_dir)
            .field("results_dir", &self.cfg.results_dir)
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

        let results = ResultStore::open(&cfg, Arc::clone(&journal)).map_err(|e| {
            format!(
                "oxidant: cannot open the result spill directory at {}: {e}",
                cfg.results_dir.display()
            )
        })?;
        // Boot reconciles `results/` against the folded id set and unlinks the orphans (§5, F13).
        // This is what closes the crash window between "tombstone appended" and "file unlinked":
        // a result file outlives its statement's journal record by at most one retention sweep,
        // and never across a restart.
        let live: std::collections::HashSet<String> = fold.statements.keys().cloned().collect();
        let _ = results.reconcile(&live);

        Ok((
            Self {
                cfg,
                journal,
                results,
                _lock: data_dir_lock,
            },
            fold,
        ))
    }
}
