//! Journal records and the fold that turns a pile of them back into statement state.
//!
//! Two shapes, both self-contained enough to be read alone (§4a): the lifecycle events
//! (`submitted`, `running`) and the `snapshot` that carries a statement's complete folded state.
//! Compaction emits snapshots, which is what lets it drop everything older without losing a
//! statement's SQL — the load-bearing property F1 turned on.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

/// Record schema version. Bumped only for a change a v1 reader cannot fold.
pub(crate) const RECORD_VERSION: u32 = 1;

/// Statement lifecycle, serialized lowercase exactly as the API contract spells it.
///
/// The journal and `GET /api/v1/statements` share this one type on purpose: there is no
/// translation table between the file and the API to get wrong (§4e, F18). A statement left
/// non-terminal by a crash replays as [`StatementStatus::Failed`] with an explicit error string,
/// **not** as a sixth value that every client switching on the documented five would miss.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum StatementStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Canceled,
}

impl StatementStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
        }
    }

    pub(crate) fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Canceled)
    }
}

/// Where a statement was submitted from. `connect` is what unifies the history for issue #134.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Source {
    Rest,
    Connect,
}

impl Source {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Rest => "rest",
            Self::Connect => "connect",
        }
    }

    pub(crate) fn parse(raw: &str) -> Self {
        match raw {
            "connect" => Self::Connect,
            _ => Self::Rest,
        }
    }
}

/// The event a record names. The terminal *status* lives in `status`, so there is no
/// `finished`/`cancelled` kind duplicating it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum RecordKind {
    Submitted,
    Running,
    Snapshot,
    Tombstone,
}

impl RecordKind {
    /// Tie-break for two records of one statement that share a sequence: a `submitted` never
    /// overwrites the `running` or `snapshot` that followed it, whichever order they are read in.
    pub(crate) fn rank(self) -> u8 {
        match self {
            Self::Submitted => 0,
            Self::Running => 1,
            Self::Snapshot => 2,
            Self::Tombstone => 3,
        }
    }
}

/// One line of `seg-NNNNNN.jsonl`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JournalRecord {
    pub v: u32,
    pub kind: RecordKind,
    /// The statement's *submit* sequence: monotonic, and stable for the statement's whole life.
    pub seq: u64,
    /// The write sequence of the newest event folded into a snapshot. Absent on lifecycle events,
    /// where `seq` is the fold key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seq: Option<u64>,
    /// `stmt-<uuid>`, engine-minted — the only identity that ever reaches a filesystem path.
    pub id: String,
    /// The client's `operation_id`, validated, kept as an alias. Never a fold key (§4b).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_op_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    /// Echoes `OXIDANT_HISTORY_SQL` at write time, so a journal whose policy changed mid-life is
    /// still readable honestly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql_encoding: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<StatementStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<Vec<(String, String)>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
    pub submitted_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<i64>,
    /// RFC-3339 UTC, human-facing only. All ordering and age arithmetic uses `seq` and
    /// `submitted_at_ms`, never this string.
    pub ts: String,
}

impl JournalRecord {
    /// The sequence the fold orders this record by.
    pub(crate) fn fold_key(&self) -> (u64, u8) {
        (self.last_seq.unwrap_or(self.seq), self.kind.rank())
    }

    pub(crate) fn tombstone(id: &str, seq: u64, submitted_at_ms: i64) -> Self {
        Self {
            v: RECORD_VERSION,
            kind: RecordKind::Tombstone,
            seq,
            last_seq: Some(seq),
            id: id.to_string(),
            client_op_id: None,
            session: None,
            source: None,
            sql: None,
            sql_encoding: None,
            status: None,
            error: None,
            schema: None,
            rows: None,
            submitted_at_ms,
            duration_ms: None,
            ts: now_rfc3339(),
        }
    }
}

/// RFC-3339 UTC to milliseconds, the `ts` field's format.
pub(crate) fn now_rfc3339() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

/// Milliseconds since the epoch rendered as RFC-3339 UTC.
pub(crate) fn rfc3339_from_ms(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|t| t.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        .unwrap_or_else(now_rfc3339)
}

/// The complete folded state of one statement — everything the API's status document needs, with
/// no reference to any other record.
#[derive(Debug, Clone)]
pub(crate) struct FoldedStatement {
    pub id: String,
    pub client_op_id: Option<String>,
    pub session: Option<String>,
    pub source: Source,
    pub sql: String,
    pub sql_encoding: String,
    pub status: StatementStatus,
    pub error: Option<String>,
    pub schema: Option<Vec<(String, String)>>,
    pub rows: Option<u64>,
    pub submitted_at_ms: i64,
    pub duration_ms: Option<i64>,
    /// The statement's submit sequence — newest-first listing and oldest-first eviction.
    pub seq: u64,
    /// The write sequence of the newest event folded in; the fold's monotone key.
    pub last_seq: u64,
    /// The kind rank of that newest event, breaking ties at an equal sequence.
    pub rank: u8,
}

impl FoldedStatement {
    /// The self-contained record compaction writes for this statement.
    pub(crate) fn to_snapshot(&self) -> JournalRecord {
        JournalRecord {
            v: RECORD_VERSION,
            kind: RecordKind::Snapshot,
            seq: self.seq,
            last_seq: Some(self.last_seq),
            id: self.id.clone(),
            client_op_id: self.client_op_id.clone(),
            session: self.session.clone(),
            source: Some(self.source.as_str().to_string()),
            sql: Some(self.sql.clone()),
            sql_encoding: Some(self.sql_encoding.clone()),
            status: Some(self.status),
            error: self.error.clone(),
            schema: self.schema.clone(),
            rows: self.rows,
            submitted_at_ms: self.submitted_at_ms,
            duration_ms: self.duration_ms,
            ts: rfc3339_from_ms(self.submitted_at_ms),
        }
    }
}

/// The result of replaying the journal: one entry per surviving statement.
///
/// The fold is **seq-monotone**, which makes it order-independent and idempotent: a record is
/// applied only when its `(fold_key)` is at least the one already folded for that id. Reading the
/// same compacted generation twice — the state a crashed compaction swap leaves behind — changes
/// nothing.
#[derive(Debug, Default)]
pub(crate) struct Fold {
    pub statements: HashMap<String, FoldedStatement>,
    /// Ids a tombstone retired; they must not come back when an older segment is read after it.
    pub tombstoned: HashSet<String>,
    /// The highest sequence seen anywhere, so the writer resumes above it.
    pub max_seq: u64,
    /// Records folded, for compaction's superseded ratio.
    pub records: u64,
}

impl Fold {
    pub(crate) fn apply(&mut self, rec: JournalRecord) {
        self.records += 1;
        let (key, rank) = rec.fold_key();
        self.max_seq = self.max_seq.max(key).max(rec.seq);
        if self.tombstoned.contains(&rec.id) {
            return;
        }
        if rec.kind == RecordKind::Tombstone {
            self.statements.remove(&rec.id);
            self.tombstoned.insert(rec.id);
            return;
        }
        match self.statements.get_mut(&rec.id) {
            Some(existing) => {
                if (key, rank) < (existing.last_seq, existing.rank) {
                    return;
                }
                existing.last_seq = key;
                existing.rank = rank;
                if let Some(status) = rec.status {
                    existing.status = status;
                }
                if let Some(sql) = rec.sql {
                    existing.sql = sql;
                }
                if let Some(enc) = rec.sql_encoding {
                    existing.sql_encoding = enc;
                }
                if let Some(source) = rec.source {
                    existing.source = Source::parse(&source);
                }
                if rec.session.is_some() {
                    existing.session = rec.session;
                }
                if rec.client_op_id.is_some() {
                    existing.client_op_id = rec.client_op_id;
                }
                if rec.error.is_some() {
                    existing.error = rec.error;
                }
                if rec.schema.is_some() {
                    existing.schema = rec.schema;
                }
                if rec.rows.is_some() {
                    existing.rows = rec.rows;
                }
                if rec.duration_ms.is_some() {
                    existing.duration_ms = rec.duration_ms;
                }
            }
            None => {
                self.statements.insert(
                    rec.id.clone(),
                    FoldedStatement {
                        id: rec.id,
                        client_op_id: rec.client_op_id,
                        session: rec.session,
                        source: rec
                            .source
                            .as_deref()
                            .map(Source::parse)
                            .unwrap_or(Source::Rest),
                        sql: rec.sql.unwrap_or_default(),
                        sql_encoding: rec.sql_encoding.unwrap_or_else(|| "text".to_string()),
                        status: rec.status.unwrap_or(StatementStatus::Pending),
                        error: rec.error,
                        schema: rec.schema,
                        rows: rec.rows,
                        submitted_at_ms: rec.submitted_at_ms,
                        duration_ms: rec.duration_ms,
                        seq: rec.seq,
                        last_seq: key,
                        rank,
                    },
                );
            }
        }
    }

    /// Mark every statement a crash left non-terminal.
    ///
    /// `failed` + this error string, never a sixth status value (§4e). Returns the ids marked so
    /// the caller can write the correction back to the journal — otherwise every boot would
    /// re-derive it and compaction would resurrect `running`.
    pub(crate) fn mark_interrupted(&mut self) -> Vec<String> {
        let mut marked = Vec::new();
        for (id, st) in self.statements.iter_mut() {
            if !st.status.is_terminal() {
                st.status = StatementStatus::Failed;
                st.error = Some(INTERRUPTED_BY_RESTART.to_string());
                marked.push(id.clone());
            }
        }
        marked
    }
}

/// The error text a statement that was still running at shutdown replays with.
pub(crate) const INTERRUPTED_BY_RESTART: &str = "interrupted by restart";

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(kind: RecordKind, id: &str, seq: u64, last_seq: Option<u64>) -> JournalRecord {
        JournalRecord {
            v: RECORD_VERSION,
            kind,
            seq,
            last_seq,
            id: id.to_string(),
            client_op_id: None,
            session: Some("s1".to_string()),
            source: Some("rest".to_string()),
            sql: matches!(kind, RecordKind::Submitted | RecordKind::Snapshot)
                .then(|| "SELECT 1".to_string()),
            sql_encoding: Some("text".to_string()),
            status: Some(match kind {
                RecordKind::Submitted => StatementStatus::Pending,
                RecordKind::Running => StatementStatus::Running,
                _ => StatementStatus::Succeeded,
            }),
            error: None,
            schema: None,
            rows: None,
            submitted_at_ms: 1_000,
            duration_ms: None,
            ts: now_rfc3339(),
        }
    }

    #[test]
    fn fold_is_order_independent_and_idempotent() {
        let records = vec![
            rec(RecordKind::Submitted, "stmt-a", 1, None),
            rec(RecordKind::Running, "stmt-a", 1, None),
            rec(RecordKind::Snapshot, "stmt-a", 1, Some(7)),
        ];
        let mut folds = Vec::new();
        for order in [[0, 1, 2], [2, 1, 0], [1, 2, 0], [2, 0, 1]] {
            let mut fold = Fold::default();
            for i in order {
                fold.apply(records[i].clone());
            }
            // Double-fold the terminal record: a crashed compaction swap leaves exactly this.
            fold.apply(records[2].clone());
            let st = fold.statements.get("stmt-a").expect("folded").clone();
            folds.push((st.status, st.sql, st.seq, st.last_seq));
        }
        assert!(
            folds.windows(2).all(|w| w[0] == w[1]),
            "fold must not depend on read order: {folds:?}"
        );
        assert_eq!(folds[0].0, StatementStatus::Succeeded);
        assert_eq!(folds[0].1, "SELECT 1");
    }

    #[test]
    fn a_tombstone_is_not_undone_by_an_older_record() {
        let mut fold = Fold::default();
        fold.apply(JournalRecord::tombstone("stmt-a", 9, 1_000));
        fold.apply(rec(RecordKind::Submitted, "stmt-a", 1, None));
        assert!(!fold.statements.contains_key("stmt-a"));
    }

    #[test]
    fn interrupted_statements_replay_as_failed_not_a_sixth_status() {
        let mut fold = Fold::default();
        fold.apply(rec(RecordKind::Running, "stmt-a", 1, None));
        let marked = fold.mark_interrupted();
        assert_eq!(marked, vec!["stmt-a".to_string()]);
        let st = &fold.statements["stmt-a"];
        assert_eq!(st.status, StatementStatus::Failed);
        assert_eq!(st.error.as_deref(), Some(INTERRUPTED_BY_RESTART));
    }

    /// The journal invents no vocabulary of its own: every status it can serialize is a value the
    /// API already documents, spelled identically.
    #[test]
    fn journal_status_vocabulary_matches_the_api() {
        for status in [
            StatementStatus::Pending,
            StatementStatus::Running,
            StatementStatus::Succeeded,
            StatementStatus::Failed,
            StatementStatus::Canceled,
        ] {
            let json = serde_json::to_string(&status).expect("serialize");
            assert_eq!(json, format!("\"{}\"", status.as_str()));
            let back: StatementStatus = serde_json::from_str(&json).expect("round-trip");
            assert_eq!(back, status);
        }
        // And nothing else parses — no `interrupted`, no `cancelled`, no `finished`.
        for bogus in ["\"interrupted\"", "\"cancelled\"", "\"finished\""] {
            assert!(
                serde_json::from_str::<StatementStatus>(bogus).is_err(),
                "{bogus}"
            );
        }
    }
}
