//! Operational status snapshot — the driver-health view a control plane polls for
//! auto-termination and autoscaling signals.
//!
//! Every field is derived from the same query lifecycle events the monitoring UI reads
//! ([`crate::QueryTracker`] → [`crate::AppStateStore`]); nothing here is synthesized.
//! Served over HTTP by `oxidant-ui-server` as `GET /api/status`.

use serde::{Deserialize, Serialize};

/// Per-query summary in a [`StatusSnapshot`].
///
/// Field names are snake_case (unlike the Spark-compatible `/api/v1` DTOs in
/// [`crate::model`]): this is an Oxidant-native operational API, not a Spark mirror.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryStatus {
    /// Spark Connect `operation_id` (or the REST statement id) the query ran under.
    pub id: String,
    /// The query's description: truncated SQL text, or `DataFrame` for a Connect plan.
    /// The engine does not carry client-supplied job tags today, so this is the only
    /// label a query actually has.
    pub tag: String,
    /// `running`, `finished`, `failed`, or `unknown`.
    pub state: String,
    /// RFC3339 submission time.
    pub started_at: String,
    /// Wall time from submission to completion; for a running query, to *now*.
    pub duration_ms: i64,
    /// Output rows reported by the last stage to finish (0 while still running).
    pub rows: i64,
    /// Bytes shuffled (read + written) across the query's stages. Always 0 for a
    /// single-node query, which never shuffles.
    pub bytes: i64,
}

/// Durability counters for `GET /api/status` — the statement journal, the spilled results, and
/// the disk guards that bound both (`docs/query-history-durability.md` §3, §7).
///
/// Published by whoever owns the journal (`oxidant-connect`'s statement store) through
/// [`set_history_status_source`], rather than being reachable from here: this crate sits *below*
/// `oxidant-connect` in the dependency graph, and inverting that to reach a `StatementStore`
/// would make the observability crate depend on the Spark Connect server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryStatus {
    /// The **aggregate** of the three durability subsystems — the journal, the result spill
    /// writer, and the disk sweep. `degraded` while any one of them is; `ok` only when all three
    /// are. Each subsystem's flag is sticky until a success *of its own* clears it, so a healthy
    /// journal can no longer report a failing result volume as `ok` (§7).
    ///
    /// [`Self::result_writes`] and [`Self::disk`] say which one, and no restart is needed for
    /// either to flip back.
    pub history_writes: String,
    /// Work history gave up on under backpressure: journal records the writer had no room for
    /// (`running` chatter and tombstones, never a `submitted` or a `snapshot`) **plus** spill
    /// jobs the spill queue had no room for. Both are dropped work, and an operator watching
    /// "am I losing history" should not have to know there are two queues (§7).
    pub history_dropped_events: u64,
    /// Total size of `history/results/*.arrow`.
    pub results_on_disk_bytes: u64,
    /// The result spill writer alone: `ok`, or `degraded` once a spill was refused by the disk
    /// or dropped for backpressure. Cleared only by a spill that lands.
    pub result_writes: String,
    /// Spills the disk refused outright (ENOSPC/EIO/…). Distinct from
    /// [`Self::history_dropped_events`], which counts jobs that never reached the disk at all.
    pub result_write_failures: u64,
    /// `ok`; `over_budget` once the sweeper has run out of things to prune under
    /// `OXIDANT_DISK_MAX_BYTES`; or `low_free` when the volume holding a managed directory is
    /// below `OXIDANT_DISK_MIN_FREE_BYTES` — a shortfall the engine did not necessarily cause,
    /// and never prunes history for. `over_budget` wins when both hold, because it is the one
    /// the engine can act on (§3).
    pub disk: String,
}

/// The two values [`HistoryStatus::history_writes`] and [`HistoryStatus::result_writes`] take.
pub mod history_writes {
    pub const OK: &str = "ok";
    pub const DEGRADED: &str = "degraded";
}

/// The three values [`HistoryStatus::disk`] takes.
pub mod disk_state {
    pub const OK: &str = "ok";
    /// The engine's own subtree is past `OXIDANT_DISK_MAX_BYTES` with nothing left to prune.
    pub const OVER_BUDGET: &str = "over_budget";
    /// The volume is below `OXIDANT_DISK_MIN_FREE_BYTES`. The engine stops spilling and reports;
    /// it does **not** delete history it did not overspend on.
    pub const LOW_FREE: &str = "low_free";
}

type HistoryStatusSource = Box<dyn Fn() -> HistoryStatus + Send + Sync>;

static HISTORY_STATUS: std::sync::OnceLock<HistoryStatusSource> = std::sync::OnceLock::new();

/// Publish the durability counters `/api/status` reads. Called once, at boot, by whoever booted
/// the journal. With `OXIDANT_HISTORY=off` it is never called and the four fields are **absent**
/// from the response — §8 says `off` restores today's behaviour exactly, and today there are no
/// such fields.
pub fn set_history_status_source(source: impl Fn() -> HistoryStatus + Send + Sync + 'static) {
    let _ = HISTORY_STATUS.set(Box::new(source));
}

/// The published counters, or `None` when history is off (or has not booted yet).
pub fn history_status() -> Option<HistoryStatus> {
    HISTORY_STATUS.get().map(|f| f())
}

/// Driver status: `GET /api/status`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusSnapshot {
    /// Engine version (`CARGO_PKG_VERSION` of the running binary's workspace).
    pub version: String,
    /// Seconds since this driver's store was created, i.e. since process start.
    pub uptime_secs: i64,
    /// Most recent query lifecycle transition — a submission *or* a completion, whichever
    /// is later — as RFC3339. `None` until the first query runs. This is the
    /// "idle since" signal: a driver with `active_queries == 0` has been idle since here.
    pub last_query_at: Option<String>,
    /// Queries currently running, counted across the whole store (not just [`Self::queries`]).
    pub active_queries: usize,
    /// Queries admitted but not yet started. The engine has no admission queue — queries
    /// execute on arrival — so this is always 0 today; it exists so a poller written
    /// against this API keeps working if one is added.
    pub queued_queries: usize,
    /// Most recent queries, newest first, capped by the caller's limit.
    pub queries: Vec<QueryStatus>,
    /// Durability counters, flattened into this object as `history_writes`,
    /// `history_dropped_events`, `results_on_disk_bytes` and `disk`. Absent with
    /// `OXIDANT_HISTORY=off`.
    #[serde(default, flatten, skip_serializing_if = "Option::is_none")]
    pub history: Option<HistoryStatus>,
}

/// Query state strings used by [`QueryStatus::state`].
pub mod query_state {
    pub const RUNNING: &str = "running";
    pub const FINISHED: &str = "finished";
    pub const FAILED: &str = "failed";
    pub const UNKNOWN: &str = "unknown";
}

#[cfg(test)]
mod tests {
    use crate::status::query_state;
    use crate::store::{AppStateStore, SharedStore};
    use crate::QueryTracker;
    use std::sync::Arc;

    fn store() -> SharedStore {
        Arc::new(AppStateStore::new())
    }

    /// A driver that has never run a query must report `last_query_at: null` rather than
    /// "now" — the control plane reads this as "idle since", and a fabricated timestamp
    /// would keep an empty cluster alive forever.
    #[test]
    fn fresh_store_has_no_last_query_and_no_active_queries() {
        let snap = store().status_snapshot(10);
        assert_eq!(snap.last_query_at, None);
        assert_eq!(snap.active_queries, 0);
        assert_eq!(snap.queued_queries, 0);
        assert!(snap.queries.is_empty());
        assert_eq!(snap.version, env!("CARGO_PKG_VERSION"));
        assert!(snap.uptime_secs >= 0);
    }

    /// The whole point of the endpoint: a query in flight is visible, and finishing it
    /// both drops `active_queries` and advances `last_query_at`.
    #[test]
    fn running_query_becomes_finished_and_advances_last_query_at() {
        let store = store();
        let mut tracker = QueryTracker::begin(store.clone(), "op-1", "SELECT 1");
        tracker.begin_local_stage("local", 1);

        let running = store.status_snapshot(10);
        assert_eq!(running.active_queries, 1);
        assert_eq!(running.queries.len(), 1);
        assert_eq!(running.queries[0].id, "op-1");
        assert_eq!(running.queries[0].tag, "SELECT 1");
        assert_eq!(running.queries[0].state, query_state::RUNNING);
        assert_eq!(running.queries[0].rows, 0);
        let started = running.last_query_at.clone().expect("submission timestamp");

        tracker.finish_success(7);

        let done = store.status_snapshot(10);
        assert_eq!(done.active_queries, 0);
        assert_eq!(done.queries[0].state, query_state::FINISHED);
        assert_eq!(done.queries[0].rows, 7);
        let finished = done.last_query_at.expect("completion timestamp");
        assert!(
            finished >= started,
            "last_query_at went backwards: {started} -> {finished}"
        );
    }

    #[test]
    fn failed_query_reports_failed_state() {
        let store = store();
        let mut tracker = QueryTracker::begin(store.clone(), "op-boom", "SELECT boom");
        tracker.begin_stage(2, "hash-agg", 2);
        tracker.finish_error("boom");

        let snap = store.status_snapshot(10);
        assert_eq!(snap.active_queries, 0);
        assert_eq!(snap.queries.len(), 1);
        assert_eq!(snap.queries[0].state, query_state::FAILED);
    }

    /// Shuffle bytes come from the stages the query actually ran; a query with a shuffle
    /// must not report 0.
    #[test]
    fn bytes_sum_shuffle_across_stages_and_rows_take_the_last_stage() {
        let store = store();
        let mut tracker = QueryTracker::begin(store.clone(), "op-dist", "SELECT count(*)");
        tracker.begin_stage(0, "partial", 2);
        tracker.finish_stage(0, 1_000, 0, 4_096);
        tracker.begin_stage(1, "final", 1);
        tracker.finish_success(3);

        let snap = store.status_snapshot(10);
        let q = &snap.queries[0];
        assert_eq!(q.bytes, 4_096, "stage 0's shuffle write must be counted");
        assert_eq!(q.rows, 3, "rows come from the last stage, not the sum");
    }

    /// `queries` is capped for the poller's benefit, but the counters must still see every
    /// job — otherwise a busy driver would look idle.
    #[test]
    fn recent_limit_truncates_queries_but_not_counters() {
        let store = store();
        for i in 0..5 {
            let mut tracker = QueryTracker::begin(store.clone(), format!("op-{i}"), "SELECT 1");
            tracker.begin_local_stage("local", 1);
        }
        let snap = store.status_snapshot(2);
        assert_eq!(snap.queries.len(), 2);
        assert_eq!(snap.active_queries, 5);
        // Newest first: the last submitted job leads the list.
        assert_eq!(snap.queries[0].id, "op-4");
        assert_eq!(snap.queries[1].id, "op-3");
    }

    /// The wire contract the control plane parses: exact snake_case keys, `last_query_at`
    /// nullable, `queries` an array.
    #[test]
    fn snapshot_serializes_with_the_documented_keys() {
        let store = store();
        QueryTracker::begin(store.clone(), "op-shape", "SELECT 1");
        let value = serde_json::to_value(store.status_snapshot(10)).unwrap();
        let obj = value.as_object().expect("object");
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "active_queries",
                "last_query_at",
                "queries",
                "queued_queries",
                "uptime_secs",
                "version",
            ]
        );
        let q = value["queries"][0].as_object().expect("query object");
        let mut qkeys: Vec<&str> = q.keys().map(String::as_str).collect();
        qkeys.sort_unstable();
        assert_eq!(
            qkeys,
            [
                "bytes",
                "duration_ms",
                "id",
                "rows",
                "started_at",
                "state",
                "tag",
            ]
        );
    }
}
