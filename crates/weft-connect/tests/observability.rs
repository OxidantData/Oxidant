//! Integration smoke: observability store records jobs after SQL execution path.

use std::sync::Arc;

use weft_connect::WeftService;
use weft_observability::AppStateStore;

#[tokio::test]
async fn observability_records_local_sql_job() {
    let store = Arc::new(AppStateStore::new());
    let svc = WeftService::with_store(store.clone());
    let engine = svc.engine().clone();
    engine
        .sql("CREATE TABLE t AS SELECT 1 AS x")
        .await
        .expect("ddl");

    let op = "test-op-1";
    let mut tracker = weft_observability::QueryTracker::begin(store.clone(), op, "SELECT x FROM t");
    tracker.begin_local_stage("local", 1);
    let task_id = store.alloc_task_id();
    tracker.task_started(0, task_id, "driver");
    let batches = engine.sql("SELECT x FROM t").await.expect("query");
    let rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
    tracker.task_finished(0, task_id, "driver", 1, rows, 0, 0);
    tracker.finish_success(rows);

    let jobs = store.list_jobs(None);
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].status, "SUCCEEDED");
    assert_eq!(
        store.operation_state(op),
        Some(weft_observability::OperationState::Succeeded)
    );
    let sql = store.list_sql();
    assert_eq!(sql.len(), 1);
}
