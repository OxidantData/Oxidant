//! Forward-fallback coverage: any locally plannable SQL gets a distributed plan.

use weft_execution::driver::ExchangeMode;
use weft_execution::plan::plan_distributed;
use weft_loom::Engine;

#[tokio::test]
async fn replicated_only_forward() {
    let e = Engine::new();
    let b = e
        .sql("SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(k, v)")
        .await
        .unwrap();
    e.register_batches("dim", b).unwrap();
    let dq = plan_distributed(&e, "SELECT k, sum(v) AS s FROM dim GROUP BY k", &["dim"])
        .await
        .expect("should plan");
    assert_eq!(dq.stages.len(), 1);
    assert_eq!(dq.stages[0].exchange, ExchangeMode::Forward);
}

#[tokio::test]
async fn subquery_sql_gets_forward_plan() {
    let e = Engine::new();
    let b = e
        .sql("SELECT * FROM (VALUES (1, 10), (1, 20), (2, 30)) AS t(k, v)")
        .await
        .unwrap();
    e.register_batches("lineitem", b.clone()).unwrap();
    e.register_batches("part", b).unwrap();
    let sql = "SELECT sum(k) AS s FROM lineitem WHERE k IN (SELECT k FROM part)";
    let dq = plan_distributed(&e, sql, &["part"])
        .await
        .expect("fallback plan");
    assert!(!dq.stages.is_empty());
    // May be shaped global-agg (Hash) or Forward — either is a valid distributed plan.
}

#[tokio::test]
async fn sharded_group_still_multi_stage() {
    let e = Engine::new();
    let b = e
        .sql("SELECT * FROM (VALUES (1, 10), (2, 20), (1, 5)) AS t(k, v)")
        .await
        .unwrap();
    e.register_batches("t", b).unwrap();
    let dq = plan_distributed(&e, "SELECT k, sum(v) AS s FROM t GROUP BY k", &[])
        .await
        .expect("shaped plan");
    assert!(dq.stages.len() >= 2);
    assert!(dq
        .stages
        .iter()
        .all(|s| s.exchange != ExchangeMode::Forward));
}
