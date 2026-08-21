//! Adaptive join-order re-optimization end-to-end (`OXIDANT_REOPT_JOIN_ORDER=1`): the driver
//! front-loads the leaf producer stages of a shuffle-join chain, and at the LAST leaf's
//! stage barrier — once every chain leaf has a barrier-measured row count — re-sequences
//! the chain's tail joins by measured right-leaf rows (smallest first) and splices the
//! re-derived stages onto the dispatched prefix. This is the reorder Spark AQE
//! structurally cannot do (its stage graph is fixed at planning time).
//!
//! A four-table inner chain with one tiny + one huge tail leaf must come back with the
//! tail permuted — the `ReoptimizedJoinOrder` event is the evidence — while the result
//! matches the single-node ground truth row-for-row. With the gate off, with a non-inner
//! join in the chain, or with `OXIDANT_STAGE_INPUT_STATS=0` (no leaf measurements), the
//! driver must run the original plan untouched: correct results, no event.

use std::sync::Arc;

use oxidant_execution::driver::{reopt_join_order_enabled, run_stages_obs, Cluster, ReoptContext};
use oxidant_execution::flight::serve_worker;
use oxidant_execution::plan::plan_distributed_logical;
use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;
use oxidant_observability::{AppStateStore, ExecutionEvent};

/// `OXIDANT_REOPT_JOIN_ORDER` / `OXIDANT_STAGE_INPUT_STATS` are process-global; serialize.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const TA_ROWS: i64 = 2_000;
const TB_ROWS: i64 = 2_000;
const TC_ROWS: i64 = 6_000; // huge tail leaf
const TD_ROWS: i64 = 100; // tiny tail leaf — written last, must join first after re-opt

/// The four-table inner chain: `ta ⋈ tb` is the fixed first join; the tail `⋈ tc ⋈ td`
/// is permutable (tc keyed on tb, td keyed on ta — no tail-internal dependencies).
const CHAIN_SQL: &str = "SELECT ta.g AS g, COUNT(*) AS c FROM ta \
     JOIN tb ON ta.k = tb.k JOIN tc ON tb.k = tc.k JOIN td ON ta.k = td.k \
     GROUP BY ta.g";

/// Same chain with the last join LEFT: re-opt must bail (non-inner join) and leave the
/// plan untouched.
const LEFT_CHAIN_SQL: &str = "SELECT ta.g AS g, COUNT(td.k) AS c FROM ta \
     JOIN tb ON ta.k = tb.k JOIN tc ON tb.k = tc.k LEFT JOIN td ON ta.k = td.k \
     GROUP BY ta.g";

fn batch(cols: Vec<(&str, Vec<i64>)>) -> RecordBatch {
    let schema = Arc::new(Schema::new(
        cols.iter()
            .map(|(n, _)| Field::new(*n, DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(
        schema,
        cols.into_iter()
            .map(|(_, v)| {
                Arc::new(Int64Array::from(v)) as Arc<dyn oxidant_loom::arrow::array::Array>
            })
            .collect(),
    )
    .unwrap()
}

/// ta(k, g): k in [start, end), g = k % 50.
fn ta(start: i64, end: i64) -> RecordBatch {
    let k: Vec<i64> = (start..end).collect();
    let g: Vec<i64> = (start..end).map(|i| i % 50).collect();
    batch(vec![("k", k), ("g", g)])
}

/// Single-key table whose keys are the range [start, end).
fn keyed(start: i64, end: i64, col: &str) -> RecordBatch {
    let k: Vec<i64> = (start..end).collect();
    batch(vec![(col, k)])
}

/// Register every chain table on `engine`: `ta`/`tb` sliced by row range, the cycling
/// `tc`/`td` key spaces sliced the same way.
fn register_tables(
    engine: &Engine,
    ta_range: (i64, i64),
    tb_range: (i64, i64),
    tc_range: (i64, i64),
    td_range: (i64, i64),
) {
    engine
        .register_batches("ta", vec![ta(ta_range.0, ta_range.1)])
        .unwrap();
    engine
        .register_batches("tb", vec![keyed(tb_range.0, tb_range.1, "k")])
        .unwrap();
    engine
        .register_batches(
            "tc",
            vec![{
                let k: Vec<i64> = (tc_range.0..tc_range.1).map(|i| i % TA_ROWS).collect();
                batch(vec![("k", k)])
            }],
        )
        .unwrap();
    engine
        .register_batches(
            "td",
            vec![{
                let k: Vec<i64> = (td_range.0..td_range.1).map(|i| i % TD_ROWS).collect();
                batch(vec![("k", k)])
            }],
        )
        .unwrap();
}

/// Start one in-process worker holding its half of every table.
async fn start_worker(half: i64) -> String {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let engine = Arc::new(Engine::new());
    register_tables(
        &engine,
        (half * TA_ROWS / 2, (half + 1) * TA_ROWS / 2),
        (half * TB_ROWS / 2, (half + 1) * TB_ROWS / 2),
        (half * TC_ROWS / 2, (half + 1) * TC_ROWS / 2),
        (half * TD_ROWS / 2, (half + 1) * TD_ROWS / 2),
    );
    let worker = engine.clone();
    tokio::spawn(async move {
        let _ = serve_worker(port, worker).await;
    });
    format!("http://127.0.0.1:{port}")
}

/// (g, c) rows sorted by g for order-insensitive comparison.
fn rows(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let g = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let c = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((g.value(i), c.value(i)));
        }
    }
    out.sort();
    out
}

/// Single-node ground truth over the whole dataset.
async fn single_node(sql: &str) -> Vec<(i64, i64)> {
    let engine = Engine::new();
    register_tables(
        &engine,
        (0, TA_ROWS),
        (0, TB_ROWS),
        (0, TC_ROWS),
        (0, TD_ROWS),
    );
    rows(&engine.sql(sql).await.unwrap())
}

/// Plan `sql` distributed (deriving the LP from an engine holding the full dataset — the
/// planner only reads schemas), then run it over the two workers with the re-opt context
/// attached. Returns the result rows plus every event the store saw.
async fn run_chain(sql: &str) -> (Vec<(i64, i64)>, Vec<ExecutionEvent>) {
    let planner = Engine::new();
    register_tables(
        &planner,
        (0, TA_ROWS),
        (0, TB_ROWS),
        (0, TC_ROWS),
        (0, TD_ROWS),
    );
    let lp = planner.logical_plan(sql).await.unwrap();
    let dq = plan_distributed_logical(&lp, &[]).expect("chain must plan distributed");
    assert!(
        dq.finalize_sql.is_none(),
        "no ORDER BY/LIMIT — nothing to finalize"
    );

    let cluster = Cluster::new(vec![start_worker(0).await, start_worker(1).await]);
    let store = Arc::new(AppStateStore::new());
    let mut rx = store.subscribe();
    let reopt = reopt_join_order_enabled().then_some(ReoptContext {
        plan: &lp,
        replicated: &[],
    });
    let mut actual = None;
    for _ in 0..50 {
        match run_stages_obs(
            &cluster,
            &dq.stages,
            Some(store.clone()),
            Some("reopt-join-order-test".into()),
            reopt.as_ref().map(|r| ReoptContext {
                plan: r.plan,
                replicated: r.replicated,
            }),
        )
        .await
        {
            Ok(b) => {
                actual = Some(rows(&b));
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
    let mut events = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        events.push(ev);
    }
    (actual.expect("distributed query never succeeded"), events)
}

fn saw_reopt(events: &[ExecutionEvent]) -> bool {
    events
        .iter()
        .any(|e| matches!(e, ExecutionEvent::ReoptimizedJoinOrder { .. }))
}

/// Gate ON, tiny tail leaf written last: the tail must be re-sequenced by measured rows
/// (the event is the evidence) and the permuted plan must still match single-node.
#[tokio::test]
async fn reopt_reorders_tail_and_matches_single_node() {
    let _guard = ENV_LOCK.lock().await;
    std::env::set_var("OXIDANT_REOPT_JOIN_ORDER", "1");
    std::env::remove_var("OXIDANT_STAGE_INPUT_STATS");

    let expected = single_node(CHAIN_SQL).await;
    let (actual, events) = run_chain(CHAIN_SQL).await;
    std::env::remove_var("OXIDANT_REOPT_JOIN_ORDER");

    assert_eq!(actual, expected, "re-optimized plan must equal single-node");
    let event = events.iter().find_map(|e| match e {
        ExecutionEvent::ReoptimizedJoinOrder {
            operation_id,
            stage_ids,
            detail,
        } => Some((operation_id.clone(), stage_ids.clone(), detail.clone())),
        _ => None,
    });
    let (operation_id, stage_ids, detail) =
        event.expect("ReoptimizedJoinOrder must fire with the gate on");
    assert_eq!(operation_id, "reopt-join-order-test");
    assert!(
        !stage_ids.is_empty(),
        "the re-planned tail must be non-empty"
    );
    assert!(
        detail.contains("td=100") && detail.contains("tc=6000"),
        "detail must carry the measured leaf rows: {detail}"
    );
}

/// Gate OFF (default): identical correct results, byte-identical behavior — no event.
#[tokio::test]
async fn reopt_gate_off_never_fires() {
    let _guard = ENV_LOCK.lock().await;
    std::env::remove_var("OXIDANT_REOPT_JOIN_ORDER");
    std::env::remove_var("OXIDANT_STAGE_INPUT_STATS");
    assert!(!reopt_join_order_enabled());

    let expected = single_node(CHAIN_SQL).await;
    let (actual, events) = run_chain(CHAIN_SQL).await;

    assert_eq!(actual, expected);
    assert!(
        !saw_reopt(&events),
        "gate off must keep the original plan — no re-opt event"
    );
}

/// A LEFT join in the chain: re-opt bails (non-inner join), results stay correct.
#[tokio::test]
async fn reopt_bails_on_left_join_chain() {
    let _guard = ENV_LOCK.lock().await;
    std::env::set_var("OXIDANT_REOPT_JOIN_ORDER", "1");
    std::env::remove_var("OXIDANT_STAGE_INPUT_STATS");

    let expected = single_node(LEFT_CHAIN_SQL).await;
    let (actual, events) = run_chain(LEFT_CHAIN_SQL).await;
    std::env::remove_var("OXIDANT_REOPT_JOIN_ORDER");

    assert_eq!(actual, expected, "LEFT-join chain must equal single-node");
    assert!(
        !saw_reopt(&events),
        "a non-inner join must bail the re-optimization"
    );
}

/// `OXIDANT_STAGE_INPUT_STATS=0`: no barrier measurements, so the trigger finds no complete
/// leaf sample and skips the re-optimization — results still correct.
#[tokio::test]
async fn reopt_skips_cleanly_without_stage_input_stats() {
    let _guard = ENV_LOCK.lock().await;
    std::env::set_var("OXIDANT_REOPT_JOIN_ORDER", "1");
    std::env::set_var("OXIDANT_STAGE_INPUT_STATS", "0");

    let expected = single_node(CHAIN_SQL).await;
    let (actual, events) = run_chain(CHAIN_SQL).await;
    std::env::remove_var("OXIDANT_REOPT_JOIN_ORDER");
    std::env::remove_var("OXIDANT_STAGE_INPUT_STATS");

    assert_eq!(actual, expected);
    assert!(
        !saw_reopt(&events),
        "no leaf measurements ⇒ clean skip, no event"
    );
}
