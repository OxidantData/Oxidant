//! End-to-end coverage for keyed branch outputs in the branch-DAG splitter (TPC-DS
//! Q4/Q39/Q78): when the outer skeleton over the materialized branch outputs is an equijoin
//! tree keyed on branch-output columns, the branch outputs hash-shuffle by those keys instead
//! of gathering every row to partition 0, and the outer join runs key-partitioned on every
//! worker (the driver finalize merges the per-partition TopK). Each test plans the real query
//! against miniature tables, asserts the plan took the keyed path, and then proves
//! `distributed == single-node` over two in-process workers with the sharded fact(s)
//! row-split across them — the same harness as
//! `auto_broadcast_row_multiple.rs::q37_two_sharded_distributed_matches_single_node`.

use std::sync::Arc;

use oxidant_execution::driver::ExchangeMode;
use oxidant_execution::plan::plan_distributed_logical;
use oxidant_loom::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::Engine;

const Q4: &str = include_str!("../../../bench/tpcds/queries/q4.sql");
const Q39: &str = include_str!("../../../bench/tpcds/queries/q39.sql");
const Q78: &str = include_str!("../../../bench/tpcds/queries/q78.sql");

fn i64f(name: &str) -> Field {
    Field::new(name, DataType::Int64, false)
}

fn strf(name: &str) -> Field {
    Field::new(name, DataType::Utf8, false)
}

fn i64v(vals: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(vals.to_vec()))
}

fn batch(fields: Vec<Field>, cols: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}

// ---------------------------------------------------------------------------
// Miniature TPC-DS tables. Values are chosen so every query's result is
// non-empty and the risky paths are exercised: join keys span both shards,
// some preserved-side rows find no match (LEFT null-extension), some
// non-preserved rows never match, and Q4's ratio-CASE join residuals pass for
// one customer and fail for another.
// ---------------------------------------------------------------------------

fn customer() -> RecordBatch {
    batch(
        vec![
            i64f("c_customer_sk"),
            strf("c_customer_id"),
            strf("c_first_name"),
            strf("c_last_name"),
            strf("c_preferred_cust_flag"),
            strf("c_birth_country"),
            strf("c_login"),
            strf("c_email_address"),
        ],
        vec![
            i64v(&[1, 2, 3, 5, 7]),
            Arc::new(StringArray::from(vec![
                "cust1", "cust2", "cust3", "cust5", "cust7",
            ])),
            Arc::new(StringArray::from(vec!["f1", "f2", "f3", "f5", "f7"])),
            Arc::new(StringArray::from(vec!["l1", "l2", "l3", "l5", "l7"])),
            Arc::new(StringArray::from(vec!["Y", "N", "Y", "N", "Y"])),
            Arc::new(StringArray::from(vec!["US", "CA", "US", "CA", "US"])),
            Arc::new(StringArray::from(vec!["lg1", "lg2", "lg3", "lg5", "lg7"])),
            Arc::new(StringArray::from(vec![
                "e1@x", "e2@x", "e3@x", "e5@x", "e7@x",
            ])),
        ],
    )
}

fn date_dim() -> RecordBatch {
    // d_date_sk 10 is year 2000 (Q78's ss_sold_year filter) — but Q4 needs 2001/2001+1 and
    // Q39 needs d_year = 2001 with d_moy 1/2. Carry four dates so all three queries work.
    batch(
        vec![i64f("d_date_sk"), i64f("d_year"), i64f("d_moy")],
        vec![
            i64v(&[10, 11, 12, 13]),
            i64v(&[2000, 2001, 2002, 2001]),
            i64v(&[1, 1, 1, 2]),
        ],
    )
}

/// store_sales with the Q4 ext-price columns and the Q78 ticket/quantity columns. Sharded
/// (row-split) for Q4 and Q78.
fn store_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ss_customer_sk"),
            i64f("ss_sold_date_sk"),
            i64f("ss_item_sk"),
            i64f("ss_ticket_number"),
            i64f("ss_quantity"),
            i64f("ss_wholesale_cost"),
            i64f("ss_sales_price"),
            i64f("ss_ext_list_price"),
            i64f("ss_ext_wholesale_cost"),
            i64f("ss_ext_discount_amt"),
            i64f("ss_ext_sales_price"),
        ],
        vec![
            // Q4 customers 1/2 at 2001+2002, customer 3 only 2001; Q78 rows at d_date_sk 10
            // (year 2000) for customers 1 (matched by ws+cs), 5 (no ws/cs match) and 9
            // (returned — exercises the anti-join).
            i64v(&[1, 1, 2, 2, 3, 1, 5, 9]),
            i64v(&[11, 12, 11, 12, 11, 10, 10, 10]),
            i64v(&[1, 1, 2, 2, 3, 1, 5, 9]),
            i64v(&[100, 100, 101, 101, 102, 103, 104, 105]),
            i64v(&[1, 1, 1, 1, 1, 10, 7, 3]),
            i64v(&[1, 1, 1, 1, 1, 5, 1, 1]),
            i64v(&[1, 1, 1, 1, 1, 90, 50, 30]),
            // ext: year_total = ((list - wholesale - discount) + sales) / 2.
            i64v(&[20, 20, 20, 40, 20, 20, 20, 20]),
            i64v(&[5, 5, 5, 5, 5, 5, 5, 5]),
            i64v(&[5, 5, 5, 5, 5, 5, 5, 5]),
            i64v(&[10, 10, 10, 20, 10, 10, 10, 10]),
        ],
    )
}

/// catalog_sales: replicated for Q78 (ReplicatedAggregate Forward arm), sharded for Q4.
fn catalog_sales() -> RecordBatch {
    batch(
        vec![
            i64f("cs_bill_customer_sk"),
            i64f("cs_sold_date_sk"),
            i64f("cs_item_sk"),
            i64f("cs_order_number"),
            i64f("cs_quantity"),
            i64f("cs_wholesale_cost"),
            i64f("cs_sales_price"),
            i64f("cs_ext_list_price"),
            i64f("cs_ext_wholesale_cost"),
            i64f("cs_ext_discount_amt"),
            i64f("cs_ext_sales_price"),
        ],
        vec![
            // Q4: customer 1 ratio 4 (passes), customer 2 ratio 1.5 (fails). Q78: customer 1
            // matches the ss row on (year 2000, item 1).
            i64v(&[1, 1, 2, 2, 1]),
            i64v(&[11, 12, 11, 12, 10]),
            i64v(&[1, 1, 2, 2, 1]),
            i64v(&[200, 200, 201, 201, 300]),
            i64v(&[1, 1, 1, 1, 6]),
            i64v(&[1, 1, 1, 1, 3]),
            i64v(&[1, 1, 1, 1, 40]),
            i64v(&[20, 80, 20, 30, 20]),
            i64v(&[5, 5, 5, 5, 5]),
            i64v(&[5, 5, 5, 5, 5]),
            i64v(&[10, 40, 10, 15, 10]),
        ],
    )
}

/// web_sales: replicated for Q78 (Forward arm), sharded for Q4.
fn web_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ws_bill_customer_sk"),
            i64f("ws_sold_date_sk"),
            i64f("ws_item_sk"),
            i64f("ws_order_number"),
            i64f("ws_quantity"),
            i64f("ws_wholesale_cost"),
            i64f("ws_sales_price"),
            i64f("ws_ext_list_price"),
            i64f("ws_ext_wholesale_cost"),
            i64f("ws_ext_discount_amt"),
            i64f("ws_ext_sales_price"),
        ],
        vec![
            // Q4: customer 1 ratio 2 (loses to catalog's 4), customer 2 ratio 1.2. Q78:
            // customer 1 matches ss; customer 7 has no ss row (non-preserved side drop).
            i64v(&[1, 1, 2, 2, 1, 7]),
            i64v(&[11, 12, 11, 12, 10, 10]),
            i64v(&[1, 1, 2, 2, 1, 7]),
            i64v(&[300, 300, 301, 301, 200, 201]),
            i64v(&[1, 1, 1, 1, 4, 3]),
            i64v(&[1, 1, 1, 1, 2, 1]),
            i64v(&[1, 1, 1, 1, 30, 20]),
            i64v(&[20, 40, 20, 24, 20, 20]),
            i64v(&[5, 5, 5, 5, 5, 5]),
            i64v(&[5, 5, 5, 5, 5, 5]),
            i64v(&[10, 20, 10, 12, 10, 10]),
        ],
    )
}

fn store_returns() -> RecordBatch {
    // Ticket 105/item 9 returns customer 9's Q78 row (excluded from ss); ticket 999 matches
    // nothing.
    batch(
        vec![i64f("sr_ticket_number"), i64f("sr_item_sk")],
        vec![i64v(&[105, 999]), i64v(&[9, 1])],
    )
}

fn web_returns() -> RecordBatch {
    batch(
        vec![i64f("wr_order_number"), i64f("wr_item_sk")],
        vec![i64v(&[999]), i64v(&[1])],
    )
}

fn catalog_returns() -> RecordBatch {
    batch(
        vec![i64f("cr_order_number"), i64f("cr_item_sk")],
        vec![i64v(&[999]), i64v(&[1])],
    )
}

fn item() -> RecordBatch {
    batch(vec![i64f("i_item_sk")], vec![i64v(&[1, 2])])
}

fn warehouse() -> RecordBatch {
    batch(
        vec![i64f("w_warehouse_sk"), strf("w_warehouse_name")],
        vec![i64v(&[1, 2]), Arc::new(StringArray::from(vec!["w1", "w2"]))],
    )
}

/// inventory rows for Q39 (sharded): (item 1, warehouse 1) has high-variance quantities at
/// both d_moy 1 (d_date_sk 11) and d_moy 2 (d_date_sk 13) — both in d_year 2001 — so the
/// cov > 1 HAVING keeps both months and the self-join produces a row; (item 2, warehouse 2)
/// is low-variance (filtered).
fn inventory() -> RecordBatch {
    batch(
        vec![
            i64f("inv_item_sk"),
            i64f("inv_warehouse_sk"),
            i64f("inv_date_sk"),
            i64f("inv_quantity_on_hand"),
        ],
        vec![
            i64v(&[1, 1, 1, 1, 1, 1, 2, 2, 2]),
            i64v(&[1, 1, 1, 1, 1, 1, 2, 2, 2]),
            i64v(&[11, 11, 11, 11, 13, 13, 11, 11, 11]),
            i64v(&[1, 100, 2, 90, 5, 95, 10, 11, 10]),
        ],
    )
}

/// Planner/ground-truth engine holding the full dataset in memory.
async fn tpcds_engine() -> Engine {
    let e = Engine::new();
    e.register_batches("customer", vec![customer()]).unwrap();
    e.register_batches("date_dim", vec![date_dim()]).unwrap();
    e.register_batches("store_sales", vec![store_sales()])
        .unwrap();
    e.register_batches("catalog_sales", vec![catalog_sales()])
        .unwrap();
    e.register_batches("web_sales", vec![web_sales()]).unwrap();
    e.register_batches("store_returns", vec![store_returns()])
        .unwrap();
    e.register_batches("web_returns", vec![web_returns()])
        .unwrap();
    e.register_batches("catalog_returns", vec![catalog_returns()])
        .unwrap();
    e.register_batches("item", vec![item()]).unwrap();
    e.register_batches("warehouse", vec![warehouse()]).unwrap();
    e.register_batches("inventory", vec![inventory()]).unwrap();
    e
}

// ---------------------------------------------------------------------------
// Two-worker harness (same shape as auto_broadcast_row_multiple.rs).
// ---------------------------------------------------------------------------

static PORT: std::sync::OnceLock<std::sync::atomic::AtomicU16> = std::sync::OnceLock::new();

fn unique_worker_port() -> u16 {
    // Base BELOW the Linux ephemeral source range (32768..=60999), offset from the other
    // distributed test files' bases to avoid cross-binary collisions on shared CI runners.
    PORT.get_or_init(|| {
        std::sync::atomic::AtomicU16::new(25000 + (std::process::id() as u16 % 512))
    })
    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Contiguous half of a table, so cross-shard keys genuinely need both workers.
fn shard_rows(full: &RecordBatch, idx: usize) -> Vec<RecordBatch> {
    let half = full.num_rows() / 2;
    let (start, len) = if idx == 0 {
        (0, half)
    } else {
        (half, full.num_rows() - half)
    };
    vec![full.slice(start, len)]
}

/// `store_sales`/`catalog_sales`/`web_sales` sharded row-wise across two in-process workers;
/// the dims held in full on each (Q4's classification).
async fn two_workers_q4() -> oxidant_execution::driver::Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        e.register_batches("store_sales", shard_rows(&store_sales(), i))
            .unwrap();
        e.register_batches("catalog_sales", shard_rows(&catalog_sales(), i))
            .unwrap();
        e.register_batches("web_sales", shard_rows(&web_sales(), i))
            .unwrap();
        e.register_batches("customer", vec![customer()]).unwrap();
        e.register_batches("date_dim", vec![date_dim()]).unwrap();
        tokio::spawn(async move {
            let _ = oxidant_execution::flight::serve_worker(port, e).await;
        });
    }
    oxidant_execution::driver::Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ])
}

/// Only `inventory` sharded; dims replicated (Q39's classification).
async fn two_workers_q39() -> oxidant_execution::driver::Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        e.register_batches("inventory", shard_rows(&inventory(), i))
            .unwrap();
        e.register_batches("item", vec![item()]).unwrap();
        e.register_batches("warehouse", vec![warehouse()]).unwrap();
        e.register_batches("date_dim", vec![date_dim()]).unwrap();
        tokio::spawn(async move {
            let _ = oxidant_execution::flight::serve_worker(port, e).await;
        });
    }
    oxidant_execution::driver::Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ])
}

/// Only `store_sales` sharded; the other channel facts, the returns tables, and `date_dim`
/// replicated (Q78's classification — `ws`/`cs` become `Forward` aggregate arms).
async fn two_workers_q78() -> oxidant_execution::driver::Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        e.register_batches("store_sales", shard_rows(&store_sales(), i))
            .unwrap();
        e.register_batches("store_returns", vec![store_returns()])
            .unwrap();
        e.register_batches("catalog_sales", vec![catalog_sales()])
            .unwrap();
        e.register_batches("catalog_returns", vec![catalog_returns()])
            .unwrap();
        e.register_batches("web_sales", vec![web_sales()]).unwrap();
        e.register_batches("web_returns", vec![web_returns()])
            .unwrap();
        e.register_batches("date_dim", vec![date_dim()]).unwrap();
        tokio::spawn(async move {
            let _ = oxidant_execution::flight::serve_worker(port, e).await;
        });
    }
    oxidant_execution::driver::Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ])
}

/// Sorted value rows (headers are not compared: single-node and distributed plans name
/// unaliased outputs differently — pre-existing behavior of every distributed shape).
fn rows_sorted(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let opts = oxidant_loom::arrow::util::display::FormatOptions::default().with_null("NULL");
    let mut rows = Vec::new();
    for b in batches {
        let fmts: Vec<_> = b
            .columns()
            .iter()
            .map(|c| oxidant_loom::arrow::util::display::ArrayFormatter::try_new(c, &opts).unwrap())
            .collect();
        for r in 0..b.num_rows() {
            rows.push(
                fmts.iter()
                    .map(|f| f.value(r).to_string())
                    .collect::<Vec<_>>(),
            );
        }
    }
    rows.sort();
    rows
}

/// Run `sql` single-node and distributed (two workers) and assert strict equality. The plan
/// must have taken the keyed path: `expect_finalize` whether a driver-side finalize is
/// expected (Q4/Q78's two-phase TopK, Q39's ORDER BY merge).
async fn assert_distributed_matches_single_node(
    sql: &str,
    replicated: &[&str],
    cluster: oxidant_execution::driver::Cluster,
    expect_finalize: bool,
) {
    let planner = tpcds_engine().await;
    let expected = planner.sql(sql).await.expect("single-node");
    assert!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
        "single-node result must be non-empty (otherwise the comparison is vacuous)"
    );
    let lp = planner.logical_plan(sql).await.unwrap();
    let dq = plan_distributed_logical(&lp, replicated).expect("must plan distributed");
    let outer = dq.stages.last().expect("stages");
    assert!(
        outer.upstream_stage_ids.iter().all(|&id| !dq
            .stages
            .iter()
            .find(|s| s.stage_id == id)
            .expect("upstream stage")
            .hash_key_cols
            .is_empty()),
        "every branch output must hash-shuffle by the skeleton key (no p0 gather): {dq:?}"
    );
    assert_eq!(
        dq.finalize_sql.is_some(),
        expect_finalize,
        "finalize presence: {dq:?}"
    );
    let mut out = None;
    for _ in 0..150 {
        match oxidant_execution::driver::run_stages(&cluster, &dq.stages).await {
            Ok(b) => {
                out = Some(b);
                break;
            }
            Err(e) => {
                eprintln!("run_stages err: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await
            }
        }
    }
    let gathered = out.expect("distributed run never succeeded");
    let actual = match &dq.finalize_sql {
        None => gathered,
        Some(fsql) => {
            let fin = Engine::new();
            fin.register_batches("result", gathered).unwrap();
            fin.sql(fsql).await.expect("finalize")
        }
    };
    assert_eq!(
        rows_sorted(&actual),
        rows_sorted(&expected),
        "distributed must equal single-node"
    );
}

/// Run an e2e body on a runtime with large worker stacks: unoptimized builds plan the
/// deeply-nested stage SQL (Q39's HAVING-wrapped stddev combine, the outer multi-join
/// skeleton) with frames far bigger than tokio's 2 MiB default allows — the same guard
/// `auto_distribute_replicated_slice.rs`'s Q71 e2e documents.
fn run_e2e(fut: impl std::future::Future<Output = ()>) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_stack_size(32 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("e2e runtime");
    rt.block_on(fut);
}

/// TPC-DS Q4: one `year_total` union CTE (three per-channel aggregate arms over the sharded
/// sales facts) deduplicated into one sub-DAG, self-joined six times on `customer_id` with
/// ratio-CASE residuals and `ORDER BY … LIMIT 100`. The branch output hash-shuffles by
/// `customer_id`; the six-way join runs key-partitioned; the driver merges per-partition
/// TopK.
#[test]
fn q4_keyed_outer_distributed_matches_single_node() {
    run_e2e(async {
        let replicated = ["customer", "date_dim"];
        let cluster = two_workers_q4().await;
        assert_distributed_matches_single_node(Q4, &replicated, cluster, true).await;
    });
}

/// TPC-DS Q39: the `inv` aggregate branch self-joins on the two-column key
/// `(i_item_sk, w_warehouse_sk)` with `d_moy` residuals and an `ORDER BY` (no LIMIT — the
/// finalize re-sorts the concatenation).
#[test]
fn q39_keyed_outer_distributed_matches_single_node() {
    run_e2e(async {
        let replicated = ["item", "warehouse", "date_dim"];
        let cluster = two_workers_q39().await;
        assert_distributed_matches_single_node(Q39, &replicated, cluster, true).await;
    });
}

/// TPC-DS Q78: `ss LEFT JOIN ws LEFT JOIN cs` on (sold_year, item, customer). The sharded
/// `ss` branch and the two `Forward` (`ws`/`cs`) replicated-aggregate arms all hash-partition
/// by the same key — LEFT null-extension is key-local.
#[test]
fn q78_keyed_outer_distributed_matches_single_node() {
    run_e2e(async {
        let replicated = [
            "date_dim",
            "store_returns",
            "web_sales",
            "web_returns",
            "catalog_sales",
            "catalog_returns",
        ];
        let cluster = two_workers_q78().await;
        assert_distributed_matches_single_node(Q78, &replicated, cluster, true).await;
    });
}

/// The Q78 plan shape: two of the three branch outputs are `Forward` stages, and every
/// upstream of the outer stage carries the same three-column key.
#[tokio::test]
async fn q78_plan_has_two_keyed_forward_arms() {
    let planner = tpcds_engine().await;
    let lp = planner.logical_plan(Q78).await.unwrap();
    let replicated = [
        "date_dim",
        "store_returns",
        "web_sales",
        "web_returns",
        "catalog_sales",
        "catalog_returns",
    ];
    let dq = plan_distributed_logical(&lp, &replicated).expect("Q78 plans");
    let outer = dq.stages.last().unwrap();
    let forwards = outer
        .upstream_stage_ids
        .iter()
        .filter(|&id| {
            dq.stages
                .iter()
                .find(|s| s.stage_id == *id)
                .is_some_and(|s| s.exchange == ExchangeMode::Forward)
        })
        .count();
    assert_eq!(forwards, 2, "ws/cs Forward arms: {dq:?}");
}
