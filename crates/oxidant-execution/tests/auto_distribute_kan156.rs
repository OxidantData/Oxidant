//! KAN-156: fan the replicated-only `Forward` arms of the specialized TPC-DS shapes out across
//! workers instead of computing them in one task (always on worker 0). At SF100 only
//! `store_sales` is sharded; `catalog_sales` (14.5 GB) and `web_sales` (7.2 GB) classify as
//! replicated, and the Q14/Q23/Q78 shapes emitted single-task `Forward` stages scanning them in
//! full (Q14: ~1200 task-seconds across stages 1/6/11; Q23: 279 s; Q78: 292 s).
//!
//! On a multi-worker cluster (`OXIDANT_WORKER_COUNT` > 1 on the driver) the planner now:
//!
//! - **Q14** (`try_rollup_union_derived_subqueries`): slices the replicated INTERSECT legs, the
//!   replicated `avg_sales` partial, and the replicated channel-arm scan exports — each worker
//!   scans a disjoint file slice of the arm's anchor table. The legs dedup after full-row hash
//!   co-location, the AVG leg combines sum/count, and the arm exports feed partial aggregates,
//!   so the per-slice outputs merge associatively downstream.
//! - **Q23** (`try_union_over_derived_ctes`): replaces each Forward arm with export → per-CTE
//!   semi → partial → recombine; the CTE terminal stages are re-keyed from the partition-0
//!   gather to the arms' join keys so equal keys co-locate.
//! - **Q78** (`dag_splitter` branch DAG): each replicated-only aggregate branch becomes a
//!   sliced per-worker partial + the ordinary recombine (keyed for the outer skeleton) instead
//!   of one Forward stage.
//!
//! Single-worker plans stay byte-identical (`Forward`). The end-to-end tests run every query
//! over two in-process workers against multi-file parquet listings — the replicated channel
//! facts sliced by the workers' explicit shard assignments — and require row-for-row equality
//! with single-node.

// ENV_LOCK serializes process-global `OXIDANT_WORKER_COUNT` / `OXIDANT_DISTRIBUTED_STRICT`
// across async tests.
#![allow(clippy::await_holding_lock)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use oxidant_execution::driver::{run_stages, Cluster, ExchangeMode};
use oxidant_execution::flight::serve_worker_with_assignment;
use oxidant_execution::plan::{plan_distributed_logical, DistributedQuery};
use oxidant_loom::arrow::array::{ArrayRef, Date32Array, Float64Array, Int64Array, StringArray};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::shard::ShardAssignment;
use oxidant_loom::Engine;

const Q14: &str = include_str!("../../../bench/tpcds/queries/q14.sql");
const Q23: &str = include_str!("../../../bench/tpcds/queries/q23.sql");
const Q78: &str = include_str!("../../../bench/tpcds/queries/q78.sql");

/// Q14's SF100-style classification: `store_sales` sharded, the other channel facts replicated.
const REPL_Q14: [&str; 8] = [
    "date_dim",
    "item",
    "customer",
    "catalog_sales",
    "web_sales",
    "store_returns",
    "catalog_returns",
    "web_returns",
];
const REPL_Q23: [&str; 5] = ["catalog_sales", "web_sales", "customer", "date_dim", "item"];
const REPL_Q78: [&str; 6] = [
    "date_dim",
    "web_sales",
    "web_returns",
    "catalog_sales",
    "catalog_returns",
    "store_returns",
];

static ENV_LOCK: Mutex<()> = Mutex::new(());

fn set_w2_env() {
    std::env::set_var("OXIDANT_WORKER_COUNT", "2");
    std::env::remove_var("OXIDANT_SHARD_INDEX");
    std::env::remove_var("OXIDANT_POD_NAME");
}

fn clear_env() {
    std::env::remove_var("OXIDANT_WORKER_COUNT");
    std::env::remove_var("OXIDANT_SHARD_INDEX");
    std::env::remove_var("OXIDANT_POD_NAME");
    std::env::remove_var("OXIDANT_REPLICATED_TABLES");
}

// ---------------------------------------------------------------------------
// RecordBatch helpers
// ---------------------------------------------------------------------------

fn i64f(name: &str) -> Field {
    Field::new(name, DataType::Int64, false)
}
fn f64f(name: &str) -> Field {
    Field::new(name, DataType::Float64, false)
}
fn strf(name: &str) -> Field {
    Field::new(name, DataType::Utf8, false)
}
fn datef(name: &str) -> Field {
    Field::new(name, DataType::Date32, false)
}

fn i64v(vals: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(vals.to_vec()))
}
fn f64v(vals: &[f64]) -> ArrayRef {
    Arc::new(Float64Array::from(vals.to_vec()))
}
fn strv(vals: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(vals.to_vec()))
}
fn datev(vals: &[i32]) -> ArrayRef {
    Arc::new(Date32Array::from(vals.to_vec()))
}

fn batch(fields: Vec<Field>, cols: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}

fn item() -> RecordBatch {
    batch(
        vec![
            i64f("i_item_sk"),
            strf("i_item_desc"),
            i64f("i_brand_id"),
            i64f("i_class_id"),
            i64f("i_category_id"),
        ],
        vec![
            i64v(&[1, 2, 3, 4]),
            strv(&["desc-one", "desc-two", "desc-three", "desc-four"]),
            // Items 1 and 3 share Q14's (brand, class, category) triple; item 2's triple must
            // fall out of the INTERSECT (no web_sale); item 4 never sells.
            i64v(&[10, 20, 10, 99]),
            i64v(&[100, 200, 100, 999]),
            i64v(&[1000, 2000, 1000, 9999]),
        ],
    )
}

/// d_date_sk 1..=6: (2000, moy 1/2/3), (2001, moy 1), (2001, moy 11 — Q14's arm window),
/// (2000, moy 11 — inside the 1999–2001 INTERSECT/AVG window but outside the arm window).
fn date_dim() -> RecordBatch {
    batch(
        vec![
            i64f("d_date_sk"),
            datef("d_date"),
            i64f("d_year"),
            i64f("d_moy"),
        ],
        vec![
            i64v(&[1, 2, 3, 4, 5, 6]),
            datev(&[10957, 10988, 11017, 11323, 11629, 11294]),
            i64v(&[2000, 2000, 2000, 2001, 2001, 2000]),
            i64v(&[1, 2, 3, 1, 11, 11]),
        ],
    )
}

fn customer() -> RecordBatch {
    batch(
        vec![
            i64f("c_customer_sk"),
            strf("c_last_name"),
            strf("c_first_name"),
        ],
        vec![
            i64v(&[1, 2, 3]),
            strv(&["Smith", "Jones", "Zed"]),
            strv(&["Ann", "Bob", "Zoey"]),
        ],
    )
}

// ---------------------------------------------------------------------------
// Parquet fixture catalog (multi-file listings the workers slice per stage)
// ---------------------------------------------------------------------------

/// Stub catalog over per-table parquet directories — the same resolution path
/// (`catalog_bridge` → file-list shard) live workers use against Glue/Hive.
struct SliceCatalog {
    tables: HashMap<String, String>,
}

#[async_trait::async_trait]
impl oxidant_catalog::CatalogProvider for SliceCatalog {
    fn name(&self) -> &str {
        "testcat"
    }

    async fn list_namespaces(
        &self,
        parent: &[String],
    ) -> oxidant_catalog::Result<Vec<Vec<String>>> {
        if parent.is_empty() {
            Ok(vec![vec!["default".to_string()]])
        } else {
            Ok(vec![])
        }
    }

    async fn list_tables(&self, _namespace: &[String]) -> oxidant_catalog::Result<Vec<String>> {
        Ok(self.tables.keys().cloned().collect())
    }

    async fn load_table(
        &self,
        _namespace: &[String],
        table: &str,
    ) -> oxidant_catalog::Result<oxidant_catalog::TableMetadata> {
        let location = self
            .tables
            .get(table)
            .ok_or_else(|| oxidant_catalog::Error::Plan(format!("no such table `{table}`")))?;
        Ok(oxidant_catalog::TableMetadata::new(
            table,
            location,
            oxidant_catalog::TableFormat::Parquet,
        ))
    }
}

/// Write every table's files into `root/<table>/part-<i>.parquet` and return the catalog.
fn write_fixtures(root: &std::path::Path, tables: &[(&str, Vec<RecordBatch>)]) -> SliceCatalog {
    let mut map = HashMap::new();
    for (name, batches) in tables {
        let dir = root.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        for (i, batch) in batches.iter().enumerate() {
            let file = std::fs::File::create(dir.join(format!("part-{i}.parquet"))).unwrap();
            let mut writer =
                datafusion::parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None)
                    .unwrap();
            writer.write(batch).unwrap();
            writer.close().unwrap();
        }
        map.insert(name.to_string(), dir.to_str().unwrap().to_string());
    }
    SliceCatalog { tables: map }
}

async fn catalog_engine(catalog: SliceCatalog) -> Engine {
    let engine = Engine::new();
    engine.register_catalog("testcat", Arc::new(catalog));
    engine
}

/// The Q14/Q23/Q78 queries read unqualified table names; rewrite bare table identifiers to the
/// fixture catalog's fully-qualified names (identifier-aware: `max_store_sales` and
/// `best_ss_customer` must NOT be rewritten, and an already-qualified name is left alone).
fn qualify(sql: &str) -> String {
    const TABLES: [&str; 9] = [
        "store_sales",
        "catalog_sales",
        "web_sales",
        "store_returns",
        "catalog_returns",
        "web_returns",
        "date_dim",
        "customer",
        "item",
    ];
    assert!(sql.is_ascii(), "fixture rewrite expects ASCII SQL");
    let bytes = sql.as_bytes();
    let ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut out = String::with_capacity(sql.len() + 256);
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' {
            let start = i;
            while i < bytes.len() && ident(bytes[i]) {
                i += 1;
            }
            let word = &sql[start..i];
            let bare = start == 0 || (!ident(bytes[start - 1]) && bytes[start - 1] != b'.');
            if bare && TABLES.contains(&word) {
                out.push_str("testcat.default.");
            }
            out.push_str(word);
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

/// Both workers resolve the same parquet directories through the catalog; each shards the
/// listings by its own explicit assignment (the in-process stand-in for per-worker env).
async fn two_workers(root: &std::path::Path, tables: &[(&str, Vec<RecordBatch>)]) -> Cluster {
    static PORT: std::sync::OnceLock<std::sync::atomic::AtomicU16> = std::sync::OnceLock::new();
    let next_port = || {
        PORT.get_or_init(|| {
            std::sync::atomic::AtomicU16::new(27000 + (std::process::id() as u16 % 512))
        })
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    };
    let (p0, p1) = (next_port(), next_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(catalog_engine(write_fixtures(root, tables)).await);
        tokio::spawn(async move {
            let _ =
                serve_worker_with_assignment(port, e, ShardAssignment { index: i, count: 2 }).await;
        });
    }
    Cluster::new(vec![
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

/// Plan `sql` at W=2 under `OXIDANT_DISTRIBUTED_STRICT=1` (the whole-fact gather must never
/// substitute), run it on the two-worker cluster, and require row-for-row equality with
/// single-node. The plan assertion callback pins the fanned-out stage shape.
async fn assert_fanned_matches_single_node(
    tag: &str,
    sql: &str,
    replicated: &[&str],
    tables: &[(&str, Vec<RecordBatch>)],
    check_plan: impl Fn(&DistributedQuery),
) {
    let _guard = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let planner = catalog_engine(write_fixtures(dir.path(), tables)).await;
    let sql = qualify(sql);
    let expected = planner.sql(&sql).await.expect("single-node");
    assert!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
        "{tag}: single-node result must be non-empty (otherwise the comparison is vacuous)"
    );

    set_w2_env();
    std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
    let lp = planner.logical_plan(&sql).await.unwrap();
    let dq = plan_distributed_logical(&lp, replicated)
        .unwrap_or_else(|e| panic!("{tag} must plan distributed at W=2: {e}"));
    clear_env();
    std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
    check_plan(&dq);

    let cluster = two_workers(dir.path(), tables).await;
    let mut out = None;
    for _ in 0..150 {
        match run_stages(&cluster, &dq.stages).await {
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
        rows_sorted(&expected),
        rows_sorted(&actual),
        "{tag}: fanned-out distributed result must equal single-node"
    );
}

fn stage_by_id(dq: &DistributedQuery, id: u32) -> &oxidant_execution::driver::StageDef {
    dq.stages
        .iter()
        .find(|s| s.stage_id == id)
        .expect("stage id present")
}

// ---------------------------------------------------------------------------
// Q14 — replicated INTERSECT legs, AVG partial, and channel-arm exports fan out
// ---------------------------------------------------------------------------

/// Q14 fixture: every channel fact spans two files; the cross-triple (items 1/3) sells in all
/// three channels inside 1999–2001, item 2 sells only in store+catalog (falls out of the
/// INTERSECT), and the arm window (2001-11) holds sales for items 1 and 3 in every channel.
fn q14_tables() -> Vec<(&'static str, Vec<RecordBatch>)> {
    /// One channel-fact file: (sold_date_sk, item_sk, quantity, list_price) rows.
    fn fact_file(
        prefix: &str,
        dates: &[i64],
        items: &[i64],
        qtys: &[i64],
        lps: &[f64],
    ) -> RecordBatch {
        batch(
            vec![
                i64f(&format!("{prefix}_sold_date_sk")),
                i64f(&format!("{prefix}_item_sk")),
                i64f(&format!("{prefix}_quantity")),
                f64f(&format!("{prefix}_list_price")),
            ],
            vec![i64v(dates), i64v(items), i64v(qtys), f64v(lps)],
        )
    }
    vec![
        ("item", vec![item()]),
        ("date_dim", vec![date_dim()]),
        ("customer", vec![customer()]),
        (
            "store_sales",
            vec![
                fact_file(
                    "ss",
                    &[5, 1, 5],
                    &[1, 1, 2],
                    &[2, 1, 3],
                    &[10.0, 10.0, 20.0],
                ),
                fact_file(
                    "ss",
                    &[5, 2, 6],
                    &[3, 2, 1],
                    &[4, 1, 2],
                    &[30.0, 20.0, 10.0],
                ),
            ],
        ),
        (
            "catalog_sales",
            vec![
                fact_file("cs", &[5, 3], &[1, 3], &[5, 1], &[40.0, 30.0]),
                fact_file("cs", &[5, 4], &[3, 2], &[1, 2], &[30.0, 20.0]),
            ],
        ),
        (
            "web_sales",
            vec![
                fact_file("ws", &[5, 1], &[1, 3], &[1, 2], &[50.0, 30.0]),
                fact_file("ws", &[5, 2], &[3, 1], &[2, 1], &[30.0, 10.0]),
            ],
        ),
        (
            "store_returns",
            vec![batch(
                vec![i64f("sr_item_sk"), i64f("sr_ticket_number")],
                vec![i64v(&[9]), i64v(&[9])],
            )],
        ),
        (
            "catalog_returns",
            vec![batch(
                vec![i64f("cr_item_sk"), i64f("cr_order_number")],
                vec![i64v(&[9]), i64v(&[9])],
            )],
        ),
        (
            "web_returns",
            vec![batch(
                vec![i64f("wr_item_sk"), i64f("wr_order_number")],
                vec![i64v(&[9]), i64v(&[9])],
            )],
        ),
    ]
}

/// The Q14 stages that scan a replicated channel fact: the catalog/web INTERSECT legs, the
/// replicated avg partial, and the catalog/web arm exports.
fn q14_replicated_stages(dq: &DistributedQuery) -> Vec<&oxidant_execution::driver::StageDef> {
    dq.stages
        .iter()
        .filter(|s| {
            s.upstream_stage_ids.is_empty()
                && (s.sql.contains("catalog_sales") || s.sql.contains("web_sales"))
        })
        .collect()
}

/// W=1: every replicated-fact stage keeps the original single-task `Forward` placement with the
/// full replicate stamp.
#[tokio::test]
async fn q14_single_worker_keeps_forward_byte_identical() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_env();
    let dir = tempfile::tempdir().unwrap();
    let tables = q14_tables();
    let engine = catalog_engine(write_fixtures(dir.path(), &tables)).await;
    let lp = engine.logical_plan(&qualify(Q14)).await.unwrap();
    let dq = plan_distributed_logical(&lp, &REPL_Q14).expect("Q14 plans at W=1");
    let stages = q14_replicated_stages(&dq);
    assert_eq!(
        stages.len(),
        5,
        "2 INTERSECT legs + 1 avg partial + 2 arm exports: {dq:?}"
    );
    for s in stages {
        assert_eq!(
            s.exchange,
            ExchangeMode::Forward,
            "single-worker placement is the original one-Forward design: {s:?}"
        );
        for t in ["catalog_sales", "web_sales"] {
            assert!(
                s.replicated_tables.split(',').any(|x| x == t),
                "full replicate stamp keeps `{t}`: {s:?}"
            );
        }
    }
}

/// W=2: placement-only change — identical stage count and stage SQL, but the replicated-fact
/// stages run on every worker with the anchor table dropped from their stamp (sliced).
#[tokio::test]
async fn q14_two_workers_slices_replicated_legs() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_env();
    let dir = tempfile::tempdir().unwrap();
    let tables = q14_tables();
    let engine = catalog_engine(write_fixtures(dir.path(), &tables)).await;
    let lp = engine.logical_plan(&qualify(Q14)).await.unwrap();
    let w1 = plan_distributed_logical(&lp, &REPL_Q14).expect("W=1 plan");

    set_w2_env();
    let dq = plan_distributed_logical(&lp, &REPL_Q14).expect("W=2 plan");
    clear_env();

    assert_eq!(dq.stages.len(), w1.stages.len(), "stage count unchanged");
    for (sliced, forward) in dq.stages.iter().zip(w1.stages.iter()) {
        assert_eq!(
            sliced.sql, forward.sql,
            "stage SQL is placement-independent"
        );
        assert_eq!(sliced.hash_key_cols, forward.hash_key_cols);
        assert_eq!(sliced.upstream_stage_ids, forward.upstream_stage_ids);
    }
    for s in q14_replicated_stages(&dq) {
        assert_ne!(
            s.exchange,
            ExchangeMode::Forward,
            "multi-worker placement fans the leg out: {s:?}"
        );
        // Each stage drops exactly the replicated channel facts it scans (the disjoint file
        // slices); the shared dims stay replicated so the leg's joins stay slice-local.
        for t in ["catalog_sales", "web_sales"] {
            let scans = s.sql.contains(t);
            let stamped = s.replicated_tables.split(',').any(|x| x == t);
            assert_ne!(
                scans, stamped,
                "`{t}` must be sliced exactly where it is scanned: {s:?}"
            );
        }
        for dim in ["item", "date_dim"] {
            assert!(
                s.replicated_tables.split(',').any(|x| x == dim),
                "shared dim `{dim}` stays replicated in the sliced stage: {s:?}"
            );
        }
    }
}

#[test]
fn q14_fanned_distributed_matches_single_node() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_stack_size(32 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("e2e runtime");
    rt.block_on(async {
        assert_fanned_matches_single_node(Q14_TAG, Q14, &REPL_Q14, &q14_tables(), |dq| {
            assert!(
                q14_replicated_stages(dq)
                    .iter()
                    .all(|s| s.exchange != ExchangeMode::Forward),
                "this test exercises the sliced legs, not the Forward fallback: {dq:?}"
            );
        })
        .await;
    });
}

const Q14_TAG: &str = "Q14";

// ---------------------------------------------------------------------------
// Q23 — channel arms become export → semi(s) → partial → recombine pipelines
// ---------------------------------------------------------------------------

/// Q23 fixture (the kan49f row design, split across two files per fact): five (item 1, date 1)
/// store rows make the `frequent_ss_items` group (count 5 > 4, straddling the two files so no
/// per-file count passes the HAVING); customer 1's five 10-sales lose to customer 2's single
/// 1000 sale, so the 0.5·max threshold keeps only customer 2. One catalog row and one web row
/// join both CTEs inside the d_year=2000/d_moy=2 window; the second row of each arm table is a
/// negative control (customer 1 / item 2).
fn q23_tables() -> Vec<(&'static str, Vec<RecordBatch>)> {
    /// One channel-fact file: (sold_date_sk, item_sk, bill_customer_sk, quantity, list_price).
    fn channel_file(
        prefix: &str,
        dates: &[i64],
        items: &[i64],
        custs: &[i64],
        qtys: &[i64],
        lps: &[f64],
    ) -> RecordBatch {
        batch(
            vec![
                i64f(&format!("{prefix}_sold_date_sk")),
                i64f(&format!("{prefix}_item_sk")),
                i64f(&format!("{prefix}_bill_customer_sk")),
                i64f(&format!("{prefix}_quantity")),
                f64f(&format!("{prefix}_list_price")),
            ],
            vec![i64v(dates), i64v(items), i64v(custs), i64v(qtys), f64v(lps)],
        )
    }
    vec![
        ("item", vec![item()]),
        ("date_dim", vec![date_dim()]),
        ("customer", vec![customer()]),
        (
            "store_sales",
            vec![
                batch(
                    vec![
                        i64f("ss_sold_date_sk"),
                        i64f("ss_item_sk"),
                        i64f("ss_customer_sk"),
                        i64f("ss_quantity"),
                        f64f("ss_sales_price"),
                    ],
                    vec![
                        i64v(&[1, 1, 1, 1]),
                        i64v(&[1, 1, 2, 1]),
                        i64v(&[1, 1, 2, 1]),
                        i64v(&[1, 1, 100, 1]),
                        f64v(&[10.0, 10.0, 10.0, 10.0]),
                    ],
                ),
                batch(
                    vec![
                        i64f("ss_sold_date_sk"),
                        i64f("ss_item_sk"),
                        i64f("ss_customer_sk"),
                        i64f("ss_quantity"),
                        f64f("ss_sales_price"),
                    ],
                    vec![
                        i64v(&[1, 1, 2]),
                        i64v(&[1, 1, 3]),
                        i64v(&[1, 1, 3]),
                        i64v(&[1, 1, 5]),
                        f64v(&[10.0, 10.0, 2.0]),
                    ],
                ),
            ],
        ),
        (
            "catalog_sales",
            vec![
                // d_date_sk 2 is the d_year=2000/d_moy=2 arm window.
                channel_file("cs", &[2], &[1], &[2], &[2], &[5.0]),
                channel_file("cs", &[2], &[1], &[1], &[9], &[5.0]),
            ],
        ),
        (
            "web_sales",
            vec![
                channel_file("ws", &[2], &[1], &[2], &[3], &[7.0]),
                channel_file("ws", &[2], &[2], &[2], &[4], &[7.0]),
            ],
        ),
    ]
}

/// The arm pipeline stages: exports scan the channel facts; semis read `shuffle_input` pairs.
fn q23_export_stages(dq: &DistributedQuery) -> Vec<&oxidant_execution::driver::StageDef> {
    dq.stages
        .iter()
        .filter(|s| {
            s.upstream_stage_ids.is_empty()
                && (s.sql.contains("catalog_sales") || s.sql.contains("web_sales"))
        })
        .collect()
}

/// W=1: byte-identical to the pre-fan-out planner — two `Forward` arm stages reading the
/// gathered CTEs.
#[tokio::test]
async fn q23_single_worker_keeps_forward_arms() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_env();
    let dir = tempfile::tempdir().unwrap();
    let tables = q23_tables();
    let engine = catalog_engine(write_fixtures(dir.path(), &tables)).await;
    let lp = engine.logical_plan(&qualify(Q23)).await.unwrap();
    let dq = plan_distributed_logical(&lp, &REPL_Q23).expect("Q23 plans at W=1");
    let arms: Vec<_> = dq
        .stages
        .iter()
        .filter(|s| {
            !s.upstream_stage_ids.is_empty()
                && (s.sql.contains("catalog_sales") || s.sql.contains("web_sales"))
        })
        .collect();
    assert_eq!(arms.len(), 2, "two per-channel Forward arms: {dq:?}");
    for arm in &arms {
        assert_eq!(arm.exchange, ExchangeMode::Forward, "{arm:?}");
    }
}

/// W=2: each arm becomes export (sliced leaf) → semi vs `frequent_ss_items` →
/// semi+partial vs `best_ss_customer` → recombine, and the CTE terminal stages re-key from the
/// partition-0 gather to the arms' join columns.
#[tokio::test]
async fn q23_two_workers_fans_out_arms() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_env();
    let dir = tempfile::tempdir().unwrap();
    let tables = q23_tables();
    let engine = catalog_engine(write_fixtures(dir.path(), &tables)).await;
    let lp = engine.logical_plan(&qualify(Q23)).await.unwrap();
    let w1 = plan_distributed_logical(&lp, &REPL_Q23).expect("W=1 plan");

    set_w2_env();
    let dq = plan_distributed_logical(&lp, &REPL_Q23).expect("W=2 plan");
    clear_env();

    assert_eq!(
        dq.stages.len(),
        w1.stages.len() + 6,
        "each of the two Forward arms becomes a 4-stage pipeline: {dq:?}"
    );
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.exchange == ExchangeMode::Forward),
        "no Forward stage remains at W=2: {dq:?}"
    );

    // Two sliced export leaves, one per channel fact.
    let exports = q23_export_stages(&dq);
    assert_eq!(exports.len(), 2, "one export per arm: {dq:?}");
    for s in &exports {
        for (fact, key) in [("catalog_sales", "cs_item_sk"), ("web_sales", "ws_item_sk")] {
            if s.sql.contains(fact) {
                assert!(
                    !s.replicated_tables.split(',').any(|x| x == fact),
                    "`{fact}` must be sliced out of its export stage: {s:?}"
                );
                assert!(
                    s.sql.contains(&format!("{key} AS j0_0")),
                    "export carries the frequent_ss_items join key: {s:?}"
                );
            }
        }
        for dim in ["customer", "date_dim"] {
            assert!(
                s.replicated_tables.split(',').any(|x| x == dim),
                "dim `{dim}` stays replicated in the export: {s:?}"
            );
        }
    }

    // The CTE terminal stages re-key to the arms' join columns: frequent_ss_items by item_sk
    // (output column 1), best_ss_customer by c_customer_sk (output column 0).
    let frequent = dq
        .stages
        .iter()
        .find(|s| s.sql.contains("\"item_sk\"") && s.sql.contains("\"cnt\""))
        .expect("frequent_ss_items terminal stage");
    assert_eq!(frequent.hash_key_cols, vec![1], "{frequent:?}");
    let best = dq
        .stages
        .iter()
        .find(|s| s.sql.contains("\"ssales\""))
        .expect("best_ss_customer terminal stage");
    assert_eq!(best.hash_key_cols, vec![0], "{best:?}");

    // Each arm's semi stages read its own export and the re-keyed CTEs; the last folds in the
    // partial aggregate keyed by the group columns.
    for s in &dq.stages {
        if s.sql.contains("semi_k.item_sk") {
            assert_eq!(s.upstream_stage_ids.len(), 2, "semi vs frequent: {s:?}");
            assert_eq!(
                s.hash_key_cols,
                vec![4],
                "re-shuffle by the best key: {s:?}"
            );
        }
        if s.sql.contains("semi_k.c_customer_sk") {
            assert!(s.sql.contains("GROUP BY gc0, gc1"), "partial: {s:?}");
            assert_eq!(s.hash_key_cols, vec![0, 1], "group-keyed partial: {s:?}");
        }
    }

    // The final UNION ALL still concatenates the two arm streams (recombined per arm — equal
    // (last, first) names across channels must NOT merge).
    let union = dq.stages.last().unwrap();
    assert!(union.sql.contains("UNION ALL"), "{union:?}");
    assert_eq!(union.upstream_stage_ids.len(), 2, "{union:?}");
}

#[test]
fn q23_fanned_distributed_matches_single_node() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_stack_size(32 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("e2e runtime");
    rt.block_on(async {
        assert_fanned_matches_single_node("Q23", Q23, &REPL_Q23, &q23_tables(), |dq| {
            assert!(
                !dq.stages
                    .iter()
                    .any(|s| s.exchange == ExchangeMode::Forward),
                "this test exercises the fanned-out arms, not the Forward fallback: {dq:?}"
            );
        })
        .await;
    });
}

// ---------------------------------------------------------------------------
// Q78 — replicated aggregate branches become sliced partial + recombine
// ---------------------------------------------------------------------------

/// Q78 fixture: one (2000, item 1, customer 1) group survives in all three channels (the second
/// store/web rows anti-join out through the returns, the second catalog row is outside
/// d_year=2000); the (2000, item 2, customer 2) store-only group drops in the outer
/// `ws_qty > 0 OR cs_qty > 0` filter.
fn q78_tables() -> Vec<(&'static str, Vec<RecordBatch>)> {
    vec![
        ("date_dim", vec![date_dim()]),
        (
            "store_sales",
            vec![
                batch(
                    vec![
                        i64f("ss_sold_date_sk"),
                        i64f("ss_item_sk"),
                        i64f("ss_customer_sk"),
                        i64f("ss_ticket_number"),
                        i64f("ss_quantity"),
                        f64f("ss_wholesale_cost"),
                        f64f("ss_sales_price"),
                    ],
                    vec![
                        i64v(&[1, 1]),
                        i64v(&[1, 1]),
                        i64v(&[1, 1]),
                        i64v(&[101, 102]),
                        i64v(&[2, 3]),
                        f64v(&[6.0, 7.0]),
                        f64v(&[10.0, 11.0]),
                    ],
                ),
                batch(
                    vec![
                        i64f("ss_sold_date_sk"),
                        i64f("ss_item_sk"),
                        i64f("ss_customer_sk"),
                        i64f("ss_ticket_number"),
                        i64f("ss_quantity"),
                        f64f("ss_wholesale_cost"),
                        f64f("ss_sales_price"),
                    ],
                    vec![
                        i64v(&[1]),
                        i64v(&[2]),
                        i64v(&[2]),
                        i64v(&[103]),
                        i64v(&[5]),
                        f64v(&[8.0]),
                        f64v(&[12.0]),
                    ],
                ),
            ],
        ),
        (
            "store_returns",
            vec![batch(
                vec![i64f("sr_item_sk"), i64f("sr_ticket_number")],
                vec![i64v(&[1]), i64v(&[102])],
            )],
        ),
        (
            "web_sales",
            vec![
                batch(
                    vec![
                        i64f("ws_sold_date_sk"),
                        i64f("ws_item_sk"),
                        i64f("ws_bill_customer_sk"),
                        i64f("ws_order_number"),
                        i64f("ws_quantity"),
                        f64f("ws_wholesale_cost"),
                        f64f("ws_sales_price"),
                    ],
                    vec![
                        i64v(&[1]),
                        i64v(&[1]),
                        i64v(&[1]),
                        i64v(&[301]),
                        i64v(&[4]),
                        f64v(&[9.0]),
                        f64v(&[13.0]),
                    ],
                ),
                batch(
                    vec![
                        i64f("ws_sold_date_sk"),
                        i64f("ws_item_sk"),
                        i64f("ws_bill_customer_sk"),
                        i64f("ws_order_number"),
                        i64f("ws_quantity"),
                        f64f("ws_wholesale_cost"),
                        f64f("ws_sales_price"),
                    ],
                    vec![
                        i64v(&[1]),
                        i64v(&[1]),
                        i64v(&[1]),
                        i64v(&[302]),
                        i64v(&[1]),
                        f64v(&[1.0]),
                        f64v(&[1.0]),
                    ],
                ),
            ],
        ),
        (
            "web_returns",
            vec![batch(
                vec![i64f("wr_item_sk"), i64f("wr_order_number")],
                vec![i64v(&[1]), i64v(&[302])],
            )],
        ),
        (
            "catalog_sales",
            vec![
                batch(
                    vec![
                        i64f("cs_sold_date_sk"),
                        i64f("cs_item_sk"),
                        i64f("cs_bill_customer_sk"),
                        i64f("cs_order_number"),
                        i64f("cs_quantity"),
                        f64f("cs_wholesale_cost"),
                        f64f("cs_sales_price"),
                    ],
                    vec![
                        i64v(&[1]),
                        i64v(&[1]),
                        i64v(&[1]),
                        i64v(&[201]),
                        i64v(&[6]),
                        f64v(&[10.0]),
                        f64v(&[14.0]),
                    ],
                ),
                batch(
                    vec![
                        i64f("cs_sold_date_sk"),
                        i64f("cs_item_sk"),
                        i64f("cs_bill_customer_sk"),
                        i64f("cs_order_number"),
                        i64f("cs_quantity"),
                        f64f("cs_wholesale_cost"),
                        f64f("cs_sales_price"),
                    ],
                    vec![
                        i64v(&[4]),
                        i64v(&[9]),
                        i64v(&[9]),
                        i64v(&[202]),
                        i64v(&[1]),
                        f64v(&[1.0]),
                        f64v(&[1.0]),
                    ],
                ),
            ],
        ),
        (
            "catalog_returns",
            vec![batch(
                vec![i64f("cr_item_sk"), i64f("cr_order_number")],
                vec![i64v(&[1]), i64v(&[999])],
            )],
        ),
    ]
}

/// W=1: the `ws`/`cs` replicated aggregate branches materialize as two single-task `Forward`
/// stages (keyed for the outer skeleton), byte-identical to the pre-fan-out planner.
#[tokio::test]
async fn q78_single_worker_keeps_forward_arms() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_env();
    let dir = tempfile::tempdir().unwrap();
    let tables = q78_tables();
    let engine = catalog_engine(write_fixtures(dir.path(), &tables)).await;
    let lp = engine.logical_plan(&qualify(Q78)).await.unwrap();
    let dq = plan_distributed_logical(&lp, &REPL_Q78).expect("Q78 plans at W=1");
    let outer = dq.stages.last().unwrap();
    assert_eq!(outer.upstream_stage_ids.len(), 3, "ss/ws/cs: {dq:?}");
    let mut forwards = 0;
    for &id in &outer.upstream_stage_ids {
        let s = stage_by_id(&dq, id);
        assert_eq!(s.hash_key_cols, vec![0, 1, 2], "keyed arms: {s:?}");
        if s.exchange == ExchangeMode::Forward {
            forwards += 1;
        }
    }
    assert_eq!(forwards, 2, "ws/cs are keyed Forward arms at W=1: {dq:?}");
}

/// W=2: each replicated arm becomes a sliced per-worker partial plus the recombine; the outer
/// keying re-targets the combines at the skeleton keys exactly as it did the Forward stages.
#[tokio::test]
async fn q78_two_workers_fans_out_arms() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_env();
    let dir = tempfile::tempdir().unwrap();
    let tables = q78_tables();
    let engine = catalog_engine(write_fixtures(dir.path(), &tables)).await;
    let lp = engine.logical_plan(&qualify(Q78)).await.unwrap();
    let w1 = plan_distributed_logical(&lp, &REPL_Q78).expect("W=1 plan");

    set_w2_env();
    let dq = plan_distributed_logical(&lp, &REPL_Q78).expect("W=2 plan");
    clear_env();

    assert_eq!(
        dq.stages.len(),
        w1.stages.len() + 2,
        "each Forward arm becomes partial + combine: {dq:?}"
    );
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.exchange == ExchangeMode::Forward),
        "no Forward stage remains at W=2: {dq:?}"
    );

    // Sliced arm partials: one per replicated channel fact, keyed by the group columns.
    for (fact, key) in [("web_sales", "ws_item_sk"), ("catalog_sales", "cs_item_sk")] {
        let partial = dq
            .stages
            .iter()
            .find(|s| s.upstream_stage_ids.is_empty() && s.sql.contains(fact))
            .unwrap_or_else(|| panic!("a partial stage scans {fact}: {dq:?}"));
        assert!(
            !partial.replicated_tables.split(',').any(|x| x == fact),
            "`{fact}` sliced out of its partial: {partial:?}"
        );
        assert!(
            partial.sql.contains(key) && partial.sql.contains("GROUP BY"),
            "arm partial shape: {partial:?}"
        );
        assert_eq!(partial.hash_key_cols, vec![0, 1, 2], "{partial:?}");
    }

    // The outer skeleton still reads three branch outputs keyed on (year, item, customer).
    let outer = dq.stages.last().unwrap();
    assert_eq!(outer.upstream_stage_ids.len(), 3, "{dq:?}");
    for &id in &outer.upstream_stage_ids {
        assert_eq!(
            stage_by_id(&dq, id).hash_key_cols,
            vec![0, 1, 2],
            "branch combine keyed for the skeleton: {dq:?}"
        );
    }
    assert!(dq.finalize_sql.is_some(), "two-phase TopK finalize: {dq:?}");
}

#[test]
fn q78_fanned_distributed_matches_single_node() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_stack_size(32 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("e2e runtime");
    rt.block_on(async {
        assert_fanned_matches_single_node("Q78", Q78, &REPL_Q78, &q78_tables(), |dq| {
            assert!(
                !dq.stages
                    .iter()
                    .any(|s| s.exchange == ExchangeMode::Forward),
                "this test exercises the fanned-out arms, not the Forward fallback: {dq:?}"
            );
        })
        .await;
    });
}
