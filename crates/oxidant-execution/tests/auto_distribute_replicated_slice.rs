//! Replicated-slice producers for the broadcast-union split (TPC-DS Q71).
//!
//! `try_split_broadcast_union` plans a per-channel `UNION ALL` under an aggregate as two
//! producer stages: the sharded arm(s) as an ordinary per-worker partial, and the
//! replicated-only arm(s) as a second partial. That second stage used to run on exactly one
//! worker (`ExchangeMode::Forward`) — Q71 at SF10 scanned+joined+aggregated ~36M rows of
//! `store_sales` + `web_sales` on a single worker while the other idled. On a multi-worker
//! cluster (`OXIDANT_WORKER_COUNT` > 1 on the driver) the stage now runs on EVERY worker, each
//! scanning a disjoint 1/W file slice of each replicated arm's anchor table — the same
//! size-weighted file assignment sharded tables use — and the per-slice partials recombine in
//! the unchanged combine stage.
//!
//! The plan tests pin the placement decision (sliced at W=2, byte-identical `Forward` at
//! W=1, and every unsafe shape keeping `Forward`); the end-to-end test proves a Q71-shaped
//! query over two in-process workers — every fact table a multi-file parquet listing sliced
//! by the workers' explicit shard assignments — is row-for-row equal to single-node.

// ENV_LOCK serializes process-global `OXIDANT_WORKER_COUNT` / `OXIDANT_REPLICATED_TABLES` across
// async tests.
#![allow(clippy::await_holding_lock)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use oxidant_execution::driver::{run_stages, ExchangeMode};
use oxidant_execution::plan::{plan_distributed, plan_distributed_logical, DistributedQuery};
use oxidant_loom::arrow::array::{ArrayRef, Int64Array};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::shard::ShardAssignment;
use oxidant_loom::Engine;

/// `OXIDANT_WORKER_COUNT` / `OXIDANT_REPLICATED_TABLES` are process-global; serialize the tests
/// that mutate them.
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Q71's shape: three per-channel fact arms under one aggregate, the union wrapped in
/// broadcast joins against the shared dims (`item`, `date_dim`) before the outer GROUP BY.
/// `catalog_sales` is the sharded fact; `store_sales` and `web_sales` are replicated.
/// Every arm aliases its columns identically (as the real Q71 does), so the union's output
/// names survive whichever arm the sharding split narrows away.
const Q71_SHAPE: &str = "SELECT i.i_item_id AS item_id, d.d_year AS yr, SUM(tmp.ext_price) AS s \
     FROM ( \
         SELECT cs.cs_item_sk AS item_sk, cs.cs_sold_date_sk AS date_sk, \
                cs.cs_ext_sales_price AS ext_price \
         FROM testcat.default.catalog_sales cs \
         UNION ALL \
         SELECT ss.ss_item_sk AS item_sk, ss.ss_sold_date_sk AS date_sk, \
                ss.ss_ext_sales_price AS ext_price \
         FROM testcat.default.store_sales ss \
         UNION ALL \
         SELECT ws.ws_item_sk AS item_sk, ws.ws_sold_date_sk AS date_sk, \
                ws.ws_ext_sales_price AS ext_price \
         FROM testcat.default.web_sales ws \
     ) tmp \
     JOIN testcat.default.item i ON tmp.item_sk = i.i_item_sk \
     JOIN testcat.default.date_dim d ON tmp.date_sk = d.d_date_sk \
     GROUP BY i.i_item_id, d.d_year";

/// The replicated classification the SF10 row-aware rule produces for this shape.
const REPLICATED: [&str; 4] = ["item", "date_dim", "store_sales", "web_sales"];

fn i64f(name: &str) -> Field {
    Field::new(name, DataType::Int64, false)
}

fn i64v(vals: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(vals.to_vec()))
}

fn batch3(a: (&str, &[i64]), b: (&str, &[i64]), c: (&str, &[i64])) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![i64f(a.0), i64f(b.0), i64f(c.0)])),
        vec![i64v(a.1), i64v(b.1), i64v(c.1)],
    )
    .unwrap()
}

fn item() -> RecordBatch {
    batch3(
        ("i_item_sk", &[1, 2, 3, 4, 5, 6]),
        ("i_item_id", &[101, 102, 103, 104, 105, 106]),
        ("i_current_price", &[10, 20, 30, 40, 50, 60]),
    )
}

fn date_dim() -> RecordBatch {
    batch3(
        ("d_date_sk", &[1, 2, 3]),
        ("d_year", &[2021, 2022, 2023]),
        ("d_moy", &[1, 2, 3]),
    )
}

/// Every table's file list, each batch one `part-i.parquet`. File row counts differ so the
/// size-weighted assignment has a non-trivial (but deterministic) split to make.
fn table_files() -> Vec<(&'static str, Vec<RecordBatch>)> {
    vec![
        (
            "catalog_sales",
            vec![
                batch3(
                    ("cs_item_sk", &[1, 2, 3, 4, 5]),
                    ("cs_sold_date_sk", &[1, 2, 3, 1, 2]),
                    ("cs_ext_sales_price", &[10, 20, 30, 40, 50]),
                ),
                batch3(
                    ("cs_item_sk", &[6, 1, 2]),
                    ("cs_sold_date_sk", &[3, 1, 2]),
                    ("cs_ext_sales_price", &[60, 70, 80]),
                ),
                batch3(
                    ("cs_item_sk", &[3, 4]),
                    ("cs_sold_date_sk", &[3, 1]),
                    ("cs_ext_sales_price", &[90, 100]),
                ),
                batch3(
                    ("cs_item_sk", &[5, 6, 1, 2, 3, 4]),
                    ("cs_sold_date_sk", &[2, 3, 1, 2, 3, 1]),
                    ("cs_ext_sales_price", &[110, 120, 130, 140, 150, 160]),
                ),
            ],
        ),
        (
            "store_sales",
            vec![
                batch3(
                    ("ss_item_sk", &[1, 2, 3, 4, 5, 6, 1]),
                    ("ss_sold_date_sk", &[1, 2, 3, 1, 2, 3, 1]),
                    ("ss_ext_sales_price", &[1, 2, 3, 4, 5, 6, 7]),
                ),
                batch3(
                    ("ss_item_sk", &[2, 3]),
                    ("ss_sold_date_sk", &[2, 3]),
                    ("ss_ext_sales_price", &[8, 9]),
                ),
            ],
        ),
        (
            "web_sales",
            vec![
                batch3(
                    ("ws_item_sk", &[4, 5, 6, 1, 2]),
                    ("ws_sold_date_sk", &[1, 2, 3, 1, 2]),
                    ("ws_ext_sales_price", &[11, 12, 13, 14, 15]),
                ),
                batch3(
                    ("ws_item_sk", &[3, 4, 5]),
                    ("ws_sold_date_sk", &[3, 1, 2]),
                    ("ws_ext_sales_price", &[16, 17, 18]),
                ),
            ],
        ),
        ("item", vec![item()]),
        ("date_dim", vec![date_dim()]),
    ]
}

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
fn fixtures(root: &std::path::Path) -> SliceCatalog {
    let mut tables = HashMap::new();
    for (name, batches) in table_files() {
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
        tables.insert(name.to_string(), dir.to_str().unwrap().to_string());
    }
    SliceCatalog { tables }
}

async fn catalog_engine(catalog: SliceCatalog) -> Engine {
    let engine = Engine::new();
    engine.register_catalog("testcat", Arc::new(catalog));
    engine
}

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

/// The replicated-side partial stage: the one whose SQL scans `store_sales`.
fn replicated_stage(dq: &DistributedQuery) -> &oxidant_execution::driver::StageDef {
    dq.stages
        .iter()
        .find(|s| s.sql.contains("store_sales"))
        .expect("a replicated-side partial stage scanning store_sales")
}

// ---------------------------------------------------------------------------
// Plan shape
// ---------------------------------------------------------------------------

/// W=1 (or unknown): byte-identical to the pre-slicing planner — one `Forward` producer for
/// the replicated arms carrying the full replicate stamp.
#[tokio::test]
async fn q71_shape_single_worker_keeps_forward_byte_identical() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_env();
    let dir = tempfile::tempdir().unwrap();
    let engine = catalog_engine(fixtures(dir.path())).await;
    let lp = engine.logical_plan(Q71_SHAPE).await.unwrap();
    let dq = plan_distributed_logical(&lp, &REPLICATED).expect("Q71 shape must plan");

    assert_eq!(
        dq.stages.len(),
        3,
        "sharded partial + replicated partial + combine: {dq:?}"
    );
    let stage = replicated_stage(&dq);
    assert_eq!(
        stage.exchange,
        ExchangeMode::Forward,
        "single-worker placement is the original one-Forward design: {stage:?}"
    );
    for t in REPLICATED {
        assert!(
            stage.replicated_tables.split(',').any(|x| x == t),
            "full replicate stamp keeps `{t}`: {stage:?}"
        );
    }
}

/// W=2: the replicated arms plan as per-worker sliced producers. Only the placement and the
/// stage-local replicate stamp change — stage SQL (including the file-scan tail the workers
/// slice) is byte-identical to the single-worker plan, and the combine is untouched.
#[tokio::test]
async fn q71_shape_two_workers_plans_sliced_replicated_producers() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_env();
    let dir = tempfile::tempdir().unwrap();
    let engine = catalog_engine(fixtures(dir.path())).await;
    let lp = engine.logical_plan(Q71_SHAPE).await.unwrap();
    let w1 = plan_distributed_logical(&lp, &REPLICATED).expect("W=1 plan");

    set_w2_env();
    let dq = plan_distributed_logical(&lp, &REPLICATED).expect("W=2 plan");
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

    let stage = replicated_stage(&dq);
    assert_ne!(
        stage.exchange,
        ExchangeMode::Forward,
        "multi-worker placement runs the partial on every worker: {stage:?}"
    );
    assert!(
        !stage.hash_key_cols.is_empty(),
        "per-slice partials hash-shuffle by the group key like the sharded side: {stage:?}"
    );
    // The stamp drops exactly the sliced anchor tables — one per replicated arm — so the
    // workers' file sharder slices those scans for this stage only; the shared dims stay
    // replicated (scanned in full, keeping each arm's join co-located within its slice).
    for sliced in ["store_sales", "web_sales"] {
        assert!(
            !stage.replicated_tables.split(',').any(|t| t == sliced),
            "`{sliced}` must be sliced, not replicated: {stage:?}"
        );
    }
    for dim in ["item", "date_dim"] {
        assert!(
            stage.replicated_tables.split(',').any(|t| t == dim),
            "shared dim `{dim}` stays replicated in the sliced stage: {stage:?}"
        );
    }
    // The combine still reads both producer streams and needs no slicing-aware change.
    let combine = dq.stages.last().unwrap();
    assert_eq!(combine.upstream_stage_ids.len(), 2);
    assert!(combine.sql.contains("shuffle_input_0") && combine.sql.contains("shuffle_input_1"));
}

/// Assert no stage slices a replicated-arm scan: any stage whose SQL reads `store_sales` or
/// `web_sales` must still carry that table in its replicate stamp.
fn assert_no_sliced_replicated_scan(dq: &DistributedQuery) {
    for s in &dq.stages {
        for t in ["store_sales", "web_sales"] {
            if s.sql.contains(t) {
                assert!(
                    s.replicated_tables.split(',').any(|x| x == t),
                    "`{t}` must stay replicated wherever it is scanned: {s:?}"
                );
            }
        }
    }
}

/// A `DISTINCT` aggregate over the mixed union keeps the safe path: the split declines (the
/// existing guard at the top of `try_split_broadcast_union`), so no sliced producer can ever
/// be emitted for it. Whatever the fallback cascade then produces — a gather composition or a
/// single `Forward` stage — every stage that scans a replicated table keeps it replicated.
#[tokio::test]
async fn distinct_aggregate_over_mixed_union_never_slices() {
    let _guard = ENV_LOCK.lock().unwrap();
    set_w2_env();
    let dir = tempfile::tempdir().unwrap();
    let engine = catalog_engine(fixtures(dir.path())).await;
    let sql = Q71_SHAPE.replace(
        "SUM(tmp.ext_price) AS s",
        "COUNT(DISTINCT tmp.ext_price) AS s",
    );
    let lp = engine.logical_plan(&sql).await.unwrap();
    // Strict planning may decline (the safe refusal) or plan a gather; both are fine as long
    // as nothing slices. The SQL entry point must always answer.
    if let Ok(dq) = plan_distributed_logical(&lp, &REPLICATED) {
        assert_no_sliced_replicated_scan(&dq);
    }
    let dq = plan_distributed(&engine, &sql, &REPLICATED)
        .await
        .expect("the fallback cascade still answers the query");
    clear_env();
    assert_no_sliced_replicated_scan(&dq);
}

/// A `UNION` (distinct) of raw per-channel arms with the sharded fact in only some arms must
/// never produce a sliced replicated producer: whichever composition plans it (the dedup
/// composition or a branch-DAG merge), replicated tables stay replicated in every stage's
/// stamp — dedup correctness relies on exact-once semantics the slicing change deliberately
/// does not touch.
#[tokio::test]
async fn distinct_union_never_slices_replicated_arms() {
    let _guard = ENV_LOCK.lock().unwrap();
    set_w2_env();
    let dir = tempfile::tempdir().unwrap();
    let engine = catalog_engine(fixtures(dir.path())).await;
    let sql = Q71_SHAPE.replace("UNION ALL", "UNION");
    let lp = engine.logical_plan(&sql).await.unwrap();
    let dq = plan_distributed_logical(&lp, &REPLICATED)
        .expect("aggregate over a distinct mixed union plans");
    clear_env();
    let mut saw_replicated_arm = false;
    for s in &dq.stages {
        for t in ["store_sales", "web_sales"] {
            if s.sql.contains(t) {
                saw_replicated_arm = true;
                assert!(
                    s.replicated_tables.split(',').any(|x| x == t),
                    "a distinct union's replicated side is never sliced: {s:?}"
                );
            }
        }
    }
    assert!(
        saw_replicated_arm,
        "the plan must actually scan the replicated arms somewhere: {dq:?}"
    );
}

/// KAN-54 shifted to the replicated side: per-arm pre-aggregates (Q33's shape) do not
/// recombine under file slicing — an inner GROUP BY key spanning two slices yields one inner
/// row per slice — so the replicated stage keeps `Forward` even at W=2. (The outer SUM over
/// the sharded side's per-worker inner partials still plans; only slicing is refused.)
#[tokio::test]
async fn pre_aggregated_replicated_arms_keep_forward() {
    let _guard = ENV_LOCK.lock().unwrap();
    set_w2_env();
    let dir = tempfile::tempdir().unwrap();
    let engine = catalog_engine(fixtures(dir.path())).await;
    let sql = "SELECT tmp.item_sk AS k, SUM(tmp.s) AS s FROM ( \
         SELECT cs.cs_item_sk AS item_sk, SUM(cs.cs_ext_sales_price) AS s \
         FROM testcat.default.catalog_sales cs GROUP BY cs.cs_item_sk \
         UNION ALL \
         SELECT ss.ss_item_sk, SUM(ss.ss_ext_sales_price) \
         FROM testcat.default.store_sales ss GROUP BY ss.ss_item_sk \
         UNION ALL \
         SELECT ws.ws_item_sk, SUM(ws.ws_ext_sales_price) \
         FROM testcat.default.web_sales ws GROUP BY ws.ws_item_sk \
     ) tmp GROUP BY tmp.item_sk";
    let lp = engine.logical_plan(sql).await.unwrap();
    let dq = plan_distributed_logical(&lp, &REPLICATED)
        .expect("Q33-style pre-aggregated arms still plan through the KAN-54 guard");
    clear_env();
    let stage = replicated_stage(&dq);
    assert_eq!(
        stage.exchange,
        ExchangeMode::Forward,
        "an aggregate on the replicated side does not recombine under slicing: {stage:?}"
    );
}

/// An operator force-include (`OXIDANT_REPLICATED_TABLES`) wins over slicing: the workers' env
/// override would replicate the table anyway and multiply the arm, so the stage keeps
/// `Forward` when every candidate anchor is env-forced.
#[tokio::test]
async fn env_forced_replicated_tables_keep_forward() {
    let _guard = ENV_LOCK.lock().unwrap();
    set_w2_env();
    std::env::set_var("OXIDANT_REPLICATED_TABLES", "store_sales,web_sales");
    let dir = tempfile::tempdir().unwrap();
    let engine = catalog_engine(fixtures(dir.path())).await;
    let lp = engine.logical_plan(Q71_SHAPE).await.unwrap();
    let dq = plan_distributed_logical(&lp, &REPLICATED).expect("Q71 shape must plan");
    clear_env();
    let stage = replicated_stage(&dq);
    assert_eq!(
        stage.exchange,
        ExchangeMode::Forward,
        "env-forced anchors refuse slicing: {stage:?}"
    );
}

// ---------------------------------------------------------------------------
// End-to-end: two in-process workers, every table a multi-file parquet listing.
// ---------------------------------------------------------------------------

static PORT: std::sync::OnceLock<std::sync::atomic::AtomicU16> = std::sync::OnceLock::new();

fn unique_worker_port() -> u16 {
    // Distinct base from the kan49*/row_multiple harnesses so co-located test binaries never
    // collide on a port.
    PORT.get_or_init(|| {
        std::sync::atomic::AtomicU16::new(26000 + (std::process::id() as u16 % 512))
    })
    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
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

/// Both workers resolve the same parquet directories through the catalog; each shards the
/// listings by its own explicit assignment (the in-process stand-in for per-worker env).
/// `catalog_sales` slices in every stage (sharded); `store_sales`/`web_sales` only in the
/// sliced producer stage (their stamp entry is dropped there); dims never.
async fn two_workers(root: &std::path::Path) -> oxidant_execution::driver::Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(catalog_engine(fixtures(root)).await);
        tokio::spawn(async move {
            let _ = oxidant_execution::flight::serve_worker_with_assignment(
                port,
                e,
                ShardAssignment { index: i, count: 2 },
            )
            .await;
        });
    }
    oxidant_execution::driver::Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ])
}

/// The Q71 shape executed over the sliced producers must equal single-node row-for-row:
/// worker 0 and worker 1 scan disjoint file slices of `store_sales`/`web_sales` (and of the
/// sharded `catalog_sales`), and any drop or duplication shows up as a wrong group sum.
/// The runtime gets a large worker stack: unoptimized builds plan the wide union+join stage
/// SQL through the catalog bridge with frames far bigger than tokio's 2 MiB default allows.
#[test]
fn q71_shape_sliced_distributed_matches_single_node() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_stack_size(32 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("e2e runtime");
    rt.block_on(q71_sliced_distributed_matches_single_node_inner());
}

async fn q71_sliced_distributed_matches_single_node_inner() {
    let _guard = ENV_LOCK.lock().unwrap();
    set_w2_env();
    let dir = tempfile::tempdir().unwrap();
    let planner = catalog_engine(fixtures(dir.path())).await;
    let expected = planner.sql(Q71_SHAPE).await.expect("single-node");
    assert!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
        "single-node result must be non-empty (otherwise the comparison is vacuous)"
    );

    let lp = planner.logical_plan(Q71_SHAPE).await.unwrap();
    let dq = plan_distributed_logical(&lp, &REPLICATED).expect("must plan distributed");
    assert_ne!(
        replicated_stage(&dq).exchange,
        ExchangeMode::Forward,
        "this test exercises the sliced producers, not the Forward fallback"
    );

    let cluster = two_workers(dir.path()).await;
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
    clear_env();
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
        "sliced distributed result must equal single-node"
    );
}
