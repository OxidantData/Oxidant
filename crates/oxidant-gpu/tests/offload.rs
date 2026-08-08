//! End-to-end tests for the offload rule against real DataFusion-planned physical
//! plans, plus execution of `GpuScanAggExec` through the (mock) FFI shim.
//!
//! The fixture is a tiny TPC-H-lineitem-shaped parquet file generated in-test with
//! `parquet::arrow::ArrowWriter`; the SessionContext mirrors the engine's parquet
//! scan settings (filter pushdown into the decoder, string-view reads) so the
//! plans the matcher sees are the plans the engine would build.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Date32Array, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures::TryStreamExt;

use oxidant_gpu::exec::GpuScanAggExec;
use oxidant_gpu::rule::GpuOffloadRule;
use oxidant_gpu::spec::{
    AggFunc, ArithOp, CmpOp, DerivedColumn, GpuExpr, GpuOpSpec, LiteralSpec, LiteralType,
};

fn lineitem_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_quantity", DataType::Int64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Date32, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8])),
            Arc::new(Int64Array::from(vec![10, 30, 5, 24, 60, 15, 8, 100])),
            Arc::new(Float64Array::from(vec![
                100.0, 200.5, 50.25, 75.0, 300.0, 125.5, 80.0, 400.0,
            ])),
            Arc::new(Float64Array::from(vec![
                0.06, 0.03, 0.07, 0.05, 0.08, 0.06, 0.04, 0.07,
            ])),
            Arc::new(Float64Array::from(vec![
                0.07, 0.08, 0.05, 0.06, 0.07, 0.08, 0.05, 0.06,
            ])),
            Arc::new(StringArray::from(vec![
                "A", "N", "A", "R", "N", "A", "N", "R",
            ])),
            // Days since epoch; 8766 = 1994-01-01. Only the dtype matters here —
            // the rule plans filters, it never evaluates them.
            Arc::new(Date32Array::from(vec![
                8552, 8766, 8908, 10474, 10565, 8039, 9209, 9080,
            ])),
        ],
    )
    .unwrap()
}

/// Write one parquet part file of the lineitem fixture into `dir`.
fn write_part(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    let batch = lineitem_batch();
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    path
}

/// A context configured like the engine's (oxidant-loom `Engine::new_inner`):
/// filters pushed into the parquet decoder, string-view reads.
async fn fixture_ctx() -> (SessionContext, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = write_part(dir.path(), "lineitem.parquet");

    let mut config = SessionConfig::new();
    {
        let opts = config.options_mut();
        opts.execution.parquet.pushdown_filters = true;
        opts.execution.parquet.reorder_filters = true;
        opts.execution.parquet.binary_as_string = true;
        opts.execution.parquet.schema_force_view_types = true;
    }
    let ctx = SessionContext::new_with_config(config);
    ctx.register_parquet(
        "lineitem",
        path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    (ctx, dir)
}

fn plan_text(plan: &Arc<dyn ExecutionPlan>) -> String {
    format!("{}", displayable(plan.as_ref()).indent(true))
}

/// Plan `sql` end-to-end, then apply the offload rule (in the engine the rule is
/// inserted into the physical optimizer pipeline ahead of EnforceDistribution).
async fn offload_plan(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
    let df = ctx.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let state = ctx.state();
    let config = state.config_options().clone();
    GpuOffloadRule
        .optimize(plan, &config)
        .unwrap_or_else(|e| panic!("gpu_offload rule failed on `{sql}`: {e}"))
}

/// The spec of the single GpuScanAggExec node in a rewritten plan (it may sit
/// below a CPU ProjectionExec — anything above the final aggregate stays CPU),
/// or panic with the plan text.
fn gpu_spec(plan: &Arc<dyn ExecutionPlan>) -> GpuOpSpec {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut spec: Option<GpuOpSpec> = None;
    plan.apply(|node| {
        let any: &dyn std::any::Any = node.as_ref();
        if let Some(exec) = any.downcast_ref::<GpuScanAggExec>() {
            spec = Some(exec.spec().clone());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    spec.unwrap_or_else(|| panic!("no GpuScanAggExec in plan:\n{}", plan_text(plan)))
}

fn sorted<T: Ord>(mut v: Vec<T>) -> Vec<T> {
    v.sort();
    v
}

// --- rule fires on the TPC-H Q1/Q6 shapes -----------------------------------

/// Q6 shape: whole-table aggregation over conjunctive filters on one parquet file.
#[tokio::test(flavor = "multi_thread")]
async fn rule_fires_on_q6_shape() {
    let (ctx, _dir) = fixture_ctx().await;
    let plan = offload_plan(
        &ctx,
        "SELECT sum(l_extendedprice) AS revenue FROM lineitem \
         WHERE l_shipdate >= DATE '1994-01-01' AND l_quantity < 24",
    )
    .await;
    let text = plan_text(&plan);
    assert!(text.contains("GpuScanAggExec"), "plan:\n{text}");
    assert!(!text.contains("AggregateExec"), "plan:\n{text}");

    let spec = gpu_spec(&plan);
    assert!(spec.table_path.ends_with("lineitem.parquet"));
    // The shim opens the path with plain filesystem calls, so the spec must
    // carry the absolute path, not object_store's slash-stripped form.
    assert!(
        spec.table_path.starts_with('/'),
        "path: {}",
        spec.table_path
    );
    // Single-file table: files == [table_path].
    assert_eq!(spec.files, vec![spec.table_path.clone()]);
    assert!(spec.derived_columns.is_empty());
    assert!(spec.group_by.is_empty());
    assert_eq!(spec.aggregations.len(), 1);
    assert_eq!(spec.aggregations[0].func, AggFunc::Sum);
    assert_eq!(spec.aggregations[0].col.as_deref(), Some("l_extendedprice"));
    // The user's `AS revenue` alias lives in the CPU ProjectionExec above; the
    // shim must name the column after the aggregate node's own output name.
    assert_eq!(spec.aggregations[0].alias, "sum(lineitem.l_extendedprice)");

    // Both conjuncts must be extracted (reorder_filters may swap their order).
    let mut filters: Vec<(String, CmpOp, LiteralType, String)> = spec
        .filters
        .iter()
        .map(|f| (f.col.clone(), f.op, f.literal.ty, f.literal.value.clone()))
        .collect();
    filters.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        filters,
        vec![
            (
                "l_quantity".to_string(),
                CmpOp::Lt,
                LiteralType::Int,
                "24".to_string(),
            ),
            (
                "l_shipdate".to_string(),
                CmpOp::GtEq,
                LiteralType::Date,
                "1994-01-01".to_string(),
            ),
        ],
        "spec: {spec:?}"
    );

    // Read set: the two filter columns plus the aggregate input, deduped.
    let cols = sorted(spec.columns.iter().map(|c| c.name.clone()).collect());
    assert_eq!(
        cols,
        vec!["l_extendedprice", "l_quantity", "l_shipdate"],
        "spec: {spec:?}"
    );
}

/// Q1 shape: group-by aggregation (sum + avg + count-star) over a filter.
#[tokio::test(flavor = "multi_thread")]
async fn rule_fires_on_q1_shape() {
    let (ctx, _dir) = fixture_ctx().await;
    let plan = offload_plan(
        &ctx,
        "SELECT l_returnflag, sum(l_quantity) AS sum_qty, \
         avg(l_extendedprice) AS avg_price, count(*) AS cnt \
         FROM lineitem WHERE l_shipdate <= DATE '1998-09-02' \
         GROUP BY l_returnflag",
    )
    .await;
    let text = plan_text(&plan);
    assert!(text.contains("GpuScanAggExec"), "plan:\n{text}");
    assert!(!text.contains("AggregateExec"), "plan:\n{text}");

    let spec = gpu_spec(&plan);
    assert_eq!(spec.group_by, vec!["l_returnflag"]);
    assert_eq!(spec.filters.len(), 1);
    assert_eq!(spec.filters[0].col, "l_shipdate");
    assert_eq!(spec.filters[0].op, CmpOp::LtEq);
    assert_eq!(spec.filters[0].literal.ty, LiteralType::Date);

    let aggs: Vec<(AggFunc, Option<String>, String)> = spec
        .aggregations
        .iter()
        .map(|a| (a.func, a.col.clone(), a.alias.clone()))
        .collect();
    assert_eq!(
        aggs,
        vec![
            (
                AggFunc::Sum,
                Some("l_quantity".to_string()),
                "sum(lineitem.l_quantity)".to_string()
            ),
            (
                AggFunc::Avg,
                Some("l_extendedprice".to_string()),
                "avg(lineitem.l_extendedprice)".to_string(),
            ),
            (AggFunc::Count, None, "count(Int64(1))".to_string()),
        ],
        "count(*) must map to col=null; spec: {spec:?}"
    );
}

/// No WHERE clause at all: still offloadable, with an empty filter list.
#[tokio::test(flavor = "multi_thread")]
async fn rule_fires_without_filters() {
    let (ctx, _dir) = fixture_ctx().await;
    let plan = offload_plan(
        &ctx,
        "SELECT l_returnflag, count(*) AS cnt FROM lineitem GROUP BY l_returnflag",
    )
    .await;
    let text = plan_text(&plan);
    assert!(text.contains("GpuScanAggExec"), "plan:\n{text}");
    let spec = gpu_spec(&plan);
    assert!(spec.filters.is_empty());
    assert_eq!(spec.aggregations.len(), 1);
    assert_eq!(spec.aggregations[0].func, AggFunc::Count);
    assert_eq!(spec.aggregations[0].col, None);
}

/// The rule inserted into the stock pipeline ahead of `EnforceDistribution` (exactly
/// where `oxidant_gpu::register_if_enabled` puts it) must fire — and the rules that
/// run after it (distribution/sort enforcement, the plan sanity checker) must accept
/// the rewritten single-partition subtree.
#[tokio::test(flavor = "multi_thread")]
async fn rule_integrates_into_pipeline_before_enforce_distribution() {
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lineitem.parquet");
    let batch = lineitem_batch();
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = true;
    let mut rules = PhysicalOptimizer::new().rules;
    let position = rules
        .iter()
        .position(|r| r.name() == "EnforceDistribution")
        .expect("stock pipeline has EnforceDistribution");
    rules.insert(position, Arc::new(GpuOffloadRule));
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(RuntimeEnvBuilder::new().build_arc().unwrap())
        .with_default_features()
        .with_physical_optimizer_rules(rules)
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_parquet(
        "lineitem",
        path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();

    let df = ctx
        .sql("SELECT sum(l_extendedprice) FROM lineitem WHERE l_quantity < 24")
        .await
        .unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let text = plan_text(&plan);
    assert!(
        text.contains("GpuScanAggExec"),
        "rule must fire from inside the pipeline:\n{text}"
    );
    assert!(!text.contains("AggregateExec"), "plan:\n{text}");
}

// --- rule refuses everything outside the shape -------------------------------

/// Joins are phase 2: a join anywhere in the plan means no offload.
#[tokio::test(flavor = "multi_thread")]
async fn rule_ignores_join_plan() {
    let (ctx, _dir) = fixture_ctx().await;
    let plan = offload_plan(
        &ctx,
        "SELECT sum(a.l_extendedprice) FROM lineitem a JOIN lineitem b \
         ON a.l_orderkey = b.l_orderkey WHERE a.l_quantity < 24",
    )
    .await;
    let text = plan_text(&plan);
    assert!(
        !text.contains("GpuScanAggExec"),
        "join plan must stay on CPU:\n{text}"
    );
}

/// An unsupported aggregate function keeps the query on CPU.
#[tokio::test(flavor = "multi_thread")]
async fn rule_ignores_unsupported_aggregate() {
    let (ctx, _dir) = fixture_ctx().await;
    let plan = offload_plan(
        &ctx,
        "SELECT approx_distinct(l_orderkey) FROM lineitem WHERE l_quantity < 24",
    )
    .await;
    let text = plan_text(&plan);
    assert!(
        !text.contains("GpuScanAggExec"),
        "approx_distinct must stay on CPU:\n{text}"
    );
}

/// KAN-75: a table registered as a DIRECTORY of parquet part files is offloadable
/// — spec.files must carry every part file's absolute path.
#[tokio::test(flavor = "multi_thread")]
async fn rule_fires_on_multi_file_dir_scan() {
    let dir = tempfile::tempdir().unwrap();
    let mut part_paths: Vec<String> = [
        write_part(dir.path(), "part-0.parquet"),
        write_part(dir.path(), "part-1.parquet"),
        write_part(dir.path(), "part-2.parquet"),
    ]
    .into_iter()
    .map(|p| p.to_str().unwrap().to_string())
    .collect();
    part_paths.sort();

    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = true;
    let ctx = SessionContext::new_with_config(config);
    ctx.register_parquet(
        "lineitem",
        dir.path().to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();

    let plan = offload_plan(
        &ctx,
        "SELECT sum(l_extendedprice) FROM lineitem WHERE l_quantity < 24",
    )
    .await;
    let text = plan_text(&plan);
    assert!(
        text.contains("GpuScanAggExec"),
        "directory scan must offload:\n{text}"
    );

    let spec = gpu_spec(&plan);
    let mut files = spec.files.clone();
    files.sort();
    assert_eq!(files, part_paths, "spec: {spec:?}");
    assert_eq!(spec.table_path, spec.files[0]);
}

/// KAN-76: the REAL TPC-H Q6 — its aggregate input is a product of two columns,
/// which must become a derived column the aggregation references.
#[tokio::test(flavor = "multi_thread")]
async fn rule_fires_on_real_q6_derived_product() {
    let (ctx, _dir) = fixture_ctx().await;
    let plan = offload_plan(
        &ctx,
        "SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem \
         WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' \
         AND l_discount >= 0.05 AND l_discount <= 0.07 AND l_quantity < 24",
    )
    .await;
    let text = plan_text(&plan);
    assert!(text.contains("GpuScanAggExec"), "plan:\n{text}");
    assert!(!text.contains("AggregateExec"), "plan:\n{text}");

    let spec = gpu_spec(&plan);
    // The product is interned as _gpu_derived_0 and the aggregation points at it.
    assert_eq!(
        spec.derived_columns,
        vec![DerivedColumn {
            name: "_gpu_derived_0".to_string(),
            expr: GpuExpr::Arith {
                op: ArithOp::Mul,
                lhs: Box::new(GpuExpr::Col {
                    col: "l_extendedprice".to_string()
                }),
                rhs: Box::new(GpuExpr::Col {
                    col: "l_discount".to_string()
                }),
            },
        }],
        "spec: {spec:?}"
    );
    assert_eq!(spec.aggregations.len(), 1);
    assert_eq!(spec.aggregations[0].func, AggFunc::Sum);
    assert_eq!(spec.aggregations[0].col.as_deref(), Some("_gpu_derived_0"));
    // The read set carries the product's BASE columns (plus the filter columns),
    // never the derived name.
    let cols = sorted(spec.columns.iter().map(|c| c.name.clone()).collect());
    assert_eq!(
        cols,
        vec!["l_discount", "l_extendedprice", "l_quantity", "l_shipdate"],
        "spec: {spec:?}"
    );
}

/// KAN-76: the full Q1 aggregate list — two distinct arithmetic inputs become two
/// derived columns; the bare-column avg stays a direct column reference.
#[tokio::test(flavor = "multi_thread")]
async fn rule_fires_on_full_q1_derived_aggregates() {
    let (ctx, _dir) = fixture_ctx().await;
    let plan = offload_plan(
        &ctx,
        "SELECT l_returnflag, \
         sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, \
         sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, \
         avg(l_quantity) AS avg_qty \
         FROM lineitem WHERE l_shipdate <= DATE '1998-09-02' GROUP BY l_returnflag",
    )
    .await;
    let text = plan_text(&plan);
    assert!(text.contains("GpuScanAggExec"), "plan:\n{text}");

    let spec = gpu_spec(&plan);
    let one = || GpuExpr::Lit {
        lit: LiteralSpec {
            ty: LiteralType::Float,
            value: "1".to_string(),
        },
    };
    let price_minus_disc = || GpuExpr::Arith {
        op: ArithOp::Mul,
        lhs: Box::new(GpuExpr::Col {
            col: "l_extendedprice".to_string(),
        }),
        rhs: Box::new(GpuExpr::Arith {
            op: ArithOp::Sub,
            lhs: Box::new(one()),
            rhs: Box::new(GpuExpr::Col {
                col: "l_discount".to_string(),
            }),
        }),
    };
    assert_eq!(
        spec.derived_columns,
        vec![
            DerivedColumn {
                name: "_gpu_derived_0".to_string(),
                expr: price_minus_disc(),
            },
            DerivedColumn {
                name: "_gpu_derived_1".to_string(),
                expr: GpuExpr::Arith {
                    op: ArithOp::Mul,
                    lhs: Box::new(price_minus_disc()),
                    rhs: Box::new(GpuExpr::Arith {
                        op: ArithOp::Add,
                        lhs: Box::new(one()),
                        rhs: Box::new(GpuExpr::Col {
                            col: "l_tax".to_string(),
                        }),
                    }),
                },
            },
        ],
        "spec: {spec:?}"
    );
    let agg_cols: Vec<Option<String>> = spec.aggregations.iter().map(|a| a.col.clone()).collect();
    assert_eq!(
        agg_cols,
        vec![
            Some("_gpu_derived_0".to_string()),
            Some("_gpu_derived_1".to_string()),
            Some("l_quantity".to_string()), // avg over a bare col stays direct
        ],
        "spec: {spec:?}"
    );
    assert!(spec
        .aggregations
        .iter()
        .all(|a| a.func == AggFunc::Sum || a.func == AggFunc::Avg));
}

/// Identical expression inputs across aggregations share ONE derived column —
/// the shim must not evaluate the same tree twice.
#[tokio::test(flavor = "multi_thread")]
async fn rule_dedupes_identical_derived_exprs() {
    let (ctx, _dir) = fixture_ctx().await;
    let plan = offload_plan(
        &ctx,
        "SELECT sum(l_extendedprice * l_discount), avg(l_extendedprice * l_discount) \
         FROM lineitem WHERE l_quantity < 24",
    )
    .await;
    let text = plan_text(&plan);
    assert!(text.contains("GpuScanAggExec"), "plan:\n{text}");

    let spec = gpu_spec(&plan);
    assert_eq!(spec.derived_columns.len(), 1, "spec: {spec:?}");
    let agg_cols: Vec<Option<String>> = spec.aggregations.iter().map(|a| a.col.clone()).collect();
    assert_eq!(
        agg_cols,
        vec![
            Some("_gpu_derived_0".to_string()),
            Some("_gpu_derived_0".to_string())
        ],
        "spec: {spec:?}"
    );
}

/// Non-arithmetic aggregate inputs still refuse: function calls and operators
/// outside +,-,*,/ (a transparent numeric cast is fine, but modulo is not).
#[tokio::test(flavor = "multi_thread")]
async fn rule_ignores_non_arithmetic_aggregate_input() {
    let (ctx, _dir) = fixture_ctx().await;
    for sql in [
        "SELECT sum(extract(year from l_shipdate)) FROM lineitem WHERE l_quantity < 24",
        "SELECT sum(l_quantity % 2) FROM lineitem WHERE l_quantity < 24",
    ] {
        let plan = offload_plan(&ctx, sql).await;
        let text = plan_text(&plan);
        assert!(
            !text.contains("GpuScanAggExec"),
            "`{sql}` must stay on CPU:\n{text}"
        );
    }
}

/// A non-comparison filter conjunct (LIKE) keeps the query on CPU.
#[tokio::test(flavor = "multi_thread")]
async fn rule_ignores_unsupported_filter() {
    let (ctx, _dir) = fixture_ctx().await;
    let plan = offload_plan(
        &ctx,
        "SELECT sum(l_extendedprice) FROM lineitem \
         WHERE l_returnflag LIKE 'A%' AND l_quantity < 24",
    )
    .await;
    let text = plan_text(&plan);
    assert!(
        !text.contains("GpuScanAggExec"),
        "LIKE filter must stay on CPU:\n{text}"
    );
}

/// A non-parquet scan (in-memory table) is not offloadable.
#[tokio::test(flavor = "multi_thread")]
async fn rule_ignores_non_parquet_scan() {
    let (ctx, _dir) = fixture_ctx().await;
    ctx.register_batch("mem_lineitem", lineitem_batch())
        .unwrap();
    let plan = offload_plan(
        &ctx,
        "SELECT l_returnflag, sum(l_quantity) FROM mem_lineitem GROUP BY l_returnflag",
    )
    .await;
    let text = plan_text(&plan);
    assert!(
        !text.contains("GpuScanAggExec"),
        "MemTable plan must stay on CPU:\n{text}"
    );
}

// --- execution through the mock shim ------------------------------------------

fn dummy_spec() -> GpuOpSpec {
    GpuOpSpec {
        table_path: "/unused/by/mock.parquet".to_string(),
        files: vec!["/unused/by/mock.parquet".to_string()],
        columns: vec![],
        derived_columns: vec![],
        filters: vec![],
        group_by: vec![],
        aggregations: vec![],
    }
}

/// The mock shim answers every spec with one row, `mock_sum` = 42; the exec must
/// import it through the Arrow C Data Interface and stream it as a single batch.
#[tokio::test(flavor = "multi_thread")]
async fn exec_streams_mock_batch_via_ffi() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "mock_sum",
        DataType::Int64,
        false,
    )]));
    let exec = GpuScanAggExec::new(dummy_spec(), schema);
    let stream = exec
        .execute(0, Arc::new(datafusion::execution::TaskContext::default()))
        .unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.schema().fields()[0].name(), "mock_sum");
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("mock_sum is Int64");
    assert_eq!(col.value(0), 42);
}

/// The declared schema is the contract with the rest of the plan: a shim returning
/// anything else must fail loudly instead of answering the wrong query.
#[tokio::test(flavor = "multi_thread")]
async fn exec_rejects_shim_schema_mismatch() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("mock_sum", DataType::Int64, false),
        Field::new("extra", DataType::Int64, false),
    ]));
    let exec = GpuScanAggExec::new(dummy_spec(), schema);
    let err = match exec.execute(0, Arc::new(datafusion::execution::TaskContext::default())) {
        Err(e) => e,
        Ok(_) => panic!("a shim batch wider than the declared schema must error"),
    };
    assert!(
        err.to_string().contains("schema"),
        "unexpected error: {err}"
    );
}

/// `collect` over the exec's stream asks for partition 0 only; anything else is a bug.
#[tokio::test(flavor = "multi_thread")]
async fn exec_rejects_nonzero_partition() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "mock_sum",
        DataType::Int64,
        false,
    )]));
    let exec = GpuScanAggExec::new(dummy_spec(), schema);
    let err = match exec.execute(1, Arc::new(datafusion::execution::TaskContext::default())) {
        Err(e) => e,
        Ok(_) => panic!("partition 1 of a single-partition exec must error"),
    };
    assert!(err.to_string().contains("partition"), "unexpected: {err}");
}
