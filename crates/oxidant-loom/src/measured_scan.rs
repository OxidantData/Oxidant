//! Measured-statistics in-memory scans: a [`datafusion::datasource::MemTable`] wrapper
//! whose physical scan reports driver-measured row counts instead of statistics recomputed
//! from the in-memory batches.
//!
//! The distributed engine's answer to Spark AQE's runtime join-strategy conversion: the
//! driver counts each producer stage's output rows at the stage barrier (a cheap local
//! worker action, KAN-32), ships the per-bucket totals on the consumer's `StageTicket`,
//! and the worker registers each `shuffle_input*` table through here with the exact row
//! count of the buckets its task pulls. The plan-time join-strategy guard
//! ([`crate::Engine::plan_time_smj_reroute`]) then sizes hash-join build sides from
//! measured data — keeping hash joins for builds that genuinely fit and rerouting only
//! builds that genuinely do not — instead of treating every upstream stage output as
//! un-estimable. Column-level statistics are deliberately not attached (nothing measures
//! them); DataFusion 54's own MemTable statistics already cover the no-ticket fallback.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::stats::Precision;
use datafusion::common::Statistics;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

/// A scan plan wrapper that reports pre-measured table statistics: delegates everything
/// except [`ExecutionPlan::partition_statistics`], which reports the stored exact row
/// count / byte size for the whole-table (`None`) query and the inner plan's
/// per-partition statistics otherwise. Column statistics are rebuilt from the exec's
/// CURRENT schema on every call (all-unknown) so projection rewrites can never leave the
/// statistics describing more columns than the plan carries. The inverse shape of the
/// `UnknownStatsExec` test double (which hides a MemTable's statistics to exercise the
/// unknown-estimate reroute).
#[derive(Debug)]
pub struct MeasuredStatsExec {
    inner: Arc<dyn ExecutionPlan>,
    props: Arc<PlanProperties>,
    num_rows: Precision<usize>,
    total_byte_size: Precision<usize>,
}

impl MeasuredStatsExec {
    pub fn new(
        inner: Arc<dyn ExecutionPlan>,
        num_rows: Precision<usize>,
        total_byte_size: Precision<usize>,
    ) -> Self {
        use datafusion::physical_plan::ExecutionPlanProperties;
        let props = PlanProperties::new(
            inner.properties().eq_properties.clone(),
            inner.output_partitioning().clone(),
            inner.pipeline_behavior(),
            inner.boundedness(),
        );
        Self {
            inner,
            props: props.into(),
            num_rows,
            total_byte_size,
        }
    }
}

impl DisplayAs for MeasuredStatsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MeasuredStatsExec(num_rows={:?})", self.num_rows)
    }
}

impl ExecutionPlan for MeasuredStatsExec {
    fn name(&self) -> &str {
        "MeasuredStatsExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            children.remove(0),
            self.num_rows,
            self.total_byte_size,
        )))
    }
    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> datafusion::error::Result<Arc<Statistics>> {
        match partition {
            None => Ok(Arc::new(Statistics {
                num_rows: self.num_rows,
                total_byte_size: self.total_byte_size,
                column_statistics: Statistics::unknown_column(&self.schema()),
            })),
            Some(p) => self.inner.partition_statistics(Some(p)),
        }
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }
}

/// Table provider for a `shuffle_input*` landing zone with driver-measured statistics:
/// the batches live in a plain [`datafusion::datasource::MemTable`]; scans are wrapped in
/// [`MeasuredStatsExec`] so the physical plan reports the measured exact row count (and the
/// batches' real in-memory byte size) rather than recomputing statistics — including a
/// per-column null-count walk over every batch — at plan time.
#[derive(Debug)]
pub struct MeasuredStatsTable {
    inner: Arc<dyn datafusion::catalog::TableProvider>,
    num_rows: usize,
    total_byte_size: usize,
}

impl MeasuredStatsTable {
    /// Build from `batches` and the driver-measured total `num_rows` of the upstream output
    /// this table holds (exact: the stage barrier counted the full stage output).
    pub fn try_new(batches: Vec<RecordBatch>, num_rows: usize) -> datafusion::error::Result<Self> {
        let schema = match batches.first() {
            Some(b) => b.schema(),
            None => {
                return Err(datafusion::error::DataFusionError::Plan(
                    "measured-stats table: no batches".into(),
                ))
            }
        };
        let total_byte_size = batches
            .iter()
            .map(|b| {
                b.columns()
                    .iter()
                    .map(|c| c.get_array_memory_size())
                    .sum::<usize>()
            })
            .sum();
        let inner = Arc::new(datafusion::datasource::MemTable::try_new(
            schema,
            vec![batches],
        )?);
        Ok(Self {
            inner,
            num_rows,
            total_byte_size,
        })
    }

    /// Attach the driver's measured statistics to an arbitrary provider.
    ///
    /// Exists so a shuffle input that was streamed to disk instead of held in a `MemTable`
    /// still reports the barrier-measured row count to the join-strategy guard. Without this
    /// the spill-backed path would silently fall back to DataFusion's file-derived estimates
    /// and plan joins differently from the in-memory path — the same input, two plans.
    pub fn from_provider(
        inner: Arc<dyn datafusion::catalog::TableProvider>,
        num_rows: usize,
        total_byte_size: usize,
    ) -> Self {
        Self {
            inner,
            num_rows,
            total_byte_size,
        }
    }
}

#[async_trait::async_trait]
impl datafusion::catalog::TableProvider for MeasuredStatsTable {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let inner = self.inner.scan(state, projection, filters, limit).await?;
        Ok(Arc::new(MeasuredStatsExec::new(
            inner,
            Precision::Exact(self.num_rows),
            Precision::Exact(self.total_byte_size),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn kv_batches(rows: i64) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        vec![RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>())),
                Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>())),
            ],
        )
        .unwrap()]
    }

    /// The spill-backed shuffle input must actually be readable and report the measured
    /// stats. SpillStore writes IPC *stream* format (`StreamWriter`) while DataFusion's
    /// `ArrowFormat` leads with `FileReader`; this proves the fallback path really works for
    /// the files we produce, rather than trusting that it does.
    #[tokio::test]
    async fn arrow_ipc_shuffle_input_streams_from_disk_with_measured_stats() {
        use datafusion::arrow::ipc::writer::StreamWriter;

        let dir =
            std::env::temp_dir().join(format!("ox-ipc-probe-{}-{}", std::process::id(), line!()));
        std::fs::create_dir_all(&dir).unwrap();
        let batches = kv_batches(40);
        let schema = batches[0].schema();
        {
            let f = std::fs::File::create(dir.join("part_0.arrow")).unwrap();
            let mut w = StreamWriter::try_new(f, &schema).unwrap();
            for b in &batches {
                w.write(b).unwrap();
            }
            w.finish().unwrap();
        }

        let engine = crate::Engine::new();
        engine
            .register_arrow_ipc_shuffle_input("spilled", &dir, schema, 40, 1234)
            .unwrap();
        let rows = engine
            .sql("SELECT count(*) AS n FROM spilled")
            .await
            .unwrap();
        let n: i64 = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 40, "spill-backed shuffle input must read back every row");

        let provider = engine.ctx.table_provider("spilled").await.unwrap();
        let scan = provider
            .scan(&engine.ctx.state(), None, &[], None)
            .await
            .unwrap();
        let stats = scan.partition_statistics(None).unwrap();
        assert!(
            matches!(stats.num_rows, Precision::Exact(40)),
            "measured stats must survive the spill path, got {:?}",
            stats.num_rows
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn scan_reports_measured_exact_row_count() {
        let engine = crate::Engine::new();
        engine
            .register_batches_with_stats("m", kv_batches(40), 40)
            .unwrap();
        let provider = engine.ctx.table_provider("m").await.unwrap();
        let scan = provider
            .scan(&engine.ctx.state(), None, &[], None)
            .await
            .unwrap();
        let stats = scan.partition_statistics(None).unwrap();
        assert!(
            matches!(stats.num_rows, Precision::Exact(40)),
            "the scan must report the measured exact row count, got {:?}",
            stats.num_rows
        );
        assert!(
            matches!(stats.total_byte_size, Precision::Exact(b) if b > 0),
            "the batches' in-memory byte size rides along, got {:?}",
            stats.total_byte_size
        );
        assert!(
            stats
                .column_statistics
                .iter()
                .all(|c| c.null_count.get_value().is_none()),
            "column statistics stay unknown — nothing measures them"
        );
        // The measured count is authoritative even when it disagrees with the materialized
        // batches (the driver's per-bucket totals describe the pulled subset by
        // construction; this pins which source wins).
        engine
            .register_batches_with_stats("m2", kv_batches(40), 7)
            .unwrap();
        let provider = engine.ctx.table_provider("m2").await.unwrap();
        let scan = provider
            .scan(&engine.ctx.state(), None, &[], None)
            .await
            .unwrap();
        assert!(matches!(
            scan.partition_statistics(None).unwrap().num_rows,
            Precision::Exact(7)
        ));
    }

    /// A projected scan must report column statistics matching the PROJECTED schema —
    /// statistics describing more columns than the plan carries trip DataFusion's
    /// `ExprBoundaries` (`try_from_column ... out of bounds`) downstream of the scan.
    #[tokio::test]
    async fn projected_scan_column_stats_match_schema() {
        let engine = crate::Engine::new();
        engine
            .register_batches_with_stats("m", kv_batches(40), 40)
            .unwrap();
        let provider = engine.ctx.table_provider("m").await.unwrap();
        let scan = provider
            .scan(&engine.ctx.state(), Some(&vec![1_usize]), &[], None)
            .await
            .unwrap();
        assert_eq!(scan.schema().fields().len(), 1);
        let stats = scan.partition_statistics(None).unwrap();
        assert_eq!(
            stats.column_statistics.len(),
            1,
            "column statistics must follow the projected schema"
        );
        assert!(matches!(stats.num_rows, Precision::Exact(40)));
    }
}
