//! [`GpuScanAggExec`]: the leaf [`ExecutionPlan`] the offload rule swaps in for a
//! scan + filter + group-by-aggregate subtree. It holds the extracted
//! [`GpuOpSpec`], executes it through the FFI shim on `execute()`, and streams
//! the resulting single batch (one partition — the shim computes FINAL aggregate
//! results, so partial/final merge stages are unnecessary by construction).

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};

use crate::ffi;
use crate::spec::GpuOpSpec;

/// Single-partition leaf producing the shim's final aggregate results. Its schema
/// is copied verbatim from the replaced final `AggregateExec`, so everything above
/// (projection / ORDER BY / LIMIT) sees exactly the plan it was built against.
#[derive(Debug)]
pub struct GpuScanAggExec {
    spec: GpuOpSpec,
    props: Arc<PlanProperties>,
}

impl GpuScanAggExec {
    pub fn new(spec: GpuOpSpec, schema: SchemaRef) -> Self {
        let props = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            spec,
            props: props.into(),
        }
    }

    /// The extracted operation spec (what gets serialized to the shim).
    pub fn spec(&self) -> &GpuOpSpec {
        &self.spec
    }
}

impl DisplayAs for GpuScanAggExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "GpuScanAggExec: table={} group_by=[{}] aggs={}",
            self.spec.table_path,
            self.spec.group_by.join(","),
            self.spec.aggregations.len()
        )
    }
}

impl ExecutionPlan for GpuScanAggExec {
    fn name(&self) -> &str {
        "GpuScanAggExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(format!(
                "GpuScanAggExec is a leaf node but got {} children",
                children.len()
            )))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "GpuScanAggExec has a single partition but got partition {partition}"
            )));
        }
        let batch = ffi::exec_spec(&self.spec)?;
        // The shim contract is "return FINAL results in the declared schema". Guard
        // it loosely (names + types; nullability/metadata may legitimately differ)
        // so a misbehaving shim fails loudly instead of answering the wrong query.
        let declared = self.schema();
        let got = batch.schema();
        if got.fields().len() != declared.fields().len()
            || got
                .fields()
                .iter()
                .zip(declared.fields().iter())
                .any(|(g, d)| g.name() != d.name() || g.data_type() != d.data_type())
        {
            return Err(DataFusionError::Execution(format!(
                "GPU shim returned schema {got} but GpuScanAggExec declared {declared}"
            )));
        }
        let schema = declared;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move { Ok(batch) }),
        )))
    }
}
