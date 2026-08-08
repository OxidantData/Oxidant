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

/// Cast the shim's float-computed columns back to the declared decimal types.
/// Passes everything else through unchanged; any non-decimal mismatch is left
/// for the schema guard in `execute()` to report.
fn coerce_shim_batch(
    batch: datafusion::arrow::record_batch::RecordBatch,
    declared: &SchemaRef,
) -> Result<datafusion::arrow::record_batch::RecordBatch> {
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::DataType;

    let got_schema = batch.schema();
    if got_schema.fields().len() != declared.fields().len() {
        return Ok(batch); // the schema guard below reports this properly
    }
    let mut columns = batch.columns().to_vec();
    let mut changed = false;
    for (i, field) in declared.fields().iter().enumerate() {
        let got_ty = got_schema.field(i).data_type();
        let declared_ty = field.data_type();
        if got_ty == declared_ty {
            continue;
        }
        match (got_ty, declared_ty) {
            (DataType::Float64, DataType::Decimal128(_, _))
            | (DataType::Float64, DataType::Decimal256(_, _))
            // The shim exports strings as Arrow "u" (Utf8); engines running
            // with schema_force_view_types declare Utf8View instead.
            | (DataType::Utf8, DataType::Utf8View)
            | (DataType::Utf8, DataType::LargeUtf8)
            // libcudf COUNT is Int32; the engine declares count(*) as Int64.
            | (DataType::Int32, DataType::Int64) => {
                columns[i] = cast(columns[i].as_ref(), declared_ty)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                changed = true;
            }
            _ => {} // guard below reports it
        }
    }
    if !changed {
        return Ok(batch);
    }
    datafusion::arrow::record_batch::RecordBatch::try_new(declared.clone(), columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

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
        // The shim computes in float64 (libcudf decimal aggregation is narrower
        // than the CPU path's); coerce its output to the declared decimal types
        // at the FFI boundary. Tradeoff, documented in the crate README: GPU
        // aggregates over decimal inputs are float-computed and cast back —
        // exact through ~15 significant digits, rounding beyond that.
        let batch = coerce_shim_batch(batch, &self.schema())?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Decimal128Array, Float64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn float64_shim_output_coerces_to_declared_decimal() {
        let got_schema = Arc::new(Schema::new(vec![Field::new(
            "sum(li_one.l_extendedprice)",
            DataType::Float64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            got_schema,
            vec![Arc::new(Float64Array::from(vec![20506615920.80]))],
        )
        .unwrap();
        let declared = Arc::new(Schema::new(vec![Field::new(
            "sum(li_one.l_extendedprice)",
            DataType::Decimal128(25, 2),
            true,
        )]));
        let out = coerce_shim_batch(batch, &declared).unwrap();
        assert_eq!(
            out.schema().field(0).data_type(),
            &DataType::Decimal128(25, 2)
        );
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        // 20506615920.80 at scale 2 = 2050661592080 exactly (53-bit mantissa
        // covers ~15 significant digits; this sum has 12)
        assert_eq!(col.value(0), 2050661592080i128);
    }

    #[test]
    fn matching_types_pass_through_unchanged() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Float64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float64Array::from(vec![1.5]))],
        )
        .unwrap();
        let out = coerce_shim_batch(batch, &schema).unwrap();
        assert_eq!(out.column(0).len(), 1);
    }

    #[test]
    fn utf8_shim_output_coerces_to_declared_utf8view() {
        use datafusion::arrow::array::{StringArray, StringViewArray};
        let got_schema = Arc::new(Schema::new(vec![Field::new(
            "l_returnflag",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            got_schema,
            vec![Arc::new(StringArray::from(vec!["A", "R"]))],
        )
        .unwrap();
        let declared = Arc::new(Schema::new(vec![Field::new(
            "l_returnflag",
            DataType::Utf8View,
            true,
        )]));
        let out = coerce_shim_batch(batch, &declared).unwrap();
        assert_eq!(out.schema().field(0).data_type(), &DataType::Utf8View);
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(col.value(0), "A");
        assert_eq!(col.value(1), "R");
    }

    #[test]
    fn int32_shim_output_coerces_to_declared_int64() {
        use datafusion::arrow::array::{Int32Array, Int64Array};
        let got_schema = Arc::new(Schema::new(vec![Field::new(
            "count_order",
            DataType::Int32,
            true,
        )]));
        let batch =
            RecordBatch::try_new(got_schema, vec![Arc::new(Int32Array::from(vec![5, 6]))]).unwrap();
        let declared = Arc::new(Schema::new(vec![Field::new(
            "count_order",
            DataType::Int64,
            true,
        )]));
        let out = coerce_shim_batch(batch, &declared).unwrap();
        assert_eq!(out.schema().field(0).data_type(), &DataType::Int64);
        let col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(col.value(0), 5);
        assert_eq!(col.value(1), 6);
    }
}
