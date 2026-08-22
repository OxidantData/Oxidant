//! SPIKE (issue #118): `ml_predict(<model uri>, f1, …, fk)` — ONNX scoring as a scalar UDF.
//!
//! Three functions are registered so the spike can measure the thing it set out to measure:
//!
//! | function | strategy | returns |
//! |---|---|---|
//! | `ml_predict(uri, f…)` | one tensor per RecordBatch | `DOUBLE` — positive-class probability, or the model's single output |
//! | `ml_predict_class(uri, f…)` | one tensor per RecordBatch | `BIGINT` — class label, `NULL` for models with no label output |
//! | `ml_predict_rowwise(uri, f…)` | one tensor **per row** | `DOUBLE` — same values as `ml_predict` |
//!
//! `ml_predict_rowwise` exists only as the benchmark's control arm; it is not an API proposal.
//! See `docs/spikes/ml-predict.md` for the rows/sec it costs.
//!
//! **Null handling.** ONNX has no null. A row with any null feature is not scored at all — the
//! feature slot is zero-filled to keep the tensor dense and the *output* for that row is NULL.
//! That is the conservative reading of Spark's null propagation, and it avoids the alternative
//! failure mode where a zero silently means "average feature value" to the model.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Float64Builder, Int64Builder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, plan_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

/// Register `ml_predict`, `ml_predict_class`, and the `ml_predict_rowwise` benchmark control.
pub fn register(ctx: &SessionContext) {
    // The engine's object store backs `s3://` model URIs; installing here (rather than in
    // `Engine::new_inner`) keeps the whole spike inside this module.
    crate::ml_blob_source::install();
    ctx.register_udf(ScalarUDF::from(MlPredict::new(
        "ml_predict",
        Strategy::Batch,
        Output::Score,
    )));
    ctx.register_udf(ScalarUDF::from(MlPredict::new(
        "ml_predict_class",
        Strategy::Batch,
        Output::Label,
    )));
    ctx.register_udf(ScalarUDF::from(MlPredict::new(
        "ml_predict_rowwise",
        Strategy::RowWise,
        Output::Score,
    )));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Strategy {
    /// Stack the whole RecordBatch into one `[rows, k]` tensor: one tract call per batch.
    Batch,
    /// One `[1, k]` tensor per row: `rows` tract calls per batch.
    RowWise,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Output {
    Score,
    Label,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct MlPredict {
    name: &'static str,
    signature: Signature,
    strategy: Strategy,
    output: Output,
}

impl MlPredict {
    fn new(name: &'static str, strategy: Strategy, output: Output) -> Self {
        Self {
            name,
            // Variadic and heterogeneous (one string + N numerics), so the coercion is ours.
            signature: Signature::user_defined(Volatility::Stable),
            strategy,
            output,
        }
    }
}

impl ScalarUDFImpl for MlPredict {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(match self.output {
            Output::Score => DataType::Float64,
            Output::Label => DataType::Int64,
        })
    }

    /// Model URI as `Utf8`, every feature as `Float64`. Casting features to `Float64` here (and
    /// down to `f32` at the tensor boundary) means an `INT`/`DECIMAL` column is accepted without
    /// the caller writing casts, which matters because feature columns are rarely all doubles.
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 {
            return plan_err!(
                "{}(model_uri, feature, …) needs a model URI and at least one feature",
                self.name
            );
        }
        let mut coerced = vec![DataType::Utf8];
        coerced.extend(vec![DataType::Float64; arg_types.len() - 1]);
        Ok(coerced)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let uri = model_uri(&args.args, self.name)?;
        let model = oxidant_ml::cache::get(&uri)
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

        let k = args.args.len() - 1;
        if k != model.n_features {
            return exec_err!(
                "{}: model `{uri}` takes {} features, got {k}",
                self.name,
                model.n_features
            );
        }

        // Row-major `[rows, k]`, plus the mask of rows that had a null feature.
        let columns: Vec<ArrayRef> = args.args[1..]
            .iter()
            .map(|arg| arg.to_array(rows))
            .collect::<Result<_>>()?;
        let mut features = vec![0f32; rows * k];
        let mut scorable = vec![true; rows];
        for (j, column) in columns.iter().enumerate() {
            let values = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Internal(format!(
                        "{}: feature {j} did not coerce to Float64",
                        self.name
                    ))
                })?;
            for row in 0..rows {
                if values.is_null(row) {
                    scorable[row] = false;
                } else {
                    features[row * k + j] = values.value(row) as f32;
                }
            }
        }

        let predictions = match self.strategy {
            Strategy::Batch => model.predict_batch(&features, rows),
            Strategy::RowWise => model.predict_rowwise(&features, rows),
        }
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

        let array: ArrayRef = match self.output {
            Output::Score => {
                let mut builder = Float64Builder::with_capacity(rows);
                for (row, score) in predictions.scores.iter().enumerate() {
                    builder.append_option(scorable[row].then_some(*score));
                }
                Arc::new(builder.finish())
            }
            Output::Label => {
                let mut builder = Int64Builder::with_capacity(rows);
                match &predictions.labels {
                    // A model with no label output (e.g. the torch MLP) yields all-NULL rather
                    // than an error, so `SELECT ml_predict(...), ml_predict_class(...)` works
                    // uniformly across model kinds.
                    None => builder.append_nulls(rows),
                    Some(labels) => {
                        for (row, label) in labels.iter().enumerate() {
                            builder.append_option(scorable[row].then_some(*label));
                        }
                    }
                }
                Arc::new(builder.finish())
            }
        };
        Ok(ColumnarValue::Array(array))
    }
}

/// The model URI must be a literal: it is the cache key, and it is read once per batch rather
/// than once per row. A column of URIs would mean a different model per row, which the batched
/// strategy cannot express at all.
fn model_uri(args: &[ColumnarValue], name: &str) -> Result<String> {
    match args.first() {
        Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(uri))))
        | Some(ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(uri))))
        | Some(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(uri)))) => Ok(uri.clone()),
        Some(ColumnarValue::Scalar(_)) => {
            exec_err!("{name}: model URI must be a non-null string literal")
        }
        _ => exec_err!("{name}: model URI must be a constant, not a column"),
    }
}
