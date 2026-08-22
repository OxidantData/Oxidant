//! A loaded, compiled ONNX model plus the two scoring strategies the spike benchmarks.
//!
//! SPIKE (issue #118).

use std::time::{Duration, Instant};

use oxidant_common::{Error, Result};
use tract_onnx::prelude::*;
// `concretize` lives on the `Factoid` trait, which the prelude does not re-export.
use tract_onnx::tract_hir::infer::Factoid;

use crate::compat::{self, Rewrites};

/// A compiled tract plan with the metadata the UDF needs to interpret its outputs.
pub struct OnnxModel {
    plan: TypedSimplePlan<TypedModel>,
    /// Number of input features (the model's input fact's last dimension).
    pub n_features: usize,
    /// Output index + column to read as "the score" (see [`OnnxModel::classify_outputs`]).
    score: ScoreSlot,
    /// Output index of an integer class label, when the graph emits one **and** we trust it.
    /// `None` for binary tree ensembles, where tract's label is wrong — see `binary_labels`.
    label_output: Option<usize>,
    /// Class labels to argmax from the probability columns ourselves, for binary tree
    /// ensembles. Set exactly when tract's own label output cannot be trusted.
    binary_labels: Option<Vec<i64>>,
    /// Human-readable output signature, for `EXPLAIN`-ish reporting and the spike report.
    pub output_signature: Vec<String>,
    /// Serialized model size in bytes (what we fetched).
    pub model_bytes: usize,
    /// Wall time spent turning those bytes into `plan`.
    pub compile_time: Duration,
    /// tract-compat rewrites this model needed.
    pub rewrites: Rewrites,
}

#[derive(Debug, Clone, Copy)]
struct ScoreSlot {
    output: usize,
    /// Column within a rank-2 `[rows, cols]` output; `None` for a rank-1 output.
    column: Option<usize>,
}

/// One batch's worth of predictions.
#[derive(Debug, Clone, PartialEq)]
pub struct Predictions {
    /// Positive-class probability (classifier) or the single regression/score output.
    pub scores: Vec<f64>,
    /// Argmax class label, when the graph emits one.
    pub labels: Option<Vec<i64>>,
}

impl std::fmt::Debug for OnnxModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OnnxModel")
            .field("n_features", &self.n_features)
            .field("outputs", &self.output_signature)
            .field("model_bytes", &self.model_bytes)
            .finish()
    }
}

/// The symbolic batch dimension. One compiled plan serves every RecordBatch size; without this
/// tract would bake the batch size into the plan and we would recompile per distinct batch.
const BATCH_SYMBOL: &str = "ox_rows";

impl OnnxModel {
    /// Parse, patch, and compile `bytes` into a runnable plan with a symbolic batch dimension.
    pub fn load(bytes: &[u8]) -> Result<Self> {
        let started = Instant::now();
        let onnx = tract_onnx::onnx();
        let mut proto = onnx
            .proto_model_for_read(&mut std::io::Cursor::new(bytes))
            .map_err(|e| Error::Plan(format!("ml_predict: not a readable ONNX model: {e}")))?;
        // Apply the tract-compat rewrites at the protobuf level, before tract's parser can
        // reject the graph (see `compat`).
        let rewrites = compat::patch_for_tract(&mut proto);

        let inference = onnx
            .model_for_proto_model(&proto)
            .map_err(|e| Error::Plan(format!("ml_predict: tract cannot build this graph: {e}")))?;

        let n_features = input_feature_count(&inference)?;
        let rows = inference.symbols.sym(BATCH_SYMBOL);
        let typed = inference
            .with_input_fact(0, f32::fact([rows.to_dim(), n_features.to_dim()]).into())
            .map_err(|e| Error::Plan(format!("ml_predict: input fact: {e}")))?
            .into_optimized()
            .map_err(|e| Error::Plan(format!("ml_predict: tract optimize: {e}")))?;

        let output_signature = typed
            .output_outlets()
            .map(|outlets| {
                outlets
                    .iter()
                    .map(|o| match typed.outlet_fact(*o) {
                        Ok(f) => format!("{:?}:{:?}", f.datum_type, f.shape),
                        Err(e) => format!("<{e}>"),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let (score, tract_label_output) = Self::classify_outputs(&typed)?;
        // tract computes a binary TreeEnsembleClassifier's label with an argmax over the
        // *pre-slice* scores — the `[n, 2]` tensor whose second column carries only
        // `base_values`, with no tree contribution — instead of over the `[1 - p, p]` it
        // actually returns. The probabilities are exact; the label flips whenever
        // `p < sigmoid(base)`. We therefore drop tract's label output for these graphs and take
        // the argmax over the probabilities ourselves. (tract 0.22.0 and 0.23.5 both.)
        let binary_labels = rewrites.binary_class_labels.clone();
        let label_output = if binary_labels.is_some() {
            None
        } else {
            tract_label_output
        };

        let plan = typed
            .into_runnable()
            .map_err(|e| Error::Plan(format!("ml_predict: tract plan: {e}")))?;

        Ok(Self {
            plan,
            n_features,
            score,
            label_output,
            binary_labels,
            output_signature,
            model_bytes: bytes.len(),
            compile_time: started.elapsed(),
            rewrites,
        })
    }

    /// Decide which output is "the score" and which (if any) is the class label.
    ///
    /// The two export shapes this spike targets:
    /// * **torch MLP** — one `f32` output of shape `[rows, 1]`. Score = that value, no label.
    /// * **sklearn classifier** — `(label: i64[rows], probabilities: f32[rows, n_classes])`.
    ///   Score = the **last** probability column, which is the positive class for the binary
    ///   models here. Label = the `i64` output.
    ///
    /// Anything else (multi-output regressors, >2 classes) is out of scope for the spike and
    /// falls back to "first float output, last column" — which is a guess, and a reason the
    /// shipping API should return a struct rather than one scalar (see the report).
    fn classify_outputs(model: &TypedModel) -> Result<(ScoreSlot, Option<usize>)> {
        let outlets = model
            .output_outlets()
            .map_err(|e| Error::Plan(format!("ml_predict: outputs: {e}")))?;
        let mut score = None;
        let mut label = None;
        for (idx, outlet) in outlets.iter().enumerate() {
            let fact = model
                .outlet_fact(*outlet)
                .map_err(|e| Error::Plan(format!("ml_predict: output fact: {e}")))?;
            if fact.datum_type == i64::datum_type() {
                label.get_or_insert(idx);
            } else if fact.datum_type == f32::datum_type() && score.is_none() {
                let column = match fact.shape.rank() {
                    0 | 1 => None,
                    _ => Some(
                        usize::try_from(fact.shape[1].to_i64().map_err(|e| {
                            Error::Plan(format!("ml_predict: non-constant output width: {e}"))
                        })?)
                        .unwrap_or(1)
                        .saturating_sub(1),
                    ),
                };
                score = Some(ScoreSlot {
                    output: idx,
                    column,
                });
            }
        }
        let score = score.ok_or_else(|| {
            Error::Plan("ml_predict: model has no float output to score from".into())
        })?;
        Ok((score, label))
    }

    /// **Strategy (b): batched.** Score `rows` records in a single tract call. `features` is
    /// row-major `rows * n_features` values.
    pub fn predict_batch(&self, features: &[f32], rows: usize) -> Result<Predictions> {
        if rows * self.n_features != features.len() {
            return Err(Error::Plan(format!(
                "ml_predict: {} features for {rows} rows x {} features",
                features.len(),
                self.n_features
            )));
        }
        if rows == 0 {
            return Ok(Predictions {
                scores: vec![],
                labels: self.emits_labels().then(Vec::new),
            });
        }
        let input = Tensor::from_shape(&[rows, self.n_features], features)
            .map_err(|e| Error::Plan(format!("ml_predict: input tensor: {e}")))?;
        let outputs = self
            .plan
            .run(tvec!(input.into_tvalue()))
            .map_err(|e| Error::Execution(format!("ml_predict: inference failed: {e}")))?;
        self.read_outputs(&outputs, rows)
    }

    /// **Strategy (a): per-row.** Same result as [`OnnxModel::predict_batch`], one tract call
    /// per row. Exists only so the spike can measure the difference; never ship this.
    pub fn predict_rowwise(&self, features: &[f32], rows: usize) -> Result<Predictions> {
        let k = self.n_features;
        if rows * k != features.len() {
            return Err(Error::Plan(format!(
                "ml_predict: {} features for {rows} rows x {k} features",
                features.len()
            )));
        }
        let mut scores = Vec::with_capacity(rows);
        let mut labels = self.emits_labels().then(|| Vec::with_capacity(rows));
        for row in 0..rows {
            let one = self.predict_batch(&features[row * k..(row + 1) * k], 1)?;
            scores.push(one.scores[0]);
            if let (Some(acc), Some(l)) = (labels.as_mut(), one.labels) {
                acc.push(l[0]);
            }
        }
        Ok(Predictions { scores, labels })
    }

    /// Whether [`Predictions::labels`] will be populated.
    pub fn emits_labels(&self) -> bool {
        self.label_output.is_some() || self.binary_labels.is_some()
    }

    fn read_outputs(&self, outputs: &[TValue], rows: usize) -> Result<Predictions> {
        let score_tensor = outputs.get(self.score.output).ok_or_else(|| {
            Error::Execution("ml_predict: model produced fewer outputs than planned".into())
        })?;
        let flat = score_tensor
            .as_slice::<f32>()
            .map_err(|e| Error::Execution(format!("ml_predict: score output: {e}")))?;
        let width = flat.len() / rows.max(1);
        let column = self.score.column.unwrap_or(0).min(width.saturating_sub(1));
        let scores = (0..rows).map(|r| flat[r * width + column] as f64).collect();

        if let Some(classes) = &self.binary_labels {
            // Argmax over the two probability columns tract actually returned.
            let labels = (0..rows)
                .map(|r| {
                    let neg = flat[r * width];
                    let pos = flat[r * width + width.saturating_sub(1)];
                    classes[usize::from(pos > neg)]
                })
                .collect();
            return Ok(Predictions {
                scores,
                labels: Some(labels),
            });
        }
        let labels = match self.label_output {
            None => None,
            Some(idx) => {
                let t = outputs
                    .get(idx)
                    .ok_or_else(|| Error::Execution("ml_predict: label output missing".into()))?;
                Some(
                    t.as_slice::<i64>()
                        .map_err(|e| Error::Execution(format!("ml_predict: label output: {e}")))?
                        .to_vec(),
                )
            }
        };
        Ok(Predictions { scores, labels })
    }
}

/// Read the feature count off the model's declared input fact.
fn input_feature_count(model: &InferenceModel) -> Result<usize> {
    let outlet = *model
        .input_outlets()
        .map_err(|e| Error::Plan(format!("ml_predict: inputs: {e}")))?
        .first()
        .ok_or_else(|| Error::Plan("ml_predict: model has no input".into()))?;
    let fact = model
        .outlet_fact(outlet)
        .map_err(|e| Error::Plan(format!("ml_predict: input fact: {e}")))?;
    if fact.shape.is_open() {
        return Err(Error::Plan(
            "ml_predict: model input shape is open — export with a fixed feature count".into(),
        ));
    }
    let dims: Vec<Option<i64>> = fact
        .shape
        .dims()
        .map(|d| d.concretize().and_then(|t| t.to_i64().ok()))
        .collect();
    // `[rows, features]`: rows is free (a symbol or `None`), features must be a constant.
    match dims.as_slice() {
        [_, Some(k)] if *k > 0 => Ok(*k as usize),
        [_, None] => Err(Error::Plan(
            "ml_predict: model's feature count is not a fixed dimension".into(),
        )),
        other => Err(Error::Plan(format!(
            "ml_predict: expected a rank-2 [rows, features] input, got shape {other:?}"
        ))),
    }
}
