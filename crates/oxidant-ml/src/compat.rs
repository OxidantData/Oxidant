//! ONNX graph rewrites that make real sklearn / torch exports loadable by tract.
//!
//! SPIKE (issue #118). Every rewrite here is a **workaround for a specific tract limitation**
//! that we verified by reading tract's source; each one is documented with what tract checks,
//! why the rewrite is semantics-preserving, and what the upstream fix would be.

use tract_onnx::pb::{AttributeProto, ModelProto, NodeProto};

/// What [`patch_for_tract`] had to change, for the spike report / logs.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Rewrites {
    /// `TreeEnsembleClassifier` nodes whose `base_values` we padded to `n_classes`.
    pub base_values_padded: usize,
    /// Class labels of a binary `TreeEnsembleClassifier`, when we found exactly one.
    ///
    /// Their presence is the signal that tract's own label output for this graph is
    /// **untrustworthy** and must be recomputed from the probabilities — see
    /// [`pad_base_values`] and `OnnxModel::read_outputs`.
    pub binary_class_labels: Option<Vec<i64>>,
}

impl Rewrites {
    pub fn is_empty(&self) -> bool {
        self.base_values_padded == 0 && self.binary_class_labels.is_none()
    }
}

/// Rewrite `proto` in place so tract can build it. Returns what changed.
pub fn patch_for_tract(proto: &mut ModelProto) -> Rewrites {
    let mut rewrites = Rewrites::default();
    let Some(graph) = proto.graph.as_mut() else {
        return rewrites;
    };
    let mut binary_nodes = 0;
    for node in graph.node.iter_mut() {
        if node.op_type != "TreeEnsembleClassifier" {
            continue;
        }
        if let Some(labels) = binary_class_labels(node) {
            binary_nodes += 1;
            rewrites.binary_class_labels = Some(labels);
            if pad_base_values(node) {
                rewrites.base_values_padded += 1;
            }
        }
    }
    // Two binary classifiers in one graph would make "the" label ambiguous; don't guess.
    if binary_nodes > 1 {
        rewrites.binary_class_labels = None;
    }
    rewrites
}

/// Pad a binary `TreeEnsembleClassifier`'s `base_values` from length 1 to `n_classes`.
///
/// **The gap.** skl2onnx exports a binary `GradientBoostingClassifier` as a *single* series of
/// trees plus one initial raw score, so it emits `base_values = [init]` (length 1) alongside
/// `classlabels_int64s = [0, 1]` (length 2). tract's ONNX parser reads the attribute with
/// `get_vec_attr_opt::<f32>(node, "base_values", ensemble.n_classes())`, which hard-requires
/// `base_values.len() == n_classes`, and rejects the model:
///
/// ```text
/// Node TreeEnsembleClassifier, attribute 'base_values': expected length 1 (or undefined), got 2
/// ```
///
/// (The message reads backwards — "expected {actual}, got {expected}" — which is why this
/// looks at first glance like the *model* has 2 base values. It has 1; tract wants 2.)
///
/// **Why padding is safe.** tract already implements the binary layout correctly. It detects
/// `binary_result_layout` (≤2 class labels and every leaf's `class_id` is 0), broadcasts
/// `base_values` into rank 2, adds it to the ensemble's `[n, n_classes]` score, applies the
/// `LOGISTIC` post-transform, then **slices column 0** and emits `[1 - p, p]`. Only column 0
/// is ever read, so any value we put in the padded slots is discarded. We repeat the real base
/// value rather than padding with zero purely so the pre-slice tensor stays interpretable when
/// dumping the graph.
///
/// **Upstream fix.** tract should compute `binary_result_layout` before parsing `base_values`
/// and accept length 1 in that case. Verified identical behaviour on tract 0.22.0 and 0.23.5.
fn pad_base_values(node: &mut NodeProto) -> bool {
    let Some(base) = node.attribute.iter_mut().find(|a| a.name == "base_values") else {
        return false;
    };
    if base.floats.len() != 1 {
        return false;
    }
    let value = base.floats[0];
    base.floats.resize(2, value);
    true
}

/// The `[c0, c1]` class labels of a two-class `TreeEnsembleClassifier` whose every leaf targets
/// class 0 — i.e. exactly the shape that triggers tract's `binary_result_layout` path.
fn binary_class_labels(node: &NodeProto) -> Option<Vec<i64>> {
    let labels = node
        .attribute
        .iter()
        .find(|a| a.name == "classlabels_int64s")
        .map(|a| a.ints.clone())?;
    (labels.len() == 2 && all_leaves_target_class_zero(node)).then_some(labels)
}

/// True when every leaf in the ensemble contributes to class 0 — tract's `binary_result_layout`
/// condition, restated over the protobuf attributes instead of its packed leaf table.
fn all_leaves_target_class_zero(node: &NodeProto) -> bool {
    node.attribute
        .iter()
        .find(|a: &&AttributeProto| a.name == "class_ids")
        .is_some_and(|a| !a.ints.is_empty() && a.ints.iter().all(|&c| c == 0))
}
