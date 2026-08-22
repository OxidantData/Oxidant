//! Feasibility probe: what does tract accept, and does it agree with onnxruntime?
//!
//! SPIKE scaffolding (issue #118). Drives [`crate::OnnxModel`] outside SQL so the tract
//! findings can be reproduced without building the whole engine.

use tract_onnx::prelude::*;

use crate::OnnxModel;

/// Dump every node in the raw ONNX graph, then report whether tract can build and optimize it.
/// This is the "ops gap" report: it names the op that fails, and the ones that don't.
pub fn dump_graph(path: &str) {
    println!("\n=== raw graph: {path} ===");
    match tract_onnx::onnx().model_for_path(path) {
        Ok(raw) => {
            println!("  tract parsed unpatched: {} nodes", raw.nodes().len());
            for n in raw.nodes() {
                println!("    node `{}` op={}", n.name, n.op().name());
            }
        }
        Err(e) => println!("  tract PARSE FAILED (unpatched): {e}"),
    }
}

/// Load `path` through the full [`OnnxModel`] path (rewrites included) and score `rows`.
pub fn score(path: &str, rows: &[Vec<f32>]) {
    println!("\n=== {path} ===");
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) => {
            println!("  read failed: {e}");
            return;
        }
    };
    let model = match OnnxModel::load(&bytes) {
        Ok(m) => m,
        Err(e) => {
            println!("  LOAD FAILED: {e}");
            return;
        }
    };
    println!(
        "  loaded: {} features, outputs {:?}, {} bytes, compile {:?}, rewrites {:?}",
        model.n_features,
        model.output_signature,
        model.model_bytes,
        model.compile_time,
        model.rewrites
    );
    if rows.is_empty() {
        return;
    }
    let flat: Vec<f32> = rows.iter().flatten().copied().collect();
    match model.predict_batch(&flat, rows.len()) {
        Ok(p) => {
            println!("  batch  scores: {:?}", round6(&p.scores));
            if let Some(l) = &p.labels {
                println!("  batch  labels: {l:?}");
            }
            // Row-wise must agree with batched, or the benchmark is comparing two different
            // computations rather than two strategies for the same one.
            match model.predict_rowwise(&flat, rows.len()) {
                Ok(r) if r == p => println!("  rowwise: identical to batch ✅"),
                Ok(r) => println!("  rowwise DIFFERS: {:?}", round6(&r.scores)),
                Err(e) => println!("  rowwise failed: {e}"),
            }
        }
        Err(e) => println!("  PREDICT FAILED: {e}"),
    }
}

fn round6(v: &[f64]) -> Vec<f64> {
    v.iter().map(|x| (x * 1e6).round() / 1e6).collect()
}

/// Parse a headerless CSV of floats into rows.
pub fn read_csv(path: &str) -> Vec<Vec<f32>> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.split(',').filter_map(|c| c.trim().parse().ok()).collect())
        .collect()
}
