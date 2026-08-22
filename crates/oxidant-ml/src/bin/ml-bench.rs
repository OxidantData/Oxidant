//! SPIKE (issue #118): pure-inference benchmark — batch vs per-row, no SQL engine involved.
//!
//! `cargo run --profile release-ci -p oxidant-ml --bin ml-bench -- <rows.csv> <model.onnx> [batch…]`
//!
//! Isolates tract's cost from the engine's, so the SQL numbers can be attributed. The row-wise
//! arm is the same [`oxidant_ml::OnnxModel::predict_rowwise`] the `ml_predict_rowwise` UDF calls.

use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (rows_csv, model_path) = match args.as_slice() {
        [r, m, ..] => (r.clone(), m.clone()),
        _ => {
            eprintln!("usage: ml-bench <rows.csv> <model.onnx> [batch_size…]");
            std::process::exit(2);
        }
    };
    let batches: Vec<usize> = if args.len() > 2 {
        args[2..].iter().filter_map(|a| a.parse().ok()).collect()
    } else {
        vec![1024, 8192]
    };

    let rows = oxidant_ml::probe::read_csv(&rows_csv);
    let model = oxidant_ml::cache::get(&model_path).expect("load model");
    let k = model.n_features;
    println!(
        "model {model_path}: {k} features, {} bytes, outputs {:?}",
        model.model_bytes, model.output_signature
    );
    println!("rows: {} x {k}", rows.len());
    let flat: Vec<f32> = rows.iter().flatten().copied().collect();
    let n = flat.len() / k;

    // Warm the plan (first call touches lazily-initialized tract scratch buffers).
    let _ = model.predict_batch(&flat[..k.min(flat.len())], 1);

    let started = Instant::now();
    let out = model.predict_rowwise(&flat, n).expect("rowwise");
    let rowwise = started.elapsed();
    println!(
        "rowwise      : {:>9.3?}  {:>12.0} rows/sec",
        rowwise,
        n as f64 / rowwise.as_secs_f64()
    );

    for chunk in batches {
        let started = Instant::now();
        let mut scored = 0usize;
        let mut checksum = 0f64;
        for offset in (0..n).step_by(chunk) {
            let len = chunk.min(n - offset);
            let p = model
                .predict_batch(&flat[offset * k..(offset + len) * k], len)
                .expect("batch");
            checksum += p.scores.iter().sum::<f64>();
            scored += len;
        }
        let elapsed = started.elapsed();
        println!(
            "batch={chunk:<6}: {:>9.3?}  {:>12.0} rows/sec  ({:.1}x rowwise)  checksum {:.4}",
            elapsed,
            scored as f64 / elapsed.as_secs_f64(),
            rowwise.as_secs_f64() / elapsed.as_secs_f64(),
            checksum / scored as f64,
        );
        // The two strategies must agree, or the speedup is measuring the wrong thing.
        assert!(
            (checksum / scored as f64 - out.scores.iter().sum::<f64>() / n as f64).abs() < 1e-9,
            "batch={chunk} disagrees with rowwise"
        );
    }
}
