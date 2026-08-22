//! SPIKE (issue #118): model lifecycle numbers — load time, cache behaviour, memory footprint.
//!
//! `ml-lifecycle <model-uri>…` loads each URI cold, then hot, and reports what the cache did.
//! Resident-set size is sampled around the loads so the report can state a real per-model
//! memory cost rather than quoting the serialized size and calling it a footprint.

use std::time::Instant;

/// Current process RSS in bytes, via `ps`. Crude, but it is the only number that captures
/// tract's *decompiled* form — the model's compiled plan, not the ONNX bytes we fetched.
fn rss_bytes() -> u64 {
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(|kb| kb * 1024)
        .unwrap_or(0)
}

fn main() {
    let uris: Vec<String> = std::env::args().skip(1).collect();
    if uris.is_empty() {
        eprintln!("usage: ml-lifecycle <model-uri>…");
        std::process::exit(2);
    }
    println!(
        "remote blob source installed: {}",
        oxidant_ml::store::remote_source().is_some()
    );
    println!("baseline RSS: {:.1} MiB", rss_bytes() as f64 / 1048576.0);

    for uri in &uris {
        oxidant_ml::cache::clear();
        let before = rss_bytes();
        let started = Instant::now();
        let model = match oxidant_ml::cache::get(uri) {
            Ok(m) => m,
            Err(e) => {
                println!("\n{uri}\n  LOAD FAILED: {e}");
                continue;
            }
        };
        let cold = started.elapsed();
        let after = rss_bytes();

        let hot_started = Instant::now();
        let _ = oxidant_ml::cache::get(uri).expect("hot load");
        let hot = hot_started.elapsed();

        println!("\n{uri}");
        println!(
            "  cold load    : {cold:?}   (fetch + parse + tract optimize + plan)\n\
             \x20 hot lookup   : {hot:?}   (cache hit, TTL-suppressed probe)\n\
             \x20 onnx bytes   : {}\n\
             \x20 RSS delta    : {:+.2} MiB\n\
             \x20 features     : {}, outputs {:?}\n\
             \x20 rewrites     : {:?}",
            model.model_bytes,
            (after as i64 - before as i64) as f64 / 1048576.0,
            model.n_features,
            model.output_signature,
            model.rewrites,
        );
    }
    println!("\ncache: {:?}", oxidant_ml::cache::stats());
}
