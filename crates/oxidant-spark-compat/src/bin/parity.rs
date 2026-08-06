//! `oxidant-parity` — run Spark's golden SQL corpus through oxidant and emit the parity scoreboard.
//!
//! Usage:
//!   oxidant-parity golden [--filter <substr>] [--out-dir <dir>]
//!     Replay the corpus, write `<out-dir>/parity.json` + `parity.md`, print the headline.
//!   oxidant-parity file <name.sql.out>
//!     Replay a single golden file and print its per-block verdicts (debugging).

use oxidant_spark_compat::report::{bucket_key, CorpusReport};
use oxidant_spark_compat::runner;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cmd = args.first().map(String::as_str).unwrap_or("golden");

    match cmd {
        "golden" => golden(&args[1..]).await,
        "ratchet" => ratchet(&args[1..]).await,
        "file" => file(&args[1..]).await,
        other => {
            eprintln!("unknown command: {other}\nusage: oxidant-parity [golden|ratchet|file] ...");
            std::process::exit(2);
        }
    }
}

async fn golden(args: &[String]) {
    let filter = flag(args, "--filter");
    let out_dir = flag(args, "--out-dir").unwrap_or_else(|| "parity".to_string());

    eprintln!(
        "Replaying Spark golden corpus through oxidant (filter: {:?}) …",
        filter
    );
    let report = runner::run_corpus(filter.as_deref()).await;

    write_artifacts(&out_dir, &report);
    println!(
        "\n=== Oxidant ↔ Spark SQL parity ({}) ===",
        report.spark_version
    );
    println!(
        "strict   : {:>6.1}%  ({}/{} queries)",
        report.strict_pct(),
        report.strict_pass,
        report.blocks_total
    );
    println!(
        "semantic : {:>6.1}%  ({}/{} queries)",
        report.semantic_pct(),
        report.semantic_pass,
        report.blocks_total
    );
    println!(
        "files    : {} total, {} skipped",
        report.files_total, report.files_skipped
    );
    println!("\nwrote {out_dir}/{{parity.json,report.md,parity.html,scoreboard.json}}");
}

/// Run the corpus and fail (exit 1) if parity dropped below the committed baseline. This is the
/// CI gate: oxidant can only get *more* Spark-compatible, never less. Improvements should be locked
/// in by re-baselining (`oxidant-parity golden` → commit `parity/baseline.json`).
async fn ratchet(args: &[String]) {
    let baseline_path = flag(args, "--baseline").unwrap_or_else(|| "parity/baseline.json".into());
    let out_dir = flag(args, "--out-dir").unwrap_or_else(|| "parity".to_string());

    #[derive(serde::Deserialize)]
    struct Baseline {
        strict_pass: usize,
        semantic_pass: usize,
        blocks_total: usize,
    }
    let base: Baseline = serde_json::from_str(
        &std::fs::read_to_string(&baseline_path)
            .unwrap_or_else(|_| panic!("read baseline {baseline_path}")),
    )
    .expect("parse baseline json");

    let report = runner::run_corpus(None).await;
    write_artifacts(&out_dir, &report);

    println!(
        "parity: strict {} (base {}), semantic {} (base {}), blocks {} (base {})",
        report.strict_pass,
        base.strict_pass,
        report.semantic_pass,
        base.semantic_pass,
        report.blocks_total,
        base.blocks_total
    );

    let mut failed = false;
    if report.blocks_total != base.blocks_total {
        eprintln!(
            "✗ corpus size changed ({} vs baseline {}) — re-baseline if the Spark tag moved",
            report.blocks_total, base.blocks_total
        );
        failed = true;
    }
    if report.strict_pass < base.strict_pass {
        eprintln!(
            "✗ strict parity regressed: {} < {}",
            report.strict_pass, base.strict_pass
        );
        failed = true;
    }
    if report.semantic_pass < base.semantic_pass {
        eprintln!(
            "✗ semantic parity regressed: {} < {}",
            report.semantic_pass, base.semantic_pass
        );
        failed = true;
    }
    if failed {
        std::process::exit(1);
    }
    let gained =
        (report.strict_pass - base.strict_pass) + (report.semantic_pass - base.semantic_pass);
    if gained > 0 {
        println!("✓ parity held or improved (+{gained} passing) — remember to re-baseline.");
    } else {
        println!("✓ parity held at baseline.");
    }
}

/// Write the four artifacts every run produces: full JSON, triage markdown, the self-contained
/// HTML scoreboard, and the compact scoreboard JSON the site reads.
fn write_artifacts(out_dir: &str, report: &CorpusReport) {
    std::fs::create_dir_all(out_dir).ok();
    std::fs::write(format!("{out_dir}/parity.json"), report.to_json()).expect("write parity.json");
    std::fs::write(format!("{out_dir}/report.md"), report.to_markdown()).expect("write report.md");
    std::fs::write(format!("{out_dir}/parity.html"), report.to_html()).expect("write parity.html");
    std::fs::write(
        format!("{out_dir}/scoreboard.json"),
        report.to_scoreboard_json(),
    )
    .expect("write scoreboard.json");
}

async fn file(args: &[String]) {
    let Some(name) = args.first() else {
        eprintln!("usage: oxidant-parity file <name.sql.out>");
        std::process::exit(2);
    };
    let report = runner::run_file(name).await;
    if let Some(reason) = &report.skipped {
        println!("{name}: SKIPPED ({reason})");
        return;
    }
    println!(
        "{}: strict {}/{}, semantic {}/{}",
        report.file, report.strict_pass, report.total, report.semantic_pass, report.total
    );
    for (k, n) in &report.buckets {
        println!("  {k}: {n}");
    }
    for f in &report.failures {
        println!("  [{}] {} -- {}", f.bucket, f.sql, f.detail);
    }
    let _ = bucket_key; // keep import used if failures empty
}

/// Tiny `--flag value` extractor.
fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}
