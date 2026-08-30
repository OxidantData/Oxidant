//! `oxidant-parity` — run Spark's golden SQL corpus through oxidant and emit the parity scoreboard.
//!
//! Usage:
//!   oxidant-parity golden [--corpus spark|databricks] [--filter <substr>] [--out-dir <dir>]
//!     Replay the corpus, write `<out-dir>/parity.json` + `parity.md`, print the headline.
//!   oxidant-parity ratchet [--corpus spark|databricks] [--baseline <path>] [--out-dir <dir>]
//!     Replay the corpus and fail if parity dropped below the committed baseline.
//!   oxidant-parity file [--corpus spark|databricks] <name.sql.out>
//!     Replay a single golden file and print its per-block verdicts (debugging).
//!   oxidant-parity functions [--markdown] [--json <path>]
//!     Diff oxidant's live function registry against the Databricks builtin-function surface
//!     (`databricks-functions.json`). Prints the headline + per-category rollup; `--markdown`
//!     emits the full matrix on stdout (this is how `docs/databricks-functions.md` is generated).
//!
//! `--corpus` defaults to `spark` (the vendored Apache Spark `sql-tests` corpus); the
//! `databricks` corpus is the authored Databricks SQL corpus under `databricks-tests/`,
//! scored through the same pipeline with its own baseline/artifact defaults.

use oxidant_spark_compat::report::{bucket_key, CorpusReport};
use oxidant_spark_compat::runner;
use oxidant_spark_compat::Corpus;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cmd = args.first().map(String::as_str).unwrap_or("golden");

    match cmd {
        "golden" => golden(&args[1..]).await,
        "ratchet" => ratchet(&args[1..]).await,
        "file" => file(&args[1..]).await,
        "functions" => functions(&args[1..]).await,
        other => {
            eprintln!(
                "unknown command: {other}\nusage: oxidant-parity [golden|ratchet|file|functions] ..."
            );
            std::process::exit(2);
        }
    }
}

/// Extract the `--corpus` flag (default: the vendored Spark corpus).
fn corpus(args: &[String]) -> Corpus {
    match flag(args, "--corpus") {
        None => Corpus::Spark,
        Some(name) => Corpus::from_name(&name)
            .unwrap_or_else(|| panic!("unknown --corpus {name:?} (expected spark|databricks)")),
    }
}

async fn golden(args: &[String]) {
    let corpus = corpus(args);
    let filter = flag(args, "--filter");
    let out_dir = flag(args, "--out-dir").unwrap_or_else(|| corpus.default_out_dir().to_string());

    eprintln!(
        "Replaying {} golden corpus through oxidant (filter: {:?}) …",
        corpus.name(),
        filter
    );
    let report = runner::run_corpus(corpus, filter.as_deref()).await;

    write_artifacts(&out_dir, &report);
    println!(
        "\n=== Oxidant ↔ Spark SQL parity ({}, {} corpus) ===",
        report.spark_version,
        corpus.name()
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
    let corpus = corpus(args);
    let baseline_path =
        flag(args, "--baseline").unwrap_or_else(|| corpus.default_baseline().into());
    let out_dir = flag(args, "--out-dir").unwrap_or_else(|| corpus.default_out_dir().into());

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

    let report = runner::run_corpus(corpus, None).await;
    write_artifacts(&out_dir, &report);

    println!(
        "parity ({} corpus): strict {} (base {}), semantic {} (base {}), blocks {} (base {})",
        corpus.name(),
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
            "✗ corpus size changed ({} vs baseline {}) — re-baseline if the corpus tag moved",
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
    let corpus = corpus(args);
    // First non-flag argument, skipping `--flag value` pairs.
    let mut name: Option<&str> = None;
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--corpus" {
            i += 2;
            continue;
        }
        if !args[i].starts_with("--") {
            name = Some(&args[i]);
            break;
        }
        i += 1;
    }
    let Some(name) = name else {
        eprintln!("usage: oxidant-parity file [--corpus spark|databricks] <name.sql.out>");
        std::process::exit(2);
    };
    let report = runner::run_file(corpus, name).await;
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

/// Diff oxidant's live function registry against the Databricks builtin-function surface.
///
/// Unlike `golden`/`ratchet` this replays no SQL — it boots one engine, reads the registry that
/// answers `SHOW FUNCTIONS`, and scores it against the checked-in `databricks-functions.json`
/// catalog. Cheap enough to run on every change to `spark_functions/`.
async fn functions(args: &[String]) {
    let report = oxidant_spark_compat::functions::run().await;

    if let Some(path) = flag(args, "--json") {
        std::fs::write(&path, report.to_json()).unwrap_or_else(|e| panic!("write {path}: {e}"));
        eprintln!("wrote {path}");
    }

    // `--markdown` writes the matrix to stdout and nothing else, so it can be redirected
    // straight into `docs/databricks-functions.md`.
    if args.iter().any(|a| a == "--markdown") {
        print!("{}", report.to_markdown());
        return;
    }

    println!("\n=== Oxidant ↔ Databricks SQL function coverage ===");
    println!(
        "in scope : {:>6.1}%  ({}/{} functions registered, {} missing)",
        report.coverage_pct(),
        report.registered,
        report.in_scope,
        report.missing
    );
    println!(
        "surface  : {} documented Databricks functions; engine registry holds {}",
        report.documented, report.engine_registry_size
    );
    println!("\nout of scope:");
    for (reason, n) in &report.excluded {
        println!("  {reason:<16} {n:>4}");
    }
    println!("\nby category (registered / in scope):");
    for c in &report.categories {
        println!(
            "  {:<44} {:>3} / {:<3}{}",
            c.category,
            c.registered,
            c.in_scope,
            if c.missing.is_empty() {
                String::new()
            } else {
                format!("  missing: {}", c.missing.join(", "))
            }
        );
    }
}

/// Tiny `--flag value` extractor.
fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}
