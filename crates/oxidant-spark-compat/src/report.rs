//! Aggregate per-block verdicts into a parity scoreboard: a machine-readable JSON (the CI
//! ratchet + the site scoreboard read this) and a human triage markdown.
//!
//! Two headline numbers are reported, because "parity" has two honest readings:
//! - **strict** — byte-for-byte identical to Spark's golden (schema + rows). The hard claim.
//! - **semantic** — right answer / right rejection, tolerating benign divergences (column-name
//!   spelling in the schema line, and "both engines reject this query" with different error
//!   text). The fair claim for a drop-in replacement.

use crate::classify::{Bucket, Verdict};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Stable kebab key for a bucket (used for JSON map keys + the ratchet).
pub fn bucket_key(b: Bucket) -> &'static str {
    match b {
        Bucket::Pass => "pass",
        Bucket::ErrorParity => "error-parity",
        Bucket::Ordering => "ordering",
        Bucket::SchemaOnly => "schema-only",
        Bucket::DecimalPrecision => "decimal-precision",
        Bucket::Datetime => "datetime",
        Bucket::NullSemantics => "null-semantics",
        Bucket::FunctionMissing => "function-missing",
        Bucket::ParserUnsupported => "parser-unsupported",
        Bucket::FeatureUnsupported => "feature-unsupported",
        Bucket::MissingRelation => "missing-relation",
        Bucket::EnginePanic => "engine-panic",
        Bucket::ExecError => "exec-error",
        Bucket::MissingError => "missing-error",
        Bucket::Correctness => "correctness",
        Bucket::Nondeterministic => "nondeterministic",
        Bucket::RequiresUdfRegistration => "requires-udf-registration",
    }
}

/// One non-passing block, kept for the triage backlog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Failure {
    pub bucket: String,
    pub sql: String,
    pub detail: String,
}

/// Per-file roll-up.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileReport {
    pub file: String,
    /// Set when the whole file was skipped (e.g. `requires-udf-registration`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped: Option<String>,
    pub total: usize,
    pub strict_pass: usize,
    pub semantic_pass: usize,
    pub buckets: BTreeMap<String, usize>,
    /// Up to a cap of non-passing blocks, for the triage report.
    pub failures: Vec<Failure>,
}

impl FileReport {
    pub fn skipped(file: &str, reason: &str) -> Self {
        FileReport {
            file: file.to_string(),
            skipped: Some(reason.to_string()),
            total: 0,
            strict_pass: 0,
            semantic_pass: 0,
            buckets: BTreeMap::new(),
            failures: Vec::new(),
        }
    }

    /// Build from the ordered per-block verdicts of a replayed file.
    pub fn from_verdicts(file: &str, verdicts: &[(String, Verdict)]) -> Self {
        let mut buckets: BTreeMap<String, usize> = BTreeMap::new();
        let mut strict_pass = 0;
        let mut semantic_pass = 0;
        let mut failures = Vec::new();
        // Per-file sample of actionable failures kept in the report. Capped so `parity.json`
        // (the committed baseline) stays a reviewable size — the per-file *bucket tallies* beside
        // it are uncapped, so no count is lost, only the examples.
        //
        // Raise it with `OXIDANT_PARITY_FAILURE_CAP` when auditing a re-baseline: with the default
        // 20, a file that gains failures past the cap shows the new rows only as a bucket delta,
        // with no SQL to inspect — which is exactly the position a reviewer is left in when asked
        // whether a rise is honest unmasking or a real regression.
        let failure_cap: usize = std::env::var("OXIDANT_PARITY_FAILURE_CAP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20);

        for (sql, v) in verdicts {
            *buckets.entry(bucket_key(v.bucket).to_string()).or_default() += 1;
            if v.bucket.is_strict_pass() {
                strict_pass += 1;
            }
            if v.bucket.is_semantic_pass() {
                semantic_pass += 1;
            }
            // Backlog = every strict-blocker, so it drives *both* scores. We exclude clean
            // `error-parity` (both engines reject — nothing to fix) but keep `schema-only` /
            // `ordering` (semantic passes that still block strict parity).
            let actionable = !v.bucket.is_strict_pass()
                && !matches!(
                    v.bucket,
                    crate::classify::Bucket::ErrorParity
                        | crate::classify::Bucket::Nondeterministic
                        | crate::classify::Bucket::RequiresUdfRegistration
                );
            if actionable && failures.len() < failure_cap {
                failures.push(Failure {
                    bucket: bucket_key(v.bucket).to_string(),
                    sql: sql.chars().take(160).collect(),
                    detail: v.detail.clone(),
                });
            }
        }
        FileReport {
            file: file.to_string(),
            skipped: None,
            total: verdicts.len(),
            strict_pass,
            semantic_pass,
            buckets,
            failures,
        }
    }
}

/// The whole-corpus scoreboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorpusReport {
    pub spark_version: String,
    pub files_total: usize,
    pub files_skipped: usize,
    /// Total replayed blocks (queries).
    pub blocks_total: usize,
    pub strict_pass: usize,
    pub semantic_pass: usize,
    pub buckets: BTreeMap<String, usize>,
    pub files: Vec<FileReport>,
}

impl CorpusReport {
    pub fn build(spark_version: String, files: Vec<FileReport>) -> Self {
        let mut buckets: BTreeMap<String, usize> = BTreeMap::new();
        let mut blocks_total = 0;
        let mut strict_pass = 0;
        let mut semantic_pass = 0;
        let mut files_skipped = 0;

        for f in &files {
            if f.skipped.is_some() {
                files_skipped += 1;
            }
            blocks_total += f.total;
            strict_pass += f.strict_pass;
            semantic_pass += f.semantic_pass;
            for (k, n) in &f.buckets {
                *buckets.entry(k.clone()).or_default() += n;
            }
        }
        CorpusReport {
            spark_version,
            files_total: files.len(),
            files_skipped,
            blocks_total,
            strict_pass,
            semantic_pass,
            buckets,
            files,
        }
    }

    pub fn strict_pct(&self) -> f64 {
        pct(self.strict_pass, self.blocks_total)
    }
    pub fn semantic_pct(&self) -> f64 {
        pct(self.semantic_pass, self.blocks_total)
    }

    /// Compact JSON suitable for the CI ratchet and the site scoreboard.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap()
    }

    /// Self-contained HTML scoreboard for the static Pages site (no fetch / no JS deps).
    pub fn to_html(&self) -> String {
        let mut bucket_rows = String::new();
        let mut rows: Vec<(&String, &usize)> = self.buckets.iter().collect();
        rows.sort_by(|a, b| b.1.cmp(a.1));
        for (k, n) in rows {
            if k == "pass" {
                continue;
            }
            bucket_rows.push_str(&format!("<tr><td>{k}</td><td class=num>{n}</td></tr>"));
        }
        format!(
            r#"<!doctype html><html lang=en><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Oxidant SQL parity</title>
<style>
body{{font:15px/1.5 system-ui,sans-serif;max-width:760px;margin:3rem auto;padding:0 1rem;color:#111}}
h1{{font-size:1.5rem}} .big{{font-size:2.6rem;font-weight:700;margin:.2rem 0}}
.cards{{display:flex;gap:1rem;flex-wrap:wrap;margin:1.5rem 0}}
.card{{flex:1;min-width:210px;border:1px solid #e3e3e3;border-radius:12px;padding:1rem 1.2rem}}
.sub{{color:#666;font-size:.85rem}} table{{border-collapse:collapse;width:100%;margin:.5rem 0}}
td,th{{border-bottom:1px solid #eee;padding:.35rem .5rem;text-align:left}} .num{{text-align:right;font-variant-numeric:tabular-nums}}
code{{background:#f4f4f4;padding:.1rem .3rem;border-radius:4px}}
</style></head><body>
<h1>Oxidant SQL parity</h1>
<p class=sub>Measured by replaying the {ver} SQL golden corpus
(<code>inputs/</code> + <code>results/</code>, {blocks} queries across {files} files) through Oxidant
and diffing against its recorded <code>.sql.out</code> outputs.</p>
<div class=cards>
<div class=card><div class=sub>Semantic parity</div><div class=big>{sem:.1}%</div>
<div class=sub>right answer / right rejection — {semn}/{blocks}</div></div>
<div class=card><div class=sub>Strict parity</div><div class=big>{strict:.1}%</div>
<div class=sub>byte-for-byte identical — {strictn}/{blocks}</div></div>
</div>
<h2>Failures by triage bucket</h2>
<table><tr><th>bucket</th><th class=num>count</th></tr>{bucket_rows}</table>
<p class=sub>Generated by <code>oxidant-parity</code>. Strict = exact match to Spark's golden output;
semantic also credits benign column-name divergence and "both engines reject this query".</p>
</body></html>"#,
            ver = self.spark_version,
            blocks = self.blocks_total,
            files = self.files_total,
            sem = self.semantic_pct(),
            semn = self.semantic_pass,
            strict = self.strict_pct(),
            strictn = self.strict_pass,
            bucket_rows = bucket_rows,
        )
    }

    /// Compact JSON for the site scoreboard (headline + buckets only, no per-file detail).
    pub fn to_scoreboard_json(&self) -> String {
        serde_json::json!({
            "spark_version": self.spark_version,
            "blocks_total": self.blocks_total,
            "files_total": self.files_total,
            "files_skipped": self.files_skipped,
            "strict_pass": self.strict_pass,
            "semantic_pass": self.semantic_pass,
            "strict_pct": (self.strict_pct() * 10.0).round() / 10.0,
            "semantic_pct": (self.semantic_pct() * 10.0).round() / 10.0,
            "buckets": self.buckets,
        })
        .to_string()
    }

    /// Human triage report.
    pub fn to_markdown(&self) -> String {
        let mut s = String::new();
        s.push_str("# Oxidant SQL parity scoreboard\n\n");
        s.push_str(&format!("Corpus tag: {}\n\n", self.spark_version));
        s.push_str(&format!(
            "- **Strict parity** (byte-for-byte): **{:.1}%**  ({}/{} queries)\n",
            self.strict_pct(),
            self.strict_pass,
            self.blocks_total
        ));
        s.push_str(&format!(
            "- **Semantic parity** (right answer/rejection): **{:.1}%**  ({}/{} queries)\n",
            self.semantic_pct(),
            self.semantic_pass,
            self.blocks_total
        ));
        s.push_str(&format!(
            "- Files: {} total, {} skipped\n\n",
            self.files_total, self.files_skipped
        ));

        s.push_str("## Failures by triage bucket\n\n");
        s.push_str("| bucket | count |\n|---|---:|\n");
        // Highest counts first.
        let mut rows: Vec<(&String, &usize)> = self.buckets.iter().collect();
        rows.sort_by(|a, b| b.1.cmp(a.1));
        for (k, n) in rows {
            if k == "pass" {
                continue;
            }
            s.push_str(&format!("| {k} | {n} |\n"));
        }

        s.push_str("\n## Lowest-parity files (top 25)\n\n");
        s.push_str("| file | strict | semantic | blocks |\n|---|---:|---:|---:|\n");
        let mut files: Vec<&FileReport> = self
            .files
            .iter()
            .filter(|f| f.skipped.is_none() && f.total > 0)
            .collect();
        files.sort_by(|a, b| {
            pct(a.strict_pass, a.total)
                .partial_cmp(&pct(b.strict_pass, b.total))
                .unwrap()
        });
        for f in files.iter().take(25) {
            s.push_str(&format!(
                "| {} | {:.0}% | {:.0}% | {} |\n",
                f.file,
                pct(f.strict_pass, f.total),
                pct(f.semantic_pass, f.total),
                f.total
            ));
        }

        let skipped: Vec<&FileReport> = self.files.iter().filter(|f| f.skipped.is_some()).collect();
        if !skipped.is_empty() {
            s.push_str(&format!(
                "\n## Skipped files ({}) — not counted, never silent\n\n",
                skipped.len()
            ));
            for f in &skipped {
                s.push_str(&format!(
                    "- `{}` — {}\n",
                    f.file,
                    f.skipped.as_deref().unwrap_or("")
                ));
            }
        }
        s
    }
}

fn pct(n: usize, d: usize) -> f64 {
    if d == 0 {
        0.0
    } else {
        100.0 * n as f64 / d as f64
    }
}
