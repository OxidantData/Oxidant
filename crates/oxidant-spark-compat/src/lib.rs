//! `oxidant-spark-compat` — the Apache Spark parity harness.
//!
//! Oxidant claims to be a *drop-in Spark replacement*. This crate turns that claim into a
//! **measured, provable number**: it runs Apache Spark's own golden SQL tests
//! (`sql/core/src/test/resources/sql-tests/`, vendored under `spark-tests/`) through oxidant,
//! formats the results exactly the way Spark's `SQLQueryTestSuite` does, and diffs them
//! against Spark's committed `.sql.out` golden outputs.
//!
//! The golden `.sql.out` files are *authoritative* — Spark generated them with
//! `SPARK_GENERATE_GOLDEN_FILES=1`. Each file is a sequence of blocks:
//!
//! ```text
//! -- !query
//! SELECT COUNT(a), COUNT(b) FROM testData GROUP BY a
//! -- !query schema
//! struct<count(a):bigint,count(b):bigint>
//! -- !query output
//! 0\t1
//! 2\t2
//! ```
//!
//! So the golden file itself is the authoritative *list of statements* — we never have to
//! re-implement Spark's `.sql` splitter to know what to run. We replay each block's SQL
//! through one [`oxidant_loom::Engine`] per file (so `CREATE TEMP VIEW` setup persists), format
//! the result Spark-style, and compare. Every mismatch is bucketed by [`classify`] into a
//! triage taxonomy so the output is an actionable backlog, not a wall of diffs.
//!
//! Module map:
//! - [`golden`]    — parse `.sql.out` into [`GoldenBlock`]s.
//! - [`format`]    — oxidant `RecordBatch` → Spark `hiveResultString` form (schema + rows).
//! - [`normalize`] — allowlisted normalizations (row sorting for unordered queries, etc).
//! - [`classify`]  — map a (golden, actual) pair to a [`Verdict`] + triage [`Bucket`].
//! - [`runner`]    — replay a whole file / corpus, collecting reports.
//! - [`report`]    — aggregate into a JSON + markdown parity scoreboard.
//! - [`splitter`]  — `.sql` input helpers (`--IMPORT` resolution; secondary to golden replay).
//! - [`functions`] — Oxidant's live function registry vs. the documented Spark builtin surface.

pub mod classify;
pub mod format;
pub mod functions;
pub mod golden;
pub mod normalize;
pub mod report;
pub mod runner;
pub mod splitter;

/// Absolute path to the vendored Spark corpus root (`spark-tests/`).
pub const CORPUS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/spark-tests");

/// Absolute path to the authored Spark-compat corpus root (`spark-compat-tests/`).
pub const SPARK_COMPAT_CORPUS_DIR: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/spark-compat-tests");

/// Which golden corpus a run replays. Both corpora share the same
/// `inputs/*.sql` + `results/*.sql.out` layout and the same report schema, so the
/// ratchet / scoreboard pipeline treats them identically; they differ only in root
/// directory and the default artifact locations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Corpus {
    /// The vendored Apache Spark `sql-tests` corpus (`spark-tests/`). The default; its
    /// ratchet output schema and artifact paths are unchanged.
    Spark,
    /// The authored Spark-compat corpus (`spark-compat-tests/`), drawn from Spark SQL
    /// language-manual categories (USING, TBLPROPERTIES, LATERAL VIEW, PIVOT, QUALIFY,
    /// COPY INTO, MERGE INTO, Delta Lake SQL, Lake Formation, …) that the vendored
    /// `spark-tests/` corpus does not cover.
    SparkCompat,
}

impl Corpus {
    /// All corpora, in stable order (Spark first).
    pub const ALL: [Corpus; 2] = [Corpus::Spark, Corpus::SparkCompat];

    /// Filesystem root of the corpus (`inputs/`, `results/`, `VERSION` live under it).
    pub fn root(self) -> std::path::PathBuf {
        match self {
            Corpus::Spark => std::path::PathBuf::from(CORPUS_DIR),
            Corpus::SparkCompat => std::path::PathBuf::from(SPARK_COMPAT_CORPUS_DIR),
        }
    }

    /// CLI / display name (`--corpus <name>`).
    pub fn name(self) -> &'static str {
        match self {
            Corpus::Spark => "spark",
            Corpus::SparkCompat => "spark-compat",
        }
    }

    /// Parse a `--corpus` flag value.
    pub fn from_name(s: &str) -> Option<Corpus> {
        match s {
            "spark" => Some(Corpus::Spark),
            "spark-compat" => Some(Corpus::SparkCompat),
            _ => None,
        }
    }

    /// Default `--out-dir` for `golden` / `ratchet` artifacts.
    pub fn default_out_dir(self) -> &'static str {
        match self {
            Corpus::Spark => "parity",
            Corpus::SparkCompat => "parity/spark-compat",
        }
    }

    /// Default `--baseline` path for `ratchet`.
    pub fn default_baseline(self) -> &'static str {
        match self {
            Corpus::Spark => "parity/baseline.json",
            Corpus::SparkCompat => "parity/baseline-spark.json",
        }
    }
}

#[cfg(test)]
mod corpus_tests {
    use super::*;

    #[test]
    fn corpus_names_select_the_expected_roots_and_artifacts() {
        assert_eq!(Corpus::from_name("spark"), Some(Corpus::Spark));
        assert_eq!(Corpus::from_name("spark-compat"), Some(Corpus::SparkCompat));
        assert_eq!(Corpus::from_name("unknown"), None);

        assert!(Corpus::Spark.root().ends_with("spark-tests"));
        assert!(Corpus::SparkCompat.root().ends_with("spark-compat-tests"));
        assert_eq!(Corpus::Spark.default_out_dir(), "parity");
        assert_eq!(Corpus::SparkCompat.default_out_dir(), "parity/spark-compat");
        assert_eq!(
            Corpus::SparkCompat.default_baseline(),
            "parity/baseline-spark.json"
        );
    }
}

/// One `-- !query` / `-- !query schema` / `-- !query output` unit from a golden `.sql.out`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoldenBlock {
    /// The SQL statement to replay (verbatim, may be multi-line, no trailing `;`).
    pub sql: String,
    /// Spark's declared output schema, e.g. `struct<count(a):bigint>` or `struct<>` for DDL.
    pub schema: String,
    /// Spark's expected output: tab-separated rows joined by `\n`, or an error rendering
    /// (`<exception classname>\n<json body>`). Empty for DDL / no-row statements.
    pub output: String,
}

impl GoldenBlock {
    /// True when Spark's expected output is an error (the first line is a JVM exception
    /// class name such as `org.apache.spark.sql.AnalysisException`).
    pub fn expects_error(&self) -> bool {
        self.output
            .lines()
            .next()
            .map(is_exception_classname)
            .unwrap_or(false)
    }
}

/// Heuristic: does this line look like a fully-qualified JVM exception class name?
/// Spark renders errors as e.g. `org.apache.spark.sql.catalyst.ExtendedAnalysisException`.
pub(crate) fn is_exception_classname(line: &str) -> bool {
    let line = line.trim();
    (line.starts_with("org.apache.spark")
        || line.starts_with("java.")
        || line.starts_with("scala."))
        && line.ends_with("Exception")
}

/// What actually happened when oxidant replayed a [`GoldenBlock`]'s SQL.
#[derive(Debug, Clone)]
pub enum Outcome {
    /// Query ran; we captured a Spark-formatted schema line and (already-normalized) rows.
    Ok {
        /// Spark-style schema, e.g. `struct<count(a):bigint>`.
        schema: String,
        /// Output rendered the Spark way: tab-joined cells per row, `\n`-joined, normalized.
        output: String,
    },
    /// Query failed inside oxidant (parse / plan / execute). We keep the message for triage.
    Err {
        /// oxidant's error string (used only for triage classification, never matched against
        /// Spark's JVM error text — the engines word errors differently).
        message: String,
    },
}
