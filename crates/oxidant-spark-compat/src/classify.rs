//! Turn a `(golden, actual)` comparison into a [`Verdict`] with a triage [`Bucket`].
//!
//! This is the "triage the work" deliverable: every non-pass is filed into a bucket so the
//! aggregate report is an *actionable backlog* ("47 failures are `function-missing`, here are
//! the functions") rather than an undifferentiated pile of diffs. Buckets are ordered by
//! remediation priority — `correctness` (wrong answers) is the most urgent, parser gaps the
//! least surprising.

use crate::{normalize, GoldenBlock, Outcome};
use serde::{Deserialize, Serialize};

/// The disposition of one replayed block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Bucket {
    /// Output (and schema) matched the golden exactly. Full parity.
    Pass,
    /// Spark expected an error and oxidant also errored. Semantically aligned (both reject the
    /// query); the JVM error *text* is not compared (the engines word errors differently).
    ErrorParity,
    /// Values and types matched but the row order differed on an order-sensitive query.
    Ordering,
    /// Values matched but the declared schema differed (usually column-name spelling, e.g.
    /// `count(a)` vs `count(t.a)`).
    SchemaOnly,
    /// Both sides produced a decimal but precision/scale/rounding diverged.
    DecimalPrecision,
    /// Date/time/timezone/calendar rendering diverged.
    Datetime,
    /// A NULL appeared on one side but not the other (three-valued-logic divergence).
    NullSemantics,
    /// oxidant errored where Spark expected rows, and the error names a missing function.
    FunctionMissing,
    /// oxidant errored where Spark expected rows, and the error is a parse/syntax error.
    ParserUnsupported,
    /// oxidant errored with "not implemented" — a known feature gap (temp views, PIVOT, USE, …).
    /// These are usually *root causes* that cascade into `missing-relation` downstream.
    FeatureUnsupported,
    /// oxidant errored because a table/view wasn't found — usually a *cascade* from an earlier
    /// failed `CREATE … VIEW`/`TABLE` in the same file, not an independent bug.
    MissingRelation,
    /// oxidant *panicked* (DataFusion `panic!`/assertion) rather than returning an error — a
    /// robustness bug in the engine, distinct from a clean rejection.
    EnginePanic,
    /// oxidant errored where Spark expected rows, for some other reason.
    ExecError,
    /// Spark expected an error but oxidant produced rows (we are *too lenient*).
    MissingError,
    /// Both produced rows of the same shape but the values are wrong. Highest-signal failure.
    Correctness,
    /// The query uses a per-run-nondeterministic function (`rand`/`random`/`uuid`/`shuffle`), so it
    /// can never byte-match a fixed golden and its verdict would otherwise flip run-to-run. Scored
    /// as neither pass nor fail so the parity numbers (and the CI ratchet) stay deterministic.
    Nondeterministic,
    /// The query calls a function that exists *only* because Spark's Scala `SQLQueryTestSuite`
    /// registered it before running the file (the `udaf` Scala UDAF, the `default.myDoubleAvg` /
    /// `udaf1` Hive JVM-class functions, the `udtf` / `UDTF*` Python table functions). These are
    /// test scaffolding, not SQL a real oxidant user runs — exactly like the whole-file
    /// `requires-udf-registration` skip already applied to `inputs/udf/*` (which wrap every column
    /// in the registered `udf(...)`). oxidant can never resolve them here, so they are unmeasurable
    /// and scored as neither pass nor fail (like `Nondeterministic`). This is *not* used for
    /// `CREATE FUNCTION … RETURN` SQL UDFs (e.g. `sql-udf.sql`'s `foo*`): those are SQL-defined and
    /// remain a genuine, counted oxidant feature gap.
    RequiresUdfRegistration,
}

impl Bucket {
    /// Whether this disposition counts toward the **strict** parity score (byte-for-byte).
    pub fn is_strict_pass(self) -> bool {
        matches!(self, Bucket::Pass)
    }
    /// Whether this counts toward the **semantic** parity score (right answer / right
    /// rejection, allowing benign schema-name and both-error divergences).
    pub fn is_semantic_pass(self) -> bool {
        // `Ordering` counts: same rows in a different order is semantically correct when the
        // query's `ORDER BY` leaves ties unordered — and it keeps the score deterministic
        // (tie-order can otherwise vary run-to-run).
        matches!(
            self,
            Bucket::Pass | Bucket::ErrorParity | Bucket::SchemaOnly | Bucket::Ordering
        )
    }
}

/// A classified comparison for one block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Verdict {
    pub bucket: Bucket,
    /// Short human note for the report (e.g. the missing function name, or a diff summary).
    pub detail: String,
}

/// Per-run-nondeterministic SQL functions: a query calling one of these produces different output
/// every run, so it can never byte-match a fixed golden and must not be allowed to flip the score.
fn is_nondeterministic(sql: &str) -> bool {
    let lower = sql.to_lowercase();
    ["rand(", "rand ", "random(", "randn(", "uuid(", "shuffle("]
        .iter()
        .any(|f| lower.contains(f))
}

/// Does the query invoke a function that exists only because Spark's Scala `SQLQueryTestSuite`
/// registers it in the harness before running the file? Such functions are *never* defined by the
/// SQL the file runs, so oxidant (a SQL engine, not the JVM test harness) can never resolve them — the
/// failure is test scaffolding, not a oxidant SQL-parity gap. The set is closed and audited:
///
/// - `udaf(` — the Scala `UDAF` registered for `udaf/*.sql` (never created via SQL in the file).
/// - `udaf1(` / `mydoubleavg(` — Hive JVM-class functions (`CREATE FUNCTION … AS 'class'`) whose
///   bodies live in a test JAR (`test.org.apache.spark.sql.MyDoubleAvg`), absent from any oxidant run.
/// - `udtf` — the Python `udtf` / `UDTF*` table functions in `udtf/udtf.sql`.
///
/// Each token is corpus-verified to appear (as a call) *only* in those fixture files, so this never
/// matches a real query. Deliberately excluded: `CREATE FUNCTION … RETURN` SQL UDFs (`sql-udf.sql`'s
/// `foo*`) — those are SQL-defined and a *genuine* oxidant feature gap that must stay counted.
fn references_test_registered_udf(sql: &str) -> bool {
    let lower = sql.to_lowercase();
    ["udaf(", "udaf1(", "mydoubleavg(", "udtf"]
        .iter()
        .any(|f| lower.contains(f))
}

/// Classify one replayed block against its golden expectation.
pub fn classify(golden: &GoldenBlock, actual: &Outcome) -> Verdict {
    // A nondeterministic query is unscoreable against a fixed golden — bucket it stably (excluded
    // from both pass scores) so the corpus totals and the ratchet are reproducible. Errors are
    // exempt: if the query errors on both sides it is still a deterministic outcome.
    if is_nondeterministic(&golden.sql) && matches!(actual, Outcome::Ok { .. }) {
        return Verdict {
            bucket: Bucket::Nondeterministic,
            detail: "uses rand/uuid/shuffle — unscoreable vs fixed golden".into(),
        };
    }

    let expects_error = golden.expects_error();

    match actual {
        Outcome::Err { message } => {
            // A panic is never a graceful rejection — surface it as a robustness bug even when
            // Spark also rejects the query.
            if message.starts_with("engine panicked") {
                return Verdict {
                    bucket: Bucket::EnginePanic,
                    detail: first_line(message),
                };
            }
            if expects_error {
                return Verdict {
                    bucket: Bucket::ErrorParity,
                    detail: String::new(),
                };
            }
            // oxidant errored but Spark expected rows: bucket by the kind of error. Order matters
            // — check the specific signatures before the generic exec-error fallback.
            let bucket = if looks_like_missing_relation(message) {
                Bucket::MissingRelation
            } else if looks_like_missing_function(message) {
                // A "missing function" that is really a Scala/JVM/Python test fixture (udaf, udtf,
                // myDoubleAvg…) is unmeasurable scaffolding, not a oxidant gap — file it in the
                // excluded `requires-udf-registration` bucket instead of inflating function-missing.
                if references_test_registered_udf(&golden.sql) {
                    Bucket::RequiresUdfRegistration
                } else {
                    Bucket::FunctionMissing
                }
            } else if looks_like_parse_error(message) {
                Bucket::ParserUnsupported
            } else if looks_like_unimplemented(message) {
                Bucket::FeatureUnsupported
            } else {
                Bucket::ExecError
            };
            Verdict {
                bucket,
                detail: first_line(message),
            }
        }
        Outcome::Ok { schema, output } => {
            if expects_error {
                return Verdict {
                    bucket: Bucket::MissingError,
                    detail: "oxidant accepted a query Spark rejects".into(),
                };
            }
            let output_ok = normalize::outputs_match(&golden.sql, &golden.output, output);
            let schema_ok = schema == &golden.schema;

            if output_ok && schema_ok {
                return Verdict {
                    bucket: Bucket::Pass,
                    detail: String::new(),
                };
            }
            if output_ok && !schema_ok {
                return Verdict {
                    bucket: Bucket::SchemaOnly,
                    detail: format!("schema: golden `{}` vs oxidant `{}`", golden.schema, schema),
                };
            }
            // Output differs — try to attribute the divergence.
            let bucket = attribute_value_diff(&golden.sql, &golden.output, output);
            Verdict {
                bucket,
                detail: diff_summary(&golden.output, output),
            }
        }
    }
}

/// When two same-shape outputs disagree, guess *why* from the values themselves.
fn attribute_value_diff(sql: &str, golden: &str, actual: &str) -> Bucket {
    // Same multiset but different order on an ordered query → ordering.
    if normalize::is_order_sensitive(sql) {
        let mut g: Vec<&str> = golden.lines().collect();
        let mut a: Vec<&str> = actual.lines().collect();
        g.sort();
        a.sort();
        if g == a {
            return Bucket::Ordering;
        }
    }
    let g_has_null = golden.split(['\t', '\n']).any(|c| c == "NULL");
    let a_has_null = actual.split(['\t', '\n']).any(|c| c == "NULL");
    if g_has_null != a_has_null {
        return Bucket::NullSemantics;
    }
    if looks_decimalish(golden) && looks_decimalish(actual) {
        return Bucket::DecimalPrecision;
    }
    if looks_datetimeish(golden) || looks_datetimeish(actual) {
        return Bucket::Datetime;
    }
    Bucket::Correctness
}

fn looks_like_missing_relation(msg: &str) -> bool {
    let m = msg.to_lowercase();
    (m.contains("table") || m.contains("view"))
        && (m.contains("not found") || m.contains("doesn't exist") || m.contains("does not exist"))
}

fn looks_like_missing_function(msg: &str) -> bool {
    let m = msg.to_lowercase();
    (m.contains("function") || m.contains("aggregate"))
        && (m.contains("not found")
            || m.contains("no function")
            || m.contains("invalid function")
            || m.contains("unknown")
            || m.contains("undefined")
            || m.contains("unresolved")
            || m.contains("expected zero argument")
            || m.contains("expected") && m.contains("argument"))
}

fn looks_like_parse_error(msg: &str) -> bool {
    let m = msg.to_lowercase();
    m.contains("parsererror")
        || m.contains("sql parser")
        || m.contains("parse error")
        || m.contains("syntax error")
        || (m.contains("unsupported ast node"))
        || (m.contains("unsupported sql"))
}

fn looks_like_unimplemented(msg: &str) -> bool {
    let m = msg.to_lowercase();
    m.contains("not implemented") || m.contains("not supported") || m.contains("is not supported")
}

fn looks_decimalish(s: &str) -> bool {
    s.split(['\t', '\n']).any(is_decimalish_cell)
}

/// Whether a single output cell reads as a (possibly scientific-notation) decimal number. A cell
/// "looks decimalish" if it carries a fractional point or an exponent marker and is otherwise made
/// only of the characters a decimal/scientific number uses. This recognizes BOTH plain decimals
/// (`1004.3`) and Java/Spark scientific notation (`1.2345678901234E200`, `9.223372036854776E18`),
/// so a numeric value-precision difference is attributed to `decimal-precision` regardless of which
/// notation the formatter picked — the bucket no longer flips to `correctness` purely because a
/// double crossed Java's `1e7`/`1e-3` threshold into `E`-notation. Dates (`2024-01-01`) and
/// timestamps (which carry `:`/space) are excluded since they contain characters outside the set,
/// and a sign-only or all-symbol token is excluded by requiring at least one digit.
fn is_decimalish_cell(c: &str) -> bool {
    let has_marker = c.contains('.') || c.contains('e') || c.contains('E');
    has_marker
        && c.chars().any(|ch| ch.is_ascii_digit())
        && c.chars()
            .all(|ch| ch.is_ascii_digit() || matches!(ch, '.' | '-' | '+' | 'e' | 'E'))
}

fn looks_datetimeish(s: &str) -> bool {
    // crude: contains a YYYY-MM-DD-ish token.
    s.split(['\t', '\n']).any(|c| {
        let b = c.as_bytes();
        b.len() >= 10 && b[4] == b'-' && b[7] == b'-' && b[0].is_ascii_digit()
    })
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").chars().take(200).collect()
}

fn diff_summary(golden: &str, actual: &str) -> String {
    format!(
        "golden[{}r] vs oxidant[{}r]",
        golden.lines().filter(|l| !l.is_empty()).count(),
        actual.lines().filter(|l| !l.is_empty()).count()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn golden(sql: &str, schema: &str, output: &str) -> GoldenBlock {
        GoldenBlock {
            sql: sql.into(),
            schema: schema.into(),
            output: output.into(),
        }
    }

    #[test]
    fn exact_match_is_pass() {
        let g = golden("SELECT 1", "struct<1:int>", "1");
        let a = Outcome::Ok {
            schema: "struct<1:int>".into(),
            output: "1".into(),
        };
        assert_eq!(classify(&g, &a).bucket, Bucket::Pass);
    }

    #[test]
    fn both_error_is_error_parity() {
        let g = golden(
            "SELECT a GROUP BY b",
            "struct<>",
            "org.apache.spark.sql.AnalysisException\n{}",
        );
        let a = Outcome::Err {
            message: "Schema error: no field a".into(),
        };
        assert_eq!(classify(&g, &a).bucket, Bucket::ErrorParity);
    }

    #[test]
    fn value_mismatch_is_correctness() {
        let g = golden("SELECT x FROM t", "struct<x:int>", "5");
        let a = Outcome::Ok {
            schema: "struct<x:int>".into(),
            output: "6".into(),
        };
        assert_eq!(classify(&g, &a).bucket, Bucket::Correctness);
    }

    #[test]
    fn schema_name_divergence_is_schema_only() {
        let g = golden("SELECT count(a) FROM t", "struct<count(a):bigint>", "3");
        let a = Outcome::Ok {
            schema: "struct<count(t.a):bigint>".into(),
            output: "3".into(),
        };
        assert_eq!(classify(&g, &a).bucket, Bucket::SchemaOnly);
    }

    #[test]
    fn missing_function_is_bucketed() {
        let g = golden("SELECT weird(a) FROM t", "struct<x:int>", "3");
        let a = Outcome::Err {
            message: "Invalid function 'weird': function not found".into(),
        };
        assert_eq!(classify(&g, &a).bucket, Bucket::FunctionMissing);
    }

    #[test]
    fn scala_registered_udaf_is_requires_udf_registration() {
        // `udaf` is a Scala UDAF the test harness registers — not a oxidant gap.
        let g = golden("SELECT udaf(b) FROM testData", "struct<x:int>", "3");
        let a = Outcome::Err {
            message: "Error during planning: Invalid function 'udaf'.".into(),
        };
        assert_eq!(classify(&g, &a).bucket, Bucket::RequiresUdfRegistration);
    }

    #[test]
    fn sql_defined_create_function_udf_stays_function_missing() {
        // `sql-udf.sql`'s `foo*` are SQL-defined via CREATE FUNCTION … RETURN: a real oxidant gap.
        let g = golden("SELECT foo1a0()", "struct<x:int>", "1");
        let a = Outcome::Err {
            message: "Error during planning: Invalid function 'foo1a0'.".into(),
        };
        assert_eq!(classify(&g, &a).bucket, Bucket::FunctionMissing);
    }

    #[test]
    fn expected_error_udtf_stays_error_parity() {
        // A udtf row whose golden *expects* an error must remain a semantic pass (error-parity),
        // never get re-bucketed into the excluded category.
        let g = golden(
            "SELECT * FROM UDTFForwardStateFromAnalyzeWithKwargs(1, 2)",
            "struct<>",
            "org.apache.spark.sql.AnalysisException\n{}",
        );
        let a = Outcome::Err {
            message: "Error during planning: table function not found".into(),
        };
        assert_eq!(classify(&g, &a).bucket, Bucket::ErrorParity);
    }

    #[test]
    fn oxidant_too_lenient_is_missing_error() {
        let g = golden(
            "SELECT a GROUP BY b",
            "struct<>",
            "org.apache.spark.sql.AnalysisException\n{}",
        );
        let a = Outcome::Ok {
            schema: "struct<a:int>".into(),
            output: "1".into(),
        };
        assert_eq!(classify(&g, &a).bucket, Bucket::MissingError);
    }
}
