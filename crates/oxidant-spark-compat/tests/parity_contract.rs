//! Classifier and ratchet contracts (OxidantData/Oxidant#191).
//! Classifier tests only — no engine, no corpus run.

use oxidant_spark_compat::classify::{classify, Bucket};
use oxidant_spark_compat::ratchet::{find_regressions, BlockScore};
use oxidant_spark_compat::{GoldenBlock, Outcome};

fn g(sql: &str, schema: &str, output: &str) -> GoldenBlock {
    GoldenBlock {
        sql: sql.into(),
        schema: schema.into(),
        output: output.into(),
    }
}

fn ok(schema: &str, output: &str) -> Outcome {
    Outcome::Ok {
        schema: schema.into(),
        output: output.into(),
    }
}

#[test]
fn unrelated_error_is_not_verified_parity() {
    let expected = g(
        "SELECT 1/0",
        "struct<>",
        "org.apache.spark.SparkArithmeticException\n{\"errorClass\":\"DIVIDE_BY_ZERO\"}",
    );
    let actual = Outcome::Err {
        message: "table t not found".into(),
    };
    assert!(!classify(&expected, &actual).bucket.is_semantic_pass());
}

#[test]
fn equal_display_does_not_erase_type_change() {
    let expected = g("SELECT x FROM t", "struct<x:int>", "1");
    assert!(!classify(&expected, &ok("struct<x:string>", "1"))
        .bucket
        .is_semantic_pass());
}

#[test]
fn unequal_key_order_reversal_is_not_a_pass() {
    let expected = g("SELECT x FROM t ORDER BY x", "struct<x:int>", "1\n2");
    assert!(!classify(&expected, &ok("struct<x:int>", "2\n1"))
        .bucket
        .is_semantic_pass());
}

#[test]
fn invalid_random_query_acceptance_is_missing_error() {
    let expected = g(
        "SELECT rand(x) FROM VALUES (1) AS t(x)",
        "struct<>",
        "org.apache.spark.sql.AnalysisException\n{}",
    );
    assert_eq!(
        classify(&expected, &ok("struct<x:double>", "0.5")).bucket,
        Bucket::MissingError
    );
}

#[test]
fn exact_match_control() {
    let expected = g("SELECT 1", "struct<1:int>", "1");
    assert_eq!(
        classify(&expected, &ok("struct<1:int>", "1")).bucket,
        Bucket::Pass
    );
}

#[test]
fn swapped_pass_and_failure_is_a_named_regression() {
    let old = [
        BlockScore {
            id: "A".into(),
            strict: true,
            semantic: true,
        },
        BlockScore {
            id: "B".into(),
            strict: false,
            semantic: false,
        },
    ];
    let new = [
        BlockScore {
            id: "A".into(),
            strict: false,
            semantic: false,
        },
        BlockScore {
            id: "B".into(),
            strict: true,
            semantic: true,
        },
    ];
    let regs = find_regressions(&old, &new);
    assert!(
        regs.iter().any(|r| r.id == "A"),
        "A pass→failure must be reported even when B failure→pass keeps aggregates"
    );
}
