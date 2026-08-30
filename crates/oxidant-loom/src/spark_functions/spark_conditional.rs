//! Spark's null-handling predicates and the function spellings of `LIKE`.
//!
//! Every function here has a native DataFusion `Expr` with identical semantics — it is the *name*
//! that is missing, not the behaviour. So, exactly like [`super::spark_if`], each is registered as
//! a `ScalarUDF` whose [`ScalarUDFImpl::simplify`] rewrites the call into that native expression
//! before execution. That is strictly better than a real UDF: the optimizer keeps seeing an
//! `IS NULL` / `LIKE` it can push into a filter, prune a partition with, or fold to a constant,
//! which it could never do through an opaque scalar function.
//!
//! Functions:
//! - `isnull(expr)` / `isnotnull(expr)` → `expr IS [NOT] NULL`.
//! - `equal_null(a, b)` → `a IS NOT DISTINCT FROM b`, Spark's null-safe equality (`a <=> b`):
//!   two NULLs are equal and a NULL against a value is `false`, never NULL.
//! - `like(str, pattern[, escape])` / `ilike(str, pattern[, escape])` → `Expr::Like`. Databricks
//!   documents both an operator and a function spelling; only the operator parsed before. The
//!   optional escape must be a backslash literal: DataFusion's LIKE kernel implements no other,
//!   and a clean planning error beats matching against the wrong escape character.
//!
//! `invoke_with_args` is unreachable for all of them, and says so if the optimizer is ever bypassed.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, plan_err, Result, ScalarValue};
use datafusion::logical_expr::expr::Like;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::type_coercion::binary::comparison_coercion;
use datafusion::logical_expr::{
    BinaryExpr, ColumnarValue, Expr, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Register the null predicates and `LIKE` function spellings into `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(NullPredicate::new("isnull", true)));
    ctx.register_udf(ScalarUDF::from(NullPredicate::new("isnotnull", false)));
    ctx.register_udf(ScalarUDF::from(EqualNull::new()));
    ctx.register_udf(ScalarUDF::from(LikeFn::new("like", false)));
    ctx.register_udf(ScalarUDF::from(LikeFn::new("ilike", true)));
}

// ---------------------------------------------------------------------------
// isnull / isnotnull
// ---------------------------------------------------------------------------

/// `isnull(expr)` (`is_null = true`) and `isnotnull(expr)` (`is_null = false`).
#[derive(Debug, PartialEq, Eq, Hash)]
struct NullPredicate {
    name: &'static str,
    is_null: bool,
    signature: Signature,
}

impl NullPredicate {
    fn new(name: &'static str, is_null: bool) -> Self {
        Self {
            name,
            is_null,
            // Any type can be null-checked, and no coercion is wanted — casting the argument
            // could itself change nullness.
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NullPredicate {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let mut it = args.into_iter();
        let (Some(arg), None) = (it.next(), it.next()) else {
            return plan_err!("{} expects exactly 1 argument", self.name);
        };
        Ok(ExprSimplifyResult::Simplified(if self.is_null {
            arg.is_null()
        } else {
            arg.is_not_null()
        }))
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!(
            "{} should have been simplified to IS [NOT] NULL before execution",
            self.name
        )
    }
}

// ---------------------------------------------------------------------------
// equal_null
// ---------------------------------------------------------------------------

/// `equal_null(a, b)` — Spark's null-safe equality, the function spelling of `a <=> b`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct EqualNull {
    signature: Signature,
}

impl EqualNull {
    fn new() -> Self {
        // `user_defined` so `coerce_types` can widen the two sides the way a comparison would;
        // `IsNotDistinctFrom` is planned as a binary comparison and needs matching operand types.
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for EqualNull {
    fn name(&self) -> &str {
        "equal_null"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [a, b] = arg_types else {
            return plan_err!("equal_null expects exactly 2 arguments");
        };
        // The same helper `=` uses, so `equal_null` accepts exactly the operand pairs `<=>` does
        // and rejects the same ones.
        match comparison_coercion(a, b) {
            Some(common) => Ok(vec![common.clone(), common]),
            None => plan_err!("equal_null: incompatible operand types ({a} and {b})"),
        }
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let mut it = args.into_iter();
        let (Some(a), Some(b), None) = (it.next(), it.next(), it.next()) else {
            return plan_err!("equal_null expects exactly 2 arguments");
        };
        Ok(ExprSimplifyResult::Simplified(Expr::BinaryExpr(
            BinaryExpr::new(Box::new(a), Operator::IsNotDistinctFrom, Box::new(b)),
        )))
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("equal_null should have been simplified to <=> before execution")
    }
}

// ---------------------------------------------------------------------------
// like / ilike
// ---------------------------------------------------------------------------

/// `like(str, pattern[, escape])` / `ilike(...)` — the function spellings of the `LIKE` operator.
#[derive(Debug, PartialEq, Eq, Hash)]
struct LikeFn {
    name: &'static str,
    case_insensitive: bool,
    signature: Signature,
}

impl LikeFn {
    fn new(name: &'static str, case_insensitive: bool) -> Self {
        Self {
            name,
            case_insensitive,
            signature: Signature::one_of(
                vec![TypeSignature::String(2), TypeSignature::String(3)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for LikeFn {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let mut it = args.into_iter();
        let (Some(expr), Some(pattern)) = (it.next(), it.next()) else {
            return plan_err!("{} expects 2 or 3 arguments", self.name);
        };
        // `Expr::Like` carries the escape as a single `char` decided at plan time, so a
        // non-literal escape cannot be represented. Spark also requires a literal here.
        let escape_char = match it.next() {
            None => None,
            Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _))
            | Some(Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _))
            | Some(Expr::Literal(ScalarValue::Utf8View(Some(s)), _))
                if s.chars().count() == 1 =>
            {
                let c = s.chars().next().expect("checked exactly one char");
                // DataFusion's LIKE kernel hard-codes backslash as the escape character and
                // rejects any other at execution time. Refusing here turns that into a clear
                // planning error instead of a confusing runtime one — and, more importantly,
                // never lets a query silently match against the wrong escape.
                if c != '\\' {
                    return plan_err!(
                        "{}: escape character {c:?} is not supported (only backslash is); \
                         rewrite the call as a LIKE ... ESCAPE expression if you need another",
                        self.name
                    );
                }
                Some(c)
            }
            Some(other) => {
                return plan_err!(
                "{}: the escape argument must be a single-character string literal, got {other}",
                self.name
            )
            }
        };
        if it.next().is_some() {
            return plan_err!("{} expects 2 or 3 arguments", self.name);
        }
        Ok(ExprSimplifyResult::Simplified(Expr::Like(Like::new(
            false,
            Box::new(expr),
            Box::new(pattern),
            escape_char,
            self.case_insensitive,
        ))))
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!(
            "{} should have been simplified to a LIKE expression before execution",
            self.name
        )
    }
}

#[cfg(test)]
mod tests {
    async fn row(q: &str) -> String {
        let engine = crate::Engine::new();
        let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        crate::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn null_predicates_agree_with_the_operators() {
        for (q, want) in [
            ("SELECT isnull(NULL) AS x", "true"),
            ("SELECT isnull(1) AS x", "false"),
            ("SELECT isnotnull(NULL) AS x", "false"),
            ("SELECT isnotnull(1) AS x", "true"),
            ("SELECT isnull('') AS x", "false"),
        ] {
            let got = row(q).await;
            assert!(got.contains(want), "{q} -> want {want}, got:\n{got}");
        }
    }

    /// `equal_null` is total: it never returns NULL, which is the whole point of `<=>`.
    #[tokio::test]
    async fn equal_null_is_null_safe() {
        for (q, want) in [
            ("SELECT equal_null(NULL, NULL) AS x", "true"),
            ("SELECT equal_null(1, NULL) AS x", "false"),
            ("SELECT equal_null(NULL, 1) AS x", "false"),
            ("SELECT equal_null(1, 1) AS x", "true"),
            ("SELECT equal_null(1, 2) AS x", "false"),
            ("SELECT equal_null('a', 'a') AS x", "true"),
            // Widening still applies, exactly as it does for `=`.
            ("SELECT equal_null(1, CAST(1 AS BIGINT)) AS x", "true"),
        ] {
            let got = row(q).await;
            assert!(got.contains(want), "{q} -> want {want}, got:\n{got}");
        }
    }

    #[tokio::test]
    async fn like_function_spellings_work() {
        for (q, want) in [
            ("SELECT like('abc', 'a%') AS x", "true"),
            ("SELECT like('abc', 'A%') AS x", "false"),
            ("SELECT ilike('abc', 'A%') AS x", "true"),
            ("SELECT like('abc', '_bc') AS x", "true"),
            // The escape form works with backslash, the only escape DataFusion's LIKE kernel
            // implements.
            ("SELECT like('a_c', 'a\\_c', '\\') AS x", "true"),
            ("SELECT like('abc', 'a\\_c', '\\') AS x", "false"),
        ] {
            let got = row(q).await;
            assert!(got.contains(want), "{q} -> want {want}, got:\n{got}");
        }
    }

    /// The rewrite must survive into the plan as a real predicate, not an opaque UDF call —
    /// that is what keeps filter pushdown and constant folding working.
    #[tokio::test]
    async fn isnull_lowers_to_a_native_predicate() {
        let engine = crate::Engine::new();
        let batches = engine
            .sql("EXPLAIN SELECT 1 WHERE isnull(CAST(NULL AS INT))")
            .await
            .expect("EXPLAIN");
        let plan = crate::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
        assert!(
            !plan.contains("isnull("),
            "isnull survived into the plan as a UDF call:\n{plan}"
        );
    }
}
