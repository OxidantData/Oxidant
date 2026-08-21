//! Spark `if(cond, a, b)` — exactly `CASE WHEN cond THEN a ELSE b END` with the two result
//! branches widened to their least-common type. DataFusion has no `if` builtin (its planner
//! rejects the call with `Invalid function 'if'`).
//!
//! ## Why this is a faithful lowering, not a 3-arg UDF
//!
//! Spark's `if` is **short-circuiting**: in `if(c2 >= 0, 1 - 0, 1 / 0)` the `1 / 0` else-branch
//! is only evaluated on rows where the predicate is *false*. A plain 3-argument `ScalarUDF`
//! receives its arguments as already-evaluated arrays, so `1 / 0` would be computed (and could
//! error) for *every* row before `invoke` is even called — it cannot reproduce the short-circuit.
//!
//! Instead we register `if` as a `ScalarUDF` whose [`ScalarUDFImpl::simplify`] rewrites the call
//! into a real [`Expr::Case`] (the same trick DataFusion's own `arrow_cast` uses to become a
//! `Cast`). DataFusion's `CASE` physical operator evaluates each branch only on the rows that
//! reach it (`evaluate_selection`), giving exactly Spark's lazy, per-row branch evaluation.
//!
//! ## Why the coercion matches Spark
//!
//! Branch widening is delegated to [`get_coerce_type_for_case_expression`] — the very helper the
//! `TypeCoercion` analyzer uses to widen `CASE` THEN/ELSE branches. We declare a
//! [`Signature::user_defined`] and return `[Boolean, common, common]` from [`coerce_types`], so the
//! analyzer inserts exactly the casts a native `CASE` would (predicate → `Boolean`, both branches →
//! the common result type). The rewritten `Expr::Case` is therefore byte-identical to the plan
//! DataFusion builds for `CASE WHEN cond THEN a ELSE b END` — which is also how Spark implements
//! `If` (the `IfCoercion`/`CaseWhenCoercion` rules share the same conditional widening). When the
//! two branches have no common type, `coerce_types`/`return_type` error, mirroring Spark's
//! rejection of the same incompatible pairs (see `typeCoercion/native/ifCoercion.sql.out`).

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, plan_datafusion_err, plan_err, DataFusionError, Result};
use datafusion::logical_expr::expr::Case;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::type_coercion::other::get_coerce_type_for_case_expression;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Register Spark's `if` into `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(SparkIf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkIf {
    signature: Signature,
}

impl SparkIf {
    fn new() -> Self {
        Self {
            // `user_defined` so DataFusion calls our `coerce_types` (where we delegate branch
            // widening to the same helper a native `CASE` uses).
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    /// The widened (least-common) type of the two result branches — Spark's `if` result type.
    ///
    /// We delegate to DataFusion's `CASE` THEN/ELSE coercion ([`get_coerce_type_for_case_expression`]),
    /// **but only for branch-type families where DataFusion's conditional coercion is known to agree
    /// with Spark's `If` coercion**. For the families where the two engines diverge — verified
    /// against `typeCoercion/native/ifCoercion.sql.out` — we return `None` (the query errors)
    /// rather than emit a result that contradicts Spark (correctness over coverage):
    ///
    /// * temporal × numeric — Spark rejects e.g. `int`/`date`; DataFusion would wrongly unify them;
    /// * decimal × float/double — Spark widens to `double`; DataFusion picks a divergent decimal;
    /// * string × float/double/decimal — Spark widens to `double`; DataFusion keeps the string,
    ///   so the value prints differently.
    ///
    /// `None` is also returned (matching Spark's rejection) when the branches have no common type at
    /// all. Declining a pair only ever turns a query into a clean error — never a wrong answer.
    fn branch_type(then_t: &DataType, else_t: &DataType) -> Option<DataType> {
        if Self::coercion_diverges_from_spark(then_t, else_t) {
            return None;
        }
        get_coerce_type_for_case_expression(std::slice::from_ref(then_t), Some(else_t))
    }

    /// `true` for the branch-type pairs where DataFusion's `CASE` coercion disagrees with Spark's
    /// `If` coercion (see [`branch_type`]). Symmetric in its arguments.
    fn coercion_diverges_from_spark(a: &DataType, b: &DataType) -> bool {
        use DataType::*;
        let is_temporal = |t: &DataType| matches!(t, Date32 | Date64 | Timestamp(..));
        let is_decimal = |t: &DataType| matches!(t, Decimal128(..) | Decimal256(..));
        let is_float = |t: &DataType| matches!(t, Float16 | Float32 | Float64);
        let is_int = |t: &DataType| {
            matches!(
                t,
                Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64
            )
        };
        let is_numeric = |t: &DataType| is_int(t) || is_float(t) || is_decimal(t);
        let is_string = |t: &DataType| matches!(t, Utf8 | LargeUtf8 | Utf8View);
        let inexact = |t: &DataType| is_float(t) || is_decimal(t);

        (is_temporal(a) && is_numeric(b))
            || (is_numeric(a) && is_temporal(b))
            || (is_decimal(a) && is_float(b))
            || (is_float(a) && is_decimal(b))
            || (is_string(a) && inexact(b))
            || (inexact(a) && is_string(b))
    }

    /// Error returned (matching Spark's rejection) when the branches can't be unified.
    fn no_common_type(then_t: &DataType, else_t: &DataType) -> DataFusionError {
        plan_datafusion_err!(
            "if: the true and false branches have incompatible types ({then_t} and {else_t})"
        )
    }
}

impl ScalarUDFImpl for SparkIf {
    fn name(&self) -> &str {
        "if"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Coerce like a native `CASE`: predicate → `Boolean`, both result branches → their common
    /// type. Returning these target types makes the `TypeCoercion` analyzer insert the same casts a
    /// `CASE` would, so the `Expr::Case` produced by `simplify` is already correctly typed.
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 3 {
            return plan_err!("if expects exactly 3 arguments, got {}", arg_types.len());
        }
        let common = Self::branch_type(&arg_types[1], &arg_types[2])
            .ok_or_else(|| Self::no_common_type(&arg_types[1], &arg_types[2]))?;
        Ok(vec![DataType::Boolean, common.clone(), common])
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 3 {
            return plan_err!("if expects exactly 3 arguments, got {}", arg_types.len());
        }
        Self::branch_type(&arg_types[1], &arg_types[2])
            .ok_or_else(|| Self::no_common_type(&arg_types[1], &arg_types[2]))
    }

    /// Rewrite `if(cond, a, b)` → `CASE WHEN cond THEN a ELSE b END`. By the time `simplify` runs
    /// (the `SimplifyExpressions` optimizer pass) the analyzer has already applied `coerce_types`,
    /// so `args` are `[cast(cond, Boolean), cast(a, common), cast(b, common)]` — the `Case` needs no
    /// further coercion and has the same result type/field this UDF declared.
    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let mut it = args.into_iter();
        let (Some(cond), Some(then_expr), Some(else_expr), None) =
            (it.next(), it.next(), it.next(), it.next())
        else {
            return plan_err!("if expects exactly 3 arguments");
        };
        let case = Case::new(
            None,
            vec![(Box::new(cond), Box::new(then_expr))],
            Some(Box::new(else_expr)),
        );
        Ok(ExprSimplifyResult::Simplified(Expr::Case(case)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Unreachable: `simplify` always rewrites `if` to a `CASE` before execution (same contract
        // as DataFusion's `arrow_cast`). If this ever fires, the optimizer was bypassed.
        exec_err!("if should have been simplified to CASE WHEN ... END before execution")
    }
}

#[cfg(test)]
mod tests {
    use crate::Engine;

    async fn one(engine: &Engine, q: &str) -> String {
        let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        crate::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string()
    }

    /// `if` must short-circuit: the false-branch `1/0` is never evaluated when the predicate is
    /// true, so this returns a value instead of erroring on divide-by-zero.
    #[tokio::test]
    async fn if_is_short_circuiting() {
        let engine = Engine::new();
        let got = one(&engine, "SELECT if(1 == 1, 42, 1/0) AS x").await;
        assert!(got.contains("42"), "want 42, got:\n{got}");
    }

    /// Branch widening: `tinyint` and `bigint` unify to `bigint`, exactly like a `CASE`.
    #[tokio::test]
    async fn if_widens_branches() {
        let engine = Engine::new();
        let got = one(
            &engine,
            "SELECT typeof(if(true, cast(1 as tinyint), cast(2 as bigint))) AS t",
        )
        .await;
        assert!(got.contains("bigint"), "want bigint, got:\n{got}");
    }

    /// The false branch is selected when the predicate is false.
    #[tokio::test]
    async fn if_selects_else() {
        let engine = Engine::new();
        let got = one(&engine, "SELECT if(false, 1, 2) AS x").await;
        assert!(
            got.contains('2') && !got.contains('1'),
            "want 2, got:\n{got}"
        );
    }
}
