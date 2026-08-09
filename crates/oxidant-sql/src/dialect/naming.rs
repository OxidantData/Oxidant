//! Stage 3 — output naming pass (post-plan).
//!
//! A drop-in Spark replacement must return the same *result column names* as Spark — BI tools,
//! `df.columns`, and `CREATE TABLE AS` all depend on them. Spark derives names for anonymous
//! (un-aliased) projection expressions with its `Expression.sql` / `prettyName` algorithm;
//! DataFusion's auto-generated names diverge (`Int64(1)` vs `1`, `count(*)`-rendering, `t.a` vs
//! `a`, …). This pass is a registry of per-expression naming rules so each Spark naming idiom
//! is an independent, separately-testable rule (the "Spark output-name reconciliation" ticket
//! plugs the real rules in; `oxidant-loom::spark_names` already carries a first implementation
//! of the full algorithm on the engine path).
//!
//! The pass reads the **top** projection's expressions and, when any rule produces a different
//! name than DataFusion's output field, wraps the whole plan in one **outer** renaming
//! projection: `SELECT col0 AS spark0, … FROM (original plan)`. Wrapping rather than mutating
//! the inner projection is load-bearing: a `Sort`/`Filter` above the projection references its
//! output columns *by name*, so renaming in place breaks resolution. Only names change; types
//! and row order are untouched. If the rename would produce duplicate output names (Spark
//! permits `SELECT 1, 1`; DataFusion projections forbid it) the plan is left unchanged rather
//! than regressing.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::Column;
use datafusion::logical_expr::{Expr, LogicalPlan, Projection};

/// A per-expression output-naming rule. See the module docs for the contract.
pub trait NamingRule: Send + Sync {
    /// Stable rule name, used in diagnostics.
    fn name(&self) -> &'static str;

    /// Spark's output name for a top-projection expression, or `None` to defer to the next
    /// rule (and ultimately to DataFusion's name).
    fn spark_name(&self, expr: &Expr) -> Option<String>;
}

/// Wrap `plan` in an outer projection that renames anonymous output columns to the names the
/// `rules` produce. Returns the plan unchanged when there are no rules, when nothing fires,
/// when the root isn't a projection, or when the rename would collide on output names (all
/// safe no-ops).
pub fn project_spark_names(plan: LogicalPlan, rules: &[Arc<dyn NamingRule>]) -> LogicalPlan {
    if rules.is_empty() {
        return plan;
    }
    let LogicalPlan::Projection(proj) = &plan else {
        return plan;
    };
    let schema = plan.schema();
    // The root's output fields must line up 1:1 with the projection's expressions; if they
    // somehow don't, bail rather than mis-map.
    if schema.fields().len() != proj.expr.len() {
        return plan;
    }

    let mut outer: Vec<Expr> = Vec::with_capacity(proj.expr.len());
    let mut seen: HashSet<String> = HashSet::with_capacity(proj.expr.len());
    let mut changed = false;
    for (i, pe) in proj.expr.iter().enumerate() {
        let (qualifier, field) = schema.qualified_field(i);
        let col = Expr::Column(Column::new(qualifier.cloned(), field.name()));
        // First rule to claim the expression wins; explicit user aliases simply see no rule
        // fire (rules target anonymous-expression shapes) and keep their chosen name.
        let out_name = rules
            .iter()
            .find_map(|r| r.spark_name(pe))
            .unwrap_or_else(|| field.name().to_string());
        if out_name != *field.name() {
            changed = true;
        }
        // Duplicate output names would make `Projection::try_new` reject the plan — bail and
        // keep DataFusion's (distinct) names instead.
        if !seen.insert(out_name.clone()) {
            return plan;
        }
        outer.push(if out_name == *field.name() {
            col
        } else {
            col.alias(out_name)
        });
    }

    if !changed {
        return plan;
    }
    let input = Arc::new(plan);
    match Projection::try_new(outer, Arc::clone(&input)) {
        Ok(p) => LogicalPlan::Projection(p),
        // Unreachable in practice (columns come from the input schema and names are unique),
        // but never panic the engine: fall back to the original plan.
        Err(_) => (*input).clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{lit, LogicalPlanBuilder};

    /// Toy rule standing in for the real Spark naming rules: integer literals are named by
    /// their value (`Int64(1)` → `1`).
    struct IntLiteralName;

    impl NamingRule for IntLiteralName {
        fn name(&self) -> &'static str {
            "int-literal-name"
        }

        fn spark_name(&self, expr: &Expr) -> Option<String> {
            match expr {
                Expr::Literal(ScalarValue::Int64(Some(v)), _) => Some(v.to_string()),
                _ => None,
            }
        }
    }

    /// Toy rule that renames *everything* to one fixed name (drives the collision path).
    struct FixedName;

    impl NamingRule for FixedName {
        fn name(&self) -> &'static str {
            "fixed-name"
        }

        fn spark_name(&self, _expr: &Expr) -> Option<String> {
            Some("x".to_string())
        }
    }

    fn rule(r: impl NamingRule + 'static) -> Arc<dyn NamingRule> {
        Arc::new(r)
    }

    fn output_names(plan: &LogicalPlan) -> Vec<&str> {
        plan.schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect()
    }

    #[test]
    fn renames_anonymous_exprs_and_keeps_shape() {
        let plan = LogicalPlanBuilder::empty(false)
            .project(vec![lit(1i64), lit(2i64)])
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(output_names(&plan), ["Int64(1)", "Int64(2)"]);

        let renamed = project_spark_names(plan, &[rule(IntLiteralName)]);
        assert_eq!(output_names(&renamed), ["1", "2"]);
        // The rename is an *outer* projection: the inner plan is preserved verbatim.
        let LogicalPlan::Projection(outer) = &renamed else {
            panic!("expected outer projection, got {renamed:?}");
        };
        assert!(
            matches!(outer.input.as_ref(), LogicalPlan::Projection(_)),
            "inner plan must be the original projection"
        );
    }

    #[test]
    fn no_rules_is_a_no_op() {
        let plan = LogicalPlanBuilder::empty(false)
            .project(vec![lit(1i64)])
            .unwrap()
            .build()
            .unwrap();
        let out = project_spark_names(plan.clone(), &[]);
        assert_eq!(out, plan);
    }

    #[test]
    fn non_projection_root_is_a_no_op() {
        let plan = LogicalPlanBuilder::empty(false).build().unwrap();
        let out = project_spark_names(plan.clone(), &[rule(IntLiteralName)]);
        assert_eq!(out, plan);
    }

    #[test]
    fn rule_miss_keeps_datafusion_names() {
        let plan = LogicalPlanBuilder::empty(false)
            .project(vec![lit("hello")])
            .unwrap()
            .build()
            .unwrap();
        let out = project_spark_names(plan.clone(), &[rule(IntLiteralName)]);
        assert_eq!(out, plan);
    }

    #[test]
    fn duplicate_output_names_bail_out() {
        // `SELECT 1, 2` with a rule that maps both to `x`: Spark permits duplicate output
        // names, DataFusion projections don't — the pass must leave the plan unchanged.
        let plan = LogicalPlanBuilder::empty(false)
            .project(vec![lit(1i64), lit(2i64)])
            .unwrap()
            .build()
            .unwrap();
        let out = project_spark_names(plan.clone(), &[rule(FixedName)]);
        assert_eq!(out, plan);
    }

    #[test]
    fn first_claiming_rule_wins() {
        let plan = LogicalPlanBuilder::empty(false)
            .project(vec![lit(7i64)])
            .unwrap()
            .build()
            .unwrap();
        let out = project_spark_names(plan, &[rule(IntLiteralName), rule(FixedName)]);
        assert_eq!(output_names(&out), ["7"]);
    }

    #[test]
    fn later_rule_fires_where_the_earlier_one_declines() {
        // Per-column fallthrough: `IntLiteralName` claims the integer, declines the string, and
        // `FixedName` names the column it left behind.
        let plan = LogicalPlanBuilder::empty(false)
            .project(vec![lit(7i64), lit("s")])
            .unwrap()
            .build()
            .unwrap();
        let out = project_spark_names(plan, &[rule(IntLiteralName), rule(FixedName)]);
        assert_eq!(output_names(&out), ["7", "x"]);
    }

    #[test]
    fn a_rule_that_reproduces_the_datafusion_name_is_a_no_op() {
        // Nothing changed → no wrapping projection at all, so the pass never costs a plan node
        // it does not need.
        struct EchoName;
        impl NamingRule for EchoName {
            fn name(&self) -> &'static str {
                "echo-name"
            }
            fn spark_name(&self, _expr: &Expr) -> Option<String> {
                Some("Int64(1)".to_string())
            }
        }
        let plan = LogicalPlanBuilder::empty(false)
            .project(vec![lit(1i64)])
            .unwrap()
            .build()
            .unwrap();
        let out = project_spark_names(plan.clone(), &[rule(EchoName)]);
        assert_eq!(out, plan);
    }

    #[test]
    fn renaming_preserves_types_and_user_aliases() {
        // A user-written alias is not an anonymous expression, so no rule sees a bare literal
        // there and the chosen name survives; only the un-aliased column is renamed.
        let plan = LogicalPlanBuilder::empty(false)
            .project(vec![lit(1i32).alias("keep_me"), lit(9i64)])
            .unwrap()
            .build()
            .unwrap();
        let before: Vec<_> = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect();

        let out = project_spark_names(plan, &[rule(IntLiteralName)]);
        assert_eq!(output_names(&out), ["keep_me", "9"]);
        // Only the header changed — the column types are untouched.
        let after: Vec<_> = out
            .schema()
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        assert_eq!(before, after);
    }
}
