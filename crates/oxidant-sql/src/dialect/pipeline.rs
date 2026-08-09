//! The staged dialect pipeline driver.
//!
//! [`DialectPipeline::lower`] runs one SQL statement through the stages in order:
//!
//! ```text
//! Engine::sql(query)
//!    │
//!    ▼
//! DialectPipeline::lower(query, &ctx)   ──►  LowerOutcome
//!    │                                    ├─ Sql(Cow<str>)          → ctx.sql()
//!    │                                    ├─ Rewritten(LogicalPlan) → ctx.execute_logical_plan()
//!    │                                    └─ Direct(Vec<RecordBatch>) → return (e.g. USE, SHOW)
//!    ▼
//! ctx.sql() → df → DialectPipeline::apply_output_naming(df.schema) → collect
//! ```
//!
//! Stage 1 ([`StrRule`]s) runs pre-parse on the raw text; the result is parsed once with the
//! Databricks dialect and handed to Stage 2 ([`StatementIntercept`]s); if no rule claims the
//! statement, the (possibly rewritten) text is returned for `ctx.sql()` to plan. Stage 3
//! ([`NamingRule`]s) is a separate post-plan step — [`DialectPipeline::apply_output_naming`] —
//! because it operates on the analyzed `LogicalPlan`, not on SQL text.
//!
//! Rule contract (all stages): **either a rule fires and fully owns the statement, or it
//! returns `None` and the next rule sees the original input unchanged.** No rule mutates
//! shared state; ordering is by specificity, and a conflict — two rules claiming the same
//! statement — is a hard error in debug builds and first-wins in release, so additions stay
//! conflict-free by construction.

use std::borrow::Cow;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;

use super::intercept::{parse_single_statement, StatementIntercept};
use super::naming::{project_spark_names, NamingRule};
use super::str_rule::{StrRule, StripTemporaryView};

/// What the dialect layer decided to do with a statement.
#[derive(Debug)]
pub enum LowerOutcome<'a> {
    /// Pass this (possibly rewritten) SQL text on to `SessionContext::sql`. Borrows the
    /// input when no Stage-1 rule fired, so the common pass-through path never allocates.
    Sql(Cow<'a, str>),
    /// An intercept rewrote the statement into a logical plan (e.g. `PIVOT`) — execute via
    /// `SessionContext::execute_logical_plan`. Boxed so the enum stays small on the common
    /// [`Self::Sql`] path (`LogicalPlan` is hundreds of bytes).
    Rewritten(Box<LogicalPlan>),
    /// An intercept fully resolved the statement itself (e.g. `SHOW`/`DESCRIBE` from catalog
    /// metadata) — return these batches to the client directly.
    Direct(Vec<RecordBatch>),
}

/// The staged Spark-SQL dialect pipeline. Cheap to build and stateless across statements;
/// construct once (e.g. [`DialectPipeline::spark`]) and reuse.
#[derive(Default)]
pub struct DialectPipeline {
    str_rules: Vec<Arc<dyn StrRule>>,
    intercepts: Vec<Arc<dyn StatementIntercept>>,
    naming_rules: Vec<Arc<dyn NamingRule>>,
}

impl DialectPipeline {
    /// An empty pipeline — every stage is a pass-through.
    pub fn new() -> Self {
        Self::default()
    }

    /// The pipeline Oxidant runs in production. Today only Stage 1 carries a rule — the
    /// `CREATE … TEMPORARY VIEW` strip migrated from `oxidant-loom::normalize_spark_sql`;
    /// Stages 2 and 3 are empty registries that later dialect tickets plug rules into.
    pub fn spark() -> Self {
        Self::new().with_str_rule(StripTemporaryView)
    }

    /// Register a Stage-1 string prefilter rule. Rules run in registration order; register
    /// more specific rules first.
    pub fn with_str_rule(mut self, rule: impl StrRule + 'static) -> Self {
        self.str_rules.push(Arc::new(rule));
        self
    }

    /// Register a Stage-2 statement intercept. Rules run in registration order; register
    /// more specific rules first.
    pub fn with_intercept(mut self, rule: impl StatementIntercept + 'static) -> Self {
        self.intercepts.push(Arc::new(rule));
        self
    }

    /// Register a Stage-3 output naming rule. Rules run in registration order.
    pub fn with_naming_rule(mut self, rule: impl NamingRule + 'static) -> Self {
        self.naming_rules.push(Arc::new(rule));
        self
    }

    /// Lower one SQL statement through Stage 1 (string prefilter) and Stage 2 (statement
    /// intercepts). Infallible: unparseable or multi-statement input is not interceptable, so
    /// it is passed through as [`LowerOutcome::Sql`] and DataFusion plans it (or reports the
    /// parse error) exactly as it would without the dialect layer.
    pub fn lower<'a>(&self, query: &'a str, ctx: &SessionContext) -> LowerOutcome<'a> {
        let sql = self.rewrite_str(query);
        // Parsing here costs as much as DataFusion's own parse, so skip it outright while
        // Stage 2 is empty — an empty registry can never claim a statement.
        if self.intercepts.is_empty() {
            return LowerOutcome::Sql(sql);
        }
        match parse_single_statement(&sql) {
            Some(stmt) => self
                .intercept_statement(&stmt, ctx)
                .unwrap_or(LowerOutcome::Sql(sql)),
            None => LowerOutcome::Sql(sql),
        }
    }

    /// Stage 1 only: run the string prefilter registry over `query`. Every rule sees the
    /// original input; the first rule to fire owns the rewrite.
    pub fn rewrite_str<'a>(&self, query: &'a str) -> Cow<'a, str> {
        let mut fired: Option<(&'static str, Cow<'a, str>)> = None;
        for rule in &self.str_rules {
            // Every rule is handed the *original* `query`: a rule that declined must never be
            // able to observe an earlier rule's rewrite.
            let Some(rewritten) = rule.try_rewrite(query) else {
                continue;
            };
            match &fired {
                None => fired = Some((rule.name(), rewritten)),
                Some((first, _)) => debug_assert!(
                    false,
                    "dialect str-rules `{first}` and `{}` both claim the same statement",
                    rule.name()
                ),
            }
            // Release builds are first-wins, so the remaining rules cannot change the answer;
            // debug builds keep scanning to surface a conflicting rule.
            if !cfg!(debug_assertions) {
                break;
            }
        }
        fired
            .map(|(_, rewritten)| rewritten)
            .unwrap_or(Cow::Borrowed(query))
    }

    /// Stage 3 only: rename the analyzed plan's anonymous output columns to Spark's names.
    pub fn apply_output_naming(&self, plan: LogicalPlan) -> LogicalPlan {
        project_spark_names(plan, &self.naming_rules)
    }

    /// Run the Stage-2 intercepts over one parsed statement; the first rule to fire owns it.
    fn intercept_statement(
        &self,
        stmt: &datafusion::sql::sqlparser::ast::Statement,
        ctx: &SessionContext,
    ) -> Option<LowerOutcome<'static>> {
        let mut fired: Option<(&'static str, LowerOutcome<'static>)> = None;
        for rule in &self.intercepts {
            let Some(outcome) = rule.intercept(stmt, ctx) else {
                continue;
            };
            match &fired {
                None => fired = Some((rule.name(), outcome)),
                Some((first, _)) => debug_assert!(
                    false,
                    "dialect intercepts `{first}` and `{}` both claim the same statement",
                    rule.name()
                ),
            }
            if !cfg!(debug_assertions) {
                break;
            }
        }
        fired.map(|(_, outcome)| outcome)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use datafusion::sql::sqlparser::ast::{Statement, Use};

    fn ctx() -> SessionContext {
        SessionContext::new()
    }

    /// Stage-1 rule that claims any statement whose first token matches `lead`, prefixing the
    /// rewrite with its own marker so tests can tell which rule fired. Records every input it
    /// was handed, which is how the "next rule sees the original input" contract is checked.
    struct LeadRule {
        rule_name: &'static str,
        lead: &'static str,
        seen: Mutex<Vec<String>>,
    }

    impl LeadRule {
        fn new(rule_name: &'static str, lead: &'static str) -> Arc<Self> {
            Arc::new(Self {
                rule_name,
                lead,
                seen: Mutex::new(Vec::new()),
            })
        }
    }

    impl StrRule for Arc<LeadRule> {
        fn name(&self) -> &'static str {
            self.rule_name
        }

        fn try_rewrite<'a>(&self, sql: &'a str) -> Option<Cow<'a, str>> {
            self.seen.lock().unwrap().push(sql.to_string());
            sql.strip_prefix(self.lead)
                .map(|rest| Cow::Owned(format!("/*{}*/{rest}", self.rule_name)))
        }
    }

    /// Toy intercept standing in for the real `USE` rule (KAN-96): claims `USE <ns>` and
    /// rewrites it to the DataFusion session-setting SQL it would emit.
    struct UseIntercept;

    impl StatementIntercept for UseIntercept {
        fn name(&self) -> &'static str {
            "use-intercept"
        }

        fn intercept(
            &self,
            statement: &Statement,
            _ctx: &SessionContext,
        ) -> Option<LowerOutcome<'static>> {
            let Statement::Use(Use::Object(name)) = statement else {
                return None;
            };
            Some(LowerOutcome::Sql(Cow::Owned(format!(
                "SET datafusion.catalog.default_schema = '{name}'"
            ))))
        }
    }

    /// Intercept that never fires — proves pass-through composes.
    struct NeverIntercept;

    impl StatementIntercept for NeverIntercept {
        fn name(&self) -> &'static str {
            "never-intercept"
        }

        fn intercept(
            &self,
            _statement: &Statement,
            _ctx: &SessionContext,
        ) -> Option<LowerOutcome<'static>> {
            None
        }
    }

    fn as_sql(outcome: LowerOutcome<'_>) -> Cow<'_, str> {
        match outcome {
            LowerOutcome::Sql(sql) => sql,
            other => panic!("expected LowerOutcome::Sql, got {other:?}"),
        }
    }

    #[test]
    fn empty_pipeline_passes_everything_through() {
        let p = DialectPipeline::new();
        let sql = "SELECT a FROM t WHERE a > 1";
        // Pass-through borrows the input — no allocation on the common path.
        assert!(matches!(
            p.lower(sql, &ctx()),
            LowerOutcome::Sql(Cow::Borrowed(_))
        ));
        assert_eq!(as_sql(p.lower(sql, &ctx())), sql);
    }

    #[test]
    fn spark_pipeline_strips_temporary_view() {
        let p = DialectPipeline::spark();
        let out = p.lower("CREATE OR REPLACE TEMPORARY VIEW v AS SELECT 1", &ctx());
        assert_eq!(as_sql(out), "CREATE OR REPLACE VIEW v AS SELECT 1");
    }

    #[test]
    fn spark_pipeline_leaves_plain_select_alone() {
        let p = DialectPipeline::spark();
        let sql = "SELECT count(1) FROM hits";
        assert!(matches!(
            p.lower(sql, &ctx()),
            LowerOutcome::Sql(Cow::Borrowed(_))
        ));
        assert_eq!(as_sql(p.lower(sql, &ctx())), sql);
    }

    #[test]
    fn intercept_claims_its_statement() {
        let p = DialectPipeline::spark().with_intercept(UseIntercept);
        let out = p.lower("USE analytics", &ctx());
        assert_eq!(
            as_sql(out),
            "SET datafusion.catalog.default_schema = 'analytics'"
        );
    }

    #[test]
    fn intercept_miss_passes_through() {
        let p = DialectPipeline::spark()
            .with_intercept(UseIntercept)
            .with_intercept(NeverIntercept);
        let sql = "SELECT 1";
        assert!(matches!(
            p.lower(sql, &ctx()),
            LowerOutcome::Sql(Cow::Borrowed(_))
        ));
    }

    #[test]
    fn stage1_rewrite_feeds_stage2_parse() {
        // The stripped statement parses cleanly, no intercept claims it, and the *rewritten*
        // text (not the original) is what flows on to ctx.sql().
        let p = DialectPipeline::spark().with_intercept(NeverIntercept);
        let out = p.lower("CREATE TEMP VIEW `v` AS SELECT 1", &ctx());
        assert_eq!(as_sql(out), "CREATE VIEW `v` AS SELECT 1");
    }

    #[test]
    fn multi_statement_passes_through_unintercepted() {
        let p = DialectPipeline::spark().with_intercept(UseIntercept);
        let sql = "USE analytics; SELECT 1";
        assert_eq!(as_sql(p.lower(sql, &ctx())), sql);
    }

    #[test]
    fn unparseable_passes_through_for_datafusion_to_report() {
        let p = DialectPipeline::spark().with_intercept(UseIntercept);
        let sql = "SELEC T FROM WAT";
        assert_eq!(as_sql(p.lower(sql, &ctx())), sql);
    }

    #[test]
    fn apply_output_naming_delegates_to_stage3() {
        use datafusion::logical_expr::{lit, LogicalPlanBuilder};

        struct IntName;
        impl NamingRule for IntName {
            fn name(&self) -> &'static str {
                "int-name"
            }
            fn spark_name(&self, expr: &datafusion::logical_expr::Expr) -> Option<String> {
                use datafusion::common::ScalarValue;
                match expr {
                    datafusion::logical_expr::Expr::Literal(ScalarValue::Int64(Some(v)), _) => {
                        Some(v.to_string())
                    }
                    _ => None,
                }
            }
        }

        let plan = LogicalPlanBuilder::empty(false)
            .project(vec![lit(42i64)])
            .unwrap()
            .build()
            .unwrap();
        // No naming rules registered → plan unchanged.
        let p = DialectPipeline::new();
        assert_eq!(p.apply_output_naming(plan.clone()), plan);
        // Registered → outer renaming projection.
        let p = DialectPipeline::new().with_naming_rule(IntName);
        let out = p.apply_output_naming(plan);
        assert_eq!(out.schema().fields()[0].name(), "42");
    }

    #[test]
    fn first_matching_str_rule_owns_the_statement() {
        // Two rules, disjoint leading tokens: each claims only its own statement and the other
        // rule's rewrite is nowhere in the output.
        let create = LeadRule::new("create-rule", "CREATE ");
        let select = LeadRule::new("select-rule", "SELECT ");
        let p = DialectPipeline::new()
            .with_str_rule(Arc::clone(&create))
            .with_str_rule(Arc::clone(&select));
        assert_eq!(p.rewrite_str("CREATE x"), "/*create-rule*/x");
        assert_eq!(p.rewrite_str("SELECT x"), "/*select-rule*/x");
        // Neither claims it → the original input is returned, borrowed.
        assert!(matches!(p.rewrite_str("DROP TABLE t"), Cow::Borrowed(_)));
    }

    #[test]
    fn every_str_rule_sees_the_original_input() {
        // The contract: a rule that fires owns the statement, and every *other* rule is still
        // handed the unmodified input — never the first rule's rewrite.
        let create = LeadRule::new("create-rule", "CREATE ");
        let spy = LeadRule::new("spy", "DROP ");
        let p = DialectPipeline::new()
            .with_str_rule(Arc::clone(&create))
            .with_str_rule(Arc::clone(&spy));
        assert_eq!(p.rewrite_str("CREATE x"), "/*create-rule*/x");
        assert_eq!(spy.seen.lock().unwrap().as_slice(), ["CREATE x"]);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "dialect str-rules `first` and `second` both claim")]
    fn conflicting_str_rules_are_a_debug_hard_error() {
        let p = DialectPipeline::new()
            .with_str_rule(LeadRule::new("first", "CREATE "))
            .with_str_rule(LeadRule::new("second", "CREATE "));
        let _ = p.rewrite_str("CREATE x");
    }

    #[test]
    #[cfg(not(debug_assertions))]
    fn conflicting_str_rules_are_first_wins_in_release() {
        let p = DialectPipeline::new()
            .with_str_rule(LeadRule::new("first", "CREATE "))
            .with_str_rule(LeadRule::new("second", "CREATE "));
        assert_eq!(p.rewrite_str("CREATE x"), "/*first*/x");
    }

    #[test]
    #[cfg(not(debug_assertions))]
    fn conflicting_intercepts_are_first_wins_in_release() {
        struct Fixed(&'static str);
        impl StatementIntercept for Fixed {
            fn name(&self) -> &'static str {
                "fixed"
            }
            fn intercept(
                &self,
                _statement: &Statement,
                _ctx: &SessionContext,
            ) -> Option<LowerOutcome<'static>> {
                Some(LowerOutcome::Sql(Cow::Borrowed(self.0)))
            }
        }
        let p = DialectPipeline::new()
            .with_intercept(Fixed("first"))
            .with_intercept(Fixed("second"));
        assert_eq!(as_sql(p.lower("SELECT 1", &ctx())), "first");
    }

    #[test]
    fn intercept_sees_the_stage1_rewrite_not_the_original() {
        // Stage 2 parses the text Stage 1 produced: the intercept below only matches
        // `CREATE VIEW`, which exists solely because `StripTemporaryView` dropped `TEMPORARY`.
        struct CreateViewIntercept;
        impl StatementIntercept for CreateViewIntercept {
            fn name(&self) -> &'static str {
                "create-view-intercept"
            }
            fn intercept(
                &self,
                statement: &Statement,
                _ctx: &SessionContext,
            ) -> Option<LowerOutcome<'static>> {
                let Statement::CreateView(cv) = statement else {
                    return None;
                };
                assert!(!cv.temporary, "Stage 1 must have dropped TEMPORARY");
                Some(LowerOutcome::Sql(Cow::Owned(cv.name.to_string())))
            }
        }
        let p = DialectPipeline::spark().with_intercept(CreateViewIntercept);
        assert_eq!(
            as_sql(p.lower("CREATE TEMPORARY VIEW v AS SELECT 1", &ctx())),
            "v"
        );
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "both claim the same statement")]
    fn conflicting_intercepts_are_a_debug_hard_error() {
        struct OtherUse;
        impl StatementIntercept for OtherUse {
            fn name(&self) -> &'static str {
                "other-use"
            }
            fn intercept(
                &self,
                statement: &Statement,
                ctx: &SessionContext,
            ) -> Option<LowerOutcome<'static>> {
                UseIntercept.intercept(statement, ctx)
            }
        }
        let p = DialectPipeline::new()
            .with_intercept(UseIntercept)
            .with_intercept(OtherUse);
        let _ = p.lower("USE analytics", &ctx());
    }
}
