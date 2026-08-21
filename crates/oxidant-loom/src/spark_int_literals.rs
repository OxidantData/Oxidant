//! Spark-compatible integer-literal typing (`INT` vs `BIGINT` default).
//!
//! Spark types an integer literal as the narrowest of `INT` (Arrow `Int32`) / `BIGINT` (`Int64`)
//! that holds it: a literal in the signed-32-bit range is `IntegerType`, a wider one `LongType`.
//! DataFusion's SQL planner *always* types an integer literal as `Int64`. So every Spark result
//! column whose type is driven by an in-range integer literal — `SELECT 1` (`struct<1:int>`),
//! `array(1, 2)` (`array<int>`), a `VALUES`-defined view column (`struct<a:int,b:int>`) — shows up
//! in oxidant as `bigint`. This is the single biggest remaining purely-type divergence in the parity
//! corpus (concentrated in `typeCoercion/native/*`, `group-by*`, `natural-join`, `having`, …).
//!
//! [`downcast_int_literals`] rewrites every in-range `Int64` **literal** in a freshly-planned
//! (pre-analysis) logical plan to `Int32`, exactly matching Spark's literal typing. Because it runs
//! on the **raw** plan — before DataFusion's `TypeCoercion` analyzer pass — coercion then re-derives
//! every downstream type from the `Int32` literals just as Spark does: `int + int → int`,
//! `int + bigint → bigint`, `sum(int) → bigint`. This is a faithful **type-semantics** change, not a
//! cosmetic display patch: the executed result's Arrow types (what `df.schema` / Spark Connect
//! report, and what the parity harness compares) become Spark's.
//!
//! Faithfulness boundaries:
//! - Literals **outside** the i32 range stay `Int64` (Spark's `LongType`).
//! - A literal inside `CAST(… AS BIGINT)` / `1L` keeps its `bigint` result type (the cast wins);
//!   downcasting the *inner* literal is a value-preserving no-op on the cast's output.
//! - `count(*)`, explicit `BIGINT` columns, and other non-literal `Int64` producers are untouched.
//!
//! Implementation notes:
//! - `Values` needs special care: neither [`LogicalPlan::recompute_schema`] nor the `TypeCoercion`
//!   pass recomputes a `Values` node's stored schema, so a column there is downcast only when its
//!   declared type is `Int64` **and every cell** is an in-range `Int64` literal (or a typed `Int64`
//!   `NULL`); the stored schema field types and the cell literals are rewritten together so the node
//!   stays internally consistent.
//! - Every other node is rewritten expression-wise, its **output column names preserved** (via the
//!   same [`NamePreserver`] the `TypeCoercion` pass uses), and its schema recomputed. Name
//!   preservation is load-bearing: an anonymous column's auto-name embeds the literal type
//!   (`Int64(1)`, `hav.v + Int64(1)`), and HAVING / `EXCEPT` / `INTERSECT` / lateral correlations
//!   reference that exact string. Retyping the literal to `Int32(1)` without preserving the name
//!   would rename the column and break those by-name references; restoring the original name as an
//!   alias keeps every reference resolvable while the *type* still flows through as `Int32`. The
//!   Spark output-name pass ([`crate::spark_names`]) must therefore run *before* this rewrite (it
//!   renames the still-bare literal columns to their Spark names; this rewrite then preserves the
//!   names its outer projection now references).
//! - If any node cannot be re-validated on the raw (un-coerced) plan, the whole rewrite is abandoned
//!   and the original plan is returned unchanged — never an error, never a partial/inconsistent plan.

use std::cell::Cell;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, Result, ScalarValue, TableReference};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{Expr, LogicalPlan, Values};

const I32_MIN: i64 = i32::MIN as i64;
const I32_MAX: i64 = i32::MAX as i64;

/// Does `v` fit Spark's `IntegerType` (signed 32-bit) range?
#[inline]
fn fits_i32(v: i64) -> bool {
    (I32_MIN..=I32_MAX).contains(&v)
}

/// Downcast a single in-range `Int64` literal to `Int32`, preserving field metadata. Anything else
/// (out-of-range `Int64`, a `NULL`/`None` `Int64`, any other type) is left untouched.
fn downcast_one(expr: Expr) -> Transformed<Expr> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(v)), meta) if fits_i32(v) => {
            Transformed::yes(Expr::Literal(ScalarValue::Int32(Some(v as i32)), meta))
        }
        other => Transformed::no(other),
    }
}

/// Rewrite every in-range `Int64` literal nested anywhere inside one expression (binary ops,
/// function arguments, `array(…)`, `CASE`, …). Subquery-nested plans are not descended here; they
/// are reached, if at all, by the plan-level walk.
fn rewrite_expr(expr: Expr) -> Result<Transformed<Expr>> {
    expr.transform_down(|e| Ok(downcast_one(e)))
}

/// Downcast the safe columns of a `Values` node, rewriting its stored schema field types and its
/// cell literals together so the node stays internally consistent. A column is eligible only when
/// its declared type is exactly `Int64` and every cell in it is an in-range `Int64` literal (or a
/// typed `Int64` `NULL`); otherwise the column is left exactly as-is (so e.g. a column that mixes
/// an `Int64` and a `Double` keeps DataFusion's common-type coercion).
fn rewrite_values(values: Values) -> Transformed<LogicalPlan> {
    let Values {
        schema,
        values: rows,
    } = values;
    let ncols = schema.fields().len();

    let mut downcast = vec![false; ncols];
    for (j, slot) in downcast.iter_mut().enumerate() {
        if schema.field(j).data_type() != &DataType::Int64 {
            continue;
        }
        *slot = rows.iter().all(|row| match &row[j] {
            Expr::Literal(ScalarValue::Int64(Some(v)), _) => fits_i32(*v),
            Expr::Literal(ScalarValue::Int64(None), _) => true,
            _ => false,
        });
    }
    if !downcast.iter().any(|&b| b) {
        return Transformed::no(LogicalPlan::Values(Values {
            schema,
            values: rows,
        }));
    }

    // New schema: only the eligible columns' types change to Int32; names, qualifiers, nullability
    // and per-column metadata are preserved exactly.
    let qualified_fields: Vec<(Option<TableReference>, Arc<Field>)> = schema
        .iter()
        .enumerate()
        .map(|(j, (qualifier, field))| {
            let new_field = if downcast[j] {
                Arc::new(
                    Field::new(field.name(), DataType::Int32, field.is_nullable())
                        .with_metadata(field.metadata().clone()),
                )
            } else {
                Arc::clone(field)
            };
            (qualifier.cloned(), new_field)
        })
        .collect();
    let new_schema = match DFSchema::new_with_metadata(qualified_fields, schema.metadata().clone())
    {
        Ok(s) => Arc::new(s),
        // Should never happen (we only changed types, not names), but never panic the engine.
        Err(_) => {
            return Transformed::no(LogicalPlan::Values(Values {
                schema,
                values: rows,
            }))
        }
    };

    let new_rows: Vec<Vec<Expr>> = rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .enumerate()
                .map(|(j, cell)| {
                    if !downcast[j] {
                        return cell;
                    }
                    match cell {
                        Expr::Literal(ScalarValue::Int64(Some(v)), meta) => {
                            Expr::Literal(ScalarValue::Int32(Some(v as i32)), meta)
                        }
                        Expr::Literal(ScalarValue::Int64(None), meta) => {
                            Expr::Literal(ScalarValue::Int32(None), meta)
                        }
                        other => other,
                    }
                })
                .collect()
        })
        .collect();

    Transformed::yes(LogicalPlan::Values(Values {
        schema: new_schema,
        values: new_rows,
    }))
}

/// Rewrite a raw (pre-analysis) logical plan so every in-range `Int64` literal becomes `Int32`,
/// matching Spark's integer-literal typing. Returns the plan unchanged when no eligible literal is
/// present, or if any node cannot be safely re-validated on the raw plan (a conservative, never-an-
/// error fallback). Recurses into `CREATE VIEW`/`CREATE TABLE AS` bodies so view/table column types
/// match Spark too.
pub fn downcast_int_literals(plan: LogicalPlan) -> LogicalPlan {
    // Tracks whether an actual literal was downcast anywhere. If not, we return the original plan
    // untouched (zero behavioral change for literal-free plans / out-of-range-only literals).
    let changed = Cell::new(false);

    let rewritten = plan.clone().transform_up(|node| match node {
        LogicalPlan::Values(values) => {
            let t = rewrite_values(values);
            if t.transformed {
                changed.set(true);
            }
            Ok(t)
        }
        other => {
            // Preserve each output expression's DataFusion-generated column name across the
            // retype, *exactly* as `TypeCoercion` does: an anonymous column's auto-name embeds the
            // literal's type (`hav.v + Int64(1)`, `Int64(1)`), and that name is what HAVING / set
            // operations / lateral correlations reference by string. Renaming it (Int64→Int32)
            // would break those internal references; restoring the original name via an alias keeps
            // every reference resolvable while the *type* still flows through as Int32. (For
            // Filter/Join/etc., whose expressions don't contribute to the output schema,
            // `NamePreserver` is a no-op — matching DataFusion's own behavior.)
            let preserver = NamePreserver::new(&other);
            let mut node_changed = false;
            let t = other.map_expressions(|expr| {
                let saved = preserver.save(&expr);
                let r = rewrite_expr(expr)?;
                node_changed |= r.transformed;
                Ok(r.update_data(|e| saved.restore(e)))
            })?;
            if node_changed {
                changed.set(true);
            }
            // Recompute this node's schema from its (already bottom-up-rewritten) inputs and new
            // expressions, so the intermediate plan stays internally consistent. On the rare raw
            // node that can't yet be re-validated, the `?` abandons the whole rewrite and we fall
            // back to the original plan (never an error, never a partial/inconsistent plan).
            let node = t.data.recompute_schema()?;
            Ok(Transformed::yes(node))
        }
    });

    match rewritten {
        Ok(t) if changed.get() => t.data,
        // No literal changed, or a node failed to re-validate: keep the original plan verbatim.
        _ => plan,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::expr::FieldMetadata;

    fn int64(v: i64) -> Expr {
        Expr::Literal(ScalarValue::Int64(Some(v)), None)
    }

    #[test]
    fn downcasts_in_range_int64_literal() {
        let t = downcast_one(int64(1));
        assert!(t.transformed);
        assert_eq!(t.data, Expr::Literal(ScalarValue::Int32(Some(1)), None));
    }

    #[test]
    fn keeps_out_of_range_int64_literal() {
        let big = 2_147_483_648i64; // i32::MAX + 1
        let t = downcast_one(int64(big));
        assert!(!t.transformed);
        assert_eq!(t.data, int64(big));
        // Boundaries: i32::MIN and i32::MAX are in range.
        assert!(downcast_one(int64(I32_MAX)).transformed);
        assert!(downcast_one(int64(I32_MIN)).transformed);
        assert!(!downcast_one(int64(I32_MAX + 1)).transformed);
        assert!(!downcast_one(int64(I32_MIN - 1)).transformed);
    }

    #[test]
    fn keeps_null_int64_literal() {
        let t = downcast_one(Expr::Literal(ScalarValue::Int64(None), None));
        assert!(!t.transformed);
    }

    #[test]
    fn preserves_literal_metadata() {
        let meta = FieldMetadata::from(std::collections::HashMap::from([(
            "k".to_string(),
            "v".to_string(),
        )]));
        let lit = Expr::Literal(ScalarValue::Int64(Some(7)), Some(meta.clone()));
        let t = downcast_one(lit);
        assert!(t.transformed);
        assert_eq!(
            t.data,
            Expr::Literal(ScalarValue::Int32(Some(7)), Some(meta))
        );
    }

    #[test]
    fn rewrites_nested_expr_literals() {
        use datafusion::logical_expr::{col, BinaryExpr, Operator};
        // (a + 1) -> (a + Int32(1))
        let e = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("a")),
            Operator::Plus,
            Box::new(int64(1)),
        ));
        let t = rewrite_expr(e).unwrap();
        assert!(t.transformed);
        let Expr::BinaryExpr(b) = t.data else {
            panic!("expected binary expr")
        };
        assert_eq!(*b.right, Expr::Literal(ScalarValue::Int32(Some(1)), None));
    }
}
