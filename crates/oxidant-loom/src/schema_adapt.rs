//! Case-insensitive file→table column matching for the **declared-schema** read path.
//!
//! # The problem
//!
//! When a catalog (AWS Glue / Hive Metastore) hands us an authoritative table schema, we attach it
//! to the `ListingTable` so the engine reads files *against* it (casting physically-mismatched
//! types — `Int32`→`Int64` etc. — at scan time). DataFusion 54 performs that adaptation through a
//! [`PhysicalExprAdapter`](datafusion::physical_expr_adapter::PhysicalExprAdapter): the Parquet
//! opener rewrites each projected column expression to the physical file schema, resolving columns
//! **by name** (`physical_file_schema.index_of(name)`), which is **case-sensitive**.
//!
//! Glue lowercases column names (`vendorid`), while the Parquet files often store mixed case
//! (`VendorID`). The default adapter's case-sensitive `index_of("vendorid")` misses `VendorID`, so
//! the column is treated as *missing from the file* and filled with NULLs — mixed-case columns read
//! back null even though the data is right there. Databricks / Spark-on-Glue resolve this
//! case-insensitively (`spark.sql.caseSensitive=false`); we match that.
//!
//! # The fix
//!
//! [`CaseInsensitiveExprAdapterFactory`] installs a [`PhysicalExprAdapter`] that rewrites each
//! `Column` reference against the file schema **ignoring ASCII case**:
//!
//! - **Found (case-insensitive):** emit a `Column` carrying the file's *actual* name and index, then
//!   wrap it in a `CastExpr` to the table field's type when the two differ (so `Int32`→`Int64` and
//!   friends still work — exactly as DataFusion's default adapter does).
//! - **Genuinely absent:** fall back to a typed NULL literal (nullable columns) or an error
//!   (non-nullable) — same contract as the default adapter.
//!
//! Emitting the file's real name (not the table spelling) is essential: a later stage in the
//! Parquet opener (`reassign_expr_columns`) re-resolves each projected `Column` **by name** against
//! the narrowed file stream schema, so a rewritten column must use the file's name to survive it.
//!
//! We only install this on the declared-schema branch of `build_listing_table`, so the
//! schema-inference path is byte-for-byte unchanged.

use std::sync::Arc;

use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{exec_err, Result as DfResult, ScalarValue};
use datafusion::physical_expr::expressions::{CastExpr, Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};

/// A [`PhysicalExprAdapterFactory`] that resolves file→table column names **case-insensitively**
/// and casts to the table field's type, matching Spark-on-Glue's `caseSensitive=false` behavior.
#[derive(Debug, Default)]
pub(crate) struct CaseInsensitiveExprAdapterFactory;

impl PhysicalExprAdapterFactory for CaseInsensitiveExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> DfResult<Arc<dyn PhysicalExprAdapter>> {
        Ok(Arc::new(CaseInsensitiveExprAdapter {
            logical_file_schema,
            physical_file_schema,
        }))
    }
}

#[derive(Debug)]
struct CaseInsensitiveExprAdapter {
    /// The table (catalog-declared) schema columns the expressions reference.
    logical_file_schema: SchemaRef,
    /// The actual schema of the file being scanned.
    physical_file_schema: SchemaRef,
}

impl PhysicalExprAdapter for CaseInsensitiveExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> DfResult<Arc<dyn PhysicalExpr>> {
        expr.transform_down(|e| {
            if let Some(column) = e.downcast_ref::<Column>() {
                let mut t = self.rewrite_column(Arc::clone(&e), column)?;
                // A column is a leaf; once rewritten (e.g. into `cast(file_col)`) we must NOT
                // descend into the replacement, or we'd re-visit the inner column and wrap it
                // again — an infinite loop. Jump past the new subtree.
                if t.transformed {
                    t.tnr = TreeNodeRecursion::Jump;
                }
                return Ok(t);
            }
            Ok(Transformed::no(e))
        })
        .data()
    }
}

impl CaseInsensitiveExprAdapter {
    fn rewrite_column(
        &self,
        expr: Arc<dyn PhysicalExpr>,
        column: &Column,
    ) -> DfResult<Transformed<Arc<dyn PhysicalExpr>>> {
        // The table field this reference targets (its name comes from the table schema).
        let logical_field = match self.logical_file_schema.field_with_name(column.name()) {
            Ok(f) => f,
            // Unknown to the table schema — leave it for someone else (mirrors the default adapter).
            Err(_) => return Ok(Transformed::no(expr)),
        };

        // Find the file column whose name matches ignoring ASCII case. Prefer an exact match so a
        // table with both `Id` and `id` (rare) stays deterministic.
        let phys_idx = self
            .physical_file_schema
            .index_of(column.name())
            .ok()
            .or_else(|| {
                self.physical_file_schema
                    .fields()
                    .iter()
                    .position(|f| f.name().eq_ignore_ascii_case(column.name()))
            });

        let Some(idx) = phys_idx else {
            // Genuinely missing from the file: fill with a typed NULL (nullable) or error.
            if !logical_field.is_nullable() {
                return exec_err!(
                    "Non-nullable column '{}' is missing from the file schema",
                    column.name()
                );
            }
            let null_value = ScalarValue::Null.cast_to(logical_field.data_type())?;
            return Ok(Transformed::yes(Arc::new(Literal::new_with_metadata(
                null_value,
                Some(FieldMetadata::from(logical_field)),
            ))));
        };

        let physical_field = self.physical_file_schema.field(idx);
        // Emit a column carrying the FILE's actual name + index (positional read), so the opener's
        // later name-based `reassign_expr_columns` against the file stream schema resolves it.
        let file_column = Arc::new(Column::new(physical_field.name(), idx));

        // Same name (case-insensitively differs only) AND same type/nullability/metadata → still
        // need the rename to the file column; only skip the cast when fields are truly equal.
        if logical_field == physical_field {
            // Identical field — index may differ from the original `column`; use the file column.
            return Ok(Transformed::yes(file_column));
        }

        // Differ in type (and/or metadata/nullability): cast the file column to the table field.
        if logical_field.data_type() != physical_field.data_type()
            && !can_cast_types(physical_field.data_type(), logical_field.data_type())
        {
            // Arrow has no *direct* cast for this pair, but an integer source can often reach the
            // target through an `Int32`/`Int64` bridge. The motivating case is ClickBench's
            // `EventDate`, stored in Parquet as `UInt16` (days since the epoch) yet declared `DATE`
            // (`Date32`) in Glue: arrow rejects `UInt16`->`Date32` outright, but `UInt16`->`Int32`
            // and `Int32`->`Date32` both exist and compose to the correct day count. We only bridge
            // integer sources, so string/float->date (which arrow casts directly, or rejects with a
            // clear error) keep their existing behavior.
            if let Some(bridge) =
                integer_cast_bridge(physical_field.data_type(), logical_field.data_type())
            {
                let mid: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(file_column, bridge, None));
                return Ok(Transformed::yes(Arc::new(CastExpr::new_with_target_field(
                    mid,
                    Arc::new(logical_field.clone()),
                    None,
                ))));
            }
            return exec_err!(
                "Cannot cast column '{}' from '{}' (file) to '{}' (table)",
                column.name(),
                physical_field.data_type(),
                logical_field.data_type()
            );
        }
        Ok(Transformed::yes(Arc::new(CastExpr::new_with_target_field(
            file_column,
            Arc::new(logical_field.clone()),
            None,
        ))))
    }
}

/// When `from`->`to` has no direct arrow cast, find an `Int32`/`Int64` bridge type `b` such that
/// `from`->`b`->`to` are both castable — but only for **integer** sources. This rescues integer
/// columns declared as a temporal type (e.g. `UInt16` days -> `Date32`) without affecting the
/// string/float cases arrow already handles (or rejects) directly.
fn integer_cast_bridge(from: &DataType, to: &DataType) -> Option<DataType> {
    let is_integer = matches!(
        from,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    );
    if !is_integer {
        return None;
    }
    [DataType::Int32, DataType::Int64]
        .into_iter()
        .find(|bridge| can_cast_types(from, bridge) && can_cast_types(bridge, to))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn field(name: &str, dt: DataType) -> Field {
        Field::new(name, dt, true)
    }

    fn rewrite(
        logical: Schema,
        physical: Schema,
        expr: Arc<dyn PhysicalExpr>,
    ) -> DfResult<Arc<dyn PhysicalExpr>> {
        let adapter = CaseInsensitiveExprAdapter {
            logical_file_schema: Arc::new(logical),
            physical_file_schema: Arc::new(physical),
        };
        adapter.rewrite(expr)
    }

    #[test]
    fn case_insensitive_match_inserts_cast_with_file_name() {
        // Table column `vendorid` Int64, file column `VendorID` Int32.
        let logical = Schema::new(vec![field("vendorid", DataType::Int64)]);
        let physical = Schema::new(vec![field("VendorID", DataType::Int32)]);
        let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("vendorid", 0));
        let out = rewrite(logical, physical, col).unwrap();
        // Wrapped in a cast to Int64...
        let cast = out.downcast_ref::<CastExpr>().expect("cast inserted");
        assert_eq!(cast.cast_type(), &DataType::Int64);
        // ...over a column carrying the FILE's actual name (so downstream name-resolution works).
        let inner = cast.expr().downcast_ref::<Column>().expect("inner column");
        assert_eq!(inner.name(), "VendorID");
        assert_eq!(inner.index(), 0);
    }

    #[test]
    fn exact_match_same_type_keeps_file_column() {
        let logical = Schema::new(vec![field("total_amount", DataType::Float64)]);
        let physical = Schema::new(vec![field("total_amount", DataType::Float64)]);
        let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("total_amount", 0));
        let out = rewrite(logical, physical, col).unwrap();
        let c = out.downcast_ref::<Column>().expect("plain column");
        assert_eq!(c.name(), "total_amount");
    }

    #[test]
    fn missing_nullable_column_becomes_null_literal() {
        let logical = Schema::new(vec![field("absent", DataType::Int64)]);
        let physical = Schema::new(vec![field("present", DataType::Int64)]);
        let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("absent", 0));
        let out = rewrite(logical, physical, col).unwrap();
        assert!(out.downcast_ref::<Literal>().is_some());
    }

    #[test]
    fn uint16_file_to_date32_table_bridges_through_int32() {
        // ClickBench `EventDate`: Parquet `UInt16` (days) declared `DATE` (`Date32`) in Glue.
        // Arrow has no direct `UInt16`->`Date32` cast, so we must bridge `UInt16`->`Int32`->`Date32`.
        let logical = Schema::new(vec![field("eventdate", DataType::Date32)]);
        let physical = Schema::new(vec![field("eventdate", DataType::UInt16)]);
        let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("eventdate", 0));
        let out = rewrite(logical, physical, col).expect("bridged cast, not an error");
        // Outer cast targets Date32...
        let outer = out
            .downcast_ref::<CastExpr>()
            .expect("outer cast to Date32");
        assert_eq!(outer.cast_type(), &DataType::Date32);
        // ...over an inner Int32 cast of the file column.
        let inner = outer
            .expr()
            .downcast_ref::<CastExpr>()
            .expect("inner Int32 bridge cast");
        assert_eq!(inner.cast_type(), &DataType::Int32);
        let leaf = inner.expr().downcast_ref::<Column>().expect("file column");
        assert_eq!(leaf.name(), "eventdate");
    }

    #[test]
    fn integer_cast_bridge_rejects_non_integer_source() {
        // A non-integer source with no direct cast must NOT be bridged (no Float64->Date32 here).
        assert!(integer_cast_bridge(&DataType::Float64, &DataType::Date32).is_none());
        // Integer source reaches Date32 via Int32.
        assert_eq!(
            integer_cast_bridge(&DataType::UInt16, &DataType::Date32),
            Some(DataType::Int32)
        );
    }

    #[test]
    fn second_position_case_match_resolves_to_correct_index() {
        // File order differs in case only; ensure we pick the right physical index.
        let logical = Schema::new(vec![
            field("a", DataType::Int64),
            field("pulocationid", DataType::Int64),
        ]);
        let physical = Schema::new(vec![
            field("a", DataType::Int64),
            field("PULocationID", DataType::Int32),
        ]);
        let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("pulocationid", 1));
        let out = rewrite(logical, physical, col).unwrap();
        let cast = out.downcast_ref::<CastExpr>().expect("cast");
        let inner = cast.expr().downcast_ref::<Column>().unwrap();
        assert_eq!(inner.name(), "PULocationID");
        assert_eq!(inner.index(), 1);
    }
}
