//! Applies a [`TableAccess`] decision to a table's scans.
//!
//! [`LakeFormationTableProvider`] wraps the `TableProvider` the catalog bridge resolved and is what
//! actually enforces fine-grained access control at read time:
//!
//! - **Column security** — the provider's schema is the *authorized* subset, so a denied column is
//!   simply absent. `SELECT *` narrows to what the principal may see and naming a denied column is
//!   an unknown-column error. That is what Athena and EMR do; the alternative (keep the column and
//!   raise a permission error on reference) breaks every existing `SELECT *`.
//! - **Row security** — the decision's row filter is `AND`-ed into the scan, below anything the
//!   query itself does, so no operator above the scan can observe a filtered-out row.
//!
//! Wrapping at the `TableProvider` seam rather than rewriting logical plans means enforcement
//! applies to every path that can read a table — SQL, the DataFrame API, a distributed worker
//! resolving its own stage — and to every format the bridge can resolve, because it sits above the
//! format-specific providers rather than inside any one of them.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DFSchema, DataFusionError, Result as DfResult, Statistics};
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::expressions::Column as PhysicalColumn;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use oxidant_catalog::TableAccess;

/// Locate a field by name, ignoring ASCII case. Exact match wins so a schema that genuinely
/// contains both `Region` and `region` resolves the one that was named.
fn index_of_ignore_ascii_case(schema: &SchemaRef, name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .position(|f| f.name() == name)
        .or_else(|| {
            schema
                .fields()
                .iter()
                .position(|f| f.name().eq_ignore_ascii_case(name))
        })
}

/// A `TableProvider` that enforces a fine-grained access decision over an inner provider.
pub struct LakeFormationTableProvider {
    inner: Arc<dyn TableProvider>,
    /// The authorized projection of the inner schema — what every consumer above sees.
    schema: SchemaRef,
    /// Indices into the *inner* schema for each field of [`Self::schema`], in order.
    authorized_indices: Vec<usize>,
    /// The row predicate, already parsed and validated against the inner (unrestricted) schema.
    row_filter: Option<Expr>,
    /// Inner-schema indices the row filter reads. These are scanned even when the query did not
    /// ask for them, then projected away — a filter may legitimately key on a hidden column.
    filter_indices: Vec<usize>,
    /// Table name, for error messages.
    table_name: String,
}

impl std::fmt::Debug for LakeFormationTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LakeFormationTableProvider")
            .field("table", &self.table_name)
            .field("authorized_columns", &self.schema.fields().len())
            .field("row_filter", &self.row_filter.is_some())
            .finish()
    }
}

impl LakeFormationTableProvider {
    /// Wrap `inner` so its scans honor `access`.
    ///
    /// Everything fallible happens here, once, at table-resolution time rather than per scan: an
    /// unparseable row filter or a decision that authorizes no readable column must fail the query
    /// loudly, and failing at resolution gives a clearer error than failing mid-plan.
    pub fn try_new(
        state: &SessionState,
        inner: Arc<dyn TableProvider>,
        access: &TableAccess,
        table_name: &str,
    ) -> DfResult<Self> {
        let inner_schema = inner.schema();

        // Column names cross a Glue/Parquet boundary where case can differ (Glue lowercases; the
        // files may not) — the same mismatch `schema_adapt::CaseInsensitiveExprAdapterFactory`
        // exists to absorb. Match case-insensitively so a legitimate grant is not read as a denial.
        let authorized_indices: Vec<usize> = match &access.authorized_columns {
            None => (0..inner_schema.fields().len()).collect(),
            Some(allowed) => {
                let allowed: HashSet<String> =
                    allowed.iter().map(|c| c.to_ascii_lowercase()).collect();
                inner_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(_, f)| allowed.contains(&f.name().to_ascii_lowercase()))
                    .map(|(i, _)| i)
                    .collect()
            }
        };

        if authorized_indices.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "lake formation authorizes no readable column of `{table_name}` for this \
                 principal; refusing to scan it"
            )));
        }

        let schema = Arc::new(Schema::new(
            authorized_indices
                .iter()
                .map(|i| inner_schema.field(*i).clone())
                .collect::<Vec<_>>(),
        ));

        // Parse against the UNRESTRICTED schema: a row filter may key on a column the principal
        // cannot select (hide `ssn`, but restrict rows by it). A filter that will not parse is a
        // hard failure — degrading to "no filter" would silently disable row security.
        let (row_filter, filter_indices) = match access.row_filter.as_deref() {
            None => (None, Vec::new()),
            Some(sql) if sql.trim().is_empty() => (None, Vec::new()),
            Some(sql) => {
                let df_schema = DFSchema::try_from(inner_schema.as_ref().clone())?;
                let expr = state.create_logical_expr(sql, &df_schema).map_err(|e| {
                    DataFusionError::Plan(format!(
                        "lake formation row filter `{sql}` on `{table_name}` could not be \
                         parsed against the table schema: {e}. Refusing the query rather than \
                         scanning unfiltered."
                    ))
                })?;
                let mut indices: Vec<usize> = expr
                    .column_refs()
                    .iter()
                    .map(|c| {
                        // Case-insensitive for the same reason the authorized-column match is:
                        // Glue lowercases column names while the files may not, and
                        // `create_logical_expr` lowercases unquoted identifiers. An exact match
                        // would make a table with a `Region` column and a `region = 'us'` filter
                        // fail to resolve at all — entirely unqueryable, in precisely the case the
                        // column path was written to tolerate.
                        index_of_ignore_ascii_case(&inner_schema, &c.name).ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "lake formation row filter `{sql}` on `{table_name}` references \
                                 unknown column `{}`",
                                c.name
                            ))
                        })
                    })
                    .collect::<DfResult<Vec<_>>>()?;
                indices.sort_unstable();
                indices.dedup();
                (Some(expr), indices)
            }
        };

        Ok(Self {
            inner,
            schema,
            authorized_indices,
            row_filter,
            filter_indices,
            table_name: table_name.to_string(),
        })
    }

    /// Whether this wrapper changes anything. The bridge skips wrapping when it does not.
    pub fn is_noop(access: &TableAccess) -> bool {
        !access.enforced || !access.restricts_scan()
    }
}

#[async_trait]
impl TableProvider for LakeFormationTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    /// Statistics become inexact once rows are filtered: the inner provider's row count is an
    /// upper bound, not a count. Reporting it as exact would let the planner size joins and
    /// broadcasts off a number that no longer holds.
    fn statistics(&self) -> Option<Statistics> {
        let mut stats = self.inner.statistics()?;
        // `column_statistics` is POSITIONAL and must line up with `schema()`, which this provider
        // narrowed to the authorized columns. Passing the inner vector straight through leaves it
        // at the inner width, so a consumer indexing by output position reads a different column's
        // bounds — `max(amount)` picking up `region`'s, when a denied column sits before it.
        if stats.column_statistics.len() == self.inner.schema().fields().len() {
            stats.column_statistics = self
                .authorized_indices
                .iter()
                .map(|i| stats.column_statistics[*i].clone())
                .collect();
        }
        if self.row_filter.is_none() {
            return Some(stats);
        }
        Some(stats.to_inexact())
    }

    /// Delegate pushdown. Query predicates reference columns by name and every authorized name
    /// exists in the inner schema, so the inner provider can still push them into its scan — which
    /// matters at TPC-DS scale. Composition is safe because these predicates and the row filter are
    /// conjunctive: applying one inside the scan and the other above yields the same rows.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // What the caller asked for, as indices into the RESTRICTED schema.
        let requested: Vec<usize> = match projection {
            Some(p) => p.clone(),
            None => (0..self.schema.fields().len()).collect(),
        };
        // Translate to inner-schema indices, then add whatever the row filter needs to read.
        let mut inner_projection: Vec<usize> = requested
            .iter()
            .map(|i| self.authorized_indices[*i])
            .collect();
        for idx in &self.filter_indices {
            if !inner_projection.contains(idx) {
                inner_projection.push(*idx);
            }
        }

        // A limit must not be pushed below the row filter: the inner scan would stop after N
        // pre-filter rows and the query would silently return fewer than N authorized rows.
        let inner_limit = if self.row_filter.is_some() {
            None
        } else {
            limit
        };

        let plan = self
            .inner
            .scan(state, Some(&inner_projection), filters, inner_limit)
            .await?;

        let plan: Arc<dyn ExecutionPlan> = match &self.row_filter {
            None => plan,
            Some(expr) => {
                // Compile against the schema the scan ACTUALLY produced, not the declared one:
                // the Parquet reader may hand back `Utf8View` where the catalog declared `Utf8`,
                // and a predicate built against the declared types then fails at runtime with
                // `Invalid comparison operation: Utf8View == Utf8`. `Session::create_physical_expr`
                // runs type coercion, which the bare `physical_expr::create_physical_expr` does not.
                let df_schema = DFSchema::try_from(plan.schema().as_ref().clone())?;
                let predicate = state
                    .create_physical_expr(expr.clone(), &df_schema)
                    .map_err(|e| {
                        DataFusionError::Plan(format!(
                            "lake formation row filter on `{}` could not be compiled: {e}",
                            self.table_name
                        ))
                    })?;
                Arc::new(FilterExec::try_new(predicate, plan)?)
            }
        };

        // Project back to exactly the requested output. Needed whenever the row filter forced extra
        // columns into the scan; skipped when the shapes already agree.
        let scanned = plan.schema();
        let output_names: Vec<String> = requested
            .iter()
            .map(|i| self.schema.field(*i).name().clone())
            .collect();
        let already_shaped = scanned.fields().len() == output_names.len()
            && scanned
                .fields()
                .iter()
                .zip(&output_names)
                .all(|(f, want)| f.name() == want);
        if already_shaped {
            return Ok(plan);
        }

        let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = output_names
            .iter()
            .map(|name| {
                let idx = scanned.index_of(name)?;
                Ok((
                    Arc::new(PhysicalColumn::new(name, idx)) as Arc<dyn PhysicalExpr>,
                    name.clone(),
                ))
            })
            .collect::<DfResult<Vec<_>>>()?;
        Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    /// `(id INT64, region UTF8, amount INT64, ssn UTF8)` — `ssn` is the column-security subject,
    /// `region` the row-filter subject.
    fn fixture() -> (Arc<dyn TableProvider>, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("ssn", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["us", "eu", "us", "eu"])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            ],
        )
        .expect("batch");
        let table = MemTable::try_new(schema.clone(), vec![vec![batch]]).expect("memtable");
        (Arc::new(table), schema)
    }

    fn access(columns: Option<&[&str]>, row_filter: Option<&str>) -> TableAccess {
        TableAccess {
            authorized_columns: columns
                .map(|c| c.iter().map(|s| s.to_string()).collect::<Vec<_>>()),
            row_filter: row_filter.map(|s| s.to_string()),
            credentials: None,
            enforced: true,
        }
    }

    async fn query(access: TableAccess, sql: &str) -> DfResult<Vec<RecordBatch>> {
        let ctx = SessionContext::new();
        let (inner, _) = fixture();
        let provider =
            LakeFormationTableProvider::try_new(&ctx.state(), inner, &access, "customers")?;
        ctx.register_table("customers", Arc::new(provider))?;
        ctx.sql(sql).await?.collect().await
    }

    #[tokio::test]
    async fn denied_column_is_absent_from_select_star() {
        let batches = query(
            access(Some(&["id", "region", "amount"]), None),
            "SELECT * FROM customers",
        )
        .await
        .expect("select star");
        let schema = batches[0].schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "region", "amount"]);
        assert!(!names.contains(&"ssn"));
    }

    #[tokio::test]
    async fn denied_column_cannot_be_selected_by_name() {
        let err = query(
            access(Some(&["id", "region", "amount"]), None),
            "SELECT ssn FROM customers",
        )
        .await
        .expect_err("ssn is denied");
        // It is not in the schema at all, so it reads as an unknown column — no existence leak.
        assert!(format!("{err}").contains("ssn"), "{err}");
    }

    #[tokio::test]
    async fn row_filter_applies_to_select_star() {
        let batches = query(
            access(None, Some("region = 'us'")),
            "SELECT id FROM customers ORDER BY id",
        )
        .await
        .expect("filtered");
        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64")
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, vec![1, 3], "only region='us' rows");
    }

    #[tokio::test]
    async fn count_star_sees_only_authorized_rows() {
        let batches = query(
            access(None, Some("region = 'us'")),
            "SELECT COUNT(*) FROM customers",
        )
        .await
        .expect("count");
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64")
            .value(0);
        assert_eq!(n, 2, "raw table has 4 rows");
    }

    #[tokio::test]
    async fn user_predicate_and_row_filter_both_apply() {
        let batches = query(
            access(None, Some("region = 'us'")),
            "SELECT id FROM customers WHERE amount > 20 ORDER BY id",
        )
        .await
        .expect("both predicates");
        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64")
                    .values()
                    .to_vec()
            })
            .collect();
        // amount>20 alone would give {3,4}; region='us' alone {1,3}; together {3}.
        assert_eq!(ids, vec![3]);
    }

    /// The limit-below-filter trap: pushing `LIMIT` into the inner scan would stop after N
    /// pre-filter rows and return fewer than N authorized rows.
    #[tokio::test]
    async fn limit_is_applied_after_the_row_filter_not_before() {
        let batches = query(
            access(None, Some("region = 'us'")),
            "SELECT id FROM customers ORDER BY id LIMIT 2",
        )
        .await
        .expect("limit");
        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64")
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(
            ids,
            vec![1, 3],
            "2 authorized rows, not 2 raw rows then filtered"
        );
    }

    /// A filter may key on a column the principal cannot see. The scan reads it, filters, and
    /// projects it away — it must never become selectable.
    #[tokio::test]
    async fn row_filter_over_a_hidden_column_works_without_exposing_it() {
        let a = access(Some(&["id", "amount"]), Some("ssn IN ('a', 'c')"));
        let batches = query(a.clone(), "SELECT id FROM customers ORDER BY id")
            .await
            .expect("filter on hidden column");
        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64")
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, vec![1, 3]);

        let err = query(a, "SELECT ssn FROM customers")
            .await
            .expect_err("ssn still hidden");
        assert!(format!("{err}").contains("ssn"), "{err}");
    }

    #[tokio::test]
    async fn select_star_projection_is_correct_when_filter_adds_columns() {
        // `region` is hidden but drives the filter — `SELECT *` must still return exactly
        // (id, amount), not leak the extra scanned column.
        let batches = query(
            access(Some(&["id", "amount"]), Some("region = 'us'")),
            "SELECT * FROM customers ORDER BY id",
        )
        .await
        .expect("star with filter-only column");
        let schema = batches[0].schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "amount"]);
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    }

    #[tokio::test]
    async fn column_matching_is_case_insensitive() {
        // Glue lowercases column names; the underlying files may not. A case difference must not
        // read as a denial.
        let batches = query(
            access(Some(&["ID", "Region"]), None),
            "SELECT * FROM customers",
        )
        .await
        .expect("case-insensitive match");
        let schema = batches[0].schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "region"]);
    }

    #[tokio::test]
    async fn unparseable_row_filter_fails_instead_of_scanning_unfiltered() {
        let err = query(
            access(None, Some("this is not sql (((")),
            "SELECT * FROM customers",
        )
        .await
        .expect_err("must not silently drop the filter");
        assert!(format!("{err}").contains("row filter"), "{err}");
    }

    #[tokio::test]
    async fn row_filter_on_unknown_column_fails_closed() {
        let err = query(
            access(None, Some("nonexistent_col = 1")),
            "SELECT * FROM customers",
        )
        .await
        .expect_err("unknown column in filter");
        let msg = format!("{err}");
        assert!(msg.contains("nonexistent_col"), "{msg}");
    }

    #[tokio::test]
    async fn authorizing_no_readable_column_is_refused() {
        let ctx = SessionContext::new();
        let (inner, _) = fixture();
        let err = LakeFormationTableProvider::try_new(
            &ctx.state(),
            inner,
            &access(Some(&["column_that_does_not_exist"]), None),
            "customers",
        )
        .expect_err("no authorized column is a denial, not an empty table");
        assert!(format!("{err}").contains("no readable column"), "{err}");
    }

    #[test]
    fn noop_detection_skips_wrapping_when_nothing_is_restricted() {
        assert!(LakeFormationTableProvider::is_noop(
            &TableAccess::unenforced()
        ));
        assert!(LakeFormationTableProvider::is_noop(&access(None, None)));
        assert!(!LakeFormationTableProvider::is_noop(&access(
            Some(&["id"]),
            None
        )));
        assert!(!LakeFormationTableProvider::is_noop(&access(
            None,
            Some("region = 'us'")
        )));
    }

    #[tokio::test]
    async fn statistics_go_inexact_only_when_rows_are_filtered() {
        use datafusion::common::stats::Precision;
        let ctx = SessionContext::new();
        let (inner, _) = fixture();

        // Column-only restriction keeps the row count exact.
        let cols_only = LakeFormationTableProvider::try_new(
            &ctx.state(),
            inner.clone(),
            &access(Some(&["id"]), None),
            "customers",
        )
        .expect("provider");
        if let Some(stats) = cols_only.statistics() {
            assert!(matches!(stats.num_rows, Precision::Exact(_)), "{stats:?}");
        }

        // With a row filter the inner count is only an upper bound.
        let filtered = LakeFormationTableProvider::try_new(
            &ctx.state(),
            inner,
            &access(None, Some("region = 'us'")),
            "customers",
        )
        .expect("provider");
        if let Some(stats) = filtered.statistics() {
            assert!(!matches!(stats.num_rows, Precision::Exact(_)), "{stats:?}");
        }
    }
}
