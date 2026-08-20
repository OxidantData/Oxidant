//! AUTO CDC (SCD Type 1) merge planning and execution.
//!
//! A Databricks `AUTO CDC` flow is a *streaming merge*: every micro-batch of the source is
//! merged into the target by key, with the row carrying the largest `SEQUENCE BY` value winning.
//! SCD Type 1 keeps no history — the target holds exactly the current state of each key.
//!
//! There is no Delta `MERGE` in this engine, so the merge is expressed as a read-modify-write:
//! the whole target is combined with the micro-batch in one SQL statement, and the result
//! replaces the table through the Delta sink's atomic `replace` commit. That is **O(target rows)
//! per micro-batch**; see `docs/pipelines.md` for the honest limits.
//!
//! Three semantics are worth stating outright, because they are the ones a reader is likely to
//! assume differently:
//!
//! * **Deletes are physical and leave no tombstone.** Once `APPLY AS DELETE WHEN` removes a key,
//!   nothing records *when* it was removed, so a record for that key that arrives in a *later*
//!   micro-batch is treated as new — even if its `SEQUENCE BY` value is older than the delete's.
//!   Within one batch the delete still wins. This matches Databricks' SCD Type 1, which also
//!   deletes the row outright; keeping a tombstone would mean carrying a column the target does
//!   not declare and never being able to drop it. "Out of order safe" therefore means *within a
//!   batch, and against rows still present in the target* — not across a delete.
//! * **A NULL `SEQUENCE BY` value is an error**, not a silently dropped row: a row with no
//!   ordering value cannot be placed against the target, and dropping it would lose a change
//!   event without a word. Databricks fails the same way.
//! * **NULL key values compare equal**, via `IS NOT DISTINCT FROM`. A NULL-keyed row is one key
//!   like any other; with plain `=` it would never match the target and the target would grow a
//!   fresh duplicate row per batch, forever.
//!
//! The generated statement is:
//!
//! ```sql
//! WITH __cdc_scored   AS (batch rows, with delete/truncate flags)
//!    , __cdc_trunc_at AS (the largest sequence value that truncated the target, if any)
//!    , __cdc_latest   AS (one row per key: the largest sequence value in this batch)
//!    , __cdc_live     AS (target rows that survive a truncate)
//!    , __cdc_kept     AS (live target rows this batch does not supersede)
//!    , __cdc_applied  AS (batch rows that do supersede their key, minus deletes)
//! SELECT … FROM __cdc_kept UNION ALL SELECT … FROM __cdc_applied
//! ```

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use oxidant_common::{Error, Result};
// `simple_column` is shared with config load so `oxidant.yaml` and `CREATE FLOW ... AS AUTO CDC`
// agree on what counts as a plain column name, and so an expression key is rejected at define
// time rather than on the first micro-batch.
use oxidant_config::{auto_cdc_simple_column as simple_column, AutoCdcConfig};
use oxidant_loom::Engine;

/// Validate AUTO CDC options (re-exported so the SDP wiring has one place to call).
pub fn validate_auto_cdc(config: &AutoCdcConfig, table_name: &str) -> Result<()> {
    oxidant_config::validate_auto_cdc(config, table_name)
}

fn require_column(expr: &str, what: &str) -> Result<String> {
    simple_column(expr).ok_or_else(|| {
        Error::Unsupported(format!(
            "AUTO CDC {what} must be a plain column name, got `{expr}`"
        ))
    })
}

/// Resolve `name` against the batch's columns, case-insensitively, returning the schema spelling.
fn resolve_in(columns: &[String], name: &str) -> Option<String> {
    let wanted = simple_column(name).unwrap_or_else(|| name.trim().to_string());
    columns
        .iter()
        .find(|c| c.eq_ignore_ascii_case(&wanted))
        .cloned()
}

fn quote_ident(name: &str) -> String {
    if !name.is_empty()
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_alphanumeric() || c == '_')
    {
        name.to_string()
    } else {
        format!("`{}`", name.replace('`', "``"))
    }
}

/// Resolve the output column names of a merged CDC table, in target order.
pub fn output_columns(config: &AutoCdcConfig, batch_schema: &SchemaRef) -> Result<Vec<String>> {
    let batch_cols: Vec<String> = batch_schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    let selected = if let Some(list) = &config.column_list {
        if list.is_empty() {
            return Err(Error::Plan(
                "AUTO CDC COLUMNS list must not be empty".into(),
            ));
        }
        list.iter()
            .map(|c| {
                resolve_in(&batch_cols, c).ok_or_else(|| {
                    Error::Plan(format!("AUTO CDC COLUMNS references unknown column `{c}`"))
                })
            })
            .collect::<Result<Vec<_>>>()?
    } else if let Some(except) = &config.except_column_list {
        let skip: BTreeSet<String> = except
            .iter()
            .map(|c| {
                resolve_in(&batch_cols, c)
                    .unwrap_or_else(|| c.trim().to_string())
                    .to_ascii_lowercase()
            })
            .collect();
        let out: Vec<String> = batch_cols
            .iter()
            .filter(|c| !skip.contains(&c.to_ascii_lowercase()))
            .cloned()
            .collect();
        if out.is_empty() {
            return Err(Error::Plan(
                "AUTO CDC COLUMNS * EXCEPT would remove every column".into(),
            ));
        }
        out
    } else {
        batch_cols.clone()
    };

    // The keys and the sequence column have to be persisted: the next micro-batch compares its
    // rows against the ones already in the target, and it can only do that through stored
    // columns. Dropping them would silently let an older event overwrite a newer one.
    for key in &config.keys {
        let key = require_column(key, "KEYS")?;
        if resolve_in(&selected, &key).is_none() {
            return Err(Error::Plan(format!(
                "AUTO CDC key column `{key}` must be part of the target columns"
            )));
        }
    }
    let seq = require_column(&config.sequence_by, "SEQUENCE BY")?;
    if resolve_in(&selected, &seq).is_none() {
        return Err(Error::Plan(format!(
            "AUTO CDC SEQUENCE BY column `{seq}` must be part of the target columns — the merge \
             compares it against the sequence already stored in the target"
        )));
    }
    Ok(selected)
}

/// Which output columns keep the target's value when the incoming value is NULL.
fn ignore_null_update_columns(
    config: &AutoCdcConfig,
    columns: &[String],
) -> Result<BTreeSet<String>> {
    let all = || -> BTreeSet<String> { columns.iter().map(|c| c.to_ascii_lowercase()).collect() };
    if let Some(list) = &config.ignore_null_updates_columns {
        if list.iter().any(|c| c.trim() == "*") {
            return Ok(all());
        }
        return list
            .iter()
            .map(|c| {
                resolve_in(columns, c)
                    .map(|c| c.to_ascii_lowercase())
                    .ok_or_else(|| {
                        Error::Plan(format!(
                            "AUTO CDC IGNORE NULL UPDATES references unknown column `{c}`"
                        ))
                    })
            })
            .collect();
    }
    if let Some(except) = &config.ignore_null_updates_except {
        let skip: BTreeSet<String> = except
            .iter()
            .map(|c| {
                resolve_in(columns, c)
                    .unwrap_or_else(|| c.trim().to_string())
                    .to_ascii_lowercase()
            })
            .collect();
        return Ok(all().difference(&skip).cloned().collect());
    }
    Ok(BTreeSet::new())
}

fn bool_expr(expr: Option<&String>) -> String {
    expr.map(|e| e.trim())
        .filter(|e| !e.is_empty())
        .map(|e| format!("COALESCE(({e}), FALSE)"))
        .unwrap_or_else(|| "FALSE".to_string())
}

/// Build the statement that merges `batch_view` into `target_view` (SCD Type 1).
pub fn build_merge_sql(
    config: &AutoCdcConfig,
    columns: &[String],
    batch_view: &str,
    target_view: &str,
) -> Result<String> {
    validate_auto_cdc(config, "auto cdc")?;
    if columns.is_empty() {
        return Err(Error::Plan(
            "AUTO CDC merge requires at least one output column".into(),
        ));
    }
    let keys: Vec<String> = config
        .keys
        .iter()
        .map(|k| require_column(k, "KEYS").map(|k| quote_ident(&k)))
        .collect::<Result<Vec<_>>>()?;
    let seq = quote_ident(&require_column(&config.sequence_by, "SEQUENCE BY")?);
    let ignore = ignore_null_update_columns(config, columns)?;

    // NULL-safe on purpose: `PARTITION BY` already treats two NULL keys as one key, so a plain
    // `=` here would dedup a NULL-keyed row *within* a batch and then fail to match it against
    // the target, inserting a fresh copy every batch and fanning out the LEFT JOIN below.
    let key_eq = keys
        .iter()
        // Parenthesized: `IS NOT DISTINCT FROM` binds looser than `AND` in the parser, so an
        // unbracketed conjunction reads as `b.k IS NOT DISTINCT FROM (t.k AND ...)`.
        .map(|k| format!("(b.{k} IS NOT DISTINCT FROM t.{k})"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let partition_by = keys
        .iter()
        .map(|k| format!("s.{k}"))
        .collect::<Vec<_>>()
        .join(", ");
    // Two rows for one key with the *same* sequence value have no defined winner, and an
    // arbitrary one means the table's contents can differ between two runs over the same input
    // — including between the run that committed and the replay that recomputes it. So ties
    // break deterministically: a delete first (it is the more conservative answer, and it is
    // what the within-batch ordering already implied), then the remaining output columns.
    let key_names: BTreeSet<String> = keys.iter().map(|k| k.to_ascii_lowercase()).collect();
    let seq_lower = seq.to_ascii_lowercase();
    let tiebreak: String = columns
        .iter()
        .map(|c| quote_ident(c))
        .filter(|q| {
            let lower = q.to_ascii_lowercase();
            lower != seq_lower && !key_names.contains(&lower)
        })
        .map(|q| format!(", s.{q} DESC"))
        .collect();
    let delete_expr = bool_expr(config.apply_as_deletes.as_ref());
    let truncate_expr = bool_expr(config.apply_as_truncates.as_ref());

    let kept_projection = columns
        .iter()
        .map(|c| {
            let q = quote_ident(c);
            format!("t.{q} AS {q}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let applied_projection = columns
        .iter()
        .map(|c| {
            let q = quote_ident(c);
            if ignore.contains(&c.to_ascii_lowercase()) {
                // Only fall back to the target when a target row actually matched; for a brand
                // new key a NULL is the value, not a missing update.
                format!("CASE WHEN b.{q} IS NULL AND t.{seq} IS NOT NULL THEN t.{q} ELSE b.{q} END AS {q}")
            } else {
                format!("b.{q} AS {q}")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let col_list = columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    Ok(format!(
        "WITH __cdc_scored AS ( \
             SELECT *, {delete_expr} AS __cdc_del, {truncate_expr} AS __cdc_trunc \
             FROM {batch_view} \
             WHERE {seq} IS NOT NULL \
         ), \
         __cdc_trunc_at AS ( \
             SELECT MAX(CASE WHEN __cdc_trunc THEN {seq} END) AS __cdc_at FROM __cdc_scored \
         ), \
         __cdc_latest AS ( \
             SELECT * FROM ( \
                 SELECT s.*, ROW_NUMBER() OVER ( \
                     PARTITION BY {partition_by} \
                     ORDER BY s.{seq} DESC, s.__cdc_del DESC{tiebreak} \
                 ) AS __cdc_rn \
                 FROM __cdc_scored s CROSS JOIN __cdc_trunc_at tt \
                 WHERE NOT s.__cdc_trunc \
                   AND (tt.__cdc_at IS NULL OR s.{seq} > tt.__cdc_at) \
             ) WHERE __cdc_rn = 1 \
         ), \
         __cdc_live AS ( \
             SELECT t.* FROM {target_view} t CROSS JOIN __cdc_trunc_at tt \
             WHERE tt.__cdc_at IS NULL OR t.{seq} > tt.__cdc_at \
         ), \
         __cdc_kept AS ( \
             SELECT {kept_projection} FROM __cdc_live t \
             WHERE NOT EXISTS ( \
                 SELECT 1 FROM __cdc_latest b WHERE {key_eq} AND b.{seq} > t.{seq} \
             ) \
         ), \
         __cdc_applied AS ( \
             SELECT {applied_projection} \
             FROM __cdc_latest b LEFT JOIN __cdc_live t ON {key_eq} \
             WHERE NOT b.__cdc_del AND (t.{seq} IS NULL OR b.{seq} > t.{seq}) \
         ) \
         SELECT {col_list} FROM __cdc_kept UNION ALL SELECT {col_list} FROM __cdc_applied"
    ))
}

/// A prepared AUTO CDC merge: the resolved output columns plus the statement that produces them.
#[derive(Debug, Clone)]
pub struct CdcMerge {
    columns: Vec<String>,
    /// The `SEQUENCE BY` column, as spelled in the source schema.
    sequence_column: String,
    schema: SchemaRef,
    sql: String,
    batch_view: String,
    target_view: String,
}

impl CdcMerge {
    /// Plan the merge for a source micro-batch schema.
    ///
    /// `id` disambiguates the temporary views this merge registers, so two AUTO CDC flows in one
    /// pipeline never see each other's batch.
    pub fn new(config: &AutoCdcConfig, batch_schema: &SchemaRef, id: &str) -> Result<Self> {
        let columns = output_columns(config, batch_schema)?;
        let sequence_column = resolve_in(&columns, &config.sequence_by).ok_or_else(|| {
            Error::Plan(format!(
                "AUTO CDC SEQUENCE BY column `{}` is not in the target columns",
                config.sequence_by
            ))
        })?;
        let id: String = id
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        let batch_view = format!("__cdc_batch_{id}");
        let target_view = format!("__cdc_target_{id}");
        let sql = build_merge_sql(config, &columns, &batch_view, &target_view)?;
        // Every output column is nullable: a merge target is populated through outer joins and
        // unions, so a column that is non-null in the source is not guaranteed non-null here.
        let fields: Vec<Field> = columns
            .iter()
            .map(|c| {
                let f = batch_schema.field_with_name(c).map_err(|_| {
                    Error::Plan(format!("AUTO CDC column `{c}` is not in the source"))
                })?;
                Ok(Field::new(f.name(), f.data_type().clone(), true))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            columns,
            sequence_column,
            schema: Arc::new(Schema::new(fields)),
            sql,
            batch_view,
            target_view,
        })
    }

    /// The merged table's columns, in order.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// The schema the merged batches — and therefore the target table — have.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// The generated merge statement (exposed for tests and `EXPLAIN`-style debugging).
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Merge one micro-batch into the current target contents, returning the new contents.
    pub async fn apply(
        &self,
        engine: &Engine,
        batch: &[RecordBatch],
        target: &[RecordBatch],
    ) -> Result<Vec<RecordBatch>> {
        self.reject_null_sequence(batch)?;
        let target = if target.is_empty() {
            vec![RecordBatch::new_empty(self.schema.clone())]
        } else {
            target.to_vec()
        };
        engine.register_batches(&self.batch_view, batch.to_vec())?;
        engine.register_batches(&self.target_view, target)?;
        let merged = engine.sql(&self.sql).await;
        engine.deregister_table(&self.batch_view);
        engine.deregister_table(&self.target_view);
        merged?
            .into_iter()
            .map(|b| self.conform(b))
            .collect::<Result<Vec<_>>>()
    }

    /// Fail the batch if any row has no `SEQUENCE BY` value.
    ///
    /// The merge orders every decision by that column, so a row without one cannot be placed
    /// against the target at all. The alternative — the `IS NOT NULL` filter in the statement
    /// quietly eating the row — loses a change event, and a lost delete or truncate is not a
    /// small loss. Databricks fails on a null sequencing value too.
    fn reject_null_sequence(&self, batch: &[RecordBatch]) -> Result<()> {
        let nulls: usize = batch
            .iter()
            .filter_map(|b| b.column_by_name(&self.sequence_column))
            .map(|c| c.null_count())
            .sum();
        if nulls > 0 {
            return Err(Error::Execution(format!(
                "AUTO CDC: {nulls} row(s) in this micro-batch have a NULL `{}` — the merge \
                 orders every row by it, so a row without one cannot be applied",
                self.sequence_column
            )));
        }
        Ok(())
    }

    /// Force the merge output onto the declared target schema.
    ///
    /// The Delta sink rejects a batch whose schema is not *equal* to the table's, and a UNION of
    /// join outputs can legitimately differ in nullability or field metadata.
    fn conform(&self, batch: RecordBatch) -> Result<RecordBatch> {
        if batch.schema() == self.schema {
            return Ok(batch);
        }
        let columns: Vec<ArrayRef> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let col = batch.column(i);
                if col.data_type() == field.data_type() {
                    Ok(col.clone())
                } else {
                    cast(col, field.data_type()).map_err(|e| {
                        Error::Execution(format!("AUTO CDC merge column `{}`: {e}", field.name()))
                    })
                }
            })
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| Error::Execution(format!("AUTO CDC merge output: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::DataType;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("seq", DataType::Int64, false),
            Field::new("op", DataType::Utf8, true),
        ]))
    }

    fn cfg() -> AutoCdcConfig {
        AutoCdcConfig {
            source: "src".into(),
            keys: vec!["id".into()],
            sequence_by: "seq".into(),
            apply_as_deletes: Some("op = 'D'".into()),
            apply_as_truncates: None,
            column_list: None,
            except_column_list: Some(vec!["op".into()]),
            ignore_null_updates_columns: None,
            ignore_null_updates_except: None,
        }
    }

    fn batch(rows: &[(i64, Option<&str>, i64, &str)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| Some(r.3)).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("batch")
    }

    /// The same shape as [`schema`], but with `id` and `seq` nullable so the NULL-key and
    /// NULL-sequence cases can be built at all.
    fn nullable_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("seq", DataType::Int64, true),
            Field::new("op", DataType::Utf8, true),
        ]))
    }

    #[allow(clippy::type_complexity)]
    fn nullable_batch(rows: &[(Option<i64>, Option<&str>, Option<i64>, &str)]) -> RecordBatch {
        RecordBatch::try_new(
            nullable_schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| Some(r.3)).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("batch")
    }

    /// Like [`rows`], but keeps a NULL key visible instead of unwrapping it.
    async fn nullable_rows(
        engine: &Engine,
        batches: &[RecordBatch],
    ) -> Vec<(Option<i64>, Option<String>, i64)> {
        if batches.is_empty() {
            return Vec::new();
        }
        engine
            .register_batches("__cdc_probe_n", batches.to_vec())
            .expect("register");
        let out = engine
            .sql("SELECT id, name, seq FROM __cdc_probe_n ORDER BY id NULLS FIRST, seq")
            .await
            .expect("probe");
        engine.deregister_table("__cdc_probe_n");
        let mut collected = Vec::new();
        for b in out {
            let ids = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id");
            let names = b
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("name");
            let seqs = b
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("seq");
            for i in 0..b.num_rows() {
                collected.push((
                    (!ids.is_null(i)).then(|| ids.value(i)),
                    (!names.is_null(i)).then(|| names.value(i).to_string()),
                    seqs.value(i),
                ));
            }
        }
        collected
    }

    async fn rows(engine: &Engine, batches: &[RecordBatch]) -> Vec<(i64, Option<String>, i64)> {
        // An empty merge result is a legitimate outcome (everything deleted or truncated) and
        // arrives as zero batches, which is not something a view can be registered from.
        if batches.is_empty() {
            return Vec::new();
        }
        engine
            .register_batches("__cdc_probe", batches.to_vec())
            .expect("register");
        let out = engine
            .sql("SELECT id, name, seq FROM __cdc_probe ORDER BY id")
            .await
            .expect("probe");
        engine.deregister_table("__cdc_probe");
        let mut collected = Vec::new();
        for b in out {
            let ids = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id");
            let names = b
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("name");
            let seqs = b
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("seq");
            for i in 0..b.num_rows() {
                collected.push((
                    ids.value(i),
                    (!names.is_null(i)).then(|| names.value(i).to_string()),
                    seqs.value(i),
                ));
            }
        }
        collected
    }

    #[test]
    fn output_columns_honour_except_and_require_key_and_sequence() {
        let cols = output_columns(&cfg(), &schema()).expect("columns");
        assert_eq!(cols, vec!["id", "name", "seq"]);

        let mut dropped_seq = cfg();
        dropped_seq.except_column_list = Some(vec!["op".into(), "seq".into()]);
        let err = output_columns(&dropped_seq, &schema())
            .expect_err("sequence column must survive")
            .to_string();
        assert!(err.contains("SEQUENCE BY"), "{err}");

        let mut listed = cfg();
        listed.except_column_list = None;
        listed.column_list = Some(vec!["ID".into(), "SEQ".into()]);
        assert_eq!(
            output_columns(&listed, &schema()).expect("columns"),
            vec!["id", "seq"]
        );
    }

    #[test]
    fn rejects_expression_keys_and_sequence() {
        let mut expr_key = cfg();
        expr_key.keys = vec!["lower(id)".into()];
        assert!(output_columns(&expr_key, &schema()).is_err());

        let mut expr_seq = cfg();
        expr_seq.sequence_by = "struct(seq, id)".into();
        assert!(output_columns(&expr_seq, &schema()).is_err());
    }

    #[test]
    fn merge_sql_mentions_keys_sequence_and_flags() {
        let cols = output_columns(&cfg(), &schema()).expect("columns");
        let sql = build_merge_sql(&cfg(), &cols, "b_view", "t_view").expect("sql");
        assert!(sql.contains("PARTITION BY s.id"), "{sql}");
        assert!(sql.contains("ORDER BY s.seq DESC"), "{sql}");
        assert!(
            sql.contains("COALESCE((op = 'D'), FALSE) AS __cdc_del"),
            "{sql}"
        );
        assert!(sql.contains("FROM b_view"), "{sql}");
        assert!(sql.contains("FROM t_view t"), "{sql}");
    }

    #[test]
    fn ignore_null_updates_projects_the_target_value() {
        let mut c = cfg();
        c.ignore_null_updates_columns = Some(vec!["name".into()]);
        let cols = output_columns(&c, &schema()).expect("columns");
        let sql = build_merge_sql(&c, &cols, "b", "t").expect("sql");
        assert!(
            sql.contains(
                "CASE WHEN b.name IS NULL AND t.seq IS NOT NULL THEN t.name ELSE b.name END"
            ),
            "{sql}"
        );

        let mut except = cfg();
        except.ignore_null_updates_except = Some(vec!["name".into()]);
        let cols = output_columns(&except, &schema()).expect("columns");
        let sql = build_merge_sql(&except, &cols, "b", "t").expect("sql");
        assert!(sql.contains("CASE WHEN b.id IS NULL"), "{sql}");
        assert!(!sql.contains("CASE WHEN b.name IS NULL"), "{sql}");
    }

    #[tokio::test]
    async fn merge_keeps_latest_by_sequence_and_applies_deletes() {
        let engine = Engine::new();
        let merge = CdcMerge::new(&cfg(), &schema(), "t1").expect("plan");

        let state = merge
            .apply(
                &engine,
                &[batch(&[
                    (1, Some("alice"), 1, "I"),
                    (2, Some("bob"), 1, "I"),
                ])],
                &[],
            )
            .await
            .expect("batch 1");
        assert_eq!(
            rows(&engine, &state).await,
            vec![(1, Some("alice".into()), 1), (2, Some("bob".into()), 1)]
        );

        // Within one batch the largest sequence wins; across batches an older event must not
        // clobber the newer state already in the target.
        let state = merge
            .apply(
                &engine,
                &[batch(&[
                    (1, Some("stale"), 0, "U"),
                    (1, Some("alice2"), 2, "U"),
                    (3, Some("cy"), 1, "I"),
                ])],
                &state,
            )
            .await
            .expect("batch 2");
        assert_eq!(
            rows(&engine, &state).await,
            vec![
                (1, Some("alice2".into()), 2),
                (2, Some("bob".into()), 1),
                (3, Some("cy".into()), 1),
            ]
        );

        // An out-of-order delete for a key the target already advanced past is ignored.
        let state = merge
            .apply(&engine, &[batch(&[(1, None, 1, "D")])], &state)
            .await
            .expect("stale delete");
        assert_eq!(rows(&engine, &state).await.len(), 3);

        let state = merge
            .apply(&engine, &[batch(&[(2, None, 9, "D")])], &state)
            .await
            .expect("delete");
        assert_eq!(
            rows(&engine, &state).await,
            vec![(1, Some("alice2".into()), 2), (3, Some("cy".into()), 1)]
        );
    }

    #[tokio::test]
    async fn ignore_null_updates_keeps_the_previous_value() {
        let mut c = cfg();
        c.ignore_null_updates_columns = Some(vec!["name".into()]);
        let engine = Engine::new();
        let merge = CdcMerge::new(&c, &schema(), "t2").expect("plan");

        let state = merge
            .apply(&engine, &[batch(&[(1, Some("alice"), 1, "I")])], &[])
            .await
            .expect("batch 1");
        let state = merge
            .apply(&engine, &[batch(&[(1, None, 2, "U")])], &state)
            .await
            .expect("batch 2");
        assert_eq!(
            rows(&engine, &state).await,
            vec![(1, Some("alice".into()), 2)]
        );
    }

    #[tokio::test]
    async fn null_update_overwrites_without_ignore_null_updates() {
        let engine = Engine::new();
        let merge = CdcMerge::new(&cfg(), &schema(), "t3").expect("plan");
        let state = merge
            .apply(&engine, &[batch(&[(1, Some("alice"), 1, "I")])], &[])
            .await
            .expect("batch 1");
        let state = merge
            .apply(&engine, &[batch(&[(1, None, 2, "U")])], &state)
            .await
            .expect("batch 2");
        assert_eq!(rows(&engine, &state).await, vec![(1, None, 2)]);
    }

    #[tokio::test]
    async fn truncate_clears_the_target_and_keeps_later_rows() {
        let mut c = cfg();
        c.apply_as_truncates = Some("op = 'T'".into());
        let engine = Engine::new();
        let merge = CdcMerge::new(&c, &schema(), "t4").expect("plan");

        let state = merge
            .apply(
                &engine,
                &[batch(&[
                    (1, Some("alice"), 1, "I"),
                    (2, Some("bob"), 1, "I"),
                ])],
                &[],
            )
            .await
            .expect("batch 1");
        // A row the target already advanced *past* the truncate: the truncate is at seq 5, so
        // only rows at or below 5 are wiped. Without this row the test passes even when the
        // target side of the merge drops everything unconditionally.
        let state = merge
            .apply(&engine, &[batch(&[(4, Some("newer"), 9, "I")])], &state)
            .await
            .expect("batch 2");
        let state = merge
            .apply(
                &engine,
                &[batch(&[(9, None, 5, "T"), (7, Some("post"), 6, "I")])],
                &state,
            )
            .await
            .expect("truncate batch");
        assert_eq!(
            rows(&engine, &state).await,
            vec![(4, Some("newer".into()), 9), (7, Some("post".into()), 6)]
        );
    }

    #[tokio::test]
    async fn a_truncate_older_than_the_target_leaves_it_alone() {
        // A truncate that arrives late must not wipe state that is strictly newer than it. The
        // batch side of the merge always compared against the truncate's sequence; the target
        // side dropped every row whenever *any* truncate was present.
        let mut c = cfg();
        c.apply_as_truncates = Some("op = 'T'".into());
        let engine = Engine::new();
        let merge = CdcMerge::new(&c, &schema(), "t6").expect("plan");

        let state = merge
            .apply(&engine, &[batch(&[(1, Some("a"), 10, "I")])], &[])
            .await
            .expect("batch 1");
        assert_eq!(rows(&engine, &state).await, vec![(1, Some("a".into()), 10)]);

        let state = merge
            .apply(&engine, &[batch(&[(9, None, 5, "T")])], &state)
            .await
            .expect("late truncate");
        assert_eq!(
            rows(&engine, &state).await,
            vec![(1, Some("a".into()), 10)],
            "a truncate at seq 5 must not remove a row committed at seq 10"
        );

        // And a truncate that *is* newer still empties the table.
        let state = merge
            .apply(&engine, &[batch(&[(9, None, 11, "T")])], &state)
            .await
            .expect("current truncate");
        assert!(rows(&engine, &state).await.is_empty());
    }

    #[tokio::test]
    async fn a_null_key_is_one_key_across_batches() {
        // `PARTITION BY` already treats two NULLs as one key, so a NULL-unsafe `=` in the merge
        // dedups within a batch and then never matches the target — one row per batch, forever.
        let engine = Engine::new();
        let merge = CdcMerge::new(&cfg(), &nullable_schema(), "t7").expect("plan");

        let state = merge
            .apply(
                &engine,
                &[nullable_batch(&[(None, Some("a"), Some(1), "I")])],
                &[],
            )
            .await
            .expect("batch 1");
        let state = merge
            .apply(
                &engine,
                &[nullable_batch(&[(None, Some("b"), Some(2), "U")])],
                &state,
            )
            .await
            .expect("batch 2");
        assert_eq!(
            nullable_rows(&engine, &state).await,
            vec![(None, Some("b".into()), 2)],
            "a NULL key must be superseded like any other key"
        );

        // A stale NULL-keyed row loses, and a delete for it still lands.
        let state = merge
            .apply(
                &engine,
                &[nullable_batch(&[(None, Some("stale"), Some(1), "U")])],
                &state,
            )
            .await
            .expect("stale");
        assert_eq!(
            nullable_rows(&engine, &state).await,
            vec![(None, Some("b".into()), 2)]
        );
        let state = merge
            .apply(
                &engine,
                &[nullable_batch(&[(None, None, Some(3), "D")])],
                &state,
            )
            .await
            .expect("delete");
        assert!(nullable_rows(&engine, &state).await.is_empty());
    }

    #[tokio::test]
    async fn a_null_sequence_value_fails_the_batch() {
        // Dropping the row silently would lose a change event — and a lost delete or truncate
        // is not a small loss. Databricks fails on a null sequencing value too.
        let engine = Engine::new();
        let merge = CdcMerge::new(&cfg(), &nullable_schema(), "t8").expect("plan");
        let err = merge
            .apply(
                &engine,
                &[nullable_batch(&[
                    (Some(1), Some("a"), Some(1), "I"),
                    (Some(2), Some("b"), None, "I"),
                ])],
                &[],
            )
            .await
            .expect_err("a NULL sequence value is an error")
            .to_string();
        assert!(err.contains("NULL `seq`"), "{err}");
        assert!(err.contains("1 row(s)"), "{err}");
    }

    #[tokio::test]
    async fn tied_sequences_resolve_deterministically() {
        // Two rows for one key with the same sequence had no defined winner, so the committed
        // table could differ from the one a replay recomputes.
        let engine = Engine::new();
        let merge = CdcMerge::new(&cfg(), &schema(), "t9").expect("plan");

        // Ties break on the remaining output columns, descending: `zzz` beats `aaa`.
        let tied = batch(&[(1, Some("aaa"), 5, "U"), (1, Some("zzz"), 5, "U")]);
        let reversed = batch(&[(1, Some("zzz"), 5, "U"), (1, Some("aaa"), 5, "U")]);
        let a = merge.apply(&engine, &[tied], &[]).await.expect("tied");
        let b = merge
            .apply(&engine, &[reversed], &[])
            .await
            .expect("tied, other input order");
        assert_eq!(rows(&engine, &a).await, vec![(1, Some("zzz".into()), 5)]);
        assert_eq!(rows(&engine, &a).await, rows(&engine, &b).await);

        // A delete outranks a non-delete at the same sequence: the conservative answer, and the
        // one the within-batch ordering already implied.
        let state = merge
            .apply(&engine, &[batch(&[(1, Some("live"), 1, "I")])], &[])
            .await
            .expect("insert");
        let state = merge
            .apply(
                &engine,
                &[batch(&[(1, Some("keep"), 5, "U"), (1, None, 5, "D")])],
                &state,
            )
            .await
            .expect("tied delete");
        assert!(rows(&engine, &state).await.is_empty());
    }

    #[tokio::test]
    async fn a_record_arriving_after_a_delete_recreates_the_key() {
        // Documented, not accidental: an SCD1 delete is physical and leaves no tombstone, so
        // nothing remains to compare a later record's sequence against. Databricks behaves the
        // same way. Locked in here so a change to it is a deliberate one.
        let engine = Engine::new();
        let merge = CdcMerge::new(&cfg(), &schema(), "t10").expect("plan");

        let state = merge
            .apply(&engine, &[batch(&[(1, Some("a"), 1, "I")])], &[])
            .await
            .expect("insert");
        let state = merge
            .apply(&engine, &[batch(&[(1, None, 5, "D")])], &state)
            .await
            .expect("delete");
        assert!(rows(&engine, &state).await.is_empty());

        let state = merge
            .apply(&engine, &[batch(&[(1, Some("late"), 3, "U")])], &state)
            .await
            .expect("late record");
        assert_eq!(
            rows(&engine, &state).await,
            vec![(1, Some("late".into()), 3)],
            "no tombstone survives the delete, so the key comes back"
        );

        // Within one batch the delete still wins over an older record.
        let state = merge
            .apply(
                &engine,
                &[batch(&[(1, None, 9, "D"), (1, Some("older"), 8, "U")])],
                &state,
            )
            .await
            .expect("delete and older record together");
        assert!(rows(&engine, &state).await.is_empty());
    }

    #[tokio::test]
    async fn merged_batches_match_the_declared_schema() {
        let engine = Engine::new();
        let merge = CdcMerge::new(&cfg(), &schema(), "t5").expect("plan");
        let state = merge
            .apply(&engine, &[batch(&[(1, Some("alice"), 1, "I")])], &[])
            .await
            .expect("batch 1");
        for b in &state {
            assert_eq!(b.schema(), merge.schema());
        }
    }
}
