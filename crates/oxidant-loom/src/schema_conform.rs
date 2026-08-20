//! Cast query output batches to a declared table schema with column-named errors.
//!
//! Every catalog-table write path funnels through here so they all enforce a schema the same
//! way: an SDP flow writing to its declared output schema, a streaming query's sink, and a SQL
//! `INSERT INTO` / `INSERT OVERWRITE` writing to a catalog table. (The local-warehouse
//! `ListingTable` insert path does not — that is DataFusion's own `ListingTable::insert_into` —
//! and a CTAS writes at the `SELECT`'s schema, having no declared table schema yet to conform
//! to.) The cast is **not** safe — a value that does not fit the target type is an
//! error naming the column, never a silent `NULL` in the committed file. Its overflow
//! behaviour matches Spark's ANSI store assignment and what `docs/sql-writes.md` promises;
//! Spark's ANSI policy additionally rejects a `STRING` → numeric assignment during analysis,
//! which this engine plans and then checks for value survival instead.
//!
//! Cast-or-fail here is consistent with the rest of the engine rather than an exception to it:
//! expression `CAST` is ANSI-erroring too (`SELECT CAST('x' AS INT)` is an error, not `NULL`,
//! which is what the parity baseline's `nonansi/cast.sql` divergences record), and the
//! `spark_functions` cast-alias constructors lower to a `safe = false` `Expr::Cast` for the
//! same reason. `TRY_CAST` is the lenient form. What remains distinct about a store assignment
//! is only that it is implicit — the user did not write the cast — so its failure names the
//! column being written.

use datafusion::arrow::compute::kernels::cast::cast_with_options;
use datafusion::arrow::compute::CastOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::FormatOptions;
use oxidant_common::{Error, Result};

/// Which write is being conformed, so an error names the statement the user actually wrote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConformSubject {
    /// An SDP flow or streaming query writing to its declared output schema.
    Flow,
    /// A SQL `INSERT INTO` / `INSERT OVERWRITE` writing to a catalog table's schema.
    Insert,
}

impl ConformSubject {
    fn arity_mismatch(self, produced: usize, expected: usize) -> String {
        match self {
            Self::Flow => {
                format!("flow produces {produced} column(s) but the declared schema has {expected}")
            }
            Self::Insert => {
                format!("INSERT provides {produced} column(s) but the table has {expected}")
            }
        }
    }

    /// How a column is named in a cast failure — `flow column \`x\`` / `INSERT column \`x\``.
    fn column_noun(self) -> &'static str {
        match self {
            Self::Flow => "flow column",
            Self::Insert => "INSERT column",
        }
    }

    /// What the batch is being conformed *to*, for the assemble-the-batch failure.
    fn target_noun(self) -> &'static str {
        match self {
            Self::Flow => "conform flow batch to declared schema",
            Self::Insert => "conform INSERT batch to table",
        }
    }
}

/// Cast `batch` to a flow's declared output schema. See [`conform_batch`].
pub fn conform_batch_to_schema(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    conform_batch(batch, schema, ConformSubject::Flow)
}

/// Cast a SQL `INSERT`'s batch to the target table's schema. See [`conform_batch`].
pub fn conform_insert_batch_to_schema(
    batch: RecordBatch,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    conform_batch(batch, schema, ConformSubject::Insert)
}

/// Cast `batch` to `schema`, naming the column on any cast that loses the value.
///
/// The cast runs with `safe: false`, so an overflowing string→`INT`, an unparseable timestamp,
/// or a decimal that does not fit the declared precision fails the write instead of landing as
/// `NULL`. A `NULL` that was already `NULL` casts through untouched — only a *lost* value is an
/// error. A column the target declares `NOT NULL` that carries nulls fails when the batch is
/// assembled, for the same reason.
pub fn conform_batch(
    batch: RecordBatch,
    schema: &SchemaRef,
    subject: ConformSubject,
) -> Result<RecordBatch> {
    if batch.schema() == *schema {
        return Ok(batch);
    }
    if batch.num_columns() != schema.fields().len() {
        return Err(Error::Plan(
            subject.arity_mismatch(batch.num_columns(), schema.fields().len()),
        ));
    }
    let cast_opts = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                return Ok(column.clone());
            }
            cast_with_options(column, field.data_type(), &cast_opts).map_err(|e| {
                Error::Execution(format!(
                    "{} `{}` is {:?}, which cannot be written to a {:?} column: {e}",
                    subject.column_noun(),
                    field.name(),
                    column.data_type(),
                    field.data_type()
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| Error::Execution(format!("{}: {e}", subject.target_noun())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Array as _, Decimal128Array, Int32Array, Int64Array, StringArray,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    type ArrayRef = Arc<dyn datafusion::arrow::array::Array>;

    fn conform_as(
        subject: ConformSubject,
        source_fields: Vec<Field>,
        target_fields: Vec<Field>,
        columns: Vec<ArrayRef>,
    ) -> Result<RecordBatch> {
        let source = Arc::new(Schema::new(source_fields));
        let target = Arc::new(Schema::new(target_fields));
        let batch = RecordBatch::try_new(source, columns).expect("batch");
        conform_batch(batch, &target, subject)
    }

    /// Run a case through both subjects: the enforcement is shared, only the wording differs.
    fn conform_both(
        source_fields: Vec<Field>,
        target_fields: Vec<Field>,
        columns: Vec<ArrayRef>,
    ) -> [Result<RecordBatch>; 2] {
        [ConformSubject::Insert, ConformSubject::Flow].map(|subject| {
            conform_as(
                subject,
                source_fields.clone(),
                target_fields.clone(),
                columns.clone(),
            )
        })
    }

    fn utf8(name: &str) -> Field {
        Field::new(name, DataType::Utf8, true)
    }

    /// A string that is not a number must fail the write. Arrow's *default* cast is safe and
    /// would put a `NULL` in the committed file instead — the bug this module exists to prevent.
    #[test]
    fn rejects_invalid_string_to_bigint() {
        for result in conform_both(
            vec![utf8("id")],
            vec![Field::new("id", DataType::Int64, true)],
            vec![Arc::new(StringArray::from(vec!["1", "not-a-number"]))],
        ) {
            let err = result.expect_err("invalid cast").to_string();
            assert!(err.contains("`id`"), "{err}");
        }
    }

    #[test]
    fn rejects_string_int_overflow() {
        for result in conform_both(
            vec![utf8("n")],
            vec![Field::new("n", DataType::Int32, true)],
            vec![Arc::new(StringArray::from(vec!["2147483648"]))],
        ) {
            let err = result.expect_err("int32 overflow").to_string();
            assert!(err.contains("`n`"), "{err}");
        }
    }

    #[test]
    fn rejects_int_narrowing_overflow() {
        for result in conform_both(
            vec![Field::new("n", DataType::Int64, true)],
            vec![Field::new("n", DataType::Int32, true)],
            vec![Arc::new(Int64Array::from(vec![2_147_483_648_i64]))],
        ) {
            let err = result.expect_err("int64 does not fit int32").to_string();
            assert!(err.contains("`n`"), "{err}");
        }
    }

    #[test]
    fn rejects_invalid_timestamp_string() {
        for result in conform_both(
            vec![utf8("ts")],
            vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            )],
            vec![Arc::new(StringArray::from(vec!["not-a-timestamp"]))],
        ) {
            let err = result.expect_err("invalid timestamp").to_string();
            assert!(err.contains("`ts`"), "{err}");
        }
    }

    #[test]
    fn rejects_out_of_range_decimal() {
        for result in conform_both(
            vec![utf8("amount")],
            vec![Field::new("amount", DataType::Decimal128(5, 2), true)],
            vec![Arc::new(StringArray::from(vec!["1000.00"]))],
        ) {
            let err = result.expect_err("decimal overflow").to_string();
            assert!(err.contains("`amount`"), "{err}");
        }
    }

    /// A value that was already absent stays absent. Only a *lost* value is an error — a
    /// cast-or-fail that also rejected nulls would make every nullable column unwritable.
    #[test]
    fn null_values_stay_null_on_compatible_cast() {
        for result in conform_both(
            vec![utf8("id")],
            vec![Field::new("id", DataType::Int64, true)],
            vec![Arc::new(StringArray::from(vec![None::<&str>, Some("42")]))],
        ) {
            let batch = result.expect("null should cast through");
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 column");
            assert!(ids.is_null(0), "a NULL must stay NULL, not become a value");
            assert_eq!(ids.value(1), 42);
        }
    }

    /// The case the cast exists for: a Spark integer literal plans as `Int32` against a `BIGINT`
    /// column. Erroring on this instead would break `INSERT INTO t SELECT 9`.
    #[test]
    fn accepts_compatible_widening_cast() {
        for result in conform_both(
            vec![Field::new("id", DataType::Int32, false)],
            vec![Field::new("id", DataType::Int64, false)],
            vec![Arc::new(Int32Array::from(vec![9]))],
        ) {
            let batch = result.expect("int32 to int64");
            assert_eq!(
                batch.column(0).data_type(),
                &DataType::Int64,
                "column should be widened to the table type"
            );
        }
    }

    #[test]
    fn decimal_within_range_casts() {
        for result in conform_both(
            vec![utf8("amount")],
            vec![Field::new("amount", DataType::Decimal128(5, 2), true)],
            vec![Arc::new(StringArray::from(vec!["123.45"]))],
        ) {
            let batch = result.expect("in-range decimal");
            let amounts = batch
                .column(0)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("Decimal128 column");
            assert_eq!(amounts.value(0), 12_345);
        }
    }

    /// A null in a column the target declares `NOT NULL` fails the write rather than being
    /// committed into a file whose schema says it cannot be there.
    #[test]
    fn rejects_null_into_a_non_nullable_column() {
        let err = conform_as(
            ConformSubject::Insert,
            vec![utf8("id")],
            vec![Field::new("id", DataType::Int64, false)],
            vec![Arc::new(StringArray::from(vec![None::<&str>]))],
        )
        .expect_err("null into NOT NULL")
        .to_string();
        assert!(err.contains("conform INSERT batch to table"), "{err}");
    }

    /// The two subjects name the statement the user wrote. An `INSERT` that reports a "flow"
    /// error points at a concept that is not in their SQL.
    #[test]
    fn each_subject_names_its_own_statement() {
        let case = || {
            (
                vec![utf8("id")],
                vec![Field::new("id", DataType::Int64, true)],
                vec![Arc::new(StringArray::from(vec!["nope"])) as ArrayRef],
            )
        };
        let (src, dst, cols) = case();
        let insert = conform_as(ConformSubject::Insert, src, dst, cols)
            .expect_err("invalid cast")
            .to_string();
        assert!(insert.contains("INSERT column `id`"), "{insert}");

        let (src, dst, cols) = case();
        let flow = conform_as(ConformSubject::Flow, src, dst, cols)
            .expect_err("invalid cast")
            .to_string();
        assert!(flow.contains("flow column `id`"), "{flow}");
    }

    #[test]
    fn column_count_mismatch_names_the_target() {
        let insert = conform_as(
            ConformSubject::Insert,
            vec![utf8("a"), utf8("b")],
            vec![utf8("a")],
            vec![
                Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["y"])) as ArrayRef,
            ],
        )
        .expect_err("arity mismatch")
        .to_string();
        assert!(
            insert.contains("INSERT provides 2 column(s) but the table has 1"),
            "{insert}"
        );
    }
}
