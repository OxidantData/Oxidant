//! Cast query output batches to a declared table schema with column-named errors.

use datafusion::arrow::compute::kernels::cast::cast_with_options;
use datafusion::arrow::compute::CastOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::FormatOptions;
use oxidant_common::{Error, Result};

/// Cast `batch` to `schema`, naming the column on incompatible casts (Spark INSERT parity).
pub fn conform_batch_to_schema(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    if batch.schema() == *schema {
        return Ok(batch);
    }
    if batch.num_columns() != schema.fields().len() {
        return Err(Error::Plan(format!(
            "flow produces {} column(s) but the declared schema has {}",
            batch.num_columns(),
            schema.fields().len()
        )));
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
                    "flow column `{}` is {:?}, which cannot be written to a {:?} column: {e}",
                    field.name(),
                    column.data_type(),
                    field.data_type()
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| Error::Execution(format!("conform flow batch to declared schema: {e}")))
}
