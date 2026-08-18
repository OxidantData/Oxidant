//! Running SQL in-process, with no server.
//!
//! `oxidant sql` was originally a thin HTTP client: it POSTed a statement to a *running*
//! server's REST API and polled for the result. That made the CLI unusable on its own — asking
//! "how many rows are in this Parquet directory?" meant starting a Spark Connect server first.
//!
//! This module is the other half: build an engine in-process from the config file, run the
//! statement, and print it. The REST path is still there and still the default whenever a
//! server URL is given, because pointing the CLI at a remote driver is a genuinely different
//! thing from running the query here.
//!
//! Results are rendered by converting Arrow batches into the *same* JSON document shape the
//! REST API returns, so [`crate::render_table`] and the `--format json` printer are shared
//! verbatim between the two paths rather than drifting apart.

use oxidant_common::{Error, Result};
use oxidant_config::OxidantConfig;
use oxidant_connect::OxidantService;
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;

/// Build an engine with the config's catalogs bridged in, ready to run statements.
///
/// Goes through [`OxidantService::with_catalogs`] rather than reimplementing catalog
/// construction: that is the same call the Spark Connect server makes, so a catalog resolves
/// identically whether a query arrives over gRPC or from this CLI. The service is dropped
/// immediately — only its engine is kept — and nothing binds a port.
pub async fn build_engine(
    config: Option<&OxidantConfig>,
    sample_data: Option<std::path::PathBuf>,
) -> Result<std::sync::Arc<Engine>> {
    // `engine:` is NOT applied here. It lowers to `OXIDANT_*` via `set_var`, which is only sound
    // while the process is single-threaded, so `main` does it before the Tokio runtime starts —
    // which is also before any engine is constructed, and the engine reads those once at
    // construction.
    let catalogs = config
        .map(|c| c.catalog_conf().into_iter().collect())
        .unwrap_or_default();
    let service = OxidantService::with_catalogs(catalogs).await;
    let engine = service.engine();

    if let Some(dir) = sample_data {
        // Best-effort, exactly as the server treats it: a missing or unreadable sample tree is
        // a missing convenience, never a reason to fail the statement the user asked for.
        engine.register_sample_tables(&dir).await;
    }
    Ok(engine)
}

/// Run one statement and return its result batches.
pub async fn run_sql(engine: &Engine, sql: &str) -> Result<Vec<RecordBatch>> {
    engine.sql(sql).await
}

/// Render batches as the REST API's result document, so the shared renderers apply.
///
/// `limit` caps the rows materialized into JSON. `truncated` reports whether rows were cut,
/// which is what makes the `(N rows) [truncated]` footer honest instead of implying the result
/// was complete.
pub fn result_doc(batches: &[RecordBatch], limit: usize) -> Result<serde_json::Value> {
    use oxidant_loom::arrow::json::writer::{ArrayWriter, WriterBuilder};

    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    let truncated = total > limit;
    let fields: Vec<serde_json::Value> = batches
        .first()
        .map(|batch| {
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| {
                    serde_json::json!({
                        "name": f.name(),
                        "type": format!("{:?}", f.data_type()),
                        "nullable": f.is_nullable(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let mut buf: Vec<u8> = Vec::new();
    {
        // Explicit nulls so a null column renders as `null` rather than vanishing from the
        // row object — a missing key reads as "no such column", which is a different thing.
        let mut writer: ArrayWriter<&mut Vec<u8>> = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build(&mut buf);
        let mut remaining = limit;
        for batch in batches {
            if remaining == 0 {
                break;
            }
            let n = batch.num_rows().min(remaining);
            writer
                .write(&batch.slice(0, n))
                .map_err(|e| Error::Execution(format!("json encode: {e}")))?;
            remaining -= n;
        }
        writer
            .finish()
            .map_err(|e| Error::Execution(format!("json encode: {e}")))?;
    }
    let rows: serde_json::Value =
        serde_json::from_slice(&buf).map_err(|e| Error::Execution(format!("json encode: {e}")))?;
    let row_count = rows.as_array().map(Vec::len).unwrap_or(0);
    Ok(serde_json::json!({
        "schema": { "fields": fields },
        "rows": rows,
        "rowCount": row_count,
        "truncated": truncated,
    }))
}

/// Render batches as CSV with a header row, honoring `limit`.
pub fn result_csv(batches: &[RecordBatch], limit: usize) -> Result<String> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = oxidant_loom::arrow::csv::Writer::new(&mut buf);
        let mut remaining = limit;
        for batch in batches {
            if remaining == 0 {
                break;
            }
            let n = batch.num_rows().min(remaining);
            writer
                .write(&batch.slice(0, n))
                .map_err(|e| Error::Execution(format!("csv encode: {e}")))?;
            remaining -= n;
        }
        // Dropping the writer flushes its buffer into `buf`.
    }
    String::from_utf8(buf).map_err(|e| Error::Execution(format!("csv encode: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidant_loom::arrow::array::{Int64Array, StringArray};
    use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2), None])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .expect("batch")
    }

    #[test]
    fn the_result_doc_matches_the_shape_the_renderers_expect() {
        let doc = result_doc(&[batch()], 100).expect("doc");
        assert_eq!(doc["schema"]["fields"][0]["name"], "n");
        assert_eq!(doc["schema"]["fields"][1]["name"], "s");
        assert_eq!(doc["rowCount"], 3);
        assert_eq!(doc["truncated"], false);
        assert_eq!(doc["rows"][0]["n"], 1);
        assert_eq!(doc["rows"][0]["s"], "a");
    }

    #[test]
    fn nulls_render_explicitly_rather_than_dropping_the_key() {
        // A missing key reads as "no such column", which is a different claim from "null".
        let doc = result_doc(&[batch()], 100).expect("doc");
        assert!(
            doc["rows"][1].get("s").is_some(),
            "the null key must be present"
        );
        assert!(doc["rows"][1]["s"].is_null());
        assert!(doc["rows"][2]["n"].is_null());
    }

    #[test]
    fn a_limit_truncates_and_says_so() {
        let doc = result_doc(&[batch()], 2).expect("doc");
        assert_eq!(doc["rowCount"], 2);
        assert_eq!(
            doc["truncated"], true,
            "a cut result must report it, or the row count implies completeness"
        );
    }

    #[test]
    fn an_empty_result_is_a_valid_document() {
        let doc = result_doc(&[], 100).expect("doc");
        assert_eq!(doc["rowCount"], 0);
        assert_eq!(doc["truncated"], false);
        assert_eq!(doc["rows"].as_array().map(Vec::len), Some(0));
    }

    #[test]
    fn csv_output_carries_a_header_row() {
        let csv = result_csv(&[batch()], 100).expect("csv");
        let mut lines = csv.lines();
        assert_eq!(lines.next(), Some("n,s"));
        assert_eq!(lines.next(), Some("1,a"));
    }

    #[test]
    fn csv_honors_the_limit() {
        let csv = result_csv(&[batch()], 1).expect("csv");
        // Header plus exactly one data row.
        assert_eq!(csv.lines().count(), 2);
    }
}
