//! Partition columns that are not last in the declared schema must still read in
//! declared order. DataFusion appends Hive partition fields after the file schema;
//! without a remap, `SELECT id, name, seq` on a table partitioned by `name` swaps
//! `name` and `seq` (silent wrong data).

use oxidant_loom::arrow::array::{Int64Array, StringArray};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::datafusion::parquet::arrow::ArrowWriter;
use oxidant_loom::Engine;
use std::sync::Arc;

fn write_partitioned_delta(partition_not_last: bool) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    std::fs::create_dir_all(dir.join("_delta_log")).unwrap();

    let file_path = if partition_not_last {
        std::fs::create_dir_all(dir.join("name=ada")).unwrap();
        "name=ada/part-0.parquet".to_string()
    } else {
        std::fs::create_dir_all(dir.join("seq=2")).unwrap();
        "seq=2/part-0.parquet".to_string()
    };

    let file_schema = if partition_not_last {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("seq", DataType::Int64, false),
        ]))
    } else {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    };
    let batch = if partition_not_last {
        RecordBatch::try_new(
            file_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(Int64Array::from(vec![2i64])),
            ],
        )
        .unwrap()
    } else {
        RecordBatch::try_new(
            file_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(StringArray::from(vec!["ada"])),
            ],
        )
        .unwrap()
    };
    {
        let f = std::fs::File::create(dir.join(&file_path)).unwrap();
        let mut w = ArrowWriter::try_new(f, file_schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
    let file_size = std::fs::metadata(dir.join(&file_path)).unwrap().len();

    let (fields, partition_columns, partition_values) = if partition_not_last {
        (
            vec![
                serde_json::json!({"name": "id", "type": "long", "nullable": false, "metadata": {}}),
                serde_json::json!({"name": "name", "type": "string", "nullable": true, "metadata": {}}),
                serde_json::json!({"name": "seq", "type": "long", "nullable": false, "metadata": {}}),
            ],
            vec!["name".to_string()],
            serde_json::json!({"name": "ada"}),
        )
    } else {
        (
            vec![
                serde_json::json!({"name": "id", "type": "long", "nullable": false, "metadata": {}}),
                serde_json::json!({"name": "name", "type": "string", "nullable": false, "metadata": {}}),
                serde_json::json!({"name": "seq", "type": "string", "nullable": true, "metadata": {}}),
            ],
            vec!["seq".to_string()],
            serde_json::json!({"seq": "2"}),
        )
    };
    let schema_string = serde_json::json!({"type": "struct", "fields": fields}).to_string();
    let commit0 = [
        serde_json::json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}).to_string(),
        serde_json::json!({
            "metaData": {
                "id": "00000000-0000-0000-0000-000000000113",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_string,
                "partitionColumns": partition_columns,
                "configuration": {}
            }
        })
        .to_string(),
        serde_json::json!({
            "add": {
                "path": file_path,
                "partitionValues": partition_values,
                "size": file_size,
                "modificationTime": 0,
                "dataChange": true
            }
        })
        .to_string(),
    ]
    .join("\n");
    std::fs::write(dir.join("_delta_log/00000000000000000000.json"), commit0).unwrap();
    tmp
}

#[tokio::test]
async fn partitioned_delta_middle_column_keeps_declared_order() {
    let tmp = write_partitioned_delta(true);
    let path = tmp.path().to_str().unwrap();
    let engine = Engine::new();
    engine.register_delta("t", path).await.unwrap();

    let batches = engine
        .sql("SELECT id, name, seq FROM t")
        .await
        .unwrap_or_else(|e| panic!("select: {e}"));
    let batch = &batches[0];
    assert_eq!(
        batch.schema().field(0).data_type(),
        &DataType::Int64,
        "{:?}",
        batch.schema()
    );
    assert_eq!(
        batch.schema().field(1).data_type(),
        &DataType::Utf8,
        "{:?}",
        batch.schema()
    );
    assert_eq!(
        batch.schema().field(2).data_type(),
        &DataType::Int64,
        "{:?}",
        batch.schema()
    );
    let id = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let name = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let seq = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id.value(0), 1);
    assert_eq!(name.value(0), "ada");
    assert_eq!(seq.value(0), 2);
    assert_eq!(batch.schema().field(0).name(), "id");
    assert_eq!(batch.schema().field(1).name(), "name");
    assert_eq!(batch.schema().field(2).name(), "seq");

    let star = engine
        .sql("SELECT * FROM t")
        .await
        .unwrap_or_else(|e| panic!("star: {e}"));
    assert_eq!(star[0].schema().field(0).name(), "id");
    assert_eq!(star[0].schema().field(1).name(), "name");
    assert_eq!(star[0].schema().field(2).name(), "seq");
    let star_name = star[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(star_name.value(0), "ada");

    let filtered = engine
        .sql("SELECT id FROM t WHERE name = 'ada'")
        .await
        .unwrap_or_else(|e| panic!("where: {e}"));
    let id = filtered[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id.value(0), 1);
}

/// Control: partitioning on the last declared column already worked.
#[tokio::test]
async fn partitioned_delta_last_column_still_reads() {
    let tmp = write_partitioned_delta(false);
    let path = tmp.path().to_str().unwrap();
    let engine = Engine::new();
    engine.register_delta("t", path).await.unwrap();
    let batches = engine
        .sql("SELECT id, name FROM t WHERE seq = '2'")
        .await
        .unwrap_or_else(|e| panic!("control: {e}"));
    let name = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name.value(0), "ada");
}
