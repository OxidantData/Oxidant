--SKIP requires-delta-storage
-- Databricks SQL language manual: MERGE INTO.
-- https://docs.databricks.com/en/sql/language-manual/delta-merge-into.html
-- Skipped: MERGE INTO requires a Delta Lake target table.

MERGE INTO people AS target
USING updates AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET target.name = source.name, target.age = source.age
WHEN NOT MATCHED THEN INSERT (id, name, age) VALUES (source.id, source.name, source.age);

MERGE INTO people AS target
USING updates AS source
ON target.id = source.id
WHEN MATCHED AND source.age < 0 THEN DELETE
WHEN NOT MATCHED BY SOURCE THEN DELETE;
