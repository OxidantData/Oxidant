--SKIP requires-catalog-and-delta-storage
-- Databricks SQL language manual: CREATE TABLE … USING, TBLPROPERTIES, PARTITIONED BY.
-- https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html

CREATE TABLE people (id INT, name STRING, age INT) USING delta;

CREATE TABLE parquet_people (id INT, name STRING) USING parquet
TBLPROPERTIES ('parquet.compression' = 'snappy');

CREATE TABLE events (id INT, ts TIMESTAMP, day DATE) USING delta
PARTITIONED BY (day);

CREATE TABLE IF NOT EXISTS people (id INT, name STRING, age INT) USING delta;

CREATE TABLE broken (id INT) USING not_a_data_source;
