--SKIP requires-catalog-and-delta-storage
-- Spark SQL language manual: CREATE TABLE … USING, TBLPROPERTIES, PARTITIONED BY.

CREATE TABLE people (id INT, name STRING, age INT) USING delta;

CREATE TABLE parquet_people (id INT, name STRING) USING parquet
TBLPROPERTIES ('parquet.compression' = 'snappy');

CREATE TABLE events (id INT, ts TIMESTAMP, day DATE) USING delta
PARTITIONED BY (day);

CREATE TABLE IF NOT EXISTS people (id INT, name STRING, age INT) USING delta;

CREATE TABLE broken (id INT) USING not_a_data_source;
