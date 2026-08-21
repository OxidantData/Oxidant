--SKIP requires-delta-storage
-- Databricks SQL language manual: COPY INTO.
-- https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html
-- Skipped: COPY INTO loads files from cloud storage into a Delta table; it needs Delta
-- storage + object-store credentials, neither of which the golden-replay engine has.

COPY INTO people
FROM 's3://bucket/landing/people/'
FILEFORMAT = PARQUET;

COPY INTO events
FROM 's3://bucket/landing/events/'
FILEFORMAT = JSON
FILES = ('2024-01-01.json', '2024-01-02.json');

COPY INTO people
FROM 's3://bucket/landing/people/'
FILEFORMAT = PARQUET
COPY_OPTIONS ('mergeSchema' = 'true');
