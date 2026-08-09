--SKIP requires-delta-storage
-- Databricks SQL language manual: Delta Lake SQL (time travel, maintenance commands).
-- https://docs.databricks.com/en/sql/language-manual/delta-describe-history.html
-- Skipped: every statement here operates on Delta Lake table metadata/storage.

SELECT * FROM events TIMESTAMP AS OF '2024-01-01T00:00:00Z';

SELECT * FROM events VERSION AS OF 3;

DESCRIBE HISTORY events;

OPTIMIZE events WHERE day = DATE '2024-01-01' ZORDER BY (ts);

VACUUM events RETAIN 168 HOURS;

RESTORE TABLE events TO VERSION AS OF 2;
