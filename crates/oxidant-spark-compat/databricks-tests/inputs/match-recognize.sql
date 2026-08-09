-- Databricks SQL language manual: MATCH_RECOGNIZE.
-- https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-match-recognize
-- This is the manual's consecutive-rising-run example, retained verbatim apart from using
-- fewer rows. MATCH_RECOGNIZE is beta and applies to Databricks Runtime 19.0 and above.

CREATE OR REPLACE TEMPORARY VIEW stock_ticker AS
SELECT * FROM VALUES
  ('AAPL', TIMESTAMP '2024-01-01 09:30:00', 100.0),
  ('AAPL', TIMESTAMP '2024-01-01 09:31:00', 102.0),
  ('AAPL', TIMESTAMP '2024-01-01 09:32:00', 105.0),
  ('AAPL', TIMESTAMP '2024-01-01 09:33:00', 104.0),
  ('AAPL', TIMESTAMP '2024-01-01 09:34:00', 106.0),
  ('AAPL', TIMESTAMP '2024-01-01 09:35:00', 108.0)
AS t(symbol, tstamp, price);

SELECT symbol, start_tstamp, end_tstamp, run_length
FROM stock_ticker
MATCH_RECOGNIZE (
  PARTITION BY symbol
  ORDER BY tstamp
  MEASURES FIRST(tstamp) AS start_tstamp,
           LAST(tstamp) AS end_tstamp,
           COUNT(*) AS run_length
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (strt up+)
  DEFINE up AS price > PREV(price)
) AS t;
