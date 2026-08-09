-- Databricks SQL language manual: PIVOT / UNPIVOT.
-- https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-pivot.html
-- https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-unpivot.html

CREATE OR REPLACE TEMPORARY VIEW quarterly (region STRING, quarter STRING, amount INT) AS VALUES
  ('west', 'Q1', 100), ('west', 'Q2', 150), ('east', 'Q1', 200), ('east', 'Q2', 50);

CREATE OR REPLACE TEMPORARY VIEW wide (region STRING, q1 INT, q2 INT) AS VALUES
  ('west', 100, 150), ('east', 200, 50), ('south', NULL, 10);

SELECT * FROM quarterly PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2')) ORDER BY region;

SELECT * FROM quarterly PIVOT (SUM(amount) AS total FOR quarter IN ('Q1' AS q1, 'Q2' AS q2)) ORDER BY region;

SELECT region, quarter, amount FROM wide UNPIVOT (amount FOR quarter IN (q1, q2)) ORDER BY region, quarter;

SELECT * FROM wide UNPIVOT INCLUDE NULLS (amount FOR quarter IN (q1, q2)) ORDER BY region, quarter;
