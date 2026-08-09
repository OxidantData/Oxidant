-- Databricks SQL language manual: QUALIFY.
-- https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-qualify.html

CREATE OR REPLACE TEMPORARY VIEW sales (region STRING, amount INT) AS VALUES
  ('west', 100), ('east', 200), ('west', 150), ('east', 50), ('south', 75);

SELECT region, amount FROM sales
QUALIFY row_number() OVER (PARTITION BY region ORDER BY amount DESC) = 1
ORDER BY region;

SELECT region, amount FROM sales
QUALIFY rank() OVER (PARTITION BY region ORDER BY amount DESC) <= 1
ORDER BY region, amount DESC;
