--SKIP output-order-is-not-guaranteed
-- Spark SQL language manual: CLUSTER BY / DISTRIBUTE BY / SORT BY.

CREATE OR REPLACE TEMPORARY VIEW sales (region STRING, amount INT) AS VALUES
  ('west', 100), ('east', 200), ('west', 150), ('east', 50), ('south', 75);

SELECT region, amount FROM sales CLUSTER BY region;

SELECT region, amount FROM sales DISTRIBUTE BY region SORT BY region, amount;

SELECT region, amount FROM sales DISTRIBUTE BY region SORT BY amount DESC;

SELECT region FROM sales DISTRIBUTE BY region SORT BY region LIMIT 3;
