-- Databricks SQL language manual: numeric scalar functions.
-- https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin
-- The math names DataFusion does not register, plus Spark's sign conventions for mod/pmod.

SELECT e() AS a;

SELECT rint(CAST(2.5 AS DOUBLE)) AS a, rint(CAST(3.5 AS DOUBLE)) AS b,
       rint(CAST(-2.5 AS DOUBLE)) AS c, rint(CAST(2.4 AS DOUBLE)) AS d;

SELECT hypot(CAST(3 AS DOUBLE), CAST(4 AS DOUBLE)) AS a,
       expm1(CAST(0 AS DOUBLE)) AS b, log1p(CAST(0 AS DOUBLE)) AS c,
       sec(CAST(0 AS DOUBLE)) AS d;

SELECT mod(7, 3) AS a, mod(-7, 3) AS b, mod(7, -3) AS c;

SELECT pmod(7, 3) AS a, pmod(-7, 3) AS b, pmod(7, -3) AS c, pmod(-7, -3) AS d;

SELECT negative(1) AS a, negative(-1) AS b, negative('-1.11') AS c;

SELECT bit_reverse(CAST(1 AS TINYINT)) AS a, bit_reverse(CAST(1 AS INT)) AS b,
       bit_reverse(CAST(-1 AS INT)) AS c;

SELECT bit_get(CAST(11 AS BIGINT), 0) AS a, bit_get(CAST(11 AS BIGINT), 2) AS b;

SELECT width_bucket(5.0, 0.0, 10.0, 5) AS a, width_bucket(-1.0, 0.0, 10.0, 5) AS b,
       width_bucket(11.0, 0.0, 10.0, 5) AS c, width_bucket(5.0, 10.0, 0.0, 5) AS d;
