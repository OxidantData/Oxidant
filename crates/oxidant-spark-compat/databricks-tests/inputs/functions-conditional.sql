-- Databricks SQL language manual: conditional and null-handling functions.
-- https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin
-- The function spellings of IS NULL, <=>, LIKE and IF.

SELECT isnull(NULL) AS a, isnull(1) AS b, isnotnull(NULL) AS c, isnotnull(1) AS d;

SELECT equal_null(NULL, NULL) AS a, equal_null(1, NULL) AS b,
       equal_null(NULL, 1) AS c, equal_null(1, 1) AS d, equal_null(1, 2) AS e;

SELECT like('abc', 'a%') AS a, like('abc', 'A%') AS b,
       ilike('abc', 'A%') AS c, like('abc', '_bc') AS d;

SELECT iff(1 > 0, 'yes', 'no') AS a, iff(1 < 0, 'yes', 'no') AS b;

SELECT nullifzero(0) AS a, zeroifnull(CAST(NULL AS INT)) AS b;
