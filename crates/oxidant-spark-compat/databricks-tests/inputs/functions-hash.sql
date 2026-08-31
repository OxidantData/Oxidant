-- Databricks SQL language manual: hash functions.
-- https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin
-- Every expected digest is an independently computed reference value for the input
-- (`printf 'abc' | shasum -a N`, `python3 -c "import zlib; zlib.crc32(b'abc')"`).

SELECT sha('abc') AS a, sha1('abc') AS b, sha1('') AS c;

SELECT sha2('abc', 224) AS a;

SELECT sha2('abc', 256) AS a, sha2('abc', 0) AS b;

SELECT sha2('abc', 384) AS a;

SELECT sha2('abc', 512) AS a;

SELECT sha2('abc', 100) AS a;

SELECT crc32('abc') AS a, crc32('') AS b;

SELECT sha1(CAST(NULL AS STRING)) AS a, crc32(CAST(NULL AS STRING)) AS b;
