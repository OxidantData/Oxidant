-- Databricks SQL language manual: LATERAL VIEW.
-- https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-lateral-view.html

CREATE OR REPLACE TEMPORARY VIEW items (id INT, tags ARRAY<STRING>) AS VALUES
  (1, array('a', 'b')), (2, array('c')), (3, array());

SELECT id, tag FROM items LATERAL VIEW explode(tags) AS tag ORDER BY id, tag;

SELECT id, tag FROM items LATERAL VIEW OUTER explode(tags) AS tag ORDER BY id, tag;

SELECT id, pos, tag FROM items LATERAL VIEW posexplode(tags) AS pos, tag ORDER BY id, pos;
