# Databricks coverage probes (KAN-89)

Companion to [`docs/databricks-coverage.md`](../docs/databricks-coverage.md).

The live probe session that produced the matrix ran **578** statements
(508 first pass + 55 follow-ups + 15 semantic checks). A machine-readable
transcript was not retained from that session. This file is the audit trail
that *can* be reconstructed from the matrix Evidence columns, plus notes on
what cannot.

KAN-90 owns a reproducible Databricks SQL corpus for the parity harness; use
that for regression, not this document.

## First-pass accounting (WR-05)

| Bucket | Count |
|--------|------:|
| Scored across four axes (see coverage headline table) | 505 |
| Setup / DDL warm-ups (create fixture tables, not attributed to an axis) | 3 |
| **First pass total** | **508** |
| Follow-ups | 55 |
| Semantic checks | 15 |
| **Grand total** | **578** |

Axis denominators: Data types 32 + Operators 43 + Statements 186 + Functions 244 = 505.

## Function-category ratios (section F)

Ratios such as `21/30` or `6/8` are **probe-level** (one HTTP statement may
exercise several functions). The name lists in the matrix are **function-level**.
Where those disagree in count, the probe bundled multiple names — called out
inline for Struct (`1/5` with three working constructors in one probe) and
Conditional (`6/8` with eight working names across six probes).

Without the original per-statement log, individual function ratios cannot be
re-audited line-by-line from this file. The category totals still sum to
exactly **124/244**, matching the headline Functions axis.

## Reconstructible statement / type / operator probes

Replay against a server started as in the coverage doc (`oxidant spark server
--port 50051 --sample-data sample-data`). Fixture setup used in the original
session (representative):

```sql
CREATE TABLE cov_t (id INT, name STRING, amt DECIMAL(10,2)) USING parquet;
INSERT INTO cov_t VALUES (1, 'a', 10.0), (2, 'b', 20.0), (3, 'c', 30.0);
CREATE TABLE cov_dml (a INT, b STRING) USING parquet;
```

### Representative gap probes (Missing / Partial owners)

```sql
-- KAN-112 COPY INTO
COPY INTO cov_dml FROM '/tmp/probe/out' FILEFORMAT = PARQUET;

-- KAN-113 distribution clauses
SELECT id FROM cov_t SORT BY id;
SELECT id FROM cov_t CLUSTER BY id;
SELECT id FROM cov_t DISTRIBUTE BY id;

-- KAN-114 LATERAL VIEW
SELECT * FROM cov_t t LATERAL VIEW explode(array(1,2)) e AS v;

-- KAN-111 TABLESAMPLE (parses, ignored)
SELECT count(*) FROM cov_t TABLESAMPLE (1 ROWS);

-- KAN-115 TRANSFORM
SELECT TRANSFORM(id) USING 'cat' AS (x) FROM cov_t;

-- KAN-116 planner
SELECT t.id, s.x FROM cov_t t, LATERAL (SELECT t.id + 1 AS x) s;
WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 3) SELECT * FROM c;
SELECT id, (SELECT count(*) FROM cov_t u WHERE u.id <= t.id) AS c FROM cov_t t;
SELECT 1 = ANY (SELECT id FROM cov_t);

-- KAN-117 types
SELECT CAST('ab' AS BINARY);
SELECT CAST('2024-01-02 03:04:05' AS TIMESTAMP_NTZ);
SELECT CAST(NULL AS VARIANT);
SELECT CAST(NULL AS OBJECT<a: INT>);
SELECT INTERVAL '1-2' YEAR TO MONTH;
SELECT concat('[', CAST('ab' AS CHAR(4)), ']');

-- KAN-118 array subscript base (observed z=null, o=10; Spark expects 10, 20)
SELECT array(10,20,30)[0] AS z, array(10,20,30)[1] AS o;

-- KAN-110 SQL UDF eval
CREATE OR REPLACE FUNCTION cov_add2(x INT) RETURNS INT RETURN x + 100;
SELECT cov_add2(1);
SELECT cov_add2(5);
```

Every other Evidence cell in `docs/databricks-coverage.md` that quotes SQL is a
probe from the same session and can be replayed the same way.
