#!/usr/bin/env python3
"""Row-level diff for TPC-DS queries: engine (Spark Connect) vs DuckDB golden.
Run from FILE (pyspark quirk). Usage: golden-diff-tpcds.py Q7 Q12 ...
"""
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "bench" / "sf100"))
from sf100_common import TPCDS_TABLES, load_queries, qualify_sql  # noqa: E402

import duckdb  # noqa: E402

DB = "/tmp/oxidant-sf10/tpcds-sf10.db"
ENDPOINT = "sc://35.85.61.45:50051"


def canonical_cell(v) -> str:
    from decimal import Decimal
    if v is None:
        return "NULL"
    if isinstance(v, Decimal):
        return format(v.normalize(), "f")
    if isinstance(v, float):
        if v == 0.0:
            return "0"
        return f"{v:.12g}"
    return repr(v)


def duckdb_sql(sql: str) -> str:
    sql = sql.replace("glue.tpcds_sf10.", "")
    return re.sub(r"(interval '\d+' \w+) \(\d+\)", r"\1", sql)


def canon_lines(rows):
    lines = []
    for row in rows:
        vals = tuple(row) if hasattr(row, "__iter__") else (row,)
        lines.append("(" + ", ".join(canonical_cell(v) for v in vals) + ")")
    lines.sort()
    return lines


def main():
    names = sys.argv[1:]
    queries = dict(load_queries("tpcds", sf=10))
    con = duckdb.connect(DB, read_only=True)

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.remote(ENDPOINT).getOrCreate()

    for name in names:
        sql = queries[name]
        eng_sql = qualify_sql(sql.strip().rstrip(";").strip(), "tpcds_sf10", TPCDS_TABLES)
        try:
            golden_rows = con.execute(duckdb_sql(sql)).fetchall()
        except Exception as e:
            print(f"{name}: GOLDEN-ERROR {str(e)[:100]}")
            continue
        try:
            eng_rows = spark.sql(eng_sql).collect()
        except Exception as e:
            print(f"{name}: ENGINE-ERROR {str(e)[:200]}")
            continue

        g = canon_lines(golden_rows)
        e = canon_lines([tuple(r) for r in eng_rows])
        gs, es = set(g), set(e)
        only_g = sorted(gs - es)
        only_e = sorted(es - gs)
        print(f"{name}: engine_rows={len(e)} golden_rows={len(g)} "
              f"only-golden={len(only_g)} only-engine={len(only_e)}")
        for line in only_g[:4]:
            print("G:", line[:250])
        for line in only_e[:4]:
            print("E:", line[:250])
        sys.stdout.flush()
    spark.stop()


if __name__ == "__main__":
    main()
