#!/usr/bin/env python3
"""Golden validation: engine (Spark Connect) TPC-H SF10 results vs DuckDB answers.

For each query: run through the engine (glue.tpch_sf10.*) and through DuckDB
(/tmp/weft-sf10/tpch-sf10.db), then compare canonical checksums using the SAME
canonicalization as bench/sf100/run-spark-connect.py (_result_checksum).
"""
import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "bench" / "sf100"))
from sf100_common import load_queries, qualify_sql  # noqa: E402

import duckdb  # noqa: E402

SPARK_CONNECT = "sc://18.236.223.115:50051"
DB = "/tmp/weft-sf10/tpch-sf10.db"


def canonical_cell(v):
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


def checksum(rows) -> str:
    import hashlib
    lines = []
    for row in rows:
        vals = tuple(row) if hasattr(row, "__iter__") else (row,)
        lines.append("(" + ", ".join(canonical_cell(v) for v in vals) + ")")
    lines.sort()
    h = hashlib.sha256()
    for line in lines:
        h.update(line.encode("utf-8", "replace"))
        h.update(b"\n")
    return h.hexdigest()


def duckdb_sql(sql: str) -> str:
    # engine: glue.tpch_sf10.<table> -> duckdb: <table>
    return sql.replace("glue.tpch_sf10.", "")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="")
    ap.add_argument("--json", default="")
    args = ap.parse_args()

    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder.remote(SPARK_CONNECT)
        .config("spark.sql.catalog.glue.type", "glue")
        .config("spark.sql.catalog.glue.region", "us-west-2")
        .getOrCreate()
    )
    con = duckdb.connect(DB, read_only=True)

    only = {f"Q{n}" for n in args.only.split(",")} if args.only else None
    out = open(args.json, "a") if args.json else None
    n_ok = n_bad = n_err = 0
    for name, sql in load_queries("tpch", sf=10):
        if only and name not in only:
            continue
        eng_sql = qualify_sql(sql, "tpch_sf10", [
            "lineitem", "orders", "part", "partsupp", "customer", "supplier", "nation", "region"])
        rec = {"q": name}
        try:
            golden = checksum(con.execute(duckdb_sql(sql)).fetchall())
            rec["golden"] = golden[:12]
        except Exception as e:
            rec["golden_error"] = str(e)[:160]
            golden = None
        try:
            rows = spark.sql(eng_sql).collect()
            eng = checksum([tuple(r) for r in rows])
            rec["engine"] = eng[:12]
            rec["rows"] = len(rows)
        except Exception as e:
            rec["engine_error"] = str(e)[:160]
            eng = None
        if golden and eng:
            rec["match"] = golden == eng
            n_ok += rec["match"]
            n_bad += not rec["match"]
        else:
            n_err += 1
        print(rec, flush=True)
        if out:
            out.write(json.dumps(rec) + "\n")
            out.flush()
    print(f"SUMMARY match={n_ok} mismatch={n_bad} errors={n_err}")
    spark.stop()


if __name__ == "__main__":
    main()
