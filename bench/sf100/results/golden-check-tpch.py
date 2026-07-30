#!/usr/bin/env python3
"""Golden checksums: compute TPC-H SF10 answers via DuckDB and compare against
the engine suite's recorded checksums (from run-spark-connect.py --json output).
No cluster load — reads the suite JSONL offline.
"""
import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "bench" / "sf100"))
from sf100_common import load_queries  # noqa: E402

import duckdb  # noqa: E402

DB = "/tmp/weft-sf10/tpch-sf10.db"


def duckdb_sql(sql: str) -> str:
    import re
    sql = sql.replace("glue.tpch_sf10.", "")
    # DuckDB rejects interval precision: interval '90' day (3) -> interval '90' day
    return re.sub(r"(interval '\d+' \w+) \(\d+\)", r"\1", sql)


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jsonl", required=True, help="suite jsonl from run-spark-connect.py")
    ap.add_argument("--only", default="")
    args = ap.parse_args()

    # engine checksums recorded by the suite
    eng = {}
    for line in Path(args.jsonl).read_text().splitlines():
        rec = json.loads(line)
        if rec.get("checksum") and rec.get("status") == "ok":
            eng[rec["query"]] = (rec["checksum"], rec.get("row_count"))

    only = {f"Q{n}" for n in args.only.split(",")} if args.only else None
    con = duckdb.connect(DB, read_only=True)
    n_ok = n_bad = n_skip = 0
    for name, sql in load_queries("tpch", sf=10):
        if only and name not in only:
            continue
        if name not in eng:
            print(f"{name}: SKIP (no engine result in jsonl)")
            n_skip += 1
            continue
        try:
            golden = checksum(con.execute(duckdb_sql(sql)).fetchall())
        except Exception as e:
            print(f"{name}: GOLDEN-ERROR {str(e)[:120]}")
            continue
        e_sum, e_rows = eng[name]
        tag = "MATCH" if golden == e_sum else "MISMATCH"
        n_ok += tag == "MATCH"
        n_bad += tag != "MATCH"
        print(f"{name}: {tag}  engine={e_sum[:12]} golden={golden[:12]} rows={e_rows}")
    print(f"SUMMARY match={n_ok} mismatch={n_bad} skipped={n_skip}")


if __name__ == "__main__":
    main()
