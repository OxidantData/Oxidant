#!/usr/bin/env python3
"""Golden checksums: compute TPC-H SF10 answers via DuckDB and compare against
the engine suite's recorded checksums (from run-spark-connect.py --json output).
No cluster load — reads the suite JSONL offline.

Three-way verdict per query (KAN-50): MATCH / BENIGN / MISMATCH. BENIGN covers
the two known-benign diff classes — numeric-scale (engine DECIMAL scale vs
DuckDB f64, e.g. Q1/Q8) and ORDER BY…LIMIT boundary picks — with the reason
printed per query and benign queries listed separately in the summary. The
canonicalization and verdict rules live in golden_common.py (run
``python3 golden_common.py --self-test`` for their unit-ish validation).
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from golden_common import run_check  # noqa: E402

DB = "/tmp/weft-sf10/tpch-sf10.db"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--jsonl", required=True, help="suite jsonl from run-spark-connect.py")
    ap.add_argument("--only", default="")
    ap.add_argument("--db", default=DB, help="DuckDB database with the SF10 goldens")
    ap.add_argument(
        "--strict-benign",
        action="store_true",
        help="reclassify heuristic (unproven) BENIGN verdicts as MISMATCH",
    )
    args = ap.parse_args()
    sys.exit(
        run_check(
            "tpch",
            args.db,
            args.jsonl,
            "glue.tpch_sf10.",
            only=args.only,
            strict_benign=args.strict_benign,
        )
    )


if __name__ == "__main__":
    main()
