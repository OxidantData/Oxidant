#!/usr/bin/env python3
"""Emit site/src/data/tpch.json + site/src/data/tpcds.json from the SF10 distributed runs.

Ground truth is the run-spark-connect.py JSONL (one record per query: status, hot_s,
runDate). perQuery is positional (Q1 -> index 0); failed queries stay `null` so the
site renders them as gaps, never dropped. failedQueries holds the canonical 1-based
query numbers (TPC-H/TPC-DS name their queries Q1..QN and the site prints them
verbatim as "Q<n>"). With a single measured engine the common set is exactly the
ok set, so total == totalAll == sum of ok hot times.

Run after a suite completes (no figure on the site is ever hand-entered):

  python3 bench/sf100/results/to-site-sf10.py
"""
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
SITE = os.path.join(REPO, "site", "src", "data")

MACHINE = "ec2/weft-sf10 distributed (1 driver + 2 workers, AL2023 arm64)"
METHOD = (
    "DISTRIBUTED strict mode via Spark Connect (run-spark-connect.py), "
    "golden-validated vs DuckDB SF10"
)

SUITES = [
    {
        "jsonl": os.path.join(HERE, "kan47-validation", "tpch-sf10-final.jsonl"),
        "out": os.path.join(SITE, "tpch.json"),
        "dataset": "TPC-H SF10 via Glue (`glue.tpch_sf10.*`) on S3",
    },
    {
        "jsonl": os.path.join(HERE, "tpcds-final", "tpcds-sf10.jsonl"),
        "out": os.path.join(SITE, "tpcds.json"),
        "dataset": "TPC-DS SF10 via Glue (`glue.tpcds_sf10.*`) on S3",
    },
]


def qnum(name):
    """"Q14" -> 14."""
    return int(re.sub(r"^[Qq]", "", name))


def emit(spec):
    rows = [json.loads(line) for line in open(spec["jsonl"]) if line.strip()]
    rows.sort(key=lambda r: qnum(r["query"]))
    nums = [qnum(r["query"]) for r in rows]
    if nums != list(range(1, len(rows) + 1)):
        raise SystemExit(f"{spec['jsonl']}: non-contiguous query set: {nums}")

    run_date = max(r["runDate"] for r in rows)[:10]
    per_query = [round(r["hot_s"], 3) if r["status"] == "ok" else None for r in rows]
    failed = [n for n, r in zip(nums, rows) if r["status"] != "ok"]
    total = round(sum(v for v in per_query if v is not None), 3)

    doc = {
        "dataset": spec["dataset"],
        "machine": MACHINE,
        "runDate": run_date,
        "queryCount": len(rows),
        "commonCount": len(rows) - len(failed),
        "method": METHOD,
        "engines": [
            {
                "key": "weft-dist",
                "name": "Weft (distributed, 2 workers)",
                "highlight": True,
                "total": total,
                "totalAll": total,
                "source": f"measured distributed (ec2/weft-sf10, 1 driver + 2 workers, {run_date})",
                "perQuery": per_query,
                "failures": len(failed),
                "failedQueries": failed,
            }
        ],
    }
    with open(spec["out"], "w") as f:
        json.dump(doc, f, indent=2)
        f.write("\n")
    ok = len(rows) - len(failed)
    print(f"wrote {spec['out']}\n  {ok}/{len(rows)} ok, total {total}s, runDate {run_date}")


def main():
    for spec in SUITES:
        emit(spec)


if __name__ == "__main__":
    main()
