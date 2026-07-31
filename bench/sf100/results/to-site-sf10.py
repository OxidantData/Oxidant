#!/usr/bin/env python3
"""Emit site/src/data/tpch.json + site/src/data/tpcds.json from the SF10 runs.

Ground truth is per-query JSONL (one record per query: status, hot_s, runDate):

- weft: run-spark-connect.py against the distributed cluster (kan49-wave3).
- Spark on EMR: bench/sf100/emr/run-emr-suite.py (emr-compare/), same queries and
  same S3 bytes on the same instance spec (1x c6g.2xlarge driver/master,
  2x m8g.2xlarge workers/core).

perQuery is positional (Q1 -> index 0); failed queries stay `null` so the site
renders them as gaps, never dropped. failedQueries holds the canonical 1-based
query numbers. With more than one engine, `total` is the fair headline: the sum
over the *common set* — queries every engine completed. `totalAll` is each
engine's own total over everything it finished. No figure on the site is ever
hand-entered; regenerate after a suite completes:

  python3 bench/sf100/results/to-site-sf10.py
"""
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
SITE = os.path.join(REPO, "site", "src", "data")

MACHINE = "ec2 1x c6g.2xlarge driver/master + 2x m8g.2xlarge workers/core (arm64, us-west-2)"
METHOD = (
    "Same query text, same SF10 Parquet bytes on S3. Weft: DISTRIBUTED strict mode via "
    "Spark Connect (run-spark-connect.py), golden-validated vs DuckDB SF10. "
    "Spark: stock EMR 7.13.0 on YARN (run-emr-suite.py), temp views over the same S3 prefix."
)

ENGINES = {
    "weft": {
        "key": "weft-dist",
        "name": "Weft (distributed, 2 workers)",
        "highlight": True,
        "source": "measured distributed (ec2/weft-sf10, 1 driver + 2 workers)",
    },
    "emr": {
        "key": "spark-emr",
        "name": "Apache Spark 3.5.6 (EMR 7.13.0, YARN)",
        "highlight": False,
        "source": "measured (EMR 7.13.0, 1 master + 2 core, same instance spec)",
    },
}

SUITES = [
    {
        "out": os.path.join(SITE, "tpch.json"),
        "dataset": "TPC-H SF10 Parquet on S3 (`s3://weft-artifacts-…/tpch-sf10/`)",
        "jsonl": {
            "weft": os.path.join(HERE, "kan49-wave3", "tpch-sf10-99stack.jsonl"),
            "emr": os.path.join(HERE, "emr-compare", "tpch-sf10-emr.jsonl"),
        },
    },
    {
        "out": os.path.join(SITE, "tpcds.json"),
        "dataset": "TPC-DS SF10 Parquet on S3 (`s3://weft-artifacts-…/tpcds-sf10/`)",
        "jsonl": {
            "weft": os.path.join(HERE, "kan49-wave3", "tpcds-sf10-99.jsonl"),
            "emr": os.path.join(HERE, "emr-compare", "tpcds-sf10-emr.jsonl"),
        },
    },
]


def qnum(name):
    """"Q14" -> 14."""
    return int(re.sub(r"^[Qq]", "", name))


def load(path):
    """Return {qnum: record} for a JSONL run; raises if the query set isn't contiguous."""
    rows = [json.loads(line) for line in open(path) if line.strip()]
    by_q = {}
    for r in rows:
        n = qnum(r["query"])
        # later records win (reruns overwrite earlier failures)
        if n not in by_q or r["status"] == "ok":
            by_q[n] = r
    nums = sorted(by_q)
    if nums != list(range(1, len(nums) + 1)):
        raise SystemExit(f"{path}: non-contiguous query set: {nums}")
    return by_q


def emit(spec):
    engines_data = {}
    run_dates = []
    for eng_key, path in spec["jsonl"].items():
        if not os.path.exists(path):
            raise SystemExit(f"missing {path} — run the suite first")
        engines_data[eng_key] = load(path)
        run_dates.append(max(r["runDate"] for r in engines_data[eng_key].values())[:10])

    n_queries = len(next(iter(engines_data.values())))
    ok_sets = {
        k: {n for n, r in d.items() if r["status"] == "ok"} for k, d in engines_data.items()
    }
    common = set.intersection(*ok_sets.values())

    engines = []
    for eng_key, meta in ENGINES.items():
        d = engines_data[eng_key]
        ok = ok_sets[eng_key]
        per_query = [
            round(d[n]["hot_s"], 3) if d[n]["status"] == "ok" else None
            for n in range(1, n_queries + 1)
        ]
        failed = [n for n in range(1, n_queries + 1) if n not in ok]
        total = round(sum(d[n]["hot_s"] for n in common), 3)
        total_all = round(sum(d[n]["hot_s"] for n in ok), 3)
        engines.append(
            {
                "key": meta["key"],
                "name": meta["name"],
                "highlight": meta["highlight"],
                "total": total,
                "totalAll": total_all,
                "source": meta["source"],
                "perQuery": per_query,
                "failures": len(failed),
                "failedQueries": failed,
            }
        )

    doc = {
        "dataset": spec["dataset"],
        "machine": MACHINE,
        "runDate": max(run_dates),
        "queryCount": n_queries,
        "commonCount": len(common),
        "method": METHOD,
        "engines": engines,
    }
    with open(spec["out"], "w") as f:
        json.dump(doc, f, indent=2)
        f.write("\n")
    summary = ", ".join(
        f"{e['key']}: {n_queries - e['failures']}/{n_queries} ok, common {e['total']}s, all {e['totalAll']}s"
        for e in engines
    )
    print(f"wrote {spec['out']}\n  common {len(common)}/{n_queries}; {summary}")


def main():
    for spec in SUITES:
        emit(spec)


if __name__ == "__main__":
    main()
