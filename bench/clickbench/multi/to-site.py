#!/usr/bin/env python3
"""Merge results/<engine>.json into site/src/data/benchmarks.json (the chart data source).

Per query, the *hot* time is min(try2, try3) (the ClickBench convention). An engine with no
results/<engine>.json yet stays `pending` with a null total, so the site renders a muted
"awaiting run" bar and the layout is final before any number exists. Failed queries are kept as
`null` in perQuery (shown as a gap), never dropped.

  python3 bench/clickbench/multi/to-site.py [--run-date YYYY-MM-DD] [--machine c6a.4xlarge]
"""
import argparse
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
RESULTS = os.path.join(HERE, "results")
OUT = os.path.join(REPO, "site", "src", "data", "benchmarks.json")

# Fixed display order + labels. `key` matches results/<key>.json and run-engine.sh.
ENGINES = [
    {"key": "oxidant", "name": "Oxidant", "highlight": True},
    {"key": "sail", "name": "Sail", "highlight": False},
    {"key": "spark", "name": "Spark", "highlight": False},
    {"key": "gluten", "name": "Spark + Gluten", "highlight": False},
]


def hot(tries):
    """ClickBench hot = min of the 2nd/3rd try, ignoring nulls; None if the query never ran."""
    vals = [t for t in tries[1:] if t is not None]
    return round(min(vals), 4) if vals else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-date", default=None)
    ap.add_argument("--machine", default="c6a.4xlarge")
    args = ap.parse_args()

    # First pass: load every available engine's per-query hot times.
    loaded = {}
    run_date = args.run_date
    for spec in ENGINES:
        path = os.path.join(RESULTS, f"{spec['key']}.json")
        if not os.path.exists(path):
            continue
        data = json.load(open(path))
        loaded[spec["key"]] = [hot(row) for row in data.get("result", [])]
        run_date = run_date or data.get("date")

    # The common set: query indices that EVERY measured engine completed. The headline speedup and
    # the main bar chart use totals over this set, so engines are compared on identical queries —
    # an engine isn't credited for skipping a hard query. Per-engine full totals + the exact failed
    # queries are still reported for transparency.
    n = max((len(v) for v in loaded.values()), default=43)
    common = [
        qi for qi in range(n)
        if loaded and all(v[qi] is not None for v in loaded.values() if qi < len(v))
        and all(qi < len(v) for v in loaded.values())
    ]

    out_engines = []
    for spec in ENGINES:
        if spec["key"] not in loaded:
            out_engines.append({
                "key": spec["key"], "name": spec["name"], "highlight": spec["highlight"],
                "total": None, "totalAll": None, "source": "pending",
                "perQuery": [], "failures": None, "failedQueries": [],
            })
            continue
        pq = loaded[spec["key"]]
        total_all = round(sum(h for h in pq if h is not None), 3)
        total_common = round(sum(pq[qi] for qi in common), 3) if common else None
        failed = [qi for qi, h in enumerate(pq) if h is None]
        out_engines.append({
            "key": spec["key"], "name": spec["name"], "highlight": spec["highlight"],
            "total": total_common,          # fair, common-set total (chart + speedup)
            "totalAll": total_all,          # this engine's full successful-query total
            "source": f"measured (EC2 {args.machine} {run_date})",
            "perQuery": pq,
            "failures": len(failed),
            "failedQueries": failed,
        })

    doc = {
        "dataset": "hits.parquet — 99,997,497 rows, 14.78 GB (ClickBench)",
        "machine": args.machine,
        "runDate": run_date,
        "queryCount": 43,
        "commonCount": len(common),
        "method": "Spark Connect, stock PySpark client, 3 tries/query, hot = min(try2, try3)",
        "engines": out_engines,
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(doc, f, indent=2)
        f.write("\n")
    measured = [e["name"] for e in out_engines if e["total"] is not None]
    print(f"wrote {OUT}\n  measured: {measured or 'none yet'}")


if __name__ == "__main__":
    main()
