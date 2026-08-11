#!/usr/bin/env python3
"""Run TPC-H against a remote Oxidant Spark Connect endpoint (EC2 SF100 path).

Features the ad-hoc loop lacked on 2026-08-10:
  - qualify unqualified table names to glue.<db>.<table>
  - 3 tries / query, hot = min(try2, try3)
  - recreate the Spark session after driver OOM / session loss
  - resume from a prior results JSON (skip queries that already have hot times)

Example:
  python3 bench/tpch/run-ec2-connect.py \\
    --endpoint sc://35.163.191.126:50051 \\
    --glue-database tpch_sf100 \\
    --out bench/tpch/results/tpch-sf100-ec2.json
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import date
from pathlib import Path

TABLES = [
    "lineitem",
    "orders",
    "customer",
    "part",
    "partsupp",
    "supplier",
    "nation",
    "region",
]


def qualify(sql: str, database: str) -> str:
    body = sql
    for t in TABLES:
        body = re.sub(
            rf"(?i)(?<![\w.]){t}(?![\w.])",
            f"glue.{database}.{t}",
            body,
        )
    return body


def session_dead(err: str) -> bool:
    e = err.lower()
    return any(
        x in e
        for x in (
            "no_active_session",
            "no active spark session",
            "incorrect server side session",
            "connection refused",
            "statuscode.unavailable",
            "not found (released or expired)",
            "stage cancelled by driver",
        )
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--endpoint", required=True, help="sc://host:50051")
    ap.add_argument("--glue-database", default="tpch_sf100")
    ap.add_argument(
        "--queries",
        type=Path,
        default=Path(__file__).resolve().parent / "queries",
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parent / "results" / "tpch-sf100-ec2.json",
    )
    ap.add_argument("--machine", default="oxidant-sf100")
    ap.add_argument("--start", type=int, default=1)
    ap.add_argument("--end", type=int, default=22)
    ap.add_argument(
        "--tries",
        type=int,
        default=3,
        help="Attempts per query (default 3; use 1 for SF100 stability gates)",
    )
    ap.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore prior hot times in --out",
    )
    args = ap.parse_args()
    if args.tries < 1:
        ap.error("--tries must be >= 1")

    from pyspark.sql import SparkSession

    prior: dict = {}
    if args.out.exists() and not args.no_resume:
        prior = {
            q["query"]: q
            for q in json.loads(args.out.read_text()).get("queries", [])
        }

    def new_spark() -> SparkSession:
        return SparkSession.builder.remote(args.endpoint).getOrCreate()

    spark = new_spark()
    print(f"connected {args.endpoint}", flush=True)

    results = []
    failures = 0
    for n in range(args.start, args.end + 1):
        name = f"Q{n}"
        prev = prior.get(name)
        if prev and prev.get("hot_s") is not None and not prev.get("error"):
            print(f"{name} SKIP (prior hot={prev['hot_s']:.4f}s)", flush=True)
            results.append(prev)
            continue

        sql = qualify((args.queries / f"q{n}.sql").read_text(), args.glue_database)
        times: list[float | None] = []
        err: str | None = None
        for try_i in range(args.tries):
            ok = False
            for attempt in range(4):
                t0 = time.time()
                try:
                    rows = spark.sql(sql).collect()
                    dt = time.time() - t0
                    times.append(dt)
                    print(
                        f"{name} try{try_i + 1} {dt:.4f}s rows={len(rows)}",
                        flush=True,
                    )
                    err = None
                    ok = True
                    break
                except Exception as e:  # noqa: BLE001 — bench harness
                    dt = time.time() - t0
                    msg = str(e)
                    print(
                        f"{name} try{try_i + 1} err({attempt}) {dt:.4f}s {msg[:240]}",
                        flush=True,
                    )
                    if session_dead(msg):
                        try:
                            spark.stop()
                        except Exception:  # noqa: BLE001
                            pass
                        time.sleep(3)
                        spark = new_spark()
                        print("reconnected", flush=True)
                        continue
                    times.append(None)
                    err = msg
                    break
            if not ok:
                if len(times) == try_i:
                    times.append(None)
                break

        if len(times) < args.tries or any(t is None for t in times):
            failures += 1
            hot = None
        elif args.tries == 1:
            hot = times[0]
            print(f"{name} HOT {hot:.4f}s (single try)", flush=True)
        else:
            # Cold = try1; hot = min of remaining tries.
            hot = min(t for t in times[1:] if t is not None)
            print(f"{name} HOT {hot:.4f}s", flush=True)
        results.append(
            {"query": name, "tries": times, "hot_s": hot, "error": err}
        )
        # Checkpoint after every query so a kill still keeps progress.
        payload = {
            "dataset": f"TPC-H SF100 (Glue {args.glue_database} via Connect)",
            "machine": args.machine,
            "run_date": str(date.today()),
            "endpoint": args.endpoint,
            "failures": failures,
            "queries": results,
            "hot_total_s": sum(
                r["hot_s"] for r in results if r["hot_s"] is not None
            ),
        }
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(payload, indent=2) + "\n")

    try:
        spark.stop()
    except Exception:  # noqa: BLE001
        pass

    hot_total = sum(r["hot_s"] for r in results if r["hot_s"] is not None)
    print(
        f"\n=== DONE failures={failures} hot_total={hot_total:.4f}s → {args.out} ===",
        flush=True,
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
