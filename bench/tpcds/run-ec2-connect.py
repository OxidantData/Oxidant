#!/usr/bin/env python3
"""Run TPC-DS against a remote Oxidant Spark Connect endpoint (EC2 SF100 path).

Qualifies unqualified table names to glue.<db>.<table>, reconnects after
session loss, checkpoints results JSON, and supports --tries (default 1).

Example:
  python3 bench/tpcds/run-ec2-connect.py \\
    --endpoint sc://35.163.191.126:50051 \\
    --glue-database tpcds_sf100 \\
    --tries 1 \\
    --out bench/tpcds/results/tpcds-sf100-ec2.json
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import date
from pathlib import Path

TABLES = [
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
]


def rewrite_bare_intervals(sql: str) -> str:
    """Postgres/TPC-DS `(… date) + N days` → `(… date) + INTERVAL 'N' DAY`.

    Needed until the cluster AMI includes the oxidant-loom normalizer fix. Anchored on a
    closing `)` so aliases like `"31-60 days"` are never rewritten.
    """

    def repl(m: re.Match[str]) -> str:
        sign, amount, unit = m.group(1), m.group(2), m.group(3).lower()
        singular = {
            "days": "DAY",
            "day": "DAY",
            "months": "MONTH",
            "month": "MONTH",
            "years": "YEAR",
            "year": "YEAR",
        }.get(unit)
        if not singular:
            return m.group(0)
        return f"){sign} INTERVAL '{amount}' {singular}"

    return re.sub(
        r"\)\s*([+-])\s+(\d+)\s+(days?|months?|years?)\b",
        repl,
        sql,
        flags=re.IGNORECASE,
    )


def qualify(sql: str, database: str) -> str:
    # Longer names first so customer_address is not partially rewritten via customer.
    body = rewrite_bare_intervals(sql)
    for t in sorted(TABLES, key=len, reverse=True):
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
    ap.add_argument("--glue-database", default="tpcds_sf100")
    ap.add_argument(
        "--queries",
        type=Path,
        default=Path(__file__).resolve().parent / "queries",
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parent / "results" / "tpcds-sf100-ec2.json",
    )
    ap.add_argument("--machine", default="oxidant-sf100")
    ap.add_argument("--start", type=int, default=1)
    ap.add_argument("--end", type=int, default=99)
    ap.add_argument(
        "--tries",
        type=int,
        default=1,
        help="Runs per query (1 = single timing; 3 = cold+hot min(try2,try3))",
    )
    ap.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore prior successful timings in --out",
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
    print(f"connected {args.endpoint} tries={args.tries}", flush=True)

    results = []
    failures = 0
    for n in range(args.start, args.end + 1):
        name = f"Q{n}"
        qpath = args.queries / f"q{n}.sql"
        if not qpath.exists():
            print(f"{name} SKIP (missing {qpath.name})", flush=True)
            continue

        prev = prior.get(name)
        done = prev and prev.get("error") is None and (
            prev.get("hot_s") is not None or prev.get("elapsed_s") is not None
        )
        if done:
            elapsed = prev.get("hot_s", prev.get("elapsed_s"))
            print(f"{name} SKIP (prior {elapsed:.4f}s)", flush=True)
            results.append(prev)
            continue

        sql = qualify(qpath.read_text(), args.glue_database)
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
            elapsed = None
        elif args.tries == 1:
            hot = None
            elapsed = times[0]
            print(f"{name} OK {elapsed:.4f}s", flush=True)
        else:
            # Cold = try1; hot = min of remaining tries.
            hot = min(times[1:])  # type: ignore[type-var]
            elapsed = hot
            print(f"{name} HOT {hot:.4f}s", flush=True)

        results.append(
            {
                "query": name,
                "tries": times,
                "elapsed_s": elapsed,
                "hot_s": hot,
                "error": err,
            }
        )
        payload = {
            "dataset": f"TPC-DS SF100 (Glue {args.glue_database} via EC2 Connect)",
            "machine": args.machine,
            "run_date": str(date.today()),
            "endpoint": args.endpoint,
            "tries": args.tries,
            "failures": failures,
            "queries": results,
            "elapsed_total_s": sum(
                r["elapsed_s"] for r in results if r.get("elapsed_s") is not None
            ),
        }
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(payload, indent=2) + "\n")

    try:
        spark.stop()
    except Exception:  # noqa: BLE001
        pass

    elapsed_total = sum(
        r["elapsed_s"] for r in results if r.get("elapsed_s") is not None
    )
    print(
        f"\n=== DONE failures={failures} elapsed_total={elapsed_total:.4f}s → {args.out} ===",
        flush=True,
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
