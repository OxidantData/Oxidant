#!/usr/bin/env python3
"""Run TPC-H / TPC-DS on EMR (Spark on YARN) over the oxidant SF10 parquet bytes in S3.

Apples-to-apples counterpart of bench/sf100/run-spark-connect.py (which drives oxidant):
same query text, same SF10 data files, same per-query wall-clock methodology. Tables
are exposed as Spark temp views over s3://<bucket>/{tpch,tpcds}-sf10/<table>/ — the
Glue DBs are oxidant-specific schema-less registrations (0-column StorageDescriptors),
so Spark infers schema from the parquet footers. Same bytes, same queries.

On the EMR master (after scp'ing this file + the query dirs up):

  spark-submit --master yarn --deploy-mode client run-emr-suite.py \
      --suite tpcds --queries-dir queries/tpcds --runs 2 --out /tmp/tpcds-sf10-emr.jsonl

Each query runs `--runs` times; run 1 is the cold number, the best of the rest is hot
(matches the oxidant harness's cold/hot semantics). JSONL schema mirrors
run-spark-connect.py so the site tooling can merge both engines.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import datetime, timezone
from pathlib import Path

TPCH_TABLES = [
    "nation",
    "region",
    "supplier",
    "customer",
    "part",
    "partsupp",
    "orders",
    "lineitem",
]
TPCDS_TABLES = [
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


def substitute_sf(sql: str, sf: float) -> str:
    return sql.replace("__OXIDANT_SF__", f"{sf:g}")


# Standard-SQL interval field precision (TPC-H Q1's `interval '90' day (3)`) is a
# parse error on Spark — oxidant/DataFusion accept it. Dropping the precision annotation
# is semantics-preserving ('90' fits DAY(3)); documented here so the comparison stays
# honest: this is a Spark dialect gap, not a query change.
_INTERVAL_PRECISION = re.compile(r"(interval\s+'[^']*'\s+\w+)\s*\(\s*\d+\s*\)", re.I)

# EMR's Spark build (3.5.6-amzn-2) rejects double-quoted identifiers outside ANSI
# mode (verified empirically: only ansi.enabled=true + ansi.doubleQuotedIdentifiers=true
# parses `AS "x"`). Rather than change engine arithmetic semantics with full ANSI mode,
# rewrite the alias to Spark's native backtick quoting — identical identifier, zero
# behavioral change. Verified: every `"` in the bench queries is an `AS "..."` alias.
_DQ_ALIAS = re.compile(r'AS\s+"([^"]+)"', re.I)


def normalize_for_spark(sql: str) -> str:
    sql = _INTERVAL_PRECISION.sub(r"\1", sql)
    return _DQ_ALIAS.sub(r"AS `\1`", sql)


def load_queries(qdir: Path, sf: float, only: str) -> list[tuple[str, str]]:
    files = sorted(qdir.glob("q*.sql"), key=lambda p: int(p.stem[1:]))
    if not files:
        raise SystemExit(f"no queries in {qdir}")
    queries = [
        (f"Q{p.stem[1:]}", substitute_sf(p.read_text(), sf)) for p in files
    ]
    if only:
        want = {f"Q{x.strip()}" for x in only.split(",") if x.strip()}
        queries = [(n, s) for n, s in queries if n in want]
    return queries


# Single-worker executor so collect() can be bounded by a wall-clock timeout,
# mirroring the oxidant harness (a wedged query fails fast instead of hanging the suite).
_EXEC = ThreadPoolExecutor(max_workers=1)


def main() -> int:
    ap = argparse.ArgumentParser(description="TPC-H/DS SF10 runner for Spark on EMR/YARN")
    ap.add_argument("--suite", choices=["tpch", "tpcds"], required=True)
    ap.add_argument("--sf", type=float, default=10)
    ap.add_argument("--bucket", default="oxidant-artifacts-810738286322")
    ap.add_argument("--prefix", default="", help="S3 prefix (default <suite>-sf<sf>)")
    ap.add_argument("--queries-dir", required=True)
    ap.add_argument("--runs", type=int, default=2, help="runs per query (1st=cold)")
    ap.add_argument("--only", default="", help="comma-separated query nums")
    ap.add_argument(
        "--query-timeout",
        type=float,
        default=900,
        help="per-query wall-clock timeout in seconds (default 900, matches oxidant runs)",
    )
    ap.add_argument("--out", required=True, help="JSONL output path")
    args = ap.parse_args()

    from pyspark.sql import SparkSession

    prefix = args.prefix or f"{args.suite}-sf{int(args.sf)}"
    tables = TPCH_TABLES if args.suite == "tpch" else TPCDS_TABLES

    def new_session():
        s = (
            SparkSession.builder.appName(
                f"oxidant-compare-{args.suite}-sf{int(args.sf)}"
            ).getOrCreate()
        )
        for t in tables:
            s.read.parquet(f"s3://{args.bucket}/{prefix}/{t}/").createOrReplaceTempView(t)
        return s

    spark = new_session()
    spark_version = spark.version
    machine = (
        f"emr/yarn (spark {spark_version}) 1x c6g.2xlarge master + 2x m8g.2xlarge core"
    )
    queries = load_queries(Path(args.queries_dir), args.sf, args.only)
    out = Path(args.out)
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    print(
        f"[run] suite={args.suite} sf={args.sf} spark={spark_version} "
        f"queries={len(queries)} runs={args.runs} prefix=s3://{args.bucket}/{prefix}/",
        flush=True,
    )

    failed: list[str] = []
    for name, raw in queries:
        sql = normalize_for_spark(raw.strip().rstrip(";").strip())
        times: list[float] = []
        err: str | None = None
        row_count = None
        spark.sparkContext.setJobGroup(name, f"{args.suite} {name}")

        for attempt in range(1, max(args.runs, 1) + 1):
            t0 = time.perf_counter()
            try:
                rows = _EXEC.submit(spark.sql(sql).collect).result(
                    timeout=args.query_timeout
                )
                wall = time.perf_counter() - t0
                row_count = len(rows)
                times.append(wall)
                print(f"{name:<5} run{attempt} {wall:.4f}s rows={row_count}", flush=True)
            except FutureTimeoutError:
                err = (
                    f"TimeoutError: query exceeded {args.query_timeout:.0f}s "
                    f"(cancelled job group, recreated session)"
                )
                print(f"{name:<5} FAIL run{attempt}: {err}", flush=True)
                times.clear()
                try:
                    spark.sparkContext.cancelJobGroup(name)
                    spark.stop()
                except Exception:  # noqa: BLE001
                    pass
                spark = new_session()
                break
            except Exception as e:  # noqa: BLE001 — record any engine failure verbatim
                err = f"{type(e).__name__}: {e}"
                print(f"{name:<5} FAIL run{attempt}: {err}", flush=True)
                times.clear()
                break

        if err or not times:
            rec = {
                "query": name,
                "status": "fail",
                "error": err or "no successful runs",
                "suite": args.suite,
                "sf": args.sf,
                "machine": machine,
                "runDate": run_date,
            }
            with out.open("a", encoding="utf-8") as f:
                f.write(json.dumps(rec, sort_keys=True) + "\n")
            failed.append(name)
            continue

        hot = min(times[1:]) if len(times) > 1 else times[0]
        rec = {
            "query": name,
            "status": "ok",
            "wall_s": times[0],
            "hot_s": hot,
            "tries_s": times,
            "row_count": row_count,
            "suite": args.suite,
            "sf": args.sf,
            "glue_db": f"s3://{args.bucket}/{prefix}/ (temp views)",
            "catalog": "emr-tempview",
            "endpoint": "yarn",
            "machine": machine,
            "runDate": run_date,
            "strict": False,
        }
        with out.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, sort_keys=True) + "\n")
        print(f"{name:<5} HOT  {hot:.4f}s", flush=True)

    print(
        f"[run] wrote {out}  ok={len(queries) - len(failed)} failed={len(failed)}",
        flush=True,
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
