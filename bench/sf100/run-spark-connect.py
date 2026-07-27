#!/usr/bin/env python3
"""Run TPC-H / TPC-DS against a plain Spark Connect endpoint (sc://host:port).

Talks to the Helm-deployed ``weft-connect`` Service with stock
``pyspark-client>=4.0`` (pure Python, no JVM). Records per-query wall time and a
result checksum so Parquet / Iceberg / Delta runs can be compared later.

Examples::

  pip install "pyspark-client>=4.0"

  # Smoke a subset against a port-forwarded connect server
  WEFT_DISTRIBUTED_STRICT=1 python3 bench/sf100/run-spark-connect.py \\
      --endpoint sc://localhost:50051 \\
      --suite tpcds --sf 100 --glue-db tpcds_sf100 \\
      --only 1,3,6 --json /tmp/tpcds-sc.jsonl

  # Full resumable sweep
  WEFT_DISTRIBUTED_STRICT=1 python3 bench/sf100/run-spark-connect.py \\
      --endpoint sc://$CONNECT_HOST:50051 \\
      --suite tpcds --sf 100 --glue-db tpcds_sf100 \\
      --json results/tpcds-sf100-parquet.jsonl --resume

The server must have ``WEFT_DISTRIBUTED_STRICT=1`` (SF100 Helm overlay sets this on
the connect pod). When the client env / ``--strict`` flag is set, this runner
refuses to start without it and treats distributed-fallback errors as hard fails.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Allow `python3 bench/sf100/run-spark-connect.py` without installing a package.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from sf100_common import (  # noqa: E402
    TPCDS_TABLES,
    TPCH_TABLES,
    filter_queries,
    load_queries,
    qualify_sql,
)


def _strict_requested(flag: bool) -> bool:
    if flag:
        return True
    v = os.environ.get("WEFT_DISTRIBUTED_STRICT", "")
    return v == "1" or v.lower() == "true"


def _ready_worker_count(namespace: str) -> tuple[int, int]:
    """Return (ready_count, total_count) for pods labeled app=weft-worker."""
    import subprocess

    out = subprocess.check_output(
        [
            "kubectl",
            "-n",
            namespace,
            "get",
            "pods",
            "-l",
            "app=weft-worker",
            "-o",
            "json",
        ],
        text=True,
    )
    data = json.loads(out)
    items = data.get("items") or []
    ready = 0
    for pod in items:
        conds = (pod.get("status") or {}).get("conditions") or []
        if any(c.get("type") == "Ready" and c.get("status") == "True" for c in conds):
            ready += 1
    return ready, len(items)


def assert_workers_ready(namespace: str, expected: int) -> None:
    """Refuse to run unless Ready worker pods == expected shard count.

    Each worker shards by its own WEFT_POD_NAME ordinal over WEFT_WORKER_COUNT, while
    the driver discovers live endpoints via DNS. If a worker is not Ready, DNS omits
    it and the remaining workers still only read their own shard → silent row loss.
    """
    ready, total = _ready_worker_count(namespace)
    print(
        f"[preflight] namespace={namespace} workers ready={ready}/{total} "
        f"expected={expected}",
        flush=True,
    )
    if ready != expected:
        raise SystemExit(
            f"preflight failed: {ready} Ready weft-worker pods (total={total}), "
            f"expected {expected} (== WEFT_WORKER_COUNT). "
            "A missing worker silently drops its shard. "
            "Wait for StatefulSet Ready, or pass --skip-worker-preflight (unsafe)."
        )


def _result_checksum(rows: list) -> str:
    """Stable checksum over collected rows (order-preserving within the result)."""
    h = hashlib.sha256()
    for row in rows:
        # Row is typically a pyspark Row; fall back to tuple/repr.
        try:
            vals = tuple(row)
        except TypeError:
            vals = (row,)
        h.update(repr(vals).encode("utf-8", "replace"))
        h.update(b"\n")
    return h.hexdigest()


def _load_done(path: Path) -> set[str]:
    done: set[str] = set()
    if not path.exists():
        return done
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if rec.get("status") == "ok" and rec.get("query"):
            done.add(rec["query"])
    return done


def _append_jsonl(path: Path, rec: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, sort_keys=True) + "\n")


def _build_spark(endpoint: str, catalog: str, region: str, warehouse: str | None):
    try:
        from pyspark.sql import SparkSession
    except ImportError as e:
        raise SystemExit(
            'pyspark-client is required: pip install "pyspark-client>=4.0"'
        ) from e

    # Weft registers catalogs via spark.sql.catalog.<name>.type=glue (docs/catalogs.md).
    builder = (
        SparkSession.builder.remote(endpoint)
        .config(f"spark.sql.catalog.{catalog}.type", "glue")
        .config(f"spark.sql.catalog.{catalog}.region", region)
    )
    if warehouse:
        builder = builder.config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
    return builder.getOrCreate()


def main() -> int:
    ap = argparse.ArgumentParser(
        description="TPC-H/DS SF100 runner over Spark Connect (sc://host:port)"
    )
    ap.add_argument(
        "--endpoint",
        default=os.environ.get("WEFT_CONNECT", "sc://localhost:50051"),
        help="Spark Connect URI (default sc://localhost:50051 or $WEFT_CONNECT)",
    )
    ap.add_argument("--suite", choices=["tpch", "tpcds"], required=True)
    ap.add_argument("--sf", type=float, default=100)
    ap.add_argument("--glue-db", default="", help="Glue database (default <suite>_sf<N>)")
    ap.add_argument(
        "--catalog",
        default="glue",
        help="Spark catalog name registered as type=glue (default glue)",
    )
    ap.add_argument(
        "--region",
        default=os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-west-2")),
    )
    ap.add_argument(
        "--warehouse",
        default=os.environ.get("WEFT_WAREHOUSE", ""),
        help="Optional glue catalog warehouse URI",
    )
    ap.add_argument(
        "--json",
        required=True,
        help="JSONL output path (one record per query; appendable with --resume)",
    )
    ap.add_argument("--only", default="", help="Comma-separated query nums, e.g. 1,3,6")
    ap.add_argument(
        "--resume",
        action="store_true",
        help="Skip queries that already have status=ok in the JSONL file",
    )
    ap.add_argument(
        "--strict",
        action="store_true",
        help="Require WEFT_DISTRIBUTED_STRICT (also honoured from the env var)",
    )
    ap.add_argument(
        "--tries",
        type=int,
        default=1,
        help="Timed attempts per query (default 1; use 3 for ClickBench-style hot)",
    )
    ap.add_argument("--machine", default="eks/sf100")
    ap.add_argument(
        "--namespace",
        default=os.environ.get("WEFT_NAMESPACE", "weft"),
        help="K8s namespace for worker preflight (default weft / $WEFT_NAMESPACE)",
    )
    ap.add_argument(
        "--worker-count",
        type=int,
        default=int(os.environ.get("WEFT_WORKER_COUNT", "0") or "0"),
        help="Expected Ready weft-worker pods (== chart worker.replicas / WEFT_WORKER_COUNT)",
    )
    ap.add_argument(
        "--skip-worker-preflight",
        action="store_true",
        help="Skip Ready-worker count check (unsafe: missing workers drop shards silently)",
    )
    args = ap.parse_args()

    strict = _strict_requested(args.strict)
    if strict:
        print(
            "[run] strict mode: connect pod must have WEFT_DISTRIBUTED_STRICT=1 "
            "(values-sf100.yaml sets connect.distributedStrict); fallback is a hard failure",
            flush=True,
        )
    else:
        print(
            "[run] warning: WEFT_DISTRIBUTED_STRICT unset — distributed fallback "
            "will quietly run single-node. Export WEFT_DISTRIBUTED_STRICT=1 or pass --strict.",
            flush=True,
        )
        if args.sf >= 100:
            raise SystemExit(
                "refusing SF>=100 without WEFT_DISTRIBUTED_STRICT=1 or --strict "
                "(silent single-node fallback is not publishable)"
            )

    expected_workers = args.worker_count
    if expected_workers <= 0 and not args.skip_worker_preflight:
        # Default SF100 topology is 2 workers when unset.
        expected_workers = 2 if args.sf >= 100 else 0
    if not args.skip_worker_preflight and expected_workers > 0:
        assert_workers_ready(args.namespace, expected_workers)
    elif args.skip_worker_preflight:
        print("[preflight] SKIPPED (--skip-worker-preflight)", flush=True)

    glue_db = args.glue_db or f"{args.suite}_sf{int(args.sf)}"
    tables = TPCH_TABLES if args.suite == "tpch" else TPCDS_TABLES
    queries = filter_queries(load_queries(args.suite), args.only)
    out = Path(args.json)
    done = _load_done(out) if args.resume else set()

    print(
        f"[run] endpoint={args.endpoint} suite={args.suite} sf={args.sf} "
        f"catalog={args.catalog} glue_db={glue_db} queries={len(queries)} "
        f"resume_skip={len(done) if args.resume else 0}",
        flush=True,
    )

    spark = _build_spark(
        args.endpoint,
        args.catalog,
        args.region,
        args.warehouse or None,
    )

    failed: list[str] = []
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for name, raw in queries:
        if name in done:
            print(f"{name:<5} SKIP (resume)", flush=True)
            continue

        sql = qualify_sql(
            raw.strip().rstrip(";").strip(), glue_db, tables, catalog=args.catalog
        )
        times: list[float] = []
        checksum = None
        err: str | None = None
        row_count = None

        for attempt in range(1, max(args.tries, 1) + 1):
            t0 = time.perf_counter()
            try:
                df = spark.sql(sql)
                rows = df.collect()
                wall = time.perf_counter() - t0
                checksum = _result_checksum(rows)
                row_count = len(rows)
                times.append(wall)
                print(
                    f"{name:<5} try{attempt} {wall:.4f}s rows={row_count} "
                    f"checksum={checksum[:12]}…",
                    flush=True,
                )
            except Exception as e:  # noqa: BLE001 — surface any Connect/engine failure
                wall = time.perf_counter() - t0
                err = f"{type(e).__name__}: {e}"
                # Strict server turns distributed fallback into an error; treat as hard fail.
                print(f"{name:<5} FAIL try{attempt} ({wall:.4f}s): {err}", flush=True)
                times.clear()
                break

        if err or not times:
            rec = {
                "query": name,
                "status": "fail",
                "error": err or "no successful tries",
                "suite": args.suite,
                "sf": args.sf,
                "glue_db": glue_db,
                "catalog": args.catalog,
                "endpoint": args.endpoint,
                "machine": args.machine,
                "runDate": run_date,
                "strict": strict or _strict_requested(False),
            }
            _append_jsonl(out, rec)
            failed.append(name)
            continue

        hot = min(times[1], times[2]) if len(times) >= 3 else times[-1]
        rec = {
            "query": name,
            "status": "ok",
            "wall_s": times[-1],
            "hot_s": hot,
            "tries_s": times,
            "row_count": row_count,
            "checksum": checksum,
            "suite": args.suite,
            "sf": args.sf,
            "glue_db": glue_db,
            "catalog": args.catalog,
            "endpoint": args.endpoint,
            "machine": args.machine,
            "runDate": run_date,
            "strict": strict or _strict_requested(False),
        }
        _append_jsonl(out, rec)
        print(f"{name:<5} HOT  {hot:.4f}s", flush=True)

    print(
        f"[run] wrote {out}  ok={len(queries) - len(failed) - (len(done) if args.resume else 0)} "
        f"failed={len(failed)} skipped={len(done) if args.resume else 0}",
        flush=True,
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
