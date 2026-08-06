#!/usr/bin/env python3
"""Run TPC-H / TPC-DS against a plain Spark Connect endpoint (sc://host:port).

Talks to the Helm-deployed ``oxidant-connect`` Service with stock
``pyspark-client>=4.0`` (pure Python, no JVM). Records per-query wall time and a
result checksum so Parquet / Iceberg / Delta runs can be compared later.

Examples::

  pip install "pyspark-client>=4.0"

  # Smoke a subset against a port-forwarded connect server
  OXIDANT_DISTRIBUTED_STRICT=1 python3 bench/sf100/run-spark-connect.py \\
      --endpoint sc://localhost:50051 \\
      --suite tpcds --sf 100 --glue-db tpcds_sf100 \\
      --only 1,3,6 --json /tmp/tpcds-sc.jsonl

  # Full resumable sweep
  OXIDANT_DISTRIBUTED_STRICT=1 python3 bench/sf100/run-spark-connect.py \\
      --endpoint sc://$CONNECT_HOST:50051 \\
      --suite tpcds --sf 100 --glue-db tpcds_sf100 \\
      --json results/tpcds-sf100-parquet.jsonl --resume

The server must have ``OXIDANT_DISTRIBUTED_STRICT=1`` (SF100 Helm overlay sets this on
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
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
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
    v = os.environ.get("OXIDANT_DISTRIBUTED_STRICT", "")
    return v == "1" or v.lower() == "true"


def _ready_worker_count(namespace: str) -> tuple[int, int]:
    """Return (ready_count, total_count) for pods labeled app=oxidant-worker."""
    import subprocess

    out = subprocess.check_output(
        [
            "kubectl",
            "-n",
            namespace,
            "get",
            "pods",
            "-l",
            "app=oxidant-worker",
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

    Each worker shards by its own OXIDANT_POD_NAME ordinal over OXIDANT_WORKER_COUNT, while
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
            f"preflight failed: {ready} Ready oxidant-worker pods (total={total}), "
            f"expected {expected} (== OXIDANT_WORKER_COUNT). "
            "A missing worker silently drops its shard. "
            "Wait for StatefulSet Ready, or pass --skip-worker-preflight (unsafe)."
        )


class WorkerGate:
    """Bracket every query with a worker-readiness check.

    The one-shot preflight above only proves the cluster was healthy at t=0. A worker
    that restarts, is evicted, or fails a liveness probe *during* a sweep drops out of
    headless DNS, so the driver plans for fewer endpoints while the survivors still
    shard over the render-time OXIDANT_WORKER_COUNT. Every query after that point returns
    a subset of the data, faster, and reports success — a resumable SF100 sweep is one
    long process, so a single blip can quietly poison the rest of the run.

    Checking before *and* after each query brackets the timed region, so a worker that
    dies and recovers between queries cannot slip through. Both checks happen outside
    the measured window.
    """

    def __init__(self, namespace: str, expected: int, enabled: bool):
        self.namespace = namespace
        self.expected = expected
        self.enabled = enabled and expected > 0

    def observe(self) -> int | None:
        """Ready worker count, or None when gating is disabled/unavailable."""
        if not self.enabled:
            return None
        try:
            ready, _ = _ready_worker_count(self.namespace)
        except Exception as e:  # noqa: BLE001 — kubectl missing/unreachable
            raise SystemExit(
                f"worker gate: cannot read pod readiness ({type(e).__name__}: {e}). "
                "Results would be unverifiable; fix kubectl access or pass "
                "--skip-worker-preflight (unsafe)."
            ) from e
        return ready

    def require(self, query: str, when: str) -> int | None:
        ready = self.observe()
        if ready is not None and ready != self.expected:
            raise SystemExit(
                f"worker gate failed {when} {query}: {ready} Ready oxidant-worker pods, "
                f"expected {self.expected}. Rows from the missing shard are dropped "
                "silently, so every result from here on is void. Restore the "
                "StatefulSet and re-run with --resume (completed queries are kept)."
            )
        return ready


def _canonical_cell(v) -> str:
    """Normalize cell values so Spark vs Oxidant decimals/floats compare as multisets (KAN-8)."""
    if v is None:
        return "NULL"
    # Decimal / DecimalType often stringify with trailing zeros inconsistently.
    try:
        from decimal import Decimal

        if isinstance(v, Decimal):
            return format(v.normalize(), "f")
    except Exception:
        pass
    if isinstance(v, float):
        # Stable short form; avoids -0.0 vs 0.0 and tiny binary noise.
        if v == 0.0:
            return "0"
        return f"{v:.12g}"
    return repr(v)


def _result_checksum(rows: list) -> str:
    """Stable multiset checksum (KAN-8).

    Sort row canonical forms before hashing so ORDER BY-less or tie-broken results still
    match Spark goldens. True order-sensitive diffs remain visible via row_count + wall
    times; correctness vs Spark is multiset equality for these suites.
    """
    lines: list[str] = []
    for row in rows:
        try:
            vals = tuple(row)
        except TypeError:
            vals = (row,)
        lines.append("(" + ", ".join(_canonical_cell(v) for v in vals) + ")")
    lines.sort()
    h = hashlib.sha256()
    for line in lines:
        h.update(line.encode("utf-8", "replace"))
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

    # Oxidant registers catalogs via spark.sql.catalog.<name>.type=glue (docs/catalogs.md).
    builder = (
        SparkSession.builder.remote(endpoint)
        .config(f"spark.sql.catalog.{catalog}.type", "glue")
        .config(f"spark.sql.catalog.{catalog}.region", region)
    )
    if warehouse:
        builder = builder.config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
    return builder.getOrCreate()


# Single-worker executor so `df.collect()` can be bounded by a wall-clock timeout. A
# wedged/deadlocked query (e.g. a driver-side gather that never returns) would otherwise
# block the whole suite forever; on timeout we abandon the future and recreate the session.
_COLLECT_EXEC = ThreadPoolExecutor(max_workers=1)


def _collect_with_timeout(df, timeout: float):
    return _COLLECT_EXEC.submit(df.collect).result(timeout=timeout)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="TPC-H/DS SF100 runner over Spark Connect (sc://host:port)"
    )
    ap.add_argument(
        "--endpoint",
        default=os.environ.get("OXIDANT_CONNECT", "sc://localhost:50051"),
        help="Spark Connect URI (default sc://localhost:50051 or $OXIDANT_CONNECT)",
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
        default=os.environ.get("OXIDANT_WAREHOUSE", ""),
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
        help="Require OXIDANT_DISTRIBUTED_STRICT (also honoured from the env var)",
    )
    ap.add_argument(
        "--tries",
        type=int,
        default=1,
        help="Timed attempts per query (default 1; use 3 for ClickBench-style hot)",
    )
    ap.add_argument(
        "--query-timeout",
        type=float,
        default=float(os.environ.get("OXIDANT_QUERY_TIMEOUT", "300")),
        help="Per-query wall-clock timeout in seconds (default 300 / $OXIDANT_QUERY_TIMEOUT). "
        "A wedged/deadlocked query fails fast and the Spark session is recreated instead of "
        "hanging the whole suite.",
    )
    ap.add_argument("--machine", default="eks/sf100")
    ap.add_argument(
        "--namespace",
        default=os.environ.get("OXIDANT_NAMESPACE", "oxidant"),
        help="K8s namespace for worker preflight (default oxidant / $OXIDANT_NAMESPACE)",
    )
    ap.add_argument(
        "--worker-count",
        type=int,
        default=int(os.environ.get("OXIDANT_WORKER_COUNT", "0") or "0"),
        help="Expected Ready oxidant-worker pods (== chart worker.replicas / OXIDANT_WORKER_COUNT)",
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
            "[run] strict mode: connect pod must have OXIDANT_DISTRIBUTED_STRICT=1 "
            "(values-sf100.yaml sets connect.distributedStrict); fallback is a hard failure",
            flush=True,
        )
    else:
        print(
            "[run] warning: OXIDANT_DISTRIBUTED_STRICT unset — distributed fallback "
            "will quietly run single-node. Export OXIDANT_DISTRIBUTED_STRICT=1 or pass --strict.",
            flush=True,
        )
        if args.sf >= 100:
            raise SystemExit(
                "refusing SF>=100 without OXIDANT_DISTRIBUTED_STRICT=1 or --strict "
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
    gate = WorkerGate(
        args.namespace, expected_workers, not args.skip_worker_preflight
    )

    glue_db = args.glue_db or f"{args.suite}_sf{int(args.sf)}"
    tables = TPCH_TABLES if args.suite == "tpch" else TPCDS_TABLES
    queries = filter_queries(load_queries(args.suite, args.sf), args.only)
    out = Path(args.json)
    done = _load_done(out) if args.resume else set()

    print(
        f"[run] endpoint={args.endpoint} suite={args.suite} sf={args.sf} "
        f"catalog={args.catalog} glue_db={glue_db} queries={len(queries)} "
        f"resume_skip={len(done) if args.resume else 0}",
        flush=True,
    )

    def new_session():
        return _build_spark(
            args.endpoint,
            args.catalog,
            args.region,
            args.warehouse or None,
        )

    spark = new_session()

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

        workers_ready = gate.require(name, "before")
        for attempt in range(1, max(args.tries, 1) + 1):
            t0 = time.perf_counter()
            try:
                df = spark.sql(sql)
                rows = _collect_with_timeout(df, args.query_timeout)
                wall = time.perf_counter() - t0
                checksum = _result_checksum(rows)
                row_count = len(rows)
                times.append(wall)
                print(
                    f"{name:<5} try{attempt} {wall:.4f}s rows={row_count} "
                    f"checksum={checksum[:12]}…",
                    flush=True,
                )
            except FutureTimeoutError:
                wall = time.perf_counter() - t0
                err = (
                    f"TimeoutError: query exceeded {args.query_timeout:.0f}s "
                    f"(possible server-side wedge/deadlock)"
                )
                print(f"{name:<5} FAIL try{attempt} ({wall:.4f}s): {err}", flush=True)
                times.clear()
                # The wedged query may have poisoned the session (and the abandoned
                # collect thread is still blocked server-side) — start a fresh session.
                try:
                    spark.stop()
                except Exception:  # noqa: BLE001
                    pass
                spark = new_session()
                break
            except Exception as e:  # noqa: BLE001 — surface any Connect/engine failure
                wall = time.perf_counter() - t0
                err = f"{type(e).__name__}: {e}"
                # Strict server turns distributed fallback into an error; treat as hard fail.
                print(f"{name:<5} FAIL try{attempt} ({wall:.4f}s): {err}", flush=True)
                if not str(e).strip():
                    import traceback

                    print(traceback.format_exc(), flush=True)
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

        # A success is only publishable if the cluster was whole for the whole query.
        gate.require(name, "after")

        hot = min(times[1], times[2]) if len(times) >= 3 else times[-1]
        rec = {
            "query": name,
            "status": "ok",
            "wall_s": times[-1],
            "hot_s": hot,
            "tries_s": times,
            "row_count": row_count,
            "checksum": checksum,
            "workers_ready": workers_ready,
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
