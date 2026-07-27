#!/usr/bin/env python3
"""Run TPC-H / TPC-DS against Weft on EKS via the control-plane gateway.

Qualifies bare table names to ``glue.<db>.<table>``, posts each query to
``POST /api/sql`` (optionally pinned to a cluster), uses ClickBench timing
(3 tries, hot = min(try2, try3)), and writes site-shaped JSON.

Examples::

  # Ensure Glue connection exists, create/start a cluster, run TPC-H SF100
  python3 bench/sf100/run-via-gateway.py \\
      --suite tpch --sf 100 --glue-db tpch_sf100 \\
      --create-cluster --worker-size xlarge \\
      --json site/src/data/tpch.json

  python3 bench/sf100/run-via-gateway.py \\
      --suite tpcds --sf 100 --glue-db tpcds_sf100 \\
      --cluster-id <id> --json site/src/data/tpcds.json
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from sf100_common import (  # noqa: E402
    TPCDS_TABLES,
    TPCH_TABLES,
    filter_queries,
    load_queries,
    qualify_sql,
)


def http_json(method: str, url: str, token: str | None, body: dict | None = None, timeout: int = 3600):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "content-type": "application/json",
            **({"authorization": f"Bearer {token}"} if token else {}),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        err = e.read().decode("utf-8", "replace")
        raise RuntimeError(f"{method} {url} → {e.code}: {err}") from e


def admin_password() -> str:
    if pw := os.environ.get("WEFT_ADMIN_PASSWORD"):
        return pw
    out = subprocess.check_output(
        [
            "kubectl",
            "-n",
            "weft-system",
            "get",
            "secret",
            "weft-gateway-jwt",
            "-o",
            "jsonpath={.data.admin-password}",
        ],
        text=True,
    )
    import base64

    return base64.b64decode(out).decode()


def ensure_glue_connection(gw: str, token: str) -> None:
    conns = http_json("GET", f"{gw}/api/connections", token)
    if any(c.get("name") == "glue" for c in conns):
        return
    http_json(
        "POST",
        f"{gw}/api/connections",
        token,
        {
            "name": "glue",
            "kind": "glue",
            "options": {
                "region": os.environ.get("AWS_REGION", "us-west-2"),
                "warehouse": os.environ.get(
                    "WEFT_WAREHOUSE",
                    "s3://weft-artifacts-810738286322/warehouse",
                ),
            },
        },
    )


def create_cluster(
    gw: str,
    token: str,
    name: str,
    worker_size: str,
    *,
    distributed: bool = False,
    worker_count: int = 2,
) -> str:
    if distributed:
        worker_min = worker_max = max(worker_count, 2)
    else:
        worker_min = worker_max = 0
    body = {
        "name": name,
        "worker_min": worker_min,
        "worker_max": worker_max,
        "worker_size": worker_size,
    }
    c = http_json("POST", f"{gw}/api/clusters", token, body)
    cid = c["id"]
    print(f"[run] created cluster {cid} ({name})", flush=True)
    return cid


def wait_cluster(gw: str, token: str, cid: str, timeout_s: int = 900) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        clusters = http_json("GET", f"{gw}/api/clusters", token)
        c = next((x for x in clusters if x["id"] == cid), None)
        if not c:
            raise RuntimeError(f"cluster {cid} disappeared")
        state = c.get("state") or c.get("status") or "?"
        print(f"[run] cluster {cid} state={state}", flush=True)
        if str(state).upper() in {"RUNNING", "READY", "ACTIVE"}:
            return
        if str(state).upper() in {"FAILED", "ERROR", "TERMINATED"}:
            raise RuntimeError(f"cluster entered {state}")
        time.sleep(10)
    raise TimeoutError(f"cluster {cid} not ready in {timeout_s}s")


def run_one(gw: str, token: str, sql: str, cluster_id: str | None) -> tuple[float, str | None]:
    body: dict = {"sql": sql, "no_limit": True}
    if cluster_id:
        body["cluster_id"] = cluster_id
    t0 = time.perf_counter()
    resp = http_json("POST", f"{gw}/api/sql", token, body, timeout=7200)
    wall = time.perf_counter() - t0
    err = resp.get("error")
    if err or str(resp.get("status", "")).upper() not in {"FINISHED", "OK", ""}:
        return wall, err or json.dumps(resp)[:300]
    # Prefer server duration when present and non-zero; else wall clock.
    ms = resp.get("duration_ms") or 0
    return (ms / 1000.0 if ms else wall), None


def wait_driver_ready(cluster_id: str, timeout_s: int = 180) -> None:
    """Block until the cluster driver pod is Ready (post-OOM restart)."""
    if not cluster_id:
        return
    ns = f"weft-cl-{cluster_id}"
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            out = subprocess.check_output(
                [
                    "kubectl", "-n", ns, "get", "pods", "-l", "weft.io/role=driver",
                    "-o", "jsonpath={.items[0].status.conditions[?(@.type==\"Ready\")].status}",
                ],
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
            if out == "True":
                return
        except subprocess.CalledProcessError:
            pass
        time.sleep(5)
    print(f"[run] warning: driver in {ns} not Ready within {timeout_s}s", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gateway", default=os.environ.get("WEFT_GATEWAY", ""))
    ap.add_argument("--suite", choices=["tpch", "tpcds"], required=True)
    ap.add_argument("--sf", type=float, default=100)
    ap.add_argument("--glue-db", default="")
    ap.add_argument("--cluster-id", default="")
    ap.add_argument("--create-cluster", action="store_true")
    ap.add_argument(
        "--distributed",
        action="store_true",
        help="Provision worker_min=worker_max=N (default 2). Without this flag, workers=0 (driver-only).",
    )
    ap.add_argument(
        "--worker-count",
        type=int,
        default=int(os.environ.get("WEFT_WORKER_COUNT", "2")),
        help="Worker replicas when --distributed (min 2).",
    )
    ap.add_argument("--worker-size", default="xlarge")
    ap.add_argument("--cluster-name", default="")
    ap.add_argument("--json", required=True, help="Output site JSON path")
    ap.add_argument("--machine", default="eks/t4g.xlarge")
    ap.add_argument("--only", default="", help="Comma-separated query nums, e.g. 1,3,6")
    args = ap.parse_args()

    gw = args.gateway.rstrip("/")
    if not gw:
        # Default public LB HTTP port (TLS cert is ACM on 443 without matching SAN).
        gw = "http://ae5c0de98b8374a178515b1957b1c6bb-1631147956.us-west-2.elb.amazonaws.com:8080"

    glue_db = args.glue_db or f"{args.suite}_sf{int(args.sf)}"
    tables = TPCH_TABLES if args.suite == "tpch" else TPCDS_TABLES
    queries = filter_queries(load_queries(args.suite), args.only)

    pw = admin_password()
    token = http_json(
        "POST",
        f"{gw}/api/auth/login",
        None,
        {"username": os.environ.get("WEFT_ADMIN_USER", "admin"), "password": pw},
    )["token"]
    ensure_glue_connection(gw, token)

    cluster_id = args.cluster_id or None
    if args.create_cluster:
        name = args.cluster_name or f"bench-{args.suite}-sf{int(args.sf)}"
        cluster_id = create_cluster(
            gw,
            token,
            name,
            args.worker_size,
            distributed=args.distributed,
            worker_count=args.worker_count,
        )
        wait_cluster(gw, token, cluster_id)

    print(
        f"[run] suite={args.suite} sf={args.sf} glue_db={glue_db} "
        f"cluster={cluster_id or 'embedded'} queries={len(queries)}",
        flush=True,
    )

    per_query: list[float | None] = []
    failed: list[int] = []

    def transient(err: str | None) -> bool:
        if not err:
            return False
        e = err.lower()
        return any(s in e for s in ("transport", "unavailable", "connection reset", "broken pipe", "oom"))

    for qi, (name, raw) in enumerate(queries, start=1):
        sql = qualify_sql(raw.strip().rstrip(";").strip(), glue_db, tables)
        times: list[float] = []
        err = None
        attempt = 0
        transient_left = 6
        while attempt < 3:
            wall, err = run_one(gw, token, sql, cluster_id)
            if err and transient(err) and transient_left > 0:
                transient_left -= 1
                print(f"{name:<5} RETRY try{attempt+1}: {err}", flush=True)
                wait_driver_ready(cluster_id)
                time.sleep(5)
                # Do not consume a successful-try slot on transport blips.
                continue
            if err:
                print(f"{name:<5} FAIL try{attempt+1}: {err}", flush=True)
                times.clear()
                break
            times.append(wall)
            print(f"{name:<5} try{attempt+1} {wall:.4f}s", flush=True)
            attempt += 1
        if len(times) < 3:
            per_query.append(None)
            failed.append(qi)
            continue
        hot = min(times[1], times[2])
        per_query.append(hot)
        print(f"{name:<5} HOT  {hot:.4f}s", flush=True)

    total_all = sum(t for t in per_query if t is not None) if any(per_query) else None
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    label = "TPC-H" if args.suite == "tpch" else "TPC-DS"
    doc = {
        "dataset": f"{label} SF{int(args.sf)} via Glue (`glue.{glue_db}.*`) on S3",
        "machine": args.machine,
        "runDate": run_date,
        "queryCount": len(queries),
        "commonCount": sum(1 for t in per_query if t is not None),
        "method": (
            "Weft on EKS via gateway POST /api/sql; Glue catalog; "
            "3 tries/query; hot = min(try2, try3); no_limit"
            + (
                f"; distributed ({args.worker_count} workers)"
                if args.distributed
                else "; driver-only (workers=0)"
            )
        ),
        "engines": [
            {
                "key": "weft",
                "name": "Weft",
                "highlight": True,
                "total": total_all,
                "totalAll": total_all,
                "source": f"measured ({args.machine} {run_date})",
                "perQuery": per_query,
                "failures": len(failed),
                "failedQueries": failed,
            }
        ],
    }
    out = Path(args.json)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=2) + "\n")
    print(f"[run] wrote {out}  hot_total={total_all} failures={len(failed)}", flush=True)
    return 1 if failed and total_all is None else 0


if __name__ == "__main__":
    sys.exit(main())
