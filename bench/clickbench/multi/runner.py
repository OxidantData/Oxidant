#!/usr/bin/env python3
"""Engine-agnostic ClickBench runner — drives ANY Spark Connect endpoint.

Every engine in this harness (Oxidant, Sail, Apache Spark, Spark+Gluten/Velox) speaks the
**Spark Connect** protocol, so one stock PySpark client drives all four. That is the whole
point: identical SQL text, identical dataset, identical client, identical machine — the only
variable is the engine behind `sc://`.

Flow:
  1. connect to --remote (sc://host:port)
  2. run the engine-specific registration SQL from --register-file (creates the `hits` view)
  3. run each of the 43 queries --tries times; time only the analytical query, not registration
  4. write a ClickBench-format JSON: result = [[t1, t2, t3], ...]; hot = min(t2, t3)

Failures are recorded as `null` for that try (never silently dropped) plus an `errors` map, so
the site can show a gap rather than pretend the query ran.
"""
import argparse
import json
import sys
import time
import traceback


def load_statements(path):
    """Split a .sql file into statements on `;` at end-of-line (handles the giant Q30)."""
    raw = open(path, "r", encoding="utf-8").read()
    stmts = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("--"):
            continue
        stmts.append(line.rstrip(";").strip())
    return [s for s in stmts if s]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--remote", required=True, help="sc://host:port")
    ap.add_argument("--queries", required=True, help="queries.spark.sql (or queries.oxidant.sql)")
    ap.add_argument("--register-mode", default="sql", choices=["sql", "dataframe"],
                    help="sql: run --register-file DDL; dataframe: read.parquet + typed casts")
    ap.add_argument("--register-file", default=None, help="engine-specific DDL (register-mode=sql)")
    ap.add_argument("--data", default=None, help="parquet path (register-mode=dataframe)")
    ap.add_argument("--out", required=True, help="output results JSON path")
    ap.add_argument("--engine", required=True, help="engine label, e.g. oxidant / sail / spark / gluten")
    ap.add_argument("--data-size", type=int, default=14779976446)
    ap.add_argument("--machine", default="c6a.4xlarge")
    ap.add_argument("--tags", default="")
    ap.add_argument("--tries", type=int, default=3)
    args = ap.parse_args()

    from pyspark.sql import SparkSession

    print(f"[runner:{args.engine}] connecting to {args.remote} …", flush=True)
    spark = SparkSession.builder.remote(args.remote).getOrCreate()

    # ---- registration (engine-specific; not timed) --------------------------------------
    # The `hits` table must expose EventTime as a TIMESTAMP and EventDate as a DATE: in the raw
    # parquet they are int64 epoch-seconds and int days-since-epoch, so bare date functions
    # (extract/date_trunc, Q18/Q42) and date-string filters (Q37-43) fail without the cast. The
    # same treatment is applied to every engine, so the comparison stays apples-to-apples.
    load_start = time.monotonic()
    if args.register_mode == "dataframe":
        from pyspark.sql import functions as F
        df = spark.read.parquet(args.data)
        cols = set(df.columns)
        if "EventTime" in cols:
            df = df.withColumn("EventTime", F.timestamp_seconds(F.col("EventTime").cast("long")))
        if "EventDate" in cols:
            df = df.withColumn(
                "EventDate", F.expr("date_add(date'1970-01-01', cast(EventDate as int))"))
        df.createOrReplaceTempView("hits")
        print(f"[runner:{args.engine}] registered `hits` via DataFrame API", flush=True)
    else:
        for stmt in load_statements(args.register_file):
            print(f"[runner:{args.engine}] register: {stmt[:90]}…", flush=True)
            spark.sql(stmt).collect()
    load_time = time.monotonic() - load_start
    print(f"[runner:{args.engine}] registered `hits` in {load_time:.2f}s", flush=True)

    # ---- 43 queries × N tries ------------------------------------------------------------
    queries = load_statements(args.queries)
    result = []
    errors = {}
    hot_total = 0.0
    for i, q in enumerate(queries):
        tries = []
        for t in range(args.tries):
            try:
                start = time.monotonic()
                # .collect() forces full execution; all result sets here are small (COUNT /
                # LIMIT 10/25), so client transfer is negligible and identical across engines.
                spark.sql(q).collect()
                tries.append(round(time.monotonic() - start, 6))
            except Exception as e:  # noqa: BLE001 — record, don't crash the whole run
                tries.append(None)
                errors.setdefault(str(i), str(e).splitlines()[0][:300])
        result.append(tries)
        hots = [x for x in tries[1:] if x is not None]
        mark = f"{min(hots):.3f}s" if hots else "FAILED"
        if hots:
            hot_total += min(hots)
        print(f"[runner:{args.engine}] Q{i:02d}  {mark}", flush=True)

    out = {
        "system": args.engine,
        "date": time.strftime("%Y-%m-%d"),
        "machine": args.machine,
        "cluster_size": 1,
        "proprietary": "no",
        "hardware": "cpu",
        "tuned": "no",
        "tags": [t for t in args.tags.split(",") if t],
        "load_time": round(load_time, 3),
        "data_size": args.data_size,
        "result": result,
        "errors": errors,
        "hot_total": round(hot_total, 3),
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    ok = sum(1 for r in result if any(x is not None for x in r[1:]))
    print(f"\n[runner:{args.engine}] {ok}/{len(queries)} queries ok — hot total "
          f"{hot_total:.2f}s — wrote {args.out}", flush=True)
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
