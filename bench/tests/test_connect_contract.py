#!/usr/bin/env python3
"""Offline CLI contract tests; fixture results are NOT benchmark measurements.

Only the Spark boundary is replaced. Each invocation uses real argparse, SQL
transformation, resume decisions, retry loop, and on-disk checkpoint writes.
Set BENCH_CONTRACT_EVIDENCE to a new directory to retain source generations,
SQL inputs, events, command exits, and unmodified checkpoint snapshots.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

BENCH = Path(__file__).resolve().parents[1]
SOURCES = ("qualify_sql.py", "resume_identity.py", "tpch/run-ec2-connect.py",
           "tpcds/run-ec2-connect.py")

# Synthetic success is used only to produce real checkpoint fixtures. Refusal
# probes record the outgoing SQL BEFORE raising a named offline error.
SPARK_FIXTURE = '''import itertools, json, os, time
from pathlib import Path
clock = itertools.count()
time.time = lambda: next(clock) / 4
time.sleep = lambda seconds: None
calls = {}
def record(kind, **fields):
    with open(os.environ["OFFLINE_EVENTS"], "a") as f:
        f.write(json.dumps({"kind": kind, **fields}) + "\\n")
class OfflineSparkRefusal(RuntimeError): pass
class Result:
    def collect(self):
        record("collect", fixture=True)
        return [("SYNTHETIC CHECKPOINT FIXTURE ONLY",)]
class Session:
    def sql(self, sql):
        record("sql", sql=sql)
        calls[sql] = calls.get(sql, 0) + 1
        mode = os.environ["OFFLINE_MODE"]
        if mode == "refuse" or (mode == "mixed" and "ordinary_error" in sql):
            raise OfflineSparkRefusal("OfflineSparkRefusal: no engine execution")
        if mode == "mixed" and "exhausted_retry" in sql and calls[sql] > 1:
            raise OfflineSparkRefusal("NO_ACTIVE_SESSION OfflineSparkRefusal")
        return Result()
    def stop(self): record("stop")
class Builder:
    def remote(self, endpoint):
        record("remote", endpoint=endpoint)
        return self
    def getOrCreate(self):
        record("session", fixture=True)
        return Session()
class SparkSession:
    builder = Builder()
'''
SOCKET_GUARD = '''import sys
def guard(event, args):
    if event.startswith("socket."):
        raise RuntimeError("OfflineSocketRefusal: " + event)
sys.addaudithook(guard)
'''


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ConnectContractTests(unittest.TestCase):
    def setUp(self):
        retained = os.environ.get("BENCH_CONTRACT_EVIDENCE")
        if retained:
            parent = Path(retained)
            parent.mkdir(parents=True, exist_ok=True)
            self.root = Path(tempfile.mkdtemp(prefix=self._testMethodName + "-", dir=parent))
        else:
            temporary = tempfile.TemporaryDirectory(prefix="connect-contract-")
            self.addCleanup(temporary.cleanup)
            self.root = Path(temporary.name)
        self.serial = 0
        self.stub = self.root / "offline"
        (self.stub / "pyspark").mkdir(parents=True)
        (self.stub / "pyspark/__init__.py").write_text("")
        (self.stub / "pyspark/sql.py").write_text(SPARK_FIXTURE)
        (self.stub / "sitecustomize.py").write_text(SOCKET_GUARD)
        for name in ("home", "tmp"):
            (self.root / name).mkdir()

    def generation(self, name):
        root = self.root / name / "bench"
        for relative in SOURCES:
            source = BENCH / relative
            if source.exists():
                target = root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)
        return root

    def inputs(self, name, entries):
        directory = self.root / name
        directory.mkdir()
        for n, sql in entries.items():
            (directory / f"q{n}.sql").write_text(sql)
        return directory

    def invoke(self, generation, suite, queries, output, *, mode="refuse",
               start=1, end=1, tries=1, extra=()):
        self.serial += 1
        receipt = self.root / f"invoke-{self.serial:03d}"
        receipt.mkdir()
        events = receipt / "events.jsonl"
        argv = [sys.executable, "-B", str(generation / suite / "run-ec2-connect.py"),
                "--endpoint", "sc://offline.invalid:50051", "--glue-database", "fixture_db",
                "--machine", "offline-fixture", "--queries", str(queries), "--out", str(output),
                "--start", str(start), "--end", str(end), "--tries", str(tries), *extra]
        env = dict(os.environ, HOME=str(self.root / "home"), TMPDIR=str(self.root / "tmp"),
                   PYTHONDONTWRITEBYTECODE="1", PYTHONPATH=str(self.stub),
                   OFFLINE_EVENTS=str(events), OFFLINE_MODE=mode)
        if output.exists():
            shutil.copy2(output, receipt / "before.json")
        run = subprocess.run(argv, cwd=self.root, env=env, capture_output=True,
                             text=True, timeout=30)
        (receipt / "stdout.txt").write_text(run.stdout)
        (receipt / "stderr.txt").write_text(run.stderr)
        observed = [json.loads(line) for line in events.read_text().splitlines()] if events.exists() else []
        payload = None
        if output.exists():
            shutil.copy2(output, receipt / "after.json")
            payload = json.loads(output.read_text())
        record = {"argv": argv, "exit": run.returncode, "mode": mode,
                  "fixture_only": True, "sql_calls": sum(e["kind"] == "sql" for e in observed),
                  "session_calls": sum(e["kind"] == "session" for e in observed),
                  "source_sha256": {p: digest((generation / p).read_bytes())
                                    for p in SOURCES if (generation / p).exists()},
                  "output_sha256": digest(output.read_bytes()) if output.exists() else None}
        (receipt / "receipt.json").write_text(json.dumps(record, indent=2) + "\n")
        self.assertEqual(run.stderr, "", run.stderr)
        assert payload is not None, "runner did not leave a checkpoint"
        return run.returncode, payload, run.stdout, [e["sql"] for e in observed if e["kind"] == "sql"]

    def test_real_callers_preserve_qualified_sql_provenance(self):
        generation = self.generation("current")
        for suite in ("tpch", "tpcds"):
            with self.subTest(suite=suite):
                original = ((BENCH / "tpch/queries/q16.sql").read_text() if suite == "tpch" else
                            "SELECT 'store' AS store_sales, (d_date) + 1 days FROM date_dim, store_sales")
                queries = self.inputs(suite, {1: original})
                rc, payload, _, calls = self.invoke(generation, suite, queries, self.root / f"{suite}.json")
                self.assertEqual(rc, 1)
                self.assertEqual(len(calls), 1)
                transformed = calls[0]
                if suite == "tpch":
                    self.assertIn("'%Customer%Complaints%'", transformed)
                    self.assertIn("glue.fixture_db.supplier", transformed)
                else:
                    self.assertIn("'store' AS store_sales", transformed)
                    self.assertIn("INTERVAL '1' DAY", transformed)
                    self.assertIn("glue.fixture_db.store_sales", transformed)
                row = payload["queries"][0]
                self.assertEqual(row.get("original_sha256"), digest(original.encode()))
                self.assertEqual(row.get("sql_sha256"), digest(original.encode()))
                self.assertEqual(row.get("transformed_sha256"), digest(transformed.encode()))
                self.assertIn("OfflineSparkRefusal", row["error"])

    def test_helper_generation_cannot_reuse_stale_transformation(self):
        a = self.generation("generation-a")
        b = self.generation("generation-b")
        helper = b / "qualify_sql.py"
        original_helper = helper.read_text()
        changed_helper = original_helper.replace(
            'f"{catalog}.{database}.{low}"', 'f"{catalog}.changed_{database}.{low}"')
        self.assertNotEqual(changed_helper, original_helper)
        helper.write_text(changed_helper)
        for suite in ("tpch", "tpcds"):
            with self.subTest(suite=suite):
                original_sql = ((BENCH / "tpch/queries/q16.sql").read_text() if suite == "tpch" else
                                "SELECT 'store' AS store_sales, (d_date) + 1 days FROM date_dim, store_sales")
                queries = self.inputs(suite, {1: original_sql, 2: "SELECT 2 AS sibling"})
                output = self.root / f"{suite}.json"
                rc, original, _, _ = self.invoke(a, suite, queries, output, mode="success", end=2)
                self.assertEqual(rc, 0)
                original_bytes = output.read_bytes()
                rc, same, _, calls = self.invoke(a, suite, queries, output)
                self.assertEqual((rc, calls), (0, []))
                self.assertEqual(same["queries"], original["queries"])
                # Copy, never relabel, the genuine original checkpoint for migration.
                output.write_bytes(original_bytes)
                rc, resumed, _, calls = self.invoke(b, suite, queries, output)
                self.assertEqual(len(calls), 1, "changed helper reused stale transformed SQL")
                self.assertEqual(rc, 1)
                self.assertIn("glue.changed_fixture_db.", calls[0])
                self.assertIn("'%Customer%Complaints%'" if suite == "tpch" else "INTERVAL '1' DAY", calls[0])
                self.assertNotEqual(resumed["source_sha"], original["source_sha"])
                self.assertEqual([r["query"] for r in resumed["queries"]], ["Q1"])
                self.assertEqual(resumed["queries"][0]["transformed_sha256"], digest(calls[0].encode()))

    def test_query_hashes_must_match_the_actual_transformation(self):
        generation = self.generation("current")
        for suite in ("tpch", "tpcds"):
            original_sql = ((BENCH / "tpch/queries/q16.sql").read_text() if suite == "tpch" else
                            "SELECT (d_date) + 1 days FROM date_dim")
            queries = self.inputs(suite, {1: original_sql})
            pristine = self.root / f"{suite}-pristine.json"
            rc, original, _, _ = self.invoke(generation, suite, queries, pristine, mode="success")
            self.assertEqual(rc, 0)
            pristine_bytes = pristine.read_bytes()
            for field, value in (("transformed_sha256", "missing"),
                                 ("transformed_sha256", None),
                                 ("transformed_sha256", "wrong"),
                                 ("original_sha256", "wrong"),
                                 ("sql_sha256", "wrong")):
                with self.subTest(suite=suite, field=field, value=value):
                    payload = json.loads(pristine_bytes)
                    if value == "missing":
                        del payload["queries"][0][field]
                    else:
                        payload["queries"][0][field] = value
                    output = self.root / f"{suite}-{field}-{value}.json"
                    output.write_text(json.dumps(payload))
                    rc, result, _, calls = self.invoke(generation, suite, queries, output)
                    self.assertEqual(len(calls), 1, f"reused incompatible {field}={value}")
                    self.assertEqual(rc, 1)
                    self.assertIn("OfflineSparkRefusal", result["queries"][0]["error"])
                    self.assertEqual(result["queries"][0]["transformed_sha256"], digest(calls[0].encode()))
            self.assertEqual(pristine.read_bytes(), pristine_bytes)

    def test_tpcds_qualified_partial_resume_preserves_all_failure_kinds(self):
        generation = self.generation("current")
        for variant in ("skip", "execute"):
            with self.subTest(variant=variant):
                q1 = "SELECT 'store' AS store_sales, (d_date) + 1 days FROM date_dim, store_sales"
                queries = self.inputs(variant, {
                    1: q1, 2: "SELECT 'ordinary_error' FROM customer",
                    3: "SELECT 'exhausted_retry' FROM store", 5: "SELECT 'sibling' FROM item"})
                output = self.root / f"{variant}.json"
                rc, original, _, calls = self.invoke(generation, "tpcds", queries, output,
                                                      mode="mixed", end=5, tries=3)
                self.assertEqual((rc, original["failures"], len(calls)), (1, 3, 12))
                self.assertIsNone(original["queries"][2]["error"])
                self.assertEqual(original["queries"][2]["tries"], [0.25, None])
                self.assertEqual(original["queries"][3]["error"], "missing q4.sql")
                if variant == "execute":
                    (queries / "q1.sql").write_text(q1 + " -- changed raw SQL")
                rc, partial, stdout, calls = self.invoke(generation, "tpcds", queries, output,
                                                        mode="success", tries=3)
                self.assertEqual(len(calls), 0 if variant == "skip" else 3)
                self.assertEqual((rc, partial["failures"]), (1, 3))
                self.assertEqual(partial["queries"][1:], original["queries"][1:])
                self.assertIn("artifact_failures=3", stdout)
                self.assertIn("selected_elapsed_total=0.2500s", stdout)
                self.assertEqual(partial["elapsed_total_s"], 0.5)
                if variant == "execute":
                    self.assertIn("INTERVAL '1' DAY", calls[0])
                    self.assertIn("'store' AS store_sales", calls[0])
                for number, remaining in ((2, 2), (3, 1), (4, 0)):
                    if number == 4:
                        (queries / "q4.sql").write_text("SELECT 'repaired missing' FROM date_dim")
                    rc, repaired, stdout, calls = self.invoke(generation, "tpcds", queries, output,
                                                            mode="success", start=number,
                                                            end=number, tries=3)
                    self.assertEqual(len(calls), 3)
                    self.assertEqual((rc, repaired["failures"]), (1 if remaining else 0, remaining))
                    self.assertEqual(repaired["queries"][4], original["queries"][4])
                    self.assertIn(f"artifact_failures={remaining}", stdout)
                rc, all_reused, _, calls = self.invoke(generation, "tpcds", queries, output,
                                                       end=5, tries=3)
                self.assertEqual((rc, calls), (0, []))
                self.assertEqual(all_reused["queries"], repaired["queries"])
                self.assertEqual(all_reused["elapsed_total_s"], 1.25)

    def test_each_run_identity_boundary_and_raw_sql_edit_refuses_reuse(self):
        generation = self.generation("current")
        for suite in ("tpch", "tpcds"):
            table = "supplier" if suite == "tpch" else "store"
            queries = self.inputs(suite, {1: f"SELECT 'customer' AS customer FROM {table}",
                                         2: "SELECT 'healthy sibling'"})
            pristine = self.root / f"{suite}-pristine.json"
            rc, original, _, _ = self.invoke(generation, suite, queries, pristine, mode="success", end=2)
            self.assertEqual(rc, 0)
            before = pristine.read_bytes()
            cases = {
                "endpoint": ("--endpoint", "sc://other-offline.invalid:50051"),
                "database": ("--glue-database", "other_fixture"),
                "machine": ("--machine", "other-fixture"),
                "tries": ("--tries", "3"),
                "no-resume": ("--no-resume",),
            }
            for label, flags in cases.items():
                with self.subTest(suite=suite, field=label):
                    output = self.root / f"{suite}-{label}.json"
                    output.write_bytes(before)
                    rc, result, _, calls = self.invoke(generation, suite, queries, output, extra=flags)
                    self.assertEqual((rc, len(calls)), (1, 1))
                    self.assertEqual([r["query"] for r in result["queries"]], ["Q1"])
                    self.assertIn("OfflineSparkRefusal", result["queries"][0]["error"])
            output = self.root / f"{suite}-sql-change.json"
            output.write_bytes(before)
            (queries / "q1.sql").write_text(f"SELECT 'changed raw SQL' FROM {table}")
            rc, result, _, calls = self.invoke(generation, suite, queries, output)
            self.assertEqual((rc, len(calls)), (1, 1))
            self.assertIn("'changed raw SQL'", calls[0])
            self.assertEqual(pristine.read_bytes(), before)

    def test_caller_and_resume_helper_source_changes_invalidate_the_envelope(self):
        a = self.generation("original")
        for suite in ("tpch", "tpcds"):
            queries = self.inputs(suite, {1: "SELECT 1 AS unchanged_sql", 2: "SELECT 2 AS sibling"})
            pristine = self.root / f"{suite}-pristine.json"
            rc, original, _, _ = self.invoke(a, suite, queries, pristine, mode="success", end=2)
            self.assertEqual(rc, 0)
            for relative in (f"{suite}/run-ec2-connect.py", "resume_identity.py"):
                with self.subTest(suite=suite, relative=relative):
                    b = self.generation(suite + "-" + relative.replace("/", "-"))
                    changed = b / relative
                    changed.write_bytes(changed.read_bytes() + b"\n# offline generation identity fixture\n")
                    output = self.root / f"{suite}-{changed.name}.json"
                    output.write_bytes(pristine.read_bytes())
                    rc, result, _, calls = self.invoke(b, suite, queries, output)
                    self.assertEqual((rc, len(calls)), (1, 1))
                    self.assertNotEqual(result["source_sha"], original["source_sha"])
                    self.assertEqual([r["query"] for r in result["queries"]], ["Q1"])

    def test_noop_compatibility_rows_are_not_relabelled(self):
        generation = self.generation("current")
        for suite in ("tpch", "tpcds"):
            queries = self.inputs(suite, {1: "SELECT 1 AS no_transform"})
            output = self.root / f"{suite}.json"
            rc, original, _, _ = self.invoke(generation, suite, queries, output, mode="success")
            self.assertEqual(rc, 0)
            # Explicit compatibility projection of a current-source fixture;
            # this is NOT a migration or relabelling of a retired artifact.
            row = original["queries"][0]
            del row["original_sha256"]
            del row["transformed_sha256"]
            output.write_text(json.dumps(original))
            rc, reused, _, calls = self.invoke(generation, suite, queries, output)
            self.assertEqual((rc, calls), (0, []))
            self.assertEqual(reused["queries"], [row])
            row["transformed_sha256"] = None
            output.write_text(json.dumps(original))
            rc, _, _, calls = self.invoke(generation, suite, queries, output)
            self.assertEqual((rc, len(calls)), (1, 1))

    def test_bare_intervals_remain_ds_only_through_real_callers(self):
        generation = self.generation("current")
        sql = "SELECT (d_date) + 1 days FROM customer -- customer\n"
        for suite in ("tpch", "tpcds"):
            queries = self.inputs(suite, {1: sql})
            rc, _, _, calls = self.invoke(generation, suite, queries, self.root / f"{suite}.json")
            self.assertEqual((rc, len(calls)), (1, 1))
            self.assertEqual("INTERVAL '1' DAY" in calls[0], suite == "tpcds")
            self.assertIn("-- customer", calls[0])
            self.assertIn("FROM glue.fixture_db.customer", calls[0])


if __name__ == "__main__":
    unittest.main()
