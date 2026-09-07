#!/usr/bin/env python3
"""Resume must bind query timings to run identity (OxidantData/Oxidant#188)."""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import types
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


class _Result:
    def collect(self):
        return []


class Session:
    queries: list[str] = []

    def sql(self, sql):
        self.queries.append(sql)
        return _Result()

    def stop(self):
        pass


class Builder:
    def remote(self, endpoint):
        return self

    def getOrCreate(self):
        return Session()


def _install_pyspark():
    pyspark = types.ModuleType("pyspark")
    sql_module = types.ModuleType("pyspark.sql")
    sql_module.SparkSession = types.SimpleNamespace(builder=Builder())
    sys.modules["pyspark"] = pyspark
    sys.modules["pyspark.sql"] = sql_module


def _load_runner(suite: str):
    root = Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location(
        "runner_" + suite, root / f"bench/{suite}/run-ec2-connect.py"
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _seed(output: Path) -> None:
    output.write_text(
        json.dumps(
            {
                "source_sha": "old-source-fixture",
                "dataset": "old-data-fixture",
                "endpoint": "sc://127.0.0.1:2",
                "tries": 1,
                "machine": "old-machine",
                "queries": [
                    {
                        "query": "Q1",
                        "hot_s": 1.0,
                        "elapsed_s": 1.0,
                        "tries": [1.0],
                        "error": None,
                    }
                ],
            }
        )
    )


def _invoke_tpcds(module, queries: Path, output: Path, *, end: int, tries: int = 1):
    """Exercise real argparse/SQL handling/JSON writes; Spark is in-process only."""
    argv = [
        "runner", "--endpoint", "sc://fixture.invalid:50051",
        "--glue-database", "fixture_db", "--machine", "fixture-machine",
        "--tries", str(tries), "--start", "1", "--end", str(end),
        "--queries", str(queries), "--out", str(output),
    ]
    Session.queries = []
    stdout = io.StringIO()
    with patch.object(sys, "argv", argv), redirect_stdout(stdout):
        rc = module.main()
    return rc, json.loads(output.read_text()), stdout.getvalue()


class ResumeIdentityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_pyspark()

    def test_changed_identity_does_not_reuse_q1(self):
        for suite in ("tpch", "tpcds"):
            with self.subTest(suite=suite):
                module = _load_runner(suite)
                with TemporaryDirectory(prefix="benchmark-identity-") as directory:
                    tmp = Path(directory)
                    queries = tmp / "queries"
                    queries.mkdir()
                    (queries / "q1.sql").write_text("SELECT 2 AS changed_query")
                    output = tmp / "result.json"
                    _seed(output)
                    argv = [
                        "runner",
                        "--endpoint",
                        "sc://127.0.0.1:1",
                        "--glue-database",
                        "changed_database",
                        "--machine",
                        "new-machine",
                        "--tries",
                        "3",
                        "--start",
                        "1",
                        "--end",
                        "1",
                        "--queries",
                        str(queries),
                        "--out",
                        str(output),
                    ]
                    old = sys.argv
                    Session.queries = []
                    try:
                        sys.argv = argv
                        self.assertEqual(module.main(), 0)
                    finally:
                        sys.argv = old
                    self.assertTrue(
                        Session.queries,
                        f"{suite} reused Q1 across a changed run identity",
                    )

    def test_same_identity_reuses_q1_without_sql(self):
        module = _load_runner("tpch")
        with TemporaryDirectory(prefix="benchmark-same-") as directory:
            tmp = Path(directory)
            queries = tmp / "queries"
            queries.mkdir()
            sql = "SELECT 1 AS same_query"
            (queries / "q1.sql").write_text(sql)
            import hashlib
            import sys as _sys

            _sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
            from resume_identity import run_identity, source_sha_for

            runner = Path(__file__).resolve().parents[1] / "tpch" / "run-ec2-connect.py"
            identity = run_identity(
                endpoint="sc://127.0.0.1:1",
                dataset="TPC-H SF100 (Glue db via Connect)",
                machine="m",
                tries=1,
                source_sha=source_sha_for(runner),
            )
            output = tmp / "result.json"
            output.write_text(
                json.dumps(
                    {
                        **identity,
                        "queries": [
                            {
                                "query": "Q1",
                                "hot_s": 1.0,
                                "error": None,
                                "sql_sha256": hashlib.sha256(sql.encode()).hexdigest(),
                            }
                        ],
                    }
                )
            )
            argv = [
                "runner",
                "--endpoint",
                "sc://127.0.0.1:1",
                "--glue-database",
                "db",
                "--machine",
                "m",
                "--tries",
                "1",
                "--start",
                "1",
                "--end",
                "1",
                "--queries",
                str(queries),
                "--out",
                str(output),
            ]
            old = sys.argv
            Session.queries = []
            try:
                sys.argv = argv
                self.assertEqual(module.main(), 0)
            finally:
                sys.argv = old
            self.assertEqual(Session.queries, [])

    def test_tpcds_missing_query_is_recorded_in_out(self):
        module = _load_runner("tpcds")
        with TemporaryDirectory(prefix="benchmark-missing-") as directory:
            tmp = Path(directory)
            queries = tmp / "queries"
            queries.mkdir()
            (queries / "q1.sql").write_text("SELECT 1 AS present")
            output = tmp / "result.json"
            argv = [
                "runner",
                "--endpoint",
                "sc://127.0.0.1:1",
                "--glue-database",
                "db",
                "--machine",
                "m",
                "--tries",
                "1",
                "--start",
                "1",
                "--end",
                "2",
                "--queries",
                str(queries),
                "--out",
                str(output),
            ]
            old = sys.argv
            Session.queries = []
            try:
                sys.argv = argv
                self.assertEqual(module.main(), 1)
            finally:
                sys.argv = old
            self.assertTrue(output.exists(), "trailing missing query left no --out artifact")
            payload = json.loads(output.read_text())
            self.assertEqual(payload["failures"], 1)
            names = [row["query"] for row in payload["queries"]]
            self.assertEqual(names, ["Q1", "Q2"])
            self.assertEqual(payload["queries"][1]["error"], "missing q2.sql")

    def test_tpcds_partial_resume_keeps_missing_sibling_failure(self):
        # Fixture results test checkpoint status, not engine performance.
        module = _load_runner("tpcds")
        for variant in ("skip", "execute"):
            with self.subTest(variant=variant), TemporaryDirectory(
                prefix="benchmark-missing-resume-"
            ) as directory:
                tmp = Path(directory)
                queries = tmp / "queries"
                queries.mkdir()
                q1 = queries / "q1.sql"
                q1.write_text("SELECT 1 AS one")
                output = tmp / "result.json"
                rc, original, _ = _invoke_tpcds(module, queries, output, end=2)
                self.assertEqual(rc, 1)
                self.assertEqual(original["failures"], 1)
                self.assertEqual(original["queries"][1]["error"], "missing q2.sql")
                if variant == "execute":
                    q1.write_text("SELECT 9 AS changed")

                rc, resumed, stdout = _invoke_tpcds(module, queries, output, end=1)
                self.assertEqual(
                    Session.queries, [] if variant == "skip" else [q1.read_text()]
                )
                self.assertEqual([r["query"] for r in resumed["queries"]], ["Q1", "Q2"])
                self.assertEqual(resumed["queries"][1], original["queries"][1])
                self.assertEqual(resumed["failures"], 1)
                self.assertEqual(rc, 1, "retained Q2 failure must keep the artifact failed")
                self.assertIn("artifact_failures=1", stdout)
                self.assertIn("selected_elapsed_total=", stdout)

                # Repair the actual missing input; replacing Q2 clears its failure.
                (queries / "q2.sql").write_text("SELECT 2 AS two")
                rc, repaired, stdout = _invoke_tpcds(module, queries, output, end=2)
                self.assertEqual(Session.queries, ["SELECT 2 AS two"])
                self.assertEqual(repaired["failures"], 0)
                self.assertEqual(rc, 0)
                self.assertIn("artifact_failures=0", stdout)

    def test_tpcds_partial_resume_counts_failed_attempt_without_error(self):
        module = _load_runner("tpcds")
        for variant in ("skip", "execute"):
            with self.subTest(variant=variant), TemporaryDirectory(
                prefix="benchmark-failed-attempt-resume-"
            ) as directory:
                tmp = Path(directory)
                queries = tmp / "queries"
                queries.mkdir()
                q1 = queries / "q1.sql"
                q1.write_text("SELECT 1 AS one")
                q2_sql = "SELECT 2 AS two"
                (queries / "q2.sql").write_text(q2_sql)
                output = tmp / "result.json"
                q2_calls = 0

                def fixture_sql(session, sql):
                    nonlocal q2_calls
                    session.queries.append(sql)
                    if sql == q2_sql:
                        q2_calls += 1
                        if q2_calls > 1:
                            raise RuntimeError("NO_ACTIVE_SESSION fixture failure")
                    return _Result()

                # The real retry loop exhausts reconnects after Q2's cold try.
                # Only Spark and its reconnect delay are fixtures; no live service.
                with patch.object(Session, "sql", fixture_sql), patch.object(
                    module.time, "sleep"
                ):
                    original_rc, original, _ = _invoke_tpcds(
                        module, queries, output, end=3, tries=3
                    )
                failed = original["queries"][1]
                self.assertEqual(q2_calls, 5)
                self.assertIsNone(failed["error"])
                self.assertEqual(len(failed["tries"]), 2)
                self.assertIsNotNone(failed["tries"][0])
                self.assertIsNone(failed["tries"][1])
                self.assertIsNone(failed["elapsed_s"])
                if variant == "execute":
                    q1.write_text("SELECT 9 AS changed")

                rc, resumed, stdout = _invoke_tpcds(
                    module, queries, output, end=1, tries=3
                )
                self.assertEqual(
                    Session.queries, [] if variant == "skip" else [q1.read_text()] * 3
                )
                self.assertEqual(resumed["queries"][1:], original["queries"][1:])
                self.assertEqual(resumed["failures"], 2)
                self.assertEqual(original["failures"], 2)
                self.assertEqual(original_rc, 1)
                self.assertEqual(rc, 1)
                self.assertIn("artifact_failures=2", stdout)

                # A successful Q2 retry replaces that failed row, but not missing Q3.
                rc, repaired, _ = _invoke_tpcds(module, queries, output, end=2, tries=3)
                self.assertEqual(Session.queries, [q2_sql] * 3)
                self.assertEqual(repaired["failures"], 1)
                self.assertEqual(repaired["queries"][2], original["queries"][2])
                self.assertEqual(rc, 1)

    def test_tpcds_skip_does_not_drop_unread_prior_queries(self):
        module = _load_runner("tpcds")
        with TemporaryDirectory(prefix="benchmark-skip-keep-") as directory:
            tmp = Path(directory)
            queries = tmp / "queries"
            queries.mkdir()
            sql = "SELECT 1 AS same_query"
            (queries / "q1.sql").write_text(sql)
            import sys as _sys

            _sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
            from resume_identity import run_identity, source_sha_for, sql_sha256

            runner = Path(__file__).resolve().parents[1] / "tpcds" / "run-ec2-connect.py"
            identity = run_identity(
                endpoint="sc://127.0.0.1:1",
                dataset="TPC-DS SF100 (Glue db via EC2 Connect)",
                machine="m",
                tries=1,
                source_sha=source_sha_for(runner),
            )
            sha = sql_sha256(sql)
            output = tmp / "result.json"
            output.write_text(
                json.dumps(
                    {
                        **identity,
                        "queries": [
                            {
                                "query": "Q1",
                                "hot_s": 1.0,
                                "elapsed_s": 1.0,
                                "error": None,
                                "sql_sha256": sha,
                            },
                            {
                                "query": "Q2",
                                "hot_s": 2.0,
                                "elapsed_s": 2.0,
                                "error": None,
                                "sql_sha256": "q2-prior",
                            },
                            {
                                "query": "Q3",
                                "hot_s": 3.0,
                                "elapsed_s": 3.0,
                                "error": None,
                                "sql_sha256": "q3-prior",
                            },
                        ],
                    }
                )
            )
            argv = [
                "runner",
                "--endpoint",
                "sc://127.0.0.1:1",
                "--glue-database",
                "db",
                "--machine",
                "m",
                "--tries",
                "1",
                "--start",
                "1",
                "--end",
                "1",
                "--queries",
                str(queries),
                "--out",
                str(output),
            ]
            old = sys.argv
            Session.queries = []
            try:
                sys.argv = argv
                self.assertEqual(module.main(), 0)
            finally:
                sys.argv = old
            self.assertEqual(Session.queries, [])
            payload = json.loads(output.read_text())
            names = [row["query"] for row in payload["queries"]]
            self.assertEqual(
                names,
                ["Q1", "Q2", "Q3"],
                "SKIP of Q1 overwrote --out and dropped unread prior queries",
            )


if __name__ == "__main__":
    unittest.main()
