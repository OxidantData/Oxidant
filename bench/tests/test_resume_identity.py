#!/usr/bin/env python3
"""Resume must bind query timings to run identity (OxidantData/Oxidant#188)."""

from __future__ import annotations

import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


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
