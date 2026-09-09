"""Relation qualifier: literals, comments and aliases stay intact."""

from __future__ import annotations

import importlib.util
import re
import sys
import unittest
from pathlib import Path
from types import ModuleType

BENCH = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCH))

from qualify_sql import qualify_relations  # noqa: E402

TPCH_QUERIES = BENCH / "tpch" / "queries"
_STRING = re.compile(r"'(?:[^']|'')*'")
_COMMENT = re.compile(r"--[^\n]*|/\*.*?\*/", re.DOTALL)


def _load(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


TPCH = _load("tpch_runner", BENCH / "tpch" / "run-ec2-connect.py")
TPCDS = _load("tpcds_runner", BENCH / "tpcds" / "run-ec2-connect.py")


def _opaque_payloads(sql: str) -> list[str]:
    return [m.group(0) for m in list(_STRING.finditer(sql)) + list(_COMMENT.finditer(sql))]


class QualifyTpchTests(unittest.TestCase):
    def test_q16_keeps_customer_complaints_literal(self) -> None:
        sql = (TPCH_QUERIES / "q16.sql").read_text()
        actual = TPCH.qualify(sql, "fixture_db")
        self.assertIn("'%Customer%Complaints%'", sql)
        self.assertIn("'%Customer%Complaints%'", actual)
        self.assertNotIn("glue.fixture_db.customer%Complaints", actual)
        self.assertIn("glue.fixture_db.supplier", actual)
        self.assertIn("glue.fixture_db.partsupp", actual)

    def test_all_tpch_queries_keep_literal_and_comment_text(self) -> None:
        for path in sorted(TPCH_QUERIES.glob("q*.sql")):
            with self.subTest(path.name):
                original = path.read_text()
                qualified = TPCH.qualify(original, "fixture_db")
                self.assertEqual(_opaque_payloads(original), _opaque_payloads(qualified))

    def test_qualification_is_idempotent(self) -> None:
        sql = (TPCH_QUERIES / "q16.sql").read_text()
        once = TPCH.qualify(sql, "fixture_db")
        self.assertEqual(once, TPCH.qualify(once, "fixture_db"))

    def test_escaped_quotes_and_comments_are_opaque(self) -> None:
        sql = (
            "SELECT * FROM customer -- customer\n"
            "WHERE c_name = 'customer''s' /* from customer */"
        )
        out = qualify_relations(sql, TPCH.TABLES, "glue", "db")
        self.assertIn("glue.db.customer", out)
        self.assertIn("'customer''s'", out)
        self.assertIn("-- customer", out)
        self.assertIn("/* from customer */", out)
        self.assertEqual(out.count("glue.db.customer"), 1)

    def test_alias_and_already_qualified_are_left(self) -> None:
        sql = "SELECT customer FROM glue.other.customer AS customer, orders"
        out = qualify_relations(sql, TPCH.TABLES, "glue", "db")
        self.assertIn("glue.other.customer", out)
        self.assertIn("glue.db.orders", out)
        self.assertNotIn("glue.db.customer", out)

    def test_comma_join_and_nested_select(self) -> None:
        sql = "SELECT * FROM orders, (SELECT * FROM customer) c"
        out = qualify_relations(sql, TPCH.TABLES, "glue", "db")
        self.assertIn("glue.db.orders", out)
        self.assertIn("glue.db.customer", out)

    def test_select_literal_named_customer(self) -> None:
        sql = "SELECT 'customer' AS customer FROM customer"
        out = qualify_relations(sql, TPCH.TABLES, "glue", "db")
        self.assertIn("'customer'", out)
        self.assertIn("glue.db.customer", out)
        self.assertTrue(out.startswith("SELECT 'customer' AS customer FROM"))


class QualifyTpcdsTests(unittest.TestCase):
    def test_store_literal_and_alias(self) -> None:
        sql = "SELECT 'store' AS store_sales FROM store_sales AS store_sales"
        out = TPCDS.qualify(sql, "fixture_db")
        self.assertIn("'store'", out)
        self.assertIn("glue.fixture_db.store_sales", out)
        self.assertIn("AS store_sales", out)

    def test_interval_rewrite_stays_outside_the_qualifier(self) -> None:
        sql = "SELECT (d_date) + 1 days FROM date_dim"
        out = TPCDS.qualify(sql, "db")
        self.assertIn("INTERVAL '1' DAY", out)
        self.assertIn("glue.db.date_dim", out)


if __name__ == "__main__":
    unittest.main()
