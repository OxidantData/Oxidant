#!/usr/bin/env python3
"""Required-table coverage through the real converter CLI (#187).

Tiny authored SQL-flat-format fixtures, not toolkit or benchmark data. Expected
schemas/values and JSON/Parquet readback never import the converter/validator.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


TPC = Path(__file__).resolve().parents[1]
CONVERTER = TPC / "tbl_to_parquet.py"
TABLES = (
    "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
    "customer_address", "customer_demographics", "date_dim", "household_demographics",
    "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store",
    "store_returns", "store_sales", "time_dim", "warehouse", "web_page",
    "web_returns", "web_sales", "web_site",
)


def fixture_tables():
    names = dict(line.split("\t") for line in
                 (TPC / "tpcds_columns.tsv").read_text().splitlines())
    types = dict(line.split("\t") for line in
                 (TPC / "tpcds_types.tsv").read_text().splitlines())
    assert set(names) == set(TABLES) and len(names) == 24
    result = {}
    for table in TABLES:
        fields, tokens, values = [], [], []
        columns = names[table].split(",")
        specs = [entry.split(":", 1)[1] for entry in types[table].split("|")]
        assert len(columns) == len(specs)
        for index, (column, sql_type) in enumerate(zip(columns, specs)):
            if sql_type == "integer":
                arrow_type, token, value = pa.int32(), str(index + 1), index + 1
            elif sql_type == "date":
                arrow_type, token = pa.date32(), "2001-02-03"
                value = datetime.date(2001, 2, 3)
            elif sql_type.startswith("decimal("):
                match = re.fullmatch(r"decimal\((\d+),(\d+)\)", sql_type)
                assert match is not None
                precision, scale = map(int, match.groups())
                arrow_type, token, value = pa.decimal128(precision, scale), "1.25", Decimal("1.25")
            else:
                assert sql_type.startswith(("char(", "varchar(")), sql_type
                arrow_type, token, value = pa.string(), "é", "é"
            fields.append((column, arrow_type))
            tokens.append(token)
            values.append(value)
        row = dict(zip(columns, values))
        rows = [row, dict.fromkeys(columns), row.copy()]
        text = "|".join(tokens) + "|\n"
        raw = (text + "|" * len(columns) + "\n" + text).encode("latin1")
        result[table] = (raw, pa.schema(fields), rows)
    return result


class TpcdsCoverageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="tpcds-coverage-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.fixtures = fixture_tables()

    def prepare(self, label, tables):
        root = self.root / label
        (root / "raw").mkdir(parents=True)
        for table in tables:
            (root / "raw" / f"{table}.dat").write_bytes(self.fixtures[table][0])
        (root / "unrelated.txt").write_bytes(b"keep me\n")
        return root

    def invoke(self, root, *, only=None, check=False, force=False, expected=0):
        argv = [sys.executable, "-B", str(CONVERTER), "--suite", "tpcds",
                "--raw", str(root / "raw"), "--out", str(root / "parquet"),
                "--row-group", "2", "--target-part-bytes", "1024"]
        if only is not None:
            argv += ["--only", only]
        if check:
            argv += ["--check-complete"]
        if force:
            argv += ["--force"]
        result = subprocess.run(argv, capture_output=True, text=True, timeout=30,
                                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
        self.assertEqual(result.returncode, expected,
                         f"{argv}\nstdout={result.stdout}\nstderr={result.stderr}")
        return result

    def snapshot(self, root):
        result = {}
        for path in [root, *sorted(root.rglob("*"))]:
            info = path.lstat()
            record = [info.st_mode, info.st_size, info.st_mtime_ns]
            if stat.S_ISREG(info.st_mode):
                record.append(hashlib.sha256(path.read_bytes()).hexdigest())
            elif path.is_symlink():
                record.append(os.readlink(path))
            result[str(path.relative_to(root))] = record
        return result

    def assert_table(self, root, table, *, source_names=None):
        directory = root / "parquet" / table
        marker = json.loads((directory / "_oxidant_complete.json").read_text())
        parts = sorted(directory.glob("part-*.parquet"))
        self.assertEqual(set(p.name for p in directory.iterdir()),
                         {"_oxidant_complete.json", *(p.name for p in parts)})
        self.assertEqual(marker["version"], 2)
        self.assertEqual(marker["parts"], [
            {"name": p.name, "size": p.stat().st_size,
             "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
             "rows": pq.read_table(p).num_rows} for p in parts])
        self.assertEqual(marker["sources"], [
            {"name": p.name, "size": p.stat().st_size,
             "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
            for p in (root / "raw" / name for name in
                      (source_names if source_names is not None else [f"{table}.dat"]))])
        actual = pq.read_table(directory)
        self.assertEqual(actual.schema, self.fixtures[table][1])
        self.assertEqual(actual.to_pylist(), self.fixtures[table][2])
        self.assertEqual(marker["rows"], 3)

    def test_healthy_explicit_subset_is_reused(self):
        root = self.prepare("selected", ["income_band"])
        self.invoke(root, only="income_band")
        self.assert_table(root, "income_band")
        before = self.snapshot(root)
        self.invoke(root, only="income_band", check=True)
        result = self.invoke(root, only="income_band")
        self.assertIn("skip income_band (complete)", result.stdout)
        self.assertEqual(self.snapshot(root), before)

    def test_missing_required_tables_never_check_complete(self):
        root = self.prepare("partial", ["income_band"])
        self.invoke(root, only="income_band")
        before = self.snapshot(root)
        for only in (None, "customer", "income_band,customer"):
            with self.subTest(only=only):
                self.invoke(root, only=only, check=True, expected=1)
                self.assertEqual(self.snapshot(root), before)

    def test_missing_required_input_refuses_before_any_publication(self):
        for only in (None, "income_band,web_site"):
            for published in (False, True):
                with self.subTest(only=only, published=published):
                    root = self.prepare(f"early-{only}-{published}", ["income_band"])
                    if published:
                        self.invoke(root, only="income_band")
                        staging = root / "parquet" / ".income_band.converting"
                        staging.mkdir()
                        (staging / "unrelated.txt").write_bytes(b"retain staging on refusal")
                    before = self.snapshot(root)
                    result = self.invoke(root, only=only, force=True, expected=1)
                    self.assertNotIn("Traceback", result.stderr)
                    self.assertEqual(self.snapshot(root), before)

    def test_all_declared_tables_and_full_selection_are_complete(self):
        root = self.prepare("all-tables", TABLES)
        self.invoke(root)
        self.assertEqual({p.name for p in (root / "parquet").iterdir()}, set(TABLES))
        for table in TABLES:
            self.assert_table(root, table)
        before = self.snapshot(root)
        for only in (None, ",".join(TABLES)):
            with self.subTest(only=only):
                self.invoke(root, only=only, check=True)
                self.invoke(root, only=only)
                self.assertEqual(self.snapshot(root), before)
        # A subset on a full dataset must preserve every unselected sibling.
        sibling_before = {t: self.snapshot(root / "parquet" / t)
                          for t in TABLES if t != "income_band"}
        self.invoke(root, only="income_band,income_band", force=True)
        self.assert_table(root, "income_band")
        self.assertEqual({t: self.snapshot(root / "parquet" / t)
                          for t in sibling_before}, sibling_before)
        self.invoke(root, check=True)

    def test_invalid_explicit_selection_is_not_normalized_or_ignored(self):
        selections = ("", ",", " ", "income_band,", ",income_band",
                      "income_band,,customer", " income_band", "income_band ",
                      "Income_band", "unknown", "income_band,unknown", "dbgen_version")
        for index, only in enumerate(selections):
            for check in (True, False):
                with self.subTest(only=only, check=check):
                    root = self.prepare(f"invalid-{index}-{check}", TABLES)
                    self.invoke(root)
                    before = self.snapshot(root)
                    result = self.invoke(root, only=only, check=check,
                                         force=True, expected=1)
                    self.assertNotIn("Traceback", result.stderr)
                    self.assertEqual(self.snapshot(root), before)

    def test_unusable_required_inputs_refuse_before_replacing_siblings(self):
        for only in (None, "income_band,web_site"):
            for change in ("empty", "directory", "child-directory"):
                with self.subTest(only=only, change=change):
                    root = self.prepare(f"unusable-{only}-{change}",
                                        TABLES if only is None else ["income_band", "web_site"])
                    self.invoke(root, only=only)
                    path = root / "raw" / "web_site.dat"
                    path.unlink()
                    if change == "empty":
                        path.touch()
                    elif change == "directory":
                        path.mkdir()
                    else:
                        # A healthy first child must not hide an unusable later child.
                        (root / "raw" / "web_site_1_2.dat").write_bytes(self.fixtures["web_site"][0])
                        (root / "raw" / "web_site_2_2.dat").mkdir()
                    before = self.snapshot(root)
                    self.invoke(root, only=only, check=True, expected=1)
                    self.assertEqual(self.snapshot(root), before)
                    result = self.invoke(root, only=only, force=True, expected=1)
                    self.assertNotIn("Traceback", result.stderr)
                    self.assertEqual(self.snapshot(root), before)


    def test_each_declared_raw_table_is_required_even_with_complete_output(self):
        root = self.prepare("missing-raw", TABLES)
        self.invoke(root)
        for table in TABLES:
            with self.subTest(table=table):
                source = root / "raw" / f"{table}.dat"
                original = source.read_bytes()
                source.unlink()
                try:
                    before = self.snapshot(root)
                    for only in (None, table):
                        self.invoke(root, only=only, check=True, expected=1)
                        self.invoke(root, only=only, force=True, expected=1)
                        self.assertEqual(self.snapshot(root), before)
                finally:
                    source.write_bytes(original)
        self.invoke(root, check=True)

    def test_each_missing_table_output_is_refused_then_repaired(self):
        root = self.prepare("missing-output", TABLES)
        self.invoke(root)
        for table in TABLES:
            with self.subTest(table=table):
                directory = root / "parquet" / table
                saved = root / f"saved-{table}"
                directory.rename(saved)
                saved_before = self.snapshot(saved)
                before = self.snapshot(root)
                for only in (None, table, f"income_band,{table}"):
                    self.invoke(root, only=only, check=True, expected=1)
                    self.assertEqual(self.snapshot(root), before)
                siblings = {t: self.snapshot(root / "parquet" / t)
                            for t in TABLES if t != table}
                self.invoke(root, only=table)
                self.assert_table(root, table)
                self.assertEqual(self.snapshot(saved), saved_before)
                self.assertEqual({t: self.snapshot(root / "parquet" / t)
                                  for t in siblings}, siblings)
                self.invoke(root, check=True)

    def test_absent_or_unclassified_raw_inputs_never_create_output(self):
        for change in ("absent", "empty", "metadata", "unclassified"):
            for only in (None, "income_band", "unknown", "income_band,unknown"):
                with self.subTest(change=change, only=only):
                    root = self.prepare(f"no-raw-{change}-{only}", [])
                    if change == "absent":
                        (root / "raw").rmdir()
                    elif change in ("metadata", "unclassified"):
                        name = "dbgen_version_1_2.dat" if change == "metadata" else "unknown.dat"
                        (root / "raw" / name).write_bytes(b"not table data\n")
                    before = self.snapshot(root)
                    self.invoke(root, only=only, check=True, expected=1)
                    self.invoke(root, only=only, expected=1)
                    self.assertEqual(self.snapshot(root), before)

    def test_unclassified_raw_files_do_not_invalidate_known_tables(self):
        root = self.prepare("extra-raw", TABLES)
        for name in ("dbgen_version.dat", "dbgen_version_1_2.dat", "notes.dat", "notes_1_2.dat"):
            (root / "raw" / name).write_bytes(b"not table data\n")
        self.invoke(root)
        self.assertEqual({p.name for p in (root / "parquet").iterdir()}, set(TABLES))
        before = self.snapshot(root)
        self.invoke(root, check=True)
        self.invoke(root)
        self.assertEqual(self.snapshot(root), before)

    def test_parallel_sources_empty_child_and_repaired_retry(self):
        for published in (False, True):
            with self.subTest(published=published):
                root = self.prepare(f"parallel-{published}", ["income_band", "reason"])
                self.invoke(root, only="reason")
                sibling = root / "parquet" / "reason"
                sibling_before = self.snapshot(sibling)
                lines = self.fixtures["income_band"][0].splitlines(keepends=True)
                sources = [f"income_band_{index}_3.dat" for index in (1, 2, 3)]
                for name, content in zip(sources, (lines[0], b"".join(lines[1:]), b"")):
                    (root / "raw" / name).write_bytes(content)
                # Parallel children retain precedence over a stale concatenation.
                (root / "raw" / "income_band.dat").write_bytes(b"invalid-concatenation\n")
                if published:
                    self.invoke(root, only="income_band")
                second = root / "raw" / sources[1]
                second.write_bytes(b"invalid-integer|2|3|\n")
                output_before = {p.name: self.snapshot(p) for p in (root / "parquet").iterdir()}
                self.invoke(root, only="income_band", expected=1)
                self.assertEqual({p.name: self.snapshot(p) for p in (root / "parquet").iterdir()},
                                 output_before)
                self.invoke(root, only="income_band", check=True, expected=1)
                second.write_bytes(b"".join(lines[1:]))
                self.invoke(root, only="income_band")
                self.assert_table(root, "income_band", source_names=sources)
                self.assertEqual(self.snapshot(sibling), sibling_before)
                before = self.snapshot(root)
                self.invoke(root, only="income_band,reason", check=True)
                self.invoke(root, only="income_band,reason")
                self.assertEqual(self.snapshot(root), before)

    def test_prepare_completion_boundary_uses_required_table_coverage(self):
        # Execute only the existing check/convert boundary, never kit setup,
        # generation, dependency installation, registration or size reporting.
        lines = (TPC / "prepare.sh").read_text().splitlines()
        start = next(i for i, line in enumerate(lines) if line.startswith('if "$PYTHON"'))
        end = lines.index("fi", start)
        boundary = "\n".join(lines[start:end + 1])
        self.assertIn("--check-complete", boundary)
        for state in ("missing-raw", "missing-output", "healthy"):
            with self.subTest(state=state):
                root = self.prepare(f"shell-{state}", ["income_band"]
                                    if state == "missing-raw" else TABLES)
                self.invoke(root, only=None if state == "healthy" else "income_band")
                before = self.snapshot(root)
                sibling_before = self.snapshot(root / "parquet" / "income_band")
                result = subprocess.run(
                    ["bash", "-euo", "pipefail", "-c", boundary],
                    capture_output=True, text=True, timeout=30,
                    env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1",
                         "PYTHON": sys.executable, "ROOT": str(TPC.parents[1]),
                         "SUITE": "tpcds", "OUT": str(root), "PARQUET": str(root / "parquet")})
                self.assertEqual(result.returncode, 1 if state == "missing-raw" else 0,
                                 result.stdout + result.stderr)
                if state == "missing-output":
                    for table in TABLES:
                        self.assert_table(root, table)
                    self.assertEqual(self.snapshot(root / "parquet" / "income_band"), sibling_before)
                else:
                    self.assertEqual(self.snapshot(root), before)
                self.assertEqual("skipping convert" in result.stdout, state == "healthy")


if __name__ == "__main__":
    unittest.main()
