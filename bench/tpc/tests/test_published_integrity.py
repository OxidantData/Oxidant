#!/usr/bin/env python3
"""Real converter CLI contracts for published Parquet integrity (#187).

Tiny authored flat files only; no toolkit, SQL endpoint or benchmark execution.
The reader below does not import converter code or its completion validator.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq


CONVERTER = Path(__file__).resolve().parents[1] / "tbl_to_parquet.py"
TPCH_ROWS = {
    "nation": "1|one|0|ok|\n2|two|0||\n1|one|0|ok|\n",
    "region": "0|region|ok|\n",
    "supplier": "1|name|address|1|phone|1.00|ok|\n",
    "customer": "1|name|address|1|phone|1.00|segment|ok|\n",
    "part": "1|name|mfgr|brand|type|1|container|1.00|ok|\n",
    "partsupp": "1|1|1|1.00|ok|\n",
    "orders": "1|1|O|1.00|1994-01-01|priority|clerk|0|ok|\n",
    "lineitem": "1|1|1|1|1.00|1.00|0.00|0.00|R|O|1994-01-01|1994-01-01|1994-01-01|instruct|mode|ok|\n",
}


class PublishedIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="tpc-published-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def prepare(self, suite, label=""):
        root = self.root / f"{suite}{label}"
        raw, out = root / "raw", root / "parquet"
        raw.mkdir(parents=True)
        if suite == "tpch":
            for table, rows in TPCH_ROWS.items():
                (raw / f"{table}.tbl").write_text(rows)
            table = "nation"
        else:
            # income_band: integer key and lower/upper bounds, including a null.
            (raw / "income_band.dat").write_text("1|0|9|\n2|10||\n1|0|9|\n")
            table = "income_band"
        self.invoke(suite, root)
        return root, out / table

    def invoke(self, suite, root, *, check=False, expected=0):
        argv = [sys.executable, "-B", str(CONVERTER), "--suite", suite,
                "--raw", str(root / "raw"), "--out", str(root / "parquet"),
                "--row-group", "2", "--target-part-bytes", "1024"]
        if suite == "tpcds":
            argv += ["--only", "income_band"]
        if check:
            argv += ["--check-complete"]
        result = subprocess.run(argv, capture_output=True, text=True, timeout=30,
                                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
        self.assertEqual(result.returncode, expected,
                         f"{argv}\nstdout={result.stdout}\nstderr={result.stderr}")
        return result

    def snapshot(self, directory):
        return {p.name: (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns)
                for p in directory.iterdir() if p.is_file()}

    def test_nonempty_corrupted_parquet_is_not_complete_and_retry_repairs(self):
        for suite in ("tpch", "tpcds"):
            with self.subTest(suite=suite):
                root, table = self.prepare(suite)
                expected = pq.read_table(table)
                siblings = {p.name: self.snapshot(p) for p in table.parent.iterdir()
                            if p.is_dir() and p != table}
                self.invoke(suite, root, check=True)
                part = next(table.glob("part-*.parquet"))
                original = part.read_bytes()
                # Preserve the nonzero size and change the actual footer magic.
                part.write_bytes(original[:-4] + b"FAIL")
                self.invoke(suite, root, check=True, expected=1)
                self.invoke(suite, root)
                self.assertTrue(pq.read_table(table).equals(expected))
                self.assertEqual({p.name: self.snapshot(p) for p in table.parent.iterdir()
                                  if p.is_dir() and p != table}, siblings)
                self.invoke(suite, root, check=True)

    def test_valid_parquet_value_change_is_not_complete(self):
        for suite in ("tpch", "tpcds"):
            with self.subTest(suite=suite):
                root, table = self.prepare(suite)
                expected = pq.read_table(table)
                part = next(table.glob("part-*.parquet"))
                # A readable footer, unchanged schema and cardinality are not enough.
                changed = expected.set_column(0, expected.schema.field(0),
                                              expected.column(0).take([1, 0, 1]))
                original_size = part.stat().st_size
                pq.write_table(changed, part, row_group_size=2, compression="snappy")
                self.assertEqual(part.stat().st_size, original_size)
                self.assertEqual(pq.read_table(table).num_rows, expected.num_rows)
                self.assertFalse(pq.read_table(table).equals(expected))
                self.invoke(suite, root, check=True, expected=1)
                self.invoke(suite, root)
                self.assertTrue(pq.read_table(table).equals(expected))
                self.invoke(suite, root, check=True)

    def test_output_membership_must_match_exactly(self):
        for suite in ("tpch", "tpcds"):
            for change in ("extra-part", "other-parquet", "nested", "missing", "symlink"):
                with self.subTest(suite=suite, change=change):
                    root, table = self.prepare(suite, change)
                    expected = pq.read_table(table)
                    part = next(table.glob("part-*.parquet"))
                    saved = root / "unrelated.parquet"
                    original = part.read_bytes()
                    if change == "extra-part":
                        (table / "part-99999.parquet").write_bytes(part.read_bytes())
                    elif change == "other-parquet":
                        (table / "unexpected.parquet").write_bytes(part.read_bytes())
                    elif change == "nested":
                        child = table / "unexpected"
                        child.mkdir()
                        (child / part.name).write_bytes(part.read_bytes())
                    elif change == "missing":
                        part.unlink()
                    else:
                        saved.write_bytes(original)
                        part.unlink()
                        part.symlink_to(saved)
                    self.invoke(suite, root, check=True, expected=1)
                    self.invoke(suite, root)
                    self.assertTrue(pq.read_table(table).equals(expected))
                    self.invoke(suite, root, check=True)
                    if change == "symlink":
                        self.assertEqual(saved.read_bytes(), original)

    def test_manifest_matches_independent_file_and_row_readers(self):
        for suite in ("tpch", "tpcds"):
            with self.subTest(suite=suite):
                root, table = self.prepare(suite)
                marker = json.loads((table / "_oxidant_complete.json").read_text())
                self.assertEqual(marker["version"], 2)
                files = sorted(table.glob("*.parquet"))
                self.assertEqual(marker["parts"], [
                    {"name": p.name, "size": len(p.read_bytes()),
                     "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
                     "rows": pq.read_table(p).num_rows}
                    for p in files
                ])
                self.assertEqual(marker["rows"], pq.read_table(table).num_rows)
                self.assertEqual(marker["rows"], 3)
                self.invoke(suite, root, check=True)

    def test_unverified_marker_is_refused_without_changing_files(self):
        for suite in ("tpch", "tpcds"):
            for change in ("legacy", "unknown-version", "part-count", "total-count",
                           "hash", "traversal", "invalid-json", "not-an-object"):
                with self.subTest(suite=suite, change=change):
                    root, table = self.prepare(suite, change)
                    path = table / "_oxidant_complete.json"
                    marker = json.loads(path.read_text())
                    if change == "legacy":
                        marker = {"version": 1, "sources": marker["sources"],
                                  "parts": [p.name for p in sorted(table.glob("*.parquet"))]}
                    elif change == "unknown-version":
                        marker["version"] = 999
                    elif change == "part-count":
                        marker["parts"][0]["rows"] += 1
                    elif change == "total-count":
                        marker["rows"] += 1
                    elif change == "hash":
                        marker["parts"][0]["sha256"] = "0" * 64
                    elif change == "traversal":
                        marker["parts"][0]["name"] = "../unrelated.parquet"
                    elif change == "not-an-object":
                        marker = []
                    path.write_text("{" if change == "invalid-json" else json.dumps(marker))
                    before = self.snapshot(table)
                    self.invoke(suite, root, check=True, expected=1)
                    self.assertEqual(self.snapshot(table), before)

    def test_healthy_output_is_reused_without_rewriting(self):
        for suite in ("tpch", "tpcds"):
            with self.subTest(suite=suite):
                root, table = self.prepare(suite)
                expected = pq.read_table(table)
                self.assertEqual(expected.num_rows, 3)
                self.assertEqual(expected.to_pylist()[0], expected.to_pylist()[2])
                self.assertIn(None, expected.to_pylist()[1].values())
                before = self.snapshot(table)
                self.invoke(suite, root, check=True)
                result = self.invoke(suite, root)
                self.assertIn("(complete)", result.stdout)
                self.assertEqual(self.snapshot(table), before)
                self.assertTrue(pq.read_table(table).equals(expected))


if __name__ == "__main__":
    unittest.main()
