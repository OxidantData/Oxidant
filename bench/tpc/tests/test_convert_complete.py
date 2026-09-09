#!/usr/bin/env python3
"""TPC conversion must not treat a partial Parquet dir as complete (OxidantData/Oxidant#187)."""

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq


def load_converter():
    path = Path(__file__).resolve().parents[1] / "tbl_to_parquet.py"
    spec = importlib.util.spec_from_file_location("tbl_to_parquet", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class ConvertCompletionTests(unittest.TestCase):
    def test_repaired_second_child_is_converted_not_skipped(self):
        m = load_converter()
        m.TPCH_SCHEMAS = {"nation": m.TPCH_SCHEMAS["nation"]}
        with tempfile.TemporaryDirectory(prefix="tpc-completion-") as directory:
            root = Path(directory)
            raw, out = root / "raw", root / "parquet"
            raw.mkdir()
            (raw / "nation.tbl.1").write_text("1|one|0|ok|\n")
            second = raw / "nation.tbl.2"
            second.write_text("invalid-integer|two|0|bad|\n")
            with self.assertRaises(Exception):
                m.convert_tpch(raw, out, 8, 1024)
            second.write_text("2|two|0|ok|\n")
            m.convert_tpch(raw, out, 8, 1024)
            rows = pq.read_table(out / "nation").to_pylist()
            self.assertEqual(sorted(r["n_nationkey"] for r in rows), [1, 2], rows)

    def test_healthy_two_child_control(self):
        m = load_converter()
        m.TPCH_SCHEMAS = {"nation": m.TPCH_SCHEMAS["nation"]}
        with tempfile.TemporaryDirectory(prefix="tpc-healthy-") as directory:
            root = Path(directory)
            raw, out = root / "raw", root / "parquet"
            raw.mkdir()
            (raw / "nation.tbl.1").write_text("1|one|0|ok|\n")
            (raw / "nation.tbl.2").write_text("2|two|0|ok|\n")
            m.convert_tpch(raw, out, 8, 1024)
            rows = pq.read_table(out / "nation").to_pylist()
            self.assertEqual(sorted(r["n_nationkey"] for r in rows), [1, 2], rows)
            m.convert_tpch(raw, out, 8, 1024)
            rows = pq.read_table(out / "nation").to_pylist()
            self.assertEqual(sorted(r["n_nationkey"] for r in rows), [1, 2], rows)


if __name__ == "__main__":
    unittest.main()
