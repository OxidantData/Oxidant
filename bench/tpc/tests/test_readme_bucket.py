#!/usr/bin/env python3
"""TPC README bucket derivation must use the oxidant- prefix (OxidantData/Oxidant#149)."""

from pathlib import Path
import unittest


class ReadmeBucketTests(unittest.TestCase):
    def test_readme_derives_oxidant_artifacts_prefix(self):
        readme = Path(__file__).resolve().parents[1] / "README.md"
        text = readme.read_text()
        self.assertIn(
            "BUCKET=oxidant-artifacts-$(aws sts get-caller-identity --query Account --output text)",
            text,
        )
        self.assertNotIn(
            "BUCKET=weft-artifacts-$(aws sts get-caller-identity --query Account --output text)",
            text,
        )


if __name__ == "__main__":
    unittest.main()
