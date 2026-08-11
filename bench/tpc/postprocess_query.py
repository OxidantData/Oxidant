#!/usr/bin/env python3
"""Post-process qgen/dsqgen stdout into a single harness .sql file."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


def extract_sql(text: str, label: str, *, first_statement_only: bool) -> str:
    m = re.search(r"(?is)\b(with|select|create)\b", text)
    if not m:
        raise SystemExit(f"{label}: no SELECT/WITH/CREATE in generator output")
    body = text[m.start() :]
    # qgen :n -1 → "limit -1" (means unlimited) — drop it.
    body = re.sub(r"(?im)^\s*limit\s+-1\s*;?\s*$", "", body)
    # PostgreSQL dialect of qgen emits `…;\nlimit N;` as a second statement — fold into SQL LIMIT.
    body = re.sub(
        r";\s*\n\s*limit\s+(\d+)\s*;?\s*$",
        r"\nLIMIT \1;",
        body,
        flags=re.IGNORECASE,
    )
    body = body.strip() + "\n"
    if first_statement_only:
        # TPC-DS Q14/Q23/Q24/Q39 templates emit two result-set statements; the
        # engineering harness is single-statement. Keep the first (matches prior
        # DuckDB harness behavior). Full power-test coverage can run both later.
        stmts = split_statements(body)
        if len(stmts) > 1:
            body = stmts[0].rstrip() + ";\n"
    if not body.rstrip().endswith(";"):
        body = body.rstrip() + ";\n"
    return body


def rewrite_tpch_q15(body: str) -> str:
    """Official qgen emits CREATE VIEW / SELECT / DROP VIEW — fold to a WITH CTE."""
    m = re.search(
        r"(?is)create\s+view\s+(\w+)\s*\(([^)]+)\)\s+as\s+(.*?);\s*"
        r"(select\b.*?)\s*;\s*"
        r"drop\s+view\s+\w+\s*;?",
        body,
    )
    if not m:
        # Already a CTE from a prior regen.
        if re.search(r"(?is)^\s*with\b", body):
            return body if body.rstrip().endswith(";") else body.rstrip() + ";\n"
        raise SystemExit(f"TPC-H Q15: could not rewrite CREATE VIEW form:\n{body}")
    view, cols, view_body, select_body = m.group(1), m.group(2), m.group(3), m.group(4)
    # Prefer a stable CTE name used historically by the harness.
    cte_name = "revenue"
    select_body = re.sub(rf"\b{re.escape(view)}\b", cte_name, select_body)
    # Attach column aliases from the view definition when the inner SELECT lacks them.
    out = (
        f"WITH {cte_name} ({cols}) AS (\n"
        f"{view_body.strip()}\n"
        f")\n"
        f"{select_body.strip()}\n"
    )
    if not out.rstrip().endswith(";"):
        out = out.rstrip() + ";\n"
    return out


def split_statements(sql: str) -> list[str]:
    """Split on semicolons outside quotes/comments — good enough for TPC templates."""
    parts: list[str] = []
    buf: list[str] = []
    i = 0
    n = len(sql)
    in_squote = False
    in_dquote = False
    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if not in_squote and not in_dquote and ch == "-" and nxt == "-":
            while i < n and sql[i] != "\n":
                buf.append(sql[i])
                i += 1
            continue
        if ch == "'" and not in_dquote:
            buf.append(ch)
            if in_squote and nxt == "'":
                buf.append(nxt)
                i += 2
                continue
            in_squote = not in_squote
            i += 1
            continue
        if ch == '"' and not in_squote:
            in_dquote = not in_dquote
            buf.append(ch)
            i += 1
            continue
        if ch == ";" and not in_squote and not in_dquote:
            part = "".join(buf).strip()
            if part:
                parts.append(part)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def rewrite_tpch_q11(body: str) -> str:
    """Keep multi-SF placeholder: fraction = 0.0001 / SF (KAN-30)."""
    patterns = [
        (r"\*\s*0\.0+10*", "* (0.0001 / __OXIDANT_SF__)"),
        (r"\*\s*\(0\.0001\s*/\s*__OXIDANT_SF__\)", "* (0.0001 / __OXIDANT_SF__)"),
        (r"\*\s*0\.0001\s*/\s*__OXIDANT_SF__", "* (0.0001 / __OXIDANT_SF__)"),
    ]
    for pat, repl in patterns:
        body2, count = re.subn(pat, repl, body, count=1)
        if count == 1:
            return body2
    if "* (0.0001 / __OXIDANT_SF__)" in body:
        return body
    raise SystemExit(f"TPC-H Q11: could not rewrite SF fraction:\n{body}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite", choices=("tpch", "tpcds"), required=True)
    ap.add_argument("--num", type=int, required=True)
    ap.add_argument("--raw", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    label = f"{args.suite.upper()} Q{args.num}"
    body = extract_sql(
        args.raw.read_text(encoding="utf-8", errors="replace"),
        label,
        first_statement_only=(args.suite == "tpcds"),
    )
    if args.suite == "tpch" and args.num == 11:
        body = rewrite_tpch_q11(body)
    if args.suite == "tpch" and args.num == 15:
        body = rewrite_tpch_q15(body)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    header = (
        f"-- Generated by bench/tpc/generate-queries.sh from official TPC "
        f"{'qgen' if args.suite == 'tpch' else 'dsqgen'}.\n"
        f"-- Do not hand-edit; regenerate with: ./bench/tpc/generate-queries.sh\n"
    )
    args.out.write_text(header + body, encoding="utf-8")
    print(f"  wrote {args.out}")


if __name__ == "__main__":
    main()
