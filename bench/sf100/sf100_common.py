"""Shared TPC-H / TPC-DS helpers for SF100 harness runners."""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

TPCH_TABLES = [
    "nation",
    "region",
    "supplier",
    "customer",
    "part",
    "partsupp",
    "orders",
    "lineitem",
]
TPCDS_TABLES = [
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
]


def qualify_sql(sql: str, glue_db: str, tables: list[str], catalog: str = "glue") -> str:
    """Rewrite bare TPC table refs to ``<catalog>.<db>.<table>``.

    Walks the SQL and only qualifies identifiers that appear as relations in a
    FROM/JOIN list (not column aliases, not ``alias.col``, not ``EXTRACT(… FROM …)``).
    """
    table_map = {t.lower(): f"{catalog}.{glue_db}.{t}" for t in tables}
    n = len(sql)
    out: list[str] = []
    i = 0
    in_from = False
    expect_table = False
    state_stack: list[tuple[bool, bool]] = []

    def is_word_start(idx: int) -> bool:
        return idx == 0 or not (sql[idx - 1].isalnum() or sql[idx - 1] == "_")

    def word_at(idx: int) -> tuple[str, int] | None:
        if idx >= n or not (sql[idx].isalnum() or sql[idx] == "_"):
            return None
        j = idx
        while j < n and (sql[j].isalnum() or sql[j] == "_"):
            j += 1
        return sql[idx:j], j

    while i < n:
        if sql[i] in ("'", '"'):
            quote = sql[i]
            j = i + 1
            while j < n:
                if sql[j] == quote:
                    if quote == "'" and j + 1 < n and sql[j + 1] == "'":
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            out.append(sql[i:j])
            i = j
            continue

        if sql[i] == "(":
            state_stack.append((in_from, expect_table))
            in_from, expect_table = False, False
            out.append("(")
            i += 1
            continue
        if sql[i] == ")":
            out.append(")")
            i += 1
            if state_stack:
                in_from, expect_table = state_stack.pop()
                expect_table = False
            continue

        if sql[i] == ",":
            out.append(",")
            i += 1
            if in_from:
                expect_table = True
            continue

        if is_word_start(i):
            w = word_at(i)
            if w:
                word, j = w
                lw = word.lower()

                if lw == "extract":
                    out.append(sql[i:j])
                    i = j
                    while i < n and sql[i].isspace():
                        out.append(sql[i])
                        i += 1
                    if i < n and sql[i] == "(":
                        out.append("(")
                        i += 1
                        ed = 1
                        while i < n and ed:
                            if sql[i] == "(":
                                ed += 1
                            elif sql[i] == ")":
                                ed -= 1
                            out.append(sql[i])
                            i += 1
                    continue

                if lw in {"from", "join"}:
                    out.append(word)
                    i = j
                    in_from = True
                    expect_table = True
                    continue

                # ON/USING stay inside the FROM list so ``JOIN t ON (…) , other``
                # (TPC-DS style) still qualifies ``other``.
                if in_from and lw in {"on", "using"}:
                    expect_table = False
                    out.append(word)
                    i = j
                    continue

                if in_from and lw in {
                    "where",
                    "group",
                    "order",
                    "having",
                    "limit",
                    "union",
                    "except",
                    "intersect",
                }:
                    in_from = False
                    expect_table = False
                    out.append(word)
                    i = j
                    continue

                if in_from and expect_table and lw in table_map:
                    out.append(table_map[lw])
                    i = j
                    expect_table = False
                    continue

                if in_from and not expect_table and lw == "as":
                    out.append(word)
                    i = j
                    while i < n and sql[i].isspace():
                        out.append(sql[i])
                        i += 1
                    alias = word_at(i)
                    if alias:
                        out.append(alias[0])
                        i = alias[1]
                    continue

                out.append(word)
                i = j
                if in_from and expect_table:
                    expect_table = False
                continue

        out.append(sql[i])
        i += 1

    text = "".join(out)
    # Collapse accidental double-catalog prefixes from already-qualified SQL.
    return re.sub(
        rf"(?i)\b{re.escape(catalog)}\.{re.escape(catalog)}\.{re.escape(glue_db)}\.",
        f"{catalog}.{glue_db}.",
        text,
    )


def load_queries(suite: str) -> list[tuple[str, str]]:
    qdir = REPO / "bench" / suite / "queries"
    files = sorted(qdir.glob("q*.sql"), key=lambda p: int(p.stem[1:]))
    if not files:
        raise SystemExit(f"no queries in {qdir}")
    return [(f"Q{p.stem[1:]}", p.read_text()) for p in files]


def filter_queries(
    queries: list[tuple[str, str]], only: str
) -> list[tuple[str, str]]:
    if not only:
        return queries
    want = {f"Q{x.strip()}" for x in only.split(",") if x.strip()}
    return [(n, s) for n, s in queries if n in want]
