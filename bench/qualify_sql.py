"""Qualify bare table references without touching literals, comments or aliases.

Shared by the TPC-H and TPC-DS Connect runners. A case-insensitive regex over
table names also rewrites string contents (TPC-H Q16 ``LIKE '%Customer%Complaints%'``)
and aliases; only table-reference position is rewritten here.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

_TOKEN = re.compile(
    r"(?P<str>'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\")"
    r"|(?P<comment>--[^\n]*|/\*.*?\*/)"
    r"|(?P<word>[A-Za-z_][\w$]*)"
    r"|(?P<ws>\s+)"
    r"|(?P<punct>.)",
    re.DOTALL,
)

_TABLE_REF_START = {"from", "join", "into"}
_FROM_LIST_HARD_END = {
    "where",
    "group",
    "order",
    "having",
    "limit",
    "offset",
    "union",
    "intersect",
    "except",
    "minus",
    "window",
    "qualify",
    "select",
    "with",
    "values",
    "set",
    "returning",
    "sort",
    "cluster",
    "distribute",
    "lateral",
    "pivot",
    "unpivot",
    "tablesample",
}
_FROM_LIST_SOFT_END = {"on", "using", "as"}

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def qualify_relations(
    sql: str,
    tables: Iterable[str],
    catalog: str,
    database: str,
) -> str:
    """Rewrite unqualified table names in FROM/JOIN/INTO position to catalog.db.table.

    Strings, comments, aliases, column names, already-qualified names and
    relation-function arguments are left intact. ``catalog`` and ``database``
    must be simple identifiers.
    """
    if not _IDENT.match(catalog) or not _IDENT.match(database):
        raise ValueError(
            f"catalog and database must be simple identifiers, got {catalog!r}/{database!r}"
        )
    names = {t.lower() for t in tables}
    tokens = list(_TOKEN.finditer(sql))
    out: list[str] = []
    stack: list[tuple[bool, bool]] = []
    expect_table = False
    in_from = False
    i = 0
    while i < len(tokens):
        m = tokens[i]
        tok = m.group(0)
        if m.lastgroup in ("str", "comment", "ws"):
            out.append(tok)
        elif m.lastgroup == "punct":
            if tok == "(":
                stack.append((expect_table, in_from))
                expect_table = in_from = False
            elif tok == ")":
                expect_table, in_from = stack.pop() if stack else (False, False)
                expect_table = False
            elif tok == "," and in_from:
                expect_table = True
            out.append(tok)
        else:
            low = tok.lower()
            if low in _TABLE_REF_START:
                expect_table = in_from = True
                out.append(tok)
            elif low in _FROM_LIST_HARD_END:
                expect_table = in_from = False
                out.append(tok)
            elif low in _FROM_LIST_SOFT_END:
                expect_table = False
                out.append(tok)
            elif expect_table:
                j = i + 1
                tail = ""
                while True:
                    k = j
                    if k < len(tokens) and tokens[k].lastgroup == "ws":
                        k += 1
                    m2 = k + 1
                    if (
                        k < len(tokens)
                        and tokens[k].lastgroup == "punct"
                        and tokens[k].group(0) == "."
                    ):
                        if m2 < len(tokens) and tokens[m2].lastgroup == "ws":
                            m2 += 1
                        if m2 < len(tokens) and tokens[m2].lastgroup == "word":
                            tail += "".join(t.group(0) for t in tokens[j : m2 + 1])
                            j = m2 + 1
                            continue
                    break
                if not tail and low in names:
                    out.append(f"{catalog}.{database}.{low}")
                else:
                    out.append(tok + tail)
                i = j
                expect_table = False
                continue
            else:
                out.append(tok)
        i += 1
    return "".join(out)
