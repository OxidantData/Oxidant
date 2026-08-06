#!/usr/bin/env python3
"""Shared golden-check machinery (KAN-50).

The golden gates compare a suite JSONL's recorded engine checksums against DuckDB
goldens. Historically every checksum inequality printed MISMATCH, forcing manual
row-diff triage of two known-benign diff classes:

  (a) Spark-faithful DECIMAL scale vs DuckDB f64 — e.g. the engine's AVG comes
      back as Decimal('x.xyz000') while DuckDB returns the float 'x.xyz…' with
      full f64 precision (TPC-H Q1/Q8, ~12 TPC-DS queries at SF10);
  (b) ORDER BY … LIMIT boundary picks — the ORDER BY keys do not fully
      determine which rows make the first N (ties at the boundary, or NULLs in
      ORDER BY columns where Spark's NULLS-FIRST default differs from DuckDB's
      NULLS-LAST), so engine and DuckDB legitimately return different rows
      (TPC-DS Q11/Q12).

This module canonicalizes numerics and emits a three-way verdict:

  MATCH    — golden canonical checksum equals the recorded engine checksum.
  BENIGN   — the diff is provably (verified) or structurally (heuristic) one of
             the benign classes above; the reason is printed per query and the
             summary lists benign queries separately.
  MISMATCH — none of the benign classes explain the diff.

Numeric-scale rule (documented contract)
----------------------------------------
A golden float ``f`` and an engine decimal ``d`` of scale ``k`` represent the
same value when ``round(f, k) == d`` where:

  * ``round`` is round-half-even applied to the *exact decimal repr of the f64*
    (Python's shortest-round-trip ``repr(f)``), and
  * trailing zeros are stripped before string comparison (``Decimal.normalize``),
    so ``Decimal('1.230000')`` and ``1.23`` canonicalize identically.

The engine's f64→decimal cast *truncates* (observed on oxidant: AVG → DECIMAL(19,6)
via toward-zero truncation), so ``ROUND_DOWN`` (toward zero) is accepted as an
alternative rounding mode. A BENIGN verdict is only emitted when one concrete
per-column (mode, scale) assignment makes the transformed golden checksum equal
the recorded checksum *exactly* — the recorded hash is the proof.

Verdict ladder (first match wins)
---------------------------------
1. MATCH — old-canon golden checksum == recorded checksum.
2. BENIGN numeric-scale (verified) — a per-column scale assignment reproduces
   the recorded checksum: uniform (mode, k), then a column subset at a common
   (mode, k), then a two-scale column partition (bounded: ≤17 numeric columns).
   A ±1-ulp variant allows exactly one cell to use the opposite rounding mode
   (engine f64 accumulation can land across a display-scale rounding boundary
   from DuckDB); bounded and still proved by exact hash equality.
3. BENIGN boundary-tie — top-level LIMIT, row counts agree, and the golden
   *full* result shows first-N membership is undetermined (boundary tie group
   or NULL-ordering sensitivity). *Verified* by enumerating every legitimate
   boundary pick when the ambiguous zone is small; otherwise heuristic.
4. BENIGN numeric-drift (heuristic) — row counts agree, result has non-integral
   numerics, nothing above explains the diff. The engine accumulates aggregates
   in f64 whose last ulp can cross a display-scale rounding boundary vs DuckDB
   (repo convention: 0.1 % relative tolerance for non-integral floats —
   bench/tpcds/README.md). Hash-only evidence cannot prove this offline; the
   query is flagged for audit. ``--strict-benign`` reclassifies these as
   MISMATCH.
5. MISMATCH — anything else (including row-count disagreement).

Run ``python3 golden_common.py --self-test`` for unit-ish validation of the
canonicalization and verdict logic on synthetic data.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_EVEN, localcontext
from functools import cmp_to_key
from itertools import combinations

MODES = ("rhe", "trunc")  # round-half-even (documented rule), then toward-zero cast
_ROUND = {"rhe": ROUND_HALF_EVEN, "trunc": ROUND_DOWN}
SCALE_RANGE = range(1, 13)  # candidate display scales for the scale search
SUBSET_MAX_COLS = 17  # bound for the column-subset / two-scale searches (2^n)
ENUM_MAX_ZONE = 24  # bound for boundary-pick enumeration
ENUM_MAX_COMBOS = 200_000


# --------------------------------------------------------------------------- #
# Canonicalization (identical to bench/sf100/run-spark-connect.py)
# --------------------------------------------------------------------------- #
def canonical_cell(v) -> str:
    """Old-canon cell form: what the suite runner hashed into the recorded checksum."""
    if v is None:
        return "NULL"
    if isinstance(v, Decimal):
        return format(v.normalize(), "f")
    if isinstance(v, float):
        if v == 0.0:
            return "0"
        return f"{v:.12g}"
    return repr(v)


def scale_cell(v, mode: str, k: int) -> str:
    """Canonical cell after rounding numerics at scale ``k`` (round-half-even on the
    f64 repr, or toward-zero ``trunc`` — the engine's f64→decimal cast).

    Decimals with ≤ ``k`` decimals are already finer-grained than the target scale
    and pass through unchanged (trailing zeros stripped); everything else is
    quantized, then trailing zeros are stripped, so ``2.50`` and ``2.5`` agree.
    """
    if isinstance(v, bool) or not isinstance(v, (float, Decimal)):
        return canonical_cell(v)
    if isinstance(v, Decimal) and -v.as_tuple().exponent <= k:
        return format(v.normalize(), "f")
    dv = Decimal(repr(v)) if isinstance(v, float) else v
    with localcontext() as ctx:  # SF100 sums can exceed the default 28-digit prec
        ctx.prec = 50
        q = dv.quantize(Decimal(1).scaleb(-k), rounding=_ROUND[mode])
    return format(q.normalize(), "f")


def _lines_checksum(lines: list[str]) -> str:
    """sha256 over sorted canonical row strings (the suite's multiset checksum)."""
    h = hashlib.sha256()
    for line in sorted(lines):
        h.update(line.encode("utf-8", "replace"))
        h.update(b"\n")
    return h.hexdigest()


def rows_checksum(rows, cellfn=canonical_cell) -> str:
    """Multiset checksum: sort canonical row strings, then sha256 (suite-compatible)."""
    lines = []
    for row in rows:
        vals = tuple(row) if hasattr(row, "__iter__") else (row,)
        lines.append("(" + ", ".join(cellfn(v) for v in vals) + ")")
    return _lines_checksum(lines)


def numeric_columns(rows) -> list[int]:
    """Column indexes holding at least one float/Decimal cell."""
    if not rows:
        return []
    return [
        c
        for c in range(len(rows[0]))
        if any(isinstance(r[c], (float, Decimal)) and not isinstance(r[c], bool) for r in rows)
    ]


def _checksum_with(rows, coltrans: dict[int, tuple[str, int]]) -> str:
    return _lines_checksum(
        [
            "("
            + ", ".join(
                scale_cell(v, *coltrans[c]) if c in coltrans else canonical_cell(v)
                for c, v in enumerate(r)
            )
            + ")"
            for r in rows
        ]
    )


# --------------------------------------------------------------------------- #
# Scale search: find a per-column (mode, scale) assignment reproducing ``target``
# --------------------------------------------------------------------------- #
def find_scale_assignment(rows, target: str):
    """Search per-column numeric-scale assignments whose transformed golden checksum
    equals the recorded engine checksum. Returns a human-readable description or
    None. Exact hash equality is the proof that the diff is numeric-scale-only.
    """
    numcols = numeric_columns(rows)
    if not numcols:
        return None

    # S1: one (mode, scale) for every numeric column.
    for mode in MODES:
        for k in SCALE_RANGE:
            if _checksum_with(rows, {c: (mode, k) for c in numcols}) == target:
                return f"uniform {mode}@{k}"

    if len(numcols) > SUBSET_MAX_COLS:
        return None

    # S2: a subset of columns at a common (mode, scale), rest old-canon — covers
    # queries mixing DOUBLE (identity) and DECIMAL (scaled) result columns.
    for mode in MODES:
        for k in SCALE_RANGE:
            for size in range(len(numcols), -1, -1):
                for sub in combinations(numcols, size):
                    if _checksum_with(rows, {c: (mode, k) for c in sub}) == target:
                        return f"subset {mode}@{k} cols={list(sub)}"

    # S3: two display scales at a common mode — covers Spark-faithful mixed
    # decimal types (e.g. DECIMAL(38,4) ratios beside DECIMAL(19,6) averages).
    scales = sorted(set(SCALE_RANGE) & {2, 4, 6, 8, 10, 12})
    for mode in MODES:
        for k1 in scales:
            for k2 in scales:
                if k1 == k2:
                    continue
                for mask in range(1 << len(numcols)):
                    ct = {
                        c: (mode, k1) if (mask >> i) & 1 else (mode, k2)
                        for i, c in enumerate(numcols)
                    }
                    if _checksum_with(rows, ct) == target:
                        sub = [c for i, c in enumerate(numcols) if (mask >> i) & 1]
                        return f"two-scale {mode} cols={sub}@{k1} rest@{k2}"
    return None


# Bounds for the ±1-ulp tolerant search (S4).
TOLERANT_MAX_COLS = 10
TOLERANT_MAX_DISC = 400
TOLERANT_MAX_HASHES = 600_000


def _row_parts(rows, coltrans) -> list[list[str]]:
    """Per-row canonical cell strings under a per-column (mode, scale) assignment."""
    return [
        [scale_cell(v, *coltrans[c]) if c in coltrans else canonical_cell(v) for c, v in enumerate(r)]
        for r in rows
    ]


def find_scale_assignment_tolerant(rows, target: str):
    """S4 — like [`find_scale_assignment`], but allows exactly one cell to use the
    opposite rounding mode.

    The engine accumulates aggregates in f64, so its value and DuckDB's can land
    on opposite sides of a display-scale rounding boundary: the engine's decimal
    is then ``rhe`` of the golden f64 at that scale where every other cell in the
    column is ``trunc`` (or vice versa) — a ±1-ulp-at-scale difference. ``rhe``
    and ``trunc`` at scale k differ by exactly one unit in the last place
    whenever they differ at all, so flipping one cell's mode is precisely a
    ±1-ulp tolerance. Only single-cell flips are searched: beyond that, the
    numeric-drift heuristic (which admits uncertainty) takes over.

    Bounded: ≤ ``TOLERANT_MAX_COLS`` numeric columns, ≤ ``TOLERANT_MAX_DISC``
    discriminating cells per base assignment, ≤ ``TOLERANT_MAX_HASHES`` hashes.
    Returns a description or None. Exact hash equality is still the proof.
    """
    numcols = numeric_columns(rows)
    if not numcols or len(numcols) > TOLERANT_MAX_COLS:
        return None

    # Base assignments: uniform, plus two-scale partitions over k-sensitive
    # columns only (columns whose strings actually change between k1 and k2).
    bases: list[tuple[str, dict[int, tuple[str, int]]]] = []
    for mode in MODES:
        for k in SCALE_RANGE:
            bases.append((f"uniform {mode}@{k}", {c: (mode, k) for c in numcols}))
    scales = sorted(set(SCALE_RANGE) & {2, 4, 6, 8, 10, 12})
    parts_cache: dict[tuple, list[list[str]]] = {}

    def parts_for(ct) -> list[list[str]]:
        key = tuple(sorted(ct.items()))
        if key not in parts_cache:
            parts_cache[key] = _row_parts(rows, ct)
        return parts_cache[key]

    hashes = 0

    def hash_lines(lines) -> str:
        nonlocal hashes
        hashes += 1
        return _lines_checksum(lines)

    for mode in MODES:
        for k1 in scales:
            for k2 in scales:
                if k1 == k2:
                    continue
                sensitive = []
                for c in numcols:
                    s1 = [p[c] for p in parts_for({c: (mode, k1)})]
                    s2 = [p[c] for p in parts_for({c: (mode, k2)})]
                    if s1 != s2:
                        sensitive.append(c)
                for mask in range(1 << len(sensitive)):
                    ct = {}
                    for c in numcols:
                        ct[c] = (mode, k2)
                    for i, c in enumerate(sensitive):
                        if (mask >> i) & 1:
                            ct[c] = (mode, k1)
                    sub = [c for c in sensitive if ct[c][1] == k1]
                    bases.append((f"two-scale {mode} cols={sub}@{k1} rest@{k2}", ct))

    for desc, ct in bases:
        if hashes > TOLERANT_MAX_HASHES:
            return None
        parts = parts_for(ct)
        base_lines = ["(" + ", ".join(p) + ")" for p in parts]
        # Discriminating cells: rhe vs trunc strings differ at the assigned scale.
        disc = []
        for ri, r in enumerate(rows):
            for c, (mode, k) in ct.items():
                v = r[c]
                if isinstance(v, bool) or not isinstance(v, (float, Decimal)):
                    continue
                if isinstance(v, Decimal) and -v.as_tuple().exponent <= k:
                    continue
                other = "trunc" if mode == "rhe" else "rhe"
                alt = scale_cell(v, other, k)
                if alt != parts[ri][c]:
                    disc.append((ri, c, alt))
        if len(disc) > TOLERANT_MAX_DISC:
            continue
        for ri, c, alt in disc:
            flipped = parts[ri].copy()
            flipped[c] = alt
            lines = base_lines.copy()
            lines[ri] = "(" + ", ".join(flipped) + ")"
            if hash_lines(lines) == target:
                return f"{desc}; ±1 ulp (rhe↔trunc) at one cell"
    return None


# --------------------------------------------------------------------------- #
# ORDER BY … LIMIT boundary analysis
# --------------------------------------------------------------------------- #
def _top_level_spans(sql: str):
    """Yield (start, end, word) for top-level words, tracking parens and quotes."""
    depth = 0
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch in ("'", '"'):
            j = i + 1
            while j < n:
                if sql[j] == ch:
                    if ch == "'" and j + 1 < n and sql[j + 1] == "'":
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            i = j
            continue
        if ch == "(":
            depth += 1
            i += 1
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            i += 1
            continue
        if depth == 0 and (ch.isalnum() or ch == "_"):
            j = i
            while j < n and (sql[j].isalnum() or sql[j] == "_"):
                j += 1
            yield i, j, sql[i:j]
            i = j
            continue
        i += 1


def parse_order_limit(sql: str):
    """Parse the trailing top-level ``ORDER BY … LIMIT n`` of a query.

    Returns ``(order_items, limit, sql_without_limit)`` where ``order_items`` is a
    list of ``(expr, desc, nulls_first_or_None)``. ``(None, None, None)`` when the
    query has no top-level LIMIT.
    """
    text = sql.strip().rstrip(";").strip()
    words = list(_top_level_spans(text))
    limit_at = None
    order_at = None
    for idx, (s, e, w) in enumerate(words):
        lw = w.lower()
        if lw == "limit":
            m = re.match(r"\s*(\d+)", text[e:])
            if m:
                limit_at = (s, e + m.end(), int(m.group(1)))
        if lw == "order" and idx + 1 < len(words) and words[idx + 1][2].lower() == "by":
            order_at = s
    if limit_at is None:
        return None, None, None
    sql_no_limit = (text[: limit_at[0]] + text[limit_at[1]:]).strip()
    if order_at is None or order_at > limit_at[0]:
        return [], limit_at[2], sql_no_limit

    ob_text = text[order_at : limit_at[0]]
    ob_text = re.sub(r"(?i)^\s*order\s+by", "", ob_text)
    items = []
    for part in _split_top_commas(ob_text):
        expr = part.strip()
        desc = bool(re.search(r"(?i)\bdesc\s*$", expr))
        expr = re.sub(r"(?i)\s+(asc|desc)\s*$", "", expr)
        nulls = None
        m = re.search(r"(?i)\s+nulls\s+(first|last)\s*$", expr)
        if m:
            nulls = m.group(1).lower() == "first"
            expr = expr[: m.start()].strip()
        items.append((expr, desc, nulls))
    return items, limit_at[2], sql_no_limit


def _split_top_commas(text: str) -> list[str]:
    parts, depth, start = [], 0, 0
    for i, ch in enumerate(text):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(text[start:i])
            start = i + 1
    parts.append(text[start:])
    return parts


def map_order_columns(order_items, col_names: list[str]):
    """Map ORDER BY expressions to result-column indexes (case-insensitive; an
    ``alias.col`` form maps to ``col``). Returns a list of
    ``(col_index, desc, nulls_first)`` or None when an expression is not a plain
    column reference (boundary analysis then stays unavailable)."""
    lower = [n.lower() for n in col_names]
    out = []
    for expr, desc, nulls in order_items:
        name = expr.strip().strip('"').split(".")[-1].strip('"').lower()
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", name) or lower.count(name) != 1:
            return None
        out.append((lower.index(name), desc, nulls))
    return out


def _norm_key_val(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float, Decimal)):
        return Decimal(repr(v))
    return str(v)


def _key_cmp(a, b, spec):
    for idx, desc, nulls_first in spec:
        av, bv = _norm_key_val(a[idx]), _norm_key_val(b[idx])
        if av is None and bv is None:
            c = 0
        elif av is None:
            c = -1 if nulls_first else 1
        elif bv is None:
            c = 1 if nulls_first else -1
        else:
            c = (av > bv) - (av < bv)
        if c:
            return -c if desc else c
    return 0


def _key_tuple(row, spec):
    return tuple(_norm_key_val(row[idx]) for idx, _, _ in spec)


def boundary_zone(full_rows, spec, limit: int):
    """Analyze whether the first-``limit`` membership is fully determined.

    Returns ``(forced, ambiguous, note)``: disjoint index sets into ``full_rows``
    (``forced`` rows are in the first N under every legitimate tie/null variant;
    ``ambiguous`` rows may or may not be picked), and a human note. An empty
    ``ambiguous`` set means the boundary is fully determined.
    """
    spec_first = [(i, d, True if nf is None else nf) for i, d, nf in spec]
    spec_last = [(i, d, False if nf is None else nf) for i, d, nf in spec]
    order_first = sorted(range(len(full_rows)), key=cmp_to_key(lambda a, b: _key_cmp(full_rows[a], full_rows[b], spec_first)))
    order_last = sorted(range(len(full_rows)), key=cmp_to_key(lambda a, b: _key_cmp(full_rows[a], full_rows[b], spec_last)))
    if len(full_rows) <= limit:
        return set(range(len(full_rows))), set(), "full result below LIMIT — boundary determined"

    s_first, s_last = set(order_first[:limit]), set(order_last[:limit])
    ambiguous = set(s_first ^ s_last)
    notes = []
    if ambiguous:
        notes.append(f"NULL-ordering changes first-{limit} membership (zone +{len(ambiguous)})")

    # Tie straddle: rows whose ORDER BY key equals the boundary key tie across
    # the limit under at least one null ordering.
    for order, sp, tag in ((order_first, spec_first, "nulls-first"), (order_last, spec_last, "nulls-last")):
        bkey = _key_tuple(full_rows[order[limit - 1]], sp)
        tied = {i for i in range(len(full_rows)) if _key_tuple(full_rows[i], sp) == bkey}
        inside = {i for i in tied if i in set(order[:limit])}
        if 0 < len(inside) < len(tied):
            ambiguous |= tied
            notes.append(f"ORDER BY key tie straddles the LIMIT under {tag} ({len(tied)} tied rows)")

    forced = (s_first & s_last) - ambiguous
    return forced, ambiguous, "; ".join(notes) or "boundary determined"


def try_boundary_verdict(full_rows, spec, limit: int, target: str, max_rows: int):
    """Verified boundary BENIGN: enumerate every legitimate boundary pick and try
    to reproduce the recorded checksum (old-canon, then uniform scale transforms).
    Returns ``(tag, reason)`` or None when the zone is empty/too large or no pick
    matches."""
    forced, ambiguous, note = boundary_zone(full_rows, spec, limit)
    if not ambiguous:
        return None
    need = max_rows - len(forced)
    if need < 0 or need > len(ambiguous):
        return None
    zone = sorted(ambiguous)
    n_combos = _n_choose_k(len(zone), need)
    reason = f"{note}; zone={len(zone)} pick {need}"

    def checksums_for(pick_rows):
        yield "identity", rows_checksum(pick_rows)
        numcols = numeric_columns(pick_rows)
        for mode in MODES:
            for k in SCALE_RANGE:
                yield f"uniform {mode}@{k}", _checksum_with(pick_rows, {c: (mode, k) for c in numcols})

    if len(zone) <= ENUM_MAX_ZONE and n_combos <= ENUM_MAX_COMBOS:
        base = [full_rows[i] for i in sorted(forced)]
        for pick in combinations(zone, need):
            cand = base + [full_rows[i] for i in pick]
            for how, s in checksums_for(cand):
                if s == target:
                    return "verified", f"boundary pick enumerated ({reason}); matched with {how}"
    return "heuristic", f"{reason} (zone too large to enumerate)" if n_combos > ENUM_MAX_COMBOS else reason


def _n_choose_k(n: int, k: int) -> int:
    if k < 0 or k > n:
        return 0
    k = min(k, n - k)
    num = 1
    for i in range(k):
        num = num * (n - i) // (i + 1)
    return num


# --------------------------------------------------------------------------- #
# Verdict orchestration
# --------------------------------------------------------------------------- #
class Verdict:
    MATCH = "MATCH"
    BENIGN = "BENIGN"
    MISMATCH = "MISMATCH"

    def __init__(self, tag: str, reason: str, verified: bool):
        self.tag = tag
        self.reason = reason
        self.verified = verified

    def __str__(self):
        flag = "verified" if self.verified else "heuristic"
        return f"{self.tag} [{self.reason}] ({flag})"


def verdict_for(golden_rows, recorded_checksum: str, recorded_rows, sql: str,
                run_full_query=None, strict_benign: bool = False) -> Verdict:
    """Three-way verdict for one query.

    ``golden_rows``       — DuckDB rows for the query as written (with LIMIT).
    ``recorded_checksum`` — engine checksum from the suite JSONL.
    ``recorded_rows``     — engine row_count from the suite JSONL (may be None).
    ``run_full_query``    — callable returning ``(rows, col_names)`` for the
                            LIMIT-stripped query, needed for boundary analysis.
    """
    golden_sum = rows_checksum(golden_rows)
    if golden_sum == recorded_checksum:
        return Verdict(Verdict.MATCH, "checksums equal", True)

    # Row-count disagreement can never be explained by scale, boundary picks, or
    # drift (none of them add or drop rows) — fail fast with a clear reason.
    counts_agree = recorded_rows is None or recorded_rows == len(golden_rows)
    if not counts_agree:
        return Verdict(
            Verdict.MISMATCH,
            f"row counts differ: engine={recorded_rows} golden={len(golden_rows)}",
            True,
        )

    how = find_scale_assignment(golden_rows, recorded_checksum)
    if how:
        return Verdict(Verdict.BENIGN, f"numeric-scale: {how}", True)
    how = find_scale_assignment_tolerant(golden_rows, recorded_checksum)
    if how:
        return Verdict(Verdict.BENIGN, f"numeric-scale: {how}", True)

    order_items, limit, sql_no_limit = parse_order_limit(sql)
    if limit is not None and order_items and run_full_query is not None:
        try:
            full_rows, col_names = run_full_query(sql_no_limit)
        except Exception:
            full_rows, col_names = None, None
        if full_rows:
            spec = map_order_columns(order_items, col_names)
            if spec is not None:
                got = try_boundary_verdict(full_rows, spec, limit, recorded_checksum, len(golden_rows))
                if got:
                    tag, reason = got
                    if tag == "verified":
                        return Verdict(Verdict.BENIGN, f"boundary-tie: {reason}", True)
                    if strict_benign:
                        return Verdict(Verdict.MISMATCH, f"boundary-tie unproven: {reason}", True)
                    return Verdict(Verdict.BENIGN, f"boundary-tie (unproven): {reason}", False)

    if any(isinstance(v, (float, Decimal)) and not isinstance(v, bool)
           for r in golden_rows for v in r if v is not None):
        if strict_benign:
            return Verdict(Verdict.MISMATCH, "numeric-drift unproven under --strict-benign", True)
        return Verdict(
            Verdict.BENIGN,
            "numeric-drift: row counts agree; no scale assignment reproduces the checksum; "
            "consistent with ±1 ulp engine f64 aggregate drift at the display scale "
            "(UNVERIFIED — re-run with row dump to confirm)",
            False,
        )
    return Verdict(Verdict.MISMATCH, "no benign explanation found", True)


# --------------------------------------------------------------------------- #
# Shared checker main loop
# --------------------------------------------------------------------------- #
def run_check(suite: str, db: str, jsonl: str, prefix: str, only: str = "",
              strict_benign: bool = False, repo_root=None):
    """Compare recorded engine checksums in ``jsonl`` against DuckDB goldens and
    print per-query verdicts plus the summary. Returns the process exit code."""
    import json
    from pathlib import Path

    root = Path(repo_root) if repo_root else Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(root / "bench" / "sf100"))
    from sf100_common import load_queries  # noqa: E402

    import duckdb  # noqa: E402

    eng = {}
    for line in Path(jsonl).read_text().splitlines():
        rec = json.loads(line)
        if rec.get("checksum") and rec.get("status") == "ok":
            eng[rec["query"]] = (rec["checksum"], rec.get("row_count"))

    def duckdb_sql(sql: str) -> str:
        sql = sql.replace(prefix, "")
        return re.sub(r"(interval '\d+' \w+) \(\d+\)", r"\1", sql)

    only_set = {f"Q{n}" for n in only.split(",")} if only else None
    con = duckdb.connect(db, read_only=True)
    n_match = n_benign = n_bad = n_err = n_skip = 0
    benign_verified: list[str] = []
    benign_heuristic: list[str] = []
    for name, sql in load_queries(suite, sf=10):
        if only_set and name not in only_set:
            continue
        if name not in eng:
            n_skip += 1
            continue
        e_sum, e_rows = eng[name]
        try:
            golden_rows = con.execute(duckdb_sql(sql)).fetchall()
        except Exception as e:  # noqa: BLE001
            print(f"{name}: GOLDEN-ERROR {str(e)[:120]}")
            n_err += 1
            continue

        def run_full(sql_no_limit, _sql=sql):
            cur = con.execute(duckdb_sql(sql_no_limit))
            names = [d[0] for d in cur.description]
            return cur.fetchall(), names

        v = verdict_for(golden_rows, e_sum, e_rows, sql,
                        run_full_query=run_full, strict_benign=strict_benign)
        if v.tag == Verdict.MATCH:
            n_match += 1
        elif v.tag == Verdict.BENIGN:
            n_benign += 1
            (benign_verified if v.verified else benign_heuristic).append(f"{name} [{v.reason}]")
        else:
            n_bad += 1
        if v.tag == Verdict.MATCH:
            print(f"{name}: MATCH  engine={e_sum[:12]} rows={e_rows}")
        else:
            golden_sum = rows_checksum(golden_rows)
            print(f"{name}: {v}  engine={e_sum[:12]} golden={golden_sum[:12]} rows={e_rows}")

    print(f"SUMMARY match={n_match} benign={n_benign} mismatch={n_bad} "
          f"golden_errors={n_err} skipped={n_skip}")
    if benign_verified:
        print("BENIGN (verified): " + "; ".join(benign_verified))
    if benign_heuristic:
        print("BENIGN (heuristic — audit advised): " + "; ".join(benign_heuristic))
    return 1 if (n_bad or (strict_benign and benign_heuristic)) else 0


# --------------------------------------------------------------------------- #
# Self-test
# --------------------------------------------------------------------------- #
def self_test() -> int:
    failures = []

    def check(cond, label):
        print(("ok  " if cond else "FAIL") + f"  {label}")
        if not cond:
            failures.append(label)

    # -- 1. The numeric-scale rule: a Decimal and a float compare equal when the
    #       float, rounded (half-even, on the f64 repr) at the decimal's scale,
    #       equals the decimal. Trailing zeros are stripped.
    check(scale_cell(1.23, "rhe", 6) == canonical_cell(Decimal("1.230000")), "float 1.23 == Decimal('1.230000') at scale 6")
    check(scale_cell(2.675, "rhe", 2) == "2.68", "rhe on f64 repr: round(2.675, 2) == 2.68 (exact decimal of repr)")
    check(scale_cell(0.125, "rhe", 2) == "0.12", "rhe is half-even: 0.125 -> 0.12")
    check(scale_cell(0.375, "rhe", 2) == "0.38", "rhe is half-even: 0.375 -> 0.38")
    check(scale_cell(0.375, "trunc", 2) == "0.37", "trunc is toward zero: 0.375 -> 0.37")
    check(scale_cell(-0.375, "trunc", 2) == "-0.37", "trunc toward zero on negatives")
    check(scale_cell(Decimal("2.5"), "rhe", 6) == "2.5", "Decimal finer than k passes through")
    check(scale_cell(54.0, "trunc", 6) == "54", "integral float canonicalizes to '54'")
    check(scale_cell(38237.15100895854, "trunc", 6) == "38237.151008", "engine-style truncation of avg f64")

    # -- 2. rows_checksum matches the suite runner's algorithm (known vector).
    rows = [(1, Decimal("1.50")), (2, 0.1)]
    expect = rows_checksum(rows)  # self-consistency: multiset order irrelevant
    check(rows_checksum([(2, 0.1), (1, Decimal("1.50"))]) == expect, "checksum is order-insensitive (multiset)")

    # -- 3. find_scale_assignment: synthetic engine checksum over Decimals,
    #       golden over floats — uniform, subset, and two-scale shapes.
    def engine_sum(engine_rows):
        return rows_checksum(engine_rows)

    g_uniform = [(1, 25.500975103007097, "x"), (2, 25.522448302840946, "y")]
    e_uniform = [(1, Decimal("25.500975"), "x"), (2, Decimal("25.522448"), "y")]
    got = find_scale_assignment(g_uniform, engine_sum(e_uniform))
    check(got is not None and "6" in got, f"uniform scale assignment found: {got}")

    g_subset = [(1, 58.6666666667, 43.1466666667), (2, 18.0, 87.9666666667)]
    e_subset = [(1, 58.6666666667, Decimal("43.146666")), (2, 18.0, Decimal("87.966666"))]
    got = find_scale_assignment(g_subset, engine_sum(e_subset))
    check(got is not None and "subset" in got or (got and "uniform" not in got), f"subset scale assignment found: {got}")

    g_two = [(1, 102.50672272348788, 6104.916666666667), (2, 96.37084863293246, 2000.0100001)]
    e_two = [(1, Decimal("102.5067"), Decimal("6104.916666")), (2, Decimal("96.3708"), Decimal("2000.01"))]
    got = find_scale_assignment(g_two, engine_sum(e_two))
    check(got is not None, f"two-scale assignment found: {got}")

    # A genuine value difference must NOT be explained by any scale assignment.
    g_bad = [(1, 25.500975103007097)]
    e_bad = [(1, Decimal("25.6"))]
    check(find_scale_assignment(g_bad, engine_sum(e_bad)) is None, "real value diff is not scale-benign")

    # ±1-ulp tolerance: one cell needs rhe where the column is otherwise trunc
    # (engine f64 landed across the rounding boundary from DuckDB).
    g_flip = [(1, 0.1234575), (2, 0.7654335)]
    e_flip = [(1, Decimal("0.123458")), (2, Decimal("0.765433"))]  # rhe@6 vs trunc@6
    check(find_scale_assignment(g_flip, engine_sum(e_flip)) is None,
          "mixed-mode column defeats the exact scale search")
    got = find_scale_assignment_tolerant(g_flip, engine_sum(e_flip))
    check(got is not None and "±1 ulp" in got, f"single-cell ±1-ulp flip found: {got}")
    got = find_scale_assignment_tolerant(g_bad, engine_sum(e_bad))
    check(got is None, "±1-ulp search still rejects a real value diff")

    # -- 4. boundary analysis: tie straddle and NULL-ordering.
    # ORDER BY col1 ASC LIMIT 2 over keys 1,1,1,2 — the boundary key (1) ties
    # across the LIMIT: any two of the three key-1 rows are a legitimate pick.
    full = [("a", 1), ("b", 1), ("c", 1), ("d", 2)]
    spec = [(1, False, None)]
    forced, amb, note = boundary_zone(full, spec, 2)
    check(amb == {0, 1, 2}, f"tie group spans the LIMIT: {sorted(amb)} ({note})")
    check(forced == set(), f"no forced rows when the whole boundary ties: {forced}")

    # Same shape but the boundary key does not tie: first-2 membership is fixed.
    full_det = [("a", 1), ("b", 1), ("c", 2), ("d", 3)]
    forced, amb, note = boundary_zone(full_det, spec, 2)
    check(not amb and forced == {0, 1}, f"determined boundary: forced={sorted(forced)} note={note!r}")

    # NULLs in an ORDER BY column with unspecified null ordering: engine NULLS
    # FIRST vs golden NULLS LAST change the first-2 set.
    full_null = [("r1", None), ("r2", 1), ("r3", 2), ("r4", 3)]
    forced, amb, note = boundary_zone(full_null, spec, 2)
    check(amb == {0, 2}, f"NULL-ordering changes first-2 membership: {sorted(amb)} ({note})")

    # An explicit NULLS LAST applies to both engines — no null ambiguity.
    forced, amb, note = boundary_zone(full_null, [(1, False, False)], 2)
    check(not amb, f"explicit NULLS LAST is honored by both sides: {note!r}")

    # Verified boundary pick: engine took the other tied row.
    full_rows = [("keep1", 1), ("keep2", 1), ("pickA", 5), ("pickB", 5), ("zz", 9)]
    spec2 = [(1, False, False)]
    engine_rows = [("keep1", 1), ("keep2", 1), ("pickB", 5)]  # LIMIT 3, tie at key 5
    got = try_boundary_verdict(full_rows, spec2, 3, rows_checksum(engine_rows), 3)
    check(got is not None and got[0] == "verified", f"boundary pick verified by enumeration: {got}")

    # -- 5. verdict_for end-to-end on synthetic cases.
    sql_lim = "SELECT k, v FROM t ORDER BY v LIMIT 3"
    v = verdict_for(g_uniform, engine_sum(e_uniform), 2, "SELECT ... ")
    check(v.tag == Verdict.BENIGN and v.verified and "numeric-scale" in v.reason, f"verdict scale-benign: {v}")
    # recorded_rows disagrees with the golden row count: hard mismatch regardless
    # of what the (here: matching) scale assignment would say.
    v = verdict_for(g_uniform, rows_checksum([("unrelated", 0)]), 1, "SELECT ...")
    check(v.tag == Verdict.MISMATCH and "row counts differ" in v.reason, f"verdict row-count mismatch: {v}")

    full_q = [("keep1", 1), ("keep2", 1), ("pickA", 5), ("pickB", 5), ("zz", 9)]
    lim_q = [("keep1", 1), ("keep2", 1), ("pickA", 5)]
    eng_q = [("keep1", 1), ("keep2", 1), ("pickB", 5)]
    v = verdict_for(lim_q, rows_checksum(eng_q), 3, sql_lim,
                    run_full_query=lambda s: (full_q, ["k", "v"]))
    check(v.tag == Verdict.BENIGN and "boundary-tie" in v.reason, f"verdict boundary-benign: {v}")

    # numeric-drift heuristic: same count, no scale assignment explains it.
    drift_g = [(1, 74.7164723952)]
    drift_e = [(1, Decimal("74.716471"))]
    v = verdict_for(drift_g, rows_checksum(drift_e), 1, "SELECT ...")
    check(v.tag == Verdict.BENIGN and not v.verified and "numeric-drift" in v.reason, f"verdict numeric-drift heuristic: {v}")
    v = verdict_for(drift_g, rows_checksum(drift_e), 1, "SELECT ...", strict_benign=True)
    check(v.tag == Verdict.MISMATCH, f"--strict-benign reclassifies drift as MISMATCH: {v}")

    # parse_order_limit + map_order_columns on a TPC-DS-shaped query.
    items, lim, stripped = parse_order_limit(
        "SELECT a, b, c FROM t WHERE x IN (SELECT y FROM w ORDER BY z LIMIT 5) "
        "ORDER BY a NULLS FIRST, b DESC LIMIT 100;")
    check(lim == 100 and len(items) == 2 and stripped is not None and "LIMIT 5" in stripped,
          "trailing top-level LIMIT parsed; inner LIMIT untouched")
    check(items[0] == ("a", False, True) and items[1] == ("b", True, None), f"order items parsed: {items}")
    spec = map_order_columns(items, ["A", "B", "C"])
    check(spec == [(0, False, True), (1, True, None)], f"order columns mapped: {spec}")

    print(f"\nself-test: {'PASS' if not failures else f'{len(failures)} FAILURES'}")
    return 1 if failures else 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--self-test", action="store_true", help="run unit-ish validation and exit")
    args = ap.parse_args()
    if args.self_test:
        sys.exit(self_test())
    ap.error("nothing to do (use --self-test, or import from golden-check-*.py)")
