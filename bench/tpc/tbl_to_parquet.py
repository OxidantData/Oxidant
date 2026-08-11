#!/usr/bin/env python3
"""Convert official TPC flat files (.tbl / .dat) to Snappy Parquet directories.

Writes **multiple part files** sized for distributed scans (Spark-like). Apache Spark
splits a *single* Parquet file across tasks using ``spark.sql.files.maxPartitionBytes``
(default 128 MiB) at **row-group** granularity; Oxidant's worker sharding is
**file-level**, so one giant ``store_sales.parquet`` leaves all but one worker idle.
Target ~128 MiB parts (override with ``--target-part-bytes``).

TPC-H:  pipe-separated, trailing delimiter, no header.
TPC-DS: pipe-separated, trailing delimiter, no header.

Usage:
  ./bench/tpc/tbl_to_parquet.py --suite tpch --raw /data/tpch-sf100/raw --out /data/tpch-sf100/parquet
  ./bench/tpc/tbl_to_parquet.py --suite tpcds --raw /data/tpcds-sf100/raw --out /data/tpcds-sf100/parquet
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

try:
    import pyarrow as pa
    import pyarrow.csv as pacsv
    import pyarrow.parquet as pq
except ImportError as e:
    sys.stderr.write(
        "tbl_to_parquet.py needs pyarrow — pip install 'pyarrow>=14'\n"
        f"import error: {e}\n"
    )
    sys.exit(1)

# Spark's default spark.sql.files.maxPartitionBytes — good part size for file sharding.
DEFAULT_TARGET_PART_BYTES = 128 * 1024 * 1024

# TPC-H schemas (spec types → Arrow). Money columns are decimal(15,2).
TPCH_SCHEMAS: dict[str, pa.Schema] = {
    "nation": pa.schema(
        [
            ("n_nationkey", pa.int64()),
            ("n_name", pa.string()),
            ("n_regionkey", pa.int64()),
            ("n_comment", pa.string()),
        ]
    ),
    "region": pa.schema(
        [
            ("r_regionkey", pa.int64()),
            ("r_name", pa.string()),
            ("r_comment", pa.string()),
        ]
    ),
    "supplier": pa.schema(
        [
            ("s_suppkey", pa.int64()),
            ("s_name", pa.string()),
            ("s_address", pa.string()),
            ("s_nationkey", pa.int64()),
            ("s_phone", pa.string()),
            ("s_acctbal", pa.decimal128(15, 2)),
            ("s_comment", pa.string()),
        ]
    ),
    "customer": pa.schema(
        [
            ("c_custkey", pa.int64()),
            ("c_name", pa.string()),
            ("c_address", pa.string()),
            ("c_nationkey", pa.int64()),
            ("c_phone", pa.string()),
            ("c_acctbal", pa.decimal128(15, 2)),
            ("c_mktsegment", pa.string()),
            ("c_comment", pa.string()),
        ]
    ),
    "part": pa.schema(
        [
            ("p_partkey", pa.int64()),
            ("p_name", pa.string()),
            ("p_mfgr", pa.string()),
            ("p_brand", pa.string()),
            ("p_type", pa.string()),
            ("p_size", pa.int32()),
            ("p_container", pa.string()),
            ("p_retailprice", pa.decimal128(15, 2)),
            ("p_comment", pa.string()),
        ]
    ),
    "partsupp": pa.schema(
        [
            ("ps_partkey", pa.int64()),
            ("ps_suppkey", pa.int64()),
            ("ps_availqty", pa.int32()),
            ("ps_supplycost", pa.decimal128(15, 2)),
            ("ps_comment", pa.string()),
        ]
    ),
    "orders": pa.schema(
        [
            ("o_orderkey", pa.int64()),
            ("o_custkey", pa.int64()),
            ("o_orderstatus", pa.string()),
            ("o_totalprice", pa.decimal128(15, 2)),
            ("o_orderdate", pa.date32()),
            ("o_orderpriority", pa.string()),
            ("o_clerk", pa.string()),
            ("o_shippriority", pa.int32()),
            ("o_comment", pa.string()),
        ]
    ),
    "lineitem": pa.schema(
        [
            ("l_orderkey", pa.int64()),
            ("l_partkey", pa.int64()),
            ("l_suppkey", pa.int64()),
            ("l_linenumber", pa.int32()),
            ("l_quantity", pa.decimal128(15, 2)),
            ("l_extendedprice", pa.decimal128(15, 2)),
            ("l_discount", pa.decimal128(15, 2)),
            ("l_tax", pa.decimal128(15, 2)),
            ("l_returnflag", pa.string()),
            ("l_linestatus", pa.string()),
            ("l_shipdate", pa.date32()),
            ("l_commitdate", pa.date32()),
            ("l_receiptdate", pa.date32()),
            ("l_shipinstruct", pa.string()),
            ("l_shipmode", pa.string()),
            ("l_comment", pa.string()),
        ]
    ),
}


def _part_path(dest_dir: Path, part_idx: int) -> Path:
    return dest_dir / f"part-{part_idx:05d}.parquet"


_SQL_TYPE_RE = re.compile(
    r"^(integer|int|bigint|smallint|boolean|date|time|char|varchar|decimal)\b"
    r"(?:\((\d+)(?:,(\d+))?\))?$",
    re.I,
)


def sql_type_to_arrow(sql_type: str) -> pa.DataType:
    """Map TPC-DS ``tpcds.sql`` types to Arrow (integers → int32, money → decimal)."""
    m = _SQL_TYPE_RE.match(sql_type.strip())
    if not m:
        return pa.string()
    base = m.group(1).lower()
    if base in ("integer", "int", "smallint"):
        return pa.int32()
    if base == "bigint":
        return pa.int64()
    if base == "boolean":
        return pa.bool_()
    if base == "date":
        return pa.date32()
    if base == "decimal":
        precision = int(m.group(2) or 38)
        scale = int(m.group(3) or 0)
        return pa.decimal128(precision, scale)
    # char / varchar / time (store as string; dsdgen emits text)
    return pa.string()


def load_tpcds_schemas(cols_path: Path, types_path: Path) -> dict[str, pa.Schema]:
    """Build Arrow schemas: column *names* from ``tpcds_columns.tsv``, *types* by
    position from ``tpcds_types.tsv`` (official ``tpcds.sql``). Names can diverge
    slightly from the SQL DDL (e.g. ``c_last_review_date_sk``).
    """
    sql_types: dict[str, list[str]] = {}
    for line in types_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        table, specs = line.split("\t", 1)
        # Fields are `|`-separated so `decimal(7,2)` commas stay inside one token.
        sql_types[table] = [spec.split(":", 1)[1] for spec in specs.split("|")]

    schemas: dict[str, pa.Schema] = {}
    for line in cols_path.read_text(encoding="utf-8").splitlines():
        line = line.strip().strip('"')
        if not line:
            continue
        table, cols = line.split("\t", 1)
        names = cols.split(",")
        types = sql_types.get(table)
        if types is None:
            fields = [(n, pa.string()) for n in names]
        elif len(types) != len(names):
            raise SystemExit(
                f"tpcds type/name count mismatch for `{table}`: "
                f"{len(names)} names vs {len(types)} types"
            )
        else:
            fields = [(n, sql_type_to_arrow(t)) for n, t in zip(names, types)]
        schemas[table] = pa.schema(fields)
    return schemas


def _convert_one(
    src: Path,
    dest_dir: Path,
    schema: pa.Schema | None,
    row_group: int,
    target_part_bytes: int,
    start_part: int = 0,
) -> int:
    """Convert one flat file into one or more part-*.parquet files.

    Returns the next unused part index.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)

    parse_opts = pacsv.ParseOptions(delimiter="|", quote_char=False)

    with src.open("rb") as f:
        first = f.readline()
    trailing = first.rstrip(b"\r\n").endswith(b"|")

    if schema is not None:
        names = list(schema.names) + (["_trail"] if trailing else [])
        # TPC kits emit latin-1 bytes in comment columns; utf-8 decode fails mid-file.
        read_opts = pacsv.ReadOptions(column_names=names, encoding="latin1")
        col_types = {f.name: f.type for f in schema}
        if trailing:
            col_types["_trail"] = pa.string()
        convert_opts = pacsv.ConvertOptions(
            column_types=col_types,
            include_columns=list(schema.names),
            strings_can_be_null=True,
            null_values=[""],
        )
    else:
        nfields = first.count(b"|")
        if trailing:
            names = [f"c{i}" for i in range(nfields)] + ["_trail"]
            read_opts = pacsv.ReadOptions(column_names=names, encoding="latin1")
            convert_opts = pacsv.ConvertOptions(
                include_columns=names[:-1],
                strings_can_be_null=True,
                null_values=[""],
            )
        else:
            read_opts = pacsv.ReadOptions(
                autogenerate_column_names=True, encoding="latin1"
            )
            convert_opts = pacsv.ConvertOptions()

    part_idx = start_part
    out_file = _part_path(dest_dir, part_idx)
    print(f"[parquet] {src.name} -> {out_file.name}+ (target ~{target_part_bytes // (1024*1024)} MiB)")
    writer: pq.ParquetWriter | None = None
    bytes_in_part = 0
    try:
        with pacsv.open_csv(
            src,
            read_options=read_opts,
            parse_options=parse_opts,
            convert_options=convert_opts,
        ) as reader:
            for batch in reader:
                table = pa.Table.from_batches([batch])
                if schema is not None:
                    table = table.cast(schema, safe=False)
                approx = table.nbytes
                if (
                    writer is not None
                    and target_part_bytes > 0
                    and bytes_in_part > 0
                    and bytes_in_part + approx >= target_part_bytes
                ):
                    writer.close()
                    writer = None
                    part_idx += 1
                    out_file = _part_path(dest_dir, part_idx)
                    bytes_in_part = 0
                    print(f"[parquet]   rotate -> {out_file.name}")
                if writer is None:
                    writer = pq.ParquetWriter(
                        out_file, table.schema, compression="snappy"
                    )
                writer.write_table(table, row_group_size=row_group)
                bytes_in_part += approx
    finally:
        if writer is not None:
            writer.close()
    if not out_file.exists() or out_file.stat().st_size == 0:
        raise SystemExit(f"failed to write {out_file}")
    return part_idx + 1


def _tpch_sources(raw: Path, name: str) -> list[Path]:
    """Prefer parallel dbgen parts (name.tbl.1 …) over a single concatenated name.tbl."""
    parts = sorted(
        raw.glob(f"{name}.tbl.[0-9]*"),
        key=lambda p: int(p.suffix.lstrip(".") or "0"),
    )
    if parts:
        return parts
    src = raw / f"{name}.tbl"
    if src.exists():
        return [src]
    return []


def convert_tpch(raw: Path, out: Path, row_group: int, target_part_bytes: int) -> None:
    for name, schema in TPCH_SCHEMAS.items():
        sources = _tpch_sources(raw, name)
        if not sources:
            raise SystemExit(f"missing {raw / (name + '.tbl')} (and no .tbl.N parts)")
        dest = out / name
        existing = sorted(dest.glob("part-*.parquet"))
        if existing and all(p.stat().st_size > 0 for p in existing):
            print(f"[parquet] skip {name} ({len(existing)} parts exist)")
            continue
        part = 0
        for src in sources:
            part = _convert_one(src, dest, schema, row_group, target_part_bytes, start_part=part)


_TPCDS_PART_RE = re.compile(r"^(.+)_(\d+)_(\d+)\.dat$")


def _tpcds_sources(raw: Path) -> dict[str, list[Path]]:
    """Group TPC-DS .dat files by table; prefer parallel CHILD parts over concatenated."""
    by_table: dict[str, list[Path]] = {}
    parallel: dict[str, list[Path]] = {}
    for src in sorted(raw.glob("*.dat")):
        m = _TPCDS_PART_RE.match(src.name)
        if m:
            name = m.group(1)
            if name == "dbgen_version":
                continue
            parallel.setdefault(name, []).append(src)
            continue
        name = src.stem
        if name == "dbgen_version":
            continue
        by_table.setdefault(name, []).append(src)
    # Parallel parts win when present (do not also convert concatenated .dat).
    for name, parts in parallel.items():
        parts.sort(key=lambda p: int(_TPCDS_PART_RE.match(p.name).group(2)))  # type: ignore[union-attr]
        by_table[name] = parts
    return by_table


def convert_tpcds(
    raw: Path,
    out: Path,
    row_group: int,
    target_part_bytes: int,
    *,
    force: bool = False,
    only: set[str] | None = None,
) -> None:
    here = Path(__file__).resolve().parent
    cols_path = here / "tpcds_columns.tsv"
    types_path = here / "tpcds_types.tsv"
    if not types_path.is_file():
        raise SystemExit(
            f"missing {types_path} — regenerate from kits/tpcds-kit/tools/tpcds.sql"
        )
    schemas = load_tpcds_schemas(cols_path, types_path)

    by_table = _tpcds_sources(raw)
    if not by_table:
        raise SystemExit(f"no .dat files under {raw}")
    for name, sources in sorted(by_table.items()):
        if name == "dbgen_version":
            continue
        if only is not None and name not in only:
            continue
        schema = schemas.get(name)
        if schema is None:
            print(f"[parquet] skip unknown table `{name}` (not in {cols_path.name})")
            continue
        dest = out / name
        existing = sorted(dest.glob("part-*.parquet"))
        if existing and all(p.stat().st_size > 0 for p in existing) and not force:
            print(f"[parquet] skip {name} ({len(existing)} parts exist)")
            continue
        if force and dest.exists():
            import shutil

            shutil.rmtree(dest)
        part = 0
        for src in sources:
            part = _convert_one(src, dest, schema, row_group, target_part_bytes, start_part=part)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite", choices=("tpch", "tpcds"), required=True)
    ap.add_argument("--raw", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--row-group", type=int, default=128 * 1024)
    ap.add_argument(
        "--target-part-bytes",
        type=int,
        default=DEFAULT_TARGET_PART_BYTES,
        help="Rotate to a new part-NNNNN.parquet after ~this many uncompressed Arrow bytes "
        f"(default {DEFAULT_TARGET_PART_BYTES} = Spark maxPartitionBytes). 0 = one part per source file.",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Rebuild Parquet even when part-*.parquet already exist",
    )
    ap.add_argument(
        "--only",
        default="",
        help="Comma-separated table names to convert (TPC-DS only)",
    )
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    if args.suite == "tpch":
        convert_tpch(args.raw, args.out, args.row_group, args.target_part_bytes)
    else:
        only = {t.strip() for t in args.only.split(",") if t.strip()} or None
        convert_tpcds(
            args.raw,
            args.out,
            args.row_group,
            args.target_part_bytes,
            force=args.force,
            only=only,
        )
    print(f"[parquet] done: {args.out}")


if __name__ == "__main__":
    main()
