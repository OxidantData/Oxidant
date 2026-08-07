//! Real TPC-H data generation via the pure-Rust [`tpchgen`] crate, written as CSV files that both
//! oxidant (DataFusion) and the DuckDB oracle read. CSV (not the crate's Arrow path) keeps oxidant on its
//! own pinned Arrow — no cross-crate Arrow-version coupling.

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use tpchgen::csv::{
    CustomerCsv, LineItemCsv, NationCsv, OrderCsv, PartCsv, PartSuppCsv, RegionCsv, SupplierCsv,
};
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};

/// The eight TPC-H table names (data files are `<name>.csv` under the data dir).
pub const TABLES: [&str; 8] = [
    "nation", "region", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

/// Generate scale-factor `sf` TPC-H data as CSV (with headers) under `dir`. Idempotent: if
/// `lineitem.csv` already exists the generation is skipped (so reruns are cheap).
pub fn generate(sf: f64, dir: &Path) -> std::io::Result<()> {
    generate_prefixed(sf, dir, "")
}

/// Like [`generate`], but each file is named `<prefix><name>.csv` — the committed
/// `sample-data/` tree carries the `tpch_` table-name prefix its SQL-visible tables use.
pub fn generate_prefixed(sf: f64, dir: &Path, prefix: &str) -> std::io::Result<()> {
    fs::create_dir_all(dir)?;
    if dir.join(format!("{prefix}lineitem.csv")).exists() {
        return Ok(());
    }
    // Each table: write its header, then every generated row via the CSV formatter. `part=1,
    // part_count=1` generates the whole table in one shot.
    write_csv(dir, &format!("{prefix}nation"), NationCsv::header(), || {
        NationGenerator::new(sf, 1, 1)
            .into_iter()
            .map(NationCsv::new)
    })?;
    write_csv(dir, &format!("{prefix}region"), RegionCsv::header(), || {
        RegionGenerator::new(sf, 1, 1)
            .into_iter()
            .map(RegionCsv::new)
    })?;
    write_csv(
        dir,
        &format!("{prefix}supplier"),
        SupplierCsv::header(),
        || {
            SupplierGenerator::new(sf, 1, 1)
                .into_iter()
                .map(SupplierCsv::new)
        },
    )?;
    write_csv(
        dir,
        &format!("{prefix}customer"),
        CustomerCsv::header(),
        || {
            CustomerGenerator::new(sf, 1, 1)
                .into_iter()
                .map(CustomerCsv::new)
        },
    )?;
    write_csv(dir, &format!("{prefix}part"), PartCsv::header(), || {
        PartGenerator::new(sf, 1, 1).into_iter().map(PartCsv::new)
    })?;
    write_csv(
        dir,
        &format!("{prefix}partsupp"),
        PartSuppCsv::header(),
        || {
            PartSuppGenerator::new(sf, 1, 1)
                .into_iter()
                .map(PartSuppCsv::new)
        },
    )?;
    write_csv(dir, &format!("{prefix}orders"), OrderCsv::header(), || {
        OrderGenerator::new(sf, 1, 1).into_iter().map(OrderCsv::new)
    })?;
    write_csv(
        dir,
        &format!("{prefix}lineitem"),
        LineItemCsv::header(),
        || {
            LineItemGenerator::new(sf, 1, 1)
                .into_iter()
                .map(LineItemCsv::new)
        },
    )?;
    Ok(())
}

/// Write `<name>.csv` under `dir`: the header line, then every formatted row from `rows()`.
fn write_csv<I, R>(
    dir: &Path,
    name: &str,
    header: &str,
    rows: impl Fn() -> I,
) -> std::io::Result<()>
where
    I: Iterator<Item = R>,
    R: std::fmt::Display,
{
    let mut f = BufWriter::new(File::create(dir.join(format!("{name}.csv")))?);
    writeln!(f, "{header}")?;
    for row in rows() {
        writeln!(f, "{row}")?;
    }
    f.flush()
}

fn i64f(name: &str) -> Field {
    Field::new(name, DataType::Int64, false)
}
fn i32f(name: &str) -> Field {
    Field::new(name, DataType::Int32, false)
}
/// TPC-H money/quantity columns are `DECIMAL(15,2)` — exact, so aggregates are deterministic
/// (Q15 filters on exact equality of a summed value, which float64 makes non-deterministic).
fn decf(name: &str) -> Field {
    Field::new(name, DataType::Decimal128(15, 2), false)
}
fn strf(name: &str) -> Field {
    Field::new(name, DataType::Utf8, false)
}
fn datef(name: &str) -> Field {
    Field::new(name, DataType::Date32, false)
}

/// Explicit Arrow schema for `table` (so the CSV reader gets dates/decimals/keys right rather than
/// inferring). Money/quantity columns are `Decimal128(15,2)` per the TPC-H spec — exact arithmetic,
/// so equality-on-aggregate queries (Q15) are deterministic.
pub fn schema(table: &str) -> SchemaRef {
    let fields = match table {
        "nation" => vec![
            i64f("n_nationkey"),
            strf("n_name"),
            i64f("n_regionkey"),
            strf("n_comment"),
        ],
        "region" => vec![i64f("r_regionkey"), strf("r_name"), strf("r_comment")],
        "supplier" => vec![
            i64f("s_suppkey"),
            strf("s_name"),
            strf("s_address"),
            i64f("s_nationkey"),
            strf("s_phone"),
            decf("s_acctbal"),
            strf("s_comment"),
        ],
        "customer" => vec![
            i64f("c_custkey"),
            strf("c_name"),
            strf("c_address"),
            i64f("c_nationkey"),
            strf("c_phone"),
            decf("c_acctbal"),
            strf("c_mktsegment"),
            strf("c_comment"),
        ],
        "part" => vec![
            i64f("p_partkey"),
            strf("p_name"),
            strf("p_mfgr"),
            strf("p_brand"),
            strf("p_type"),
            i32f("p_size"),
            strf("p_container"),
            decf("p_retailprice"),
            strf("p_comment"),
        ],
        "partsupp" => vec![
            i64f("ps_partkey"),
            i64f("ps_suppkey"),
            i32f("ps_availqty"),
            decf("ps_supplycost"),
            strf("ps_comment"),
        ],
        "orders" => vec![
            i64f("o_orderkey"),
            i64f("o_custkey"),
            strf("o_orderstatus"),
            decf("o_totalprice"),
            datef("o_orderdate"),
            strf("o_orderpriority"),
            strf("o_clerk"),
            i32f("o_shippriority"),
            strf("o_comment"),
        ],
        "lineitem" => vec![
            i64f("l_orderkey"),
            i64f("l_partkey"),
            i64f("l_suppkey"),
            i32f("l_linenumber"),
            decf("l_quantity"),
            decf("l_extendedprice"),
            decf("l_discount"),
            decf("l_tax"),
            strf("l_returnflag"),
            strf("l_linestatus"),
            datef("l_shipdate"),
            datef("l_commitdate"),
            datef("l_receiptdate"),
            strf("l_shipinstruct"),
            strf("l_shipmode"),
            strf("l_comment"),
        ],
        other => panic!("unknown TPC-H table `{other}`"),
    };
    Arc::new(Schema::new(fields))
}
