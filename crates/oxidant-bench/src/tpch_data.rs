//! TPC-H data via official TPC `dbgen` (not the `tpchgen` crate / DuckDB).
//!
//! `dbgen` emits pipe-delimited `.tbl` files; we convert them to headered CSV so the existing
//! harness and DuckDB oracle paths keep working. Idempotent when `lineitem.csv` exists.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::tpc_kits;

/// The eight TPC-H table names (data files are `<name>.csv` under the data dir).
pub const TABLES: [&str; 8] = [
    "nation", "region", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

const HEADERS: &[(&str, &[&str])] = &[
    (
        "nation",
        &["n_nationkey", "n_name", "n_regionkey", "n_comment"],
    ),
    ("region", &["r_regionkey", "r_name", "r_comment"]),
    (
        "supplier",
        &[
            "s_suppkey",
            "s_name",
            "s_address",
            "s_nationkey",
            "s_phone",
            "s_acctbal",
            "s_comment",
        ],
    ),
    (
        "customer",
        &[
            "c_custkey",
            "c_name",
            "c_address",
            "c_nationkey",
            "c_phone",
            "c_acctbal",
            "c_mktsegment",
            "c_comment",
        ],
    ),
    (
        "part",
        &[
            "p_partkey",
            "p_name",
            "p_mfgr",
            "p_brand",
            "p_type",
            "p_size",
            "p_container",
            "p_retailprice",
            "p_comment",
        ],
    ),
    (
        "partsupp",
        &[
            "ps_partkey",
            "ps_suppkey",
            "ps_availqty",
            "ps_supplycost",
            "ps_comment",
        ],
    ),
    (
        "orders",
        &[
            "o_orderkey",
            "o_custkey",
            "o_orderstatus",
            "o_totalprice",
            "o_orderdate",
            "o_orderpriority",
            "o_clerk",
            "o_shippriority",
            "o_comment",
        ],
    ),
    (
        "lineitem",
        &[
            "l_orderkey",
            "l_partkey",
            "l_suppkey",
            "l_linenumber",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
            "l_commitdate",
            "l_receiptdate",
            "l_shipinstruct",
            "l_shipmode",
            "l_comment",
        ],
    ),
];

/// Generate scale-factor `sf` TPC-H data as CSV (with headers) under `dir`.
pub fn generate(sf: f64, dir: &Path) -> std::io::Result<()> {
    generate_prefixed(sf, dir, "")
}

/// Like [`generate`], but each file is named `<prefix><name>.csv`.
pub fn generate_prefixed(sf: f64, dir: &Path, prefix: &str) -> std::io::Result<()> {
    fs::create_dir_all(dir)?;
    if dir.join(format!("{prefix}lineitem.csv")).exists() {
        return Ok(());
    }

    let raw = dir.join(".dbgen-raw");
    if raw.exists() {
        let _ = fs::remove_dir_all(&raw);
    }
    fs::create_dir_all(&raw)?;
    eprintln!(
        "[tpch-data] official dbgen SF{sf} → {} (then CSV)",
        raw.display()
    );
    tpc_kits::run_dbgen(sf, &raw)?;

    for (name, headers) in HEADERS {
        let tbl = raw.join(format!("{name}.tbl"));
        if !tbl.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("dbgen did not produce {}", tbl.display()),
            ));
        }
        let csv = dir.join(format!("{prefix}{name}.csv"));
        tpc_kits::pipe_tbl_to_csv(&tbl, &csv, headers)?;
    }
    let _ = fs::remove_dir_all(&raw);
    Ok(())
}

fn i64f(name: &str) -> Field {
    Field::new(name, DataType::Int64, false)
}
fn i32f(name: &str) -> Field {
    Field::new(name, DataType::Int32, false)
}
fn decf(name: &str) -> Field {
    Field::new(name, DataType::Decimal128(15, 2), false)
}
fn strf(name: &str) -> Field {
    Field::new(name, DataType::Utf8, false)
}
fn datef(name: &str) -> Field {
    Field::new(name, DataType::Date32, false)
}

/// Explicit Arrow schema for `table` (CSV reader gets dates/decimals/keys right).
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
