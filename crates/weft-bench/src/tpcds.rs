//! TPC-DS harness: register Parquet (or a DuckDB `.db` via prepare.sh export) and run the
//! official 99 queries with ClickBench-style hot timing. Optionally times DuckDB on the same
//! data as a CPU baseline for the site.

use std::path::Path;

use weft_loom::Engine;

use crate::suite::{self, Query};

/// The 24 TPC-DS tables (DuckDB `dsdgen` / official schema).
pub const TABLES: [&str; 24] = [
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
];

pub(crate) fn queries() -> Vec<(&'static str, &'static str)> {
    macro_rules! q {
        ($n:literal) => {
            (
                concat!("Q", $n),
                include_str!(concat!("../../../bench/tpcds/queries/q", $n, ".sql")),
            )
        };
    }
    vec![
        q!(1),
        q!(2),
        q!(3),
        q!(4),
        q!(5),
        q!(6),
        q!(7),
        q!(8),
        q!(9),
        q!(10),
        q!(11),
        q!(12),
        q!(13),
        q!(14),
        q!(15),
        q!(16),
        q!(17),
        q!(18),
        q!(19),
        q!(20),
        q!(21),
        q!(22),
        q!(23),
        q!(24),
        q!(25),
        q!(26),
        q!(27),
        q!(28),
        q!(29),
        q!(30),
        q!(31),
        q!(32),
        q!(33),
        q!(34),
        q!(35),
        q!(36),
        q!(37),
        q!(38),
        q!(39),
        q!(40),
        q!(41),
        q!(42),
        q!(43),
        q!(44),
        q!(45),
        q!(46),
        q!(47),
        q!(48),
        q!(49),
        q!(50),
        q!(51),
        q!(52),
        q!(53),
        q!(54),
        q!(55),
        q!(56),
        q!(57),
        q!(58),
        q!(59),
        q!(60),
        q!(61),
        q!(62),
        q!(63),
        q!(64),
        q!(65),
        q!(66),
        q!(67),
        q!(68),
        q!(69),
        q!(70),
        q!(71),
        q!(72),
        q!(73),
        q!(74),
        q!(75),
        q!(76),
        q!(77),
        q!(78),
        q!(79),
        q!(80),
        q!(81),
        q!(82),
        q!(83),
        q!(84),
        q!(85),
        q!(86),
        q!(87),
        q!(88),
        q!(89),
        q!(90),
        q!(91),
        q!(92),
        q!(93),
        q!(94),
        q!(95),
        q!(96),
        q!(97),
        q!(98),
        q!(99),
    ]
}

/// Register every TPC-DS Parquet table under `dir` (`<table>.parquet`).
pub async fn register_parquet(engine: &Engine, dir: &Path) {
    for t in TABLES {
        let path = dir.join(format!("{t}.parquet"));
        if !path.exists() {
            // DuckDB EXPORT sometimes nests; also accept `dir/t/t.parquet` or `dir/t/*.parquet`.
            let alt = dir.join(t);
            if alt.is_dir() {
                engine
                    .register_parquet(t, alt.to_str().unwrap())
                    .await
                    .unwrap_or_else(|e| panic!("register {t}: {e}"));
                continue;
            }
            panic!(
                "missing TPC-DS table data for `{t}` under {} — run bench/tpcds/prepare.sh first",
                dir.display()
            );
        }
        engine
            .register_parquet(t, path.to_str().unwrap())
            .await
            .unwrap_or_else(|e| panic!("register {t}: {e}"));
    }
}

pub struct RunOpts<'a> {
    pub sf: f64,
    pub data: &'a Path,
    /// DuckDB `.db` for the DuckDB baseline (optional; falls back to parquet dir).
    pub duckdb_db: Option<&'a Path>,
    pub out_json: &'a Path,
    pub machine: &'a str,
    pub run_date: &'a str,
    pub with_duckdb: bool,
}

pub async fn run(opts: RunOpts<'_>) {
    eprintln!(
        "[tpcds] SF{} data={} → {}",
        opts.sf,
        opts.data.display(),
        opts.out_json.display()
    );

    let engine = Engine::new();
    register_parquet(&engine, opts.data).await;

    let qs: Vec<Query<'_>> = queries()
        .iter()
        .map(|(n, s)| Query { name: n, sql: s })
        .collect();

    eprintln!("[tpcds] running Weft ({} queries × 3 tries) …", qs.len());
    let weft = suite::run_weft(&engine, &qs).await;

    let mut engines = vec![weft];
    if opts.with_duckdb {
        if let Some(duck) = suite::duckdb_path() {
            let duck_data = opts.duckdb_db.unwrap_or(opts.data);
            eprintln!("[tpcds] running DuckDB baseline on {} …", duck_data.display());
            engines.push(suite::run_duckdb(&duck, duck_data, &qs));
        } else {
            eprintln!("[tpcds] DuckDB not on PATH — skipping baseline");
        }
    }

    let dataset = format!("TPC-DS SF{} (DuckDB dsdgen / blobs.duckdb.org)", opts.sf);
    suite::write_site_json(
        opts.out_json,
        &dataset,
        opts.machine,
        opts.run_date,
        "engine-direct Parquet, 3 tries/query, hot = min(try2, try3)",
        qs.len(),
        &engines,
    )
    .expect("write tpcds results json");

    let failed = engines[0].failures;
    let hot = engines[0].total().unwrap_or(0.0);
    eprintln!("\n=== TPC-DS sf{}: weft hot total {hot:.4}s, {failed} failure(s) ===", opts.sf);
}
