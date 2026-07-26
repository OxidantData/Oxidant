//! The `weft` command-line entry point.
//!
//! ```text
//! weft spark server --port 50051         # Spark Connect server; point PySpark at sc://host:50051
//! weft worker --port 50561 [--data hits.parquet --table t]   # a distributed Flight worker
//! weft driver --workers h:p,h:p \         # orchestrate a 2-stage distributed aggregation
//!   --partial-sql "SELECT k, COUNT(*) c, SUM(v) s FROM t GROUP BY k" \
//!   --final-sql   "SELECT k, SUM(c) c, SUM(s) s FROM shuffle_input GROUP BY k" \
//!   --hash-keys 0
//! ```

use weft_connect::{serve, ServerConfig};
use weft_execution::driver::{run_distributed, Cluster, DistributedPlan};
use weft_execution::flight::serve_worker;

#[tokio::main]
async fn main() {
    // TODO(issue #1): replace this hand-rolled arg handling with clap.
    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).map(String::as_str);

    let result = match cmd {
        Some("worker") => run_worker(&args).await,
        Some("driver") => run_driver(&args).await,
        // `weft spark server ...` (and the bare `server` alias) keep the Spark Connect path.
        _ if args.iter().any(|a| a == "server") => run_server(&args).await,
        _ => {
            usage();
            return;
        }
    };
    if let Err(e) = result {
        eprintln!("weft: {e}");
        std::process::exit(1);
    }
}

fn usage() {
    eprintln!("weft {}", env!("CARGO_PKG_VERSION"));
    eprintln!("usage:");
    eprintln!("  weft spark server --port <PORT>");
    eprintln!("  weft worker --port <PORT> [--data <parquet> --table <name>]");
    eprintln!(
        "  weft driver --workers <h:p,h:p> --partial-sql <SQL> --final-sql <SQL> --hash-keys <c,c>"
    );
}

async fn run_server(args: &[String]) -> weft_common::Result<()> {
    let port = flag(args, "--port")
        .and_then(|s| s.parse().ok())
        .unwrap_or(50051);
    let catalogs = catalog_conf(args);
    if !catalogs.is_empty() {
        eprintln!("Declared {} catalog config entrie(s)", catalogs.len());
    }
    eprintln!("Weft Spark Connect server listening on sc://0.0.0.0:{port}");
    serve(ServerConfig { port, catalogs }).await
}

/// Collect startup catalog config from repeated `--catalog-conf key=value` flags and the
/// `WEFT_CATALOG_CONF` env var (`;`-separated `key=value`). Keys are full Spark config keys, e.g.
/// `spark.sql.catalog.prod.type=hive`. Example:
///   weft spark server --catalog-conf spark.sql.catalog.prod.type=hive \
///                     --catalog-conf spark.sql.catalog.prod.uri=thrift://hms:9083
fn catalog_conf(args: &[String]) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    let mut insert_kv = |kv: &str| {
        if let Some((k, v)) = kv.split_once('=') {
            out.insert(k.trim().to_string(), v.trim().to_string());
        }
    };
    if let Ok(env) = std::env::var("WEFT_CATALOG_CONF") {
        for kv in env.split(';').filter(|s| !s.trim().is_empty()) {
            insert_kv(kv);
        }
    }
    for (i, a) in args.iter().enumerate() {
        if a == "--catalog-conf" {
            if let Some(kv) = args.get(i + 1) {
                insert_kv(kv);
            }
        } else if let Some(kv) = a.strip_prefix("--catalog-conf=") {
            insert_kv(kv);
        }
    }
    out
}

async fn run_worker(args: &[String]) -> weft_common::Result<()> {
    let port: u16 = flag(args, "--port")
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| weft_common::Error::Io("worker requires --port".into()))?;
    // Same catalog bootstrap as `weft spark server` so Glue/Hive tables resolve on workers.
    let catalogs = catalog_conf(args);
    if !catalogs.is_empty() {
        eprintln!("Worker declared {} catalog config entrie(s)", catalogs.len());
    }
    let service = weft_connect::WeftService::with_catalogs(catalogs);
    let engine = service.engine();
    // Optionally register a Parquet table so a driver query has data to read.
    if let (Some(data), Some(table)) = (flag(args, "--data"), flag(args, "--table")) {
        engine.register_parquet(&table, &data).await?;
        eprintln!("registered `{table}` from {data}");
    }
    eprintln!("Weft worker listening on Flight 0.0.0.0:{port}");
    serve_worker(port, engine).await
}

async fn run_driver(args: &[String]) -> weft_common::Result<()> {
    let workers: Vec<String> = flag(args, "--workers")
        .or_else(|| std::env::var("WEFT_WORKERS").ok())
        .map(|s| {
            s.split(',')
                .map(|w| {
                    let w = w.trim();
                    if w.starts_with("http") {
                        w.to_string()
                    } else {
                        format!("http://{w}")
                    }
                })
                .collect()
        })
        .ok_or_else(|| {
            weft_common::Error::Io("driver requires --workers or WEFT_WORKERS".into())
        })?;
    let partial_sql = flag(args, "--partial-sql")
        .ok_or_else(|| weft_common::Error::Io("driver requires --partial-sql".into()))?;
    let final_sql = flag(args, "--final-sql")
        .ok_or_else(|| weft_common::Error::Io("driver requires --final-sql".into()))?;
    let hash_key_cols: Vec<u32> = flag(args, "--hash-keys")
        .unwrap_or_else(|| "0".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    let cluster = Cluster::new(workers);
    let plan = DistributedPlan {
        partial_sql,
        final_sql,
        hash_key_cols,
    };
    let batches = run_distributed(&cluster, &plan).await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    eprintln!(
        "distributed result: {rows} rows in {} batches",
        batches.len()
    );
    if let Some(first) = batches.first() {
        eprintln!("schema: {:?}", first.schema());
    }
    Ok(())
}

/// Read the value following `--name` in `args`.
fn flag(args: &[String], name: &str) -> Option<String> {
    let i = args.iter().position(|a| a == name)?;
    args.get(i + 1).cloned()
}
