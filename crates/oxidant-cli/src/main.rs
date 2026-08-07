//! The `oxidant` command-line entry point.
//!
//! ```text
//! oxidant spark server --port 50051         # Spark Connect server; point PySpark at sc://host:50051
//! oxidant spark server --mode local-cluster --workers 2
//! oxidant worker --port 50561 [--data hits.parquet --table t]   # a distributed Flight worker
//! oxidant driver --workers h:p,h:p \         # orchestrate a 2-stage distributed aggregation
//!   --partial-sql "SELECT k, COUNT(*) c, SUM(v) s FROM t GROUP BY k" \
//!   --final-sql   "SELECT k, SUM(c) c, SUM(s) s FROM shuffle_input GROUP BY k" \
//!   --hash-keys 0
//! ```

use std::sync::Arc;

use oxidant_connect::{serve, ServerConfig};
use oxidant_execution::driver::{run_distributed, Cluster, DistributedPlan};
use oxidant_execution::flight::serve_worker;
use oxidant_loom::Engine;

// Deep SQL (nested derived tables + large CASE trees in generated stage SQL) recurses well past
// tokio's 2 MiB default worker-thread stack inside DataFusion's parser/optimizer once the async
// serve layers above it consume their share (KAN-2: TPC-DS Q39/Q70 stage re-parse overflowed on
// workers). Give every runtime thread a generous stack — production driver, worker, and server
// all enter through here.
fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(32 * 1024 * 1024)
        .build()
        .expect("tokio runtime")
        .block_on(async_main())
}

async fn async_main() {
    // TODO(issue #1): replace this hand-rolled arg handling with clap.
    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).map(String::as_str);

    let result = match cmd {
        Some("worker") => run_worker(&args).await,
        Some("driver") => run_driver(&args).await,
        Some("history-server") => run_history_server(&args).await,
        // `oxidant spark server ...` (and the bare `server` alias) keep the Spark Connect path.
        _ if args.iter().any(|a| a == "server") => run_server(&args).await,
        _ => {
            usage();
            return;
        }
    };
    if let Err(e) = result {
        eprintln!("oxidant: {e}");
        std::process::exit(1);
    }
}

fn usage() {
    eprintln!("oxidant {}", env!("CARGO_PKG_VERSION"));
    eprintln!("usage:");
    eprintln!(
        "  oxidant spark server --port <PORT> [--ui-port <PORT>] [--ui-bind <ADDR>] [--no-ui] [--mode local|local-cluster] [--workers <N>]"
    );
    eprintln!("  oxidant history-server --dir <LOG_DIR> [--port <PORT>]");
    eprintln!("  oxidant worker --port <PORT> [--data <parquet> --table <name>]");
    eprintln!(
        "  oxidant driver --workers <h:p,h:p> --partial-sql <SQL> --final-sql <SQL> --hash-keys <c,c>"
    );
}

async fn run_server(args: &[String]) -> oxidant_common::Result<()> {
    let mode = server_mode(args)?;
    let port = flag(args, "--port")
        .and_then(|s| s.parse().ok())
        .unwrap_or(50051);
    let ui_port = if args.iter().any(|a| a == "--no-ui") {
        None
    } else {
        Some(
            flag(args, "--ui-port")
                .and_then(|s| s.parse().ok())
                .unwrap_or(4040),
        )
    };
    let ui_bind = ui_bind_addr(args);
    let catalogs = catalog_conf(args);
    if !catalogs.is_empty() {
        eprintln!("Declared {} catalog config entrie(s)", catalogs.len());
    }
    let workers = match mode {
        ServerMode::Local => Vec::new(),
        ServerMode::LocalCluster { workers } => start_local_cluster_workers(workers).await?,
    };
    if !workers.is_empty() {
        let csv = workers.join(",");
        // Mirror the generated endpoints into the process environment for helper paths that still
        // consult OXIDANT_WORKERS, while passing the explicit list below as the authoritative config.
        std::env::set_var("OXIDANT_WORKERS", &csv);
        eprintln!("Oxidant local-cluster workers: {csv}");
    }
    eprintln!("Oxidant Spark Connect server listening on sc://0.0.0.0:{port}");
    if let Some(ui) = ui_port {
        eprintln!("Oxidant UI at http://{ui_bind}:{ui}");
    }
    let mut config = ServerConfig {
        port,
        ui_port,
        ui_bind: Some(ui_bind),
        catalogs,
        ..Default::default()
    };
    if !workers.is_empty() {
        config.workers = workers;
    }
    serve(config).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServerMode {
    Local,
    LocalCluster { workers: usize },
}

/// `--ui-bind <ADDR>`: interface for the monitoring UI (no auth). Defaults to
/// 0.0.0.0, matching Spark; public AMI images should pass 127.0.0.1.
fn ui_bind_addr(args: &[String]) -> std::net::IpAddr {
    flag(args, "--ui-bind")
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::net::IpAddr::from([0, 0, 0, 0]))
}

fn server_mode(args: &[String]) -> oxidant_common::Result<ServerMode> {
    let mode = flag(args, "--mode").unwrap_or_else(|| "local".to_string());
    match mode.as_str() {
        "local" | "single-node" => Ok(ServerMode::Local),
        "local-cluster" => {
            let workers = flag(args, "--workers")
                .or_else(|| std::env::var("OXIDANT_DEFAULT_PARALLELISM").ok())
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(2);
            if workers == 0 {
                return Err(oxidant_common::Error::Io(
                    "local-cluster requires at least one worker".into(),
                ));
            }
            Ok(ServerMode::LocalCluster { workers })
        }
        other => Err(oxidant_common::Error::Io(format!(
            "unknown spark server mode `{other}` (expected local or local-cluster)"
        ))),
    }
}

async fn start_local_cluster_workers(count: usize) -> oxidant_common::Result<Vec<String>> {
    let mut endpoints = Vec::with_capacity(count);
    for idx in 0..count {
        let port = pick_ephemeral_port()?;
        let endpoint = format!("http://127.0.0.1:{port}");
        let engine = Arc::new(Engine::new());
        tokio::spawn(async move {
            if let Err(e) = serve_worker(port, engine).await {
                eprintln!("oxidant local-cluster worker {idx} exited: {e}");
            }
        });
        eprintln!("Oxidant local-cluster worker {idx} listening on Flight {endpoint}");
        endpoints.push(endpoint);
    }
    Ok(endpoints)
}

fn pick_ephemeral_port() -> oxidant_common::Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .map_err(|e| oxidant_common::Error::Io(format!("bind ephemeral worker port: {e}")))?;
    let port = listener
        .local_addr()
        .map_err(|e| oxidant_common::Error::Io(format!("read ephemeral worker port: {e}")))?
        .port();
    drop(listener);
    Ok(port)
}

async fn run_history_server(args: &[String]) -> oxidant_common::Result<()> {
    use oxidant_observability::AppStateStore;
    use oxidant_ui_server::{serve as serve_ui, UiServerConfig};
    use std::sync::Arc;

    let port: u16 = flag(args, "--port")
        .and_then(|s| s.parse().ok())
        .unwrap_or(18080);
    let dir = flag(args, "--dir")
        .or_else(|| std::env::var("OXIDANT_EVENT_LOG_DIR").ok())
        .ok_or_else(|| {
            oxidant_common::Error::Io(
                "history-server requires --dir or OXIDANT_EVENT_LOG_DIR".into(),
            )
        })?;
    let store = Arc::new(AppStateStore::load_event_log(std::path::Path::new(&dir)));
    eprintln!("Oxidant history server on http://0.0.0.0:{port} (log: {dir})");
    serve_ui(UiServerConfig {
        port,
        store,
        bind: std::net::IpAddr::from([0, 0, 0, 0]),
        // The history server replays an event log — no live engine, so no REST statements.
        merge_router: None,
    })
    .await
}

/// Collect startup catalog config from repeated `--catalog-conf key=value` flags and the
/// `OXIDANT_CATALOG_CONF` env var (`;`-separated `key=value`). Keys are full Spark config keys, e.g.
/// `spark.sql.catalog.prod.type=hive`. Example:
///   oxidant spark server --catalog-conf spark.sql.catalog.prod.type=hive \
///                     --catalog-conf spark.sql.catalog.prod.uri=thrift://hms:9083
fn catalog_conf(args: &[String]) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    let mut insert_kv = |kv: &str| {
        if let Some((k, v)) = kv.split_once('=') {
            out.insert(k.trim().to_string(), v.trim().to_string());
        }
    };
    if let Ok(env) = std::env::var("OXIDANT_CATALOG_CONF") {
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

async fn run_worker(args: &[String]) -> oxidant_common::Result<()> {
    let port: u16 = flag(args, "--port")
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| oxidant_common::Error::Io("worker requires --port".into()))?;
    // Same catalog bootstrap as `oxidant spark server` so Glue/Hive tables resolve on workers.
    let catalogs = catalog_conf(args);
    if !catalogs.is_empty() {
        eprintln!(
            "Worker declared {} catalog config entrie(s)",
            catalogs.len()
        );
    }
    let service = oxidant_connect::OxidantService::with_catalogs(catalogs);
    let engine = service.engine();
    // Optionally register a Parquet table so a driver query has data to read.
    if let (Some(data), Some(table)) = (flag(args, "--data"), flag(args, "--table")) {
        engine.register_parquet(&table, &data).await?;
        eprintln!("registered `{table}` from {data}");
    }
    eprintln!("Oxidant worker listening on Flight 0.0.0.0:{port}");
    serve_worker(port, engine).await
}

async fn run_driver(args: &[String]) -> oxidant_common::Result<()> {
    let workers: Vec<String> = flag(args, "--workers")
        .or_else(|| std::env::var("OXIDANT_WORKERS").ok())
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
            oxidant_common::Error::Io("driver requires --workers or OXIDANT_WORKERS".into())
        })?;
    let partial_sql = flag(args, "--partial-sql")
        .ok_or_else(|| oxidant_common::Error::Io("driver requires --partial-sql".into()))?;
    let final_sql = flag(args, "--final-sql")
        .ok_or_else(|| oxidant_common::Error::Io("driver requires --final-sql".into()))?;
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
    let eq = format!("{name}=");
    if let Some(value) = args.iter().find_map(|a| a.strip_prefix(&eq)) {
        return Some(value.to_string());
    }
    let i = args.iter().position(|a| a == name)?;
    args.get(i + 1).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn local_cluster_mode_parses_workers() {
        let parsed = server_mode(&args(&[
            "oxidant",
            "spark",
            "server",
            "--mode",
            "local-cluster",
            "--workers",
            "3",
        ]))
        .unwrap();
        assert_eq!(parsed, ServerMode::LocalCluster { workers: 3 });
    }

    #[test]
    fn local_cluster_mode_rejects_zero_workers() {
        let parsed = server_mode(&args(&[
            "oxidant",
            "spark",
            "server",
            "--mode",
            "local-cluster",
            "--workers",
            "0",
        ]));
        assert!(parsed.is_err());
    }

    #[test]
    fn ui_bind_parses_loopback() {
        let a = args(&["oxidant", "spark", "server", "--ui-bind", "127.0.0.1"]);
        assert_eq!(ui_bind_addr(&a), std::net::IpAddr::from([127, 0, 0, 1]));
    }

    #[test]
    fn ui_bind_defaults_to_all_interfaces() {
        let a = args(&["oxidant", "spark", "server"]);
        assert_eq!(ui_bind_addr(&a), std::net::IpAddr::from([0, 0, 0, 0]));
    }

    #[test]
    fn ui_bind_invalid_value_falls_back_to_default() {
        let a = args(&["oxidant", "spark", "server", "--ui-bind", "not-an-ip"]);
        assert_eq!(ui_bind_addr(&a), std::net::IpAddr::from([0, 0, 0, 0]));
    }
}
