//! The `oxidant` command-line entry point.
//!
//! ```text
//! oxidant start --port 50051                # start the Spark Connect server as a DAEMON
//! oxidant status                            # pid, uptime, ports, log path, health probe
//! oxidant stop                              # SIGTERM -> grace -> SIGKILL, pidfile cleared
//! oxidant restart                           # stop + start, same flags
//! oxidant spark server --port 50051 --foreground   # supervisors only (systemd, CI harnesses)
//! oxidant spark server --mode local-cluster --workers 2 --foreground
//! oxidant spark server --workers host1:50561,host2:50561 --foreground   # remote Flight workers
//! oxidant worker --port 50561 --foreground [--data hits.parquet --table t]   # a Flight worker
//! oxidant driver --workers h:p,h:p \         # orchestrate a 2-stage distributed aggregation
//!   --partial-sql "SELECT k, COUNT(*) c, SUM(v) s FROM t GROUP BY k" \
//!   --final-sql   "SELECT k, SUM(c) c, SUM(s) s FROM shuffle_input GROUP BY k" \
//!   --hash-keys 0
//! oxidant pipeline run -c oxidant.yaml      # build the declarative table DAG (Kafka -> lake)
//! oxidant pipeline validate -c oxidant.yaml  # parse + plan + topo-sort, run nothing
//! oxidant pipeline reconcile -c oxidant.yaml # postgres_cdc drift report; 1 drifted, 2 could not run
//! oxidant sql -e "SELECT 1"                 # run SQL in-process (no server needed)
//! oxidant sql -c oxidant.yaml -e "SELECT count(*) FROM local.live.orders"
//! oxidant sql --url http://driver:4040 -e "SELECT 1"   # ... or via a server's REST API
//! oxidant mcp                               # stdio MCP server over the same API
//! ```

use std::sync::Arc;

use oxidant_connect::{serve, ServerConfig};
use oxidant_execution::driver::{run_distributed, Cluster, DistributedPlan};
use oxidant_execution::flight::serve_worker;
use oxidant_loom::Engine;

mod client;
mod daemon;
mod embedded;
mod mcp;
mod pipeline;
mod portguard;
#[cfg(test)]
mod testutil;

// Deep SQL (nested derived tables + large CASE trees in generated stage SQL) recurses well past
// tokio's 2 MiB default worker-thread stack inside DataFusion's parser/optimizer once the async
// serve layers above it consume their share (KAN-2: TPC-DS Q39/Q70 stage re-parse overflowed on
// workers). Give every runtime thread a generous stack — production driver, worker, and server
// all enter through here.
fn main() {
    // `engine:` is lowered to `OXIDANT_*` variables with `std::env::set_var`, which is only sound
    // while the process is still single-threaded — `setenv` races any concurrent `getenv` from
    // another thread, which is exactly why Rust 2024 made `set_var` unsafe. So the config is
    // resolved and applied HERE, before the Tokio runtime and its worker threads exist, and the
    // loaded config is handed to the subcommands rather than each of them re-reading it.
    let args: Vec<String> = std::env::args().collect();
    let config = match oxidant_config::OxidantConfig::resolve(config_flag(&args).as_deref()) {
        Ok(config) => {
            if let Some(config) = &config {
                config.apply_engine_env();
            }
            config
        }
        Err(e) => {
            eprintln!("oxidant: {e}");
            std::process::exit(1);
        }
    };

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(32 * 1024 * 1024)
        .build()
        .expect("tokio runtime")
        .block_on(async_main(args, config))
}

async fn async_main(args: Vec<String>, config: Option<oxidant_config::OxidantConfig>) {
    // TODO(issue #1): replace this hand-rolled arg handling with clap.
    let cmd = args.get(1).map(String::as_str);

    let result = match cmd {
        // Daemon control. These are the *only* way a human starts a long-running oxidant; the
        // role subcommands below refuse to run attached to a terminal (see `daemon`).
        Some("start") => daemon::start(&args[2..], server_ports(&args)).await,
        Some("stop") => run_stop(&args),
        Some("status") => daemon::status().await,
        Some("restart") => run_restart(&args).await,
        Some("worker") => run_worker(&args, config).await,
        Some("driver") => run_driver(&args).await,
        Some("history-server") => run_history_server(&args).await,
        Some("pipeline") => run_pipeline(&args, config).await,
        Some("sql") => run_sql(&args, config).await,
        Some("mcp") => run_mcp(&args).await,
        // `oxidant spark server ...` (and the bare `server` alias) keep the Spark Connect path.
        _ if args.iter().any(|a| a == "server") => run_server(&args, config).await,
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
        "  oxidant start [--port <PORT>] [--ui-port <PORT>] [--ui-bind <ADDR>] [--no-ui] [--mode local|local-cluster] [--workers <N|host:port,...>] [--sample-data <DIR>]"
    );
    eprintln!("  oxidant stop [--timeout <SECS>]");
    eprintln!("  oxidant status");
    eprintln!("  oxidant restart [same flags as start]");
    eprintln!(
        "  oxidant spark server --foreground [...]   # supervisors only (systemd, CI harnesses)"
    );
    eprintln!("  oxidant history-server --dir <LOG_DIR> [--port <PORT>]");
    eprintln!("  oxidant worker --port <PORT> --foreground [--data <parquet> --table <name>]");
    eprintln!(
        "  oxidant driver --workers <h:p,h:p> --partial-sql <SQL> --final-sql <SQL> --hash-keys <c,c>"
    );
    eprintln!(
        "  oxidant sql (-e <SQL> | -f <FILE> | stdin) [--format table|csv|json] [--config <FILE>] [--url <URL>] [--timeout <SECS>]"
    );
    eprintln!("  oxidant mcp [--url <URL>]");
    eprintln!(
        "  oxidant pipeline (run|validate|show) [--config <FILE>] [--table <NAME>]... [--once]"
    );
    eprintln!(
        "  oxidant pipeline reconcile [--config <FILE>] [--table <NAME>]... [--sample <KEYS>] [--cron <EXPR>|off]"
    );
    eprintln!();
    eprintln!(
        "  Long-running processes are daemons: `oxidant start` spawns the server detached, and"
    );
    eprintln!(
        "  `status`/`stop`/`restart` drive it through $OXIDANT_DATA_DIR/run/oxidant.pid. Pass"
    );
    eprintln!("  --foreground to run a role in the foreground under your own supervisor instead.");
    eprintln!("  `oxidant status` exits 0 (running), 3 (stopped) or 4 (alive but not answering).");
    eprintln!();
    eprintln!(
        "  `oxidant sql` runs the statement IN-PROCESS by default — no server needed. Catalogs"
    );
    eprintln!(
        "  come from --config <FILE> (or $OXIDANT_CONFIG, or ./oxidant.yaml). Pass --url (or set"
    );
    eprintln!("  $OXIDANT_URL) to run it against a running server's REST API instead.");
    eprintln!("  `oxidant mcp` always talks to the REST statement API on the UI port.");
    eprintln!(
        "  `oxidant pipeline reconcile` reads only. It exits 0 when every postgres_cdc table"
    );
    eprintln!(
        "  matches its lakehouse target and 1 when any drifted, so it drops into cron or CI."
    );
}

async fn run_server(
    args: &[String],
    config: Option<oxidant_config::OxidantConfig>,
) -> oxidant_common::Result<()> {
    // The first thing checked, before the mode is even parsed: a bare `oxidant spark server` is
    // the habit this release breaks. Long-lived processes are daemons — `oxidant start` — and a
    // supervisor that wants to own the process passes `--foreground`.
    daemon::require_foreground(args, daemon::Role::Server);
    // Release builds only; see `daemon::enforce_single_instance` for why debug must multiply.
    daemon::enforce_single_instance();
    let mode = server_mode(args)?;
    let daemon::ServerPorts {
        port,
        ui_port,
        ui_bind,
    } = server_ports(args);
    // Before anything expensive (engine, catalogs, sample data) and before the "listening on"
    // banner: if either listener's port is taken, say who has it and stop. `serve` binds gRPC on
    // all interfaces and spawns the UI listener on `--ui-bind`, so those are the two addresses
    // to probe — the REST statement API rides the UI listener and needs no probe of its own.
    //
    // `oxidant start` probes the same two addresses before it spawns us, so in the daemon path
    // this is the second look. It stays: `--foreground` reaches here without one.
    portguard::ensure_available(
        std::net::SocketAddr::from((std::net::IpAddr::from([0, 0, 0, 0]), port)),
        portguard::PortKind::SparkConnect,
    );
    if let Some(ui) = ui_port {
        portguard::ensure_available(
            std::net::SocketAddr::from((ui_bind, ui)),
            portguard::PortKind::Ui,
        );
    }
    // The config file is the base layer; `--catalog-conf` / `OXIDANT_CATALOG_CONF` are applied
    // on top, so an explicit flag always beats the file — the same direction every other
    // override in this CLI runs.
    // `engine:` was already applied in `main`, before the runtime started — see the comment
    // there on why `set_var` cannot happen from inside a multi-threaded runtime.
    let mut catalogs: std::collections::HashMap<String, String> = config
        .as_ref()
        .map(|c| c.catalog_conf().into_iter().collect())
        .unwrap_or_default();
    catalogs.extend(catalog_conf(args));
    if !catalogs.is_empty() {
        eprintln!("Declared {} catalog config entrie(s)", catalogs.len());
    }
    // `serve` re-checks this — it is the library's own guarantee — but only after this function
    // has already printed "listening on …", which would make a config error read like a crash
    // after a successful boot. Check it here so the refusal comes *before* the banner.
    oxidant_connect::validate_default_catalog(&catalogs)
        .map_err(|e| oxidant_common::Error::Plan(e.message().to_string()))?;
    let sample_data_dir = sample_data_dir(args);
    let workers = match mode {
        ServerMode::Local => static_workers(args)?,
        ServerMode::LocalCluster { workers } => start_local_cluster_workers(workers).await?,
    };
    if !workers.is_empty() {
        let csv = workers.join(",");
        // Mirror the endpoints into the process environment for helper paths that still
        // consult OXIDANT_WORKERS, while passing the explicit list below as the authoritative config.
        std::env::set_var("OXIDANT_WORKERS", &csv);
        let origin = match mode {
            ServerMode::Local => "static",
            ServerMode::LocalCluster { .. } => "local-cluster",
        };
        eprintln!("Oxidant {origin} workers: {csv}");
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
        sample_data_dir,
        ..Default::default()
    };
    if !workers.is_empty() {
        config.workers = workers;
    }
    serve(config).await
}

/// The two listener addresses a `spark server` will bind, read off the flags.
///
/// Shared with `oxidant start`, which needs them *before* it spawns anything: it runs the port
/// guard in the operator's terminal and records the ports in the pidfile so `status` can name
/// them and probe the UI without re-parsing a command line.
fn server_ports(args: &[String]) -> daemon::ServerPorts {
    daemon::ServerPorts {
        port: flag(args, "--port")
            .and_then(|s| s.parse().ok())
            .unwrap_or(50051),
        ui_port: if args.iter().any(|a| a == "--no-ui") {
            None
        } else {
            Some(
                flag(args, "--ui-port")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(4040),
            )
        },
        ui_bind: ui_bind_addr(args),
    }
}

/// `oxidant stop [--timeout <SECS>]` — how long SIGTERM gets before SIGKILL.
fn run_stop(args: &[String]) -> oxidant_common::Result<()> {
    let grace = match flag(args, "--timeout") {
        Some(t) => std::time::Duration::from_secs(t.parse::<u64>().map_err(|_| {
            oxidant_common::Error::Io(format!(
                "invalid --timeout `{t}` (expected a number of seconds)"
            ))
        })?),
        None => daemon::DEFAULT_STOP_GRACE,
    };
    daemon::stop(grace)
}

/// `oxidant restart [flags]` — stop, then start.
///
/// The flags are read from the *running* daemon's pidfile before it is stopped, so a bare
/// `oxidant restart` comes back on the ports it went down on. Flags typed on the restart
/// override them wholesale, which is how you move a running server to a new port.
async fn run_restart(args: &[String]) -> oxidant_common::Result<()> {
    // Read the file, not the *state*: `daemon::running()` deletes the pidfile of a process that
    // is gone, which is precisely the case `restart` is run in — after a SIGKILL, an OOM kill or
    // a reboot. Reading the state first threw away the recorded ports and silently brought the
    // server back on 50051/4040, breaking every client pointed at the old ones.
    let recorded = daemon::recorded_pidfile();
    let flags = daemon::restart_flags(&args[2..], recorded.as_ref());
    run_stop(args)?;
    let mut argv = vec![args[0].clone(), "start".to_string()];
    argv.extend(flags.iter().cloned());
    daemon::start(&flags, server_ports(&argv)).await
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
            let raw = flag(args, "--workers")
                .or_else(|| std::env::var("OXIDANT_DEFAULT_PARALLELISM").ok());
            let workers = match raw {
                None => 2,
                Some(s) => s.parse::<usize>().map_err(|_| {
                    oxidant_common::Error::Io(format!(
                        "local-cluster expects a worker count for --workers, got `{s}` \
                         (remote host:port,... endpoints attach in the default local mode)"
                    ))
                })?,
            };
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

/// `--workers` in the default local mode: a static list of remote Flight worker endpoints
/// (`host:port,...` — scheme defaults to `http://`). A bare number is the in-process worker
/// *count* and only makes sense with `--mode local-cluster`; reject it here so the two
/// meanings can't be confused silently.
fn static_workers(args: &[String]) -> oxidant_common::Result<Vec<String>> {
    let Some(raw) = flag(args, "--workers") else {
        return Ok(Vec::new());
    };
    if raw.trim().is_empty() {
        return Err(oxidant_common::Error::Io(
            "empty --workers value (expected host:port,... worker endpoints)".into(),
        ));
    }
    if raw.parse::<usize>().is_ok() {
        return Err(oxidant_common::Error::Io(format!(
            "`--workers {raw}` is a worker count, which requires `--mode local-cluster`; \
             to attach remote workers pass endpoints instead: `--workers host:port,...`"
        )));
    }
    Ok(oxidant_connect::parse_worker_list(Some(&raw)))
}

async fn start_local_cluster_workers(count: usize) -> oxidant_common::Result<Vec<String>> {
    // N in-process workers each run `Engine::new()`; without this hint every engine
    // auto-sizes to ~70% of host RAM and they overcommit. See `resolve_memory_pool_bytes`.
    if count > 1 {
        std::env::set_var("OXIDANT_COLOCATED_ENGINES", count.to_string());
    }
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
    portguard::ensure_available(
        std::net::SocketAddr::from((std::net::IpAddr::from([0, 0, 0, 0]), port)),
        portguard::PortKind::History,
    );
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

/// `--sample-data <DIR>` (or `OXIDANT_SAMPLE_DATA_DIR`): preload the bundled sample tables
/// under the `samples` schema at startup. The flag wins over the env var; an empty value is
/// treated as unset. When neither is set, a bundled sample-data tree installed next to the
/// binary is auto-discovered (release tarballs / curl|sh / deb / rpm layouts).
fn sample_data_dir(args: &[String]) -> Option<std::path::PathBuf> {
    let explicit = sample_data_dir_from(
        flag(args, "--sample-data"),
        std::env::var("OXIDANT_SAMPLE_DATA_DIR").ok(),
    );
    if explicit.is_some() {
        return explicit;
    }
    let exe = std::env::current_exe().ok()?;
    resolve_sample_data_dir(exe.parent()?)
}

fn sample_data_dir_from(flag: Option<String>, env: Option<String>) -> Option<std::path::PathBuf> {
    flag.or(env)
        .filter(|s| !s.trim().is_empty())
        .map(std::path::PathBuf::from)
}

/// Sample-data auto-discovery. `exe_dir` is the directory of the current executable; the
/// first candidate that exists and contains a `parquet/` subdir (sanity check that it is
/// really a sample-data tree) wins. Returns None when nothing matches (no `samples` schema
/// — the pre-discovery behavior). Checked in order:
/// 1. `<exe_dir>/sample-data` — release tarballs + the curl|sh installer
/// 2. `<exe_dir>/../share/oxidant/sample-data` — prefix layouts (/usr/local/bin/…)
/// 3. `/usr/share/oxidant/sample-data` — deb / rpm packages (packaging/nfpm.yaml)
fn resolve_sample_data_dir(exe_dir: &std::path::Path) -> Option<std::path::PathBuf> {
    [
        exe_dir.join("sample-data"),
        exe_dir.join("../share/oxidant/sample-data"),
        std::path::PathBuf::from("/usr/share/oxidant/sample-data"),
    ]
    .into_iter()
    .find(|dir| dir.join("parquet").is_dir())
}

async fn run_worker(
    args: &[String],
    config: Option<oxidant_config::OxidantConfig>,
) -> oxidant_common::Result<()> {
    // A worker outlives the shell that started it just as surely as a server does, so it is
    // held to the same rule. Unlike the server it has no `oxidant start` form — workers are
    // started by a supervisor (systemd on the AMI), which passes `--foreground`.
    daemon::require_foreground(args, daemon::Role::Worker);
    let port: u16 = flag(args, "--port")
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| oxidant_common::Error::Io("worker requires --port".into()))?;
    // Ahead of `logging::init` and the engine: a worker refused for a port conflict should not
    // first open a log directory and build a catalog it is about to throw away.
    portguard::ensure_available(
        std::net::SocketAddr::from((std::net::IpAddr::from([0, 0, 0, 0]), port)),
        portguard::PortKind::Flight,
    );
    // A standalone worker builds no REST router, so before this it installed no `tracing`
    // subscriber at all and its logs went nowhere — and worker OOMs are exactly what operators
    // dig for (docs/query-history-durability.md §6c). Every node writes its own `logs/` under
    // its own root; the driver federates *reads* over them (PR4) rather than ingesting them.
    oxidant_connect::logging::init("worker", port);
    // The worker half of §6's shutdown flush: a scaled-down worker gets SIGTERM, and the tail of
    // its log is exactly what an operator reads afterwards.
    oxidant_connect::logging::install_shutdown_flush();
    // Same catalog bootstrap as `oxidant spark server` so Glue/Hive tables resolve on workers.
    // Workers must see the same catalogs as the driver, or a distributed stage cannot resolve
    // the tables its plan references. Same precedence as the server: file first, flags on top.
    // `engine:` was already applied in `main`, before the runtime started — see the comment
    // there on why `set_var` cannot happen from inside a multi-threaded runtime.
    let mut catalogs: std::collections::HashMap<String, String> = config
        .as_ref()
        .map(|c| c.catalog_conf().into_iter().collect())
        .unwrap_or_default();
    catalogs.extend(catalog_conf(args));
    if !catalogs.is_empty() {
        eprintln!(
            "Worker declared {} catalog config entrie(s)",
            catalogs.len()
        );
    }
    let service = oxidant_connect::OxidantService::with_catalogs(catalogs).await;
    let engine = service.engine();
    // Optionally register a Parquet table so a driver query has data to read.
    if let (Some(data), Some(table)) = (flag(args, "--data"), flag(args, "--table")) {
        engine.register_parquet(&table, &data).await?;
        eprintln!("registered `{table}` from {data}");
    }
    eprintln!("Oxidant worker listening on Flight 0.0.0.0:{port}");
    // Durable, not just stderr: this is the last thing before the serve loop, so its presence in
    // `logs/oxidant.log` is what proves the `tracing` layer is still attached after every other
    // crate in the process has had its chance at `try_init` — the init line alone cannot say that.
    // It is also the line an operator greps to answer "did this worker actually come up".
    tracing::info!(role = "worker", port, "oxidant worker listening on Flight");
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

/// `oxidant mcp [--url U]` — serve the statements API as MCP tools on stdin/stdout.
async fn run_mcp(args: &[String]) -> oxidant_common::Result<()> {
    let url = client::resolve_url(flag(args, "--url"));
    // Protocol frames own stdout; this is the one allowed diagnostic and it goes to stderr.
    eprintln!("oxidant mcp server on stdio (statements API: {url})");
    mcp::serve(&url).await
}

/// Where `oxidant sql` reads the statement text from.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SqlSource {
    /// `-e <SQL>`
    Inline(String),
    /// `-f <FILE>`
    File(String),
    /// Neither flag: slurp stdin.
    Stdin,
}

/// `--format table|csv|json` for `oxidant sql` (default `table`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputFormat {
    Table,
    Csv,
    Json,
}

impl OutputFormat {
    fn parse(s: &str) -> oxidant_common::Result<Self> {
        match s {
            "table" => Ok(Self::Table),
            "csv" => Ok(Self::Csv),
            "json" => Ok(Self::Json),
            other => Err(oxidant_common::Error::Io(format!(
                "unknown --format `{other}` (expected table, csv or json)"
            ))),
        }
    }
}

/// Parsed `oxidant sql` arguments.
#[derive(Debug, PartialEq, Eq)]
struct SqlOptions {
    source: SqlSource,
    format: OutputFormat,
    url: String,
    timeout_secs: u64,
    /// Whether a server was *asked for* (`--url` or `OXIDANT_URL`), as opposed to the default
    /// URL being filled in.
    ///
    /// This is what selects between the two execution paths, and it has to be a separate
    /// signal: `url` is always populated, so "is it non-empty" cannot distinguish "point at
    /// this driver" from "nobody said anything". Running against `localhost:4040` because the
    /// user did not mention a server would fail with a connection error on a machine that has
    /// no server, which is precisely the case this whole path exists to serve.
    server_requested: bool,
    /// `--config <PATH>`, resolved for the embedded path.
    config: Option<String>,
    /// `--sample-data <DIR>` override for the embedded path.
    sample_data: Option<String>,
}

fn sql_options(args: &[String]) -> oxidant_common::Result<SqlOptions> {
    let source = match (flag(args, "-e"), flag(args, "-f")) {
        (Some(sql), None) => SqlSource::Inline(sql),
        (None, Some(path)) => SqlSource::File(path),
        (Some(_), Some(_)) => {
            return Err(oxidant_common::Error::Io(
                "pass only one of -e <SQL> or -f <FILE>".into(),
            ));
        }
        (None, None) => SqlSource::Stdin,
    };
    let format = match flag(args, "--format") {
        Some(f) => OutputFormat::parse(&f)?,
        None => OutputFormat::Table,
    };
    let timeout_secs = match flag(args, "--timeout") {
        Some(t) => t.parse::<u64>().ok().filter(|n| *n > 0).ok_or_else(|| {
            oxidant_common::Error::Io(format!(
                "invalid --timeout `{t}` (expected a positive number of seconds)"
            ))
        })?,
        None => client::DEFAULT_STATEMENT_TIMEOUT.as_secs(),
    };
    let url_flag = flag(args, "--url");
    let server_requested = url_flag.as_deref().is_some_and(|u| !u.trim().is_empty())
        || std::env::var("OXIDANT_URL")
            .ok()
            .is_some_and(|u| !u.trim().is_empty());
    Ok(SqlOptions {
        source,
        format,
        url: client::resolve_url(url_flag),
        timeout_secs,
        server_requested,
        config: config_flag(args),
        sample_data: flag(args, "--sample-data"),
    })
}

/// `--config <PATH>` / `-c <PATH>`, shared by every subcommand.
fn config_flag(args: &[String]) -> Option<String> {
    flag(args, "--config")
        .or_else(|| flag(args, "-c"))
        .filter(|p| !p.trim().is_empty())
}

/// `oxidant pipeline ...` — build the declarative table DAG from the config file.
async fn run_pipeline(
    args: &[String],
    config: Option<oxidant_config::OxidantConfig>,
) -> oxidant_common::Result<()> {
    let command = pipeline::parse_command_or_exit(args)?;
    pipeline::run(config, command).await
}

/// `oxidant sql ...` — run one statement and print the result.
///
/// Two paths. With `--url` or `OXIDANT_URL` set, the statement goes to that server's REST API,
/// exactly as it always has. Otherwise it runs **in-process**: the config file's catalogs are
/// bridged into a local engine and the query executes here, so the CLI is useful with no
/// server running at all.
///
/// Failed statements exit non-zero with the error on stderr (via `main`).
async fn run_sql(
    args: &[String],
    config: Option<oxidant_config::OxidantConfig>,
) -> oxidant_common::Result<()> {
    let opts = sql_options(args)?;
    let sql = read_sql(&opts.source).await?;
    if opts.server_requested {
        run_sql_remote(&opts, &sql).await
    } else {
        run_sql_embedded(args, config, &opts, &sql).await
    }
}

/// Run a statement in-process against a locally-built engine.
async fn run_sql_embedded(
    args: &[String],
    config: Option<oxidant_config::OxidantConfig>,
    opts: &SqlOptions,
    sql: &str,
) -> oxidant_common::Result<()> {
    let sample_data = match &opts.sample_data {
        Some(dir) => sample_data_dir_from(Some(dir.clone()), None),
        None => sample_data_dir(args),
    };
    let engine = embedded::build_engine(config.as_ref(), sample_data).await?;
    let batches = embedded::run_sql(&engine, sql).await?;
    match opts.format {
        OutputFormat::Csv => {
            print!("{}", embedded::result_csv(&batches, EMBEDDED_ROW_LIMIT)?);
        }
        OutputFormat::Json => {
            let doc = embedded::result_doc(&batches, EMBEDDED_ROW_LIMIT)?;
            let pretty = serde_json::to_string_pretty(&doc)
                .map_err(|e| oxidant_common::Error::Io(format!("encode result json: {e}")))?;
            println!("{pretty}");
        }
        OutputFormat::Table => {
            let doc = embedded::result_doc(&batches, EMBEDDED_ROW_LIMIT)?;
            print!("{}", render_table(&doc));
        }
    }
    Ok(())
}

/// Rows the embedded path will materialize for display.
///
/// A cap, not a silent one: past it the rendered footer says `[truncated]`. Without a limit a
/// `SELECT *` over a large table would try to format the whole thing into memory as JSON.
const EMBEDDED_ROW_LIMIT: usize = 10_000;

/// Run a statement against a running server's REST API.
async fn run_sql_remote(opts: &SqlOptions, sql: &str) -> oxidant_common::Result<()> {
    let client = client::StatementClient::new(&opts.url)?;
    let terminal = client::run_to_completion(
        &client,
        sql,
        std::time::Duration::from_secs(opts.timeout_secs),
    )
    .await?;
    let id = terminal["statementId"].as_str().unwrap_or("?").to_string();
    match terminal["status"].as_str().unwrap_or("unknown") {
        "succeeded" => print_result(&client, &id, opts.format).await,
        "failed" => {
            let detail = terminal["error"].as_str().unwrap_or("no error detail");
            Err(oxidant_common::Error::Execution(format!(
                "statement {id} failed: {detail}"
            )))
        }
        other => Err(oxidant_common::Error::Execution(format!(
            "statement {id} ended with status `{other}`"
        ))),
    }
}

/// Resolve the SQL text from `-e`/`-f`/stdin and reject empty input.
async fn read_sql(source: &SqlSource) -> oxidant_common::Result<String> {
    use tokio::io::AsyncReadExt;
    let raw = match source {
        SqlSource::Inline(sql) => sql.clone(),
        SqlSource::File(path) => std::fs::read_to_string(path)
            .map_err(|e| oxidant_common::Error::Io(format!("read {path}: {e}")))?,
        SqlSource::Stdin => {
            let mut buf = String::new();
            tokio::io::stdin()
                .read_to_string(&mut buf)
                .await
                .map_err(|e| oxidant_common::Error::Io(format!("read stdin: {e}")))?;
            buf
        }
    };
    let sql = raw.trim();
    if sql.is_empty() {
        return Err(oxidant_common::Error::Io(
            "no SQL provided (use -e <SQL>, -f <FILE>, or pipe SQL on stdin)".into(),
        ));
    }
    Ok(sql.to_string())
}

/// Fetch and print the result of a succeeded statement in the requested format.
async fn print_result(
    client: &client::StatementClient,
    id: &str,
    format: OutputFormat,
) -> oxidant_common::Result<()> {
    match format {
        OutputFormat::Csv => {
            let client::ResultBody::Csv(text) = client
                .get_result(id, client::ResultFormat::Csv, None)
                .await?
            else {
                return Err(oxidant_common::Error::Execution(
                    "statements API returned JSON for a CSV result request".into(),
                ));
            };
            // The API's CSV payload already ends with a newline.
            print!("{text}");
            Ok(())
        }
        OutputFormat::Json => {
            let client::ResultBody::Json(doc) = client
                .get_result(id, client::ResultFormat::Json, None)
                .await?
            else {
                return Err(oxidant_common::Error::Execution(
                    "statements API returned CSV for a JSON result request".into(),
                ));
            };
            let pretty = serde_json::to_string_pretty(&doc)
                .map_err(|e| oxidant_common::Error::Io(format!("encode result json: {e}")))?;
            println!("{pretty}");
            Ok(())
        }
        OutputFormat::Table => {
            let client::ResultBody::Json(doc) = client
                .get_result(id, client::ResultFormat::Json, None)
                .await?
            else {
                return Err(oxidant_common::Error::Execution(
                    "statements API returned CSV for a JSON result request".into(),
                ));
            };
            print!("{}", render_table(&doc));
            Ok(())
        }
    }
}

/// Render a result document (`{"schema":{"fields":[{"name",...}]},"rows":[{...}],...}`) as an
/// aligned ASCII table with a header, followed by a `(N rows)` line (`[truncated]` when the
/// server cut rows at the result limit).
fn render_table(doc: &serde_json::Value) -> String {
    let mut columns: Vec<String> = doc["schema"]["fields"]
        .as_array()
        .map(|fields| {
            fields
                .iter()
                .filter_map(|f| f["name"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let empty_rows = Vec::new();
    let rows: &Vec<serde_json::Value> = doc["rows"].as_array().unwrap_or(&empty_rows);
    // Defensive fallback: no schema doc — derive columns from the first row's keys.
    if columns.is_empty() {
        if let Some(first) = rows.first().and_then(serde_json::Value::as_object) {
            columns = first.keys().cloned().collect();
        }
    }
    let cell = |row: &serde_json::Value, col: &str| -> String {
        match row.get(col) {
            None | Some(serde_json::Value::Null) => "null".to_string(),
            Some(serde_json::Value::String(s)) => s.clone(),
            Some(other) => other.to_string(),
        }
    };
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for row in rows {
        for (i, col) in columns.iter().enumerate() {
            widths[i] = widths[i].max(cell(row, col).len());
        }
    }
    // One table cell: ` value ` left-aligned and right-padded to the column width.
    let push_cell = |out: &mut String, value: &str, width: usize| {
        out.push(' ');
        out.push_str(value);
        for _ in value.len()..width {
            out.push(' ');
        }
        out.push_str(" |");
    };
    let mut out = String::new();
    if !columns.is_empty() {
        let border = |out: &mut String| {
            out.push('+');
            for w in &widths {
                for _ in 0..*w + 2 {
                    out.push('-');
                }
                out.push('+');
            }
            out.push('\n');
        };
        border(&mut out);
        out.push('|');
        for (col, w) in columns.iter().zip(&widths) {
            push_cell(&mut out, col, *w);
        }
        out.push('\n');
        border(&mut out);
        for row in rows {
            out.push('|');
            for (col, w) in columns.iter().zip(&widths) {
                push_cell(&mut out, &cell(row, col), *w);
            }
            out.push('\n');
        }
        border(&mut out);
    }
    let count = rows.len();
    out.push('(');
    out.push_str(&count.to_string());
    out.push_str(if count == 1 { " row)" } else { " rows)" });
    if doc["truncated"].as_bool().unwrap_or(false) {
        out.push_str(" [truncated]");
    }
    out.push('\n');
    out
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
    fn local_cluster_mode_rejects_non_numeric_workers() {
        let parsed = server_mode(&args(&[
            "oxidant",
            "spark",
            "server",
            "--mode",
            "local-cluster",
            "--workers",
            "host1:50561",
        ]));
        assert!(parsed.is_err());
    }

    #[test]
    fn static_workers_parses_endpoint_list() {
        let a = args(&[
            "oxidant",
            "spark",
            "server",
            "--workers",
            "host1:50561, http://host2:50562",
        ]);
        assert_eq!(
            static_workers(&a).unwrap(),
            vec![
                "http://host1:50561".to_string(),
                "http://host2:50562".to_string()
            ]
        );
    }

    #[test]
    fn static_workers_rejects_bare_count_without_local_cluster() {
        let a = args(&["oxidant", "spark", "server", "--workers", "3"]);
        let Err(oxidant_common::Error::Io(msg)) = static_workers(&a) else {
            panic!("expected Io error for a bare worker count in local mode");
        };
        assert!(msg.contains("local-cluster"), "unexpected error: {msg}");
    }

    #[test]
    fn static_workers_absent_is_empty_and_blank_is_an_error() {
        assert!(static_workers(&args(&["oxidant", "spark", "server"]))
            .unwrap()
            .is_empty());
        assert!(static_workers(&args(&["oxidant", "spark", "server", "--workers", " "])).is_err());
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

    #[test]
    fn sql_options_parse_inline_with_defaults() {
        let opts = sql_options(&args(&[
            "oxidant",
            "sql",
            "-e",
            "SELECT 1",
            "--url",
            "http://h:9/",
        ]))
        .unwrap();
        assert_eq!(opts.source, SqlSource::Inline("SELECT 1".to_string()));
        assert_eq!(opts.format, OutputFormat::Table);
        assert_eq!(opts.url, "http://h:9");
        assert_eq!(opts.timeout_secs, 300);
    }

    #[test]
    fn sql_options_parse_file_source_and_csv_format() {
        let opts = sql_options(&args(&[
            "oxidant",
            "sql",
            "-f",
            "/tmp/q.sql",
            "--format=csv",
            "--timeout",
            "5",
            "--url",
            "http://h:9",
        ]))
        .unwrap();
        assert_eq!(opts.source, SqlSource::File("/tmp/q.sql".to_string()));
        assert_eq!(opts.format, OutputFormat::Csv);
        assert_eq!(opts.timeout_secs, 5);
    }

    #[test]
    fn sql_options_default_source_is_stdin() {
        let opts = sql_options(&args(&["oxidant", "sql", "--url", "http://h:9"])).unwrap();
        assert_eq!(opts.source, SqlSource::Stdin);
    }

    #[test]
    fn sql_options_rejects_e_and_f_together() {
        let parsed = sql_options(&args(&["oxidant", "sql", "-e", "SELECT 1", "-f", "q.sql"]));
        assert!(parsed.is_err());
    }

    #[test]
    fn sql_options_reject_unknown_format_and_bad_timeout() {
        let parsed = sql_options(&args(&[
            "oxidant", "sql", "-e", "SELECT 1", "--format", "xml",
        ]));
        assert!(parsed.is_err());
        let parsed = sql_options(&args(&[
            "oxidant",
            "sql",
            "-e",
            "SELECT 1",
            "--timeout",
            "soon",
        ]));
        assert!(parsed.is_err());
        let parsed = sql_options(&args(&[
            "oxidant",
            "sql",
            "-e",
            "SELECT 1",
            "--timeout",
            "0",
        ]));
        assert!(parsed.is_err());
    }

    #[test]
    fn sql_options_accept_json_format() {
        let opts = sql_options(&args(&[
            "oxidant",
            "sql",
            "-e",
            "SELECT 1",
            "--format",
            "json",
            "--url",
            "http://h:9",
        ]))
        .unwrap();
        assert_eq!(opts.format, OutputFormat::Json);
    }

    #[test]
    fn sample_data_dir_prefers_flag_then_env_and_skips_empty() {
        let flag = Some("data/samples".to_string());
        let env = Some("/opt/oxidant/sample-data".to_string());
        assert_eq!(
            sample_data_dir_from(flag.clone(), env.clone()),
            Some(std::path::PathBuf::from("data/samples"))
        );
        assert_eq!(
            sample_data_dir_from(None, env),
            Some(std::path::PathBuf::from("/opt/oxidant/sample-data"))
        );
        assert_eq!(sample_data_dir_from(None, Some("  ".to_string())), None);
        assert_eq!(sample_data_dir_from(None, None), None);
    }

    /// A fake installed tree: `mkdir -p <root>/<rel>/parquet`.
    fn make_sample_tree(root: &std::path::Path, rel: &str) {
        std::fs::create_dir_all(root.join(rel).join("parquet")).unwrap();
    }

    #[test]
    fn discovery_finds_sample_data_next_to_exe() {
        let tmp = tempfile::tempdir().unwrap();
        let exe_dir = tmp.path().join("bin");
        make_sample_tree(tmp.path(), "bin/sample-data");
        assert_eq!(
            resolve_sample_data_dir(&exe_dir),
            Some(exe_dir.join("sample-data"))
        );
    }

    #[test]
    fn discovery_falls_through_to_prefix_share_layout() {
        let tmp = tempfile::tempdir().unwrap();
        let exe_dir = tmp.path().join("usr/local/bin");
        std::fs::create_dir_all(&exe_dir).unwrap();
        make_sample_tree(tmp.path(), "usr/local/share/oxidant/sample-data");
        assert_eq!(
            resolve_sample_data_dir(&exe_dir),
            Some(exe_dir.join("../share/oxidant/sample-data"))
        );
    }

    #[test]
    fn discovery_prefers_sibling_dir_over_prefix_share_layout() {
        let tmp = tempfile::tempdir().unwrap();
        let exe_dir = tmp.path().join("bin");
        make_sample_tree(tmp.path(), "bin/sample-data");
        make_sample_tree(tmp.path(), "share/oxidant/sample-data");
        assert_eq!(
            resolve_sample_data_dir(&exe_dir),
            Some(exe_dir.join("sample-data"))
        );
    }

    #[test]
    fn discovery_requires_the_parquet_sanity_subdir() {
        let tmp = tempfile::tempdir().unwrap();
        let exe_dir = tmp.path().join("bin");
        // A `sample-data` dir without `parquet/` is not a sample-data tree.
        std::fs::create_dir_all(exe_dir.join("sample-data/csv")).unwrap();
        assert_eq!(resolve_sample_data_dir(&exe_dir), None);
    }

    #[test]
    fn discovery_returns_none_when_nothing_is_installed() {
        // Assumes the host has no /usr/share/oxidant/sample-data (true unless the deb/rpm is
        // installed — dev machines and CI runners don't have it).
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(resolve_sample_data_dir(tmp.path()), None);
    }

    #[test]
    fn mcp_url_comes_from_flag_env_or_default() {
        let a = args(&["oxidant", "mcp", "--url", "http://h:4040/"]);
        assert_eq!(client::resolve_url(flag(&a, "--url")), "http://h:4040");
        let a = args(&["oxidant", "mcp"]);
        assert!(!client::resolve_url(flag(&a, "--url")).is_empty());
    }

    #[test]
    fn render_table_aligns_columns_and_counts_rows() {
        let doc = serde_json::json!({
            "schema": { "fields": [{ "name": "k", "type": "Int32" }, { "name": "label", "type": "String" }] },
            "rows": [
                { "k": 1, "label": "one" },
                { "k": 22, "label": null },
            ],
            "rowCount": 2,
            "truncated": false,
        });
        let rendered = render_table(&doc);
        let expected = "+----+-------+\n\
                        | k  | label |\n\
                        +----+-------+\n\
                        | 1  | one   |\n\
                        | 22 | null  |\n\
                        +----+-------+\n\
                        (2 rows)\n";
        assert_eq!(rendered, expected);
    }

    #[test]
    fn render_table_marks_truncation_and_empty_results() {
        let doc = serde_json::json!({
            "schema": { "fields": [{ "name": "hello", "type": "Int32" }] },
            "rows": [{ "hello": 1 }],
            "rowCount": 1,
            "truncated": true,
        });
        assert!(render_table(&doc).ends_with("(1 row) [truncated]\n"));

        let empty = serde_json::json!({
            "schema": { "fields": [] },
            "rows": [],
            "rowCount": 0,
            "truncated": false,
        });
        assert_eq!(render_table(&empty), "(0 rows)\n");
    }
}
