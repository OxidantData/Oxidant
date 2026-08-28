//! `POST /api/v1/pipelines/lifecycle` — start, pause, resume, or re-snapshot a file-defined
//! pipeline the way `systemctl` and a shell already can, but over the wire.
//!
//! ## Why this exists
//!
//! `oxidant pipeline run --config <path>` has no server of its own — the process that runs a
//! pipeline listens on no socket (see `docs/pipelines.md`). Before this route, a control plane
//! that wanted to start, stop, or re-snapshot one had exactly one option: an `Unsupported`
//! answer carrying a copy-pasteable `systemctl` / `rm -rf` command for an operator to run by
//! hand (see the platform's `docs/connectors.md`, "no route that starts a pipeline"). This route
//! closes that gap for the one place a pipeline's *lifecycle* is reachable from: the driver,
//! which already serves `GET /api/v1/pipelines` for the same connector.
//!
//! It does not make the pipeline process itself reachable — that process still listens on
//! nothing. What changed is that the **driver** now knows how to find and drive the systemd
//! unit that runs a given pipeline config, and how to clear a table's checkpoint prefix through
//! the same object-store path the pipeline itself checkpoints through
//! ([`oxidant_streaming::checkpoint_store`]) rather than a shell `rm -rf`.
//!
//! ## Discovery, not naming convention
//!
//! The demo unit is `oxidant-connector-<name>.service`, but nothing enforces that shape, and a
//! future deployment tool is free to name units differently. So the unit is found the way an
//! operator running `systemctl status` by hand would find it: by reading every `*.service` file
//! under [`UNIT_DIR`] and matching the `--config` argument of its `ExecStart=` line against the
//! (canonicalized) `config_path` the caller asked about — see [`discover_unit`].
//!
//! ## Security
//!
//! This route executes process-level actions (`systemctl start/stop/restart`) and deletes
//! object-store data, so every input is validated against the driver's own state, never trusted
//! from the caller alone:
//!
//! - `config_path` must canonicalize to somewhere **under** [`CONFIG_DIR`] — no traversal, no
//!   pointing this route at an arbitrary file on the box (see [`validate_config_path`]).
//! - `checkpoint_root`, when the caller sends one, must match the root the pipeline's **own**
//!   config declares (`pipeline.checkpoints`, read fresh off disk) — the caller's word for the
//!   root is checked, never substituted for it. A resnapshot always deletes under the
//!   config-declared root.
//! - Table names must match `[A-Za-z0-9_]+` and must be tables the pipeline's own config
//!   declares — a request naming a table that is not this pipeline's is refused rather than
//!   deleting an arbitrary checkpoint prefix.
//! - Every process invocation goes through [`std::process::Command`] with an argv array
//!   (`systemctl`, one action, one unit name) — never a shell string, so there is no quoting
//!   layer for a crafted unit name or config path to escape.
//!
//! ## Auth
//!
//! Gated by the same bearer token as [`crate::status`] and [`crate::pipelines`]
//! ([`crate::status::denied`]) — see `docs/api.md`. Unset token, `404`; wrong token, `401`; this
//! is operator-triggered process control, so it is never unauthenticated.
//!
//! ## Privilege
//!
//! The driver runs as the unprivileged `oxidant` system user with `NoNewPrivileges=true`
//! (`deploy/packer/files/systemd/oxidant-driver.service`), which rules out a `sudo`-based
//! mechanism outright: `sudo` elevates by executing a setuid binary, and `NoNewPrivileges`
//! blocks exactly that, for any binary, unconditionally. Loosening it to make `sudo` work would
//! trade away the hardening on the one process that terminates a customer's inbound SQL.
//!
//! Instead this route shells out to the plain `systemctl` binary, which talks to PID 1 over the
//! system D-Bus rather than executing anything with elevated privileges itself; the privileged
//! part of the operation happens inside `systemd`, already running as root, on the caller's
//! behalf. Authorization for that D-Bus call is a **polkit** rule
//! (`deploy/packer/files/polkit/49-oxidant-connector-lifecycle.rules`) scoped to the `oxidant`
//! user and to unit names matching `oxidant-connector-*.service`, installed by
//! `deploy/packer/scripts/provision.sh`. `NoNewPrivileges` does not affect this: it constrains
//! *this process* gaining privileges via `execve`, not an already-privileged daemon acting on an
//! authorized IPC request.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use oxidant_config::OxidantConfig;
use oxidant_streaming::{checkpoint_store, Engine};
use serde::Deserialize;
use serde_json::json;

use crate::{routes::AppState, status};

/// Every pipeline config this route will act on must resolve under here. Mirrors the
/// platform's own `CONFIG_DIR` constant (`server/src/connectors/mod.rs` in
/// `oxidant-platform`) — the two must agree, since the platform is what tells a caller what
/// `config_path` to send.
pub const CONFIG_DIR: &str = "/etc/oxidant/connectors";

/// Overrides [`CONFIG_DIR`] — unset in production. Exists so a router-level test can drive the
/// real handler (auth, parsing, validation, discovery) against a fixture directory instead of
/// the real `/etc/oxidant/connectors`, which a test process has no business creating.
pub const CONFIG_DIR_ENV: &str = "OXIDANT_CONNECTOR_CONFIG_DIR";

/// Where systemd unit files are installed on the AMI (`deploy/packer/scripts/provision.sh`).
pub const UNIT_DIR: &str = "/etc/systemd/system";

/// Overrides [`UNIT_DIR`] — unset in production. Same reasoning as [`CONFIG_DIR_ENV`]:
/// [`discover_unit`] itself takes a directory explicitly, but the handler still needs
/// something to pass it when driven through the router rather than called directly.
pub const UNIT_DIR_ENV: &str = "OXIDANT_SYSTEMD_UNIT_DIR";

fn config_dir() -> PathBuf {
    env_dir(CONFIG_DIR_ENV).unwrap_or_else(|| PathBuf::from(CONFIG_DIR))
}

fn unit_dir() -> PathBuf {
    env_dir(UNIT_DIR_ENV).unwrap_or_else(|| PathBuf::from(UNIT_DIR))
}

fn env_dir(var: &str) -> Option<PathBuf> {
    std::env::var(var)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleCommand {
    Start,
    Pause,
    Resume,
    Resnapshot,
}

impl LifecycleCommand {
    fn as_str(self) -> &'static str {
        match self {
            LifecycleCommand::Start => "start",
            LifecycleCommand::Pause => "pause",
            LifecycleCommand::Resume => "resume",
            LifecycleCommand::Resnapshot => "resnapshot",
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct LifecycleRequest {
    pub command: LifecycleCommand,
    /// The connector's name — used only in error messages here; the config it names is what
    /// everything else is checked against.
    #[serde(default)]
    pub name: String,
    pub config_path: String,
    /// Checked against the pipeline's own declared root when present; never substituted for it.
    #[serde(default)]
    pub checkpoint_root: Option<String>,
    /// Empty means "every table this pipeline declares" — see the module docs on every-table
    /// honesty. Non-empty must be a subset of the pipeline's declared tables.
    #[serde(default)]
    pub tables: Vec<String>,
}

fn error_response(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({ "error": message.into() }))).into_response()
}

/// `POST /api/v1/pipelines/lifecycle`.
///
/// Authorize, then parse — same rule as [`crate::pipelines`] and `oxidant-connect`'s log
/// routes: the body is taken as raw [`Bytes`] rather than a `Json<T>` extractor so a malformed
/// body cannot 400 an unauthenticated caller before the auth gate runs (which would leak that
/// the route exists).
pub async fn pipeline_lifecycle(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(denied) = status::denied(&state, &headers) {
        return denied;
    }

    let request: LifecycleRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, format!("invalid request: {e}")),
    };

    let config_path = match validate_config_path(&request.config_path) {
        Ok(p) => p,
        Err(resp) => return *resp,
    };

    let config = match OxidantConfig::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            return error_response(
                StatusCode::NOT_FOUND,
                format!("config `{}` did not load: {e}", config_path.display()),
            )
        }
    };
    let Some(pipeline) = config.pipeline.as_ref() else {
        return error_response(
            StatusCode::BAD_REQUEST,
            format!("`{}` declares no `pipeline:` block", config_path.display()),
        );
    };

    if let Some(claimed_root) = request.checkpoint_root.as_deref() {
        if !roots_match(claimed_root, &pipeline.checkpoints) {
            return error_response(
                StatusCode::FORBIDDEN,
                "checkpoint_root does not match this pipeline's configured root — refusing to \
                 act on a caller-supplied root",
            );
        }
    }

    let declared: HashSet<&str> = config.tables.iter().map(|t| t.name.as_str()).collect();
    let tables: Vec<String> = if request.tables.is_empty() {
        // Every declared table, not the first one — see the module docs.
        config.tables.iter().map(|t| t.name.clone()).collect()
    } else {
        for table in &request.tables {
            if !valid_table_name(table) {
                return error_response(
                    StatusCode::FORBIDDEN,
                    format!("table name `{table}` is not [A-Za-z0-9_]+"),
                );
            }
            if !declared.contains(table.as_str()) {
                return error_response(
                    StatusCode::FORBIDDEN,
                    format!(
                        "table `{table}` is not declared by `{}`",
                        config_path.display()
                    ),
                );
            }
        }
        request.tables.clone()
    };

    let unit_dir_path = unit_dir();
    let unit = match tokio::task::spawn_blocking({
        let config_path = config_path.clone();
        move || discover_unit(&unit_dir_path, &config_path)
    })
    .await
    {
        Ok(Ok(Some(unit))) => unit,
        Ok(Ok(None)) => {
            return error_response(
                StatusCode::NOT_FOUND,
                format!(
                    "no systemd unit under {} runs `{}`",
                    unit_dir().display(),
                    config_path.display()
                ),
            )
        }
        Ok(Err(e)) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("{}: {e}", unit_dir().display()),
            )
        }
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    match request.command {
        LifecycleCommand::Start | LifecycleCommand::Resume => {
            match run_systemctl("start", &unit).await {
                Ok(()) => success(&unit, request.command, "started", None),
                Err(e) => error_response(StatusCode::BAD_GATEWAY, e),
            }
        }
        LifecycleCommand::Pause => match run_systemctl("stop", &unit).await {
            Ok(()) => success(&unit, request.command, "stopped", None),
            Err(e) => error_response(StatusCode::BAD_GATEWAY, e),
        },
        LifecycleCommand::Resnapshot => {
            let deleted = match delete_checkpoints(&pipeline.checkpoints, &tables).await {
                Ok(n) => n,
                Err(e) => {
                    return error_response(
                        StatusCode::BAD_GATEWAY,
                        format!(
                            "deleting checkpoints under `{}` for {}: {e}",
                            pipeline.checkpoints,
                            tables.join(", ")
                        ),
                    )
                }
            };
            // `delete_checkpoints` only returns `Ok` once every requested table's prefix is
            // gone, so a restart failure here is strictly "the delete finished, the restart
            // didn't" — the operator needs exactly that sentence, not a guess at which half of
            // the operation landed.
            match run_systemctl("restart", &unit).await {
                Ok(()) => success(&unit, request.command, "restarted", Some(deleted)),
                Err(e) => error_response(
                    StatusCode::BAD_GATEWAY,
                    format!(
                        "checkpoints deleted for all {} requested table{} ({deleted} object{} \
                         removed), but restarting {unit} failed: {e}",
                        tables.len(),
                        if tables.len() == 1 { "" } else { "s" },
                        if deleted == 1 { "" } else { "s" },
                    ),
                ),
            }
        }
    }
}

fn success(
    unit: &str,
    command: LifecycleCommand,
    action: &str,
    checkpoints_deleted: Option<usize>,
) -> Response {
    Json(json!({
        "unit": unit,
        "command": command.as_str(),
        "action": action,
        "checkpointsDeleted": checkpoints_deleted,
    }))
    .into_response()
}

/// `raw` must canonicalize to a path under the configured connectors directory ([`config_dir`])
/// — no traversal, no pointing this route at an arbitrary file on the box.
fn validate_config_path(raw: &str) -> Result<PathBuf, Box<Response>> {
    resolve_config_path(&config_dir(), raw)
}

/// The logic [`validate_config_path`] runs, with the root taken as a parameter — what a test
/// exercises directly, against a fixture directory, rather than the real `config_dir()`.
///
/// Boxes the `Err` variant: `Response` is ~130+ bytes, and every `Ok` return here would
/// otherwise pay that size regardless of which variant it is (`clippy::result_large_err`).
fn resolve_config_path(root_dir: &Path, raw: &str) -> Result<PathBuf, Box<Response>> {
    let root = root_dir.canonicalize().map_err(|e| {
        Box::new(error_response(
            StatusCode::NOT_FOUND,
            format!("{}: {e}", root_dir.display()),
        ))
    })?;
    let candidate = Path::new(raw).canonicalize().map_err(|e| {
        Box::new(error_response(
            StatusCode::NOT_FOUND,
            format!("config_path `{raw}` does not exist: {e}"),
        ))
    })?;
    if !candidate.starts_with(&root) {
        return Err(Box::new(error_response(
            StatusCode::FORBIDDEN,
            format!("config_path must resolve under {}", root_dir.display()),
        )));
    }
    Ok(candidate)
}

/// `[A-Za-z0-9_]+`, nothing else — this is the component of an object-store key deleted below,
/// so it must never carry `/`, `.`, or anything a path-like value could exploit.
fn valid_table_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Whether a caller-supplied checkpoint root names the same location as the pipeline's own,
/// modulo a trailing slash — the one piece of formatting variance worth tolerating on a value
/// that is otherwise compared as an opaque string, not resolved: resolving it here would run a
/// second, possibly divergent, interpretation of "the same root" from the one actually used.
fn roots_match(claimed: &str, configured: &str) -> bool {
    claimed.trim().trim_end_matches('/') == configured.trim().trim_end_matches('/')
}

/// The unit under `dir` whose `ExecStart=` runs `... --config <config_path>` (or
/// `--config=<config_path>` / `-c <config_path>`), matched by canonical path rather than by
/// string equality — a unit file may reasonably use a different (but equivalent) spelling of
/// the same path.
///
/// Splits `ExecStart=` on whitespace, which is enough for the units this project generates
/// (paths and unit names with no embedded spaces); it does not implement systemd's full
/// quoting/escaping grammar for `ExecStart=`.
fn discover_unit(dir: &Path, config_path: &Path) -> std::io::Result<Option<String>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("service") {
            continue;
        }
        let Ok(content) = std::fs::read_to_string(&path) else {
            continue;
        };
        for line in content.lines() {
            let Some(exec_start) = line.trim().strip_prefix("ExecStart=") else {
                continue;
            };
            if exec_start_targets_config(exec_start, config_path) {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    return Ok(Some(name.to_string()));
                }
            }
        }
    }
    Ok(None)
}

/// Whether `exec_start`'s `--config` / `-c` argument names `config_path`.
fn exec_start_targets_config(exec_start: &str, config_path: &Path) -> bool {
    let tokens: Vec<&str> = exec_start.split_whitespace().collect();
    for (i, tok) in tokens.iter().enumerate() {
        let candidate = if let Some(v) = tok.strip_prefix("--config=") {
            Some(v)
        } else if *tok == "--config" || *tok == "-c" {
            tokens.get(i + 1).copied()
        } else {
            None
        };
        let Some(candidate) = candidate else { continue };
        if paths_match(Path::new(candidate), config_path) {
            return true;
        }
    }
    false
}

/// Canonicalized comparison where possible, falling back to a literal one when either side
/// will not canonicalize — the ordinary case in a unit test fixture where neither path exists
/// on the real filesystem.
fn paths_match(a: &Path, b: &Path) -> bool {
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(a), Ok(b)) => a == b,
        _ => a == b,
    }
}

/// `systemctl <action> <unit>` — an argv array, never a shell string. Runs on a blocking
/// thread: this is a synchronous child-process call inside an async handler.
async fn run_systemctl(action: &'static str, unit: &str) -> Result<(), String> {
    let unit = unit.to_string();
    let unit_for_panic_msg = unit.clone();
    tokio::task::spawn_blocking(move || {
        let output = std::process::Command::new("systemctl")
            .arg(action)
            .arg(&unit)
            .output()
            .map_err(|e| format!("invoking systemctl {action} {unit}: {e}"))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(format!(
                "systemctl {action} {unit} exited {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ))
        }
    })
    .await
    .map_err(|e| format!("systemctl {action} {unit_for_panic_msg}: task panicked: {e}"))?
}

/// Delete `{root}/{table}` for every table in `tables`, through the same
/// [`oxidant_streaming::checkpoint_store`] resolver the pipeline itself checkpoints through —
/// never a shell `rm -rf`. Returns the total number of objects removed.
async fn delete_checkpoints(root: &str, tables: &[String]) -> Result<usize, String> {
    let store = checkpoint_store(&Engine::default(), root).map_err(|e| e.to_string())?;
    let mut deleted = 0usize;
    for table in tables {
        let table_store = store.child(table);
        let objects = table_store
            .list("")
            .await
            .map_err(|e| format!("listing checkpoints for `{table}`: {e}"))?;
        for object in objects {
            table_store
                .remove(&object.name)
                .await
                .map_err(|e| format!("deleting `{table}/{}`: {e}", object.name))?;
            deleted += 1;
        }
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_names_are_alnum_underscore_only() {
        for ok in ["orders", "orders_live", "T1", "a_b_c"] {
            assert!(valid_table_name(ok), "{ok} should be accepted");
        }
        for bad in [
            "",
            "orders-live",
            "orders.live",
            "a/b",
            "../etc",
            "a b",
            "a\0b",
        ] {
            assert!(!valid_table_name(bad), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn checkpoint_roots_match_modulo_trailing_slash() {
        assert!(roots_match("/srv/ckpt", "/srv/ckpt/"));
        assert!(roots_match("/srv/ckpt/", "/srv/ckpt"));
        assert!(roots_match(" /srv/ckpt ", "/srv/ckpt"));
        assert!(!roots_match("/srv/ckpt", "/srv/other"));
        assert!(!roots_match("s3://bucket/a", "s3://bucket/b"));
    }

    #[test]
    fn config_path_must_resolve_under_the_connectors_directory() {
        let dir = std::env::temp_dir().join(format!("oxidant-lc-cfg-{}", uuid::Uuid::new_v4()));
        let connectors = dir.join("etc-oxidant-connectors");
        std::fs::create_dir_all(&connectors).unwrap();
        std::fs::write(connectors.join("orders.yaml"), "pipeline: {}\n").unwrap();
        let outside = dir.join("outside.yaml");
        std::fs::write(&outside, "pipeline: {}\n").unwrap();

        // A config inside the root resolves.
        let ok = resolve_config_path(
            &connectors,
            connectors.join("orders.yaml").to_str().unwrap(),
        );
        assert!(ok.is_ok(), "{ok:?}");

        // A `..` traversal that resolves outside the root is refused with 403, not 200.
        let traversal = resolve_config_path(
            &connectors,
            connectors.join("../outside.yaml").to_str().unwrap(),
        );
        let Err(resp) = traversal else {
            panic!("traversal should be refused");
        };
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // A path that does not exist at all is refused too, distinctly (404, not a panic on
        // `canonicalize`).
        let missing =
            resolve_config_path(&connectors, connectors.join("nope.yaml").to_str().unwrap());
        let Err(resp) = missing else {
            panic!("a missing config path should be refused");
        };
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn discover_unit_matches_by_exec_start_config_argument() {
        let dir = std::env::temp_dir().join(format!("oxidant-lc-units-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let config_path = dir.join("orders.yaml");
        std::fs::write(&config_path, "pipeline: {}\n").unwrap();

        std::fs::write(
            dir.join("oxidant-connector-orders.service"),
            format!(
                "[Unit]\nDescription=x\n[Service]\nExecStart=/usr/local/bin/oxidant pipeline run --config {}\n",
                config_path.display()
            ),
        )
        .unwrap();
        // A decoy unit for a different config must not match.
        std::fs::write(
            dir.join("oxidant-connector-other.service"),
            "[Service]\nExecStart=/usr/local/bin/oxidant pipeline run --config /etc/oxidant/connectors/other.yaml\n",
        )
        .unwrap();
        // Not a .service file — ignored even if it happens to mention the path.
        std::fs::write(
            dir.join("README.txt"),
            format!(
                "ExecStart=oxidant pipeline run --config {}\n",
                config_path.display()
            ),
        )
        .unwrap();

        let found = discover_unit(&dir, &config_path).unwrap();
        assert_eq!(found.as_deref(), Some("oxidant-connector-orders.service"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn discover_unit_does_not_assume_the_naming_scheme() {
        let dir = std::env::temp_dir().join(format!("oxidant-lc-units2-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let config_path = dir.join("orders.yaml");
        std::fs::write(&config_path, "pipeline: {}\n").unwrap();

        // A unit named nothing like `oxidant-connector-*` still matches, because discovery
        // goes by `ExecStart=`, not by name.
        std::fs::write(
            dir.join("my-custom-pipeline-unit.service"),
            format!(
                "[Service]\nExecStart=/usr/local/bin/oxidant pipeline run --config={}\n",
                config_path.display()
            ),
        )
        .unwrap();

        let found = discover_unit(&dir, &config_path).unwrap();
        assert_eq!(found.as_deref(), Some("my-custom-pipeline-unit.service"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn discover_unit_returns_none_when_nothing_matches() {
        let dir = std::env::temp_dir().join(format!("oxidant-lc-units3-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("oxidant-connector-other.service"),
            "[Service]\nExecStart=/usr/local/bin/oxidant pipeline run --config /etc/oxidant/connectors/other.yaml\n",
        )
        .unwrap();

        let missing = dir.join("orders.yaml");
        let found = discover_unit(&dir, &missing).unwrap();
        assert_eq!(found, None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn discover_unit_on_a_missing_directory_is_none_not_an_error() {
        let dir = std::env::temp_dir().join(format!("oxidant-lc-absent-{}", uuid::Uuid::new_v4()));
        let found = discover_unit(&dir, Path::new("/etc/oxidant/connectors/orders.yaml")).unwrap();
        assert_eq!(found, None);
    }

    /// The primitive [`delete_checkpoints`] composes: every object under `{root}/{table}` for
    /// every requested table, and nothing under a table that was not requested. This is the
    /// part of a resnapshot that touches data, proved directly against a real (local
    /// filesystem) checkpoint store rather than through the route, which also needs a real
    /// systemd unit to restart — see the module docs on why that part is manual proof instead.
    #[tokio::test]
    async fn delete_checkpoints_removes_every_object_under_each_requested_table_only() {
        let root = std::env::temp_dir().join(format!("oxidant-lc-ckpt-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join("orders/commits")).unwrap();
        std::fs::write(root.join("orders/offsets.json"), "{}").unwrap();
        std::fs::write(root.join("orders/commits/1"), "x").unwrap();
        std::fs::create_dir_all(root.join("clicks")).unwrap();
        std::fs::write(root.join("clicks/offsets.json"), "{}").unwrap();
        // Not requested — must survive untouched.
        std::fs::create_dir_all(root.join("untouched")).unwrap();
        std::fs::write(root.join("untouched/offsets.json"), "{}").unwrap();

        let deleted = delete_checkpoints(
            root.to_str().unwrap(),
            &["orders".to_string(), "clicks".to_string()],
        )
        .await
        .unwrap();
        assert_eq!(
            deleted, 3,
            "orders/offsets.json + orders/commits/1 + clicks/offsets.json"
        );

        assert!(!root.join("orders/offsets.json").exists());
        assert!(!root.join("orders/commits/1").exists());
        assert!(!root.join("clicks/offsets.json").exists());
        assert!(
            root.join("untouched/offsets.json").exists(),
            "a table that was not in the request must not be touched"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    /* ---------- POST /api/v1/pipelines/lifecycle, through the real router ---------- */

    use axum::http::header;
    use axum::{body::Body, routing::post as post_route, Router};
    use http_body_util::BodyExt;
    use oxidant_observability::AppStateStore;
    use serde_json::Value;
    use std::sync::Arc;
    use tower::ServiceExt;

    const TOKEN: &str = "s3cret-status-token";

    /// Serializes every test that points [`CONFIG_DIR_ENV`] / [`UNIT_DIR_ENV`] at a fixture —
    /// they mutate process-wide environment, same rule as the sibling `ENV_LOCK`s in
    /// `pipelines.rs` and `status.rs`.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn router(token: Option<&str>) -> Router {
        let state = AppState {
            store: Arc::new(AppStateStore::new()),
            status_token: token.map(Into::into),
            logs: None,
        };
        Router::new()
            .route(
                "/api/v1/pipelines/lifecycle",
                post_route(pipeline_lifecycle),
            )
            .with_state(state)
    }

    async fn post(app: Router, body: Value, auth: Option<&str>) -> (StatusCode, Value) {
        let mut req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/pipelines/lifecycle")
            .header(header::CONTENT_TYPE, "application/json");
        if let Some(auth) = auth {
            req = req.header(header::AUTHORIZATION, auth);
        }
        let resp = app
            .oneshot(req.body(Body::from(body.to_string())).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    fn bearer() -> String {
        format!("Bearer {TOKEN}")
    }

    /// A minimal fixture: a connectors directory holding one valid pipeline config, and
    /// (usually empty) unit directory. Returns `(base_dir, config_dir, unit_dir, config_path)`.
    fn fixture(tables: &[&str], checkpoints: &Path) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
        let base =
            std::env::temp_dir().join(format!("oxidant-lc-fixture-{}", uuid::Uuid::new_v4()));
        let config_dir = base.join("connectors");
        let unit_dir = base.join("units");
        let warehouse = base.join("warehouse");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::create_dir_all(&unit_dir).unwrap();
        std::fs::create_dir_all(&warehouse).unwrap();
        std::fs::create_dir_all(checkpoints).unwrap();

        let table_yaml: String = tables
            .iter()
            .map(|t| format!("  - name: {t}\n    sql: \"SELECT 1\"\n"))
            .collect();
        let config_path = config_dir.join("orders.yaml");
        std::fs::write(
            &config_path,
            format!(
                "catalogs:\n  local:\n    type: local\n    warehouse: {}\n\
                 pipeline:\n  name: orders\n  catalog: local\n  schema: live\n  checkpoints: {}\n\
                 tables:\n{table_yaml}",
                warehouse.display(),
                checkpoints.display(),
            ),
        )
        .unwrap();
        (base, config_dir, unit_dir, config_path)
    }

    fn set_dirs(config_dir: &Path, unit_dir: &Path) {
        std::env::set_var(CONFIG_DIR_ENV, config_dir);
        std::env::set_var(UNIT_DIR_ENV, unit_dir);
    }

    fn clear_dirs() {
        std::env::remove_var(CONFIG_DIR_ENV);
        std::env::remove_var(UNIT_DIR_ENV);
    }

    /// Same posture as `/api/v1/pipelines` and `/api/status`: no token, no route.
    #[tokio::test]
    async fn lifecycle_route_is_gated_like_every_other_operational_route() {
        let (status, _) = post(
            router(None),
            json!({"command": "pause", "name": "orders", "config_path": "/etc/oxidant/connectors/orders.yaml"}),
            Some(&bearer()),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "no token configured");

        for auth in [None, Some("Bearer wrong")] {
            let (status, _) = post(
                router(Some(TOKEN)),
                json!({"command": "pause", "name": "orders", "config_path": "/etc/oxidant/connectors/orders.yaml"}),
                auth,
            )
            .await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{auth:?}");
        }
    }

    /// Auth runs before the body is ever parsed — an unauthenticated caller must not learn
    /// "the route exists but your JSON is wrong" (400) versus "the route does not exist" (404).
    #[tokio::test]
    async fn malformed_json_answers_400_only_once_authenticated() {
        let (status, _) = post(router(None), json!("not an object"), Some(&bearer())).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "auth gate wins over a bad body"
        );

        let (status, body) =
            post(router(Some(TOKEN)), json!("not an object"), Some(&bearer())).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body["error"].as_str().unwrap().contains("invalid request"));
    }

    #[tokio::test]
    // ENV_LOCK serializes this test's process-global env mutation against any sibling that
    // grows one; the guard must therefore span the awaits it protects.
    #[allow(clippy::await_holding_lock)]
    async fn refuses_a_config_path_that_resolves_outside_the_connectors_directory() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let checkpoints =
            std::env::temp_dir().join(format!("oxidant-lc-ckpt-{}", uuid::Uuid::new_v4()));
        let (base, config_dir, unit_dir, _config_path) = fixture(&["orders"], &checkpoints);
        // A file that exists, but outside `config_dir` — the traversal a symlink or a `..`
        // could also produce.
        let outside = base.join("outside.yaml");
        std::fs::write(&outside, "pipeline: {}\n").unwrap();
        set_dirs(&config_dir, &unit_dir);

        let (status, body) = post(
            router(Some(TOKEN)),
            json!({
                "command": "pause",
                "name": "orders",
                "config_path": outside.to_str().unwrap(),
            }),
            Some(&bearer()),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(body["error"]
            .as_str()
            .unwrap()
            .contains("must resolve under"));

        clear_dirs();
        let _ = std::fs::remove_dir_all(&base);
        let _ = std::fs::remove_dir_all(&checkpoints);
    }

    #[tokio::test]
    // ENV_LOCK serializes this test's process-global env mutation against any sibling that
    // grows one; the guard must therefore span the awaits it protects.
    #[allow(clippy::await_holding_lock)]
    async fn refuses_a_table_name_with_disallowed_characters() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let checkpoints =
            std::env::temp_dir().join(format!("oxidant-lc-ckpt-{}", uuid::Uuid::new_v4()));
        let (base, config_dir, unit_dir, config_path) = fixture(&["orders"], &checkpoints);
        set_dirs(&config_dir, &unit_dir);

        let (status, body) = post(
            router(Some(TOKEN)),
            json!({
                "command": "resnapshot",
                "name": "orders",
                "config_path": config_path.to_str().unwrap(),
                "tables": ["orders; rm -rf /"],
            }),
            Some(&bearer()),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(body["error"].as_str().unwrap().contains("A-Za-z0-9_"));

        clear_dirs();
        let _ = std::fs::remove_dir_all(&base);
        let _ = std::fs::remove_dir_all(&checkpoints);
    }

    #[tokio::test]
    // ENV_LOCK serializes this test's process-global env mutation against any sibling that
    // grows one; the guard must therefore span the awaits it protects.
    #[allow(clippy::await_holding_lock)]
    async fn refuses_a_table_the_pipeline_does_not_declare() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let checkpoints =
            std::env::temp_dir().join(format!("oxidant-lc-ckpt-{}", uuid::Uuid::new_v4()));
        let (base, config_dir, unit_dir, config_path) = fixture(&["orders"], &checkpoints);
        set_dirs(&config_dir, &unit_dir);

        let (status, body) = post(
            router(Some(TOKEN)),
            json!({
                "command": "resnapshot",
                "name": "orders",
                "config_path": config_path.to_str().unwrap(),
                "tables": ["not_declared"],
            }),
            Some(&bearer()),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(body["error"].as_str().unwrap().contains("not declared"));

        clear_dirs();
        let _ = std::fs::remove_dir_all(&base);
        let _ = std::fs::remove_dir_all(&checkpoints);
    }

    #[tokio::test]
    // ENV_LOCK serializes this test's process-global env mutation against any sibling that
    // grows one; the guard must therefore span the awaits it protects.
    #[allow(clippy::await_holding_lock)]
    async fn refuses_a_caller_supplied_checkpoint_root_that_does_not_match_the_config() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let checkpoints =
            std::env::temp_dir().join(format!("oxidant-lc-ckpt-{}", uuid::Uuid::new_v4()));
        let (base, config_dir, unit_dir, config_path) = fixture(&["orders"], &checkpoints);
        set_dirs(&config_dir, &unit_dir);

        let (status, body) = post(
            router(Some(TOKEN)),
            json!({
                "command": "resnapshot",
                "name": "orders",
                "config_path": config_path.to_str().unwrap(),
                "checkpoint_root": "/some/other/root",
            }),
            Some(&bearer()),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(body["error"]
            .as_str()
            .unwrap()
            .contains("does not match this pipeline's configured root"));

        clear_dirs();
        let _ = std::fs::remove_dir_all(&base);
        let _ = std::fs::remove_dir_all(&checkpoints);
    }

    #[tokio::test]
    // ENV_LOCK serializes this test's process-global env mutation against any sibling that
    // grows one; the guard must therefore span the awaits it protects.
    #[allow(clippy::await_holding_lock)]
    async fn answers_404_when_no_unit_runs_this_config() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let checkpoints =
            std::env::temp_dir().join(format!("oxidant-lc-ckpt-{}", uuid::Uuid::new_v4()));
        let (base, config_dir, unit_dir, config_path) = fixture(&["orders"], &checkpoints);
        set_dirs(&config_dir, &unit_dir); // unit_dir is empty — nothing declares this config

        let (status, body) = post(
            router(Some(TOKEN)),
            json!({
                "command": "start",
                "name": "orders",
                "config_path": config_path.to_str().unwrap(),
            }),
            Some(&bearer()),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body["error"].as_str().unwrap().contains("no systemd unit"));

        clear_dirs();
        let _ = std::fs::remove_dir_all(&base);
        let _ = std::fs::remove_dir_all(&checkpoints);
    }

    /// Every-table honesty end to end: an empty `tables` on a resnapshot deletes *every*
    /// declared table's checkpoint prefix, not just the first one. The final `systemctl
    /// restart` fails (there is no such unit on the machine running this test — see the module
    /// docs), which is the expected, and separately asserted, partial-failure sentence: the
    /// checkpoints are provably gone by the time that failure is reported.
    #[tokio::test]
    // ENV_LOCK serializes this test's process-global env mutation against any sibling that
    // grows one; the guard must therefore span the awaits it protects.
    #[allow(clippy::await_holding_lock)]
    async fn resnapshot_with_no_tables_deletes_every_declared_table_then_reports_the_restart_failure(
    ) {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let checkpoints =
            std::env::temp_dir().join(format!("oxidant-lc-ckpt-{}", uuid::Uuid::new_v4()));
        let (base, config_dir, unit_dir, config_path) =
            fixture(&["orders", "clicks"], &checkpoints);
        std::fs::write(checkpoints.join("orders_offsets.json"), "{}").unwrap();
        std::fs::create_dir_all(checkpoints.join("orders")).unwrap();
        std::fs::write(checkpoints.join("orders/offsets.json"), "{}").unwrap();
        std::fs::create_dir_all(checkpoints.join("clicks")).unwrap();
        std::fs::write(checkpoints.join("clicks/offsets.json"), "{}").unwrap();
        // A real unit whose ExecStart points at this config, so discovery succeeds; systemctl
        // itself will still fail because no such unit is actually loaded.
        std::fs::write(
            unit_dir.join("oxidant-connector-orders.service"),
            format!(
                "[Service]\nExecStart=/usr/local/bin/oxidant pipeline run --config {}\n",
                config_path.display()
            ),
        )
        .unwrap();
        set_dirs(&config_dir, &unit_dir);

        let (status, body) = post(
            router(Some(TOKEN)),
            json!({
                "command": "resnapshot",
                "name": "orders",
                "config_path": config_path.to_str().unwrap(),
            }),
            Some(&bearer()),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
        let message = body["error"].as_str().unwrap();
        assert!(
            message.contains("checkpoints deleted for all 2 requested tables (2 objects removed)"),
            "{message}"
        );
        assert!(
            message.contains("restarting oxidant-connector-orders.service failed"),
            "{message}"
        );

        assert!(!checkpoints.join("orders/offsets.json").exists());
        assert!(!checkpoints.join("clicks/offsets.json").exists());
        // Not a declared table's checkpoint — never touched, and never should be by anything
        // named `_offsets.json` at the root either.
        assert!(checkpoints.join("orders_offsets.json").exists());

        clear_dirs();
        let _ = std::fs::remove_dir_all(&base);
        let _ = std::fs::remove_dir_all(&checkpoints);
    }
}
