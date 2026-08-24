//! Where the journal lives, what bounds it, and how the SQL text is written.
//!
//! Every knob here is read once at boot into a [`HistoryConfig`]; nothing downstream reads the
//! environment again, so a test can build a config for a `tempdir` without touching process
//! state. See `docs/query-history-durability.md` §3.

#[cfg(test)]
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

/// `OXIDANT_HISTORY_SQL`: how much of the query text reaches the journal.
///
/// The default is `text`, i.e. *off* — the journal keeps 30 days of raw SQL, which is a real
/// change in exposure over today's 1000-entry memory ring, and the two other modes exist so an
/// operator can trade query text away without turning history off entirely.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SqlMode {
    /// Journal the query verbatim.
    Text,
    /// Journal the query with credential-shaped literals replaced.
    Redacted,
    /// Journal a `sha256` digest plus the first 120 characters (shape without the text).
    Hash,
}

impl Default for SqlMode {
    fn default() -> Self {
        Self::Text
    }
}

impl SqlMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Redacted => "redacted",
            Self::Hash => "hash",
        }
    }

    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "text" => Some(Self::Text),
            "redacted" => Some(Self::Redacted),
            "hash" => Some(Self::Hash),
            _ => None,
        }
    }

    /// Encode `sql` for the journal under this mode.
    pub(crate) fn encode(self, sql: &str) -> String {
        match self {
            Self::Text => sql.to_string(),
            Self::Redacted => redact_sql(sql),
            Self::Hash => {
                use sha2::{Digest, Sha256};
                let digest = Sha256::digest(sql.as_bytes());
                let head: String = sql.chars().take(120).collect();
                format!("sha256:{digest:x} {head}")
            }
        }
    }
}

/// `OXIDANT_RESULT_PERSIST`: when a terminal result's rows are written to
/// `history/results/<statement-id>.arrow`.
///
/// The default is `on_pressure`: rows stay in memory until the in-memory budget
/// ([`HistoryConfig::result_memory_budget_bytes`]) would be exceeded, and the oldest terminal
/// result is spilled to make room. `always` spills every terminal result as it lands, which is
/// what makes `/result` answer across a restart for a *quiet* driver too. `never` writes nothing
/// — and, because nothing is ever durable under it, never releases in-memory rows either: the
/// count cap and the hot TTL bound memory exactly as they did before this design.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResultPersist {
    Always,
    OnPressure,
    Never,
}

impl Default for ResultPersist {
    fn default() -> Self {
        Self::OnPressure
    }
}

impl ResultPersist {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Always => "always",
            Self::OnPressure => "on_pressure",
            Self::Never => "never",
        }
    }

    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "always" => Some(Self::Always),
            "on_pressure" | "onpressure" | "on-pressure" => Some(Self::OnPressure),
            "never" | "off" => Some(Self::Never),
            _ => None,
        }
    }

    /// Does a terminal result get written to disk the moment it lands?
    pub(crate) fn spills_eagerly(self) -> bool {
        matches!(self, Self::Always)
    }

    /// May a result ever reach the disk under this mode?
    pub(crate) fn spills_at_all(self) -> bool {
        !matches!(self, Self::Never)
    }
}

/// Replace credential-shaped values in a SQL string.
///
/// Two shapes, both of which appear in real DDL and both of which the review named:
///
/// - a quoted literal introduced by a credential-shaped identifier — `OPTIONS(secret 'abc')`,
///   `WITH (aws_access_key_id = 'AKIA…')`. Whether an identifier is credential-shaped is
///   [`oxidant_observability::is_secret_key`], the same list `/api/v1/environment` redacts with;
/// - URL userinfo — `s3://key:secret@bucket/path`.
///
/// This is a redaction, not a parser: it is deliberately conservative about what it blanks and
/// makes no claim to catch a secret an operator hid somewhere novel.
pub(crate) fn redact_sql(sql: &str) -> String {
    let out = redact_quoted_after_secret_ident(sql);
    redact_url_userinfo(&out)
}

fn redact_quoted_after_secret_ident(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.char_indices().peekable();
    // The most recent identifier seen; a quoted literal is blanked when it was credential-shaped.
    let mut armed = false;
    while let Some((i, c)) = chars.next() {
        if c == '\'' {
            // Consume the whole literal, honouring the doubled-quote escape.
            let mut literal = String::from("'");
            let mut closed = false;
            while let Some((_, lc)) = chars.next() {
                literal.push(lc);
                if lc == '\'' {
                    if matches!(chars.peek(), Some((_, '\''))) {
                        let (_, esc) = chars.next().expect("peeked");
                        literal.push(esc);
                        continue;
                    }
                    closed = true;
                    break;
                }
            }
            if armed && closed {
                out.push('\'');
                out.push_str(oxidant_observability::REDACTED);
                out.push('\'');
            } else {
                out.push_str(&literal);
            }
            armed = false;
            continue;
        }
        if c.is_alphanumeric() || c == '_' {
            let start = i;
            let mut end = i + c.len_utf8();
            while let Some((j, nc)) = chars.peek().copied() {
                if nc.is_alphanumeric() || nc == '_' {
                    end = j + nc.len_utf8();
                    chars.next();
                } else {
                    break;
                }
            }
            let word = &sql[start..end];
            armed = oxidant_observability::is_secret_key(word);
            out.push_str(word);
            continue;
        }
        // `=`, `(`, whitespace and friends sit between the identifier and its literal and must
        // not disarm it; anything else does.
        if !matches!(c, '=' | '(' | ' ' | '\t' | '\n' | '\r' | ',' | ':') {
            armed = false;
        }
        out.push(c);
    }
    out
}

fn redact_url_userinfo(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut rest = sql;
    while let Some(pos) = rest.find("://") {
        let after = &rest[pos + 3..];
        // Userinfo ends at the first `@` and cannot contain a delimiter or whitespace.
        let end = after
            .find(|c: char| c.is_whitespace() || matches!(c, '/' | '\'' | '"' | ')' | ','))
            .unwrap_or(after.len());
        let authority = &after[..end];
        match authority.find('@') {
            Some(at) if authority[..at].contains(':') => {
                out.push_str(&rest[..pos + 3]);
                out.push_str(oxidant_observability::REDACTED);
                out.push('@');
                rest = &after[at + 1..];
            }
            _ => {
                out.push_str(&rest[..pos + 3]);
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out
}

/// Journal segment roll size (§3). Overridable so a test does not have to write 64 MiB.
const DEFAULT_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
/// `OXIDANT_RESULT_MAX_BYTES` (§3) — the per-file ceiling on a spilled result.
const DEFAULT_RESULT_MAX_BYTES: u64 = 256 * 1024 * 1024;
/// `OXIDANT_RESULT_MEMORY_BUDGET_BYTES` (F8) — the in-memory ceiling across all retained results.
const DEFAULT_RESULT_MEMORY_BUDGET_BYTES: u64 = 512 * 1024 * 1024;
/// `OXIDANT_DISK_MAX_BYTES` (§3) — everything the engine owns under the root, combined.
const DEFAULT_DISK_MAX_BYTES: u64 = 8 * 1024 * 1024 * 1024;
/// `OXIDANT_DISK_MIN_FREE_BYTES` (§3) — the filesystem free-space floor.
const DEFAULT_DISK_MIN_FREE_BYTES: u64 = 1024 * 1024 * 1024;

/// Resolved statement-history configuration.
#[derive(Clone, Debug)]
pub(crate) struct HistoryConfig {
    /// `OXIDANT_HISTORY=off` leaves this false and the store reverts to today's memory-only path.
    pub enabled: bool,
    /// `$OXIDANT_DATA_DIR` — the single root knob.
    pub root: PathBuf,
    /// `<history-dir>/statements`, the journal's own directory.
    pub statements_dir: PathBuf,
    /// `<statements-dir>/compacted`.
    pub compacted_dir: PathBuf,
    /// Interval fsync for records nobody is waiting on.
    pub flush_interval: Duration,
    /// How long a response waits for its terminal record to be durable before degrading.
    pub ack_timeout: Duration,
    /// Hot-tier TTL — today's `STATEMENT_TTL`.
    pub hot_ttl: Duration,
    /// Bound on folded records in the history tier (and on the hot tier).
    pub max_records: usize,
    /// Bound on any one session's share of `max_records`.
    pub max_per_session: usize,
    /// Age at which a terminal statement is pruned.
    pub retention_days: i64,
    pub sql_mode: SqlMode,
    pub segment_max_bytes: u64,
    /// `<history-dir>/results`, or `OXIDANT_RESULT_DIR` — spilled Arrow IPC results (§5).
    pub results_dir: PathBuf,
    /// `<root>/logs`, or `OXIDANT_LOG_DIR`. PR2 does not *write* here (the rolling writer is
    /// PR3); the disk sweeper prunes it, which is why the path is resolved now.
    pub logs_dir: PathBuf,
    /// `<root>/dumps`, or `OXIDANT_DUMP_DIR` — §6b support bundles, swept like results.
    pub dumps_dir: PathBuf,
    pub result_persist: ResultPersist,
    /// `OXIDANT_RESULT_MAX_BYTES` — a result whose Arrow IPC encoding exceeds this is refused
    /// and the statement records `result_too_large` instead of a pointer.
    pub result_max_bytes: u64,
    /// `OXIDANT_RESULT_MEMORY_BUDGET_BYTES` — the in-memory ceiling across *all* retained
    /// results that `on_pressure` triggers on (F8).
    pub result_memory_budget_bytes: u64,
    /// `OXIDANT_DISK_MAX_BYTES` — total budget for everything the engine owns under the root.
    pub disk_max_bytes: u64,
    /// `OXIDANT_DISK_MIN_FREE_BYTES` — filesystem free-space floor.
    pub disk_min_free_bytes: u64,
    /// How often the disk-budget sweeper runs (§3: every 5 minutes, plus at boot).
    pub disk_sweep_interval: Duration,
    /// A fake free-space reading for the whole tree (tests only).
    ///
    /// The free-space floor is the one guard that cannot be driven from a tempdir — it depends
    /// on how full the *host* volume is — and it is also the guard whose misbehaviour deleted
    /// the entire statement history (H1). Read through [`Self::free_bytes_override`], which is
    /// `None` in every non-test build.
    #[cfg(test)]
    pub free_bytes_override: Option<u64>,
    /// `driver` / `worker`, recorded in the lockfile.
    pub role: String,
    /// The process's port, recorded in the lockfile.
    pub port: u16,
}

impl HistoryConfig {
    /// Read the environment. `Err` is a boot failure: a misconfigured root must be loud, not
    /// silently turned into a literal `s3:/bucket/…` directory (§3, F20).
    pub(crate) fn from_env(role: &str, port: u16) -> Result<Self, String> {
        let enabled = !matches!(
            env_str("OXIDANT_HISTORY")
                .unwrap_or_else(|| "on".to_string())
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "off" | "0" | "false"
        );
        let mut root = match env_path("OXIDANT_DATA_DIR")? {
            Some(p) => p,
            None => default_root(),
        };
        if env_flag("OXIDANT_DATA_DIR_PER_PROCESS") {
            root = root.join(format!("{role}-{port}"));
        }
        let history_dir = env_path("OXIDANT_HISTORY_DIR")?.unwrap_or_else(|| root.join("history"));
        let statements_dir = history_dir.join("statements");
        let compacted_dir = statements_dir.join("compacted");
        // An explicit subtree override wins over the root, and an overridden subtree is still
        // counted against — and pruned by — the disk budget (§3, "Root and precedence").
        let results_dir =
            env_path("OXIDANT_RESULT_DIR")?.unwrap_or_else(|| history_dir.join("results"));
        let logs_dir = env_path("OXIDANT_LOG_DIR")?.unwrap_or_else(|| root.join("logs"));
        let dumps_dir = env_path("OXIDANT_DUMP_DIR")?.unwrap_or_else(|| root.join("dumps"));
        Ok(Self {
            enabled,
            root,
            statements_dir,
            compacted_dir,
            flush_interval: Duration::from_millis(env_u64("OXIDANT_HISTORY_FLUSH_MS", 500).max(1)),
            ack_timeout: Duration::from_millis(env_u64("OXIDANT_HISTORY_ACK_TIMEOUT_MS", 2000)),
            hot_ttl: Duration::from_secs(env_u64("OXIDANT_HISTORY_HOT_TTL_SECS", 3600)),
            max_records: env_u64("OXIDANT_HISTORY_MAX_RECORDS", 10_000).max(1) as usize,
            max_per_session: env_u64("OXIDANT_HISTORY_MAX_PER_SESSION", 2_000).max(1) as usize,
            retention_days: env_u64("OXIDANT_HISTORY_RETENTION_DAYS", 30) as i64,
            sql_mode: env_str("OXIDANT_HISTORY_SQL")
                .and_then(|v| SqlMode::parse(&v))
                .unwrap_or(SqlMode::Text),
            segment_max_bytes: env_u64("OXIDANT_HISTORY_SEGMENT_BYTES", DEFAULT_SEGMENT_BYTES)
                .max(1),
            results_dir,
            logs_dir,
            dumps_dir,
            result_persist: env_str("OXIDANT_RESULT_PERSIST")
                .and_then(|v| ResultPersist::parse(&v))
                .unwrap_or_default(),
            result_max_bytes: env_u64("OXIDANT_RESULT_MAX_BYTES", DEFAULT_RESULT_MAX_BYTES),
            // `OXIDANT_RESULT_MEM_BYTES` is the name §3's knob table shipped with; the budget is
            // one number under two spellings rather than two knobs that can disagree.
            result_memory_budget_bytes: env_u64(
                "OXIDANT_RESULT_MEMORY_BUDGET_BYTES",
                env_u64(
                    "OXIDANT_RESULT_MEM_BYTES",
                    DEFAULT_RESULT_MEMORY_BUDGET_BYTES,
                ),
            ),
            disk_max_bytes: env_u64("OXIDANT_DISK_MAX_BYTES", DEFAULT_DISK_MAX_BYTES),
            disk_min_free_bytes: env_u64(
                "OXIDANT_DISK_MIN_FREE_BYTES",
                DEFAULT_DISK_MIN_FREE_BYTES,
            ),
            disk_sweep_interval: Duration::from_secs(
                env_u64("OXIDANT_DISK_SWEEP_SECS", 300).max(1),
            ),
            #[cfg(test)]
            free_bytes_override: None,
            role: role.to_string(),
            port,
        })
    }

    /// A config rooted at `root` with the shipped defaults — the seam tests build on.
    #[cfg(test)]
    pub(crate) fn for_root(root: &Path) -> Self {
        Self::for_root_with_history_dir(root, &root.join("history"))
    }

    /// `for_root`, with the `OXIDANT_HISTORY_DIR` override applied — the shape where the journal
    /// does *not* live under the root, which is what the data-dir lock has to reckon with.
    #[cfg(test)]
    pub(crate) fn for_root_with_history_dir(root: &Path, history_dir: &Path) -> Self {
        let statements_dir = history_dir.join("statements");
        Self {
            enabled: true,
            root: root.to_path_buf(),
            compacted_dir: statements_dir.join("compacted"),
            statements_dir,
            results_dir: history_dir.join("results"),
            logs_dir: root.join("logs"),
            dumps_dir: root.join("dumps"),
            result_persist: ResultPersist::default(),
            result_max_bytes: DEFAULT_RESULT_MAX_BYTES,
            result_memory_budget_bytes: DEFAULT_RESULT_MEMORY_BUDGET_BYTES,
            // Neutral disk guards, opted into per test. Inheriting the shipped 1 GiB floor
            // meant every history test's journal and results were at the mercy of how full
            // *this machine* happened to be — and this repo has a documented hazard of a
            // `target/` directory measured at 174 GB.
            disk_max_bytes: u64::MAX,
            disk_min_free_bytes: 0,
            disk_sweep_interval: Duration::from_secs(300),
            free_bytes_override: None,
            flush_interval: Duration::from_millis(500),
            ack_timeout: Duration::from_millis(2000),
            hot_ttl: Duration::from_secs(3600),
            max_records: 10_000,
            max_per_session: 2_000,
            retention_days: 30,
            sql_mode: SqlMode::Text,
            segment_max_bytes: DEFAULT_SEGMENT_BYTES,
            role: "driver".to_string(),
            port: 0,
        }
    }
}

impl HistoryConfig {
    /// The fake free-space reading a test installed, or `None` — always `None` outside tests.
    pub(crate) fn free_bytes_override(&self) -> Option<u64> {
        #[cfg(test)]
        {
            self.free_bytes_override
        }
        #[cfg(not(test))]
        {
            None
        }
    }
}

/// `$XDG_DATA_HOME/oxidant`, `~/.local/share/oxidant`, or `/var/lib/oxidant` for a service.
fn default_root() -> PathBuf {
    if running_as_service() {
        return PathBuf::from("/var/lib/oxidant");
    }
    if let Some(xdg) = env_str("XDG_DATA_HOME").filter(|v| !v.is_empty()) {
        return PathBuf::from(xdg).join("oxidant");
    }
    match env_str("HOME").filter(|v| !v.is_empty()) {
        Some(home) => PathBuf::from(home).join(".local/share/oxidant"),
        // No HOME (a bare container init): the service path is the only sane answer left.
        None => PathBuf::from("/var/lib/oxidant"),
    }
}

/// Is this process a system service? `OXIDANT_SYSTEM=1`, or euid 0.
///
/// euid is read from `/proc/self/status` where it exists rather than by linking `libc` for one
/// integer; elsewhere the environment's user name is the fallback and `OXIDANT_SYSTEM=1` is the
/// explicit answer.
fn running_as_service() -> bool {
    if env_flag("OXIDANT_SYSTEM") {
        return true;
    }
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("Uid:") {
                // `Uid: real effective saved fs`
                if let Some(effective) = rest.split_whitespace().nth(1) {
                    return effective == "0";
                }
            }
        }
    }
    matches!(env_str("USER").as_deref(), Some("root"))
}

fn env_str(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.trim().is_empty())
}

fn env_flag(key: &str) -> bool {
    matches!(
        env_str(key)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "on" | "true" | "yes"
    )
}

fn env_u64(key: &str, default: u64) -> u64 {
    env_str(key)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

/// A path knob that must name a filesystem path.
///
/// `checkpoint.rs` documents at length what happens when an object-store URL is handed to
/// `std::fs`: a directory literally named `s3:/bucket/…` under the working directory, and a
/// durability story that is quietly fiction. This journal is `std::fs`-only and node-local by
/// design, so a URL is a boot failure rather than a surprise directory.
fn env_path(key: &str) -> Result<Option<PathBuf>, String> {
    let Some(value) = env_str(key) else {
        return Ok(None);
    };
    if value.contains("://") {
        return Err(format!(
            "{key}={value} is an object-store URL. The statement journal is written with \
             std::fs and is node-local by design (docs/query-history-durability.md §3); set it \
             to a filesystem path, or set OXIDANT_HISTORY=off."
        ));
    }
    Ok(Some(PathBuf::from(value)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacted_mode_keeps_option_secrets_out_of_the_journal() {
        let sql = "CREATE TABLE t USING delta OPTIONS(secret 'hunter2', path '/tmp/t')";
        let out = SqlMode::Redacted.encode(sql);
        assert!(!out.contains("hunter2"), "{out}");
        // A benign literal beside it is untouched.
        assert!(out.contains("'/tmp/t'"), "{out}");
        assert!(out.contains(oxidant_observability::REDACTED), "{out}");
    }

    #[test]
    fn redacted_mode_blanks_url_userinfo() {
        let out = SqlMode::Redacted.encode("SELECT * FROM 's3://AKIA:topsecret@bucket/k.parquet'");
        assert!(!out.contains("topsecret"), "{out}");
        assert!(out.contains("s3://<redacted>@bucket/k.parquet"), "{out}");
    }

    #[test]
    fn redacted_mode_leaves_ordinary_sql_alone() {
        let sql = "SELECT a, 'literal' FROM t WHERE b = 'x'";
        assert_eq!(SqlMode::Redacted.encode(sql), sql);
    }

    #[test]
    fn hash_mode_stores_a_digest_and_a_bounded_prefix() {
        let sql = format!("SELECT {}", "x".repeat(500));
        let out = SqlMode::Hash.encode(&sql);
        assert!(out.starts_with("sha256:"), "{out}");
        assert!(
            out.len() < sql.len(),
            "digest+prefix must be bounded: {out}"
        );
        assert_eq!(SqlMode::Hash.encode(&sql), out, "digest is stable");
    }

    #[test]
    fn object_store_urls_are_refused() {
        for url in ["s3://bucket/history", "gs://b/h", "https://example.com/h"] {
            std::env::set_var("OXIDANT_TEST_PATH_KNOB", url);
            let err = env_path("OXIDANT_TEST_PATH_KNOB").expect_err("must refuse a URL");
            assert!(err.contains("object-store URL"), "{err}");
        }
        std::env::remove_var("OXIDANT_TEST_PATH_KNOB");
    }
}
