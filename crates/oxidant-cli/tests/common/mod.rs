//! Shared helpers for the `oxidant-cli` integration tests.
//!
//! Every test in this crate spawns the `oxidant` binary as a subprocess, so they all need the
//! same answer to "where is it?". Keeping one copy here is not tidiness: issue #110 was two of
//! six hand-copied resolvers missing the `cargo llvm-cov` fallback, so the informational
//! `line-coverage` job failed while the blocking `clippy + test` job stayed green.

// Not every test binary calls every helper (the resolver's own guard test never spawns a
// subprocess), and `mod common;` compiles the whole module into each one.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

/// The workspace root, derived from this crate's manifest dir.
pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Where the `oxidant` binary may live, in probe order.
///
/// Mirrors the chain documented in `AGENTS.md`: `$CARGO_TARGET_DIR/$PROFILE/oxidant`,
/// `target/$PROFILE/oxidant`, then `target/llvm-cov-target/$PROFILE/oxidant`.
pub fn bin_candidates(
    cargo_target_dir: Option<&str>,
    workspace_root: &Path,
    profile: &str,
) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    if let Some(dir) = cargo_target_dir.filter(|d| !d.is_empty()) {
        // Cargo resolves a relative `CARGO_TARGET_DIR` against the workspace root, but an
        // integration test's cwd is the *package* dir — join explicitly. An absolute `dir`
        // replaces the base, so this covers both spellings.
        candidates.push(workspace_root.join(dir).join(profile).join("oxidant"));
    }
    let target = workspace_root.join("target");
    candidates.push(target.join(profile).join("oxidant"));
    // `cargo llvm-cov` re-runs the suite under its own target dir; CI pre-builds oxidant-cli
    // there with `--target-dir target/llvm-cov-target`. Without this the line-coverage job
    // finds nothing, because llvm-cov never populates plain `target/debug/`.
    candidates.push(target.join("llvm-cov-target").join(profile).join("oxidant"));
    candidates
}

/// First candidate that exists, or every path tried so the caller can say what it looked for.
pub fn resolve_bin(
    cargo_target_dir: Option<&str>,
    workspace_root: &Path,
    profile: &str,
) -> Result<PathBuf, Vec<PathBuf>> {
    let candidates = bin_candidates(cargo_target_dir, workspace_root, profile);
    match candidates.iter().find(|c| c.exists()) {
        Some(found) => Ok(found.clone()),
        None => Err(candidates),
    }
}

/// The `oxidant` binary to spawn.
///
/// `cargo test -p oxidant-cli` sets `CARGO_BIN_EXE_oxidant`; a bare `cargo test --workspace`
/// does not, because `oxidant-cli` is binary-only and Cargo skips orphan binaries. Fall back
/// to probing the target dirs a prebuild would have used.
pub fn oxidant_bin() -> PathBuf {
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_oxidant") {
        return PathBuf::from(p);
    }
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "debug".into());
    let root = workspace_root();
    let target_dir = std::env::var("CARGO_TARGET_DIR").ok();
    resolve_bin(target_dir.as_deref(), &root, &profile).unwrap_or_else(|tried| {
        let probed = tried
            .iter()
            .map(|p| format!("  {}", p.display()))
            .collect::<Vec<_>>()
            .join("\n");
        panic!(
            "oxidant binary not found — run `cargo build -p oxidant-cli` first. Probed:\n{probed}"
        )
    })
}
