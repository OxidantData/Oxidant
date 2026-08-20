//! Guards the `oxidant` binary resolver — issue #110.
//!
//! `cargo llvm-cov` re-runs the whole suite under `target/llvm-cov-target/`, and CI pre-builds
//! `oxidant-cli` into that dir. Two of the six hand-copied resolvers never learned about it, so
//! the `line-coverage` job failed with `oxidant binary not found at …/target/debug/oxidant`
//! while every blocking job stayed green. These tests pin the fallback chain and the
//! single-implementation invariant that stops the copies from drifting again.

mod common;

use std::path::Path;

/// A stand-in `oxidant` at `<root>/<rel>`; returns the file path.
fn plant(root: &Path, rel: &str) -> std::path::PathBuf {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
    std::fs::write(&path, b"#!/bin/sh\nexit 0\n").expect("write");
    path
}

/// The line-coverage job's exact layout: llvm-cov's target dir is populated, plain
/// `target/debug/` is not, and `CARGO_TARGET_DIR` is unset in the test process.
#[test]
fn resolves_llvm_cov_target_when_plain_target_is_absent() {
    let root = tempfile::tempdir().expect("tempdir");
    let planted = plant(root.path(), "target/llvm-cov-target/debug/oxidant");
    assert!(!root.path().join("target/debug/oxidant").exists());

    let found = common::resolve_bin(None, root.path(), "debug").expect("resolver found nothing");
    assert_eq!(found, planted);
}

/// A normal `cargo build -p oxidant-cli` still wins: llvm-cov's copy is instrumented and
/// slower, so it must stay the last resort rather than shadow the plain build.
#[test]
fn plain_target_wins_over_llvm_cov_target() {
    let root = tempfile::tempdir().expect("tempdir");
    let plain = plant(root.path(), "target/debug/oxidant");
    plant(root.path(), "target/llvm-cov-target/debug/oxidant");

    let found = common::resolve_bin(None, root.path(), "debug").expect("resolver found nothing");
    assert_eq!(found, plain);
}

/// `CARGO_TARGET_DIR` outranks both. A *relative* value resolves against the directory `cargo`
/// was invoked from — the workspace root in CI and under `scripts/ci-local.sh`, which is why the
/// resolver joins it against the workspace root and not the test's cwd (the package dir).
#[test]
fn relative_cargo_target_dir_resolves_against_the_cargo_invocation_dir() {
    let root = tempfile::tempdir().expect("tempdir");
    let planted = plant(root.path(), "custom-target/debug/oxidant");
    plant(root.path(), "target/debug/oxidant");

    let found = common::resolve_bin(Some("custom-target"), root.path(), "debug")
        .expect("resolver found nothing");
    assert_eq!(found, planted);
}

#[test]
fn absolute_cargo_target_dir_is_used_verbatim() {
    let root = tempfile::tempdir().expect("tempdir");
    let elsewhere = tempfile::tempdir().expect("tempdir");
    let planted = plant(elsewhere.path(), "debug/oxidant");

    let found = common::resolve_bin(
        Some(elsewhere.path().to_str().expect("utf8")),
        root.path(),
        "debug",
    )
    .expect("resolver found nothing");
    assert_eq!(found, planted);
}

/// A non-`debug` profile must not silently fall back to `debug` binaries.
#[test]
fn profile_is_honoured_in_every_candidate() {
    let root = tempfile::tempdir().expect("tempdir");
    plant(root.path(), "target/debug/oxidant");

    let tried =
        common::resolve_bin(None, root.path(), "release").expect_err("debug must not match");
    assert!(
        tried
            .iter()
            .all(|p| p.to_string_lossy().contains("release")),
        "probed non-release paths: {tried:?}"
    );
}

/// The failure message is the only thing CI shows, so it must name every path tried —
/// including the llvm-cov one, which is what made #110 hard to read.
#[test]
fn failure_reports_the_whole_documented_chain() {
    let root = tempfile::tempdir().expect("tempdir");
    let tried = common::resolve_bin(Some("custom-target"), root.path(), "debug")
        .expect_err("empty tempdir must not resolve");

    let shown: Vec<String> = tried.iter().map(|p| p.display().to_string()).collect();
    assert_eq!(shown.len(), 3, "chain changed: {shown:?}");
    for suffix in [
        "custom-target/debug/oxidant",
        "target/debug/oxidant",
        "target/llvm-cov-target/debug/oxidant",
    ] {
        assert!(
            shown.iter().any(|p| p.ends_with(suffix)),
            "{suffix} missing from {shown:?}"
        );
    }
}

/// Scans one test file's source for a hand-rolled resolver, and says which signal fired.
///
/// Comment lines are stripped first: every test module here names `CARGO_BIN_EXE_oxidant` in its
/// `//!` header, and a guard that fires on prose asserts something untrue about the file. Two
/// code-form signals, because #110 can come back under a different name:
///   * the env-var lookup itself — the first leg of any hand-rolled chain, whatever it is called;
///   * a `fn oxidant_bin(` definition — a copy that skips the env var entirely.
///
/// The needle is spelled as an escaped literal, which is also what keeps this file from matching
/// itself when it scans its own directory.
fn local_resolver_offence(src: &str) -> Option<&'static str> {
    let code: Vec<&str> = src
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect();
    if code
        .iter()
        .any(|l| l.contains("env::var(\"CARGO_BIN_EXE_oxidant\")"))
    {
        return Some("probes CARGO_BIN_EXE_oxidant itself");
    }
    if code.iter().any(|l| {
        let l = l.trim_start().trim_start_matches("pub ").trim_start();
        l.starts_with("fn oxidant_bin(")
    }) {
        return Some("defines its own `fn oxidant_bin`");
    }
    None
}

/// The flaw class, not just the instance: every integration test here must spawn the binary
/// through `common::oxidant_bin`. A local copy is how #110 happened — one file was fixed and
/// the others were not.
#[test]
fn no_test_file_defines_its_own_resolver() {
    let tests = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut offenders = Vec::new();
    // Deliberately non-recursive: `tests/common/` is the one place the resolver may live. A
    // future `tests/helpers/*.rs` would escape this scan — widen it here if that ever appears.
    for entry in std::fs::read_dir(&tests).expect("read tests dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().is_some_and(|e| e == "rs") {
            let src = std::fs::read_to_string(&path).expect("read test source");
            if let Some(why) = local_resolver_offence(&src) {
                offenders.push(format!("{} ({why})", path.display()));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "these resolve the binary themselves instead of using `common::oxidant_bin`: {offenders:?}"
    );
}

/// The guard must not fire on prose. Every test module's `//!` header names
/// `CARGO_BIN_EXE_oxidant` and `oxidant_bin`; flagging those would assert something untrue about
/// the file, which is the worst kind of guard failure to debug.
#[test]
fn doc_comment_mention_does_not_trip_the_guard() {
    let src = r#"//! Lives in `oxidant-cli` so Cargo sets `CARGO_BIN_EXE_oxidant` for the test binary.
//! Historically each file had its own `fn oxidant_bin(` copy; now it uses `common::oxidant_bin`.

mod common;

/// Spawns via the shared resolver — see `common::oxidant_bin`.
#[test]
fn t() {
    let _ = common::oxidant_bin();
}
"#;
    assert_eq!(local_resolver_offence(src), None);
}

/// ...but a real copy is caught, under any name — the #110 flaw class, not the identifier.
///
/// Both fixtures are spelled with escaped quotes / split fragments so that this file's own
/// source never contains the code forms the guard scans for, and so the guard does not flag
/// the very test that pins it.
#[test]
fn hand_rolled_resolver_trips_the_guard_under_any_name() {
    let renamed = "fn cli_binary_path() -> PathBuf {\n\
                   \x20   if let Ok(p) = std::env::var(\"CARGO_BIN_EXE_oxidant\") {\n\
                   \x20       return PathBuf::from(p);\n\
                   \x20   }\n\
                   \x20   PathBuf::from(\"target/debug/oxidant\")\n}\n";
    assert!(
        local_resolver_offence(renamed).is_some(),
        "renamed copy escaped the guard"
    );

    let env_var_free = concat!(
        "pub fn ",
        "oxidant_bin() -> PathBuf {\n    PathBuf::from(\"target/debug/oxidant\")\n}\n"
    );
    assert!(
        local_resolver_offence(env_var_free).is_some(),
        "env-var-free copy escaped the guard"
    );
}
