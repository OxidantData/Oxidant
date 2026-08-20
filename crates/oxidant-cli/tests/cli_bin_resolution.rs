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

/// `CARGO_TARGET_DIR` outranks both, and a *relative* value resolves against the workspace
/// root — not the test's cwd, which for an integration test is the package dir.
#[test]
fn relative_cargo_target_dir_resolves_against_workspace_root() {
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

/// The flaw class, not just the instance: every integration test here must spawn the binary
/// through `common::oxidant_bin`. A local copy is how #110 happened — one file was fixed and
/// the others were not.
#[test]
fn no_test_file_defines_its_own_resolver() {
    let tests = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    // Split so this file's own needle does not match itself.
    let needle = concat!("fn ", "oxidant_bin");
    let mut offenders = Vec::new();
    for entry in std::fs::read_dir(&tests).expect("read tests dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().is_some_and(|e| e == "rs") {
            let src = std::fs::read_to_string(&path).expect("read test source");
            if src.contains(needle) {
                offenders.push(path);
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "these define a local oxidant_bin instead of using `common::oxidant_bin`: {offenders:?}"
    );
}
