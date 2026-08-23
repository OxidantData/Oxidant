//! Tells the live-Postgres test suite whether there is a server to run against.
//!
//! Same mechanism, and the same reason, as `oxidant-pipelines/build.rs`: `tests/postgres_cdc.rs`
//! needs a server named by `OXIDANT_PG_TEST_DSN` and skips its body without one, which the test
//! harness could only report as `passed`. `#[ignore]` is how a skip reads as a skip, and making
//! the attribute conditional on this cfg keeps the suite running under
//! `OXIDANT_PG_TEST_DSN=… cargo test`, which is how the docs spell it.
fn main() {
    // The single-colon spelling: the workspace declares `rust-version = "1.72"`, and cargo
    // rejects the `cargo::` form under an MSRV older than 1.77 even on a newer toolchain.
    println!("cargo:rustc-check-cfg=cfg(pg_live)");
    println!("cargo:rerun-if-env-changed=OXIDANT_PG_TEST_DSN");
    if std::env::var_os("OXIDANT_PG_TEST_DSN").is_some_and(|dsn| !dsn.is_empty()) {
        println!("cargo:rustc-cfg=pg_live");
    }
}
