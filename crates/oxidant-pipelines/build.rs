//! Tells the live-Postgres test suites whether there is a server to run against.
//!
//! `tests/reconcile_pg.rs` needs a PostgreSQL server with `wal_level = logical`, named by
//! `OXIDANT_PG_TEST_DSN`. Without one those tests used to early-`return` after an `eprintln!`,
//! so `cargo test --test reconcile_pg` on a machine with no database printed `N passed` — and a
//! skipped test reporting as passed is worse than no test, because the summary says the
//! cross-engine assumptions were checked when nothing checked them.
//!
//! `#[ignore]` is the mechanism the harness has for "not run", but a fixed attribute would also
//! skip the tests on a machine that *does* have a server. So the attribute is conditional on this
//! cfg: the suites are `#[ignore]`d when the variable is unset at build time, and run normally
//! under `OXIDANT_PG_TEST_DSN=… cargo test`, which is how the docs and the PR gate spell it.
fn main() {
    // The single-colon spelling: the workspace declares `rust-version = "1.72"`, and cargo
    // rejects the `cargo::` form under an MSRV older than 1.77 even on a newer toolchain.
    println!("cargo:rustc-check-cfg=cfg(pg_live)");
    println!("cargo:rerun-if-env-changed=OXIDANT_PG_TEST_DSN");
    if std::env::var_os("OXIDANT_PG_TEST_DSN").is_some_and(|dsn| !dsn.is_empty()) {
        println!("cargo:rustc-cfg=pg_live");
    }
}
