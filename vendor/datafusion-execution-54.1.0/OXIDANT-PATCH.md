# Oxidant downstream patch: DataFusion execution 54.1.0

## Origin and license

This directory contains `datafusion-execution` **54.1.0**, published by the
Apache DataFusion project (`https://github.com/apache/datafusion`):

- Archive: `https://static.crates.io/crates/datafusion-execution/datafusion-execution-54.1.0.crate`
- Archive SHA-256: `d8eac0a09bc8d263f52025cad9e001da4d8138d633fa288edda4d06b1772eae6`
- Upstream license: Apache-2.0. The full `LICENSE.txt`, `NOTICE.txt`, README,
  normalized `Cargo.toml`, original `Cargo.toml.orig`, and all 17 Rust source
  files are retained. Only the upstream standalone `Cargo.lock` is omitted:
  Engine's root `Cargo.lock` is authoritative.
- The archive checksum matches the previously pinned Engine lock and the
  crates.io index entry. All retained archive files were compared byte-for-byte
  before the change. `UPSTREAM-SHA256.json` records their unmodified identities.

The root `[patch.crates-io]` uses an exact `=54.1.0` version and this repository-
relative directory. The upstream normalized manifest is unchanged, including
features and dependency constraints; `Cargo.toml.orig` is provenance, not the
active manifest. The root lock changes only this package's source identity,
removing its registry source/checksum fields. Every other resolved version and
edge is retained. No second DataFusion type universe or patched physical-plan
crate is introduced.

## Local change and limits

`src/memory_pool/pool.rs`, `FairSpillPool::try_grow`, now checks **aggregate
spillable plus unspillable reservations** under the existing mutex before
admitting a fallible request. Checked subtraction from pool capacity avoids
both addition overflow and underflow when prior infallible growth has already
overcommitted the pool. Refusal changes neither pool nor reservation accounting.
The existing per-reservation fair-share check remains in addition to this bound.

The check is required because late registrations, split reservations and changes
in unspillable usage can leave existing reservations above their current share.
Fair share alone does not prevent the next spillable request from exceeding the
pool's total capacity.

`MemoryPool::grow` remains **infallible** and can deliberately exceed capacity;
this patch does not change that API or silently clip its accounting. While the
pool is already over capacity, fallible requests (including zero-byte requests)
are rejected until enough memory is returned. At or below capacity, zero-byte
requests retain the existing fair-share condition. Infallible allocations whose
combined total cannot fit in `usize` are outside `reserved()`'s representable
range; the new admission check still rejects safely without adding that total.

This is tracked-reservation accounting safety only, **not** an RSS limit or a
sort/merge liveness fix. Strict admission can make resource exhaustion happen
earlier. It does not aggregate all splits into a consumer-level fair share,
reserve sorter working memory, change merge fan-in, add retries, or guarantee
completion under arbitrary concurrency. Engine issue
[#199](https://github.com/OxidantData/Oxidant/issues/199) remains open pending the
separate sort/merge ownership/admission correction and full-query acceptance.

## Verification and maintenance

Engine-owned tests exercise the actual production pool through DataFusion:

```sh
cargo test -p oxidant-loom --test fair_spill_pool --locked
cargo clippy -p datafusion-execution --lib --locked --no-deps -- -D warnings
cargo fmt --all -- --check
rustfmt --edition 2024 --config max_width=90 --check \
  vendor/datafusion-execution-54.1.0/src/memory_pool/pool.rs
python3 deploy/docker/tests/test_vendored_patch_context.py -v
```

The explicit dependency formatter uses upstream 54.1.0's edition and line width.
The vendor crate is excluded from workspace membership, so upstream development
dependencies are not added to the Engine lock. These tests are not a claim that
all upstream DataFusion unit tests were run; those require the separate upstream
`insta` development dependency. Upstream tests remain in the source unchanged.

The image Dockerfile copies this excluded patch **before** `cargo chef cook`.
The pinned cargo-chef 0.1.73 prepares only workspace members, so the recipe alone
cannot materialize the patch directory. The explicit COPY also invalidates the
dependency-cook cache on patched source changes. The Python check verifies that
pre-cook source-selection boundary; it is not an image build. The final build
still uses the full checkout and root lock. No image or release was produced as
part of the local pool verification.

The regression suite covers late consumers, split/new-empty/taken reservations,
mixed classes, arithmetic boundaries, concurrent admissions, no-op/healthy
controls, infallible growth, failed-request immutability and shrink/free/drop/
last-registration accounting. Query diagnostics keep the existing 64 MiB, two
partitions, 1024 batch size, 170k/340k rows, width 400 and count plus both sums;
pool tests alone must never certify that query as live.

No advisory or license exception, wildcard dependency, new registry or Git
source is added. Apache-2.0 is already allowed by `deny.toml`, which is unchanged.
The manual advisory review found no DataFusion entry in the RustSec advisory
repository snapshot `8a1eb4f933fb5821add5b4e98601ebd90b8b3538`; this is not a
whole-graph scan or a promise of no future advisories.

For an upstream refresh, verify the new archive/license/source delta, rerun
these tests and the separate bounded query gates, and remove this patch only
when the replacement demonstrably retains the bound. Do not remove failing
query assertions or raise limits as a rollback. The patch adds constant-time
integer admission checks within an already-required lock, not a new allocator,
per-consumer map or reservation owner; no performance improvement is claimed.
