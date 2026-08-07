# TODOS

Open work items and gates, grouped by area. Pick work here; keep entries short and
link to docs/issues for detail. (Fresh file — an earlier internal TODOS.md was
removed in the pre-launch cleanup; this list starts with the public repo's items.)

## Binary releases / packaging

Binary releases ship via cargo-dist on every `v*` tag (see
[.github/workflows/binaries.yml](../.github/workflows/binaries.yml)): curl|sh
installer, Homebrew tap, tarballs + checksums, and `.deb`/`.rpm` packages on the
GitHub Release. Future items:

- [ ] Hosted APT repo (Cloudsmith / Gemfury) so users can `apt install oxidant`
      with upgrades — today the `.deb` is a manual download + `dpkg -i` from
      GitHub Releases.
- [ ] Hosted RPM repo (yum/dnf) — `.rpm` artifacts already ship per release but
      there is no repo metadata to subscribe to.
- [ ] Submit `oxidant` to Homebrew core once the project meets homebrew-core's
      notability requirements — until then the tap is
      [OxidantData/homebrew-tap](https://github.com/OxidantData/homebrew-tap).
- [ ] Linux binaries need glibc ≥ 2.35 (built on ubuntu-22.04), so the `.rpm`
      installs but cannot run on RHEL 9 / Amazon Linux 2023 (glibc 2.34 —
      `GLIBC_2.35 not found`, found in v0.1.0 install verification). Fix by
      building Linux targets on an older-glibc base (AL2023/manylinux container)
      or via the musl item below; documented as Docker-only for those distros
      in getting-started.md meanwhile.
- [ ] curl|sh installs the binary only (cargo-dist ships no data files), so the
      sample tables need `sample-data.tar.gz` from the release + `--sample-data`
      there — documented in getting-started.md. Tarballs, Homebrew and deb/rpm
      all auto-discover bundled samples (verified v0.1.0). Revisit if cargo-dist
      adds data-file installs.
- [ ] musl static builds (`x86_64_`/`aarch64-unknown-linux-musl`) for Alpine and
      minimal containers — needs a native-dep audit (ring, zstd-sys) for musl
      safety; gnu targets ship today.
