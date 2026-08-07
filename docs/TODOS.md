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
- [ ] Homebrew users currently get no sample data (the cargo-dist-generated formula
      installs only the binary) — workaround: `--sample-data` pointing at a repo clone;
      revisit with a custom formula later.
- [ ] musl static builds (`x86_64_`/`aarch64-unknown-linux-musl`) for Alpine and
      minimal containers — needs a native-dep audit (ring, zstd-sys) for musl
      safety; gnu targets ship today.
