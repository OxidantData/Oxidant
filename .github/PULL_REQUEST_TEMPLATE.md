## Summary

<!-- What changed, and why. Link the issue/ticket if there is one. -->

## Release

<!-- Merging to `main` cuts a release ONLY when this PR carries exactly one of
     the release labels below. With no label, the merge ships no release. -->

- [ ] `patch` — bug fixes, no behavior change (x.y.**Z+1**)
- [ ] `minor` — new features, backwards compatible (x.**Y+1**.0)
- [ ] `major` — breaking changes (**X+1**.0.0)
- [ ] No release needed (docs/tests/chore only)

If several labels are applied, precedence is `major` > `minor` > `patch`.
The release workflow bumps the workspace version, tags `vX.Y.Z`, publishes the
GitHub Release (tarballs, installer, Homebrew, .deb/.rpm), and pushes
`ghcr.io/oxidantdata/oxidant` images — see `.github/workflows/release.yml`.

## Test plan

<!-- Commands run and their results, e.g. `./scripts/ci-local.sh`. -->
