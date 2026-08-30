## Summary

<!-- What changed, and why. Link the issue/ticket if there is one. -->

## Release

<!-- Merging to `main` starts a release ONLY when this PR carries exactly one of
     the release labels below. With no label, the merge ships no release. -->

- [ ] `patch` — bug fixes, no behavior change (x.y.**Z+1**)
- [ ] `minor` — new features, backwards compatible (x.**Y+1**.0)
- [ ] `major` — breaking changes (**X+1**.0.0)
- [ ] No release needed (docs/tests/chore only)

If several labels are applied, precedence is `major` > `minor` > `patch`.

Merging a labelled PR does **not** publish anything by itself. It opens a second,
version-bump PR (`release/vX.Y.Z`); **that** PR has to go green and be merged, and
merging it is what tags `vX.Y.Z`, publishes the GitHub Release (tarballs, installer,
Homebrew, .deb/.rpm) and pushes `ghcr.io/oxidantdata/oxidant` images. The bump goes
through a PR because `main` requires status checks, which apply to direct pushes too
— see `.github/workflows/release.yml`.

## Test plan

<!-- Commands run and their results, e.g. `./scripts/ci-local.sh`. -->
