---
type: Runbook
title: provider-runtime — release
description: How a provider-runtime version actually ships — an annotated git tag consumed via the Go module proxy; no images, no charts, no OCI artifacts.
resource: github.com/krateo-platformops/provider-runtime
tags: [library, release, go-module]
timestamp: 2026-11-05T00:00:00Z
---

# Release

provider-runtime is **tag-only**: a release is a git tag on `main` (for a fix
while main can't be tagged, on a release branch: see
[below](#a-fix-release-while-main-cant-be-tagged)), consumed by providers
through the Go module proxy. There is no build or publish
workflow, no container image, no chart and no OCI artifact — this is the
intended reality for this library, not a gap. The only thing a tag triggers is
its release notes (see [CI](#ci)).

## Tag convention

Tags are semver **with the `v` prefix** (the Go-module requirement):
`v0.4.7` … `v1.9.1`. Note this differs from Krateo *chart* repos, whose tags
carry no `v` prefix — Go module tags must have it, and any tag-triggered
workflow here must match `v[0-9]+.[0-9]+.[0-9]+`. The bare-semver trigger the
chart repos use never fired here; release notes start at v1.5.3 (#16).

## CI

One workflow, `.github/workflows/run-tests.yaml` (**Test and coverage**),
runs on every pull request to `main`: `go test -race -coverprofile=…
-covermode=atomic ./...` plus a Codecov upload (needs the `CODECOV_TOKEN`
secret). The `lint` job runs golangci-lint at the version that job pins, and
`make lint` uses whichever `golangci-lint` is on `PATH`, so install the same
version locally. The `lint-docs` job (this standard's conformance check) runs
alongside them, and `security.yml` runs the org's shared security checks.

On a tag push, `.github/workflows/release-notes.yaml` calls the org's shared
release-notes workflow. It creates the GitHub Release and writes generated
notes into it. If generation fails, it leaves a list of commit subjects
instead, and a re-run (`workflow_dispatch` with the `tag` input) replaces it.
Notes never fail a release.

## Cutting a release

1. Merge to `main`; make sure `make test` and `make lint` are green.
2. If `apis/` types changed, confirm `make generate` produced no diff
   (deepcopy in sync).
3. Tag and push:

   ```sh
   git tag -a vX.Y.Z -m "vX.Y.Z"
   git push origin vX.Y.Z
   ```

4. Verify the module proxy serves it:

   ```sh
   GOPROXY=https://proxy.golang.org go list -m \
     github.com/krateo-platformops/provider-runtime@vX.Y.Z
   ```

5. Check the GitHub Release the tag push created. Its notes are generated (see
   [CI](#ci)). Because they are generated from the commits, put the reasoning
   in the annotated tag message and commit bodies. [log](./log.md) records
   only curated history.

## A fix release while main can't be tagged

While main carries work that mustn't ship yet (the credential-use stack: no tag
from main until its last PR, docs/plan-credential-use-implementation.md §2), a
fix ships from a release branch:

1. Create `release/vX.Y` from the last tag `vX.Y.Z` if it doesn't exist.
2. Merge the fix to main first, then cherry-pick it (`git cherry-pick -x`) onto
   a branch from `release/vX.Y`, add the [log](./log.md) entry, and open a pull
   request to `release/vX.Y`. CI runs on pull requests to `release/**`.
3. After merge, tag `vX.Y.(Z+1)` on `release/vX.Y` and push it, then do steps
   4 and 5 above. Add the same log entry on main.

v1.20.1 shipped this way (#112). A minor release can too: v1.21.0, the
shared management policy, shipped from `release/v1.21`, cut from v1.20.1, with
its features cherry-picked from main (#117). A dependency bump that the
backported work forces comes along (#105, OpenTelemetry 1.47.0, for plumbing
v1.102.0); the rest of main stays out.

## After a release

Consumers (core-provider and the other providers) pick the new version up via
`go get github.com/krateo-platformops/provider-runtime@vX.Y.Z` in their own
PRs; there is no automated bump chain for this library.
