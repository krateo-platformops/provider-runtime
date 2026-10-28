---
type: Runbook
title: provider-runtime — release
description: How a provider-runtime version actually ships — an annotated git tag consumed via the Go module proxy; no images, no charts, no OCI artifacts.
resource: github.com/krateo-platformops/provider-runtime
tags: [library, release, go-module]
timestamp: 2026-10-28T00:00:00Z
---

# Release

provider-runtime is **tag-only**: a release is a git tag on `main`, consumed
by providers through the Go module proxy. There is no build or publish
workflow, no container image, no chart and no OCI artifact — this is the
intended reality for this library, not a gap. The only thing a tag triggers is
its release notes (see [CI](#ci)).

## Tag convention

Tags are semver **with the `v` prefix** (the Go-module requirement):
`v0.4.7` … `v1.7.0`. Note this differs from Krateo *chart* repos, whose tags
carry no `v` prefix — Go module tags must have it, and any tag-triggered
workflow here must match `v[0-9]+.[0-9]+.[0-9]+`. The bare-semver trigger the
chart repos use never fired here; release notes start at v1.5.3 (#16).

## CI

One workflow, `.github/workflows/run-tests.yaml` (**Test and coverage**),
runs on every pull request to `main`: `go test -race -coverprofile=…
-covermode=atomic ./...` plus a Codecov upload (needs the `CODECOV_TOKEN`
secret). The `lint-docs` job (this standard's conformance check) runs
alongside it, and `security.yml` runs the org's shared security checks.

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

## After a release

Consumers (core-provider and the other providers) pick the new version up via
`go get github.com/krateo-platformops/provider-runtime@vX.Y.Z` in their own
PRs; there is no automated bump chain for this library.
