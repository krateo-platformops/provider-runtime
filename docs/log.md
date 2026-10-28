---
type: Log
title: provider-runtime — log
description: Curated chronological history of provider-runtime — notable changes and decisions, newest first.
resource: github.com/krateo-platformops/provider-runtime
tags: [library, history]
timestamp: 2026-10-28T00:00:00Z
---

# Log

Curated history — notable changes and decisions, newest first. Full release
notes live in GitHub Releases; the mechanics are in [release](./release.md).

## 2026-10-28 — v1.7.0
- **Requires Go 1.26** (`go 1.26.0`, was 1.25.6). Every `golang.org/x/net`
  release that fixes the HTTP/2 vulnerabilities this module reaches through
  client-go requires it.
- Dependency vulnerabilities closed: x/net v0.62.0, x/text v0.43.0, gRPC
  v1.85.0, OTel v1.45.0 (six Dependabot alerts, three high, plus further
  `govulncheck` findings). With Go 1.26.10, `govulncheck` reports none.
  Standard-library findings depend on the consumer's toolchain: build with
  Go ≥ 1.26.10.
- No API or behaviour change.

## 2026-10-28 — v1.6.0
- When `Observe` errors on an unresolved create handshake, the `Synced`
  condition and the returned error name the pending create alongside the
  `Observe` error. Before, only the `Observe` error was shown.
- The runtime deliberately does **not** call `Create` while `Observe` cannot
  answer. The fix for an `Observe` that errors permanently belongs in the
  provider (core-provider#236, oasgen-provider#266).
- Documented the `Observe` contract the recovery depends on, with a test for
  each error return: "would answering `ResourceExists: false` let `Create`
  duplicate it?"

## 2026-10-28 — v1.5.2, v1.5.3
- v1.5.2: a `Create` that returns a resource-version conflict, plain or
  wrapped, is now stamped `external-create-failed` like any other error (#14).
  Before, it requeued without stamping. `pending` stayed the newest marker and
  the resource waited out the recovery window, or forever before v1.4.0. Seen
  as a stuck RestDefinition on 2 of 2 fresh installs with oasgen-provider
  pinned to v1.3.0 (oasgen-provider#254). This fixes the **conflict** path
  only. A resource whose `Observe` errors needs the consumer-side `Observe`
  contract in [overview](./overview.md); a runtime bump alone does not fix
  it (core-provider#236).
- v1.5.3: no runtime change. The release-notes trigger now matches `v`-prefixed
  tags (#16), so this is the first release with a GitHub Release.

## 2026-10-27 — v1.5.0, v1.5.1
- v1.5.0 added a `ZeroEvidence` condition constructor (#11); v1.5.1 reverted
  it (#12) because its consumer uses unstructured-runtime's conditions. Do not
  use v1.5.0.

## 2026-09-25 — v1.4.0
- An unresolved create handshake no longer refuses forever (#10). After the
  create recovery grace period (2m) with `Observe` still reporting the resource
  absent, the reconciler clears `external-create-pending` and retries.
  Motivated by six Repo CRs stuck for over a day (git-provider#22).

## 2026-09-16 — v1.3.1
- An unresolved create handshake is resolved by asking `Observe` instead of
  refusing before it (#7). If the resource exists, it is adopted.

## 2026-08-07
- Adopted the Krateo Documentation Standard (this bundle) — the repo's first
  documentation, authored fresh from source.

## 2026-08-03 — v1.3.0
- Go module identity migrated to `github.com/krateo-platformops/provider-runtime`
  (full independence from the previous org); `plumbing` dependency moved to its
  new home in the same change.

## 2026-07-10 — v1.2.2
- `pkg/logging` became the shared home for the OTel-aligned log model
  (`NewOTelJSONHandler` / `NewOTelHandler` with OTLP export via the otelslog
  bridge) that consumers previously duplicated.

## 2026-04..07 — v1.2.x
- v1.2.0 added `pkg/telemetry`: OTLP metrics for the reconcile loop
  (`provider_runtime.*` instruments) plus queue-wait instrumentation in
  `pkg/controller`.
- v1.2.1 made all `Metrics` methods nil-safe (panic fix, #22).

## 2026-04-09 — v1.1.0
- Throttled event recorder (`WithThrottledRecorder`) to damp repeated
  reconcile-error events.

## 2026-03-09 — v1.0.0
- Event handling refactored onto the `plumbing` event recorder; the local
  event package removed. First 1.x line — the baseline the Krateo providers
  (core-provider et al.) build on.

## 2024-08-30 — v0.9.0
- Total refactoring (breaking): the reconciler/external-client split took its
  current shape (#9); later 0.x releases added the exponential timed-failure
  workqueue limiter, context logger utilities, and controller-runtime bumps.

## 2023-01-26 — origin
- First commit: generic managed-resource reconciler with condition types,
  credential selectors and deepcopy generation, descending from the
  Crossplane runtime pattern.
