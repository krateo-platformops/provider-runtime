---
type: Log
title: provider-runtime — log
description: Curated chronological history of provider-runtime — notable changes and decisions, newest first.
resource: github.com/krateo-platformops/provider-runtime
tags: [library, history]
timestamp: 2026-11-05T00:00:00Z
---

# Log

Curated history — notable changes and decisions, newest first. Full release
notes live in GitHub Releases; the mechanics are in [release](./release.md).

## 2026-11-05 — rate-limit governor (unreleased)
- **Reconciles slow down by themselves** when an external API signals a rate
  limit, per endpoint key, or when the controller's own container is CPU- or
  memory-saturated. It replaces hand-tuning `--max-reconcile-rate` and
  `--poll`. Design: [design-rate-limit-governor](./design-rate-limit-governor.md).
- **Off by default.** With nothing configured, nothing is gated and
  `ratelimit.Report` is a no-op. A provider turns it on with one chart flag
  (`controller.Options.RateLimit.Enabled`), implements
  `reconciler.RateLimitKeyer`, and reports signals.
- A deferred pass makes no external call, writes
  `Synced=False/ReconcileDeferred` with the key and deadline in the message,
  leaves `Ready` unchanged, and requeues with a nil error, so it consumes no
  failure backoff.
- New API: `prv1.ReasonReconcileDeferred` (`"ReconcileDeferred"`, fixed;
  shared with unstructured-runtime) and `prv1.ReconcileDeferred(msg)`;
  `reconciler.RateLimitKeyer` and `reconciler.WithRateLimitGovernor`;
  `controller.Options.RateLimit`, `controller.Options.RateLimitGovernor` and
  `(*controller.Options).SetupRateLimitGovernor`; the packages
  `pkg/ratelimit` (Signal, Report, Governor, saturation sampler) and
  `pkg/ratelimit/httpsignal` (GitHub and Azure DevOps headers);
  `reconcilertest.AssertReportsRateLimits` and the recorded vendor
  responses; `provider_runtime.ratelimit.*` metrics.
- The pressure thresholds are still provisional. A kind measurement on the
  example provider supports 50% for CPU. It couldn't measure memory pressure,
  which stayed at 0 up to every OOMKill. It found the 90% working-set soft
  ratio too close to the limit for calls that each hold about 5% of it: 80%
  was clean. See [configuration](./configuration.md#the-pressure-thresholds-are-provisional).

## 2026-11-04 — docs: errors become readable status
- overview.md now states that every error a provider returns is written
  verbatim into `Synced`'s message, readable by anyone with `get` on the
  resource, and that keeping credentials and raw vendor bodies out of those
  errors is the provider's job. The runtime cannot tell what is sensitive.
  Decided with the user after oasgen-provider#433 found a vendor's error body
  exposed. No code change.

## 2026-11-04 — v1.8.2
- **The runtime sets `Ready=Deleting` when `Connect` or `Observe` fails on a
  resource being deleted** (deletion timestamp set). v1.8.0/v1.8.1 kept
  `Deleting` only if the provider had already written it, so a provider whose
  deletion path returned an error before setting `Ready` got `NotObserved` for
  an object whose fate is not in doubt. Every adopter had to remember to set
  `Deleting` first (oasgen-provider and incident-provider did). A
  `ReadyAssessed` error still wins: the provider's own `Ready` stays.
- docs/usage.md now states the per-cluster rollout order: core-provider
  2.13.56 live before any v1.8.x provider build.

## 2026-11-04 — v1.8.1
- **Fix: a resource being deleted is no longer reported as a Ready-contract
  breach.** v1.8.0 checked "a successful `Observe` must set `Ready`" before the
  runtime's own deletion branch, which sets `Ready=Deleting` itself on every
  path. So after an outage, deleting a resource whose `Observe` doesn't set
  `Ready` on its deletion path emitted a false `ReadyNotSetAfterObserve`
  Warning and Error log. The check now skips resources with a deletion
  timestamp. Found by incident-provider's adoption of v1.8.0.
- **`reconciler.ReasonReadyNotSetAfterObserve` is exported**, so a
  provider's tests can assert on the Warning event by constant rather than by
  string.
- `reconcilertest.AssertObserveSetsReady` is unchanged: a provider that sets
  `Ready` on its deletion paths can still check them with it.

## 2026-11-04 — v1.8.0
- **Behaviour change: `Ready` is `Unknown/NotObserved` when the external
  resource can't be observed.** On a `Connect` or `Observe` error the runtime
  now sets `Ready` to `NotObserved()` next to `Synced=False/ReconcileError`.
  Before, only `Synced` changed and `Ready` kept its last value, so it could
  show a stale `True` next to the error. The exceptions are a `Ready` of
  `Deleting` and an error wrapped with `reconciler.ReadyAssessed`. The message
  is constant, so `lastTransitionTime` doesn't move on repeated failures; the
  error stays on `Synced`. Conflicts still requeue without touching
  conditions.
- **The contract is explicit.** Every successful `Observe` must set `Ready`.
  The runtime never invents a value on recovery. If a successful `Observe`
  leaves `NotObserved` in place, the runtime keeps it and reports the breach
  with an Error log and a throttled Warning event, `ReadyNotSetAfterObserve`.
  See [overview](./overview.md).
- Why the runtime doesn't detect "did the provider set Ready" by comparing
  before and after: providers set `Ready` to the value it already had and
  then return an error (alert-provider's Alert, incident-provider's
  IncidentApply). A comparison can't see that, so the provider says it with
  `ReadyAssessed`.
- New API: `prv1.ReasonNotObserved` (`"NotObserved"`, fixed),
  `prv1.NotObserved()`, `reconciler.ReadyAssessed(err)`,
  `reconciler.IsReadyAssessed(err)`, and the package
  `pkg/reconciler/reconcilertest` with `AssertObserveSetsReady`.
- **Migration** (no provider changes until it bumps): see
  [usage](./usage.md#migrating-to-v180). Bump in the same PR as the
  provider's `Observe` fixes. core-provider's rollup of `NotObserved`
  (core-provider#310) must be released before a v1.8.0 provider build
  reaches a cluster.

## 2026-10-31 — v1.7.2
- A resource-version conflict while persisting late-initialized fields now
  requeues at once, like every other write to the managed resource. Before,
  it was reported as `ReconcileError` with a Warning event and waited out
  error backoff. The condition never stuck: the status write carried the same
  stale version and failed too. But each lost race showed up as an error.
  Found while checking provider-runtime for the shape unstructured-runtime
  fixed in v1.7.2, where a conflict became a lasting `ReconcileError`; that
  shape does not occur here (see [overview](./overview.md)).
- No API change.

## 2026-10-30 — v1.7.1
- `pkg/logging` now builds alongside `github.com/krateo-platformops/audit`
  v0.3.0. v1.7.0 pinned the OTel slog bridge at `otelslog` v0.8.0 with
  `otel/log` v0.9.0. audit requires `otel/log` v0.21.0, Go's version
  selection takes the higher, and `otelslog` v0.8.0 doesn't compile against
  it (`undefined: log.KeyValue`). Any provider using both failed to build
  until it bumped `otelslog` itself.
- Now `otelslog` v0.20.0 + `otel/log` v0.21.0, the pair that matches this
  module's `otel` v1.45.0. The core OTel modules are unchanged.
- No API or behaviour change.

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
