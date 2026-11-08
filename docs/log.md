---
type: Log
title: provider-runtime — log
description: Curated chronological history of provider-runtime — notable changes and decisions, newest first.
resource: github.com/krateo-platformops/provider-runtime
tags: [library, history]
timestamp: 2026-11-08T00:00:00Z
---

# Log

Curated history — notable changes and decisions, newest first. Full release
notes live in GitHub Releases; the mechanics are in [release](./release.md).

## 2026-11-08 — v1.12.0: the local slot is taken before the pop
- **With saturation on, each controller's queue takes the local slot before it
  pops an item,** and gives it back when the reconcile returns. Low capacity
  behaves like fewer workers: queue order is the order work runs in, and no
  popped item sleeps through a freed slot, as a deferred one did before
  (unstructured-runtime#72's shape). The slot covers the whole reconcile, so the
  requirement that providers do expensive work inside the gated calls is gone.
  An idle controller holds no slot, so controllers sharing a governor don't
  starve each other; freed slots go round-robin across queues with work.
- **Breaking, for an off-by-default feature:** a full local capacity writes
  nothing on the object. `DeferSaturated`, `DeferRamping`, `Deferral.Local`,
  `Deferral.Cause` and the quiet startup ramp are gone, and a call without a key
  is no longer gated. The `deferrals` metric loses its `saturated` and `ramping`
  causes. The input gauges are unchanged. `ratelimit.local.capacity` now reports
  nothing with saturation off, as core-provider's does, because nothing
  enforces a local capacity then.
- New API: `ratelimit.NewGatedQueue`, `(*Governor).RegisterGatedController`.
  Saturation with `UsePriorityQueue=false` is now an error from
  `SetupRateLimitGovernor`. `Start` logs an error when a reconciler's controller
  wasn't built with `ForControllerRuntime`, and warns when a second Governor with
  saturation runs in the process.
- Design: [design-local-slot-before-pop](./design-local-slot-before-pop.md).
- **Measured on GKE on 2026-11-08,** on an e2-standard-4 node, with a 250m CPU
  limit, 16 workers and a liveness probe. Each call used 20ms of CPU plus a
  400ms wait. Saturation off, threshold 75 and threshold 50 had 5 runs each.
  No run had a restart, a failed probe or a discard. With saturation off, CPU
  sat at the limit, with pressure at 53-60%. Threshold 75 never engaged. At
  threshold 50 the queue slowed reconciles down at the limit as designed, but
  overshot: it settled at about 177ms/s, roughly 70% of the limit, with 31%
  less throughput. The cause is the step rule, not the gate. It halves on every
  1s sample of a 10s-average signal, then recovers by one slot every 30s. A
  data-driven replacement, which sizes concurrency from the limits and the
  measured cost of each reconcile, is being designed. Until it ships,
  saturation stays opt-in and off by default.
- **The release also moves to controller-runtime v0.25.2 and `k8s.io/*`
  v0.37,** from v0.23.3 and v0.35 (`k8s.io/api`, `apimachinery`, `client-go`
  and `apiextensions-apiserver` v0.37.2; `controller-tools` v0.22.0). The
  reason is v0.25.0's fix for a priority queue hang on shutdown
  ([controller-runtime#3540](https://github.com/kubernetes-sigs/controller-runtime/pull/3540)).
  If the queue shut down while handing an item to a waiting pop, `Done` and
  `Len` could block for good. The ungated controllers, which use that queue
  by default, could hit it at shutdown. A targeted test hung in 2,788 of
  200,000 runs on v0.23.3, and in none of 200,000 on v0.25.2. The gated
  queue's own guard against it stays, and design §4 says why.
- **Consumers must bump** `sigs.k8s.io/controller-runtime` to v0.25.x and
  `k8s.io/*` to v0.37.x in their own `go.mod`. Go raises them anyway when
  provider-runtime is upgraded. Their code must then build against
  controller-runtime v0.24 and v0.25. The breaking changes there are the
  `k8s.io/*` v0.36 and v0.37 APIs and `GetEventRecorder`, which now returns
  controller-runtime's `recorder.EventRecorder`. That type includes client-go's
  `events.EventRecorder`, so passing it on to plumbing's recorder still works,
  but a fake `recorder.Provider` must change. `WebhookBuilder.WithCustomPath`
  is removed and the scheme builder is deprecated. The `go` floor stays 1.26.0.
  envtest moves to 1.37.0, with setup-envtest from release-0.25.
- **Behaviour change: a server's hold is capped at `RateLimit.MaxHold`,
  default 1h.** Before, a refusal held its key for as long as the server
  asked, with no bound, so a broken or hostile server sending
  `Retry-After: 86400`, or a reset time a year away, froze every object on the
  key for that long, deletions included: the finalizer needs `Connect` and
  `Observe`, which are gated. A mock-429 probe on a test cluster found it. Now
  a deadline past now+`MaxHold` is cut to it. After the cut hold one call goes
  through; if the server still refuses, the key is held again. With the
  default, a server that asks for longer than 1h gets one call per hour
  instead of a full hold: a hold longer than `MaxHold` is retried at `MaxHold`
  intervals, so a server asking for 6h receives 6 requests instead of 1. Set
  it to 0 to honour any hold in full, which restores the old unbounded hold.
  Negative is an error.
- New API: `RateLimitOptions.MaxHold` and `ratelimit.Options.MaxHold`
  (`*time.Duration`: `nil` is 1h, 0 is no cap), `ratelimit.DefaultMaxHold`,
  and `MetricsRecorder.IncRateLimitHoldClamped`, which a custom
  `MetricsRecorder` must now implement. Suggested flag and env:
  `--rate-limit-max-hold` / `<NAME>_PROVIDER_RATE_LIMIT_MAX_HOLD`; the example
  provider declares them. The first cut per key is logged at Info, once until
  the key goes a whole `RecoveryInterval` clean, and every cut is counted in
  the new `provider_runtime.ratelimit.hold_clamped{key}` counter.

## 2026-11-08 — v1.11.0: scale idle controllers to zero
- **`pkg/scaletozero`**, for owners that run one controller Deployment per
  Kind or per version (core-provider, oasgen-provider). While the Kind has
  no instances the controller runs 0 replicas; when one appears it goes back
  up. On krateo-057, 32 idle controllers used 236 MiB. Settled with both
  owners in #39. Design: [design-scale-to-zero](./design-scale-to-zero.md).
- New API: `scaletozero.Exists`, `Decide`, `Reconcile`, `Result`, `Reason`
  (`Active`, `IdleHoldDown`, `Idle`, `Unknown`).
- **The helper is the mechanism, not the trigger.** It watches nothing: the
  owner must reconcile when the first instance of a Kind appears.
- Decisions recorded in the design:
  - `Exists` instead of `Count`: the decision is binary, so it lists one
    item at a time, stops at the first match, and follows `Continue` past
    empty pages.
  - An error never reads as 0: `Result.Replicas` is then the current
    replicas if non-zero, else `active`, with reason `Unknown`, and nothing
    is patched.
  - `Reconcile` takes `active`. It wakes a controller from 0 to `active`,
    puts an idle one to 0, and leaves any non-zero count alone while
    instances exist.
  - `Exists` needs a live reader (`mgr.GetAPIReader()`), and `Reconcile` a
    client from `client.New`.
- **Precondition:** the caller writes the Deployment's `spec.replicas`
  nowhere else, including a full-object Update that omits it.
- Tests run against envtest. CI installs its binaries, and the tests fail
  rather than skip under `CI` without them.

## 2026-11-07 — v1.10.1: queue telemetry keeps item priorities
- **With `QueueWaitRecorder` set, the priority queue now honours priorities.**
  The instrumented wrapper implemented only the plain workqueue interface, so
  controller-runtime wrapped it again and ran every item at priority 0: spec
  changes queued behind low-priority resyncs. Over a priority queue the
  wrapper is now a `priorityqueue.PriorityQueue` too, and `AddWithOpts` and
  `GetWithPriority` record wait, depth, oldest age and work like the other
  methods. With `UsePriorityQueue=false` nothing changes.

## 2026-11-05 — v1.10.0: one object on several rate-limited endpoints
- **`reconciler.RateLimitKeysProvider`** names every scope an object's calls
  reach, for objects that sync between two endpoints. A call needs a slot in
  every key plus one local slot, all or nothing, and is deferred when any key
  is throttled or full. Found by git-provider's Repo, whose `fromRepo` and
  `toRepo` can be different servers with different credentials: no one key
  could describe it honestly.
- New API: `reconciler.RateLimitKeysProvider`, `(*ratelimit.Governor).AcquireAll`.
  Additive: `RateLimitKeyer` and `Acquire` behave as before.
- `Deferral.Key` is now the key that stopped the call. For a local deferral of
  a call with several keys it is empty, and so is the `key` attribute on
  `provider_runtime.ratelimit.deferrals`.

## 2026-11-05 — v1.9.1: telemetry.Config.ServiceName works
- **The field now sets `service.name`**, unless `OTEL_SERVICE_NAME` or
  `OTEL_RESOURCE_ATTRIBUTES` name one. It never had any effect before. The
  unused `provider-runtime` default is gone, because applying it would have
  given every provider the same name.

## 2026-11-05 — v1.9.0: rate-limit governor
- **Reconciles slow down by themselves** when an external API signals a rate
  limit, per endpoint key, or when the controller's own container is
  CPU-saturated. It replaces hand-tuning `--max-reconcile-rate` and
  `--poll`. Design: [design-rate-limit-governor](./design-rate-limit-governor.md).
- **Off by default.** With nothing configured, nothing is gated and
  `ratelimit.Report` is a no-op. A provider turns each input on with one chart
  flag. The rate-limit input (`controller.Options.RateLimit.Enabled`) also
  needs `reconciler.RateLimitKeyer` and reported signals. The saturation input
  (`RateLimit.Saturation.Enabled`) bounds every gated call, keyer or not.
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
- **It ships rate-limit and CPU slow-down, not memory protection.** The user
  decided this from kind measurements on the example provider (see
  [configuration](./configuration.md#the-pressure-thresholds-are-provisional)):
  - Saturation reads CPU pressure only, on a 1s sample. The threshold stays
    50%, provisional until core-provider validates it. Under a CPU limit below
    one core that means close to one call at a time; raise the threshold or
    the limit if that's too slow.
  - The memory working set is experimental and off by default
    (`ExperimentalMemoryWorkingSet`). It did not prevent OOMKills in 0 of 19
    runs on an overcommitted workload. 0.80/0.90 are its provisional ratios.
  - `memory.pressure` is dropped: it read 0 up to every OOMKill.
  - `local` starts at 1 and rises by 1 per 2s until its first cut. After any
    cut it rises by 1 per clean `RecoveryInterval` (30s), like API limits. One
    call always keeps going.
  - Saturation bounds calls without a keyer too.
  - A deferral that would write the object back unchanged skips the status
    update.
  - A deferral on the startup ramp is quiet: no `Synced`, no event, and only
    the `deferrals` metric shows it, under cause `ramping`. Otherwise a restart
    turned every healthy object `Synced=False`.
- Follow-up for the memory input is filed in the design's §11: setting the Go
  memory limit, testing headroom under it as the input, and the re-test gates.

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
