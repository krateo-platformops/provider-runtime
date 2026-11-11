---
type: Log
title: provider-runtime — log
description: Curated chronological history of provider-runtime — notable changes and decisions, newest first.
resource: github.com/krateo-platformops/provider-runtime
tags: [library, history]
timestamp: 2026-11-11T00:00:00Z
---

# Log

Curated history — notable changes and decisions, newest first. Full release
notes live in GitHub Releases; the mechanics are in [release](./release.md).

## Unreleased — v1.17.0: a failed write leaves mg's version
- **A minor release: one new export,
  `reconcilertest.AssertFailedWriteLeavesManaged`.** Behaviour is unchanged.
  It's the #80 follow-up the user approved after v1.16.0, from
  oasgen-provider's request in the review of #84.
- **What it checks:** the `ExternalClient` ordering rule. A method whose
  write of the managed object fails must leave `mg`'s resourceVersion
  untouched. A version copied onto `mg` too early, from a re-read, lets the
  runtime's next write succeed against an object the method never wrote. A
  stale version fails loudly instead.
- **How:**
  - The caller passes a kube client that fails the method's writes, for
    example the fake client with an interceptor whose `SubResourceUpdate`
    returns a conflict.
  - The helper first moves the stored version with its own `kube.Update` of
    one annotation, so a re-read returns a version `mg` doesn't carry. The
    caller's interceptor must let that Update through; failing only status
    writes does.
  - It then requires the method to return an error and its write not to
    land, and fails if `mg`'s resourceVersion changed. Only the version is
    checked: a method may change status in memory on failure.
  - A `retry.RetryOnConflict` loop that fails on every attempt is covered.
  - The rule is strict, by the user's decision: re-reading the object into
    `mg` itself, for example inside `RetryOnConflict`, fails too. Re-read into
    a copy and copy back only after success. A re-read into `mg` drops the
    pass's in-memory status, and on the REST client the Get merges the stored
    object into `mg` rather than replacing it. The failure message says so.
- **Its tests:**
  - These pass: copying the version only after success, with or without a
    retry loop.
  - These fail: copying the re-read version before the write, or despite
    its error, also inside a retry loop; re-reading into `mg` inside a retry
    loop; a method that returns nil; a write that landed; a stale `mg`.
- The `ExternalClient` doc now names both helpers.

## 2026-11-11 — v1.16.0: a status conflict names its cause
- **A minor release: the error text of a status conflict changes, and there
  are four new exports:** `reconciler.WithAPIReader`, `tracing.StepStatus`,
  `reconcilertest.AssertLeavesManagedCurrent` and `reconcilertest.Method`.
  It's option 1 of #80, the user's decision of 2026-11-11
  (design-status-write-conflict §7.1, §12). Behaviour is otherwise unchanged:
  a status write is still sent once, with no retry and no merge.
- **The contract is written down.** The `ExternalClient` doc now says a
  method that writes the managed object must leave `mg` describing what is
  stored: write `mg` itself, or copy the new resourceVersion and every field it
  wrote back onto `mg`, only after the write succeeded. Copying only the
  version silently reverts the write. A re-read-then-write can revert someone
  else's change, a human's status patch included.
- **The error names the likely cause.** A status write that conflicts now
  returns `cannot update managed resource status: <the API server's conflict>:
  the object changed since the pass's read (read at resourceVersion V): if an
  ExternalClient method writes the object itself, it must copy ... back onto mg
  (see ExternalClient); pass error: <the pass's own error>`.
  - It starts with what it said before, the prefix and the API server's
    conflict text, so existing searches still match.
  - `kerrors.IsConflict`, `errors.Is` on the pass's error, and
    `resource.IsAPIErrorWrapped` still see through it: it's a
    `kerrors.APIStatus` itself, the conflict's.
  - **So `resource.IsAPIError(err)` is now true on the bare error,** where
    it was false for the old wrapped one. A consumer that wraps a call in
    `retry.OnError(…, resource.IsAPIError, …)` would now retry a status
    conflict.
  - Before, on an error path, the status conflict replaced the pass's own
    error (an Observe or Update failure, for example). Now both are kept.
  - It stays an error that `Reconcile` returns, not a silent requeue.
  - All 20 status writes go through one function, so every one gets it. A
    status write that fails with anything but a conflict reads as before.
  - **Consumers' tests may need updating:** a test that compares a status
    conflict's error with `test.EquateErrors` against
    `errors.Wrap(conflict, errUpdateManagedStatus)` now fails, because both the
    type and the text differ. Compare with `kerrors.IsConflict`, or the prefix.
- **New step `status` on `k8s.conflict`** (`tracing.StepStatus`, in
  `tracing.Steps()`). It's the one step whose pass returns an error, so its
  root is Error.
- **`reconciler.WithAPIReader`.** Pass `mgr.GetAPIReader()` and a status
  conflict reads the object live once, for the message only: it adds the live
  resourceVersion and generation (next to the pass's, so a spec edit shows),
  the top-level status keys that differ from the pass's read, and the newest
  managedFields entry, labelled as a hint. Nothing is decided from it, and a
  failed read gives the plain message.
  - It's opt-in. `NewReconciler` can't take the reader from its manager:
    `fake.Manager` embeds a nil `manager.Manager`, whose `GetAPIReader` would
    panic, and so would any consumer's fake built the same way.
  - Its cost: one in-memory deep copy of the managed object per pass (the
    pass's read, kept for the comparison), and one uncached Get, only when a
    status write conflicts. With a rate-limit governor, the governor's copy
    is that same one, except on a pass where the trace marker write moved the
    version (at most once per trace window), which takes two.
- **`reconcilertest.AssertLeavesManagedCurrent`,** with its `Method` type. It
  runs one of a provider's methods against a stored object, writes `mg`'s
  status as the runtime would, and fails if that write conflicts or reverts a
  status field the method wrote. It checks the contract in a provider's own
  tests, on envtest or controller-runtime's fake client.
- **Consumers need no change.** oasgen-provider mirrors, incident-provider and
  git-provider write through `mg`, and alert-provider and core-provider don't
  write `mg` from handlers (design-status-write-conflict §4).

## 2026-11-11 — v1.15.0: symmetric act links, and acting passes are marked
- **A minor release: two tracing behaviours change.** Both are the user's
  decisions of 2026-11-11. Nothing changes with tracing off.
- **The act links to its root only when the root is sampled** (#74). Before,
  the act's link to its root was unconditional, while the root's link to the
  act needed the act sampled. Now both directions follow one rule: link only to
  a sampled target. Under `parentbased_always_on`, which every adopting chart
  uses, every root is sampled, so nothing visible changes today.
  "Sampled" is the head decision; the collector's tail sampler still decides
  what's stored (design-tracing §6.2).
- **`provider_runtime.acted=true` on the root of every pass that starts an act
  step** (create, update or delete), joined upstream or not, failed or not
  (design-tracing §6.7). An observe-only pass carries no such attribute, and
  it's never set to false. observability's tail sampler keeps traces with it
  (otel-collector-tailsampling 0.1.11), so a change keeps its reconcile even
  for a provider sampled at 5%. New exports: `tracing.AttrActed` and
  `tracing.MarkActed`.
- **Plumbing floor: v1.48.0** (up from v1.45.0), which carries both changes
  (plumbing#130). Consumers bumping plumbing for unstructured-runtime should
  go to v1.48.0 if they also adopt this release.
- **Also since v1.14.3:** a test pins that the default TracerProvider is read
  on every pass, not once at construction (#79).

## 2026-11-11 — v1.14.3: shared event names
- **A patch release. Behaviour is unchanged:** every span, event and
  attribute carries the same bytes as in v1.14.2.
- **Shared names:** `pkg/tracing`'s `EventReconcilePaused`
  (`reconcile.paused`), `EventConflict` (`k8s.conflict`), `EventMarkerFailed`
  (`trace.marker_failed`) and `AttrStep` (`provider_runtime.step`) now
  reference plumbing's `tracecontext` constants (plumbing v1.45.0,
  plumbing#126). unstructured-runtime emits the same constants, so a query or
  a sampler rule reads the same bytes from either runtime.
  - Their names and docs are unchanged.
  - The rate-limit events and keys stay provider-runtime's own.
  - `TestAliasedNamesAreTheirStrings` pins all four literal strings.
- **The plumbing bump, v1.44.0 to v1.45.0:** it's one commit, which adds
  `tracecontext/events.go` and its test.
  - Nothing else provider-runtime links changes. `env`, `kubeutil/event` and
    the rest of `tracecontext` are byte-identical.
  - No other module in the build graph moves.

## 2026-11-11 — v1.14.2: the trace rule lives in plumbing
- **A patch release. Behaviour is unchanged,** with tracing on or off.
- **The rule's source of truth is plumbing's `tracecontext`** (plumbing
  v1.44.0, plumbing#125). provider-runtime and unstructured-runtime now apply one
  identical `krateo.io/traceparent` rule, D8's (the user's decision).
  - `pkg/tracing`'s `Decide`, `Decision`, `Mode`, `When`, `Canonical`,
    `Object` and `StartReconcile`, `StartActStep` and `StartStep` are aliases
    and thin wrappers over it. Every exported name, signature and doc is
    unchanged.
  - The Start functions keep their `TracerProvider` signatures and pass
    `tp.Tracer(ScopeName)`, or nil when `tp` is nil, so every span keeps
    provider-runtime's instrumentation scope.
  - `Inject` reads the inbound context through `tracecontext.Inbound`.
- **What pins it:** provider-runtime's own tracing tests (D8, envtest, the
  zero-allocation tests) pass unchanged. New tests pin that a nil `tp`
  allocates nothing, that every span carries `ScopeName`, that `pkg/meta`'s
  three keys equal `tracecontext`'s, and the 10-minute default window.
  - `pkg/meta` keeps its own constants, so it still doesn't import
    OpenTelemetry.
  - `go list -deps ./pkg/tracing` gains only `plumbing/tracecontext`, with
    no SDK.
- **The plumbing bump, v1.13.0 to v1.44.0:**
  - It changes nothing else that provider-runtime links. Its other plumbing
    packages, `env` and `kubeutil/event`, are byte-identical in both
    versions.
  - No other module in the build graph moves.
  - plumbing's `go 1.26.0` directive matches provider-runtime's.
- **Follow-ups:** provider-runtime#72 closes, covered by plumbing's
  `TestStartActStepRecordOnlyActGetsNoRootLink`. `pkg/tracing` keeps a copy of
  that test, so a plumbing bump that broke the guard fails here too.
  provider-runtime#74 stays open, pinned by plumbing's
  `TestStartActStepLinksToAnUnsampledRootAsInV1141`. A change to it goes in
  `tracecontext`.

## 2026-11-10 — v1.14.1: every reconcile is its own trace
- **Design:** [design-tracing](./design-tracing.md) §13, D8, decided by the
  user on 2026-11-10 after krateo-057's first day under v1.14.0.
- **The behaviour change, with tracing on:**
  - Every `reconcile` root is a new trace. When the object carries a valid
    `krateo.io/traceparent`, the root has one link to it. In v1.14.0, a pass
    inside a new value's 10-minute window was that value's child.
  - Only the first act step for a new value, the `create`, `update` or
    `delete` span, is the value's child. It carries a link back to its
    `reconcile` root and the root's three `k8s.object.*` attributes, and the
    root links to it (when the act is sampled). What the provider does under it
    with the method's `ctx` joins the upstream trace too; `connect`, `observe`
    and every other pass don't, and every other root has exactly one link.
  - Under `parentbased_always_on`, the joined act follows the stamp's sampled
    flag: a `00` stamp drops it. Charts set `OTEL_TRACES_SAMPLER` to
    `parentbased_always_on` or leave it unset; `always_on` is allowed; never a
    ratio sampler.
  - The marker is unchanged: written with the optimistic lock just before
    that act, or when the window closes with no act. The window parents
    nothing now.
  - With tracing off, nothing changes.
- **Why:** the composition-dynamic-controller stamps every object of a
  release on every apply. Under v1.14.0, every chart upgrade put up to 10
  minutes of every child's observe-only polls into the upgrade's trace: on
  krateo-057, about 260 reconciles per upgrade, and the reconciles of 28 Alerts
  in one composition's trace (at least 3,204 spans, about 13 times the
  volume). The tail sampler kept those traces whole. It wasn't a marker bug:
  28 of 28 markers were written, and the windows closed as designed.
- **For tail sampling:** a trace that holds a stamper's span gains one
  `update`, `create` or `delete` span per object that acts on the new value,
  each with the object's `k8s.object.*` attributes and a link to a `reconcile`
  root in a trace of its own, which links back. Observe-only reconciles never
  join it.
- **API:** `tracing.StartActStep(ctx, tp, obj, step, upstream)`. `StartReconcile`
  now always starts a new root, and links to `d.Remote` in Parent and Link
  mode.

## 2026-11-10 — v1.14.0: trace every reconcile
- **Design:** [design-tracing](./design-tracing.md) (#60), built in #64, #65
  and #66, each reviewed independently.
  - `telemetry.Setup` gains a TracerProvider, and the reconciler opens a
    `reconcile` root span with `connect`, `observe`, `create`, `update` and
    `delete` children.
  - A new `krateo.io/traceparent` value is continued by the first pass that
    acts on it, within a 10-minute window (`WithTraceContinueWindow`), and is
    linked after that.
  - The marker is the `krateo.io/traceparent-continued` annotation
    (`meta.AnnotationKeyTraceparentContinued`), written with an optimistic
    lock.
- **What's in it:**
  - `pkg/tracing`: `Decide`, `StartReconcile`, `Inject`, `InjectHeader` and
    the event and step names.
  - `telemetry.Setup`'s tracing (`Config.TracingEnabled`, `ConfigFromEnv`,
    the shared resource, the protocol check following OTel's precedence).
  - httpsignal's propagation of the active span.
  - The reconciler's wiring:
    - the root span and its children, the events and the status mapping;
    - the memo, keyed by NamespacedName with the UID;
    - the locked marker write before the first act, or when the window closes,
      with a requeue before acting on a conflict;
    - `WithTracerProvider` and `WithTraceContinueWindow`.
- **Changes to existing behaviour:**
  - **The critical write carries only the runtime's own keys** (#64).
    `RetryingCriticalAnnotationUpdater`, the default
    `CriticalAnnotationUpdater`, writes back only `krateo.io/external-name`,
    the `krateo.io/external-create-*` markers and
    `krateo.io/traceparent-continued`. Before, it captured every annotation,
    re-read the object and wrote the captured map back, reverting a fresh
    `krateo.io/traceparent`, and any other writer's annotations, set while
    `Create` ran. It affects a provider that sets some other annotation on its
    managed resource in memory and relies on this write to persist it. No
    consumer in krateo-platformops does: the scan is in design-tracing §2.
  - **`telemetry.Setup` sets the global propagator** (W3C TraceContext and
    Baggage) on every call, tracing on or off. OTel's default is a no-op, so a
    provider's own `otelhttp` starts honouring and forwarding W3C context and
    baggage. core-provider and oasgen-provider already set the same
    propagator themselves.
  - **httpsignal sets `traceparent`** on outbound requests when tracing is
    on. With tracing off, it sends nothing.
  - **"Reconciling" is logged after the Get.** The per-reconcile Logger is
    bound once the root span starts, which needs the object, so the
    "Reconciling" debug record now follows the Get. A pass whose object is
    gone (NotFound) logs no "Reconciling" record.
  - **With tracing on, a concurrent write requeues before acting.** A write
    to the object between the pass's Get and the locked marker patch makes the
    update and delete paths requeue before they act, backing off from 1 s to
    60 s. Before, the act ran and the status Update failed instead. An object
    written more often than one Connect and Observe can see its act slip until
    the 10-minute window closes. That's bounded, and it's what the create path
    already does with its create-pending Update.
- **New dependency:**
  `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp` v1.45.0.
- **Adoption:** design-tracing §11. core-provider and oasgen-provider delete
  their own `SetupTracing` when they bump; observability adds each provider's
  `OTEL_SERVICE_NAME` to the tail sampler before its chart turns tracing on.

## 2026-11-09 — v1.13.0: logs can correlate with traces
- **Additive; nothing breaks.** `logging.ContextLogger` (`WithContext(ctx)
  Logger`) is an optional interface, so a `Logger` written without it still
  compiles. The helper `logging.WithContext(l, ctx)` binds `l` when it
  implements it and returns `l` unchanged when it doesn't.
- **Why:** `NewOTelJSONHandler` already stamped `trace_id` and `span_id` from
  the record's context, but the `Logger` had no context to give it. The slog
  Logger called slog without one, so every record carried
  `context.Background()` and no controller could correlate a log with a trace.
- **The slog Logger** passes the bound context with every record, and keeps
  it through `WithName` and `WithValues`. The nop Logger stays a nop. **The
  logr Logger can't carry a context**: logr's sink takes none, and its slog
  adapter calls the handler with `context.Background()`. It comes back
  unchanged.
- **`Reconcile` binds its per-reconcile Logger** to the `ctx` it is given.
  Neither provider-runtime nor controller-runtime starts a span there, so its
  records correlate only when the provider wraps the reconciler in one that
  does. Providers bind their own Logger in `ExternalClient` methods with
  `logging.WithContext(log, ctx)`; the greeting example now does.
- **Correction:** `NewOTelHandler`'s doc named a `SetupOTLPLogs` that never
  existed. The binary's main installs the OTLP LoggerProvider, with
  `global.SetLoggerProvider`.
- **Ships first,** as its own minor release, before the credential-use and
  cost-model work.

## 2026-11-09 — unreleased, decided, not built: size local capacity from what reconciles cost
- **Design:** [design-saturation-cost-model](./design-saturation-cost-model.md),
  reviewed independently five times. The saturation input stops halving on a pressure
  reading and recovering on a timer. It measures what reconciles cost, CPU per
  slot-second and live heap per reconcile, and computes how many fit in the
  container's limits: CPU at `CPUTarget` (0.9) of `cpu.max`, memory within the
  Go memory limit less a reserve. Whole slots by error diffusion. CPU pressure
  only blocks increases. Measured on GKE the old step settled at 71% of a 250m
  limit with 31% less throughput; the model predicts 4.56 slots there, at 90%.
- **Breaking, for an off-by-default feature, when it is built:**
  `StartupRampInterval`, `WorkingSetSoftRatio`, `WorkingSetHardRatio` and
  `ExperimentalMemoryWorkingSet` are removed, with their `Default…` constants and
  the `memory_working_set` input; `RecoveryInterval` no longer touches local
  capacity. Memory becomes a separate `Memory` toggle, off by default even with
  saturation on. New options: `CPUTarget`, `MemoryReserve`, `GoMemoryLimit`.
- **Behaviour change with no CPU limit:** today pressure over 50 halves local
  capacity; the design learns the CPU the node gives at pressure onset and
  applies the same law to it.
- **Not handled, by decision:** a downstream that slows without rate-limit
  signals raises concurrency up to the ceiling; provider-runtime#50 is the
  follow-up.

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
