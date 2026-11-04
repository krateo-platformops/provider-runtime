---
type: Architecture
title: provider-runtime — overview
description: Package design and the managed-resource reconcile flow provider-runtime drives for every Krateo provider.
resource: github.com/krateo-platformops/provider-runtime
tags: [library, controller-runtime, reconciler, architecture]
timestamp: 2026-11-04T00:00:00Z
---

# Overview

provider-runtime is a library, not a service: it is compiled into every Krateo
provider binary (core-provider among them) and drives the lifecycle of
**managed resources** — Kubernetes custom resources that each represent one
concrete resource in an external system (a chart release, a Git repository, a
cloud API object). The design descends from the Crossplane runtime pattern:
the provider author writes only the external-system adapter; the library owns
the reconcile loop, status conditions, finalizers, annotations, events,
logging, and metrics.

## The split: reconciler vs ExternalClient

The core contract lives in `pkg/reconciler`:

- **`ExternalConnecter`** — `Connect(ctx, mg) (ExternalClient, error)`: builds
  a client for the external system, typically reading credentials referenced
  by the managed resource (helpers: `resource.GetSecret`,
  `resource.GetConfigMapValue`).
- **`ExternalClient`** — four methods the provider implements:
  - `Observe` returns an `ExternalObservation` (`ResourceExists`,
    `ResourceUpToDate`, `ResourceLateInitialized`, `Diff`) and must not mutate
    the external system;
  - `Create` is called when the external resource does not exist (it may
    update annotations — e.g. set the external name — and those are
    persisted);
  - `Update` is called when the observation reports out-of-date;
  - `Delete` is called when the managed resource is being deleted.

`reconciler.NewReconciler(mgr, resource.ManagedKind(gvk), opts...)` assembles
the generic `Reconciler` (a `reconcile.Reconciler`) around that client. Per
reconcile it: gets the managed resource, honors the pause annotation
(`krateo.io/paused`), connects, observes, then creates/updates/deletes
according to both the observation and the **management/deletion policy
annotations** (`pkg/meta.IsActionAllowed`, `ShouldDelete`, …). It manages the
finalizer (`finalizer.managedresource.krateo.io`), records Kubernetes events
(via the plumbing event recorder, with an optional throttled recorder), sets
the `Ready`/`Synced` conditions from `apis/common/v1`, and requeues on the
poll interval (default 1m, jitter/hook configurable).

**Conflicts are transient, not errors.** A resource-version conflict on any
write to the managed resource is retried, never reported as
`Synced=False/ReconcileError`:
- the writes covered are the finalizer, the external-create annotations and the
  late-initialized fields (since v1.7.2);
- the external-create annotations go through `RetryingCriticalAnnotationUpdater`,
  which re-reads the object and retries;
- the other writes requeue at once and redo the work from a fresh read.

A failed status write records nothing and returns its error, so the reconcile
is retried with backoff. Every path that does record `ReconcileError` also
requeues, and the next successful pass sets `ReconcileSuccess`, so a lost race
cannot leave the condition stuck.

A guard against half-completed creates: before calling `Create` the reconciler
stamps `krateo.io/external-create-pending`. Afterwards it records
`-succeeded` or `-failed` through a `CriticalAnnotationUpdater` (retrying
variant in `pkg/reconciler/api.go`), which re-reads the object before writing.
Every `Create` error is stamped `-failed`, including a resource-version
conflict, so the next reconcile retries `Create` (since v1.5.2).

A handshake whose outcome was never recorded (`ExternalCreateIncomplete`:
`pending` is newer than both terminal markers) is resolved by asking `Observe`
(since v1.4.0):

- **Resource exists**: the create landed. The reconciler stamps `-succeeded`
  and adopts it.
- **Resource absent, pending younger than the recovery grace period** (default
  2m): it keeps refusing and re-observes every 15s, because an eventually
  consistent API may not show a create that landed yet.
- **Resource absent, grace period elapsed**: it clears `-pending` and retries
  `Create`.
- **Managed resource being deleted, external resource absent**: deletion
  proceeds.

**The contract this puts on `Observe`:** when the external resource does not
exist, return `ExternalObservation{ResourceExists: false}` with a **nil**
error. An `Observe` error ends the reconcile before both the guard and
`Create`, with or without a pending create. So an `Observe` that errors on
"not found" never creates anything. On a half-created resource it also holds
the unresolved handshake, because the guard needs `Observe`'s answer.

**The common trap is not a mishandled "not found".** It is an error return
*earlier* in `Observe`, on a prerequisite the provider reads before it can
look: a chart, a template, a credential, a registry. core-provider's
`Observe` handled "not found" correctly the whole time. Its existence check
was unreachable behind four error returns on reading the chart. So check your
not-found branch, and also every error return that **precedes** your existence
check. Ask whether the condition causing it can persist across reconciles (an
unreachable registry, an unreadable input, a missing credential). If it can,
it holds the resource for as long as it persists. Where existence can be
answered without that prerequisite, answer it. core-provider#236 took the GVK
the question needed from the object's status instead of from the chart.

The handshake itself never holds the resource beyond that. Once `Observe`
answers again, the guard adopts the resource or retries the create within the
recovery grace period. While `Observe` errors on an unresolved handshake, the
`Synced` condition names both the `Observe` error and the pending create
(since v1.6.0). For example:

```
external create pending since 2026-10-28T18:12:41Z cannot be resolved until observe succeeds: observe failed: <the provider's error>
```

The runtime deliberately does not bound this by calling `Create` anyway. A
create without an observation can duplicate exactly the resource whose
identity was lost, which is what the guard exists to prevent. And it gains
nothing when `Create` hits the same failure.

So the fix belongs in `Observe`. For each error return, ask: **if I answered
`ResourceExists: false` here and the resource did exist, would `Create`
duplicate it?**

- **No**, because `Create` is idempotent for this resource (a deterministic
  name, or an apply that converges): answer instead of erroring. Report the
  cause through a condition if it helps, for example
  `Unavailable().WithMessage(...)`. If the same failure then hits `Create`,
  it surfaces there as a create error, which closes the handshake
  (`external-create-failed`).
- **Yes**, because identity could be lost: an error is correct, but only for a
  *transient* "cannot tell", such as a failed read from the external system
  that a retry can answer.

Treat a **permanent** "cannot tell" as a defect to fix, not as an error to
return. Examples are a local template that fails to render, or a spec that
cannot be parsed into a lookup. Retrying never resolves it, so the resource
stays stuck until the provider changes. Watch for one error return that
carries both kinds: oasgen-provider#266 found a single call that wraps two
local renders and two cluster calls, so it can't be classified at the call
site without splitting it. core-provider#236 shows the fix: answer existence
from what the object already records, and let `Create` report the failure.

## The Ready contract (since v1.8.0)

`Synced` says whether the last reconcile worked. `Ready` says whether the
external resource is ready, and only `Observe` can know that. So `Ready` is
the provider's to set, and the contract has three rules.

**1. Every `Observe` that returns a nil error sets `Ready`.** This covers every
path, including the ones that report `ResourceExists: false` or
`ResourceUpToDate: false`. Use `Available()`, `Unavailable()`, `Creating()` or
your own `Ready` condition. The runtime still sets `Creating` after a
successful `Create` and `Deleting` while it deletes. A managed resource that
is being deleted (it has a deletion timestamp) is the one exception: on every
deletion path the runtime sets `Ready=Deleting` itself, so `Observe` doesn't
owe `Ready` there. Setting it anyway is allowed and harmless (since v1.8.1).

**2. When `Connect` or `Observe` fails, the runtime sets `Ready` to
`NotObserved`.** It writes this next to `Synced=False/ReconcileError`. Before
v1.8.0 only `Synced` changed, so `Ready` could show a stale `True` next to
the error. Two cases are different:

- The resource is being deleted (it has a deletion timestamp, or `Ready`
  already reads `Deleting`). The runtime sets `Ready=Deleting`, which stays
  true whatever the external system answers. Since v1.8.2 it does this itself,
  so a provider doesn't need to set `Deleting` before returning an error on
  its deletion path (doing so stays harmless).
- The error is wrapped with `reconciler.ReadyAssessed`. `Observe` returns
  this when it set `Ready` itself and then failed on something else, for
  example a write after the read. `ReadyAssessed` keeps the error's message,
  and `errors.Is`/`errors.As` see through it. Don't wrap an error that means
  "I could not tell": there `NotObserved` is the true answer.

A resource-version conflict still requeues without touching any condition.

**3. The runtime never invents a `Ready` on recovery.** The next successful
`Observe` replaces `NotObserved` with the provider's answer. If a successful
`Observe` leaves it at `NotObserved`, the provider broke rule 1 on that path.
The runtime keeps the marker, since it can't know the true value. It reports
the breach with an Error log (naming the kind, namespace and name) and a
Warning event with reason `ReadyNotSetAfterObserve`
(`reconciler.ReasonReadyNotSetAfterObserve`, exported since v1.8.1). A
resource being deleted is never reported (since v1.8.1). The event goes through
the throttled recorder, like the runtime's other error events. `Synced` is
not affected.

What readers see while the resource can't be observed:

| Condition | Status | Reason | Message |
|---|---|---|---|
| `Ready` | `Unknown` | `NotObserved` | `The external resource could not be observed; see the Synced condition for the error.` |
| `Synced` | `False` | `ReconcileError` | the error, for example `observe failed: <the provider's error>` |

The `Ready` message never changes, so repeated failures with different errors
don't move `Ready`'s `lastTransitionTime`. The error text is only on `Synced`.
Match `NotObserved` by its reason, which is fixed. A reader that rolls `Ready`
up should treat it as "unknown", not as "not ready". core-provider's rollup
matches this reason (core-provider#310).

`reconcilertest.AssertObserveSetsReady(t, ctx, client, mg)` checks rule 1 in
a provider's own tests. It sets `Ready` to `NotObserved` on `mg`, calls
`Observe`, and fails the test if `Ready` is still `NotObserved` afterwards. It
also fails on a plain error, because then there is nothing to check. Call it
once per success path. [greeting-controller](../examples/greeting-controller/README.md)
does this in `main_test.go`.

## Errors become status that others can read

Every error a provider returns from `Connect`, `Observe`, `Create`, `Update`
or `Delete` is written, wrapped but otherwise verbatim, into the managed
resource's `Synced` condition message (`ReconcileError(err)` uses
`err.Error()`). It is also recorded in Warning events and logs. Anyone with
`get` on the resource can read the condition, and usually the events too. The
runtime cannot tell which part of an error is sensitive, so this is the
provider's responsibility:

- Never put credentials, tokens, keys, signed URLs or connection strings in
  a returned error.
- Don't embed raw response bodies from an external API. They can echo
  request data, internal hostnames or another tenant's details. Summarise
  instead: the operation, the status code, and a short vendor error code or
  message if the vendor documents it as safe.
- Log the full body at **Info**, capped (for example 2 KiB through an
  `io.LimitReader`), so the vendor's explanation reaches operators on a
  default install. The point is *who* can read it, not how much is logged: a
  condition is readable by anyone with `get` on the resource, a log by
  operators. Debug is the wrong place, because a provider that runs without
  debug logging by default would then keep the explanation from everyone.
  Logs can also be exported to the platform's log store, so its readers are
  part of the audience too: never log credentials at any level.
- Keep the error useful. A message that says what failed and what to check
  is what users need on `Synced`; the goal is to keep secrets out, not to
  make the message vague.

`Ready`'s message is fixed when the runtime sets `NotObserved`. That keeps
`lastTransitionTime` stable; it is not a confidentiality measure, and the
error text is still on `Synced`. (oasgen-provider#433 found a vendor's error
body exposed this way.)

## Supporting packages

| Package | Role |
|---|---|
| `apis/common/v1` | The shared condition model (`Condition`, `ConditionedStatus`, `Ready`/`Synced` types, `Available()`/`Creating()`/`NotObserved()`/`ReconcileError()` constructors) and reference selectors (`Reference`, `SecretKeySelector`, `ConfigMapKeySelector`, `EnvSelector`, `CredentialSelectors`) that provider CRDs embed. Deepcopy is generated (`make generate`). |
| `pkg/resource` | The `Managed`/`Object`/`Conditioned`/`Finalizer` interfaces, `ManagedKind`, `APIFinalizer`, error-ignoring helpers (`IgnoreNotFound`, `IgnoreAny`), secret/configmap readers, plus `pkg/resource/fake` mocks. |
| `pkg/meta` | The `krateo.io/*` annotation vocabulary and helpers: external-name, external-create tracking, pause, verbose, management policy (`default`, `observe-create-update`, `observe-delete`, `observe`) and deletion policy (`delete`, `orphan`). |
| `pkg/controller` | `Options`/`DefaultOptions` and `ForControllerRuntime()`, which wires the per-item exponential rate limiter and — when a `QueueWaitRecorder` is set — an instrumented workqueue (`queue_wait.go`) measuring queue depth, wait time, oldest-item age and work duration. |
| `pkg/logging` | The `Logger` interface with nop/logr/slog adapters, plus the OTel log model home: `NewOTelJSONHandler` (one JSON object per line — `timestamp`, SeverityText+SeverityNumber, `service.name`, trace/span ids) and `NewOTelHandler`, which tees records to the global OTel LoggerProvider via the otelslog bridge (OTLP export once the binary installs a provider). |
| `pkg/telemetry` | `Setup(ctx, log, Config)` builds an OTLP-HTTP metrics pipeline and returns the `Metrics` recorder (low-cardinality `provider_runtime.*` counters/histograms covering startup, reconcile, queue, external ops, finalizers, status updates). All `Metrics` methods are nil-safe. |
| `pkg/ratelimiter` | Global token-bucket and exponential limiters, `NewController()` (1s→60s per-item exponential), `LimitRESTConfig`, and a `Reconciler` wrapper that defers over-rate requests instead of dropping them. |
| `pkg/workqueue` | `NewExponentialTimedFailureRateLimiter` — per-item exponential backoff whose failure history expires after 2× max delay, so long-quiet items restart fast. |
| `pkg/context` | `CtxWithLogger`/`LoggerFromCtx` to carry a `logging.Logger` through a `context.Context`. |
| `pkg/errors` | A stdlib-backed drop-in for the archived pkg/errors surface (`Wrap`, `Wrapf`, `Cause`, …). |
| `pkg/reconciler/reconcilertest` | `AssertObserveSetsReady`, the conformance check for the Ready contract in a provider's own tests. |
| `pkg/test` | `cmp` options (`EquateErrors`, `EquateConditions`) and a full mock `client.Client` for provider unit tests. |

## How it composes with peers

Krateo's core-provider (and any other provider) imports this module and
implements the `ExternalClient` per managed kind. The logging and telemetry
packages are the shared home for the OTel conventions those binaries
previously duplicated; the OTLP LoggerProvider/exporter setup itself lives in
the consuming binary's `main`, next to its trace/metric setup — this library
depends only on the OTel APIs and bridge, never the log SDK.
