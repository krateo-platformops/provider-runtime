---
type: API
title: provider-runtime — api
description: The exported Go API surface of provider-runtime, package by package, plus the OTLP metric instruments it emits.
resource: github.com/krateo-platformops/provider-runtime
tags: [library, go-api, reconciler, metrics]
timestamp: 2026-11-07T00:00:00Z
---

# API

The contract is the exported Go surface of the module
`github.com/krateo-platformops/provider-runtime` (browse on
[pkg.go.dev](https://pkg.go.dev/github.com/krateo-platformops/provider-runtime)).
This library ships no CRDs, HTTP endpoints or binaries. The load-bearing
types, by package:

## `pkg/reconciler`

- `NewReconciler(mgr manager.Manager, of resource.ManagedKind, o ...ReconcilerOption) *Reconciler` — the generic managed-resource reconciler; `Reconcile(ctx, req)` implements `reconcile.Reconciler`.
- `ExternalConnecter` — `Connect(ctx, mg resource.Managed) (ExternalClient, error)`; also `ExternalDisconnecter`, `ExternalConnectDisconnecter`, `NopDisconnecter`/`NewNopDisconnecter`, and `…Fn`/`…Fns` adapters.
- `ExternalClient` — `Observe(ctx, mg) (ExternalObservation, error)`, `Create(ctx, mg) error`, `Update(ctx, mg) error`, `Delete(ctx, mg) error`; `ExternalClientFns`, `NopConnecter`, `NopClient`.
- `ExternalObservation` — `ResourceExists`, `ResourceUpToDate`, `ResourceLateInitialized`, `Diff`.
- Ready contract (since v1.8.0): `ReadyAssessed(err error) error` marks an error returned after `Observe` set `Ready` itself, so the runtime leaves `Ready` alone (nil-safe; keeps the message; `errors.Is`/`As`/`Unwrap` see through it); `IsReadyAssessed(err error) bool`; `ReasonReadyNotSetAfterObserve` (`event.Reason`, since v1.8.1), the reason of the Warning event the runtime records when a successful `Observe` leaves `Ready` at `NotObserved` (never for a resource being deleted). See [overview](./overview.md#the-ready-contract-since-v180).
- Options: `WithTimeout`, `WithPollInterval`, `WithPollIntervalHook`, `WithPollJitterHook`, `WithCreationGracePeriod`, `WithCreateRecoveryGracePeriod`, `WithCreateRecoveryReobserveInterval`, `WithExternalConnecter`, `WithExternalConnectDisconnecter`, `WithCriticalAnnotationUpdater`, `WithFinalizer`, `WithLogger`, `WithMetrics`, `WithRecorder`, `WithThrottledRecorder`, `WithRateLimitGovernor`.
- Rate-limit governor (since v1.9.0): `RateLimitKeyer` — `RateLimitKey(mg resource.Managed) (string, error)`, optional on the connecter and the client, needed only for per-endpoint API limits: only calls of one that implements it are gated per endpoint. `RateLimitKeysProvider` (since v1.10.0) — `RateLimitKeys(mg resource.Managed) ([]string, error)`, the same for an object whose calls reach several rate-limited scopes: a call needs a slot in every key, all or nothing; it wins on a type that implements both. With saturation enabled, every gated call needs a local slot, keyer or not. `WithRateLimitGovernor(g *ratelimit.Governor)` gates `Connect` and every `ExternalClient` call through `g` (a `nil` `g` is a no-op). A deferred pass writes `Synced=False/ReconcileDeferred`, leaves `Ready` unchanged, records a Normal `ReconcileDeferred` event, and returns `RequeueAfter` with a nil error. A deferral that would write the object back unchanged skips the status update and only requeues.
- `MetricsRecorder` — the interface `WithMetrics` accepts (durations + failure counters for get/connect/observe/create/update/delete/finalizers/status, in-flight gauge; satisfied by `*telemetry.Metrics`).
- `CriticalAnnotationUpdater` / `RetryingCriticalAnnotationUpdater` / `NewRetryingCriticalAnnotationUpdater` — retry-on-conflict updates of the external-create tracking annotations.
- `ControllerName(kind string) string`, `FinalizerName = "finalizer.managedresource.krateo.io"`.

## `pkg/reconciler/reconcilertest`

- `AssertObserveSetsReady(t testing.TB, ctx context.Context, c reconciler.ExternalClient, mg resource.Managed) (reconciler.ExternalObservation, error)` — sets `Ready=NotObserved` on `mg`, calls `c.Observe`, and fails `t` if `Ready` is still `NotObserved`, or if `Observe` returned an error not wrapped with `ReadyAssessed`. Call once per success path.
- `AssertReportsRateLimits(t *testing.T, call RateLimitedCall)` — feeds `call` a recorded GitHub 429, a recorded Azure DevOps advisory 200 and a healthy GitHub 200, and fails unless they are reported as throttled with a deadline, advisory, and neither. The parts: `AssertReportsThrottle`, `AssertReportsAdvisory`, `AssertSilentWhenHealthy` (each `(t testing.TB, recordedName string, call RateLimitedCall) []ratelimit.Signal`). `RateLimitedCall` is `func(ctx context.Context, baseURL string) error`.
- Recorded responses: `RecordedResponse(name string, now time.Time) (*http.Response, error)` (times moved to `now`), `RecordedResponseNames()`, and the names `GitHub200Healthy`, `GitHub403PrimaryExhausted`, `GitHub403SecondaryRetryAfter`, `GitHub429RetryAfter`, `AzureDevOps200Advisory`, `AzureDevOps429Blocked`, `Service503RetryAfterDate`. Only `GitHub200Healthy` is a verbatim capture; the others follow the vendors' documented headers on a recorded envelope.

## `apis/common/v1`

- `Condition` (`Type`, `Status`, `LastTransitionTime`, `Reason`, `Message`), `ConditionType` (`TypeReady`, `TypeSynced`), `ConditionReason` (`Available`, `Unavailable`, `Creating`, `Deleting`, `NotObserved`, `ReconcileSuccess`, `ReconcileError`, `ReconcilePaused`, `ReconcileDeferred`).
- `ConditionedStatus` with `GetCondition`/`SetConditions`/`Equal`; constructors `Creating()`, `Deleting()`, `Available()`, `Unavailable()`, `NotObserved()` (`Ready` `Unknown`, set by the runtime when it can't observe; since v1.8.0), `ReconcileSuccess()`, `ReconcileError(err)`, `ReconcilePaused()`, `ReconcileDeferred(msg)` (`Synced=False`; unreleased).
- `ReasonReconcileDeferred` (`"ReconcileDeferred"`) is a consumer contract shared with unstructured-runtime's `condition.ReasonReconcileDeferred`; a test pins the literal.
- Selector types CRDs embed: `Reference`, `EnvSelector`, `SecretKeySelector`, `ConfigMapKeySelector`, `CredentialSelectors`.

## `pkg/resource`

- Interfaces: `Object`, `Managed` (Object + Conditioned), `ManagedList`, `Conditioned`, `Finalizer`.
- `ManagedKind` (a `schema.GroupVersionKind`), `MustCreateObject`.
- `APIFinalizer`/`NewAPIFinalizer`, `NewNopFinalizer`, `FinalizerFns`.
- Error helpers: `Ignore`, `IgnoreAny`, `IgnoreNotFound`, `IsAPIError`, `IsAPIErrorWrapped`, `IsMissingReference`.
- Credential readers: `GetSecret(ctx, kube, *SecretKeySelector)`, `GetConfigMapValue(ctx, kube, *ConfigMapKeySelector)`.
- `Conditions` (in `conditions.go`): `UpsertCondition`, `UpsertConditionMessage`, `JoinConditions`, `RemoveCondition`.
- Reference plumbing: `ReferenceStatus`/`ReferenceStatusType`, `CanReference`, `AttributeReferencer`.
- `pkg/resource/fake` — mock `Managed` implementations for tests.

## `pkg/meta`

Annotation keys (`AnnotationKeyExternalName`, `…ExternalCreatePending/Succeeded/Failed`, `…ReconciliationPaused`, `…ConnectorVerbose`, `…ManagementPolicy`, `…DeletionPolicy`), policy constants (`ManagementPolicyDefault/ObserveCreateUpdate/ObserveDelete/Observe`, `DeletionPolicyDelete/Orphan`), and helpers: finalizers (`AddFinalizer`, `RemoveFinalizer`, `FinalizerExists`), labels/annotations (`AddLabels`, `RemoveLabels`, `AddAnnotations`, `RemoveAnnotations`), lifecycle (`WasDeleted`, `WasCreated`), external-name and external-create accessors, `ExternalCreateIncomplete`, `ExternalCreateSucceededDuring`, `IsPaused`, `IsVerbose`, and policy predicates `IsActionAllowed`, `ShouldDelete`, `ShouldOnlyObserve`, `ShouldCreate`, `ShouldUpdate`.

## `pkg/controller`

`Options` / `DefaultOptions()` / `(Options).ForControllerRuntime()`, and `QueueWaitRecorder` (queue-wait telemetry interface consumed by the instrumented workqueue). For the rate-limit governor: `Options.RateLimit` (`RateLimitOptions`: `Enabled`, `RecoveryInterval`, `Saturation`, `Metrics`; the zero value is off), `Options.RateLimitGovernor`, `(*Options).SetupRateLimitGovernor(mgr RunnableAdder) error` (builds the governor, adds it to the manager; a no-op when nothing is enabled), and `RunnableAdder`.

## `pkg/ratelimit` (unreleased)

- `Signal` (`Key`, `RetryAfter`, `ResetAt`, `Remaining`, `Limit`, `HasQuota`, `Throttled`; same names and meaning as unstructured-runtime's) with `Deadline(now)`; `Classify(s, now) SignalKind` (`SignalThrottled`, `SignalAdvisory`, `SignalNone`).
- `Report(ctx, Signal)` (a no-op without a sink; drops a keyless signal), `Sink` (`Observe(Signal)`), `WithSink(ctx, Sink)`.
- `Governor`, `New(Options) (*Governor, error)`. Methods: `Acquire(ctx, key) (release func(), *Deferral)` (never blocks, no I/O; an empty key gates nothing; per key only), `SaturationEnabled()`, `RegisterGatedController()` (called by `controller.Options.ForControllerRuntime`), `Observe(Signal)`, `Capacity(key)`, `LocalCapacity()`, `Ceiling()`, `RegisterReconciler()`, `Snapshot()`, `Start(ctx)` / `NeedLeaderElection()` (a `manager.Runnable` running the saturation sampler).
- `Options` (`MaxConcurrentReconciles`, `IgnoreSignals`, `RecoveryInterval`, `Saturation`, `Logger`, `Metrics`), `SaturationOptions` (`Enabled` (CPU only), `ExperimentalMemoryWorkingSet` (experimental, off, not OOM protection), `CgroupDir`, `SamplePeriod` (default 1s), `StartupRampInterval` (default 2s), `CPUPressureThreshold` (default 50, provisional), `WorkingSetSoftRatio` and `WorkingSetHardRatio` (0.80 and 0.90, provisional, experimental input only)) and the `Default…` constants.
- `NewGatedQueue[T](g, name, rateLimiter) priorityqueue.PriorityQueue[T]`: a controller queue whose workers take a local slot before they pop and give it back in `Done`; `controller.Options.ForControllerRuntime` builds one per controller when saturation is on.
- `Deferral` (`Key`, `Reason`, `Until`, `RequeueAfter`, `Capacity`) with `Message()`; `DeferReason` (`DeferThrottled`, `DeferAtCapacity`) with `Cause()`. Removed: `Deferral.Local`, `Deferral.Cause`, `DeferSaturated` and `DeferRamping`, because the local capacity no longer defers a call.
- `MetricsRecorder` (satisfied by `*telemetry.Metrics`), `Snapshot` (its `SaturationEnabled` says whether `LocalCapacity` is enforced; metrics report the local capacity only then), `KeySnapshot`, `InputSnapshot`, and the input names `InputCPUPressure` and `InputMemoryWorkingSet`. `memory.pressure` is not an input (see the design's §11).

## `pkg/ratelimit/httpsignal` (unreleased)

- `NewTransport(base http.RoundTripper) *Transport` — an `http.RoundTripper` that reports a `ratelimit.Signal` for each response carrying rate-limit headers, to the sink on the request's context, and passes the response through unchanged.
- `Parse(resp, key, now) (ratelimit.Signal, bool)`, `Resource(h http.Header) string` (for display only), `WithKey(ctx, key)` (override the key per request), `HostKey(u *url.URL) string` (the default key: the host, lower case).

## `pkg/logging`

`Logger` interface (`Info`, `Debug`, `Warn`, `Error`, `WithValues`, `WithName`); `NewNopLogger`, `NewLogrLogger`, `NewSlogLogger`; OTel log model: `NewOTelJSONHandler(level, w, attrs...)`, `NewOTelHandler(level, w, serviceName)` (tees JSON stream + otelslog bridge), `ServiceNameAttr(serviceName)`.

## `pkg/telemetry`

`Config`, `Metrics`, `Setup(ctx, log, cfg) (*Metrics, shutdown func(context.Context) error, error)`. All recording methods are nil-safe on a nil `*Metrics`. Instruments (meter `github.com/krateo-platformops/provider-runtime`):

| Instrument | Kind |
|---|---|
| `provider_runtime.startup.{success,failure}` | counter |
| `provider_runtime.reconcile.duration_seconds` | histogram |
| `provider_runtime.reconcile.{success,failure}` | counter |
| `provider_runtime.reconcile.requeue.{after,immediate,error}` | counter |
| `provider_runtime.reconcile.in_flight` | observable gauge |
| `provider_runtime.reconcile.get.{duration_seconds,failure}` | histogram / counter |
| `provider_runtime.reconcile.queue.{depth,requeues}` | up-down counter / counter |
| `provider_runtime.reconcile.queue.wait.duration_seconds`, `…queue.work.duration_seconds`, `…queue.oldest_item_age_seconds` | histogram |
| `provider_runtime.external.{connect,observe,create,update,delete}.{duration_seconds,failure}` | histogram / counter |
| `provider_runtime.finalizer.{add,remove}.{duration_seconds,failure}` | histogram / counter |
| `provider_runtime.status.update.{duration_seconds,failure}` | histogram / counter |
| `provider_runtime.ratelimit.{deferrals,signals}` | counter |
| `provider_runtime.ratelimit.{capacity,seconds_to_reset,local.capacity}` | observable gauge |
| `provider_runtime.ratelimit.saturation.{input,input.enabled}` | observable gauge |

## `pkg/ratelimiter`, `pkg/workqueue`, `pkg/context`, `pkg/errors`, `pkg/test`

- `ratelimiter`: `NewGlobal(rps)`, `NewGlobalExponential(base, max)`, `NewController()`, `LimitRESTConfig(cfg, rps)`, `New(name, reconciler, limiter) *Reconciler` (defer-over-rate wrapper).
- `workqueue`: `NewExponentialTimedFailureRateLimiter[T](base, max)`, `TypedItemExponentialTimedFailureRateLimiter`, `FailureRequest` — exponential per-item backoff whose failure history expires after 2× max delay.
- `context`: `CtxWithLogger`, `LoggerFromCtx`.
- `errors`: stdlib-backed `New`, `Is`, `As`, `Unwrap`, `Errorf`, `WithMessage(f)`, `Wrap(f)`, `Cause`.
- `test`: `EquateErrors()`, `EquateConditions()` cmp options and a fully mockable `client.Client` (`MockGetFn`, `MockUpdateFn`, sub-resource mocks, …).
