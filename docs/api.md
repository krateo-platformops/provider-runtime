---
type: API
title: provider-runtime — api
description: The exported Go API surface of provider-runtime, package by package, plus the OTLP metric instruments it emits.
resource: github.com/krateo-platformops/provider-runtime
tags: [library, go-api, reconciler, metrics]
timestamp: 2026-11-13T00:00:00Z
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
- Options: `WithTimeout`, `WithPollInterval`, `WithPollIntervalHook`, `WithPollJitterHook`, `WithCreationGracePeriod`, `WithCreateRecoveryGracePeriod`, `WithCreateRecoveryReobserveInterval`, `WithExternalConnecter`, `WithExternalConnectDisconnecter`, `WithCriticalAnnotationUpdater`, `WithFinalizer`, `WithLogger`, `WithMetrics`, `WithRecorder`, `WithThrottledRecorder`, `WithRateLimitGovernor`, and, unreleased, `WithTracerProvider` and `WithTraceContinueWindow`.
- Rate-limit governor (since v1.9.0): `RateLimitKeyer` — `RateLimitKey(mg resource.Managed) (string, error)`, optional on the connecter and the client, needed only for per-endpoint API limits: only calls of one that implements it are gated per endpoint. `RateLimitKeysProvider` (since v1.10.0) — `RateLimitKeys(mg resource.Managed) ([]string, error)`, the same for an object whose calls reach several rate-limited scopes: a call needs a slot in every key, all or nothing; it wins on a type that implements both. With saturation enabled, every gated call needs a local slot, keyer or not. `WithRateLimitGovernor(g *ratelimit.Governor)` gates `Connect` and every `ExternalClient` call through `g` (a `nil` `g` is a no-op). A deferred pass writes `Synced=False/ReconcileDeferred`, leaves `Ready` unchanged, records a Normal `ReconcileDeferred` event, and returns `RequeueAfter` with a nil error. A deferral that would write the object back unchanged skips the status update and only requeues.
- `MetricsRecorder` — the interface `WithMetrics` accepts (durations + failure counters for get/connect/observe/create/update/delete/finalizers/status, in-flight gauge; satisfied by `*telemetry.Metrics`).
- `CriticalAnnotationUpdater` / `RetryingCriticalAnnotationUpdater` / `NewRetryingCriticalAnnotationUpdater` — retry-on-conflict updates of the runtime's own annotations. Unreleased: `RetryingCriticalAnnotationUpdater` writes back only `krateo.io/external-name`, the `krateo.io/external-create-*` markers and `krateo.io/traceparent-continued`, onto a fresh read, so other writers' annotations are never reverted.
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

Annotation keys (`AnnotationKeyExternalName`, `…ExternalCreatePending/Succeeded/Failed`, `…ReconciliationPaused`, `…ConnectorVerbose`, `…ManagementPolicy`, `…DeletionPolicy`, and, unreleased, `…TraceparentContinued`), policy constants (`ManagementPolicyDefault/ObserveCreateUpdate/ObserveDelete/Observe`, `DeletionPolicyDelete/Orphan`), and helpers: finalizers (`AddFinalizer`, `RemoveFinalizer`, `FinalizerExists`), labels/annotations (`AddLabels`, `RemoveLabels`, `AddAnnotations`, `RemoveAnnotations`), lifecycle (`WasDeleted`, `WasCreated`), external-name and external-create accessors (since v1.18.0 the markers are written with `MarkerTimeLayout`, RFC3339Nano, and both layouts parse), `ExternalCreateIncomplete`, `ExternalCreateSucceededDuring`, `IsPaused`, `IsVerbose`, and policy predicates `IsActionAllowed`, `ShouldDelete`, `ShouldOnlyObserve`, `ShouldCreate`, `ShouldUpdate`.

## `pkg/controller`

`Options` / `DefaultOptions()` / `(Options).ForControllerRuntime()`, and `QueueWaitRecorder` (queue-wait telemetry interface consumed by the instrumented workqueue). For the rate-limit governor: `Options.RateLimit` (`RateLimitOptions`: `Enabled`, `RecoveryInterval`, `Saturation`, `Metrics`; the zero value is off), `Options.RateLimitGovernor`, `(*Options).SetupRateLimitGovernor(mgr RunnableAdder) error` (builds the governor, adds it to the manager; a no-op when nothing is enabled), and `RunnableAdder`.

## `pkg/ratelimit` (unreleased)

- `Signal` (`Key`, `RetryAfter`, `ResetAt`, `Remaining`, `Limit`, `HasQuota`, `Throttled`; same names and meaning as unstructured-runtime's) with `Deadline(now)`; `Classify(s, now) SignalKind` (`SignalThrottled`, `SignalAdvisory`, `SignalNone`).
- `Report(ctx, Signal)` (a no-op without a sink; drops a keyless signal), `Sink` (`Observe(Signal)`), `WithSink(ctx, Sink)`.
- `Governor`, `New(Options) (*Governor, error)`. Methods: `Acquire(ctx, key) (release func(), *Deferral)` (never blocks, no I/O; an empty key gates nothing; per key only), `SaturationEnabled()`, `RegisterGatedController()` (called by `controller.Options.ForControllerRuntime`), `Observe(Signal)`, `Capacity(key)`, `LocalCapacity()`, `Ceiling()`, `RegisterReconciler()`, `Snapshot()`, `Start(ctx)` / `NeedLeaderElection()` (a `manager.Runnable` running the saturation sampler).
- `Options` (`MaxConcurrentReconciles`, `IgnoreSignals`, `RecoveryInterval`, `Saturation`, `Logger`, `Metrics`), `SaturationOptions` (`Enabled` (CPU only), `ExperimentalMemoryWorkingSet` (experimental, off, not OOM protection), `CgroupDir`, `SamplePeriod` (default 1s), `StartupRampInterval` (default 2s), `CPUPressureThreshold` (default 50, provisional), `WorkingSetSoftRatio` and `WorkingSetHardRatio` (0.80 and 0.90, provisional, experimental input only), `CapacityModel` (off; the capacity model of [design-capacity-model](./design-capacity-model.md), built, not measured on a cluster; needs `Enabled`), `CPUTarget` (default 0.9), `MinCPULimit` (default 0.5 cores) and `ModelInterval` (default 10s)) and the `Default…` constants, and `CPUPressureWindow` (10s: the span CPU pressure is measured over, and the least time between two CPU cuts).
- `NewGatedQueue[T](g, name, rateLimiter) priorityqueue.PriorityQueue[T]`: a controller queue whose workers take a local slot before they pop and give it back in `Done`; `controller.Options.ForControllerRuntime` builds one per controller when saturation is on. Each queue is guaranteed an equal share of the local capacity (at least 1) and lends what it doesn't use: a freed slot goes first to a queue holding less than its share.
- `Deferral` (`Key`, `Reason`, `Until`, `RequeueAfter`, `Capacity`) with `Message()`; `DeferReason` (`DeferThrottled`, `DeferAtCapacity`) with `Cause()`. Removed: `Deferral.Local`, `Deferral.Cause`, `DeferSaturated` and `DeferRamping`, because the local capacity no longer defers a call.
- `MetricsRecorder` (satisfied by `*telemetry.Metrics`), `Snapshot` (its `SaturationEnabled` says whether `LocalCapacity` is enforced; metrics report the local capacity only then), `KeySnapshot`, `InputSnapshot`, `ControllerSnapshot` (per gated queue: `Controller`, `InFlight`, `Guaranteed`, `Waiting`, `Arrivals`, `Completions`), `CutSnapshot` (`Input`, `Cuts`), `CPUSnapshot` (`Limit`, `ModelCapacity`, `Used`, `Background`, `PerReconcile`, `Required`; a nil field has no value), in `Snapshot.Controllers`, `Snapshot.Cuts` and `Snapshot.CPU`, and the input names `InputCPUPressure` and `InputMemoryWorkingSet`. `memory.pressure` is not an input (see the design's §11).

## `pkg/ratelimit/httpsignal` (unreleased)

- `NewTransport(base http.RoundTripper) *Transport` — an `http.RoundTripper` that reports a `ratelimit.Signal` for each response carrying rate-limit headers, to the sink on the request's context, and passes the response through unchanged.
- `Parse(resp, key, now) (ratelimit.Signal, bool)`, `Resource(h http.Header) string` (for display only), `WithKey(ctx, key)` (override the key per request), `HostKey(u *url.URL) string` (the default key: the host, lower case).
- Unreleased: trace propagation. When the request's context holds an active span, `RoundTrip` sets `traceparent`, and `tracestate` when there is one, on a clone of the request. It never sets baggage, never overwrites an existing `traceparent`, and never uses the reconciler's inbound context, so with tracing off it sends nothing. `Transport.NoTracePropagation` turns it off. The `&Transport{}` literal gets it too.

## `pkg/tracing` (unreleased)

The API-only half of reconcile tracing ([design-tracing](./design-tracing.md)): it depends on the OTel API, never on the SDK or an exporter.

Unreleased (v1.14.2): the `krateo.io/traceparent` rule lives in plumbing's `tracecontext` (plumbing v1.44.0), which unstructured-runtime applies too. `Mode`, `When`, `Decision` and `Object` are type aliases of `tracecontext`'s types. `NoParent`, `Parent`, `Link`, `Never`, `BeforeFirstAction`, `Now`, the three `AttrObject…` keys and `SpanReconcile` are its constants. `Decide`, `Canonical`, `StartReconcile`, `StartActStep` and `StartStep` are thin wrappers. The Start functions keep their `TracerProvider` parameter and pass `tp.Tracer(ScopeName)`, or a nil Tracer when `tp` is nil. Every name, signature and behaviour below is unchanged.

- `SetTracerProvider(tp)` / `TracerProvider()` — the provider the reconcilers use; `telemetry.Setup` sets it with tracing on, and nil means off. A provider with its own SDK pipeline sets it too. `ScopeName` is the instrumentation scope.
- `Decide(annotations, remembered, firstSeen, now, window) Decision` — the parent-or-link rule of design §6.2, pure; it allocates nothing when the object has no `krateo.io/traceparent`. `Decision` holds `Mode` (`NoParent`, `Parent`, `Link`), `Remote` (the annotation's span context), `Marker` and `WriteMarker` (`Never`, `BeforeFirstAction`, `Now`). `Canonical(sc)` is a span context's traceparent as the marker records it.
- `StartReconcile(ctx, tp, obj Object, d Decision, start) (ctx, trace.Span)` — the `reconcile` root span: `SpanKindServer`, exactly `k8s.object.kind`, `k8s.object.name` and `k8s.object.namespace`. It's always a new root, never a child of `d.Remote` or of an in-process span (design §13, D8); in modes `Parent` and `Link` it carries one link to `d.Remote`, plus one to its act on the first acting pass (`StartActStep`). With `tp` nil it starts nothing and carries `d.Remote` for `Inject` only. `StartStep(ctx, tp, name)` opens a child (`SpanConnect`, `SpanObserve`, `SpanCreate`, `SpanUpdate`, `SpanDelete`).
- Unreleased (v1.14.1): `StartActStep(ctx, tp, obj Object, step, upstream) (ctx, trace.Span)` — the first act step for a new value: an Internal span whose parent is `upstream`, so it's in the upstream trace, with one link back to the root in `ctx` and `obj`'s three `k8s.object.*` attributes. The root gains a link to it when it's sampled. With `upstream` invalid it's `StartStep`; with `tp` nil it starts nothing.
- `Inject(ctx, obj)` / `InjectAnnotations(ctx, annotations)` — write `krateo.io/traceparent`, and `krateo.io/tracestate` or its removal, onto an object another Krateo controller reconciles. They overwrite, and they never write the marker. The rules for callers are in the package documentation and in [usage](./usage.md).
- `InjectHeader(ctx, h)` / `HasActiveSpan(ctx)` — the HTTP side: the active span only, never overwriting, never baggage.
- `RecordError(span, err)`, `Truncate(s)` — the exception event and Error status, with the text cut to `MaxErrorText` (4 KiB).
- Names: `EventRateLimitDeferred`, `EventReconcilePaused`, `EventConflict`, `EventMarkerFailed`; the `Attr…` keys; `Step` with `StepConnect`, `StepObserve`, `StepHandshake`, `StepFinalizer`, `StepCreatePending`, `StepCreate`, `StepLateInit`, `StepMarker`, listed by `Steps()`. Unreleased (v1.14.3): `EventReconcilePaused`, `EventConflict`, `EventMarkerFailed` and `AttrStep` are `tracecontext`'s constants (plumbing v1.45.0), with the same strings.
- `pkg/meta` gains `AnnotationKeyTraceparent` and `AnnotationKeyTracestate`, the same keys as unstructured-runtime's. With `AnnotationKeyTraceparentContinued`, they equal `tracecontext`'s three keys, and a test keeps them equal. They stay plain constants, so `pkg/meta` doesn't import OpenTelemetry.

Unreleased, in `pkg/reconciler`: with a TracerProvider (`tracing.TracerProvider()`, or `WithTracerProvider`), `Reconcile` opens the root span after its Get, start-stamped at the top of `Reconcile`, and binds its Logger to the span's context. It opens the `connect`, `observe`, `create`, `update` and `delete` children around the provider's calls. It records the events and the status of design §7. It writes `krateo.io/traceparent-continued` with an optimistic lock, just before the pass's first Create, Update or Delete, or when the window (`WithTraceContinueWindow`, default 10 minutes) closes, and requeues before acting on a conflict. From v1.14.1 (D8) every root is its own trace, linked to the object's `krateo.io/traceparent`; only the first act step for a new value is that value's child. With tracing off it starts nothing, writes nothing and keeps no memo.

## `pkg/scaletozero` (unreleased)

Called by an owner (core-provider, oasgen-provider) from its own reconcile to run a per-Kind controller Deployment at 0 replicas while the Kind has no instances. The helper is the mechanism, not the trigger: it watches nothing, so the owner must reconcile when the first instance appears. Design: [design-scale-to-zero](./design-scale-to-zero.md).

- `Exists(ctx, r client.Reader, gvk, sel labels.Selector) (bool, error)` — whether at least one instance matching `sel` exists, in any namespace, terminating ones included; metadata-only LIST with `Limit: 1`, stopping at the first match and following `Continue` past empty pages. `sel` is required (nil is an error; pass `labels.Everything()` for all). A failed LIST on any page, a missing CRD or an unserved version is an error, never false. `r` must be live (`mgr.GetAPIReader()`): a cache-backed reader makes it fail whenever the Kind is empty.
- `Decide(exists bool, lastSeen, now time.Time, holdDown time.Duration) (replicas int32, requeueAfter time.Duration)` — pure; 1 (running) while an instance exists, 0 (idle) once the Kind has been empty for the whole `holdDown` since `lastSeen`. A zero `lastSeen` starts the hold-down now; a future `lastSeen` stays running; `holdDown <= 0` returns running. `requeueAfter` is the rest of the hold-down, otherwise 0.
- `Reconcile(ctx, c client.Client, deploy types.NamespacedName, gvk, sel, lastSeen time.Time, holdDown time.Duration, active int32) (Result, error)` — two reads per call (a GET of the scale, then the `Exists` LIST) and a patch of `deployments/scale` only on change: 0 → `active` to wake, any non-zero count left alone while running, → 0 when idle. Safe on every poll. `active` must be >= 1. On any error nothing is patched, `LastSeen` comes back unchanged, `Reason` is `Unknown`, and `Replicas` is never 0 (the current replicas if read and non-zero, else `active`). Precondition: the caller writes the Deployment's `spec.replicas` nowhere else.
- `Result` (`Replicas`, `Reason`, `LastSeen`, `RequeueAfter`, `Exists`, `Patched`); `Reason` with the constants `ReasonActive` (`"Active"`), `ReasonIdleHoldDown` (`"IdleHoldDown"`), `ReasonIdle` (`"Idle"`), `ReasonUnknown` (`"Unknown"`); a test pins the literals.
- RBAC the owner needs: list on the watched resource cluster-wide; get and patch on `deployments/scale`.

## `pkg/logging`

`Logger` interface (`Info`, `Debug`, `Warn`, `Error`, `WithValues`, `WithName`); `NewNopLogger`, `NewLogrLogger`, `NewSlogLogger`; OTel log model: `NewOTelJSONHandler(level, w, attrs...)`, `NewOTelHandler(level, w, serviceName)` (tees JSON stream + otelslog bridge), `ServiceNameAttr(serviceName)`.

Unreleased (v1.13.0, additive): `ContextLogger` — `WithContext(ctx) Logger`, an optional interface, so a `Logger` written without it still compiles; `WithContext(l Logger, ctx) Logger` binds `l` to `ctx` when it is a `ContextLogger` and returns it unchanged otherwise. All three built-in Loggers implement it:

- `NewSlogLogger`: the bound Logger passes `ctx` with every record (`InfoContext`, `DebugContext`, `WarnContext`, `ErrorContext`), and the Loggers its `WithName` and `WithValues` return keep it. Binding again replaces the context.
- `NewNopLogger`: still a nop.
- `NewLogrLogger`: returned unchanged. logr has no path that carries a context to its sink: `logr.LogSink`'s `Info` and `Error` take none, and logr's slog adapter (`logr.FromSlogHandler`) calls the handler with `context.Background()`. A logr-backed Logger never stamps trace ids.

### Log-to-trace correlation

A record correlates with a trace when three things hold: the Logger is slog-backed (`NewSlogLogger`) and bound with `WithContext`; the bound `ctx` holds a valid span context (one a tracer started, or one `trace.ContextWithSpanContext` put there); and the slog handler reads it. `NewOTelJSONHandler`, and so `NewOTelHandler`'s JSON stream, adds `trace_id` and `span_id`. `NewOTelHandler`'s otelslog bridge hands `ctx` to the global LoggerProvider, whose SDK sets the OTLP record's trace context, once the binary's main has installed one with `go.opentelemetry.io/otel/log/global.SetLoggerProvider`; this module installs none. A plain `slog.NewJSONHandler` ignores the context. Without a span, nothing is stamped.

`Reconciler.Reconcile` binds its per-reconcile Logger (the one built from `WithLogger` with the `request` value) to the `ctx` it is given, so its own records correlate whenever that `ctx` holds a span. It binds the incoming `ctx`, not the timeout one it derives. Unreleased: with tracing on, `Reconcile` starts the reconcile root span itself and binds the Logger to it (see `pkg/tracing` above). With tracing off, controller-runtime (v0.25) starts no span either, so that `ctx` holds one only if the provider wraps the `Reconciler` in a `reconcile.Reconciler` that starts it. The provider's `Connect` and `ExternalClient` methods get a `ctx` but not this Logger: bind their own with `logging.WithContext(log, ctx)`.

## `pkg/telemetry`

`Config`, `Metrics`, `Setup(ctx, log, cfg) (*Metrics, shutdown func(context.Context) error, error)`. All recording methods are nil-safe on a nil `*Metrics`.

Unreleased: tracing. `Config` gains `TracingEnabled`, `ServiceVersion` and `DeploymentName`, and `ConfigFromEnv() (Config, error)` reads `OTEL_ENABLED`, `OTEL_TRACING_ENABLED`, `OTEL_EXPORT_INTERVAL`, `DEPLOYMENT_NAME` and `SERVICE_VERSION` (the `Env…` constants).
- **With `TracingEnabled`,** `Setup` builds an `otlptracehttp` exporter from the standard `OTEL_*` environment, with a batcher, and sets the TracerProvider as the OTel global and as `tracing.SetTracerProvider`. With tracing on, the traces protocol must be unset or `http/protobuf`: `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` when it's set, otherwise `OTEL_EXPORTER_OTLP_PROTOCOL` (OTel's precedence).
- **On every call,** `Setup` sets the global propagator to W3C TraceContext and Baggage.
- **Shared resource.** Metrics and traces share one resource: `service.name`, `service.version`, `k8s.deployment.name` and `service.instance.id`, with the environment winning for each.
- **Shutdown** stops the TracerProvider, which flushes, and then the MeterProvider. Instruments (meter `github.com/krateo-platformops/provider-runtime`):

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
| `provider_runtime.ratelimit.local.{in_flight,guaranteed,waiting}` | observable gauge |
| `provider_runtime.ratelimit.local.{arrivals,cuts}` | observable counter |
| `provider_runtime.ratelimit.local.model_capacity` | observable gauge |
| `provider_runtime.ratelimit.cpu.{limit,used,background,per_reconcile,required}` | observable gauge |

## `pkg/ratelimiter`, `pkg/workqueue`, `pkg/context`, `pkg/errors`, `pkg/test`

- `ratelimiter`: `NewGlobal(rps)`, `NewGlobalExponential(base, max)`, `NewController()`, `LimitRESTConfig(cfg, rps)`, `New(name, reconciler, limiter) *Reconciler` (defer-over-rate wrapper).
- `workqueue`: `NewExponentialTimedFailureRateLimiter[T](base, max)`, `TypedItemExponentialTimedFailureRateLimiter`, `FailureRequest` — exponential per-item backoff whose failure history expires after 2× max delay.
- `context`: `CtxWithLogger`, `LoggerFromCtx`.
- `errors`: stdlib-backed `New`, `Is`, `As`, `Unwrap`, `Errorf`, `WithMessage(f)`, `Wrap(f)`, `Cause`.
- `test`: `EquateErrors()`, `EquateConditions()` cmp options and a fully mockable `client.Client` (`MockGetFn`, `MockUpdateFn`, sub-resource mocks, …).
