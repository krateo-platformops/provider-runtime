---
type: API
title: provider-runtime — api
description: The exported Go API surface of provider-runtime, package by package, plus the OTLP metric instruments it emits.
resource: github.com/krateo-platformops/provider-runtime
tags: [library, go-api, reconciler, metrics]
timestamp: 2026-08-07T00:00:00Z
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
- Options: `WithTimeout`, `WithPollInterval`, `WithPollIntervalHook`, `WithPollJitterHook`, `WithCreationGracePeriod`, `WithExternalConnecter`, `WithExternalConnectDisconnecter`, `WithCriticalAnnotationUpdater`, `WithFinalizer`, `WithLogger`, `WithMetrics`, `WithRecorder`, `WithThrottledRecorder`.
- `MetricsRecorder` — the interface `WithMetrics` accepts (durations + failure counters for get/connect/observe/create/update/delete/finalizers/status, in-flight gauge; satisfied by `*telemetry.Metrics`).
- `CriticalAnnotationUpdater` / `RetryingCriticalAnnotationUpdater` / `NewRetryingCriticalAnnotationUpdater` — retry-on-conflict updates of the external-create tracking annotations.
- `ControllerName(kind string) string`, `FinalizerName = "finalizer.managedresource.krateo.io"`.

## `apis/common/v1`

- `Condition` (`Type`, `Status`, `LastTransitionTime`, `Reason`, `Message`), `ConditionType` (`TypeReady`, `TypeSynced`), `ConditionReason` (`Available`, `Unavailable`, `Creating`, `Deleting`, `ReconcileSuccess`, `ReconcileError`, `ReconcilePaused`).
- `ConditionedStatus` with `GetCondition`/`SetConditions`/`Equal`; constructors `Creating()`, `Deleting()`, `Available()`, `Unavailable()`, `ReconcileSuccess()`, `ReconcileError(err)`, `ReconcilePaused()`.
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

`Options` / `DefaultOptions()` / `(Options).ForControllerRuntime()`, and `QueueWaitRecorder` (queue-wait telemetry interface consumed by the instrumented workqueue).

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

## `pkg/ratelimiter`, `pkg/workqueue`, `pkg/context`, `pkg/errors`, `pkg/test`

- `ratelimiter`: `NewGlobal(rps)`, `NewGlobalExponential(base, max)`, `NewController()`, `LimitRESTConfig(cfg, rps)`, `New(name, reconciler, limiter) *Reconciler` (defer-over-rate wrapper).
- `workqueue`: `NewExponentialTimedFailureRateLimiter[T](base, max)`, `TypedItemExponentialTimedFailureRateLimiter`, `FailureRequest` — exponential per-item backoff whose failure history expires after 2× max delay.
- `context`: `CtxWithLogger`, `LoggerFromCtx`.
- `errors`: stdlib-backed `New`, `Is`, `As`, `Unwrap`, `Errorf`, `WithMessage(f)`, `Wrap(f)`, `Cause`.
- `test`: `EquateErrors()`, `EquateConditions()` cmp options and a fully mockable `client.Client` (`MockGetFn`, `MockUpdateFn`, sub-resource mocks, …).
