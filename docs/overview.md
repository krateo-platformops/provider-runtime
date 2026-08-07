---
type: Architecture
title: provider-runtime — overview
description: Package design and the managed-resource reconcile flow provider-runtime drives for every Krateo provider.
resource: github.com/krateo-platformops/provider-runtime
tags: [library, controller-runtime, reconciler, architecture]
timestamp: 2026-08-07T00:00:00Z
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

A guard against half-completed creates: before calling `Create` the reconciler
stamps `krateo.io/external-create-pending` through a
`CriticalAnnotationUpdater` (retrying variant in `pkg/reconciler/api.go`), and
records success/failure in the paired annotations, refusing to proceed while
the outcome is unknowable (`ExternalCreateIncomplete`).

## Supporting packages

| Package | Role |
|---|---|
| `apis/common/v1` | The shared condition model (`Condition`, `ConditionedStatus`, `Ready`/`Synced` types, `Available()`/`Creating()`/`ReconcileError()` constructors) and reference selectors (`Reference`, `SecretKeySelector`, `ConfigMapKeySelector`, `EnvSelector`, `CredentialSelectors`) that provider CRDs embed. Deepcopy is generated (`make generate`). |
| `pkg/resource` | The `Managed`/`Object`/`Conditioned`/`Finalizer` interfaces, `ManagedKind`, `APIFinalizer`, error-ignoring helpers (`IgnoreNotFound`, `IgnoreAny`), secret/configmap readers, plus `pkg/resource/fake` mocks. |
| `pkg/meta` | The `krateo.io/*` annotation vocabulary and helpers: external-name, external-create tracking, pause, verbose, management policy (`default`, `observe-create-update`, `observe-delete`, `observe`) and deletion policy (`delete`, `orphan`). |
| `pkg/controller` | `Options`/`DefaultOptions` and `ForControllerRuntime()`, which wires the per-item exponential rate limiter and — when a `QueueWaitRecorder` is set — an instrumented workqueue (`queue_wait.go`) measuring queue depth, wait time, oldest-item age and work duration. |
| `pkg/logging` | The `Logger` interface with nop/logr/slog adapters, plus the OTel log model home: `NewOTelJSONHandler` (one JSON object per line — `timestamp`, SeverityText+SeverityNumber, `service.name`, trace/span ids) and `NewOTelHandler`, which tees records to the global OTel LoggerProvider via the otelslog bridge (OTLP export once the binary installs a provider). |
| `pkg/telemetry` | `Setup(ctx, log, Config)` builds an OTLP-HTTP metrics pipeline and returns the `Metrics` recorder (low-cardinality `provider_runtime.*` counters/histograms covering startup, reconcile, queue, external ops, finalizers, status updates). All `Metrics` methods are nil-safe. |
| `pkg/ratelimiter` | Global token-bucket and exponential limiters, `NewController()` (1s→60s per-item exponential), `LimitRESTConfig`, and a `Reconciler` wrapper that defers over-rate requests instead of dropping them. |
| `pkg/workqueue` | `NewExponentialTimedFailureRateLimiter` — per-item exponential backoff whose failure history expires after 2× max delay, so long-quiet items restart fast. |
| `pkg/context` | `CtxWithLogger`/`LoggerFromCtx` to carry a `logging.Logger` through a `context.Context`. |
| `pkg/errors` | A stdlib-backed drop-in for the archived pkg/errors surface (`Wrap`, `Wrapf`, `Cause`, …). |
| `pkg/test` | `cmp` options (`EquateErrors`, `EquateConditions`) and a full mock `client.Client` for provider unit tests. |

## How it composes with peers

Krateo's core-provider (and any other provider) imports this module and
implements the `ExternalClient` per managed kind. The logging and telemetry
packages are the shared home for the OTel conventions those binaries
previously duplicated; the OTLP LoggerProvider/exporter setup itself lives in
the consuming binary's `main`, next to its trace/metric setup — this library
depends only on the OTel APIs and bridge, never the log SDK.
