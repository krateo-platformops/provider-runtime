---
type: Configuration
title: provider-runtime — configuration
description: Everything provider-runtime reads — build tags, environment consumed via the OTel SDK, option structs and their defaults, and the annotation switches honored at runtime.
resource: github.com/krateo-platformops/provider-runtime
tags: [library, configuration, otel, annotations]
timestamp: 2026-08-07T00:00:00Z
---

# Configuration

provider-runtime is a library: it has no config files, no flags and no values.
Its configuration surface is (a) one build tag, (b) the environment the OTel
SDK reads inside `telemetry.Setup`, (c) option structs with defaults, and
(d) the `krateo.io/*` annotations it honors on managed resources at runtime.

## Build tags

| Tag | Where | Effect |
|---|---|---|
| `generate` | `apis/apis.go` | Guards the `go:generate` directive that runs `controller-gen` to regenerate `apis/common/v1/zz_generated.deepcopy.go` (`make generate`). Never needed by consumers. |

## Environment (read indirectly, only when metrics are enabled)

The library itself reads no environment variables. `telemetry.Setup` with
`Config.Enabled=true` constructs `otlpmetrichttp.New(ctx)` and
`resource.Default()`, which honor the standard OTel SDK environment:

| Variable | Effect |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | Where the OTLP-HTTP metric exporter sends (default `https://localhost:4318`). |
| `OTEL_EXPORTER_OTLP_HEADERS`, `_TIMEOUT`, `_COMPRESSION`, `_CERTIFICATE`, … | Standard exporter tuning, per the OTel spec. |
| `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES` | The exported resource attributes, via `resource.Default()`. |

> Honest note: `telemetry.Config.ServiceName` is validated and defaulted to
> `provider-runtime` (`pkg/telemetry/metrics.go:89-93`) but is **not applied**
> to the exported OTel resource — the resource is
> `resource.Merge(resource.Default(), resource.NewSchemaless())`, so the
> effective `service.name` comes from `OTEL_SERVICE_NAME` (or the SDK default
> `unknown_service:<binary>`). Set the env var, not just the struct field.

## `telemetry.Config`

| Field | Default | Effect |
|---|---|---|
| `Enabled` | `false` | When false, `Setup` returns a nil `Metrics` handle and a no-op shutdown — every `Metrics` method is nil-safe, so wiring it unconditionally is fine. |
| `ServiceName` | `provider-runtime` | See the honest note above — currently without effect on the exported resource. |
| `ExportInterval` | `30s` | Periodic-reader export interval for the OTLP metrics pipeline. |

## `controller.Options` (defaults from `DefaultOptions()`)

| Field | Default | Effect |
|---|---|---|
| `Logger` | nop logger | Logger the controllers use. |
| `GlobalRateLimiter` | token bucket, 1 rps | Cross-controller reconcile rate cap (use with `ratelimiter.New`). |
| `PollInterval` | `1m` | Speculative poll interval controllers should pass to the reconciler. |
| `MaxConcurrentReconciles` | `1` | Per-controller worker count. |
| `UsePriorityQueue` | `nil` (→ `true`) | Whether the instrumented queue uses controller-runtime's priority queue; only consulted when `QueueWaitRecorder` is set. |
| `QueueWaitRecorder` | `nil` | When set (e.g. `*telemetry.Metrics`), `ForControllerRuntime()` swaps in the wait-instrumented workqueue (depth, wait duration, oldest-item age, work duration, requeues). |

`ForControllerRuntime()` always sets a per-item exponential rate limiter of
1s base / 60s cap.

## `reconciler.NewReconciler` defaults (overridable via options)

| Knob | Default | Option |
|---|---|---|
| External-call timeout | `1m` (+30s reconcile grace) | `WithTimeout` |
| Poll interval | `1m` | `WithPollInterval`, `WithPollIntervalHook`, `WithPollJitterHook` |
| Creation grace period | `30s` | `WithCreationGracePeriod` |
| Finalizer | `finalizer.managedresource.krateo.io` (`APIFinalizer`) | `WithFinalizer` |
| Logger / events / metrics | nop logger, nop recorder, no metrics | `WithLogger`, `WithRecorder`, `WithThrottledRecorder`, `WithMetrics` |
| Critical-annotation updater | retrying updater | `WithCriticalAnnotationUpdater` |
| External connecter | nop (must be set) | `WithExternalConnecter`, `WithExternalConnectDisconnecter` |

## Runtime switches: annotations on managed resources

Set by users/operators on each managed CR, honored by the reconcile loop
(`pkg/meta`):

| Annotation | Values | Effect |
|---|---|---|
| `krateo.io/paused` | `"true"` | Skips reconciliation and sets the `ReconcilePaused` condition. |
| `krateo.io/management-policy` | `default` \| `observe-create-update` \| `observe-delete` \| `observe` | Restricts which external actions the provider may take. |
| `krateo.io/deletion-policy` | `delete` (default) \| `orphan` | Whether external-resource deletion follows CR deletion. |
| `krateo.io/connector-verbose` | `"true"` | Signals the external client to enable verbose behavior (`meta.IsVerbose`). |
| `krateo.io/external-name` | string | The resource's identity in the external system; set by `Create` implementations, readable via `meta.GetExternalName`. |
| `krateo.io/external-create-pending` / `-succeeded` / `-failed` | RFC3339 | Managed by the reconciler's create-tracking guard — not for hand-editing. |
