---
type: Configuration
title: provider-runtime — configuration
description: Everything provider-runtime reads — build tags, environment consumed via the OTel SDK, option structs and their defaults (including the rate-limit governor), and the annotation switches honored at runtime.
resource: github.com/krateo-platformops/provider-runtime
tags: [library, configuration, otel, annotations]
timestamp: 2026-11-05T00:00:00Z
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
| `RateLimit` | zero value (off) | Configures the rate-limit governor. See [Rate-limit governor](#rate-limit-governor-controlleroptionsratelimit). |
| `RateLimitGovernor` | `nil` | The governor `SetupRateLimitGovernor` built, or `nil` when `RateLimit` enables nothing. Pass it to `reconciler.WithRateLimitGovernor`. `ForControllerRuntime()` does not read it. |

`ForControllerRuntime()` always sets a per-item exponential rate limiter of
1s base / 60s cap.

## `reconciler.NewReconciler` defaults (overridable via options)

| Knob | Default | Option |
|---|---|---|
| External-call timeout | `1m` (+30s reconcile grace) | `WithTimeout` |
| Poll interval | `1m` | `WithPollInterval`, `WithPollIntervalHook`, `WithPollJitterHook` |
| Creation grace period (wait for a create that **succeeded** to become observable) | `30s` | `WithCreationGracePeriod` |
| Create recovery grace period (how long an **unresolved** create keeps refusing before it is retried) | `2m` | `WithCreateRecoveryGracePeriod` |
| Create recovery re-observe interval (inside that window) | `15s` | `WithCreateRecoveryReobserveInterval` |
| Finalizer | `finalizer.managedresource.krateo.io` (`APIFinalizer`) | `WithFinalizer` |
| Logger / events / metrics | nop logger, nop recorder, no metrics | `WithLogger`, `WithRecorder`, `WithThrottledRecorder`, `WithMetrics` |
| Critical-annotation updater | retrying updater | `WithCriticalAnnotationUpdater` |
| External connecter | nop (must be set) | `WithExternalConnecter`, `WithExternalConnectDisconnecter` |
| Rate-limit governor | `nil`: nothing is gated, `ratelimit.Report` is a no-op | `WithRateLimitGovernor` (a `nil` governor is a no-op) |

## Rate-limit governor (`controller.Options.RateLimit`)

The governor slows reconciles down by itself when an external API signals a
rate limit, per endpoint key, or when the controller's own container is CPU-
or memory-saturated. The design is in
[design-rate-limit-governor](./design-rate-limit-governor.md).

It is off by default. With nothing set, behaviour is the same as before the
governor existed. A provider turns it on in its chart with one flag; no other
setting is needed. Every option below is safe to leave unset: unset means off,
or a working default.

The library reads no flags or environment itself. The flag and env names
below are the suggested convention: each provider declares its own, following
the `<NAME>_PROVIDER_<SETTING>` env pattern providers already use. The
example provider declares `--rate-limit-governor` /
`GREETING_PROVIDER_RATE_LIMIT_GOVERNOR` and `--saturation-governor` /
`GREETING_PROVIDER_SATURATION_GOVERNOR`
([greeting-controller](../examples/greeting-controller/README.md)).

| Flag / env (suggested) | Option | Default | Safe unset | What it does |
|---|---|---|---|---|
| `--rate-limit-governor` / `<NAME>_PROVIDER_RATE_LIMIT_GOVERNOR` | `controller.Options.RateLimit.Enabled` | `false` | yes (off) | Turns on the rate-limit input: calls are gated per key, and the signals the provider reports slow that key down. |
| `--rate-limit-recovery-interval` / `<NAME>_PROVIDER_RATE_LIMIT_RECOVERY_INTERVAL` | `RateLimit.RecoveryInterval` | `30s` | yes | How long a key, or the local capacity, must go clean before its capacity rises by one. Also the upper bound of the "waiting for capacity" requeue delay (1s to this value). |
| `--saturation-governor` / `<NAME>_PROVIDER_SATURATION_GOVERNOR` | `RateLimit.Saturation.Enabled` | `false` | yes (off) | Turns on the local input: the controller's own CPU and memory saturation, read from its container's cgroup v2 files. Independent of `RateLimit.Enabled`. |
| `--saturation-sample-period` / `<NAME>_PROVIDER_SATURATION_SAMPLE_PERIOD` | `RateLimit.Saturation.SamplePeriod` | `1s` | yes | How often the sampler reads the cgroup files. Local capacity halves at most once per sample. |
| `--saturation-cpu-pressure-threshold` / `<NAME>_PROVIDER_SATURATION_CPU_PRESSURE_THRESHOLD` | `RateLimit.Saturation.CPUPressureThreshold` | `50` (**provisional**) | yes | `cpu.pressure` `some avg10` percentage above which the controller counts as saturated. |
| `--saturation-memory-pressure-threshold` / `<NAME>_PROVIDER_SATURATION_MEMORY_PRESSURE_THRESHOLD` | `RateLimit.Saturation.MemoryPressureThreshold` | `10` (**provisional**) | yes | `memory.pressure` `full avg10` percentage above which the controller counts as saturated. |
| `--saturation-working-set-soft-ratio` / `<NAME>_PROVIDER_SATURATION_WORKING_SET_SOFT_RATIO` | `RateLimit.Saturation.WorkingSetSoftRatio` | `0.90` | yes | Working set (`memory.current` minus `inactive_file`) as a share of `memory.max` at which local capacity halves. |
| `--saturation-working-set-hard-ratio` / `<NAME>_PROVIDER_SATURATION_WORKING_SET_HARD_RATIO` | `RateLimit.Saturation.WorkingSetHardRatio` | `0.95` | yes | Working-set share at which local capacity drops straight to 1. |
| none | `RateLimit.Saturation.CgroupDir` | the process's own cgroup (`/sys/fs/cgroup` joined with the path in `/proc/self/cgroup`, else `/sys/fs/cgroup`) | yes | The cgroup v2 directory to read. Tests point it at fixtures. |
| none | `RateLimit.Metrics` | `nil` | yes | Records the governor's metrics, for example `*telemetry.Metrics`. |
| `--max-reconcile-rate` / `<NAME>_PROVIDER_MAX_RECONCILE_RATE` (existing) | `controller.Options.MaxConcurrentReconciles` | the provider's existing default | yes | Each controller's worker count. The governor's ceiling is this times the number of reconcilers registered with it, so enabling the governor never holds a healthy endpoint below today's total workers. Unset counts as 1. |
| none | `controller.Options.RateLimitGovernor` | `nil` (off) | yes | Set by `SetupRateLimitGovernor`. `nil` when neither input is enabled. |
| none | `reconciler.WithRateLimitGovernor(g)` | not set (off) | yes | Gates the reconciler's external calls through `g`. A `nil` `g` is a no-op, so passing `o.RateLimitGovernor` unconditionally is safe. |

Setting the flag is the only configuration. For the rate-limit input to gate
anything, the provider's code must also implement `reconciler.RateLimitKeyer`
on its connecter or client, and report signals with `ratelimit.Report` or the
`pkg/ratelimit/httpsignal` transport. See [usage](./usage.md#the-pieces-you-opt-into).

`SetupRateLimitGovernor` rejects contradictory values: a soft ratio above the
hard ratio, a ratio above 1, a pressure threshold outside (0, 100], or a
negative interval. Leaving a field unset never errors.

### The pressure thresholds are provisional

The CPU and memory pressure defaults (50% and 10%) are still provisional. The
measurement below (design open question 5) supports 50% for CPU. It could not
measure the memory-pressure threshold at all, and it shows the working-set
ratios matter more than either. core-provider is validating these numbers on
its side.

**Setup.** A kind cluster (node `kindest/node:v1.36.4`, Docker Desktop's
LinuxKit 7.0.12 kernel, cgroup v2 with PSI, 8 CPUs) ran one replica of a
measurement build of [greeting-controller](../examples/greeting-controller/README.md).
It had the same wiring, governor and keyer, with a synthetic-load `Observe`
and a per-second log of its own cgroup files. It ran 16 workers, so the
ceiling was 16, with a liveness probe on `/healthz` (period 2s, timeout 1s,
3 failures to restart). Each run lasted 120 to 240s.

**CPU.** Limit 250m. 64 Greetings polled every 1s. Each `Observe` does a fixed
amount of CPU work (3000 sha256 rounds over 64 KiB, about 100ms of CPU).

| Run | `cpu.pressure` some avg10 | Observes/s | Mean `local` | Liveness failures | Restarts |
|---|---|---|---|---|---|
| governor off (2 runs) | 74%, 80% | 1.6, 2.7 | — | 3, 1 | 0, 0 |
| threshold 25 | 42% | 1.8 | 1.0 | 1 | 0 |
| threshold 50 (default) | 42% | 2.0 | 1.1 | 0 | 0 |
| threshold 75 | 55% | 2.4 | 2.4 | 0 | 0 |

- With a CPU limit below one core, a single busy call already puts
  `some avg10` at about 40 to 45%. A threshold below that pins `local` at 1 for
  as long as there is work.
- 50 and 75 both removed the probe failures without lowering throughput.
  Throughput is CPU-bound here and varied between runs (1.6 and 2.7 Observes/s
  with the governor off).
- No run restarted, so this workload doesn't bound the threshold from above.
  50 stays the default.

**Memory.** Limit 256Mi. Each `Observe` holds 16 MiB (6% of the limit) for 1s.
The poll interval is 500ms. Greetings are added 2 every 5s, so the memory in
flight grows over minutes.

| Run | Restarts (OOMKilled) in 240s | Observes/s |
|---|---|---|
| governor off | 5 (first OOMKill at about 31s, working set 85%, 5 to 8 calls in flight; then a crash loop) | — |
| defaults: soft 0.90, hard 0.95 (5 runs; memory-pressure threshold 10, 5 or 20) | 0, 0, 1, 5, 5 | 2.1 to 2.8 in the clean runs |
| soft ratio 0.80, hard 0.95 (2 runs) | 0, 0 | 2.1, 2.2 |

- **`memory.pressure` `full avg10` stayed at 0.00 in every run, up to the
  OOMKill.** Anonymous memory with no swap gives the kernel nothing to
  reclaim, so there is no stall to measure before the OOM killer acts. The
  memory-pressure threshold therefore never engaged: 5, 10 and 20 behaved the
  same, and 10 is unmeasured. It may still matter for a working set dominated
  by page cache.
- **The working-set ratios did all the work, and 90% was too close.** The OOMs
  under the defaults came between two 1s samples. The last sample before each
  one showed 85% with all 16 slots open, and the calls admitted in the next
  second crossed the limit. At 6% of the limit per call, 10% of headroom is
  less than two calls. A soft ratio of 80% was clean in both runs.
- **A crash restarts at full capacity.** `local` starts at the ceiling, so after
  an OOMKill the restarted controller meets its whole backlog at once and can
  crash-loop. Saturation can't protect a cold start.
- **The working set lags.** Go keeps freed heap for a while, so after a cut the
  working set stays high for several samples and halves `local` again, down to
  1. Recovery is then +1 per 30s, so the controller ran at 1 to 4 calls while
  16 would have fit at the working set it settled at (30 to 55%).

What this means for a provider: set `WorkingSetSoftRatio` so that the headroom
above it holds several of the provider's largest calls. For calls that each
hold about 5% of the memory limit, 0.80 measured clean where 0.90 did not.
The defaults stay the design's 0.90 and 0.95 until the design is revised.

### Metrics

Recorded when `RateLimit.Metrics` is set (meter
`github.com/krateo-platformops/provider-runtime`):

| Instrument | Kind | Attributes | Meaning |
|---|---|---|---|
| `provider_runtime.ratelimit.deferrals` | counter | `key`, `cause` (`throttled`, `capacity`, `saturated`) | External calls the governor deferred. |
| `provider_runtime.ratelimit.signals` | counter | `key`, `kind` (`throttled`, `advisory`) | Signals providers reported that changed a key's state. Healthy quota reports are not counted. |
| `provider_runtime.ratelimit.capacity` | observable gauge | `key` | Current concurrency the governor allows for the key: the number operators used to tune by hand. |
| `provider_runtime.ratelimit.seconds_to_reset` | observable gauge | `key` | Seconds until a throttled key's hold lifts; 0 when it isn't held. |
| `provider_runtime.ratelimit.local.capacity` | observable gauge | none | Current process-wide capacity the controller's own saturation allows. |
| `provider_runtime.ratelimit.saturation.input` | observable gauge (`%`) | `input` (`cpu_pressure`, `memory_pressure`, `memory_working_set`) | Each enabled input's last value. A disabled input has no data point: 0 would read as healthy. |
| `provider_runtime.ratelimit.saturation.input.enabled` | observable gauge | `input` | 1 when the input is read, 0 when it is disabled (no cgroup v2, no PSI, `memory.max` is `max`, or saturation off). |

A missing input disables that input only. It is logged once at Info, and the
other inputs still apply.

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
