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
| `--rate-limit-recovery-interval` / `<NAME>_PROVIDER_RATE_LIMIT_RECOVERY_INTERVAL` | `RateLimit.RecoveryInterval` | `30s` | yes | How long a key must go clean before its capacity rises by one (per-endpoint API limits only; the local capacity ramps per sample instead). Also the upper bound of the "waiting for capacity" requeue delay (1s to this value). |
| `--saturation-governor` / `<NAME>_PROVIDER_SATURATION_GOVERNOR` | `RateLimit.Saturation.Enabled` | `false` | yes (off) | Turns on the local input: the controller's own CPU and memory saturation, read from its container's cgroup v2 files. Independent of `RateLimit.Enabled`. With it on, every gated call (`Connect` and each `ExternalClient` method) needs a local slot, whether or not the provider implements `RateLimitKeyer`. Local capacity starts at 1 and ramps up. |
| `--saturation-sample-period` / `<NAME>_PROVIDER_SATURATION_SAMPLE_PERIOD` | `RateLimit.Saturation.SamplePeriod` | `250ms` | yes | How often the sampler reads the cgroup files. It is also the ramp step: local capacity rises by 1 per clean sample and halves at most once per sample. A shorter period makes the working-set input react faster; it doesn't make CPU pressure react faster, because `avg10` is already a 10-second average. |
| `--saturation-cpu-pressure-threshold` / `<NAME>_PROVIDER_SATURATION_CPU_PRESSURE_THRESHOLD` | `RateLimit.Saturation.CPUPressureThreshold` | `50` (**provisional**) | yes | `cpu.pressure` `some avg10` percentage above which the controller counts as saturated. |
| `--saturation-working-set-soft-ratio` / `<NAME>_PROVIDER_SATURATION_WORKING_SET_SOFT_RATIO` | `RateLimit.Saturation.WorkingSetSoftRatio` | `0.80` | yes | Working set (`memory.current` minus `inactive_file`) as a share of `memory.max` at which local capacity halves. |
| `--saturation-working-set-hard-ratio` / `<NAME>_PROVIDER_SATURATION_WORKING_SET_HARD_RATIO` | `RateLimit.Saturation.WorkingSetHardRatio` | `0.90` | yes | Working-set share at which local capacity drops straight to 1. |
| none | `RateLimit.Saturation.CgroupDir` | the process's own cgroup (`/sys/fs/cgroup` joined with the path in `/proc/self/cgroup`, else `/sys/fs/cgroup`) | yes | The cgroup v2 directory to read. Tests point it at fixtures. |
| none | `RateLimit.Metrics` | `nil` | yes | Records the governor's metrics, for example `*telemetry.Metrics`. |
| `--max-reconcile-rate` / `<NAME>_PROVIDER_MAX_RECONCILE_RATE` (existing) | `controller.Options.MaxConcurrentReconciles` | the provider's existing default | yes | Each controller's worker count. The governor's ceiling is this times the number of reconcilers registered with it, so enabling the governor never holds a healthy endpoint below today's total workers. Unset counts as 1. |
| none | `controller.Options.RateLimitGovernor` | `nil` (off) | yes | Set by `SetupRateLimitGovernor`. `nil` when neither input is enabled. |
| none | `reconciler.WithRateLimitGovernor(g)` | not set (off) | yes | Gates the reconciler's external calls through `g`. A `nil` `g` is a no-op, so passing `o.RateLimitGovernor` unconditionally is safe. |

Setting the flag is the only configuration. The saturation input needs
nothing more from the provider's code: it bounds every gated call, keyer or
not. The rate-limit input, which is per endpoint, also needs the provider to
implement `reconciler.RateLimitKeyer` on its connecter or client and to report
signals with `ratelimit.Report` or the `pkg/ratelimit/httpsignal` transport.
See [usage](./usage.md#the-pieces-you-opt-into).

`SetupRateLimitGovernor` rejects contradictory values: a soft ratio above the
hard ratio, a ratio above 1, a CPU pressure threshold outside (0, 100], or a
negative interval. Leaving a field unset never errors.

### The pressure thresholds are provisional

The CPU pressure threshold (50%) is still provisional: core-provider is
validating it. The other saturation settings were decided by the user after
the first kind measurement (design open question 5), and then measured again.
**The second measurement found they do not prevent OOMKills on the synthetic
workload**, so read "After" below before relying on them:

- `memory.pressure` is no longer an input. It read 0.00 up to every OOMKill.
- The sample period is 250ms, and the working-set ratios are 0.80 (halve) and
  0.90 (straight to 1). These replace 1s, 0.90 and 0.95.
- `local` starts at 1 and rises by 1 per clean sample, instead of starting at
  the ceiling.
- Saturation bounds every gated call, with or without a keyer.

#### After (the decided settings: 250ms / 0.80 / 0.90, slow start, +1 per clean sample)

**The decided settings did not prevent OOMKills on this workload.** They ran
clean in 0 of 19 runs. The workload is harsh on purpose: 16 workers each
holding 16 MiB is 100% of the 256Mi limit before the controller's own memory.
A provider whose workers fit well under the soft ratio may never see this.
These numbers come from the example provider under that synthetic load, not
from a real provider.

The setup and the memory workload are those of the first measurement below.
A run is clean with 0 restarts in 240s. CrashLoopBackOff caps restarts at about
5 in that time, so 4 or 5 means it crash-looped for the whole run. The host was
shared: other workloads were using several of its 8 CPUs, and host load
changed during the day. So only runs alternated in one block are comparable.

| Code and settings | How it ran | Runs | Clean | Restarts per run |
|---|---|---|---|---|
| New defaults | 5 at a time | 10 | 0 | 4, 4, 4, 3, 3, 4, 4, 4, 4, 2 |
| New code, old settings (1s, 0.90/0.95), the requested control | 5 at a time | 10 | 0 | 4 in every run |
| New defaults | alternating with the old code | 3 | 0 | 3, 4, 3 |
| Old code (1s, 0.90/0.95, starts at the ceiling, +1 per 30s) | alternating with the new defaults | 3 | 1 | 5, 0, 5 |
| New defaults | alternating with the diagnostic | 3 | 0 | 4, 4, 4 |
| Diagnostic: new defaults, but +1 per 30s after the first cut (a scratch build, not in the code) | alternating with the new defaults | 3 | 1 | 3, 1, 0 |
| New defaults | one at a time, CPU-contended host | 3 | 0 | 2, 2, 1 |
| New code, old settings | one at a time, CPU-contended host | 3 | 0 | 4, 4, 1 |
| Old code | one at a time, quieter host | 3 | 2 | 0, 5, 0 |
| New defaults with `GOMEMLIMIT=200MiB` | 5 at a time | 5 | 1 | 2, 2, 2, 0, 2 |
| Governor off with `GOMEMLIMIT=200MiB` | 5 at a time | 2 | 0 | 5, 5 |

The "CPU-contended" runs also had liveness kills, with CPU pressure of 24 to
39% on a workload that barely uses CPU; every other run's restarts were
OOMKills, apart from one liveness kill each in the `GOMEMLIMIT` off runs and one
in the block alternating with the diagnostic. So the comparison that counts
is the two alternating blocks.

The requested control reproduced the OOMs, but it is not the old behaviour: it
has the slow start and the per-sample ramp. That's why the old code is listed
separately.

- **`local` reached 1 in time, and the floor call still killed the container.**
  In 18 of the 19 runs the last sample before the first death captured read
  `local=1`, at 96 to 100% of the limit. Go keeps freed heap for a while,
  so one 16 MiB call lands on top of memory that is no longer in use but is
  still charged to the container. "One call always keeps going" (§7) can't hold
  there.
- **The ramp rebuilds the heap.** At +1 per 250ms, `local` is back at the
  ceiling 4 seconds after a cut, before the heap has shrunk. With +1 per 30s
  after the first cut, restarts fell from 4, 4, 4 to 3, 1, 0 on the same host.
  Three runs each: a direction, not a proof. That build's own OOMs came at
  `local=16`, before any cut.
- **The working set doesn't track load smoothly.** It sits on plateaus set by
  the Go GC's heap goal, then the next goal can pass the limit in one step, so
  the last sample before an OOM can read 66 to 79% with all 16 slots open. That
  happened under both old and new settings.
- **`GOMEMLIMIT` helped more than any governor setting.** With the new defaults,
  throughput rose from 2.1-2.6 to 6.3-7.2 Observes/s and restarts fell to 2 or
  fewer, but only 1 of 5 runs was clean. Without saturation it didn't help
  (5, 5), so the two work together. It is a chart setting, out of this library's
  scope (§7).

**Cold start.** 64 Greetings exist before the controller starts; each `Observe`
holds 16 MiB for 300ms.

| Code and settings | Runs | Clean | Restarts per run |
|---|---|---|---|
| New defaults (slow start) | 5 | 0 | 4, 4, 4, 4, 4 |
| New code, old settings | 2 | 0 | 4, 4 |
| Old code (starts at the ceiling) | 2 | 0 | 5, 4 |
| Governor off | 1 | 0 | 5 |
| New defaults with `GOMEMLIMIT=200MiB` | 3 | 0 | 3, 2, 3 |

**The crash loop is not gone.** At +1 per 250ms, `local` is 5 after one second
and 9 after two, and in one run the working set was at 91% by the third second.
Slow start ramps faster than the working set reacts. It delays the first death
but doesn't prevent it.

**CPU.** The first measurement's CPU workload at the 250ms period, with the
threshold at 50.

| Run | `cpu.pressure` some avg10 | Observes/s | Mean `local` | CPU used (of 250ms/s) | Liveness failures | Restarts |
|---|---|---|---|---|---|---|
| threshold 50 (3 runs) | 53% in each | 2.7, 2.6, 2.7 | 1.5 to 1.7 | 218 to 223ms | 1, 0, 0 | 0, 0, 0 |
| governor off (1 run) | 72% | 3.0 | — | 250ms | 1 | 0 |

No restarts, and 50 stays. Throughput was about 10% below the one governor-off
run, which is inside the spread the first measurement saw between off runs (1.6
and 2.7), and 11 to 13% of the CPU quota went unused, because `local` sits near
1. Remember that `avg10`
is a 10-second average: the 250ms period lets the working-set input react
faster, but CPU pressure reacts no faster than before.

**The sampler's own cost.** One sample took 3.8 to 4.4ms of CPU in a container
reading real cgroup files, measured as process CPU over 60s against an idle
baseline (2 runs). That is **1.5 to 1.8% of one core at 250ms**, or 6 to 7% of
a 250m CPU limit. At 1s it was 0.8% of a core (1 run). `BenchmarkSample` on
fixture files reports about 0.3ms; the kernel's cgroup files cost far more than
plain files, so don't read that benchmark as the real cost.

**What needs a decision.** The settings stay as decided until the user revises
them; see §11 of the [design](./design-rate-limit-governor.md). Until then, keep
workers times the largest call well under the soft ratio of the memory limit:
the governor can't rescue a controller whose workers can overrun it.

#### Before (first measurement, 1s / 0.90 / 0.95, local starting at the ceiling)

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
  50 stayed the default.

**Memory.** Limit 256Mi. Each `Observe` holds 16 MiB (6% of the limit) for 1s.
The poll interval is 500ms. Greetings are added 2 every 5s, so the memory in
flight grows over minutes.

| Run | Restarts (OOMKilled) in 240s | Observes/s |
|---|---|---|
| governor off | 5 (first OOMKill at about 31s, working set 85%, 5 to 8 calls in flight; then a crash loop) | — |
| soft 0.90, hard 0.95 (5 runs) | 0, 0, 1, 5, 5 | 2.1 to 2.8 in the clean runs |
| soft ratio 0.80, hard 0.95 (2 runs) | 0, 0 | 2.1, 2.2 |

- **`memory.pressure` `full avg10` stayed at 0.00 in every run, up to the
  OOMKill.** Anonymous memory with no swap gives the kernel nothing to
  reclaim, so there is no stall to measure before the OOM killer acts. This is
  why it was dropped as an input.
- **The working-set ratios did all the work, and 90% was too close.** The OOMs
  came between two 1s samples. The last sample before each one showed 85% with
  all 16 slots open, and the calls admitted in the next second crossed the
  limit. At 6% of the limit per call, 10% of headroom is less than two calls.
  A soft ratio of 80% was clean in both runs, which the "After" runs did not
  repeat.
- **A crash restarted at full capacity.** `local` started at the ceiling, so
  after an OOMKill the restarted controller met its whole backlog at once and
  could crash-loop. This is why `local` now starts at 1.
- **The working set lags.** Go keeps freed heap for a while, so after a cut the
  working set stays high for several samples and halves `local` again, down to
  1. Recovery was then +1 per 30s, so the controller ran at 1 to 4 calls while
  16 would have fit at the working set it settled at (30 to 55%). `local` now
  recovers by +1 per clean sample.

### Metrics

Recorded when `RateLimit.Metrics` is set (meter
`github.com/krateo-platformops/provider-runtime`):

| Instrument | Kind | Attributes | Meaning |
|---|---|---|---|
| `provider_runtime.ratelimit.deferrals` | counter | `key`, `cause` (`throttled`, `capacity`, `saturated`) | External calls the governor deferred. `key` is empty for a call deferred only by the local capacity with no key (a provider without `RateLimitKeyer`). |
| `provider_runtime.ratelimit.signals` | counter | `key`, `kind` (`throttled`, `advisory`) | Signals providers reported that changed a key's state. Healthy quota reports are not counted. |
| `provider_runtime.ratelimit.capacity` | observable gauge | `key` | Current concurrency the governor allows for the key: the number operators used to tune by hand. |
| `provider_runtime.ratelimit.seconds_to_reset` | observable gauge | `key` | Seconds until a throttled key's hold lifts; 0 when it isn't held. |
| `provider_runtime.ratelimit.local.capacity` | observable gauge | none | Current process-wide capacity the controller's own saturation allows. With saturation on it starts at 1 and ramps up per clean sample. |
| `provider_runtime.ratelimit.saturation.input` | observable gauge (`%`) | `input` (`cpu_pressure`, `memory_working_set`) | Each enabled input's last value: `some avg10` for `cpu_pressure`, the share of `memory.max` for `memory_working_set`. A disabled input has no data point: 0 would read as healthy. |
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
