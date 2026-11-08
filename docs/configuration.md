---
type: Configuration
title: provider-runtime — configuration
description: Everything provider-runtime reads — build tags, environment consumed via the OTel SDK, option structs and their defaults (including the rate-limit governor), and the annotation switches honored at runtime.
resource: github.com/krateo-platformops/provider-runtime
tags: [library, configuration, otel, annotations]
timestamp: 2026-11-07T00:00:00Z
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

The exported `service.name` comes, in order, from `OTEL_SERVICE_NAME`,
`OTEL_RESOURCE_ATTRIBUTES`, `telemetry.Config.ServiceName`, and the SDK default
`unknown_service:<binary>`. There is no library-wide default: every provider
would then export the same `service.name`. Before v1.9.1 the struct field had
no effect.

## `telemetry.Config`

| Field | Default | Effect |
|---|---|---|
| `Enabled` | `false` | When false, `Setup` returns a nil `Metrics` handle and a no-op shutdown — every `Metrics` method is nil-safe, so wiring it unconditionally is fine. |
| `ServiceName` | empty | Sets `service.name` when the environment doesn't. See the order above. |
| `ExportInterval` | `30s` | Periodic-reader export interval for the OTLP metrics pipeline. |

## `controller.Options` (defaults from `DefaultOptions()`)

| Field | Default | Effect |
|---|---|---|
| `Logger` | nop logger | Logger the controllers use. |
| `GlobalRateLimiter` | token bucket, 1 rps | Cross-controller reconcile rate cap (use with `ratelimiter.New`). |
| `PollInterval` | `1m` | Speculative poll interval controllers should pass to the reconciler. |
| `MaxConcurrentReconciles` | `1` | Per-controller worker count. |
| `UsePriorityQueue` | `nil` (→ `true`) | Whether the instrumented queue uses controller-runtime's priority queue, with item priorities kept; only consulted when `QueueWaitRecorder` is set. |
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
rate limit, per endpoint key, or when the controller's own container is
CPU-saturated. It is not memory protection: see
[the experimental memory input](#the-pressure-thresholds-are-provisional).
The design is in [design-rate-limit-governor](./design-rate-limit-governor.md).

It is off by default. With nothing set, behaviour is the same as before the
governor existed. A provider turns it on in its chart with one flag; no other
setting is needed. Every option below is safe to leave unset: unset means off,
or a working default.

The library reads no flags or environment itself. The flag and env names
below are the suggested convention: each provider declares its own, following
the `<NAME>_PROVIDER_<SETTING>` env pattern providers already use. The
example provider declares `--rate-limit-governor` /
`GREETING_PROVIDER_RATE_LIMIT_GOVERNOR`, `--rate-limit-max-hold` /
`GREETING_PROVIDER_RATE_LIMIT_MAX_HOLD`, `--saturation-governor` /
`GREETING_PROVIDER_SATURATION_GOVERNOR` and
`--saturation-experimental-memory` /
`GREETING_PROVIDER_SATURATION_EXPERIMENTAL_MEMORY`
([greeting-controller](../examples/greeting-controller/README.md)).

| Flag / env (suggested) | Option | Default | Safe unset | What it does |
|---|---|---|---|---|
| `--rate-limit-governor` / `<NAME>_PROVIDER_RATE_LIMIT_GOVERNOR` | `controller.Options.RateLimit.Enabled` | `false` | yes (off) | Turns on the rate-limit input: calls are gated per key, and the signals the provider reports slow that key down. |
| `--rate-limit-recovery-interval` / `<NAME>_PROVIDER_RATE_LIMIT_RECOVERY_INTERVAL` | `RateLimit.RecoveryInterval` | `30s` | yes | How long a key must go clean before its capacity rises by one, and the same for the local capacity after any cut. Also the upper bound of the "waiting for capacity" requeue delay (1s to this value). |
| `--rate-limit-max-hold` / `<NAME>_PROVIDER_RATE_LIMIT_MAX_HOLD` | `RateLimit.MaxHold` (`*time.Duration`) | `1h` (`nil`) | yes | The longest a server's refusal holds its key. A `Retry-After` or reset time past it is cut to it; after the cut hold one call goes through, and if the server still refuses, the key is held again. `0` means no cap: the server's deadline holds in full. Negative is an error. A pointer, so that unset (`1h`) and `0` (no cap) differ. The first cut for a key is logged at Info with the key, the deadline the server asked for and the cap; not again until the key has gone a whole `RecoveryInterval` clean. Every cut is counted in `ratelimit.hold_clamped`. |
| `--saturation-governor` / `<NAME>_PROVIDER_SATURATION_GOVERNOR` | `RateLimit.Saturation.Enabled` | `false` | yes (off) | Turns on the local input: the controller's own CPU saturation, read from its container's cgroup v2 files. CPU only: memory is the separate experimental row below. Independent of `RateLimit.Enabled`. With it on, each controller's queue takes a local slot before every reconcile and gives it back when the reconcile returns, so low capacity behaves like fewer workers ([design](./design-local-slot-before-pop.md)). Needs the controller built with `ForControllerRuntime`, and `UsePriorityQueue` left unset or true. |
| `--saturation-experimental-memory` / `<NAME>_PROVIDER_SATURATION_EXPERIMENTAL_MEMORY` | `RateLimit.Saturation.ExperimentalMemoryWorkingSet` | `false` | yes (off) | **Experimental, and not OOM protection.** Adds the memory working set to the local input. It [did not prevent OOMKills](#re-measurement-of-the-memory-input): 0 of 19 runs clean on an overcommitted workload. |
| `--saturation-sample-period` / `<NAME>_PROVIDER_SATURATION_SAMPLE_PERIOD` | `RateLimit.Saturation.SamplePeriod` | `1s` | yes | How often the sampler reads the cgroup files. Local capacity halves at most once per sample. A shorter period only matters to the experimental memory input: CPU pressure's `avg10` is already a 10-second average, so sampling it faster gains nothing and costs CPU in the container it protects. |
| `--saturation-startup-ramp-interval` / `<NAME>_PROVIDER_SATURATION_STARTUP_RAMP_INTERVAL` | `RateLimit.Saturation.StartupRampInterval` | `2s` | yes | Local capacity starts at 1 and rises by one per this interval until its first cut, so 16 workers are all admitted after about 30s. Not tied to the sample period. After any cut, `RecoveryInterval` applies instead. A call deferred on the ramp requeues in 1s to the time the ramp still needs, capped at `RecoveryInterval`. |
| `--saturation-cpu-pressure-threshold` / `<NAME>_PROVIDER_SATURATION_CPU_PRESSURE_THRESHOLD` | `RateLimit.Saturation.CPUPressureThreshold` | `50` (**provisional**) | yes | `cpu.pressure` `some avg10` percentage above which the controller counts as saturated. |
| `--saturation-working-set-soft-ratio` / `<NAME>_PROVIDER_SATURATION_WORKING_SET_SOFT_RATIO` | `RateLimit.Saturation.WorkingSetSoftRatio` | `0.80` (**provisional**) | yes | Experimental memory input only. Working set (`memory.current` minus `inactive_file`) as a share of `memory.max` at which local capacity halves. |
| `--saturation-working-set-hard-ratio` / `<NAME>_PROVIDER_SATURATION_WORKING_SET_HARD_RATIO` | `RateLimit.Saturation.WorkingSetHardRatio` | `0.90` (**provisional**) | yes | Experimental memory input only. Working-set share at which local capacity drops straight to 1. |
| none | `RateLimit.Saturation.CgroupDir` | the process's own cgroup (`/sys/fs/cgroup` joined with the path in `/proc/self/cgroup`, else `/sys/fs/cgroup`) | yes | The cgroup v2 directory to read. Tests point it at fixtures. |
| none | `RateLimit.Metrics` | `nil` | yes | Records the governor's metrics, for example `*telemetry.Metrics`. |
| `--max-reconcile-rate` / `<NAME>_PROVIDER_MAX_RECONCILE_RATE` (existing) | `controller.Options.MaxConcurrentReconciles` | the provider's existing default | yes | Each controller's worker count. The governor's ceiling is this times the number of reconcilers registered with it, so enabling the governor never holds a healthy endpoint below today's total workers. Unset counts as 1. |
| none | `controller.Options.RateLimitGovernor` | `nil` (off) | yes | Set by `SetupRateLimitGovernor`. `nil` when neither input is enabled. |
| none | `reconciler.WithRateLimitGovernor(g)` | not set (off) | yes | Gates the reconciler's external calls per key through `g`. A `nil` `g` is a no-op, so passing `o.RateLimitGovernor` unconditionally is safe. |
| none | `controller.Options.ForControllerRuntime()` | — | — | With saturation on, gates the controller's queue on the local capacity. Call it once per controller: the governor logs an error at start when fewer controllers were gated than reconcilers registered. |

Setting the flag is the only configuration. The saturation input needs
nothing more from the provider's code than building its controllers with
`ForControllerRuntime`, which every Krateo provider already does: it bounds
whole reconciles, keyer or not. The rate-limit input, which is per endpoint, also needs the provider to
implement `reconciler.RateLimitKeyer` on its connecter or client (or
`reconciler.RateLimitKeysProvider`, for objects on several endpoints) and to report
signals with `ratelimit.Report` or the `pkg/ratelimit/httpsignal` transport.
See [usage](./usage.md#the-pieces-you-opt-into).

A refused key is held for as long as the server asks, up to `MaxHold` (1h by
default). A hold longer than `MaxHold` is retried at `MaxHold` intervals, so a
server asking for 6h receives 6 requests instead of 1. Set it to 0 to honour
any hold in full. The hold includes deleted objects: removing the finalizer needs
`Connect` and `Observe`, which are gated, so a deletion on a held key waits for
the hold too. An object annotated `krateo.io/deletion-policy: orphan` skips the
gate when it is deleted: its finalizer comes off without calling the external
system.

`SetupRateLimitGovernor` rejects contradictory values: a soft ratio above the
hard ratio, a ratio above 1, a CPU pressure threshold outside (0, 100], a
negative interval or `MaxHold`, or saturation with `UsePriorityQueue` set to false. Leaving a
field unset never errors.

### The pressure thresholds are provisional

**This release ships rate-limit and CPU slow-down. It does not ship memory
protection.** The user decided what follows from the kind measurements below
(design open question 5, and §11):

- Saturation reads CPU pressure only, on a 1s sample. The threshold, 50%, stays
  provisional: core-provider is validating it.
- The memory working set is a separate, experimental input, off by default. It
  [did not prevent OOMKills](#re-measurement-of-the-memory-input) in 0 of 19
  runs on an overcommitted workload, so turning it on does not protect a
  controller from them. 0.80 and 0.90 are its provisional ratios.
- `local` starts at 1 and rises by 1 per `StartupRampInterval` (2s) until its
  first cut. After any cut, it rises by 1 per clean `RecoveryInterval` (30s),
  the same as API limits.
- `local` never goes below 1: one reconcile always keeps going.
- `local` is taken before the pop and covers the whole reconcile, so nothing is
  written on the object for it: the item waits in the queue. This replaced the
  first implementation's local deferrals and its quiet startup ramp (design
  §11 item 18).
- `memory.pressure` is no longer an input. It read 0.00 up to every OOMKill.
- Saturation bounds every reconcile, with or without a keyer.

Keep workers times the largest call well under the memory limit. The governor
can't rescue a controller whose workers can overrun it.

#### CPU at the final code

Threshold 50, 1s sample, the CPU workload of the
[first measurement](#before-first-measurement-1s--090--095-local-starting-at-the-ceiling):
250m limit, 64 Greetings polled every 1s, 16 workers, about 100ms of CPU per
`Observe`. Each run lasted 180s, one at a time, in the order 50, off, 50, 50.
Means are from 40s on, after the startup ramp.

| Run | `cpu.pressure` some avg10 | Observes/s | Mean `local` | CPU used (of 250ms/s) | Liveness failures | Restarts |
|---|---|---|---|---|---|---|
| threshold 50 (3 runs) | 47%, 45%, 40% | 1.4, 1.7, 2.1 | 1.1, 1.1, 1.3 | 218, 217, 228ms | 1, 0, 0 | 0, 0, 0 |
| governor off (1 run) | 78% | 2.6 | — | 250ms | 3 | 0 |

- No run restarted, and the governor cut CPU pressure by about 30 points.
- **It costs throughput here:** 20 to 47% below the one governor-off run, with
  about 1,300 deferred calls per run. With a CPU limit below one core, one busy
  call already reads about 40 to 45%, so a second call goes over 50 and is cut,
  and at +1 per 30s `local` stays near 1 for as long as there is work.
  This is the behaviour CPU 50 was first measured with (2.0 Observes/s, 42%).
- The host was shared and busy, and the governor-off control is a single run,
  so read the throughput as a direction.
- With a 1s sample, each 2s step lands on a tick, and these runs stepped about
  every 2.5s, so the ramp to 16 takes closer to 37s than 30s.
- These runs did not retune the threshold.

**The CPU threshold stays 50, decided by the user.** Under a CPU limit below one
core, a single busy call reads about 40 to 45%, so a controller there runs
close to one call at a time. In the runs above that was 1.4 to 2.1 Observes/s
against 2.6 with the governor off, from one control on a busy host. If that's
too slow, raise the threshold or the CPU limit.

The sampler's own cost is below, with [the re-measurement](#re-measurement-of-the-memory-input).


#### Re-measurement of the memory input

These runs measured 250ms, 0.80/0.90, a slow start and +1 per clean sample, the
settings before the user's final decision. They are why the memory input is
experimental. The startup and recovery rules above came after them, and the
memory input has not been measured with those rules.

**The memory input did not prevent OOMKills on this workload.** It ran clean in
0 of 19 runs. The workload is harsh on purpose: 16 workers each holding 16 MiB
is 100% of the 256Mi limit before the controller's own memory, so it is
overcommitted by construction. These numbers come from the example provider
under that synthetic load, not from a real provider.

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

**The crash loop was not gone.** At +1 per 250ms, `local` was 5 after one second
and 9 after two, and in one run the working set was at 91% by the third second.
That ramp was faster than the working set reacts. The user replaced it with +1
per 2s, which has not been measured against this workload.

**CPU at 250ms.** The first measurement's CPU workload, with the threshold at
50.

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

**The sampler's own cost.** Process CPU over 60s against an idle baseline, in a
container reading real cgroup files, as a share of one core:

| Period | Inputs | Quieter host | Busier host |
|---|---|---|---|
| 1s (default) | CPU | 0.06 to 0.07% (3 runs) | not measured |
| 1s | CPU and memory | 0.04 to 0.10% (2 runs) | 0.8% (1 run) |
| 250ms | CPU | 0.13% (1 run) | not measured |
| 250ms | CPU and memory | 0.18 to 0.19% (2 runs) | 1.5 to 1.8% (2 runs) |

The same code differed about tenfold between the two sessions, and the cause is
not known. So 250ms with memory cost between 0.7 and 7% of a 250m CPU limit. The
default, 1s with CPU only, costs about 0.3% of that limit.

What the user decided from these results, and the follow-up work on the memory
input, are in §11 of the [design](./design-rate-limit-governor.md).

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
| `provider_runtime.ratelimit.deferrals` | counter | `key`, `cause` (`throttled`, `capacity`) | External calls the governor deferred on a rate-limit key: `throttled` while a key is held after a refusal, `capacity` when a key's capacity is full. `key` is the key that stopped the call. The local saturation capacity defers nothing since it moved into the queue; it shows as `ratelimit.local.capacity`. |
| `provider_runtime.ratelimit.signals` | counter | `key`, `kind` (`throttled`, `advisory`) | Signals providers reported that changed a key's state. Healthy quota reports are not counted. |
| `provider_runtime.ratelimit.hold_clamped` | counter | `key` | Throttled signals whose deadline was past `MaxHold` and was cut to it. Every cut counts, including the ones not logged. A key that keeps rising here has a server asking for holds longer than the cap. |
| `provider_runtime.ratelimit.capacity` | observable gauge | `key` | Current concurrency the governor allows for the key: the number operators used to tune by hand. |
| `provider_runtime.ratelimit.seconds_to_reset` | observable gauge | `key` | Seconds until a throttled key's hold lifts; 0 when it isn't held. |
| `provider_runtime.ratelimit.local.capacity` | observable gauge | none | Current process-wide capacity the controller's own saturation allows. With saturation on it starts at 1 and ramps up per clean sample. With saturation off it reports nothing: no local capacity is enforced then. |
| `provider_runtime.ratelimit.saturation.input` | observable gauge (`%`) | `input` (`cpu_pressure`, `memory_working_set`) | Each enabled input's last value: `some avg10` for `cpu_pressure`, the share of `memory.max` for `memory_working_set`. A disabled input has no data point: 0 would read as healthy. |
| `provider_runtime.ratelimit.saturation.input.enabled` | observable gauge | `input` | 1 when the input is read, 0 when it is disabled (no cgroup v2, no PSI, `memory.max` is `max`, saturation off, or, for `memory_working_set`, the experimental option unset). |

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
