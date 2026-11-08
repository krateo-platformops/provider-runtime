---
type: Architecture
title: provider-runtime — size local capacity from what reconciles cost
description: Design, not built, for replacing the saturation input's halve-and-recover step with a per-reconcile cost model that computes how many reconciles fit in the container's CPU and memory limits, with CPU pressure as a safety check that only blocks increases.
resource: github.com/krateo-platformops/provider-runtime
tags: [design, rate-limiting, saturation, cgroup, gc]
timestamp: 2026-11-08T00:00:00Z
---

# Size local capacity from what reconciles cost

**Status:** design, for independent review. Nothing is built. The open questions
were decided by the user on 2026-11-08 (§13). It replaces the
mechanical step of §7 of [design-rate-limit-governor](./design-rate-limit-governor.md)
(`saturation.step`, `pkg/ratelimit/saturation.go`) and the memory input of the
held draft PR #48. The local slot and the gated queue of
[design-local-slot-before-pop](./design-local-slot-before-pop.md) stay as they
are; this design changes only how many slots there are. §12 reconciles it with
the survey the user commissioned, and §13 records the user's decisions.

**References.** Code is provider-runtime at `2a6ff02` (main), `CR/` is
`sigs.k8s.io/controller-runtime@v0.25.2` (the version in `go.mod`), and
`GOROOT/` is Go 1.26.10 (`toolchain go1.26.10` in `go.mod`). `#48` is
`origin/feat/experimental-memory-headroom` at `0b35f67`, and every measurement
file named below is under `hack/rlgov-measure/` on that branch. "The survey" is
a literature and industry survey the user commissioned for this design, "Compute
the knee, let pressure only veto"; it isn't in this repository, so §12
summarises everything taken from it. Where it cites a primary source, this
document names the source, and §14 lists what was checked here and what was
taken from the survey unchecked.

## 1. The problem, measured

The user's goal: slow reconciles down when CPU or memory is at its limit, from
data, not by a mechanical step. Today's input reads `cpu.pressure` `some avg10`
once a second. Over the threshold (default 50) it halves local capacity; when
clean it adds 1 per `RecoveryInterval` (30s), or 1 per `StartupRampInterval`
(2s) before the first cut (`saturation.go`, `step`).

**The GKE block of 2026-11-08** (`results/cpu-block2-report.txt`, settings in
`results/cpu2-settings.txt`): e2-standard-4, a 250m CPU limit, 16 workers, 64
Greetings polled every second, each Observe spending 20ms of CPU measured on its
own thread and then waiting 400ms, liveness probe on, 5 runs per arm.

| Arm | CPU ms/s | Pressure (avg10) | Observes/s | Local capacity |
|---|---|---|---|---|
| off | 250 (the limit) | 56.2 | 11.29 | none |
| threshold 75 | 250 | 59.0 | 11.13 | 16, never cut |
| threshold 50 | 177 (71%) | 17.0 | 7.78 (−31%) | mean 3.8, minimum 1 |

No arm restarted, failed a probe or was OOMKilled. Threshold 50 engaged in every
run and settled at 71% of the limit. It halves on every 1s sample of a signal
averaged over 10s, so one episode cuts 16 → 8 → 4 → 2 → 1 before its first cut
shows in the signal, and it then climbs back by 1 every 30s. The survey reaches
the same diagnosis: a fast actuator driven by a slow sensor (Hellerstein, ACC
2004).

**The workload's cost curve, read from the same logs.** The threshold-50 arm
spent long stretches at each capacity from 1 to 7, so its per-second `STAT`
lines give the controller's response at each concurrency. The table groups
every sample from t ≥ 20s whose capacity equals the previous sample's, over the
5 runs (`results/c2-50-*.log`), and the off arm at 16 (`results/c2-off-*.log`).
Demand always exceeded capacity (64 objects polled every second against at most
11 Observes a second), so reconciles in flight equal capacity, and the mean
reconcile duration is `D = S / X` by Little's law.

| S (slots) | Samples | CPU U, ms/s | Throughput X, /s | D = S/X, s | Burn wall per call, ms | avg10 |
|---|---|---|---|---|---|---|
| 1 | 282 | 54 | 2.33 | 0.429 | 20.2 | 16.1 |
| 2 | 184 | 105 | 4.50 | 0.444 | 20.2 | 1.6 |
| 3 | 264 | 153 | 6.65 | 0.451 | 20.3 | 10.4 |
| 4 | 296 | 203 | 8.77 | 0.456 | 24.1 | 8.1 |
| 5 | 298 | 242 | 10.34 | 0.483 | 67.2 | 16.7 |
| 6 | 300 | 248 | 10.90 | 0.550 | 120.5 | 36.6 |
| 7 | 58 | 249 | 11.10 | 0.630 | 175.4 | 47.6 |
| 16 (off) | 1800 | 250 | 11.35 | 1.41 | 587.5 | 56.0 |

Caveats: the harness computes rates per nominal second (`time.Tick`, `main.go`),
so single samples are noisy, but these are means over hundreds; X counts
Observes, one per reconcile here; avg10 lags, so the S = 1 row carries the
pressure of the cut just before it. Three facts from the table carry the design:

- **CPU per reconcile is about 23ms, not 20.** A least-squares line through the
  rows S = 1 to 5 gives `U = 23.3 ms × X − 0.8 ms/s`; through S = 1 to 4,
  `23.1 ms × X + 0.5 ms/s`. The controller's own work adds about 3ms to the
  20ms burn, and the background (informers, GC, the probe server) is within
  noise of zero for this workload.
- **The duration rises before the limit.** From 0.429s at one slot to 0.483s at
  five, at 97% of the limit: with a 250m quota (25ms per 100ms period) and
  GOMAXPROCS 2, two 20ms bursts in one period already exhaust it (§2). The
  burn's wall time shows it: 20ms up to four slots, 67ms at five.
- **90% of the limit lies between 4 and 5 slots.** 225 ms/s interpolates to
  S* = 4.56, X* = 9.65/s (85% of the off arm's 11.35), D* = 0.473s.

**A correction to the brief.** The brief's "about 11 slots at 90%" is the
allowed throughput (0.9 × 250 / 20 ≈ 11.25 completions a second), not slots.
The coordinator's correction, about 4.7 (the survey's `N* = L·R₀/D_cpu = 5.25`,
× 0.9), used 20ms of CPU and R₀ = 0.42s; with the measured 23.1ms and 0.429s the
uncontended formula gives 4.2, and the measured curve 4.56.

## 2. What we have to work with

Checked against source unless §14 says otherwise.

- **`cpu.max`** is `"$MAX $PERIOD"` in microseconds, `max` meaning no limit
  (kernel `cgroup-v2.rst`; `GOROOT/src/internal/runtime/cgroup/cgroup_linux.go:171-176`).
  The limit in cores is `L = MAX / PERIOD`. The harness logs read
  `cpu_max="200000 100000"` on GKE (`results/b2-ramp-new-1.log`).
- **`cpu.stat`** always has `usage_usec`, `user_usec`, `system_usec` for all
  processes in the cgroup. `nr_periods`, `nr_throttled`, `throttled_usec`,
  `nr_bursts`, `burst_usec` appear only when the cpu controller is enabled, and
  are non-hierarchical: "they only account for throttling caused by this
  cgroup's own bandwidth limit" (`cgroup-v2.rst`). The default period is 100ms
  and quota is handed to per-CPU runqueues in 5ms slices (`sched-bwc.rst`).
- **`cpu.pressure`** has `some` and `full` lines with `avg10`, `avg60`, `avg300`
  and `total`, "the total absolute stall time (in us)" (`psi.rst`).
- **Go's runtime metrics** (`GOROOT/src/runtime/metrics/doc.go`):
  `/gc/heap/live:bytes`, "heap memory occupied by live objects that were marked
  by the previous GC" (:219); `/gc/cycles/total:gc-cycles` (:159);
  `/gc/gomemlimit:bytes`, `math.MaxInt64` when unset (:181);
  `/gc/gogc:percent` (:176); `/gc/heap/allocs:bytes`, cumulative (:192);
  `/gc/limiter/last-enabled:gc-cycle`, the cycle the GC CPU limiter last engaged
  (:233); the `/memory/classes/*` that #48 uses for non-heap memory. Reading them
  doesn't stop the world: `readMetrics` takes only `metricsLock`
  (`GOROOT/src/runtime/metrics.go:1013`); the stop is in `ReadMemStats`
  (`mstats.go:358`). `/cpu/classes/*` are GOMAXPROCS × wall-time estimates, "an
  overestimate, and not directly comparable to system CPU time" (:133-138), so
  they aren't used for CPU.
- **GOMAXPROCS** defaults to `min(ncpu, max(2, ceil(limit)))`
  (`GOROOT/src/runtime/cgroup_linux.go:109-119`): 2 at 250m.
- **The GC CPU limiter** caps GC at about 50% of CPU, after which the heap may
  pass the Go memory limit (`GOROOT/src/runtime/mgclimit.go:9-28`).
- **The gate.** The Governor counts local slots in `localInFlight` under `g.mu`:
  `+1` when `dispatchLocked` grants one to a queue (`governor.go:406-424`), `−1`
  in `releaseLocal` (`:391-404`). `releaseLocal` is called by `Done` when a
  reconcile ends (`queue.go:394`) and by a worker that hands an unused grant
  back (`:307`, `:321`). controller-runtime defers `Done` around the reconcile
  (`CR/pkg/internal/controller/controller.go:423-441`), so a slot spans grant to
  `Done` on every exit path. Capacity is published in `g.local`, an atomic
  pointer, by the sampler (`sample`, `governor.go:694-707`).

## 3. The estimators

### 3.1 Notation

Each sample `i` ends when the sampler wakes, every `SamplePeriod` (1s). Its
length `Δtᵢ` is measured on the monotonic clock, never assumed: a sampler
delayed by throttling would otherwise read 1.5s of CPU as one second. (The
harness divides by a nominal second, the likely reason its per-second CPU in the
off arm ranges from 150 to 374 ms/s against a 250 ms/s cap; its means are still
right.)

| Symbol | Meaning | Unit | Source |
|---|---|---|---|
| `L` | CPU limit | cores | `cpu.max`, read every sample |
| `ρ` | CPU target, `CPUTarget` | fraction | option, default 0.9 |
| `ΔUᵢ` | CPU used in the sample | CPU-seconds | `cpu.stat` `usage_usec` delta |
| `Aᵢ` | slot-seconds: `∫ N dt` over the sample | seconds | the Governor's books, §3.2 |
| `N̄ᵢ = Aᵢ/Δtᵢ` | mean reconciles in flight | — | |
| `Cᵢ` | reconciles that ended (`Done`) | count | the Governor's books |
| `N⁻ᵢ` | least in flight at any moment of the sample | count | the Governor's books |
| `B̂` | background CPU | cores | idle samples, §3.3 |
| `G` | Go memory limit | bytes | `/gc/gomemlimit:bytes` |
| `r` | memory reserve, `MemoryReserve` | fraction of `G` | option, default 0.15 |

### 3.2 What a sample measures

The Governor keeps three numbers beside `localInFlight`, all under `g.mu`, which
every grant and release already holds:

- `A`, the slot-seconds: at each change of `localInFlight`, add
  `localInFlight × (now − lastChange)`. The sampler reads it, with the partial
  interval up to its own `now`, and starts the next sample from there, in one
  critical section.
- `C`, reconciles ended. `releaseLocal` gains a flag so a handed-back grant isn't
  counted.
- `N⁻`, the minimum of `localInFlight` since the last sample, for memory (§3.6).

Each sample is one of three kinds, and only the kind it is updates anything:

- **Idle**: `A = 0` and `C = 0`. It measures the background, and nothing else.
- **Capped**: the cgroup used its quota in every period of the sample:
  `Δnr_throttled ≥ Δnr_periods ≥ 1` and `ΔU ≥ (Δt − PERIOD) × L`, the second so
  that a reading within one period's quota of the cap is required too. In a
  capped sample the CPU used says nothing about how much more the reconciles
  wanted: it is censored at `L`. Its durations measure the throttling, not the
  work.
- **Active**: anything else.

### 3.3 CPU per reconcile, and the background

Go can't attribute CPU to a goroutine, so the only measurement is the whole
cgroup's CPU per sample. It includes the informers, watch decoding, the GC, the
probe server, leader election and the sampler itself. Three ways to separate
the background, and the one chosen:

1. **A ratio, `ΔU / C`,** charges all background to reconciles,
   `c_naive = c + B/X`, most at low throughput. Spinner et al. (2015, via the
   survey) found the plain ratio off by 9–50% with a 5–30% background.
2. **A regression of `ΔU` on `C` with an intercept** (non-negative least
   squares or recursive least squares with forgetting, the survey's
   recommendation, after Zhang, Cherkasova and Smirni 2007). It separates slope
   from intercept only when throughput varies across windows. At the operating
   point it doesn't: the slots are full and throughput is flat, so the two are
   confounded exactly when the model runs.
3. **Idle samples** (chosen): `B̂` is the smoothed `ΔU/Δt` over idle samples
   (§3.5), 0 until one is seen. A controller is idle between its cache sync and
   its first reconcile, whenever its queues are empty, on a standby replica,
   and on a woken scale-to-zero Deployment before its first object.

The choice rests on §4.3: **the setpoint doesn't depend on `B̂`.** For any
`B̂` below `ρL` the law of §4.2 settles where the cgroup's total CPU is `ρL`;
`B̂` changes only how fast it gets there. An accurate per-reconcile cost matters
for a feed-forward formula, which has no way to correct its own bias; it doesn't
for this one. The one condition is `B̂ ≤ U(0⁺)`, the CPU just before work starts,
for the "no overshoot" part of the proof, and an idle sample measures that
directly. In this workload the regression puts the background at
−0.8 to 0.5 ms/s, so `B̂ ≈ 0` and the choice changes nothing; the memory
block's idle samples read 1–2 ms/s (`results/b2-ramp-new-1.log`, t = 2–15).

**A mix of cheap and expensive reconciles** needs nothing separate. The law
uses the CPU per slot-second, `k = (ΔU − B̂Δt)/A`, so each reconcile weighs in
by the time it holds a slot: a key deferral that returns in a millisecond adds
almost nothing to `A` and almost nothing to `ΔU`. The mix is what the window
saw. When the mix changes, the estimate is a cost change like any other (§3.5,
the restart).

Exported for people, not used by the law: the CPU per completion
`ĉ = Σ(ΔU − B̂Δt)/ΣC` and the throughput.

### 3.4 Duration

The in-gate wall time, grant to `Done`, is `D̂ = ΣA/ΣC` over the same window:
the slot-seconds per completion. Little's law makes it the mean in-gate time of
the reconciles that ended, up to the reconciles straddling the window's edges.
It needs no timestamp per reconcile. The law of §4.2 doesn't even use it,
because `S = (ρL − B̂)/ĉ × D̂ = (ρL − B̂)/k̂` once both come from the same window.
That matters: `A` accrues continuously, while a duration is known only when a
reconcile ends, which is the timing hazard Hellerstein (ACC 2004, via the
survey) warns of when reconciles last as long as the control interval.
`D̂` is used only in capped samples (§4.2), and exported.

### 3.5 Smoothing, warm-up, change, and trust

Each estimate is a ratio of two exponentially weighted sums (for example
`k̂ = Σw(ΔU − B̂Δt) / Σw·A`), not an average of per-sample ratios, so a sample
with few completions weighs little and none divides by zero.

- **Window.** Weight `α = 1 − exp(−Δt/τ)`, with
  `τ = max(10 s, 60 / X̂)`: at least ten samples, and at least 60 completions per
  time constant, Spinner's guidance for a usable window (via the survey). Here
  `X̂ ≈ 10/s`, so `τ = 10 s`, about 19 effective samples. At 10% noise per
  sample (the simulation's assumption, §4.3; this workload's isn't measured on
  a true interval, §3.1), that puts the estimate's standard error near 2%.
- **Warm start.** The `n`-th sample after a start or a restart uses
  `max(α, 1/n)`, so early estimates are plain means, unbiased, and the EWMA
  takes over after `τ/Δt` samples.
- **Which samples feed what.** `k̂` and `D̂`: active samples only. `ĉ`: active
  and capped samples, because the cap censors the CPU wanted, not the CPU each
  completed reconcile used. `B̂`: idle samples only.
- **Change.** Each estimate keeps a smoothed variance of its per-sample values.
  A sample more than 3σ from the mean is held out, not averaged in. Two in a
  row on the same side restart the estimate from that sample (`n = 1`). Under
  Gaussian noise a false restart has probability about `2 × 0.00135² ≈ 4×10⁻⁶`
  per sample, about once every three days at 1s, and costs only a few seconds of
  a noisier estimate. Without the hold-out the outlier inflates the variance and
  the second one no longer looks like a change: the draft's simulation showed a
  cost step then oscillating between 2.5 and 4.5 slots (§4.3).
- **Trust.** An estimate is trusted once it has 60 completions since its start
  and its relative standard error, `sd / (mean × √n_eff)`, is at most
  `(1 − ρ)/2`: two standard errors fit inside the margin the target leaves.
  Until then increases are limited (§7).

### 3.6 Memory per reconcile

Δheap between samples is mostly garbage: Go's heap goal is
`live + (live + roots) × GOGC/100` (Go GC guide, via the survey), so a raw heap
delta can be off by the whole live heap. The live heap is measured only at the
end of a GC, so that is when memory is observed:

- **An observation** is a sample in which `/gc/cycles/total` rose. It records
  `live` (`/gc/heap/live:bytes`) and pairs it with `N⁻`, the fewest reconciles
  in flight during that sample. The pairing is deliberately low. The GC ended
  at an unknown moment inside the sample, and admissions since then must not be
  counted as already in the heap, or the room left would be overstated.
  (A GC notification would pair exactly, but `runtime.AddCleanup` runs its
  function "some time after" the object is unreachable, possibly "arbitrarily far
  into the future", `GOROOT/src/runtime/mcleanup.go`, so it can't.)
- **The baseline** `Û₀` is the lowest `live` seen at an observation from a sample
  with nothing in flight at any moment, 0 until one is seen. It is informer
  caches and runtime state.
- **Per reconcile**, `m̂` is the smoothed `(live − Û₀)/N⁻` over observations with
  `N⁻ ≥ 1` (§3.5's rules, weighted by `N⁻`). A low `Û₀` and a low `N⁻` both push
  `m̂` up, so `m̂ ≥ m`, the true mean, by construction. For memory that bias is the
  point: §4.4 shows it makes the approach to the limit come from below.
- **Before the first observation** with `N⁻ ≥ 1`, `m̂` is the heap allocated per
  completed reconcile, `Δ/gc/heap/allocs:bytes / C`. A reconcile can't keep more
  than it allocated, and everything else allocating in the sample makes the
  figure larger still, so it is an upper bound, available after the first
  completion. It is replaced by the GC-based estimate after three observations,
  the fewest that give a variance.

The survey suggests a regression of `live` on in-flight count, whose intercept is
the informer caches. The headroom form of §4.4 doesn't need the intercept at all,
and a regression is unbiased, which for memory is the wrong side to err on. It
also needs variation in the in-flight count, which a full set of slots doesn't
give.

## 4. The formula

### 4.1 The budget

CPU is a rate. The limit allows `ρL − B̂` cores to reconciles, so a throughput of
`X* = (ρL − B̂)/c` completions a second, and Little's law turns that into slots,
`S = X* × D`. The whole question is which `D`.

### 4.2 Which duration

Three candidates, checked against the measured curve:

1. **The live duration, `D` as measured now.** Past the knee, throttling
   stretches `D` in proportion to the reconciles in flight, so `X* × D` returns
   about what is already running. With `ρ = 1` it returns exactly that: the
   survey's ratchet, every concurrency above the knee a fixed point (it computes
   `0.25 × 1.28 / 0.02 = 16`). With `ρ < 1` the restoring force is there but
   weak: at the cap `S' = θ × S`, where `θ = (ρL − B̂)/(L − B̂)`, 0.9 here. From
   16, the raw law needs 14 samples to reach 4.6; smoothed over `τ = 10 s` it is
   still at 4.9 after 110 seconds. In practice, a ratchet.
2. **The uncontended duration, `R₀`** (the survey's choice: off-CPU wait, a low
   percentile, or low-load windows). It has no ratchet, but at small quotas it
   undershoots. `D` already rises 10% between one slot and the target, because
   bursts throttle single periods long before the mean reaches the limit (§1).
   `0.9 × 0.25 × 0.429 / 0.0231 = 4.18` slots, which the measured curve puts at
   210 ms/s, 84% of the limit; at four whole slots, 81%. And a feed-forward
   formula has no correction for its own estimator's bias: the naive
   `c + B/X` lowers it further.
3. **Both, each where it is valid** (chosen). In an active sample the live
   duration is a measurement of the plant at its current point, and the law is

   `S_cpu = (ρL − B̂) / k̂`, with `k̂` the smoothed CPU per slot-second.

   This is `X* × D` with the live `D`; its fixed point is exactly "total CPU =
   `ρL`" (§4.3), including the throttling stretch at the target. In a capped
   sample the live duration measures the cap, so it is set aside:

   `S_cpu = min( (ρL − B̂)/cᵢ × D̂ , θ × N̄ᵢ )`

   where `cᵢ = (ΔUᵢ − B̂Δtᵢ)/Cᵢ` is this sample's CPU per completion (true at
   the cap), and `D̂` the duration smoothed over active samples, frozen while
   capped. The first term is the survey's uncontended-duration formula with the
   cost of now; it lands near the target, below it by the throttling stretch
   when the cost per completion is the same at the cap as below it, above it
   when the cap's is lower (here by 5%, §4.3). The
   second is the largest `S` the sample's evidence allows: at the cap,
   `U(N̄) ≥ L`, so with throughput concave in concurrency (A2 below),
   `U(θN̄) ≥ ρL`, and the target lies at or below `θN̄`. It guarantees a descent
   when `D̂` is stale. With `Cᵢ = 0` only the second term applies.

### 4.3 Convergence, and why it doesn't oscillate

Assumptions, for a stationary workload with a backlog (`N = S`):

- **A1.** `D(S)` is non-decreasing: more in flight never makes the mean
  reconcile faster.
- **A2.** Throughput `X(S)` is non-decreasing and concave with `X(0) = 0`:
  diminishing returns, never fewer completions for more concurrency. The table
  of §1 is concave from 1 to 16. It is the closed-system behaviour of the
  asymptotic bounds (Lazowska et al., QSP ch. 5, via the survey). It fails under
  memory thrash or lock collapse, which the memory input and the floor bound.
- **A3.** `U(S) = B + c X(S)`, and `B̂ ≤ B`.
- A target is reachable: `B < ρL < U(ceiling)`. Otherwise the law sits at the
  floor or at the ceiling, which is the right answer for those cases.

Let `S*` be the point where `U(S*) = ρL`, and `g(S) = (ρL − B̂) S / (U(S) − B̂)`,
the active-sample law with unsmoothed estimates.

- **The fixed point is the target, whatever `B̂`.** `g(S) = S ⟺ U(S) = ρL`. A
  wrong background moves the path, not the destination.
- **Below the target, it rises and never passes it.** For `S < S*`:
  `g(S) ≥ S` because `U(S) ≤ ρL`. And `g(S) ≤ S*`: concavity gives
  `X(S)/S ≥ X(S*)/S*`, and with `b = B − B̂ ≥ 0`,
  `b + cX(S) ≥ (b + cX(S*)) S/S* = (ρL − B̂) S/S*`.
- **Above it, it falls and never passes it** (active samples): the same two
  inequalities reversed.
- So `S*` attracts every start, the sequence is monotone, and it never crosses
  the target: no oscillation. Near `S*`, `g'(S*) = 1 − ε`, where `ε` is the
  elasticity of the CPU spent on reconciles with respect to `S`, in `[0, 1]`:
  one step in the linear region, slower near the knee.
- **Smoothing keeps both properties.** `k(S) = (U(S) − B̂)/S` is non-increasing
  in `S` under A2–A3, and `k̂` is a weighted mean of past `k(Sⱼ)`. If every past
  `Sⱼ ≤ S*`, then `k̂ ≥ k(S*)`, so the next `S ≤ S*`; and the newest term is the
  smallest, so `k̂` falls and `S` rises. By induction, monotone from below.
- **Capped samples.** `θN̄ ≥ S*` by A2, so the second term never undershoots.
  The first can land on either side. Above, the next samples are active, or
  capped again with a lower `N̄`, and fall monotonically. Below, every sample is
  active and the rise is monotone. So a capped sample crosses the target at most
  once, towards less CPU, the safe side.
- **If `B̂ > B` by `δ`** the bound fails and the step overshoots by at most a
  fraction `δ/(ρL − B̂)` of the error, which then decays. Here `δ` is at most a
  couple of ms/s against 225.

**The worked example**, on the measured curve of §1 (interpolated, `B̂ = 0`):

- Raw law from one slot: 1 → 4.17 → 4.47 → 4.55 → 4.56. `g'(S*) = 0.21`, so
  each step removes about 80% of what is left.
- Smoothed (`τ = 10 s`) from one slot: 4.17, 4.41, 4.47, 4.49, 4.50, … 4.54 at
  15s. Monotone, never above 4.56.
- The rule for capped samples, from 16 with the estimates of a previous visit at
  the target (`D̂ = 0.473 s`): `225 / 22.0 × 0.473 = 4.83` in one sample, then
  4.57 and 4.56. It lands a little above the target, not below, because the CPU
  per completion at the cap (250/11.35 = 22.0ms) is 5% under the 23.1ms below
  it; the active law then comes down from above, monotonically.

**A fluid model with the full rules**, fitted to the measured `X(S)`
(`X = x₀/(1 + (x₀/x_max)^7)^(1/7)`, `x₀ = S/0.444 s`, `x_max = L/22 ms`,
22ms being the CPU per completion at the cap, `250/11.35`), settles at 4.9
slots rather than 4.56 because its cost is the cap's 22ms rather than the 23.1ms
below it. Its dynamics, sample by sample:

| Scenario at t = 30s | Slots after | CPU / limit after |
|---|---|---|
| start from one slot | 3.25, 4.56, 4.69, … 4.91 at 20s | 0.20, 0.64, 0.85, … 0.89 |
| CPU per reconcile doubles | 2.47 in one sample, then flat | 0.999 once, then 0.896 |
| CPU limit halves, 250m → 125m | 2.47 in one sample, then flat | 0.999 once, then 0.896 |
| CPU limit doubles, 250m → 500m | 9.88 in one sample | 0.49 once, then 0.896 |
| off-CPU wait doubles | 4.94 once, then 9.09, 9.42, … 9.76 at t = 41s | 0.49, 0.49, 0.85, … 0.89 |
| 10% Gaussian noise on every sample's CPU | mean 4.86, sd 0.14, range 4.1–5.1 | mean 0.887 |

These figures came from Python scratch scripts written for the first draft and
never committed. They are superseded by the Go simulation of §10, which drives
the real estimator and law; where the two disagree, the Go simulation is right
and this table is corrected. The wait-doubling row is the survey's caution: when
a downstream slows, the CPU model admits more, correctly for CPU. §8 says what
stops it from piling onto a struggling server, and what doesn't.

### 4.4 Memory

With a Go memory limit `G`, the GC holds the heap under `G` by collecting more
often; what it can't do is free live memory. The budget for the live heap is
`H = (1 − r) G − nonheap`, `nonheap` being #48's non-heap runtime memory
(`/memory/classes/total` minus released, objects, unused and free heap). `r`
leaves the GC room between the live heap and the limit. At each observation:

`S_mem = N⁻ + (H − live) / m̂`

the reconciles running at the GC plus as many more as the room left holds. With
no Go limit but a `memory.max` of `M` (for example `GOMEMLIMIT=off`), the peak
heap is about `live × (1 + GOGC/100)`, so `H = ((1 − r) M − nonheap)/(1 + GOGC/100)`.
This replaces the working-set ratios.

**Convergence.** With `live = U₀ + mN` (the mean per reconcile in flight) and
`N = S`: `S' = S + (H − U₀ − mS)/m̂`, slope `1 − m/m̂`. Because `m̂ ≥ m` (§3.6),
the slope is in `[0, 1)`: monotone, from below, never past `H`. The low pairing
leaves `live` short of `H` by about `m̂ × (N − N⁻)`, typically one reconcile's
memory; that is the price of not counting admissions as if they had happened
before the GC. Unlike the CPU law, nothing here depends on duration, and between
two GCs `S_mem` doesn't change, so new admissions are paced by the GCs that
measure them.

**When the GC CPU limiter has engaged** since the last sample
(`/gc/limiter/last-enabled:gc-cycle` changed), the GC is already losing; the room
is treated as zero, `S_mem ≤ N⁻`, until a GC shows room again. It is the earliest
memory alarm the runtime gives (the survey; the metric is in `doc.go:233`).

**What it can't see:** memory outside the Go runtime (cgo, mmap'd files), and a
burst of reconciles that all reach their peak at once when their mean is lower
(`m̂` is a mean over phases). Both are what `r` and the gap between `G` and
`memory.max` are for. `G` is 0.8 of `memory.max` when the runtime sets it
(#48), so with `r = 0.15` the live heap is held to about 68% of the container's
limit.

**The worked example**, #48's ramp workload: 16 MiB held 1s per Observe, 256Mi,
`G = 214748364` bytes (204.8 MiB). Idle live heap 2 MiB
(`results/b2-ramp-new-1.log`). `H ≈ 0.85 × 204.8 − ~8 ≈ 166 MiB` (non-heap
estimated from the log's mapped figures, not measured), `m ≈ 16 MiB`, so
`S_mem ≈ 10`. #48 ran at a mean local capacity of 8 and 6.2 calls in flight, at
6.12 Observes/s, 20 of 20 clean. This is a prediction, and the memory bar of §10
is clean runs, not this number.

### 4.5 Slots

`S = clamp(min(S_cpu, S_mem), 1, ceiling)`, kept as a real number. The floor of
one is decided (design-rate-limit-governor §11 item 13): one reconcile always
runs, so deletions finish. The ceiling is `MaxConcurrentReconciles` × the
reconcilers on the Governor, as today. The gate needs a whole number, and at
these sizes rounding is not a detail: on the measured curve, four slots use 81%
of the limit and five 97%.

**Whole slots by error diffusion** (decided, §13 item 1). The sampler keeps a carry
`e`, owned by the sampler goroutine, starting at 0. Each sample:

```
capacity = clamp(⌊S + e⌋, 1, ceiling)
e        = S + e − ⌊S + e⌋          // the remainder, always in [0, 1)
```

So the capacity is `⌊S⌋` or `⌊S⌋ + 1`, and over `n` samples the published
capacities sum to `ΣS + e₀ − eₙ`: their mean is within `1/n` of the mean of
`S`. From `e = 0` at 4.56 it reads 4, 5, 4, 5, 4, 5, 4, 5, 5, … and averages
4.56. This is quantisation,
not oscillation: `S` itself is monotone (§4.3), and the capacity never leaves the
two whole numbers either side of it.

**The carry resets to 0** before that sample's capacity is computed, so the
capacity is `⌊S⌋`:

- **when a limit changes**: `L` (a new `cpu.max`), `G` (a new Go memory limit),
  or `memory.max` when it sets the budget. The carry is the remainder of an
  average under the old budget, and carried into a halved limit it could add a
  whole slot, 20% of the CPU here, in the first sample. Resetting rounds that
  sample down, the safe side;
- **when `S` is clamped** at the floor or the ceiling: nothing is owed then, and a
  carry left from those samples would add a slot the moment the clamp lifts;
- **when the binding input changes** between CPU and memory: the carry belongs to
  the other input's value;
- **when an estimate restarts** (§3.5): the value it averaged is gone.

A pressure-held sample (§6) keeps the carry: the held `S` is still the value to
average. A lower capacity lets running reconciles keep their slots, as any cut
does (§8), and the books measure what actually ran, so the estimates see the
true mean in flight, not the published one. The carry is exported (§9) and
logged by the harness (§10).

## 5. No limit set

- **`cpu.max` is `max`:** no CPU cap. `S_cpu` is the ceiling, there are no capped
  samples, and the CPU input is reported disabled with the reason, once at Info,
  as today. Pressure still applies (§6): with no quota, a stall is contention on
  the node, the case where holding back helps. The warm-up of §7 still applies.
- **A limit only on an ancestor cgroup** isn't visible: the throttling counters
  are non-hierarchical, and with a private cgroup namespace (Kubernetes on
  cgroup v2) the ancestors are outside the container's view. It reads as no
  limit. Kubernetes writes the container's own `cpu.max` when the container has
  a limit.
- **No `memory.max` and no Go memory limit:** no memory cap. **A Go memory limit
  without `memory.max`** (set through `GOMEMLIMIT` or `GoMemoryLimit`): the
  budget is against it, the operator's own declaration. **`memory.max` with the
  Go limit off:** the `GOGC` form of §4.4. **`GOGC=off` with no Go limit:** the GC
  never runs, there is nothing to observe, and the memory input is disabled with
  that reason.
- **Neither limit:** slots rise to the ceiling through the warm-up, and only
  pressure can hold them.
- **A limit that changes** (in-place resize): `cpu.max` is re-read every sample,
  so `L` follows at once, and the fluid model goes from 4.94 to 9.88 slots in
  one sample when 250m becomes 500m. The Go memory limit the runtime sets is set
  once, at `Start`, as #48 decided; `G` itself is re-read every sample, so a
  limit the program changes is followed.

## 6. Pressure

**What it does: it blocks increases, and nothing else.** When the CPU stall
share of the sample, `P = Δ(some total)/Δt` as a percentage, is over
`CPUPressureThreshold`, the slots don't rise in that sample:
`Sᵢ = min(Sᵢ₋₁, S_model,ᵢ)`. A decrease the model computes still applies. Pressure
never lowers the slots by itself.

**Why the counter, not `avg10`.** `avg10` is an average with a time constant of
about 10s, recomputed every 2s (the survey, from `kernel/sched/psi.c`; §14). That
lag is half of what went wrong in §1. `total` differenced over the sample has
the sample's own resolution, and the kernel documents it for exactly this, "to
allow detection of latency spikes which wouldn't necessarily make a dent in the
time averages" (`psi.rst`, quoted in the survey).

**Why it doesn't bring the mechanical behaviour back.** It has no step size, no
hold-off and no recovery rule. A blocked sample keeps a value the model computed
earlier; the next unblocked sample takes the model's value again. A noisy
sensor costs at worst a held rise, never a cut, so a 1s stall share that is
jumpier than `avg10` does no harm.

**Why block and not cut.** The model already lowers slots whenever the CPU it is
budgeting goes over target, capped or not. A stall the model doesn't explain,
with CPU under target, comes from outside its budget: other pods on the node.
There, more slots buy no CPU, so blocking stops the one harmful thing, the climb.
Cutting would need a size and a cooldown, the fixed constants the user ruled out
(the survey proposes one ÷1.2 cut and a 15s cooldown; §12 and §13 item 3). The
evidence for not needing a cut: at the cap, with pressure 56 and the
incident-provider liveness probe, the off arm had no failed probe in 5 runs.

**The threshold stays 50, provisional.** It now reads a 1s stall share instead of
`avg10`. Measured `avg10` (§1): 8–17 at four or five slots, 37 at six, 56 at the
cap. So for blocking, 50 separates "at the cap" from the operating point. The
survey's point that a fixed threshold doesn't fit small quotas applies to a cut:
a block at the operating point costs nothing. The 1s stall share's spread at the
operating point isn't measured; §10 records it.

## 7. How fast slots may change

No fixed step, no hold-off, no halving. Every limit on a change comes from the
data:

- **Decreases** take effect in the sample that computes them, to the computed
  value.
- **Increases, once trusted** (§3.5), also go straight to the computed value:
  the estimate is already smoothed, and the law can't pass the target (§4.3).
  The fluid model doubles in one sample when the limit doubles, because the new
  limit is known exactly.
- **Increases, while not trusted** (at start, and after a restart): at most one
  more slot per reconcile that ended in the sample, `Sᵢ ≤ Sᵢ₋₁ + Cᵢ`. Each
  completion is one more measurement; this is slow start paced by data, doubling
  at most once per reconcile duration. It replaces `StartupRampInterval`: here
  one to 16 would take three samples (each second ends about 2.3 reconciles per
  slot), reconciles that last 30s would take minutes, and in both cases the pace
  is the evidence's. In this workload the model's own value, about 4.6, binds
  first.
- **Memory increases** are bounded by the room the last GC showed, and don't
  move between GCs (§4.4).
- **Capped samples** never raise (§4.2), and **pressure** blocks rises (§6).

## 8. Interactions

- **The gated queue** is unchanged. The sampler publishes the whole-number
  capacity through `g.local` and calls `dispatchLocked`, as `sample` does today.
  A cut lets running reconciles keep their slots; `A` measures what actually
  runs, so the estimates stay consistent while in flight exceeds capacity.
- **Per-key capacity** stays a separate gate after the pop, and a reconcile
  needs both, so the effective bound is the smaller. A key deferral is a cheap
  reconcile: it barely moves `A` or `ΔU`, so it barely moves `k̂` (§3.3). When
  keys throttle everything and the controller goes quiet, its samples turn idle
  and update only `B̂`; the slot estimate waits, and doesn't drift.
- **Several controllers on one Governor:** the slots are process-wide, and `k̂`
  is the slot-time-weighted mix of their costs. Round-robin grants keep the mix
  steady; a shift in it is a cost change (§3.5). Charging each controller its own
  cost would need cost-weighted grants instead of slots; not in this design.
- **Several Governors in one process:** each sees the whole cgroup's CPU against
  only its own slot-seconds, so each overestimates its cost in proportion, and in
  the fluid model their total still settles at `ρL`. The memory form composes the
  same way. The existing warning stays: nothing about two estimators sharing one
  budget is tested. The Go memory limit is process-wide and set once.
- **Scale-to-zero:** a woken controller is a new process with no estimates. It
  warms up from one slot (§7), and its idle seconds before the first object give
  `B̂`. Estimates aren't persisted: the next process may have another limit,
  node or workload.
- **A downstream that slows** raises the off-CPU wait, and the CPU law admits
  more (§4.3's table), which is right for CPU. What stops it from adding load to
  a struggling server is the per-key input, on every server that signals (429,
  `Retry-After`, the `X-RateLimit` headers, a 503 with `Retry-After`), and the
  ceiling. **A limitation, decided (§13 item 7):** a server that slows without
  saying so isn't covered. Its longer waits raise the slots, up to the ceiling,
  and so the load on it. A lowering-only downstream signal is
  provider-runtime#50, a follow-up, not part of this change.
- **An HPA on CPU:** holding CPU at `ρ × limit` keeps it from scaling out if its
  target, in limit terms, is above `ρ`. Either `ρ` sits above the HPA target, or
  the HPA scales on queue depth (AWS Builders' Library, via the survey). Whether
  any Krateo provider runs an HPA isn't checked here.
- **Leader election:** the sampler runs on every replica
  (`NeedLeaderElection` is false); a standby is idle and measures only `B̂`.

## 9. Options, API and metrics

`SaturationOptions` after the change:

| Field | Default | Change |
|---|---|---|
| `Enabled` | off | unchanged: turns on the CPU input |
| `Memory` | off, even with `Enabled` | new name for #48's `ExperimentalMemory`; turns on the memory input and lets `Start` set the Go memory limit (§13 item 5). Read only with `Enabled` |
| `GoMemoryLimit` | 0 = 0.8 × `memory.max` | #48's `ExperimentalGoMemoryLimit`, same rules: `GOMEMLIMIT` or a limit the program set wins |
| `CPUTarget` | 0.9 | new: `ρ` |
| `MemoryReserve` | 0.15 | new: `r`, replaces #48's `ExperimentalGoMemoryHeadroomRatio` (its measured 15%), provisional |
| `CPUPressureThreshold` | 50 | kept, provisional; now blocks rises, read from `total` |
| `CgroupDir`, `SamplePeriod` | own cgroup, 1s | unchanged |
| `StartupRampInterval` | — | **removed**: the warm-up is paced by completions (§7) |
| `ExperimentalMemoryWorkingSet`, `WorkingSetSoftRatio`, `WorkingSetHardRatio` | — | **removed**: replaced by the memory input of §4.4 |

`RecoveryInterval` no longer touches local capacity; it still governs keys. The
constants for the removed fields, and `InputMemoryWorkingSet`, go too.

**Removal, not deprecation** (decided, §13 item 6). Saturation is opt-in and off by
default. A field kept but ignored would change behaviour silently; removing it
fails the build of any consumer that sets it, which says exactly what changed.
The release that builds this marks it in [log](./log.md) as "Breaking, for an
off-by-default feature", as the gated-queue change (#40) did; the log records the
decision now. #48's `ExperimentalMemory`, `ExperimentalGoMemoryLimit` and
`ExperimentalGoMemoryHeadroomRatio` were never released, so they are renamed,
not removed. The example sets `ExperimentalMemoryWorkingSet` from a flag
(`examples/greeting-controller/main.go:205`) and changes with it. Which
consumers set the removed fields isn't checked here; the implementation scans
them under both module paths before the release.

**Metrics.** Kept: `provider_runtime.ratelimit.local.capacity` (the published
whole number) and the input gauges, with `memory_working_set` gone and
`cpu_pressure` now the 1s stall share. New, all with saturation on only:

| Gauge | Unit | Value |
|---|---|---|
| `…local.slots{input}` | slots | `S_cpu`, `S_mem` and the combined `S`, before rounding; `input` is `cpu`, `memory`, `combined` |
| `…local.carry` | slots | the error-diffusion carry `e` (§4.5) |
| `…local.binding{input}` | 0/1 | which input set `S` |
| `…local.trusted{input}` | 0/1 | §3.5's trust |
| `…local.cpu.limit`, `…cpu.used`, `…cpu.background` | cores | `L`, `ΔU/Δt`, `B̂` |
| `…local.cpu.per_reconcile` | seconds | `ĉ` |
| `…local.reconcile.duration` | seconds | `D̂` |
| `…local.reconcile.throughput` | 1/s | completions per second, smoothed |
| `…local.in_flight` | reconciles | `N̄` |
| `…local.cpu.capped` | 0/1 | last sample capped |
| `…local.memory.budget`, `…memory.live`, `…memory.baseline`, `…memory.per_reconcile` | bytes | `H`, `live`, `Û₀`, `m̂` |

and a counter `…local.pressure_blocked`, samples in which pressure held a rise.
`Snapshot` gains the same values. They are provider-runtime's own; core-provider's
runtime has a local capacity gauge of the same name (design-local-slot-before-pop
§6) and none of these.

## 10. Tests

**Unit, with a fake cgroup directory and a fake clock** (`testdata` already holds
cgroup fixtures): the sample kinds (idle, capped, active) from file deltas,
including a capped sample by counters but not by usage; `Δt` taken from the
clock, not the period; every formula of §3–§4 against hand-computed values;
`cpu.max` of `max`, a missing `cpu.stat` throttling block, a missing
`cpu.pressure`; counters that go backwards (treated as a restart of that input,
not a negative delta); `B̂ ≥ ρL` (floor), zero slot-seconds, zero completions;
the warm start, the 3σ hold-out and the restart; trust; the untrusted increase
bound `Sᵢ₋₁ + Cᵢ`; pressure blocking a rise and letting a fall through; error
diffusion averaging to the real value within `1/n`, its carry staying in
`[0, 1)`, and each reset of §4.5 (a new `cpu.max`, a new Go limit, a clamp, a
change of binding input, a restart) followed by `⌊S⌋`; a pressure-held sample
keeping the carry. Memory with a fake runtime-metrics reader
(#48's `goMemory` seam): observation only when the GC count rises, the `N⁻`
pairing, `Û₀` only from fully idle samples, the allocation prior and its
hand-over after three observations, the limiter alarm, `GOGC` without a Go limit,
`GOGC=off`.

**Simulation in Go,** superseding the Python scripts behind §4.3's figures, which
were never committed: a discrete-event model of the
measured workload (CFS quota per 100ms period, GOMAXPROCS 2, 23ms of CPU and
400ms of wait per reconcile, 16 workers, a backlog) driving the real estimator
and law through the fake clock. It must show, before any cluster run: settling
between 4 and 5 slots with mean CPU within 3 points of 90%; a monotone approach
from one slot; every row of §4.3's table, including no oscillation after a cost
step; and the memory workload settling at its budget from below with no sample
over `H`.

**GKE re-measure**, with the bars below fixed before run 1 (decided, §13 item 2).
The CPU block repeats `cpu2-settings.txt` exactly (250m, 512Mi, 16 workers, 64
Greetings, 1s poll, 360s, the probe), arms off and new, 5 runs each, rotating.

| Line | Bar |
|---|---|
| Validity, checked per run, never used to drop one | off arm at ≥ 225 ms/s with pressure over 50; burn 20.0–20.5 ms per call |
| CPU used, new arm, t ≥ 60s, each run | 85–95% of the limit |
| Throughput, new arm, median of 5 | ≥ 80% of the off arm's (predicted 85%) |
| Pressure, new arm, steady state | rises blocked in at most 10% of samples; the distribution of the 1s stall share reported |
| Restarts and failed probes, every arm | 0 |

Reported, not bars: the mean slots (predicted 4.56; the survey's band is 4 to
6), the mean reconcile duration (predicted 0.47s; off 1.41s), and the capacity's
alternation between 4 and 5.

**The harness changes first**, in its own commit, before any run:

- **Rates over measured intervals.** Each `STAT` line takes its interval from the
  monotonic clock between two reads (`time.Since` the previous one), prints it
  as `dt`, and divides every delta by it: CPU from `usage_usec`, Observes,
  burns, throttled time. Today it divides by a nominal second (§3.1).
- **The model's values, on every line**, from `Snapshot`: `S_cpu`, `S_mem`, `S`,
  the carry `e` and the published capacity; `k̂`, `ĉ`, `D̂`, `B̂`, the throughput
  and the mean in flight; the sample's kind (idle, capped, active), with
  `Δnr_periods` and `Δnr_throttled`; trust and restarts per estimate; the 1s
  stall share and whether it blocked a rise; and, with memory on, `H`, `live`,
  `Û₀`, `m̂`, `N⁻` and whether the sample held a GC observation. The report
  script computes the bars from these same lines.

The memory block repeats #48's (`results/block2-settings.txt`): ramp (16 MiB held
1s, 500ms poll, 2 Greetings every 5s, 256Mi, 2 CPU, 240s) and cold start (64
Greetings before start, 16 MiB held 300ms), ABBA, with `Memory` on in the new
arm, and #48's bar (design-rate-limit-governor on #48, §11 item 16): clean in at
least 18 of 20 ramp runs, no crash loop in at least 9 of 10 cold starts, no
OOMKill in either, restarts split by cause, throughput not below the control's
clean runs, the sampler's cost reported. The real-provider line stays open until
git-provider's rollout resumes.

**The control arm is #48's measured build, `new3`** (decided, §13 item 8), recorded
in `results/image-new3.txt` as commit `6d76a36bcff5e2e0d45d56fecf96f023e7094c63`
and digest
`sha256:ceb79357975eed7fc20b4eec005a83d546e8910d953642fd54a1e150492de58d`.

- **The image used is the recorded tarball,** `images/measure-new3.tar` in the
  measuring worktree, not a rebuild. Before the block, and again before it is
  loaded on the cluster, its digest is re-read with `crane digest --tarball` and
  compared with `results/image-new3.txt`. A mismatch stops the block. It read
  `sha256:ceb79357…92de58d`, matching, on 2026-11-08.
- **Provenance check, reported but not blocking.** `6d76a36` is rebuilt with
  `build.sh new new3 --local` from a checkout at the measuring worktree's path,
  with the toolchain the tarball's binary records (`go version -m` on it), and the
  SHA-256 of the rebuilt `measure` binary is compared with that of `/measure`
  extracted from the tarball's appended layer. The report says whether they
  match. A mismatch is reported and doesn't stop the block: the tarball is the
  build that was measured, whatever a rebuild produces.
- **Why the binary, not the image.** A rebuilt image's digest can't be expected to
  match: `build.sh` packs the binary with a plain `tar -cf`, which records its
  modification time, onto `alpine:3.20`, a tag that moves. The Go binary can
  match, but only from the same source path and toolchain, since the build
  doesn't use `-trimpath`.
- **The source is kept.** `6d76a36` was on no branch; it is now held by the local
  branch `measure-new3-source`, so garbage collection can't remove it. That
  branch isn't pushed.

**For the independent reviewer**, who gets the code and not this conclusion.
Attack:

1. The slot-seconds books. Every change of `localInFlight` (grant, `Done`,
   hand-back, shutdown) accrues under `g.mu` with one clock; the sampler's read
   and reset can't lose or double an interval at the boundary; a hand-back isn't
   a completion. A model test compares `A` with an independent integral under
   `-race` while the sampler runs at 1ms.
2. `N⁻` reset racing a release, and whether it can read higher than the true
   minimum.
3. Publishing a lower capacity while grants are in progress; error diffusion
   flipping capacity every sample; a stress test with flapping capacity and a
   drain test at shutdown, per the lesson of the gated queue (§10 of that
   design: the races were found by the reviewer's tests, not the builder's).
4. The sampler delayed for seconds by throttling: one long `Δt` must not dominate
   the EWMA (`α` comes from `Δt`), and rates must not spike.
5. NaN, Inf and negatives on every division; `math.MaxInt64` limits; counters
   going backwards; a cgroup path that changes.
6. The process-wide Go memory limit: `Start` on two Governors, and a program
   calling `debug.SetMemoryLimit` concurrently.

Each new behaviour gets a revert check, as before.

## 11. Alternatives rejected

- **Utilization feedback**, a PI or PID controller on CPU against a target, as
  Temporal's resource-based slot supplier does (the survey: two PID loops on
  cgroup v2, CPU target 0.9, memory 0.8 as a hard stop). Its gains have to match
  the plant's gain, `dU/dS = c/D`, which differs by orders of magnitude between a
  20ms-CPU, 0.4s reconcile and a 200ms-CPU, 30s one, so gains tuned on one
  provider are wrong on the next, and the clamps at 1 and the ceiling need
  anti-windup. Temporal's own guidance recommends fixed slots for most workloads.
  The active-sample law is a feedback law in form, `S' = (ρL − B̂) N̄/(U − B̂)`,
  but its gain is the measured inverse of the plant's, not a tuned constant,
  which is why it settles in about one step in the linear region and can't
  cross the target (§4.3).
- **A latency gradient** (Netflix Gradient2, Envoy's adaptive concurrency, TCP
  Vegas). A reconcile's duration is mostly downstream wait, which moves for
  reasons the container's CPU doesn't cause; the per-key input already answers
  those servers. The minimum-latency baseline has to be re-probed at low
  concurrency, which Envoy does by pinning the limit to 3 and accepting 503s,
  a mechanical probe. It mixes Kinds of very different cost, which Envoy's
  maintainer rules out, and it sees nothing of memory.
- **The mechanical step** (today's): measured at 71% of the limit and 31% less
  throughput (§1). It halves on a lagging average and recovers on a timer, and
  knows nothing of where the knee is.
- **The uncontended duration alone** (§4.2, option 2): no ratchet, but 84% of
  the limit here and no correction for estimator bias. Kept, for capped samples
  only.
- **A pressure veto that cuts** (the survey): §6, and §13 item 3.
- **Regression for CPU per reconcile**: §3.3. Not needed for the setpoint, and
  confounded at the operating point.

## 12. Reconciled with the survey

| The survey | This design |
|---|---|
| Compute the knee, let pressure only veto | Adopted, with "veto" as a block on rises (§6) |
| A wall-clock duration under load is a ratchet; use the uncontended one | Agreed that the live duration alone is a ratchet in practice: with `ρ < 1` it contracts by `θ` per sample, too slowly once smoothed (§4.2). Measured against the uncontended one: it undershoots by the period-level throttling stretch at small quotas, 84% instead of 90%. So the live duration in active samples, where its fixed point is exact, and the uncontended formula in capped samples |
| CPU per reconcile by regression with a background intercept, ≥ 60 completions per window | The setpoint is independent of the background estimate (§4.3), and the regression is confounded at the operating point; background from idle samples. The 60-completion window is adopted in `τ` and trust (§3.5). On this workload the regression gives 23.1–23.3ms and a background within noise of zero |
| Memory from the post-GC live heap, regressed on in-flight; a hard ceiling with hysteresis | Post-GC live heap adopted; the headroom form needs no intercept, and an estimator biased upward replaces the regression (§3.6). "Fast decrease, slow increase" is met by data rather than a band: decreases at once, increases only by the room the last GC showed (§4.4) |
| Two timescales: a ~10s cost-model tick with forgetting over tens of minutes and a step cap; a fast PSI loop that cuts once with a cooldown | Two timescales kept as estimates (`τ ≥ 10s`) and a 1s law; no fixed step cap and no cut, which the user ruled out; the law uses slot-seconds, which accrue continuously, so the completion-timing hazard behind a long tick doesn't arise (§3.4). Forgetting over tens of minutes would leave a cost change unlearned for that long; the restart handles changes instead (§3.5) |
| PSI from the `total` delta, not `avg10` | Adopted (§6) |
| Calibrate PSI at the operating point; 50 doesn't fit small quotas | True for a cut. For a block, 50 sits above the measured operating point (8–17) and below the cap (56); kept provisional, distribution recorded (§6, §10) |
| A downstream signal that can only lower | The per-key input is that signal for servers that signal; a silent slowdown isn't covered, a documented limitation, and provider-runtime#50 is the follow-up (§8, §13 item 7) |
| HPA note | §8 |
| Validate on GKE: 4–6 slots, CPU near 90% | §10's bars; predicted 4.56 slots and 90%; whole slots give 81% or 97%, so the capacity alternates 4 and 5 by error diffusion (§4.5, §13 item 1) |
| Prior art: Temporal | §11: same targets (CPU 0.9, memory as a hard stop), but feedback with tuned gains |
| Prior art: Liu et al. 2003, MaxClients | The same structure, as the survey says: predict each resource's saturation from per-unit cost, take the binding one, re-estimate on drift. This design adds the background, which Liu's zero-intercept assumption omits, and the capped rule |
| The survey's 4.7 slots and ~11/s at 90% | 4.18 by the uncontended formula and 4.56 on the measured curve, at 9.65/s: the survey used 20ms of CPU per reconcile; the measured cost is 23.1ms |

## 13. Decided by the user, 2026-11-08

The first draft put these eight questions to the user, with options and a
recommendation each. The user decided all of them, with the coordinator
deciding item 8.

1. **Whole slots: error diffusion** (§4.5). On the measured curve 4 slots use 81%
   of the limit and 5 use 97%, against a target of 4.56. Rounding down would hold
   CPU at 81%, and rounding to nearest at 97%, a target meaning ±half a slot. With
   error diffusion the capacity alternates 4 and 5 and averages the computed
   value; §4.5 specifies the carry and when it resets.
2. **`CPUTarget` is 0.9, and the GKE bars are fixed before run 1** (§10): CPU
   85–95% of the limit, throughput at least 80% of unthrottled, no restarts, no
   failed probes. The brief's bars, CPU and throughput both at least 90%, don't
   hold together at 0.9: CPU is predicted at exactly 90%, which noise would fail
   half the time, and throughput at 85%. Throughput at 90% would have needed a
   target of about 0.95.
3. **Pressure only blocks increases. It never cuts** (§6). The survey's one cut
   with a cooldown would bring back a fixed step and a hold-off.
4. **Pressure's bar** (folded into 2): rises blocked in at most 10% of
   steady-state samples, with the distribution reported. "Pressure near the
   threshold" is dropped: the model targets CPU, and pressure at the operating
   point read 8–17 (`avg10`), well under 50.
5. **Memory is a separate `Memory` toggle, off by default even with saturation
   on** (§9). Turning it on sets the process-wide Go memory limit, which the
   governor design keeps behind an explicit choice (§7 there, and #48's §11 item
   14), and its real-provider line is still open.
6. **The options that no longer mean anything are removed**: `StartupRampInterval`,
   the working-set ratios and `ExperimentalMemoryWorkingSet` (§9). The release
   marks it Breaking in [log](./log.md), as #40 did.
7. **A downstream that slows without signals is not handled in this change.** It
   is documented as a limitation (§8): it raises concurrency, up to the ceiling.
   provider-runtime#50 is the follow-up for a downstream signal that can only
   lower.
8. **The memory block's control arm is #48's measured build, `new3`**, decided by
   the coordinator: the recorded tarball, used after its digest is re-checked
   against the record. A rebuild from `6d76a36` checks provenance by comparing
   the `measure` binary's hash with the tarball's, and its result is reported but
   doesn't block (§10). The first decision, a rebuild used only if its image
   digest matched, was replaced, because that digest can't be expected to match.

## 14. Checked, and not

**Checked against source here:** every `GOROOT/` and `CR/` reference in §2, §3.6
and §4.4; the code references to `governor.go` and `queue.go`; the kernel's
`cgroup-v2.rst` (`cpu.max`, `cpu.stat` keys, their presence and
non-hierarchy, `memory.max`), `sched-bwc.rst` (period, slices, the statistics'
one-line definitions) and `psi.rst` (format, `total` in µs); every figure of §1
and §4.3, recomputed from the committed logs and the first draft's scripts,
which §10's Go simulation supersedes; the recorded `new3` tarball's digest
(§10); provider-runtime#50 is open with that title.

**Not checked here:**

- Whether `nr_throttled` counts periods or per-CPU throttling events, and whether
  `nr_periods` counts idle periods. The kernel doc defines them in one line each.
  The capped test of §3.2 also requires usage within one period's quota of the
  cap, so a wrong reading of the counters can't make an uncapped sample look
  capped; the GKE run logs both per sample.
- That PSI's averages update every 2s with a 10s time constant, and that cgroup
  CPU pressure counts `cpu.max` throttling (kernel commit b05e75d6): from the
  survey. The measured pressure of 56 at the cap is consistent with the second.
- The Go GC guide's heap-goal formula, and every literature result (Spinner,
  Zhang, Lazowska, Hellerstein, Liu, Temporal, Envoy, Netflix): from the survey
  and its notes, which say which sources it read in full.
- A2's concavity in general. It holds on §1's curve; the closed-network
  argument is the survey's.
- Which consumers set the options §9 removes, and whether any Krateo provider
  runs an HPA: outside this repository.
- `/gc/gogc:percent`'s value under `GOGC=off`; the implementation reads it and
  tests it.
