---
type: Architecture
title: provider-runtime — size local capacity from what reconciles cost
description: Design, not built, for replacing the saturation input's halve-and-recover step with a per-reconcile cost model that computes how many reconciles fit in the container's CPU and memory limits, with CPU pressure as a safety check that only blocks increases.
resource: github.com/krateo-platformops/provider-runtime
tags: [design, rate-limiting, saturation, cgroup, gc]
timestamp: 2026-11-08T00:00:00Z
---

# Size local capacity from what reconciles cost

**Status:** design, revised after its first independent review (§15), for
re-review. Nothing is built. The open questions were decided by the user on
2026-11-08 (§13). It replaces the
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
| `N⁻ᵢ`, `N⁺ᵢ` | fewest and most in flight during the sample | count | the Governor's books |
| `A_q`, `C_q` | the same per queue (per controller) | | the Governor's books |
| `B̂` | background CPU, a lower bound | cores | idle and active samples, §3.3 |
| `L_eff` | effective CPU limit with no `cpu.max` | cores | learned at pressure onset, §5.1 |
| `â` | heap allocated per completed reconcile | bytes | `/gc/heap/allocs:bytes`, §3.6 |
| `G` | Go memory limit | bytes | `/gc/gomemlimit:bytes` |
| `r` | memory reserve, `MemoryReserve` | fraction of `G` | option, default 0.15 |

### 3.2 What a sample measures

The Governor keeps these beside `localInFlight`, all under `g.mu`, which every
grant and release already holds:

- `A`, the slot-seconds, and `A_q` per queue: the Governor knows which queue
  each grant went to (`dispatchLocked` grants to `g.queues[idx]`), so each
  queue's share of the slot-time is exact. The books keep the changes of
  `localInFlight` since the last sample, as (time, count, queue) entries in a
  slice reused from sample to sample.
- `C`, reconciles ended, and `C_q` per queue. `releaseLocal` gains a flag so a
  handed-back grant isn't counted.
- `N⁻` and `N⁺`, the fewest and most reconciles in flight during the sample, for
  memory (§3.6). They are updated at the end of each critical section that
  changes `localInFlight`, after its `dispatchLocked`, so a release whose slot is
  granted on in the same critical section isn't a dip: it lasts no time.

**One instant per sample.** The sampler reads `cpu.stat` and `cpu.pressure`,
takes the monotonic time `t_read`, reads the runtime metrics, then takes `g.mu`
and closes `A`, `A_q`, `C` and `N±` at `t_read`, not at the moment it gets the
lock: the entries after `t_read` are carried into the next sample. So the CPU
and the slot-time of a sample cover the same interval, whatever the wait for the
lock. No I/O happens under `g.mu`.

Each sample is one of four kinds, and only the kind it is updates anything:

- **Idle**: `A = 0` and `C = 0`. It measures the background, and nothing else.
- **Capped**: the cgroup used its quota in every period of the sample:
  `Δnr_throttled ≥ Δnr_periods ≥ 1`, and `ΔU ≥ L·Δt − slack` with
  `slack = min(L·PERIOD, P·q)`: the runtime one 5ms bandwidth slice `q` per CPU
  can leave unused on its runqueue (`sched-bwc.rst`; read from
  `/proc/sys/kernel/sched_cfs_bandwidth_slice_us` when readable), for the
  `P = GOMAXPROCS` threads that run Go code at once, and never more than one
  period's quota. At 250m that is 10ms in a 1s sample, 4%. A truly capped sample
  that a window boundary cuts short reads as active, which only slows the
  descent (§4.2); an uncapped one can't read as capped. Every sample logs `ΔU`,
  `L·Δt − slack` and both counter deltas at debug level, and the harness records
  them (§10). In a capped sample the CPU used says nothing about how much more
  the reconciles wanted: it is censored at `L`. Its durations measure the
  throttling, not the work.
- **Pressured**, only with no `cpu.max` and a learned effective limit held
  (§5): the stall share is over `CPUPressureThreshold`. It is censored at the
  CPU the node gave, as a capped sample is at the quota, and is treated like
  one.
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
3. **A lower bound from idle and active samples** (chosen):

   `B̂ = min( min over the last W idle samples of ΔU/Δt , min over the last W active samples of ΔU/Δt )`

   with `W = ⌈3τ/Δt⌉`, the samples that still weigh in the estimates (§3.5), and
   `B̂ = 0` while no idle sample is in the window. A controller is idle between
   its cache sync and its first reconcile, whenever its queues are empty, on a
   standby replica, and on a woken scale-to-zero Deployment before its first
   object.

The choice rests on §4.3: **the setpoint doesn't depend on `B̂`.** For any
`B̂` below `ρL` the law of §4.2 settles where the cgroup's total CPU is `ρL`;
`B̂` changes only how fast it gets there. An accurate per-reconcile cost matters
for a feed-forward formula, which has no way to correct its own bias; it doesn't
for this one. **What it must not do is overestimate.** The proof's condition is
`B̂ ≤ B`, the CPU with no reconcile running. The first draft smoothed idle
samples, and one burst there (a resync, a GC) could raise `B̂` above the CPU at
one slot; the independent review showed that made `k̂ ≤ 0` and pinned the slots
at the floor for good (§15, H1). A minimum can't be raised by a burst, so it is
at or below the background's typical value, and the second term keeps it at or
below every recent active sample, which makes `k̂ ≥ 0` by construction. The cost
of a minimum is a `B̂` biased low, which slows the approach (§4.3) but can't
move the setpoint or reverse the direction.

Why subtract anything: not subtracting (`B̂ = 0`, the plain ratio) is also safe,
but slow when the background is large. In the draft's fluid model, a 1-core
limit with 300 ms/s of background reached 9.8 of its 12.7 slots after 14
samples with `B̂ = 0`, and 12.4 after 4 with `B̂ = B`. In this workload the
regression puts the background at −0.8 to 0.5 ms/s, so `B̂ ≈ 0` either way; the
memory block's idle samples read 1–2 ms/s (`results/b2-ramp-new-1.log`,
t = 2–15).

**A mix of cheap and expensive reconciles** needs nothing separate. The law
uses the CPU per slot-second, `k = (ΔU − B̂Δt)/A`, so each reconcile weighs in
by the time it holds a slot: a key deferral that returns in a millisecond adds
almost nothing to `A` and almost nothing to `ΔU`. The mix is what the window
saw. When the mix changes, the estimate describes a workload that is gone, so a
shift in the queues' shares of the slot-time, `A_q / A`, restarts it like a
change in cost (§3.5). The CPU isn't attributable per queue, but the slot-time
is, exactly.

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
`D̂` is used only in capped samples (§4.2), and exported. So is its per-queue
form, `D̂_q = ΣA_q/ΣC_q` over the active samples of each queue, which keeps its
value across a restart of the shared estimates: a queue's reconciles take as
long uncontended whatever else is running.

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
- **Which samples feed what.** `k̂`, `D̂` and `D̂_q`: active samples only. `ĉ`:
  active and capped samples, because the cap censors the CPU wanted, not the CPU
  each completed reconcile used. `B̂`: §3.3's minimum.
- **Change.** Each estimate keeps a smoothed variance of its per-sample values.
  A sample more than 3σ from the mean is held out, not averaged in. Two in a
  row on the same side restart the estimate from that sample (`n = 1`). Under
  Gaussian noise a false restart has probability about `2 × 0.00135² ≈ 4×10⁻⁶`
  per sample, about once every three days at 1s, and costs only a few seconds of
  a noisier estimate. Without the hold-out the outlier inflates the variance and
  the second one no longer looks like a change: the draft's simulation showed a
  cost step then oscillating between 2.5 and 4.5 slots (§4.3).
- **A move in concurrency is not a change in cost.** `k` falls as `S` rises
  past the knee (§4.3), so right after a large move in the slots a sample's `k`
  differs from the mean without anything having changed. The test therefore
  skips a sample whose mean in flight `N̄ᵢ` differs from the previous sample's by
  more than 3 standard deviations of `N̄`'s own sample-to-sample variation;
  such a sample is still averaged in. A real cost change shows in the samples
  after, at a steady `N̄`.
- **A shift in the mix is a change.** The same 3σ, two-in-a-row test runs on
  each queue's share of the slot-time, `A_q/A`. A shift restarts `k̂`, `D̂` and
  `ĉ` (not `D̂_q`). With one queue there is nothing to test.
- **Trust.** An estimate is trusted once it has 60 completions since its start
  and its relative standard error, `sd / (mean × √n_eff)`, is at most
  `(1 − ρ)/2`: two standard errors fit inside the margin the target leaves. The
  standard error exists from the third sample; before that the estimate is
  untrusted. Until trusted, increases are limited (§7).

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
- **The allocation bound** `â` is the heap allocated per completed reconcile,
  `Δ/gc/heap/allocs:bytes / C`, smoothed by §3.5's rules. A reconcile can't keep
  more than it allocated, and everything else allocating in the sample makes the
  figure larger still, so `â ≥ m`, the true live memory per reconcile in flight.
  It exists from the first completion.
- **The baseline** `Û₀` is the largest lower bound the observations give:

  `Û₀ = max over the observations in the window of ( live − â·N⁺ )`, and ≥ 0

  where `N⁺` is the most reconciles in flight during that observation's sample.
  Since `live = U₀ + m·N` with `N ≤ N⁺` and `m ≤ â`, each term is at most `U₀`,
  the informer caches and runtime state, and so is their maximum. An observation
  with nothing in flight gives `live` itself, the exact value. The warm-up
  makes the bound tight early: it starts at one slot (§7), so the first
  observations have `N⁺` of 1 or 2 and bound `U₀` to within one or two `â` of
  its value. The window is the last `⌈3τ/Δt⌉` observations, because caches can
  shrink when objects are deleted, and an old maximum could then exceed the
  present `U₀`.
- **Per reconcile**, `m̂ = min( smoothed (live − Û₀)/N⁻ , â )`, the first over
  observations with `N⁻ ≥ 1` (§3.5's rules, weighted by `N⁻`). Both are upper
  bounds on `m`: the first because `Û₀ ≤ U₀` and `N⁻ ≤ N`, the second as above.
  So their minimum is too, and `m̂ ≥ m` by construction. For memory that bias is
  the point: §4.4 shows it makes the approach to the limit come from below.
  Until three observations with `N⁻ ≥ 1` exist, the fewest that give a variance,
  `m̂ = â`.

**Why the cap and the bound** (§15, H2). The first draft took `Û₀` only from
fully idle observations, 0 without one. A controller that starts with a backlog
may never have one, and then `(live − 0)/N⁻ = m + U₀/N⁻` carries the whole
informer cache into each reconcile's cost. With 140 MB of caches, a 166 MB budget
and 2 MB per reconcile, 13 slots fit; the draft's law moves from one slot toward
13 with slope `1 − m/m̂ ≈ 0.99` per GC, and was still at 5.6 slots after 20 GCs.
With `â = 4 MB` the fixed law reaches 95% of 13 in 3 GCs (§4.4).

The survey suggests a regression of `live` on in-flight count, whose intercept is
the informer caches. The headroom form of §4.4 doesn't need the intercept, only
an upper bound on `m`, and a regression is unbiased, which for memory is the
wrong side to err on. It also needs variation in the in-flight count, which a
full set of slots doesn't give; the warm-up's few-slot observations are the
variation the lower bound above uses.

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
3. **Both, each where it is valid** (chosen). The CPU law, sample `i`, with
   `Uᵢ = ΔUᵢ/Δtᵢ` the measured total and `Sᵢ₋₁` the slots before it:

   - **Active sample.** The live duration is a measurement of the plant at its
     current point, and the law's value is

     `g = (ρL − B̂) / k̂`, with `k̂` the smoothed CPU per slot-second.

     This is `X* × D` with the live `D`; its fixed point is exactly "total CPU =
     `ρL`" (§4.3), including the throttling stretch at the target.
   - **Capped sample.** The live duration measures the cap, so it is set aside:

     `g = min( (ρL − B̂)/cᵢ × D_mixᵢ , θ × N̄ᵢ )`, `θ = (ρL − B̂)/(L − B̂)`

     where `cᵢ = (ΔUᵢ − B̂Δtᵢ)/Cᵢ` is this sample's CPU per completion (true at
     the cap), and `D_mixᵢ = Σ_q C_q,ᵢ D̂_q / Cᵢ` the uncontended duration of the
     reconciles this sample ended, each queue's `D̂_q` from its active samples
     (§3.4), mixed in this sample's proportions. It is computed only if every
     queue that ended a reconcile in the sample has a `D̂_q`. The first term is
     the survey's uncontended-duration formula with the cost and the mix of now;
     it lands near the target, below it by the throttling stretch when the cost
     per completion is the same at the cap as below it, above it when the cap's
     is lower (here by 5%, §4.3). The second is the largest `S` the sample's
     evidence allows: at the cap, `U(N̄) ≥ L`, so with throughput concave in
     concurrency (A2 below), `U(θN̄) ≥ ρL`, and the target lies at or below
     `θN̄`. It guarantees a descent when the first term is missing or stale. With
     `Cᵢ = 0` only the second term applies.
   - **A cost too small to resolve.** If `k̂ = 0`, or from the third sample
     `k̂ < 2·SE(k̂)`, the reconciles' CPU can't be told from the noise of the
     background, and the CPU isn't what binds: `g` is the ceiling, reached
     through §7's pacing. `B̂` is at or below every recent active sample (§3.3),
     so `k̂ ≥ 0` always and the law never divides by zero or by a negative.
   - **The sign guard.** The law may lower the slots only in a sample whose
     total is over target, and raise them only in one under it:

     `S_cpu = max(Sᵢ₋₁, g)` if `Uᵢ < ρL`; `min(Sᵢ₋₁, g)` if `Uᵢ > ρL`; `g` if equal.

     The smoothed `k̂` mixes past samples, and the guard keeps it from moving the
     slots against what this sample measured. It can't stop the law at a wrong
     point: with this sample's own `kᵢ`, the law's direction is the sign of
     `ρL − Uᵢ` whenever `B̂ < Uᵢ`, which §3.3 ensures (§4.3). When `k̂` and the
     sample disagree, the guard holds, and `k̂` moves towards `kᵢ` until they
     agree.

   **Why not this sample's own duration in the capped term** (§15, M1): with
   `D = Aᵢ/Cᵢ` the first term becomes `(ρL − B̂) Aᵢ/(ΔUᵢ − B̂Δtᵢ) = θ N̄ᵢ`, since
   `ΔUᵢ = LΔtᵢ` at the cap: it is the second term again. A capped sample alone
   can't do better than `θN̄`. A plant whose knee sits at `N̄ᵢ` fits the same
   sample and has its target at `θN̄ᵢ`, so any lower value could undershoot it.
   Descending faster takes history, which is what `D̂_q` is.

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
- **The sign guard keeps all of this.** In the region below `S*` the law only
  rises and `U < ρL`, so the guard's `max` is the law's value; above, the same
  with `min`. It binds only when the smoothed `k̂` and the sample disagree,
  and then it holds.
- **The direction is right even with a wrong `B̂`**, as long as `B̂ < Uᵢ`:
  `gᵢ = (ρL − B̂)Sᵢ/(Uᵢ − B̂)` is below `Sᵢ` exactly when `Uᵢ > ρL`. §3.3's bound
  ensures `B̂ ≤ Uᵢ` for every recent active sample, so the law can't be singular,
  and `k̂ ≥ 0`.
- **If `B̂ > B` by `δ`** anyway (a background that drops below its recent minimum
  under load), the step overshoots by at most a fraction `δ/(ρL − B̂)` of the
  error, which then decays. In the review's example `B̂` was biased by bursts;
  a minimum isn't.

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

**The review's cases**, in the same kind of fluid model (scratch, like the table
above):

| Case | The reviewed law | This revision |
|---|---|---|
| H1: 500m, 3ms of CPU per 0.8s reconcile, `B` 15 ms/s, a burst-biased `B̂` of 60 ms/s | 1, 1, 1, … for good; CPU at about 4% | 2.25, 5.06, 11.4, then the ceiling, 16, in the fourth sample (paced by completions) |
| H1: the same with `B` noisy (sd 10 ms/s) against 3.75 ms/s per slot | — | at the ceiling from the fourth sample, never below it after |
| H1: the measured workload, `B` 20 ms/s, `B̂` forced to 80 ms/s (above `U(1)`) | 3.25, 6.94, 4.01, 12.6, 4.06, … alternating | with the guard and the active bound but `B̂` still forced: 4.03, 5.13, 4.17, 4.87, … settling at 4.6 by the 14th sample; with `B̂` at or below `B`, as §3.3's minimum keeps it (the run used 0): 3.24, 3.84, 4.02, … monotone |

So the guard and the active-sample bound remove the singularity and the
absorbing floor, but not every alternation; what removes that is not letting
`B̂` exceed `B`, which is why §3.3 takes a minimum. The memory cases are in §4.4.

These figures came from Python scratch scripts written for the first draft and
the revision, and never committed. They are superseded by the Go simulation of §10, which drives
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

**Convergence**, re-derived for the revision (§15, H2). Take `live = U₀ + mN`
at a GC with `N` in flight (the mean per reconcile in flight, `m`), and the
target `N* = (H − U₀)/m` when `H > U₀`.

- **The estimates are on the safe side.** `Û₀ ≤ U₀` (each term of its maximum
  is, §3.6) and `m ≤ m̂ ≤ â` (both terms of its minimum bound `m` from above).
- **Each step rises and never passes the target.** With `N⁻ ≤ N`:
  `S' = N⁻ + (H − U₀ − mN)/m̂ ≤ N + (H − U₀ − mN)/m = N*`; and for `N < N*`,
  with `N⁻ = N`, `S' − N = (H − U₀ − mN)/m̂ > 0`. The slope of the map is
  `1 − m/m̂` in `[0, 1 − m/â]`, so each observation closes at least a fraction
  `m/â` of the remaining distance. Monotone, from below, never past `H`.
- **Above the target**, after a burst of heavy reconciles or a cache that grew,
  `H − live < 0` and `S' < N⁻`: no grants until enough reconciles end. Down to
  the floor if the caches alone exceed the budget (`H ≤ U₀`): there is no fixed
  point above one slot, and one is the right answer.
- **No fully idle observation is needed.** The draft needed one for `Û₀`; now
  `m̂ ≤ â` holds the slope at `1 − m/â` from the first completion, and the
  warm-up's few-slot observations tighten `Û₀` and so `m̂`. The fixed point is
  the same `N*`.

The low pairing leaves `live` short of `H` by about `m̂ × (N − N⁻)`, typically
one reconcile's memory; that is the price of not counting admissions as if they
had happened before the GC. Unlike the CPU law, nothing here depends on
duration, and between two GCs `S_mem` doesn't change, so new admissions are
paced by the GCs that measure them.

**The review's cases** (scratch fluid model, one GC per step, 2 MB per
reconcile, a backlog from the start and no idle observation, ceiling 64):

| Caches `U₀` | Budget `H` | Target `N*` | `â` | The reviewed law | This revision |
|---|---|---|---|---|---|
| 140 MB | 166 MB | 13 | 4 MB | 5.6 slots after 20 GCs | 7.0, 10.8, 12.4, 12.8: 95% in 3 GCs |
| 140 MB | 166 MB | 13 | 50 MB | 5.6 after 20 | 1.5, 2.0, 2.6, …: 95% in 24 GCs |
| 300 MB | 600 MB | 150 | 50 MB | 62.9 after 20 | 7.0, 16.7, 29.2, …: at the ceiling in 6 GCs |
| 500 MB | 690 MB | 95 | 4 MB | 17.8 after 20 | 48, then the ceiling in 2 GCs |
| 500 MB | 690 MB | 95 | 50 MB | 17.8 after 20 | 4.8, 10.6, 18.0, …: at the ceiling in 9 GCs |

None passed its target. The slower rows are those where a reconcile allocates
25 times what it keeps; the convergence rate is `m/â` per GC, but with a Go
limit a GC comes at least each time the heap grows by `G − live`, so GCs are most
frequent exactly when reconciles allocate most.

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

- **`cpu.max` is `max`:** a limit measured when the node runs short, below.
  With no `cpu.pressure` either, the CPU input is disabled, with the reason
  logged once at Info.
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

### 5.1 No CPU limit: a measured effective limit

Decided by the user (§13 item 3, §15 M2). With no quota, the container can use
the whole node, and the limit that matters is what the node gives it when other
pods want the same CPUs. That limit is never written anywhere; it shows only as
stall.

- **Free.** While the stall share stays at or under `CPUPressureThreshold`,
  there is no CPU cap: `S_cpu` is the ceiling, reached through §7's pacing. The
  estimates (`k̂`, `ĉ`, `D̂`, `D̂_q`, `B̂`) are kept from active samples as with a
  limit, so they are ready when one is learned.
- **Onset, and the onset window.** The first sample with the stall share over
  the threshold starts an onset window: that sample and every one after it that
  stays over the threshold. Over the window the effective limit is the CPU the
  container actually obtained,

  `L_eff = ΣΔU / ΣΔt` over the window's samples,

  a ratio of sums, so the first sample sets it and the rest refine it; it is
  frozen when the window ends. While runnable work is waiting for a CPU, what
  the container obtains is what the node is giving it: censored, like usage at a
  quota.
- **Held.** With `L_eff` set, the CPU law is §4.2's, with `L_eff` in place of `L`
  everywhere: the active-sample law `(ρL_eff − B̂)/k̂`; the sign guard against
  `ρL_eff`; and a sample with the stall over the threshold is a *pressured*
  sample (§3.2), treated as capped at `L_eff`: it doesn't feed `k̂` or `D̂`, and it
  descends by `min((ρL_eff − B̂)/cᵢ × D_mixᵢ, θ_eff N̄ᵢ)`, with
  `θ_eff = (ρL_eff − B̂)/(L_eff − B̂)`. That descent is the cut at onset: one
  computed step, from the costs measured while the node had room, to the slots
  that use `ρ` of what it gives now.
- **Re-learned at the next onset.** A new run of samples over the threshold,
  after a clear stretch, opens a new onset window, and its `L_eff` replaces the
  old one, up or down.
- **Released when pressure clears.** Pressure has cleared when the stall share
  over the last `τ` (§3.5's window, a ratio of sums) is at or under the
  threshold. The release is stepwise: each clear window raises `L_eff` by `1/ρ`,
  and the evidence window starts again. When `S_cpu` at the raised `L_eff`
  reaches the ceiling, `L_eff` is dropped and the input is free again.

**Why stepwise, and why it converges.** In a held phase `L_eff` is a fixed
limit, so §4.3 applies as written, with `L_eff` for `L`: the law settles
monotonically where the container's CPU is `ρL_eff`. Across phases there is no
fixed point to prove, because the effective limit can't be observed without
stall, and stall appears only past it: the limit can be found only by using
more. Releasing it whole, which is the decision read literally, sends the slots
to the ceiling in a few samples and back at the next onset. In the draft's fluid
model of a node giving the container 400 ms/s, the slots swung between 7.3 and
23.9 and the stall was over the threshold in half the samples. Released by `1/ρ`
per clear window, each try is one 11% step taken after a window of evidence: the
same model held the slots between 9.9 and 18.0, with the stall over the
threshold in 5 of 160 samples. A share that doubled was found in about 66s, six
or seven windows; one that halved was followed at the next onset, in one
sample. Where in that band the slots settle depends on how the stall grows past
the share, which the model only assumes; the Go simulation's node model and the
re-measure have to show it (§10). The release rule is this revision's reading of
"released when pressure clears", and §13 item 3 records it for the user to
confirm.

**Its interaction with the other rules.** While free, a sample over the
threshold is an onset, and blocks the rise in that sample like any pressured
sample (§6). While held, pressure blocks rises (§6), the sign guard uses
`ρL_eff`, and capped-sample rules use `L_eff`. With a quota and no node
contention none of this runs: the quota is the limit.

**The change from today.** Today, with no `cpu.max`, a pressure reading over 50
halves local capacity and it climbs back by one per 30s. Now nothing slows the
controller until the stall appears; at onset the slots go in one computed step
to what uses 90% of the CPU the node is giving, and they rise again in steps of
`1/ρ` per clear window.

## 6. Pressure

**What it does: it blocks increases, and nothing else.** When the CPU stall
share of the sample, `P = Δ(some total)/Δt` as a percentage, is over
`CPUPressureThreshold`, the slots don't rise. Through a run of such samples they
are held at `Sᵢ = min(S_onset, S_model,ᵢ)`, where `S_onset` is the value before
the run's first sample. So a decrease the model computes still applies, and when
the model's value comes back up within the run the slots follow it back to
`S_onset`; they aren't left at the run's lowest point, as a running minimum would
leave them (§15, L3). Pressure never lowers the slots by itself. With no
`cpu.max`, pressure also opens the onset window of §5.1, and the model's own
value then includes the learned limit.

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
With a quota, contention that holds the container under its quota is handled by
this block alone; the learned limit of §5.1 is for the case with no quota, as
decided.
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
  first. The same pacing applies to every rise towards the ceiling when the CPU
  isn't binding (§4.2) and after a release of the learned limit (§5.1).
- **What the pacing counts, and what that costs** (§15, L4). `Cᵢ` counts every
  reconcile that ended, including key deferrals and no-op reconciles that touch
  nothing; a slot is a slot whatever ran in it. A queue of cheap no-ops therefore
  opens slots faster than the expensive work behind it would justify; the CPU
  and memory laws, not the pacing, are what bound the expensive work. The other
  side: after a wake from scale-to-zero, reconciles that last 30s open one slot
  per completion, so the first minute runs at a few slots whatever the limits
  allow. That is the price of not admitting work before any of it has been
  measured.
- **Memory increases** are bounded by the room the last GC showed, and don't
  move between GCs (§4.4).
- **Capped and pressured samples** never raise (§4.2, §5.1), and **pressure**
  blocks rises (§6).

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
  steady. A shift in each queue's share of the slot-time restarts the shared
  estimates (§3.5), and each queue keeps its own uncontended duration `D̂_q`, so a
  capped sample after the shift descends with the mix of now (§4.2). A queue
  never seen uncapped descends at `θ` per sample, the fastest a capped sample
  alone allows (§4.2; §15, M1). Charging each controller its own CPU would need
  a regression of CPU on the queues' slot-seconds and cost-weighted grants
  instead of slots; not in this design.
- **Work outside the gate** (a controller built without `ForControllerRuntime`,
  any busy goroutine) is CPU the law can't control. It counts as background: it
  shows in idle samples, so `B̂` includes the part that runs then, and in the
  total the sign guard reads. If it alone keeps the CPU over target, the law
  lowers the slots to the floor and holds them there, which is all it can do.
- **Several Governors in one process:** each sees the whole cgroup's CPU against
  only its own slot-seconds, so each overestimates its cost in proportion, and
  each one's idle samples contain the other's work, which §3.3's minimum keeps
  from inflating `B̂`. Both guards read the same total, so both move in the
  direction it shows. The existing warning stays: nothing about two estimators
  sharing one budget is tested. **The Go memory limit is process-wide and set
  once** (§15, L7): the first `Start` with `Memory` on sets it, under a
  process-wide once, and a later Governor's `Start` changes nothing and logs a
  warning naming the limit already in force. #48's code instead found the limit
  "set by the program" and logged it at Info, which reads as if the operator had
  set it.
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
| `…local.cpu.capped` | 0/1 | last sample capped, or pressured with a learned limit |
| `…local.cpu.effective_limit` | cores | `L_eff` while held (§5.1); nothing while free or with a quota |
| `…local.queue.slot_share{controller}` | fraction | each queue's smoothed share of the slot-time, `A_q/A` |
| `…local.estimate.restarts{estimate, cause}` | count | restarts by estimate (`k`, `c`, `d`, `m`) and cause (`cost`, `mix`) |
| `…local.memory.budget`, `…memory.live`, `…memory.baseline`, `…memory.per_reconcile` | bytes | `H`, `live`, `Û₀`, `m̂` |

and a counter `…local.pressure_blocked`, samples in which pressure held a rise.
`Snapshot` gains the same values. They are provider-runtime's own; core-provider's
runtime has a local capacity gauge of the same name (design-local-slot-before-pop
§6) and none of these.

## 10. Tests

**Unit, with a fake cgroup directory and a fake clock** (`testdata` already holds
cgroup fixtures): the sample kinds (idle, capped, pressured, active) from file
deltas, including a capped sample by counters but not by usage, and usage just
inside and just outside `L·Δt − slack` (L1); `Δt` taken from the clock, not the
period; `A` closed at `t_read` with a grant and a release landing between the
read and the lock (L5); `N⁻` unchanged by a release re-granted in the same
critical section (L6); every formula of §3–§4 against hand-computed values;
`cpu.max` of `max`, a missing `cpu.stat` throttling block, a missing
`cpu.pressure`; counters that go backwards (treated as a restart of that input,
not a negative delta); `B̂` from a window with a burst (the minimum ignores it),
`B̂` above an active sample's usage (bounded to it), `k̂ = 0` and
`k̂ < 2·SE` (the ceiling, paced), `B̂ ≥ ρL` (floor), zero slot-seconds, zero
completions; the sign guard holding in both directions; the warm start, the 3σ
hold-out and the restart; the change test skipping a sample after a large move
in `N̄` (L2) and restarting on a shift in queue shares (M1); `D_mix` with a queue
that has no `D̂_q` (only `θN̄`); trust, and no trust before the third sample;
the untrusted increase bound `Sᵢ₋₁ + Cᵢ`; pressure blocking a rise, letting a
fall through, and holding at `min(S_onset, S_model)` when the model recovers
within a run (L3); the learned limit's onset window, hold, re-learning and
stepwise release (§5.1); error diffusion averaging to the real value within
`1/n`, its carry staying in `[0, 1)`, and each reset of §4.5 (a new `cpu.max`, a
new Go limit, a clamp, a change of binding input, a restart) followed by `⌊S⌋`;
a pressure-held sample keeping the carry. Memory with a fake runtime-metrics
reader (#48's `goMemory` seam): observation only when the GC count rises, the
`N⁻` pairing, `Û₀` as the windowed maximum of `live − â·N⁺` with and without an
idle observation, `m̂` capped at `â`, the allocation bound alone for the first
three observations, the limiter alarm, `GOGC` without a Go limit, `GOGC=off`; a
second Governor's `Start` leaving the Go memory limit alone and warning (L7).

**Simulation in Go,** superseding the Python scripts behind §4.3's figures, which
were never committed: a discrete-event model of the
measured workload (CFS quota per 100ms period, GOMAXPROCS 2, 23ms of CPU and
400ms of wait per reconcile, 16 workers, a backlog) driving the real estimator
and law through the fake clock. It must show, before any cluster run: settling
between 4 and 5 slots with mean CPU within 3 points of 90%; a monotone approach
from one slot; every row of §4.3's table, including no oscillation after a cost
step; and the memory workload settling at its budget from below with no sample
over `H`. And the review's cases, each with its pass condition:

| Case | Passes when |
|---|---|
| H1: 500m, 3ms of CPU per 0.8s reconcile, `B` 15 ms/s, idle samples with bursts to 60 ms/s | the slots reach the ceiling within 10 samples and never return to the floor |
| H1: the same, `B` noisy (sd 10 ms/s) against 3.75 ms/s per slot, 10 minutes | never below the ceiling after warm-up |
| H1: the measured workload with `B` 20 ms/s and idle bursts above `U(1)` | monotone from one slot to the target, mean CPU 85–95% |
| H2: a backlog from the start, no idle observation, caches of 100, 300 and 500 MB against budgets that fit them, 2 MB kept and 4 or 50 MB allocated per reconcile | no observation with `live` over `H`; 95% of `min(N*, ceiling)` within 30 GCs |
| M1: a cheap queue (1ms of CPU, 2s each) at the ceiling, then an expensive one (23ms, 0.43s) taking the backlog | with the expensive queue seen uncapped before: within 2 samples of its target; never seen: a descent of at least `θ` per sample, and no rise while capped |
| M2: no `cpu.max`, a node model with CFS shares among other cgroups, the container's share stepping from 400 to 800 and back to 400 ms/s | at onset, one step to within 10% of the law's value at `ρL_eff`; stall over the threshold in at most 10% of samples at a steady share; a doubled share found within 10 windows |

The node model is the part the scratch scripts only assumed: how the stall grows
as demand passes the share. It is built from CFS's weighted sharing on a fixed
number of CPUs, with the stall share computed from the time runnable tasks
wait, not from a formula.

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
  and the mean in flight; the sample's kind (idle, capped, pressured, active),
  with `Δnr_periods`, `Δnr_throttled`, `ΔU` and the capped threshold
  `L·Δt − slack`; whether the sign guard held; trust and restarts per estimate,
  with their cause; each queue's slot share; the 1s stall share, whether it
  blocked a rise, and the learned limit's phase and `L_eff`; and, with memory
  on, `H`, `live`, `Û₀`, `â`, `m̂`, `N⁻`, `N⁺` and whether the sample held a GC
  observation. The report script computes the bars from these same lines.

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
2. `N⁻` and `N⁺` reset racing a release, whether `N⁻` can read higher than the
   true minimum, and the per-queue books (`A_q`, `C_q`) against a queue removed
   at shutdown; closing at `t_read` with changes between the read and the lock.
3. Publishing a lower capacity while grants are in progress; error diffusion
   flipping capacity every sample; a stress test with flapping capacity and a
   drain test at shutdown, per the lesson of the gated queue (§10 of that
   design: the races were found by the reviewer's tests, not the builder's).
4. The sampler delayed for seconds by throttling: one long `Δt` must not dominate
   the EWMA (`α` comes from `Δt`), and rates must not spike.
5. NaN, Inf and negatives on every division; `math.MaxInt64` limits; counters
   going backwards; a cgroup path that changes.
6. The process-wide Go memory limit: `Start` on two Governors at once (the
   once must hold), and a program calling `debug.SetMemoryLimit` concurrently.
7. The estimator's states: restarts, trust, the learned limit's phases and the
   carry all owned by the sampler goroutine, and published to `Snapshot` only
   through the atomic it already uses.

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
| CPU per reconcile by regression with a background intercept, ≥ 60 completions per window | The setpoint is independent of the background estimate (§4.3), and the regression is confounded at the operating point; the background is a lower bound from idle and active samples, because an overestimate is what made the law singular in review (§3.3; §15, H1). The 60-completion window is adopted in `τ` and trust (§3.5). On this workload the regression gives 23.1–23.3ms and a background within noise of zero |
| Memory from the post-GC live heap, regressed on in-flight; a hard ceiling with hysteresis | Post-GC live heap adopted; the headroom form needs no intercept, only an upper bound on the memory per reconcile, kept upward by the allocation bound and a baseline bounded from below by the data (§3.6; §15, H2). "Fast decrease, slow increase" is met by data rather than a band: decreases at once, increases only by the room the last GC showed (§4.4) |
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
   with a cooldown would bring back a fixed step and a hold-off. **With no CPU
   limit, a measured effective limit** (decided after the review, §15 M2): with
   no pressure, no CPU cap; at pressure onset the limit is the CPU actually
   obtained over the onset window, and the same law applies with it; it is
   released when pressure clears and re-learned at the next onset (§5.1). This
   replaces today's halving on pressure when there is no quota. **For the user
   to confirm:** §5.1 releases the limit by `1/ρ` per clear window rather than
   all at once, because a release all at once swung the slots between 7 and 24 in
   the fluid model, with the stall over the threshold in half the samples.
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
one-line definitions, and the slice's 5ms default) and `psi.rst` (format, `total`
in µs); every figure of §1 and §4.3, recomputed from the committed logs; the
figures of §4.3, §4.4 and §5.1 from scratch scripts of the first draft and of
this revision, never committed and superseded by §10's Go simulation; the
recorded `new3` tarball's digest (§10); provider-runtime#50 is open with that
title.

**Not checked here:**

- Whether `nr_throttled` counts periods or per-CPU throttling events, and whether
  `nr_periods` counts idle periods. The kernel doc defines them in one line each.
  The capped test of §3.2 also requires usage within one slice per CPU of the
  cap, so a wrong reading of the counters can't make an uncapped sample look
  capped; the GKE run logs both per sample.
- How the stall share grows as demand passes a node share, which §5.1's figures
  assume; §10's node model replaces the assumption.
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

## 15. Review history

**First independent review, of `05e359c`, 2026-11-08.** Two High, two Medium,
seven Low. Each item and where this revision answers it:

| Item | Finding | Answer |
|---|---|---|
| H1, High | The CPU law is singular when `B̂ ≥ U(S)`, and the floor is absorbing: with a burst-biased `B̂`, `k̂ ≤ 0` pins the slots at 1 with the CPU at about 4%. The same with ungated work or several Governors | §3.3: `B̂` is a minimum over recent idle samples, bounded by every recent active sample, so it can't be raised by a burst and `k̂ ≥ 0`. §4.2: the sign guard (lower only when `Uᵢ > ρL`, raise only when `Uᵢ < ρL`), and an unresolvable `k̂` (`0`, or under `2·SE` from the third sample) means the CPU isn't binding: the ceiling, paced. §4.3: the direction proof without `B̂ ≤ B`, and the review's cases. §8: ungated work and several Governors. §10: the H1 simulation cases. The guard and the bound alone still left an alternation when `B̂` is forced high; the minimum is what removes it (§4.3's table) |
| H2, High | Without a fully idle GC observation, `m̂` carries the informer caches, and the memory law crawls from 1 (13 slots fit, 5.6 after 20 GCs) | §3.6: `m̂ = min(GC estimate, â)`, the allocation per completed reconcile, still `≥ m`; `Û₀` the windowed maximum of `live − â·N⁺`, at most `U₀`. §4.4: convergence re-derived, slope at most `1 − m/â`, monotone from below, and the review's cases with caches of 100–500 MB and a backlog from the start. §10: those cases |
| M1, Medium | The descent at the cap uses a frozen `D̂` from another mix: a cheap controller at the ceiling, then an expensive batch, 18 samples at the cap | §3.2 and §3.4: per-queue slot-time `A_q` and per-queue `D̂_q`. §3.5: a shift in the queues' shares restarts the shared estimates. §4.2: the capped term uses `D_mix`, each queue's `D̂_q` weighted by this sample's completions. The suggested "this sample's own slot-seconds per completion" turns the term into `θN̄` exactly, and §4.2 shows `θN̄` is the fastest descent a capped sample alone allows, so a queue never seen uncapped still descends at `θ` per sample; that residual is stated (§8). §10: the mix-shift case |
| M2, Medium | No CPU limit: no cap from the model, only pressure. Decided by the user: a measured effective limit | §5.1: free without pressure; at onset `L_eff` is the CPU obtained over the onset window; held, the same law with `L_eff`, pressured samples treated as capped; re-learned at each onset; released when pressure clears, by `1/ρ` per clear window. Why held phases converge and why the whole has a band, not a point; the interaction with §6 and the sign guard; the change from today. §13 item 3: updated, with the stepwise release for the user to confirm. §10: the node-contention case |
| L1 | The capped threshold allowed one period's quota | §3.2: within one 5ms slice per CPU that can run Go code, never more than a period's quota; logged every sample |
| L2 | The 3σ restart fires on a legitimate change of `k` with `S` | §3.5: a sample after a move in `N̄` beyond 3σ of its own noise is averaged in but not tested |
| L3 | Pressure held at a running minimum | §6: held at `min(S_onset, S_model)` |
| L4 | Untrusted pacing | §7: it counts deferrals and no-ops, and a wake with long reconciles ramps slowly; both stated |
| L5 | `A` closed at the lock, not at the read | §3.2: closed at `t_read`, the changes after it carried forward |
| L6 | Zero-length dips counted in `N⁻` | §3.2: `N⁻` and `N⁺` updated after the dispatch of each critical section |
| L7 | A second Governor's `Start` | §8: the Go memory limit is set once per process; a later `Start` changes nothing and warns |
