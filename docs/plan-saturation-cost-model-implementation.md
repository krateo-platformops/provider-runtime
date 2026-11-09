---
type: Architecture
title: provider-runtime — saturation cost model, implementation plan
description: Plan, nothing built, for implementing the saturation cost-model design as a stack of six PRs plus the harness PR and the measurement PR, starting with the §10 Go simulation driving the real estimator and law code, with each PR's scope, design sections, tests, review focus, dependencies and size, the design's ambiguities and the premises to verify.
resource: github.com/krateo-platformops/provider-runtime
tags: [plan, design, rate-limiting, saturation, cgroup, gc]
timestamp: 2026-11-09T00:00:00Z
---

# Saturation cost model: implementation plan

**Status:** approved by the user on 2026-11-09, with the decisions in §9.
Nothing is built. The spec is
[design-saturation-cost-model](./design-saturation-cost-model.md) as merged at
`d86c2b1` (PR #54, five independent reviews). Section numbers (§) are the
design's. Code references are to `d86c2b1`; the measurement harness is on
`origin/feat/experimental-memory-headroom` (#48, held), at `c1c299c`.

## 1. Summary

The simulation is the gate, and it must run the production code. So the
estimators, tests and laws are written first as **pure functions in one package
with no cgroup, clock or runtime I/O**, `pkg/ratelimit/internal/costmodel`. The
§10 simulation and, later, the Governor both call it. Nothing the simulation
checks is a copy.

| # | PR | Main design sections | Prod / test lines | Depends on | Concurrency review |
|---|---|---|---|---|---|
| 1 | `costmodel`: shared parts and the CPU path, the simulation engine, the CPU cases | §3.1–§3.5, §4.1–§4.3, §4.5, §5.1, §6, §7, §10 (CPU rows) | 1,300 / 2,400 | — | no; **statistical review** |
| 2 | `costmodel`: the memory path, the GC and heap model, the memory cases | §3.6, §4.4, §10 (memory rows), §16.3 | 1,200 / 2,300 | 1 | no; **statistical review** |
| 3 | The Governor's books: slot-seconds, per-queue shares, pop times, `N±` after dispatch, closing at `t_read` | §3.2, §3.4, §8 | 250 / 600 | 2 | **yes** |
| 4 | The sampler: `cpu.stat`, `cpu.max` every sample, PSI `total`, runtime metrics, the Go memory limit once | §2, §3.1, §3.2, §4.4, §5, §8 (L7) | 450 / 600 | 3 | light |
| 5 | The wiring: `costmodel` replaces `saturation.step`; options and API removals (Breaking); metrics; the example | §4.5, §7, §8, §9 | 700 / 900 | 4 | **yes** |
| 6 | Docs and the log entry | §9, §13 | ~400 doc lines | 5 | — |
| H | The harness on `main`, as its own module, from #48's branch: harness, scripts and existing results, no `ratelimit` code | §10 (harness) | the harness as it is | — | — |
| M | The harness changes and the GKE block plan, on `main` | §10 (GKE, harness) | ~300 harness lines | 5, H | — |

That is about **3,900 lines of production Go and 6,800 of tests**, including the
simulation engine (about 1,400). PRs 1 and 2 together are the §10 simulation;
the gate is passed when both are merged and every case of §4 passes. No later
PR merges with a simulation case failing.

The design's ambiguities, met while sketching the code, and my choice for each
are in §6. The ones that need the user are in §9. The premises are in §7.

## 2. Rules for every PR

- **Each PR builds and passes `go test -race ./...` on its own,** with CI's
  envtest. The simulation's long cases (four simulated hours) run in CI; they
  are simulated time, so they take seconds, and a `-short` flag only lowers the
  seed count, never the duration.
- **No behaviour change with saturation off.** PRs 1 to 4 add code nothing in the
  saturation path calls yet. PR 5 switches the path, and carries a test that a
  Governor with saturation off builds no `costmodel` state, reads nothing and
  publishes no local capacity, as today (the gauge is reported only with
  saturation on, `pkg/telemetry/metrics.go:585`).
- **`costmodel` imports nothing that does I/O.** No `os`, no `runtime/metrics`,
  no `time.Now`, no `debug`. Every input is a value in a `Sample` struct; every
  output is a value. PR 1 adds a test that `go list -deps` of the package
  contains none of those, and it runs in every later PR.
- **One code path, two drivers.** The Governor (PR 5) and the simulation (PRs 1
  and 2) both call the same `costmodel` functions with the same `Sample` type.
  The simulation's drivers produce samples the way the sampler does (counters
  differenced over a measured interval, the books closed at a read instant), so
  the boundary between simulated plant and real code is the `Sample`.
- **Clocks.** `costmodel` takes durations and monotonic offsets, never
  `time.Time`. The sampler measures them with `time.Since` on monotonic times.
- **Independent review.** PRs 1 and 2 get a statistical reviewer (the estimators,
  the bounds, the tests' false-alarm rates); PRs 3 and 5 get a concurrency
  reviewer. Each reviewer gets the diff, the design sections and the PR's attack
  list, not the builder's results. Findings come back as failing tests or
  simulation cases.
- **Revert checks.** Each PR adds `hack/revert-check-cost-model-<n>.py` in the
  style of `hack/revert-check-local-slot.py`: it breaks each rule the PR adds,
  one at a time, and requires a test to fail by assertion. Its output is
  committed next to it.
- **No cluster is touched** until the measurement block, whose cluster needs the
  user's approval (PR M).
- **Releases.** Nothing is tagged until PR 6. The release is breaking for an
  off-by-default feature, as #40 was. It is v1.13.0 if this stack finishes
  before the credential-use stack, v1.14.0 if after (§9).

## 3. The PRs

### PR 1: `costmodel`, the shared parts and the CPU path, and the simulation engine

**Scope.** New `pkg/ratelimit/internal/costmodel` (internal: no public API until
PR 5 decides what to export):

- `sample.go`: `Sample`, one sampler interval: `Dt`; `UsageDelta`,
  `NrPeriodsDelta`, `NrThrottledDelta`, `ThrottledPresent` (whether the cgroup
  reported them); `CPUMax` (cores, or none) and `Period`; `GOMAXPROCS` and the
  bandwidth slice; `StallDelta` (PSI `some total`) and `StallPresent`; the books
  (`A`, `C`, per-queue `A_q`, `C_q`, `NMin`, `NMax`, the longest hold); and the
  memory part filled by PR 2.
- `books.go`: `Books`, the slot-seconds accrual (§3.2) as a pure type:
  `Change(at, inFlight, queue)`, `Close(at)`, which returns the closed interval
  and carries later entries forward, and `N±` taken after a dispatch. The
  Governor wraps it under `g.mu` in PR 3; the simulation drives it directly.
- `estimate.go`: the ratio-of-weighted-sums estimator with warm start, the 3σ
  hold-out and two-in-a-row restart, the exemption after a large move in `N̄`,
  trust (§3.5).
- `cpu.go`: the sample kinds (§3.2), `B̂` as the minimum (§3.3), `k̂`, `ĉ`, `D̂`,
  `D̂_q`, the CPU law with the capped rule, `D_mix` and `θN̄`, the unresolvable
  rule and the sign guard (§4.2).
- `pressure.go`: the stall share from `total`, the block at
  `min(S_onset, S_model)` (§6), and §5.1's free, onset, freeze, held and release
  states with `U_pre` and the `×1/ρ` step.
- `slots.go`: the combination, the floor and ceiling, the untrusted pacing
  (§7), and error diffusion with its carry resets (§4.5).

New `pkg/ratelimit/internal/costmodel/sim`, the simulation engine:

- a discrete-event model of a container: CFS bandwidth (quota per period,
  refilled at the period boundary, handed out in slices to per-CPU runqueues),
  `GOMAXPROCS` threads, each reconcile a sequence of CPU and wait phases;
- the counters a cgroup exposes: `usage_usec`, `nr_periods`, `nr_throttled`,
  PSI `some` stall time from runnable tasks waiting (computed from the model's
  own runqueues, not a formula);
- a node model with other cgroups and CFS weights, for §5.1 (the stall's growth
  past the share is the part the scratch scripts only assumed);
- a gate with the gated queue's semantics (admission while in flight is under
  capacity, no preemption on a cut), per-queue workloads and a backlog;
- a driver that, every simulated second, closes the books at a read instant,
  builds a `Sample` and calls `costmodel`, and publishes the capacity back.

**Design sections:** §3.1–§3.5, §4.1–§4.3, §4.5, §5.1, §6, §7, and §10's CPU
rows.

**Tests.**

- Unit tests for every function, against hand-computed values, including:
  every sample kind; the slack threshold just inside and just outside;
  `B̂` with a burst in its window; `k̂ = 0` and `k̂ < 2·SE`; `B̂ ≥ ρL`; zero
  slot-seconds and completions; the sign guard holding both ways; the hold-out
  and restart; the `N̄` exemption; a queue-share shift; `D_mix` with a queue
  that has no `D̂_q`; trust before and after the third sample; pacing; the
  pressure hold recovering within a run; §5.1's onset shorter and longer than
  `τ`, `U_pre`, stepwise release; error diffusion within `1/n`, its carry in
  `[0, 1)`, every reset.
- The engine's own tests: the CFS model throttles a 20ms burst on a 25ms quota
  with two threads as the kernel does (§1's measured stretch: the simulated
  curve must land within 10% of §1's table at 1 to 7 slots and at 16); PSI
  `some` is 0 with one runnable task per CPU and grows with waiting.
- **The CPU simulation cases**, §4 of this plan.
- The dependency test (§2).

**Reviewer attack (statistical).** Can a wrong `B̂` reverse the law's
direction once the guard is in? Is `B̂ ≤ B` actually guaranteed by the minimum
when the background varies, and what happens when it isn't? Does the capped rule
ever raise? Can error diffusion's carry exceed `[0, 1)` or survive a limit
change? Is the false-restart rate of the change rule what the simulation says,
for 0.47s and 30s reconciles? Does the engine's CFS model match the kernel's
slice accounting closely enough that the capped test means what it says? Is
anything the simulation asserts a property of the engine rather than of
`costmodel`?

**Depends on:** nothing. **Size:** about 1,300 lines of production code and
2,400 of tests, the engine included.

### PR 2: `costmodel`, the memory path, the GC and heap model, the memory cases

**Scope.** In `costmodel`:

- `memsample.go`: the memory part of `Sample`: whether a GC cycle completed,
  `Live`, `NonHeap`, `GoMemLimit`, `GOGC`, the limiter's last cycle, the
  per-interval `NMin`/`NMax`.
- `regress.go`: weighted least squares of `live` on `N`, `t`, `t²` with the
  errors-in-variables correction, the variance model `s₀ + s₁N` by three
  reweighting passes, Newey–West standard errors with bandwidth from the longest
  hold, the long-run factor `f`, the bounds `m̂`, `m̂⁻`, `Û₀` and `L̂⁺(N)`, and
  identification (§3.6).
- `lifecycle.go`: the current fit preferred, the last identified fit kept, the
  two block checks with batch means (the `f` bound before 10 blocks), restarts
  with direction and confirmation, the queue-share restart (§3.6).
- `memlaw.go`: `H` with and without a Go limit, `N_pred`, `N_room` with its two
  divisors, `g⁺` as the floored quantile with its stand-ins, the data cap, the
  fallback with its bound and exception, the limiter alarm (§4.4).

In `sim`: a Go heap model (a live set from the reconciles in flight, each with
its own memory profile over its lifetime, so that reconciles span GCs; garbage
and the GC trigger at the heap goal under `GOGC` and the memory limit; the
caches as a separate live set that can drift; an OOM when the modelled
container memory passes `memory.max`), and the workloads of §10's memory rows.

**Design sections:** §3.6, §4.4, §10's memory rows, §16.3.

**Tests.**

- Unit tests: the regression against a reference implementation (a direct
  normal-equations solve in `big.Float`) on fixed data; the correction on a
  regressor with known error; Newey–West against a reference on series with a
  known one-lag and many-lag autocorrelation; batch means against the same; each
  identification condition failing alone; a ramp that moves only with time,
  straight and curved, never identifying; the kept fit predicting at its own
  time; each block check firing and not firing at its bound; a confirmed and an
  unconfirmed downward restart; `g⁺` floored at every `n` from 0 to 50, and with
  everything non-positive; `N_room` cutting by `m̂⁻`, by `m_fit`, by one slot;
  the fallback's bound and its exception; `GOGC=off` with no Go limit.
- **The memory simulation cases**, §4 of this plan.

**Reviewer attack (statistical).** Is `m̂ ≥ m` at the stated rate on the
heterogeneous mix, with reconciles that span GCs? Is the errors-in-variables
correction right when the regressor is an interval, not a noisy point? Is the
Newey–West bandwidth long enough with 30s holds, and can it be too long for the
window? Does batch means start from a biased estimate when a fit has just been
restarted? Can the floor make `g⁺` so large that the fallback never rises? Does
any restart reset something it shouldn't (`g⁺`)? Can the fallback and the
identified law disagree in direction from one GC to the next?

**Depends on:** PR 1. **Size:** about 1,200 lines of production code and 2,300
of tests, the heap model included.

### PR 3: The Governor's books

**Scope.** In `pkg/ratelimit`:

- `governor.go`: a `costmodel.Books` beside `localInFlight` (`:218`),
  changed at every grant (`dispatchLocked`, `:413`) and release (`releaseLocal`,
  `:391`) under `g.mu`, with the queue each grant went to (`g.queues[idx]`).
  `N±` updated at the end of each critical section that changes
  `localInFlight`, after its `dispatchLocked` (§3.2, L6 of the first review).
  `releaseLocal` gains the queue and whether the slot ended a reconcile: `Done`
  (`queue.go:394`) is a completion, the hand-backs at `:307` and `:321` are not.
- `queue.go`: `processing` (`:116`, today `map[T]struct{}`) holds the pop's
  time, so the hold is known at `Done` (§3.6's bandwidth). The oldest running
  hold is reported too, because a long reconcile still running bounds the
  bandwidth before it ends.
- A `closeBooks(tRead)` the sampler calls under `g.mu`, closing at the read's
  timestamp and carrying later entries (§3.2, L5).

Nothing reads the books yet except tests.

**Design sections:** §3.2, §3.4, §8 (several controllers, queue identity by
the gated queue's `name`, `queue.go:46`).

**Tests.** A model test comparing `A` and `A_q` with an independent integral
under `-race`, with the sampler closing at 1ms; a grant and a release landing
between the read and the lock; a release re-granted in the same critical section
leaving `N⁻` unchanged; hand-backs not counted; a queue removed at shutdown; the
hold of a key re-added while processing; stress and drain tests, as for the gated
queue.

**Reviewer attack (concurrency).** Can an interval be lost or doubled at the
close? Can `N⁻` read higher than the true minimum? Does every path that changes
`localInFlight` (grant, `Done`, both hand-backs, shutdown) reach the books? Does
the pop-time map stay exact under the races the gated queue's design fixed
(design-local-slot-before-pop §10)? Is any lock order changed?

**Depends on:** PR 2 (so the books' type is final). **Size:** about 250 lines of
production code and 600 of tests.

### PR 4: The sampler

**Scope.** In `pkg/ratelimit/saturation.go`, replacing `cgroupFiles.read`:

- `cpu.stat`: `usage_usec` always; `nr_periods`, `nr_throttled`,
  `throttled_usec` when present (§2; absent when the cpu controller isn't
  enabled); counters going backwards treated as a restart of that input;
- `cpu.max` every sample (§5's resize), with `max`;
- `cpu.pressure`: the `some` line's `total` (§6), replacing `avg10`
  (`readPressure`, `:355`);
- the runtime metrics of §2, read without I/O, ported from #48's
  `goMemoryReader` (`saturation.go:209` on the #48 branch), extended with
  `/gc/cycles/total`, `/gc/gogc:percent` and `/gc/limiter/last-enabled`; no
  allocation metric, which the design dropped (§15, H1-new);
- `memory.max`, ported from #48's `readMemoryMax` (`:589` there);
- the Go memory limit set once per process with `Memory` on, ported from #48's
  `applyGoMemoryLimit` (`:261` there), behind a process-wide once; a second
  Governor's `Start` logs a warning and changes nothing (§8, L7);
- the bandwidth slice from `/proc/sys/kernel/sched_cfs_bandwidth_slice_us` when
  readable, else 5ms, and `GOMAXPROCS`;
- one read instant `t_read` per sample (§3.2).

The output is a `costmodel.Sample` without the books, which PR 5 joins.

**Design sections:** §2, §3.1, §3.2, §4.4, §5, §8 (L7).

**Tests.** Fake cgroup directories in `pkg/ratelimit/testdata` (new fixtures:
throttled, unthrottled, no throttling block, `cpu.max` of `max`, a resize between
two reads, PSI with and without `total`, counters going backwards); a fake
runtime-metrics reader (#48's `goMemory` seam); the Go limit once under two
concurrent `Start`s; `GOMEMLIMIT` set, `off`, and a limit the program set.

**Reviewer attack.** Is any file read under `g.mu`? Can a missing key make a
counter look like a reset? Is a cgroup path change handled? Can two Governors
race on the Go limit?

**Depends on:** PR 3. **Size:** about 450 lines of production code and 600 of
tests.

### PR 5: The wiring, the options, the metrics

**Scope.**

- `Governor.sample` (`governor.go:694`) builds the `Sample` from the sampler and
  the books and calls `costmodel`; the result is published through `g.local` and
  `dispatchLocked`, as today. `saturation.step` and `evaluate` (`:245`) are
  removed, and with them the working-set reading; `RecoveryInterval` no longer
  reaches local capacity (`:698` passes it today).
- `SaturationOptions` (§9): `Memory`, `GoMemoryLimit`, `CPUTarget`,
  `MemoryReserve` added; `CPUPressureThreshold` kept with its new meaning;
  `StartupRampInterval`, `ExperimentalMemoryWorkingSet`, `WorkingSetSoftRatio`,
  `WorkingSetHardRatio`, their `Default…` constants and `InputMemoryWorkingSet`
  removed. Validation in `withDefaults`.
- `Snapshot` gains the model's values; `InputSnapshot` loses
  `memory_working_set`.
- `pkg/telemetry/metrics.go`: the gauges of §9, next to
  `ratelimit.local.capacity` (`:284`).
- The example: `--saturation-experimental-memory`
  (`examples/greeting-controller/main.go:205`) becomes `--saturation-memory`.
- A consumer scan, under both module paths, for the removed fields, recorded in
  the PR.

**Design sections:** §4.5, §7, §8, §9.

**Tests.** Governor-level tests with a fake cgroup and a fake clock: a full
sample cycle; capacity published and granted; saturation off unchanged (§2); the
carry across a limit change; telemetry tests for every new gauge; the
`withDefaults` validation; stress and drain with capacity flapping by error
diffusion every sample.

**Reviewer attack (concurrency).** Is `costmodel`'s state touched only by the
sampler goroutine? Is the published capacity consistent with what grants read?
Does a lower capacity racing grants keep the gated queue's guarantees? Does
`Snapshot` read model state without a data race?

**Depends on:** PR 4. **Size:** about 700 lines of production code and 900 of
tests.

### PR 6: Docs and the log

**Scope.** `docs/configuration.md` (the saturation options and their meaning),
`docs/api.md` (the option surface and the metrics), `docs/usage.md` where it
shows saturation, the governor design's §7 pointing to the cost model, and the
`log.md` entry: the unreleased "decided, not built" entry becomes the release
entry, with "Breaking, for an off-by-default feature" (§9, §13 item 6).

**Depends on:** PR 5. **Size:** about 400 doc lines.

### PR H: The harness on `main`

Decided by the user (§9, item 4): the harness comes to `main` as its own module,
before PR M.

**Scope.** `hack/rlgov-measure` from #48's branch at `c1c299c`, as it is there:
its `go.mod` and `go.sum` (module `rlgov-measure`, which requires
provider-runtime through a `replace` to the repository), the control's
`ctrl/go.mod` and `ctrl/go.sum` (standard names, so gitleaks' default allowlist
covers their checksums), the Go sources, the scripts, the `.gitignore` that
keeps credentials, build output and images out, and the existing `results/`
(226 files). No `pkg/ratelimit` code from #48 comes with it.

**What it builds against.** The harness's "new" arm (`opts_new.go`, build tag
`!ctrl`) sets #48's options (`ExperimentalMemory`,
`ExperimentalGoMemoryHeadroomRatio`), which `main` doesn't have, so on `main` it
doesn't compile until PR M points it at the cost model's options. The control
arm (`-tags ctrl`, against `d0632f1` through `ctrl/go.mod`) still builds. The PR
says so, and CI is unaffected: the root module's `./...` doesn't include a
nested module.

**Tests.** The control arm builds (`go build -tags ctrl -modfile=ctrl/go.mod`);
the docs lint passes; gitleaks passes on the results and the control's sums.

**Depends on:** nothing. **Size:** the harness as it is, about 260 files, most
of them results.

### PR M: The harness changes and the GKE block plan

On `main`, in `hack/rlgov-measure`, after PR H and PR 5:

- the "new" arm's options pointed at the cost model's (`Memory`, `CPUTarget`,
  `MemoryReserve`, `GoMemoryLimit`);
- rates over measured intervals and the `dt` field (§10, "The harness changes
  first");
- every model value of §10's list on every `STAT` line, from `Snapshot`;
- the heterogeneous memory workload's two classes (§10);
- the report script computing the bars from those lines, and the excess rate
  next to the residuals' autocorrelation (§10, L4).

And the block plan, a settings file fixed before run 1 in the style of
`cpu2-settings.txt`: the CPU block (off and new, 5 runs each, rotating), the
memory blocks (ramp, cold, heterogeneous, ABBA against `new3`'s recorded
tarball after its digest is re-checked), with §10's bars. **The cluster needs the
user's approval when the plan is ready**; nothing is run before. The results are
committed in `results/`.

**Depends on:** PR 5 for the image, PR H for the harness. **Size:** about 300
lines of harness changes.

## 4. The simulation's pass criteria: the gate

These are §10's, as written; every later PR keeps them green.

**The headline criteria.** On the measured workload (CFS quota per 100ms period,
GOMAXPROCS 2, 23ms of CPU and 400ms of wait per reconcile, 16 workers, a
backlog): settling between 4 and 5 slots with mean CPU within 3 points of 90%; a
monotone approach from one slot; every row of §4.3's table, including no
oscillation after a cost step; and the memory workload settling at its budget
from below with no sample over `H`.

**§4.3's table rows,** which the design gives as figures, with the pass
conditions this plan reads into them (§6, item 2):

| Row | PR | Passes when |
|---|---|---|
| Start from one slot | 1 | monotone, within 5% of the settled value in 20 samples |
| CPU per reconcile doubles | 1 | within 10% of the new target in 2 samples; no oscillation after |
| CPU limit halves | 1 | within 10% of the new target in 2 samples |
| CPU limit doubles | 1 | within 10% of the new target in 3 samples |
| Off-CPU wait doubles | 1 | mean CPU back within 85–95% within 15 samples |
| 10% noise on every sample's CPU | 1 | mean CPU 85–95%; slots' standard deviation under 0.3 |

**The review's cases,** each with its pass condition as §10 states it:

| Case | PR | Passes when |
|---|---|---|
| H1: 500m, 3ms of CPU per 0.8s reconcile, `B` 15 ms/s, idle samples with bursts to 60 ms/s | 1 | the slots reach the ceiling within 10 samples and never return to the floor |
| H1: the same, `B` noisy (sd 10 ms/s) against 3.75 ms/s per slot, 10 minutes | 1 | never below the ceiling after warm-up |
| H1: the measured workload with `B` 20 ms/s and idle bursts above `U(1)` | 1 | monotone from one slot to the target, mean CPU 85–95% |
| Fifth review, L1: the CPU change rule at steady state, with the measured workload's 0.47s reconciles and with 30s ones | 1 | the false-restart rate reported for each, and no slot rise caused by a restart |
| M1: a cheap queue (1ms of CPU, 2s each) at the ceiling, then an expensive one (23ms, 0.43s) taking the backlog | 1 | with the expensive queue seen uncapped before: the capped descent geometric at the rate §4.2 derives, `θ·D_mix/Dᵢ` per sample, out of the cap within 6 samples (the reviewer's example: 32 → 14.7 → 6.9 → 3.45, 5 samples at the cap, against about 30 at `θ`); at most one crossing of the target, then a monotone rise to it; never seen: a descent of at least `θ` per sample, and no rise while capped |
| M2: no `cpu.max`, a node model with CFS shares among other cgroups, the container's share stepping from 400 to 800 and back to 400 ms/s | 1 | at onset, one step to within 10% of the law's value at `ρL_eff`; stall over the threshold in at most 10% of samples at a steady share; a doubled share found within 10 windows |
| M2-a: a node that gives 4 cores, with a 1s blip to 0.5 core, and separately a 15s dip to 0.5 core | 1 | the blip learns no limit and blocks rises for its sample only; the dip learns one after `τ`, and once it ends the CPU is back to 90% of the pre-onset level within one clear window (scratch: 2 samples after the dip) |
| H2: a backlog from the start, no idle observation, caches of 100, 300 and 500 MB against budgets that fit them, 2 MB kept per reconcile | 2 | the fit identified within 120 GCs; `live` never over `H` by more than one reconcile's memory; 90% of `min(N_pred, ceiling)` within 120 GCs |
| H1-new: heterogeneous durations, the reviewer's mix: 150 fast Observes a second (50ms, 200 KB) and 2 creates a second (2s, 20 MB), against 256Mi with a Go limit | 2 | no OOMKill; `live` over `G` in at most 1% of GCs; over `H` in at most 10%; the slots never above the mean target `(H − U₀)/m` |
| Third review, M1: the same mix for at least four hours at steady state, each reconcile's lifetime modelled so that reconciles span GCs; run twice, once as given and once with the creates holding 30s instead of 2s | 2 | `live` over `G` in at most 0.1% of GCs, over `H` in at most 5%; no OOMKill; the fit used (current or kept) in at least 95% of GCs after the first identification; **false restarts at most one per hour, by each block check, and no downward exception taken**; the bandwidth `L` at least the longest hold in GCs; the lag-1 autocorrelation and `f` reported. With 30s creates the correlation lasts about 30 lags, which a one-lag factor would miss and batch means and Newey–West must not |
| Fifth review, M1: the reviewer's mix, a run cut into stretches with `n` from 1 to 39 measured increments in the window (by spacing the certain-rise pairs) | 2 | at every `n`, `g⁺` at least the stand-in and above 0; no fallback rise with `H − live < g⁺`; never over `G` |
| Fifth review, L1: a shift in spread only, the same mean per slot with twice the per-slot variance | 2 | an upward restart by the `z²` check within three blocks; none by the mean check |
| Fifth review, L3: a burst of heavy reconciles taking `live` over `H` | 2 | the cut frees at least the excess at the next GC in at least 97.5% of bursts |
| Third review, M2: caches shrinking from 100 to 30 MB during the ramp, and separately at a falling rate (exponential decay) | 2 | no rise in one GC larger than `(H − live)/g⁺`; never over `G`; no identification while `N` is explained by `t` and `t²` (the time-unexplained spread under one slot squared) |
| Third review, M2: a within-queue mix that lightens (creates' share 35% to 10% over 10 minutes), reconciles spanning GCs | 2 | a confirmed downward restart, and exactly one downward exception taken; the slots rise past the old `N_pred` under the data cap; never over `G` |
| Fourth review, L1: grants that always land after the sampler's read, so that no certain-rise pair ever occurs | 2 | `g⁺` from the stand-ins throughout (the fit's, else the `N⁺` pairs'); never over `G` |
| H1-new: the same mix with no spread at all (a fake holding the count fixed) | 2 | never identified; the fallback rules throughout; no OOMKill |

**Seeds.** Every stochastic case runs 20 seeds and must pass in all of them;
the rates (false restarts, excess) are pooled across seeds and must also pass
per seed. A case that passes in 19 of 20 is a failure, investigated before the
gate.

## 5. The complexity budget, decided from the simulation

§16 names five candidates for removal. Each is decided in PR 1 or 2 by a
comparison run of the same cases with the item and without it, and removed if
and only if the stated result holds in all 20 seeds:

| Candidate | PR | Comparison | Removed when |
|---|---|---|---|
| `Û₀` (only a metric) | 2 | none needed | the metrics review in PR 5 finds no use for it beyond `U₀_fit`; then the gauge reports `U₀_fit` and the bound goes |
| The quadratic time term | 2 | the two cache-drift cases (straight and exponential) and the four-hour runs, with and without `t²` | every pass condition holds without it, and the fit's share of GCs used drops by no more than 2 points |
| The variance model `s₀ + s₁N` | 2 | the heterogeneous and four-hour cases with plain weights `1/N` (plus the noise floor) | the excess over `H` and `G` stays within the bars and the prediction band's coverage stays within 96–99% |
| The `z²` block check | 2 | the spread-only case and the four-hour runs, with the mean check only | the mean check alone restarts within three blocks in the spread-only case. The design expects it won't, so this is expected to stay |
| `D_mix` on the CPU side | 1 | the M1 mix-shift case with a single `D̂` | not a simulation result: the single `D̂` reproduces the 18 samples by construction. Removed only if the consumer survey in PR 5 finds no Governor shared by queues of very different durations (git-provider shares one between `repo` and `localresource`) |

Any other item whose removal the statistical reviewer proposes is decided the
same way: a comparison run, and removal only if every pass condition holds
without it.

## 6. Ambiguities in the merged design, and my choice for each

1. **Where the harness lives.** §10 specifies harness changes, but the harness
   is only on the held #48 branch. I proposed a measurement branch. **Decided by
   the user: on `main`, as its own module** (PR H, then PR M).
2. **§4.3's table has figures, not pass conditions.** §10 says "every row of
   §4.3's table". Choice: the conditions in §4 above, set from the scratch
   figures with room for the real engine.
3. **Which samples count for `B̂`'s minimum.** §3.3 says "the last `W` idle
   samples" and "`B̂ = 0` while no idle sample is in the window", with
   `W = ⌈3τ/Δt⌉`. Choice: idle samples within the last `3τ` of time, so a
   controller busy for hours runs with `B̂ = 0`, which the proof allows.
4. **The unresolvable rule's pacing when the estimate is trusted.** §4.2 sends
   `g` to the ceiling "reached through §7's pacing", and §7 paces only untrusted
   rises. Choice: that path is always paced by completions, trusted or not.
5. **One observation per sample with several GCs in it.** Runtime metrics give
   only the latest cycle's live heap. Choice: one observation per sample in which
   the cycle count rose, as §3.6 says, with the latest values.
6. **Residuals "of the fit in use" when the fit is re-fitted every GC.** Choice:
   each residual is against the fit in use at its own observation, and the batch
   means use those residuals.
7. **The regression's window.** Weights `exp(−age/τ_m)` never reach zero.
   Choice: observations older than `3τ_m` (30 minutes) are dropped.
8. **The bandwidth `L` before any long reconcile ends.** Choice: the maximum of
   completed holds in the window and the age of the oldest hold still running
   (PR 3 reports both).
9. **Per-estimate or joint restarts on the CPU side.** §3.5 says "each
   estimate". Choice: each, except that a queue-share shift restarts `k̂`, `D̂`
   and `ĉ` together, as §3.5 says.
10. **`CPUPressureThreshold` keeps its name with a new reading** (avg10 → the
    1s stall share). Choice: keep the name, as §9 decided; the release note says
    the reading changed.
11. **Release coordination with the credential-use stack,** which also plans
    v1.13.0. **Decided by the user:** whichever stack finishes first takes
    v1.13.0, the other v1.14.0, and neither tags mid-stack.
12. **The memory input on a cgroup v1 node.** The design disables inputs whose
    files are missing. Choice: with no `memory.max` and no Go limit, the memory
    input is disabled with its reason, and CPU still applies.

## 7. Premises

### Verified now, at `d86c2b1`

- `localInFlight` changes at `governor.go:413` (grant, in `dispatchLocked`,
  `:406`) and `:397` (release, in `releaseLocal`, `:391`); `releaseLocal` is
  called from `queue.go:394` (`Done`) and `:307`, `:321` (hand-backs).
- `processing` is `map[T]struct{}` at `queue.go:116`.
- `NewGatedQueue(g, name, rateLimiter)` at `queue.go:46` carries the
  controller's name; `ForControllerRuntime` calls it at
  `pkg/controller/options.go:179`.
- `Governor.sample` at `governor.go:694` calls `g.sat.step(..., g.recovery)` at
  `:698`; `step` is `saturation.go:245`, `readPressure` `:355`, `ownCgroupDir`
  `:450`.
- `SaturationOptions` is set from `pkg/controller/options.go:92`, and the
  example sets `ExperimentalMemoryWorkingSet` at
  `examples/greeting-controller/main.go:205`.
- The local capacity gauge is registered at `pkg/telemetry/metrics.go:284` and
  observed at `:586`.
- #48's branch is at `c1c299c`; `newGoMemoryReader` is at `saturation.go:209`
  there, `applyGoMemoryLimit` at `:261`, `readMemoryMax` at `:589`.
- The harness (`hack/rlgov-measure`) is on that branch with its own `go.mod`,
  and not on `main`.
- `go.mod`: `go 1.26.0`, toolchain `go1.26.10`, controller-runtime `v0.25.2`.
- The runtime metrics, `cpu.stat`, `cpu.max` and PSI formats: as verified in the
  design's §2 and §14.

### To verify, in the PR that needs them

- The CFS model's fidelity: PR 1's engine test against §1's measured curve.
- `nr_throttled`'s exact counting (the design's §14): PR 4 reads it, and PR M's
  run logs it per sample.
- `/gc/gogc:percent` under `GOGC=off`: PR 4's test.
- `/proc/sys/kernel/sched_cfs_bandwidth_slice_us` readability in a container:
  PR 4 falls back to 5ms, and PR M records which was used.
- Which consumers set the removed fields: PR 5's scan.

## 8. Size estimates

| PR | Production | Tests | Notes |
|---|---|---|---|
| 1 | 1,300 | 2,400 | about 900 of the tests are the simulation engine |
| 2 | 1,200 | 2,300 | about 500 are the heap model |
| 3 | 250 | 600 | stress and drain tests |
| 4 | 450 | 600 | new fixtures |
| 5 | 700 | 900 | |
| 6 | — | — | about 400 doc lines |
| M | — | — | about 300 harness lines, off `main` |

The estimates come from the sketch of each function's inputs and outputs, and
from the gated queue's PR (#40) as a measure of tests per line for concurrency
code.

## 9. The user's decisions, 2026-11-09

The first version of this plan (`b98ff52`) put five items to the user. The user
approved the plan and decided them:

1. **Two simulation PRs,** CPU (PR 1) then memory (PR 2). The gate is passed only
   when both are merged and every case passes in all 20 seeds.
2. **The pass conditions read into §4.3's table,** as §4 states them.
3. **The release:** whichever stack finishes first, this one or credential-use,
   takes v1.13.0, and the other becomes v1.14.0. Neither tags mid-stack.
4. **The harness goes on `main`, as its own module:** `hack/rlgov-measure` with
   its own `go.mod`, the control in `ctrl/go.mod` and `ctrl/go.sum`, and the
   results committed there. It comes over from #48's branch (`c1c299c`) as its
   own PR, PR H, before PR M, with only the harness, scripts and existing
   results, and no `ratelimit` code.
5. **The cluster for PR M** will be asked for when its plan is ready.
