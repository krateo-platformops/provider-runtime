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
| 1 | `costmodel`: shared parts and the CPU path, the simulation engine, the CPU cases | §3.1–§3.5, §4.1–§4.3, §4.5, §5.1, §6, §7, §17, §10 (CPU rows) | 1,300 / 2,400 | — | no; **statistical review** |
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
- a node model with other cgroups and CFS weights, for §5.1's case with no CPU
  limit, where pressure only blocks (design §13 item 11);
- a gate with the gated queue's semantics (admission while in flight is under
  capacity, no preemption on a cut), per-queue workloads and a backlog;
- a driver that, every simulated second, closes the books at a read instant,
  builds a `Sample` and calls `costmodel`, and publishes the capacity back.

**Design sections:** §3.1–§3.5, §4.1–§4.3, §4.5, §5.1, §6, §7, §17, and §10's CPU
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
entry, with "Breaking, for an off-by-default feature" (§9, §13 item 6). The
configuration docs say "set a CPU limit to get CPU saturation control", and the
log entry states the behaviour change with no CPU limit: today's halving on
pressure goes away, and pressure only blocks rises (design §5.1, §13 item 11).

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

### 4.4 Conditions revised after the first simulation run

**Superseded by §4.5, and kept as history.** The statistical review found that
this section's upper band edge had no derivation and would have let two real
overshoots pass; the user replaced these conditions with the reviewer's.

**These conditions were revised after seeing results.** The first run of PR 1's
gate failed several of the conditions above. Reading the failures, the user
judged that some of this plan's readings of §10 ask for more than the design
promises, and decided (2026-11-09) that they be revised, each derived from a
guarantee the design states, not from the results, and judged by a statistical
reviewer who has not seen the results before the gate is run against them. So
this section gives no results. No code was changed to make a case pass: the
tests still assert the conditions above until the review. The derivations use
the design's sections (§3.5, §4.2, §4.3, §4.5, §5.1, §7) and its constants:
`ρ = 0.9`, `Δt = 1 s`, `τ = 10 s`, `D̂ ≈ 0.47 s` on the measured workload.

**Four quantities the conditions below use.**

1. **The estimate's noise, `r`.** `r` is the relative standard error of `k̂` that
   the estimator itself computes at each sample, `sd/(mean × √n_eff)` (§3.5). The
   bound: once trusted, `r ≤ (1 − ρ)/2 = 5%`, which is §3.5's trust condition, so
   it holds whenever the law's rises are not paced. Before the third sample after
   a start or restart there is no `r`, and no tolerance. The slots are
   `S = (ρL − B̂)/k̂`, so to first order their relative error is `k̂`'s; `B̂`'s
   error is left out, as it is 0 or a few ms/s against `ρL = 225 ms/s` in these
   cases. Two standard errors is the design's own convention (§3.5: "two standard
   errors fit inside the margin the target leaves").
2. **Small counts.** The plant runs at `⌊S⌋` and `⌊S⌋ + 1` slots, by error
   diffusion (§4.5). At the target, one more slot adds at most `k(S*)` of CPU,
   because `k` is non-increasing (§4.3, A2–A3). So a sample run at `⌊S*⌋ + 1` can
   reach the limit only when one slot's CPU is more than the target's margin,
   `k(S*) > (1 − ρ)L`. With `k(S*) = (ρL − B)/S*`, that is
   `S* < (ρL − B)/((1 − ρ)L)`, which is `ρ/(1 − ρ) = 9` slots at `B = 0`. Every
   CPU case in the gate is in this regime except the doubled limit (`S* ≈ 10`).
3. **The band, `b(S*)`.** At small counts a capped sample at the operating point
   is not oscillation: it is the actuator's quantum meeting the limit, and §4.3
   permits it to cross the target once, towards less CPU. Its value is at most
   `θN̄`, and under a backlog `N̄ ≥ ⌊S*⌋ ≥ S* − 1`, so its `θN̄` term lands at
   `θ(S* − 1)` or above: at most `b = θ + (1 − θ)S*` below the target, with
   `θ = (ρL − B̂)/(L − B̂)`. That is 1.2 slots at `S* = 2.8` and 1.4 at 4.6:
   about one slot. Above small counts no capped sample is expected at the target,
   and the band is the noise's, `b = 2r̄S* = (1 − ρ)S*` with `r̄` the trust bound.
   What the derivation does not bound: the capped rule's first term,
   `(ρL − B̂)/cᵢ × D_mix`, rests on one sample's completions and can land lower
   than `θN̄`. A sample below the band from that term counts as a failure, and
   would be a finding about the rule.
4. **Latency, `ℓ`.** A change in the cost of a reconcile shows only in reconciles
   started after it, and the first sample made only of them ends at most
   `⌈D̂/Δt⌉ + 1` samples after the change (one reconcile, plus the sample
   boundary): 2 here. A limit change shows in the sample that contains it,
   `ℓ = 1`; that sample restarts the CPU estimates, so it can't raise the slots
   (the restart rule of §3.5, which PR 1 also applies in the restart's own
   sample), and the derivation assumes nothing about where in it the change fell.

**Convergence from below, `c = 2`.** From any point at or above one slot, the
active law (smoothed, warm-started) is within 10% of the target after one step
and within 5% after two on the measured curve (§4.3's worked example: 1 → 4.17,
4.41 against 4.56, with `g'(S*) = 0.21`, so each step removes about 80% of what
is left). While untrusted the rise is paced, `Sᵢ ≤ Sᵢ₋₁ + Cᵢ` (§7), which
multiplies `S` by up to `1 + Δt/D̂ ≈ 3.1` per sample and doesn't bind for a rise
of a few slots. `c` assumes the changed plant's curve has the same shape as the
measured one, which holds in the simulation, where the change scales one
parameter.

| Condition | Original (§4 above) | Revised | Derivation |
|---|---|---|---|
| Monotone: the headline's approach from one slot; start from one slot; H1 with bursts above one slot; the mix shift's "monotone rise" | `S` never falls before it reaches 95% of the settled value (this plan's reading) | No sample is lower than an earlier one by more than their two noises: `Sᵢ ≥ Sⱼ(1 − 2rᵢ)/(1 + 2rⱼ)` for every `j < i`, until `S` first comes within `b(S*)` of the settled value. Strict where `r` doesn't exist yet | §4.3 proves the noise-free sequence monotone from below; the observed `Sᵢ` is within `2rᵢ` of it, so two observed values can differ by both errors. Monotonicity holds only while every sample is active: while `S < S* − 1`, the capacity `⌊S⌋ + 1 < S*` and no sample is capped (item 2). From one slot below the target, a capped sample is the permitted crossing, and the band applies. Once trusted, the tolerance is at least `(1 − 2r̄)/(1 + 2r̄) = 0.82` |
| Start from one slot | within 5% of the settled value in 20 samples | within `b(S*)` of the settled value in 20 samples | The band, item 3: 5% at 4.6 slots is a quarter of a slot, finer than the actuator. The count is unchanged: pacing from one slot takes `⌈ln S*/ln(1 + Δt/D̂)⌉ = 2` samples, plus one crossing, plus `c = 2`, is 5, inside 20 |
| CPU per reconcile doubles | within 10% of the new target in 2 samples; no oscillation after (read as within 15%) | within `b(S*)` of the new target in `ℓ + 1 + 2 + c = 7` samples; then the no-oscillation condition below | `ℓ = 2` for the new cost to show (item 4); 1 for the capped sample, which may cross the target once (§4.3); 2 for §3.5's change test on `k̂` once samples are active again: the first is held out (and, with `k̂` possibly stale, can't raise), the second restarts it (and can't raise in its own sample); then `c = 2` from below |
| CPU limit halves | within 10% of the new target in 2 samples | within `b(S*)` in `ℓ + 1 + 1 + c = 5` samples; then no oscillation | `ℓ = 1`, the sample with the change, which restarts and holds; 1 for the capped crossing; 1 more because every sample since the restart may have been capped, which feeds nothing to `k̂`, so the first active sample may find it empty and take §4.2's unresolvable rule (a rise paced by completions); then `c = 2` |
| CPU limit doubles | within 10% of the new target in 3 samples | within `b(S*)` in `ℓ + c = 3` samples; then no oscillation | `ℓ = 1`, the restart's sample; no crossing, as every sample is active below the target; `c = 2`. `S* ≈ 10` is not a small count, so `b = (1 − ρ)S*`, the original 10%. Unchanged in substance |
| No oscillation after a step (cost doubles, limit halves, limit doubles) | `S` stays within 15% of the target (this plan's reading) | after first reaching the band, `S` outside `b(S*)` of the target in at most 5% of the samples | The band covers a capped sample at the operating point (item 3) and the noise. Two standard errors are a 95% statement per sample (§3.5's convention), so a condition on every sample of hundreds asks for more than the bound gives; 5% is what it gives |
| Mix shift seen before: at most one crossing | at most one crossing of the target, counted with a ±5% band (this plan's reading) | at most one crossing, a crossing being a passage from one side of the band `b(S*)` around the target to the other | The target is known only to within the band: a value inside it is at the target. The crossing permitted is §4.3's, by a capped sample towards less CPU. "Out of the cap within 6 samples" is unchanged |
| Dip: recovery | the CPU back to 90% of the pre-onset level within one clear window of the dip's end (read as 11 samples) | within `1 + ⌈τ/Δt⌉ + R + 1` samples of the dip's end, `R = ⌈ln(S_rel/S_cut)/ln(1 + Δt/D̂)⌉` | 1 for the sample containing the dip's end, which may be over the threshold; `⌈τ/Δt⌉` for the clear window §5.1 requires before the first release (the window can't start before the dip ends, or before the limit is learned, which happens `τ` into a dip that spans `τ`); `R` for the ramp: a held limit's rises are paced (§7), from the slots at the release, `S_cut`, to the law's value at the released `L_eff = U_pre`, `S_rel`, at up to `1 + Δt/D̂` per sample; 1 for the slots admitted in the last ramp sample to show as CPU. The level is unchanged: after the release the law's target is `ρU_pre`, 90% of the pre-onset level, and the condition counts the first sample at or above it |
| Node share: stall over the threshold | in at most 10% of samples at a steady share, against `CPUPressureThreshold` (50) | in at most 10% of samples at a steady share, against **the threshold in force**, the one calibrated at the operating point while a limit is held | Not revised by the results' reading but by the design's amendment (design §5.2, decided by the user the same day): with no `cpu.max` the threshold is calibrated at the operating point, and "the threshold" is that one. The onset step and "a doubled share found within 10 windows" are unchanged |

**Unchanged:** the headline's settling (between 4 and 5 slots) and mean CPU
(within 3 points of 90%); off-CPU wait doubling; 10% usage noise; the H1 cases
other than the monotone approach; the false-restart case; the mix shift never
seen; the blip; the dip's learning after `τ`; the memory cases (PR 2); and the
seeds: 20, all of which must pass.

**For the reviewer.** The questions the user wants judged: whether `r` is the
right noise to use (the estimator's own, bounded by the trust condition), and
whether the pairwise form, applied to every pair of samples in an approach of up
to 20, needs a stricter `z` than two for the many comparisons; whether the
band's derivation in item 3 is right, including its regime boundary; whether the
counts in the step cases are each a guarantee of the design, or one of them
double-counts; and whether 5% outside the band is the right reading of two
standard errors for a sequence whose errors are correlated by the smoothing.

### 4.5 Conditions derived blind by the statistical reviewer

**These are the gate's conditions.** They replace §4.4's. The statistical
reviewer derived them from the design and from §4.4's derivations without seeing
any result, and the user adopted them on 2026-11-09. The reviewer then gave the
exact text of the counts and of the conditions §4.4 carried, and two
corrections; where any of that was changed after the reviewer had seen results,
the condition says so and says what the change does to the results. This
plan's author has changed none of it.

**The target, `S*`, from the engine's fixed-capacity curve.** Not a reference
run's settled `S`, which carries the controller under test. The engine runs the
workload at each whole capacity with no model (`Config.Fixed`), and `S*` is the
point where the measured `U` crosses `ρL`, interpolated between the two whole
capacities on either side. The reviewer's values: 4.73 on the measured workload,
2.83 with the CPU per reconcile doubled, 2.81 with the limit halved, 8.72 with
the limit doubled. The gate computes them from the engine for each seed and
reports them beside these values.

**The band, asymmetric.** With `r̄ = (1 − ρ)/2`, the trust bound of §3.5:

- **Lower edge `θ⌊S*⌋`.** A capped sample at the operating point is the
  actuator's quantum meeting the limit; under a backlog its `N̄ ≥ ⌊S*⌋`, so its
  `θN̄` term lands at `θ⌊S*⌋` or above (§4.4, item 3, tightened from
  `θ(S* − 1)` to the whole number).
- **Upper edge `S*(1 + 3r̄)`.** Above the target there is no permitted crossing
  (§4.3: a capped sample crosses only towards less CPU), so the only excursion
  above is the estimate's noise, bounded at three standard errors at the trust
  bound. §4.4's symmetric band had no derivation for its upper edge.

**Outside the band: at most 5% of samples, at `z = 3`.** After the band is first
reached, `S` is outside it in at most 5% of the samples, with the edges at three
standard errors as above. Untrusted samples after a restart are judged with
their own relative standard error when `n ≥ 3`; before that they are excluded,
and their number is reported.

**Monotone, attributed.** Before the band is first reached, every fall of `S` is
attributed: either it is in a capped sample (the crossing §4.3 permits; every
capped fall is accepted), or it is in an active sample with `U > ρL` (the sign
guard) and, while trusted, the fall is at most `3r̄` relative,
`Sᵢ ≥ Sᵢ₋₁(1 − 3r̄)`. While untrusted, a fall in an active sample needs `U > ρL`
and is reported. A fall that is none of these fails. The bound is on the fall's
size. (The acceptance of every capped fall is a correction the reviewer made
after results, for the real caps of the H1 cases.)

**Each step case asserts the mechanism it counts on.** The count of samples is
a sum of mechanisms, and each case asserts that the mechanisms happened, not
only that the count was met. A case that meets its count by another path fails.

**The counts, for the design as amended by §17** (the reviewer's text,
verbatim, from the re-review of design `9b861b7`).

- **`c`, frozen before any run:** on the case plant's fixed-capacity curve (each
  whole capacity via `Config.Fixed`; `U(S)`, `D(S) = ΣA/ΣC`, interpolated),
  iterate the noise-free law `S' = min(S + S·Δt/D(S), (ρL − B̂)·D(S)/c)`. The
  count is `n_iter·(1 + ⌈D̂/Δt⌉)`, computed per plant, frozen and committed before
  any run, and disclosed as a noise-free lower bound.
- **Start from one slot:** within the band by `c₁ + 1`, where
  `c₁ = n_iter(1 → θ⌊S*⌋)·(1 + ⌈D̂/Δt⌉)`, plus 1 for the crossing. That's 5 if
  `n_iter = 2`; otherwise it follows the frozen `n_iter`.
- **Cost doubles:** into the band within `ℓ + ⌈W_c/Δt⌉ = 4` (`ℓ = 2`,
  `W_c ≈ 2s`). Entry is from above. Mechanisms asserted: a capped sample within
  `ℓ`; a `ĉ` deferral, then a restart, with no rise in either; `D̂`'s warm-start
  restart at the cut's `N̄` move. No `c` term. Derived from §17 alone (the
  reviewer's note).
- **Limit halves:** into the band within `ℓ + 1 = 2`.
  `S₁ = (ρL' − B̂)N̄₁/(min(U₁, L') − B̂)`, then the first wholly-new-limit capped
  sample's first term. Then a hold of up to `⌈D̂/Δt⌉ + 1`. No `c` term.
- **Limit doubles:** within 10% of the new target in 3 samples if the frozen
  `n_iter` is 1; otherwise `3 + (n_iter − 1)·(1 + ⌈D̂/Δt⌉)`.
- **Long drain:** into the band within `ℓ_min + (2k_θ − 1)`, with
  `k_θ = ⌈ln(N̄₀/(S*'(1 + 3r̄)))/ln(1/θ)⌉`. After entry, at most 5% outside. No
  rise in any sample with `N⁻ > capacity`. Assert `D̂`'s warm-start restart at the
  cut. Zero drained samples makes the case INVALID, not passed. Report the
  Little's fallback uses and `ĉ`'s restarts.
- **Rising cost (×(1 + 0.01·N̄)):** `S*` from this plant's own curve. Mean CPU
  over the last 120s within 3 points of `ρL`. Into the band within `c₁ + 1`.
  Attributed monotone, at most 5% outside. Report max `S` against the predicted
  `0.01·(S* − 1)·S*`. `S` never exceeds `S*(1 + 3r̄)`. If it fails, the remedy is
  a design change for the user, not a condition change.
- **`ĉ`'s restarts** that weren't caused by a scenario change are reported in
  the start, headline and mix cases (design §17.1).

**Superseded counts, under the first design**, kept as history with their
disclosure:

- Cost doubles: within `ℓ + 2 + c` samples, 6 with `ℓ = ⌈D̂/Δt⌉ + 1 = 2`
  and `c = 2`. The permitted crossing falls inside `ℓ`: a mixed sample caps once
  about 7% of its CPU is at the new cost (`0.225·(1 + f) ≥ L − slack/Δt` gives
  `f ≥ 0.07`). The 2 is §3.5's change test, a held-out sample and then a
  restart. Mechanisms asserted: a capped sample within the first `ℓ`, then a
  held-out `k̂` sample, then a `k̂` restart.
- Limit halves: within `ℓ + 1 + c` samples, 4 with `ℓ = 1` and `c = 2`. `ℓ`
  is the change's own sample (a restart, which can't raise); the 1 is the first
  sample wholly at the new limit, which caps (a worst-case term, not a double
  count). There is no unresolvable-rule step: it has no path in the code (an
  active sample folds into `k̂` before the law reads it, and `2·SE` needs
  `n ≥ 3`). Mechanisms asserted: a restart in the change's sample, and a capped
  sample within `ℓ + 1`.
- Limit doubles keeps the original condition, within 10% of the new target
  in 3 samples: the reviewer withdrew its blind loosening of it.
- §4.4's sums double-count the crossing (cost doubles) and add a step with no
  mechanism (limit halves).
- **Disclosure, after results:** these counts fail the cost-doubling case at
  seeds 2, 3 and 15 and the limit-halving case at seeds 8, 9 and 12, each from
  a missing mechanism or a late entry caused by the review's findings 1 and 2,
  not by the count.
- Start from one slot: within the band by sample 5, then the 5%-outside rule.
  That is `⌈ln S*/ln(1 + Δt/D̂)⌉ = 2`, plus 1 for a permitted capped crossing at
  `⌊S⌋ + 1`, plus `c = 2`. It doesn't wait for the review's finding 6,
  because entering the band only needs `S ≥ θ⌊S*⌋`. This
  replaces "20 samples", and it is in the reviewer's set, derived blind as 5.
  **Disclosure:** the band was entered by sample 3; 1 of 20 seeds left the band
  between samples 5 and 30, not investigated.

**The mix shift, seen before: at most one crossing, and it must be downward**,
from above `S*(1 + 3r̄)` to below `θ⌊S*⌋`, with `S*` the engine's
fixed-capacity value for the expensive queue alone (4.73). After it, the
attributed monotone condition applies until the band. "Out of the cap within 6
samples" is unchanged.

**Removed with the learned limit** (design §5.1, §13 item 11: CPU saturation
control requires a CPU limit): the node case and the node case's onset step,
with their conditions as the reviewer gave them (this file at `4bc4481`). **The blip:** dropped
(its property is the new case's invariant). **In their place, no CPU limit,
pressure only blocks:** on the node model with the container's share stepping
from 400 to 800 and back to 400 ms/s, no sample's slots fall because of
pressure; a rise is blocked in every sample over `CPUPressureThreshold` and in
no other; and `S_cpu` is the ceiling throughout.

**The dip: replaced with "Contention below the quota".** The measured workload
at 250m on the node model, with the node giving 125 ms/s for 15s, then the full
quota. During it, rises are blocked in every sample over CPUPressureThreshold in
which no period was throttled, and in no other (design §17.9 item 8), and no
fall is attributed to pressure. After it, `S` is above
`S*(1 + 3r̄)` in no sample with item 2's exclusion (design §17.1: lives that
overlapped a sample over `CPUPressureThreshold` are excluded from `D̂`); without
it, at most `1 + (⌈D̂/Δt⌉ + 1)` samples, with the excursion's size reported.

**The baseline's tuning rule:**

- a grid of `K_p` and `K_i` on a 13-point log grid over `[0.1, 100]` (`K_i` also
  0), with `τ_U ∈ {0, 1, 2, 5, 10}s`, committed before any run;
- tune on the headline case with seeds 101–120;
- choose lexicographically: passes the headline's §4.5 conditions in all 20
  tuning seeds (entry within 20 samples); then the smallest mean entry time;
  then the smallest variance after entry. If none passes, choose by the smallest
  fraction outside and report that;
- commit the gains, then run every case at them and at ×0.5 and ×2;
- judge it on the same numeric conditions, without the cost model's mechanism
  assertions. Report side by side.

**Unchanged:** the conditions §4.4 lists as unchanged, and the seeds: 20, all of
which must pass.

**Corrected after seeing results, by the reviewer** (the statistical review of
the gate's first run, `4fdd385`). Each is a correction of the reviewer's own
conditions above, made after the reviewer had seen the results. The conditions
are the reviewer's; the sentence after each, on what it corrects and why, is the
plan author's summary of the reviewer's reasons.

- **(a) Start from one slot, and the rising cost's entry:** `c₁` is frozen from an
  iteration that includes the predicted excess of `ĉ` while reconciles are in
  flight, `ĉₙ ≤ c·(1 + N̄ₙ/ΣCₙ)`, or the entry allows `c₁ + 1 + ⌈D̂/Δt⌉`. The
  noise-free iteration assumed `ĉ` exact from the first sample, which §17.1's
  in-flight term makes it not. (The first form, `c·(1 + N̄ₙ·D/ΣCₙ)`, had a units
  error, corrected by the reviewer at the review of `a1b144f`: there is no `D`.)
- **(b) Limit halves:** "a capped sample at its first term, **or an active sample
  at the law with `D̂` restarted under the new limit**, within `ℓ + 1`". The
  reason, the reviewer's: "The first sample wholly at the new limit need not read
  capped (not every period throttles; under §17.8 its window must cap too), and
  the active law with `D̂` restarted under the new limit is then the mechanism."
- **(c) Limit halves, `S₁`:** `S₁ = min(S₀, (ρL' − B̂)N̄₁/(min(U₁, L') − B̂))` when
  `U₁ > ρL'`, and `S₁ = S₀` otherwise. The condition `S₁ ≤ θ'N̄₁` can hold only
  when `U₁ ≥ L'`, since the L11 value is at least `θ'N̄₁` whenever `U₁ < L'`; this
  states what §17.2 computes, with the sign guard.
- **(d) Rising cost, max `S`:** untrusted samples are judged at
  `max(own SE, r̄)×3`, or excluded and counted, as in the outside rule; the
  condition had judged every sample at `r̄`.
- **(e) Contention below the quota:** "during the dip, no rise in a sample over
  `CPUPressureThreshold`; after the dip's end, `S` above `S*(1 + 3r̄)` in at most
  `2k_θ − 1 + (n_W − 1)` samples, `k_θ` from the slots at the dip's end", with
  `k_θ = ⌈ln(S_end/(S*(1 + 3r̄)))/ln(1/θ)⌉`. The dip's stall distribution is
  reported. (`k_θ`'s formula is the long drain's, with the slots at the dip's end
  in place of `N̄₀`: confirmed by the reviewer at the review of `a1b144f`.) The reason, the reviewer's: "some
  of the dip's samples stall under CPUPressureThreshold, so their stretched lives
  qualify for D̂ and the law may rise in them, since §6 blocks only above the
  threshold; after the dip the excess descends at the capped rule's pace."
- **(f) Mix shift, "out of the cap":** within
  `min(⌈ln(32/S*)/ln(1/θ)⌉, ⌈W/Δt⌉ + 1) + (n_W − 1)` samples, not a fixed 6. The
  min is the **faster** of the two mechanisms, the `θ` descent from the ceiling
  and the first term's descent within the window's latency; `n_W − 1` is the
  window's own latency in deciding the cap (design §17.8, item 1).

**Also corrected by the reviewer:**

- **Every count is in samples after the change's own sample `i0`.** Limit halves
  becomes "within 1 after `i0`".
- **`ℓ_min` is asserted as a measured value**: the samples from the change to the
  first window-capped sample after it, measured and asserted. It is **not** the
  samples until the first sample whose lives are all at the new cost, which is
  about 34 samples in the long drain and far too lenient (corrected at the review
  of `a1b144f`).
- **The long drain's precondition:** the run is at steady state before the change,
  at least `τ_ĉ` plus 60 completions after entering the 30s plant's band, checked;
  and the `k_θ` precondition (`N̄₀ > S*'(1 + 3r̄)`, so that there is a descent to
  count) is tested. A run that fails either is INVALID, not passed.
- **The PI baseline's falls are attributed by its filtered `Ū`**, the value it
  acts on, not the raw `U`.
- **H1 with bursts above one slot:** the mean CPU is reported against the
  predicted setpoint shift (design §17.1, capped samples at the operating point);
  the 85–95% bar was never re-derived for §17's law, and it is reported, not
  judged, until it is.
- **H1's single trusted fall of 32%** (the first run, one seed) is traced in the
  report, sample by sample.
- **The no-limit case adds a share near the demand**, so that the stall straddles
  `CPUPressureThreshold` and the block is tested on both sides of it.

**The counts under design §17.8**, the reviewer's text verbatim (the review of
`a1b144f`), replacing the placeholders the plan author had put here:

- **Cost doubles:** into the band within `ℓ + 2n_W − 1` samples (5 with `ℓ = 2`
  and `n_W = 2`). Mechanisms: a window-capped sample within `ℓ + n_W − 1`; a `ĉ`
  deferral then a restart within `2⌈W/Δt⌉ + 1`, with no rise while pending; `D̂`'s
  rewarm at the cut. The change-test mechanism applies to a change of at least
  `3σ_W` in a window holding new lives (report `σ_W`), and is `3⌈W/Δt⌉ + 1` for a
  change near `3σ_W`.
- **Limit halves:** within 1 after `i0`, `W` judged against the current limit.
- **Contention:** at most `2k_θ − 1 + (n_W − 1)` samples above `S*(1 + 3r̄)` after
  the dip.
- **Mix shift:** as in (f) above.
- **`n_W`** is measured and reported at the first window-capped sample.
- **The band's lower edge after a cost increase** is design §17.8 item 4's,
  frozen from the plant. **"False restarts, 30s"** is INVALID unless `S` reaches
  its plant's band first (design §17.8, item 1).

**Corrected after seeing results, by the reviewer, at the review of the gate
at `d4083a4`.** Each is labelled so; the conditions are the reviewer's.

- **(a) Cost doubles, the change test's mechanism:** the deferral-then-restart
  within `2⌈W_test/Δt⌉ + 1` samples, `W_test` the longer of the deferring and the
  confirming windows, measured in the run (not `n_W` at the first window-capped
  sample).
- **(e) Contention below the quota:** with design §17.9 item 5's rewarm, only
  §4.3's convergence from above is allowed after the dip, the allowance below.
  (The alternative, an excursion bounded from the post-dip peak, is dropped at
  the review of `fe08b9a`.)
- **§4.3's convergence from above, near the knee,** is an allowance frozen from
  the plant's curve as `c` is, `n_above·(1 + ⌈D̂/Δt⌉)` samples (the plan author's
  reading, confirmed by the reviewer at the review of `fe08b9a`, with a frozen
  start): n_above = the steps of the noise-free law on the plant's curve from
  S_start = X*·D(⌈S*(1 + 3r̄)⌉), the law's value from lives one whole capacity
  above the band's top, down to S*(1 + 3r̄); frozen and committed per plant before
  any run.
- **(i) The no-limit case:** its third share is moved closer to the demand, so
  that the stall straddles 50.
- **The baselines in the no-limit case** are reported, not judged: design §5.1
  decides no CPU control without a limit, so the case's conditions describe the
  cost model's decision, not a requirement on a different controller.

**The gate's requirements from here** (the user's, through the coordinator):

- **The cost model passes every case in all 20 seeds, with zero invalid seeds**
  in the 30s cases. An invalid seed is not a pass.
- **Confirmation on fresh seeds 21–40.** The design and the conditions have been
  tuned against seeds 1–20 three times; the confirmation checks that mechanisms
  were fixed, not seeds (the review of `fe08b9a`, verbatim):
  (i) A commit, named in the report, freezes the seed list (21–40), the
  conditions, and the code under test (pkg/ratelimit/internal/costmodel and its
  sim package). The run is at that commit, or at a descendant whose diff under
  those paths is empty, and the report states which.
  (ii) No test, tool, explore run or committed file uses seeds 21–40 before that
  commit. That can be checked by searching the history for them.
  (iii) The report includes all 20 seeds, with nothing excluded. An invalid seed
  is a failure.
  (iv) Any failure fails the confirmation. Seeds 21–40 are then spent: a later
  confirmation uses 41–60, after a reviewed design or condition change. They are
  never re-run with 21–40 to pass.
- **The reservation gate's showing reflects Temporal's design as modelled.** The
  reviewer's caveat: the engine's reconciles, with their CPU first and their
  phases aligned at admission, are probably burstier at 100 ms than real ones.

**Corrected after seeing results, by the reviewer, at the review of
`0dfa94e`.** The conditions are the reviewer's; each is labelled so.

- **Every frozen `n_iter` count gets one extra step**, `(1 + ⌈D̂/Δt⌉)` samples.
- **Limit doubles:** within 10% of `S*'` in `1 + 2·(1 + ⌈D̂/Δt⌉) = 5` samples.
- **Limit halves:** within `1 + (⌈D̂'/Δt⌉ + 1) = 3` samples after `i0`.
- **Cost doubles** uses the measured `ℓ`, the first per-sample tight sample
  after the change, plus `n_W − 1`.
- **The long drain's count** is re-derived after design §17.10's (A)–(C),
  including the latency until post-change completions (about one lifetime at the
  cap, from the frozen curve) and the first window-capped sample's first term.

**Updated at the review of `caebca3`, the reviewer's text verbatim:**

- **Contention:** "rises are blocked in every sample of a contention run, design
  §17.10 (D), and in no other; no fall is attributed to pressure; after the dip, S
  is above S*(1 + 3r̄) in at most the frozen allowance n_above·(1 + ⌈D̂/Δt⌉)
  samples."
- **No limit:** "blocked in every sample over the threshold and in no other"
  becomes "blocked in every sample of a run, a run ending once W has passed with
  no sample over the threshold, and in no other".
- **Long drain**, replacing `ℓ_min + 2k_θ − 1`: into the band within
  ⌈(D'_cap + |W'|)/Δt⌉ + 1 samples of the change.
  - D'_cap = N₀/X'_cap is the life at the cap: N₀ = ⌈S_pre⌉, and X'_cap is the
    drain plant's throughput at the cap, frozen from its curve (about
    0.25/3.0 ≈ 0.083/s).
  - |W'| = ⌈D'_cap/Δt⌉ + 1 is the window while capped.
  - With N₀ = 7 that is about 84 + 85 + 1 ≈ 170 samples: the new cost reaches c_W
    only once W holds post-change completions.
  - Until then, (A) allows no rise and the first term descends as c_W turns over.
    During the transient the lower edge is design §17.8 item 4's floor, frozen
    from the drain plant.
  - Mechanisms asserted: no rise from the change to entry, per (A); at least one
    capped sample at its first term with no completion of its own, per (B); the
    source of D_mix reported (D̂_q, or the D̂ fallback of (C)).
- **The no-limit case's third share** is derived and frozen with the plants: the
  share at which the engine's fixed-capacity stall at the demand crosses
  `CPUPressureThreshold`. It replaces the 1.2 cores picked from a report on seeds
  1–20.
- **False restarts at 0.47s** are reported (2–9 an hour, from autocorrelated
  window means): they cost holds, never rises.

**Corrected after seeing results, at the review of `0ce649f`** (the reviewer's
text; the long drain by the user's decision of 2026-11-10):

- **Attribution by `U_W`** (replacing the monotone condition's second
  alternative): "... or it is in an active sample with U_W > ρL (the sign guard
  reads the window, design §17.10 (A)) and, while trusted, the fall is at most 3r̄
  relative. While untrusted, a fall needs U_W > ρL and is reported." The level PI
  is attributed by its `Ū`, the reservation gate by its reading.
- **The restart-edge floor:** "after ĉ's restart, until D'_cap + |W'| has passed
  (frozen: the life and the window at the cap), the lower edge is design §17.8
  item 4's frozen floor".
- **Long drain, the user's decision: safety judged, entry time reported, not
  judged.** The safety conditions:
  - from the change to entry, no rise in a sample with `U_W > ρL`, nor while ĉ's
    test is pending;
  - `S` never above `S*'(1 + 3r̄)` once ĉ has restarted;
  - no sample below the restart-edge floor;
  - either path before entry: a ĉ restart, or a capped sample at its first term
    with no completion of its own.
  The regime is documented in design §17.10. Deriving a defensible entry bound for
  it is a tracked follow-up, due before the GKE re-measure (§6, item 13).
- **Cost doubles:**
  - into the band within `max(ℓ + 2n_W − 1, 2⌈W_test/Δt⌉ + 1)`;
  - either path: a window-capped sample within `ℓ + n_W − 1`, or a ĉ restart
    within `2⌈W_test/Δt⌉ + 1`;
  - the rewarm check extends to entry + 2.
- **No limit:** the third share is replaced by an **alternation**, frozen from the
  node curve: `s_over` with mean stall at least `T + 3σ_s` and `s_under` with mean
  stall at most `T − 3σ_s` (`σ_s` from the same fixed-capacity runs), switching
  every `3·W`. The condition: "blocked in every sample of a contention run (design
  §17.10 (D)) and in no other; at least one run begins and ends in each pair of
  stretches".

**Corrected after seeing results, at the review of `b681ff7`** (the reviewer's
text; the controller unchanged):

- **(a) Long drain, the band:** "From ĉ's first restart after the change (or, if ĉ
  never restarts, from entry), S is outside the band in at most 5% of samples,
  untrusted samples judged at max(own SE, r̄)×3 or excluded and counted, as in the
  outside rule. The rise rule stays absolute (no rise with U_W > ρL, nor while ĉ's
  test is pending), and so does the restart-edge floor." With no entry, the band
  rule doesn't apply. (This also removes the failure at the change sample, where
  the restart-edge window began at the change when there was no restart.)
- **(c) Long drain, the path:** the either-path assertion is dropped, and the path
  is reported: "ĉ restart / first term / EWMA only".
- **(f) Mix shift seen:** "out of the cap within min(⌈ln(32/S*)/ln(1/θ)⌉,
  ⌈(D_out + |W|)/Δt⌉ + 1) + (n_W − 1) samples, D_out the longest lifetime among
  the outgoing queue's completions after the shift, measured: a property of that
  queue's work, not of the controller."
- **No-limit validity guard:** "the case is INVALID unless at least 25% of its
  samples are over the threshold and at least 25% under, and a run begins and
  ends in every pair of stretches."
- **The guard's scope, corrected after seeing results** (the reviewer, after the
  run at `c1acaa4`): "INVALID unless, within the alternation stretches, at least
  25% of samples are over the threshold and at least 25% under, and a run begins
  and ends in every pair." (The first form counted the whole case, whose 0.4- and
  0.8-core phases are entirely over the threshold, which left it at the 75% edge.)
- **Held until the GKE plant-curve measurement** (the user's decision,
  2026-11-10): the determinacy rule for the long drain's target, and the long
  drain's band. The engine's drain-plant curve gives `S*` 3.9, 4.8 and 4.3 on
  seeds 9001–9003, non-monotone, with `U` nearly flat near the target; the real
  curve is measured first (`hack/rlgov-measure/results/curve-settings.txt`).

**Proposed after the GKE plant curve, 2026-11-11, revised after its review: for
review before any code or gate run.** The measurement found the real drain
plant's target at `S* = 4.14`, bootstrap 95% [3.92, 4.38]
(`hack/rlgov-measure/results/curve-report.txt`; the audit is `curve-audit.md`).
The gate's long-drain trajectories come from the engine's drain plant, so the
first step was to check whether that plant has the measured target. The engine's
curves are in `pkg/ratelimit/internal/costmodel/sim/testdata/curve-engine-b2e5757.txt`,
reproduced by `TestCurveDiag` (`SIM_CURVEDIAG=1`, seeds 9001–9010; none of 1–40
used). The first version of this proposal (`73e1d72`) judged the band with 4.14.
Its review found the calibration sound, with one High and four Medium findings,
and the user then decided the two questions it left open. This version
supersedes it.

**The engine's drain plant at the committed parameters.** Under the GKE runs'
protocol (300 s warm-up, then 1800 s measured), with ten seeds per `n`, compared
with the GKE means in cores. The engine's interval is `t` on 9 df. The GKE sd is
pooled: 0.0058 on 11 df at `n` = 3–5, and 0.0022 on 4 df at `n` = 6–7. The
difference's 90% interval is Welch's.

| n | engine, mean ± 95% | GKE mean (k) | engine − GKE, 90% |
|---|---|---|---|
| 3 | 0.1894 ± 0.0049 | 0.1876 (2) | +0.0019 [−0.0063, +0.0100] |
| 4 | 0.2150 ± 0.0056 | 0.2230 (6) | −0.0079 [−0.0139, −0.0020] |
| 5 | 0.2306 ± 0.0063 | 0.2371 (6) | −0.0065 [−0.0128, −0.0001] |
| 6 | 0.2417 ± 0.0046 | 0.2428 (4) | −0.0011 [−0.0053, +0.0030] |
| 7 | 0.2461 ± 0.0023 | 0.2466 (2) | −0.0005 [−0.0041, +0.0030] |
| 8 | 0.2482 ± 0.0009 | 0.2500 (2) | reported: GKE's two runs read the cap exactly |

- **The engine's own `S*`** is the crossing of its ten-seed mean curve:
  **4.64**, with a seed bootstrap 95% interval of **[4.40, 4.99]** (10,000
  resamples, the curve re-interpolated each time). That interval is disjoint from
  the measured one. A delta-method interval isn't used, because the means at `n`
  = 4 and 5 come from the same seeds.
- **The per-seed values** run from 3.89 to 5.42. The frozen 3.90 is seed 9001's,
  the lowest of the ten. Its band's upper edge, 4.49, is under the engine's own
  target. That may explain the long drain's 5 failures in 20; it is a hypothesis,
  not traced.
- **Seed 9003.** The freeze's method gives 3.905, 4.822 and 4.492 on seeds
  9001–9003, and the review of `c1acaa4` quoted 4.3 for 9003. The code under
  `pkg/ratelimit/internal/costmodel` is identical between `c1acaa4` and
  `b2e5757`, so this isn't an engine change; it is a difference of method or
  window. Neither protocol reproduces 4.3: seed 9003's curve crosses `ρL` at 4.492
  under the freeze's method (and again at 5.72 and 6.10, a non-monotone curve), and
  at 4.432 under the GKE protocol.
- **The 0.50-slot gap, decomposed with a reference model.** The reference is
  mean-value analysis (MVA) of the processor-sharing closed queue: think time
  28.5 s, service `c/L`, exact for processor sharing.
  - About 0.15 comes from `c`: MVA's `S*` is 4.48 at 3.000 s and 4.33 at 3.157 s.
  - About 0.16 is the engine's departure from processor sharing at the same `c`:
    4.64 against MVA's 4.48. Per `n`, the engine reads 0.0057 above MVA at `n` = 3
    and 0.0010–0.0042 below it at 4–7.
  - About 0.19 is GKE below the stationary processor-sharing prediction, 4.14
    against 4.33. That lies inside GKE's own interval, and MVA at 3.157 s is
    within 0.004 cores of GKE's means at every `n`.
- **Noise.** The engine's between-run sd at `n` = 4 and 5 (0.0078, 0.0088)
  against GKE's (0.0065, 0.0055) is not a significant difference: the F-tests give
  p of 0.14–0.36. It is reported, and not calibrated.
- **The CPU per completion.** It is 3.157 s on GKE (22 runs, sd 0.036 s,
  3.12–3.18 s by `n`), against the engine's 3.00 s. It decomposes into 3012 ms
  drawn (the GKE runs' realized burn requests), 10 ms of edge effect, and
  134 ± 8 ms outside the burn: the controller's own per-reconcile work and the Go
  runtime. It is consistent with a per-reconcile cost; a background of up to
  about 0.009 cores isn't excluded, and under processor sharing the difference
  doesn't move the stationary `U` (the review's L10).

**The decisions** (the user's, 2026-11-11, on this proposal's review):

1. **The long drain's band is judged with the calibrated engine's own `S*`.**
   That is the crossing of its forty-seed mean curve under the GKE protocol
   (item 3),
   provided the measurement's pre-registered rule determines it: the seed
   bootstrap's 95% interval must be narrower than `0.15·S*`. If the rule doesn't
   determine it, the band is reported, not judged. This holds whether or not the
   fidelity tests below pass. **The GKE measurement only reports the engine
   plant's fidelity**; 4.14 enters no condition. On the uncalibrated plant the
   interval is 0.59 wide against 0.70 at ten seeds, so determined.
2. **Both drain plants get the same absolute per-reconcile excess**, the
   controller's own work, which doesn't scale with the burn.
3. **The drain plants' freeze uses forty seeds, 9001–9040** (the re-check's M5,
   decided by the user on 2026-11-11, before any run of the calibrated plants).
   This replaces ten seeds; nothing else changes. Seeds 1–40 stay the gate's,
   21–40 reserved for the confirmation. The predicted seed-bootstrap width at
   forty seeds is about 0.27 slots (the auditor's estimate; 0.59·√(10/40) ≈ 0.30),
   with a standard error of about 0.07, against `0.15·S*` ≈ 0.67.

**The calibration, from directly measured quantities, not fitted to `U`:**

- **`drainHeavy`'s CPU per reconcile becomes 3157 ms**, the GKE runs' mean cgroup
  CPU per completion, which reproduces what GKE produced. Its expected value with
  the design's 3000 ms draws is 3134 ms; at MVA's slope the difference is about
  0.02 slots (MVA 4.325 against 4.347).
- **`drainBase` (`drain0`) becomes 1657 ms**: 1500 + 157, the same absolute
  excess as the drain plant's, so the long drain's cost change is ×1.905
  (1657 → 3157 ms). The +157 ms includes about 22 ms that is not the controller's
  work: GKE's draw luck (its burn requests averaged 3012 ms, not 3000) and the
  window-edge effect (about 10 ms). 1634 ms would be the expected value. 1657
  stands, by the user's same-absolute-excess decision (the re-check's L11).
  Disclosed: the pair with the expected-value excess of
  134 ms, 1634 and 3134 ms, differs by about 0.02 slots in the drain plant's `S*`
  (MVA 4.325 against 4.347) and about 0.06 in `drain0`'s (MVA 6.65 against
  6.71); neither changes `N₀`.
- **Nothing else changes**: the waits (28,500 ms), the jitter (0.1), the limit
  and `Procs` stay.
- **Disclosed:** `c` comes from the same 22 runs the fidelity test compares
  against. Its standard error is 0.3%, so the dependence is small, but the
  fidelity test is not independent of the calibration.
- **Not fitted**, for two reasons. A fit to the per-`n` means would make the match
  true by construction. And a `c` that fits `U` but differs from the `c` measured
  directly would be the model's misfit, which a fit would hide.

**The fidelity tests, pre-registered here, run once at the freeze, reported and
not judged.** Each drain plant's curve is run at `n` = 1..12 on seeds 9001–9040
under the GKE protocol; per `n`, the mean of the forty run means is the mean
curve. The bootstrap resamples the forty seeds with replacement, 10,000 times,
with the simulation's generator seeded 20261111. It re-interpolates the curve each
time, and takes the 2.5% and 97.5% order statistics; a resample with no crossing
sorts to the end it lies at, as in `curve_report.py`. One run computes the frozen
values, the determinacy decision and these tests.

- **(i) Equivalence with GKE in `U`**, at `n` = 3–7. The margin is
  `δ = r̄·ρL = 0.05 × 0.225 = 0.0113` cores at every `n`, fixed now.
  - Why that margin: the design's guarantee is about `U`, and `r̄` is the relative
    error in the law's value it accepts (§3.5's trust bound). At the knee's slope,
    about 0.0156 cores per slot, `δ` is about 0.72 slots, roughly the band's upper
    half-width. So equivalence at `δ` says the plants agree in `U` within the
    design's tolerance; it does not bound their `S*` tightly.
  - The test is two one-sided tests at α = 0.05 per `n`: the 90% Welch interval of
    engine − GKE (engine sd on 39 df, GKE's pooled sd as in the table, df by
    Welch–Satterthwaite) must lie inside ±`δ`.
  - Equivalence is claimed only if all five `n` pass (intersection–union, so no
    multiplicity correction).
  - `n` = 3–5 carry the information. At 6–7 both plants sit near the cap and the
    test is nearly automatic.
  - Disclosed: on the uncalibrated plant it passes at 3, 6 and 7 and fails at 4
    and 5 (the table).
  - **It is reported, not judged** (the re-check's L8). GKE has two runs at
    `n` = 3, so if it fails there the report says "not shown; at n = 3 the test
    can only pass if |Δ| < 0.0032", gives the 90% intervals, and states that a
    failure isn't evidence that the plants differ. At 6–7 it is near-automatic,
    so (i) is weak at both ends.
- **(ii) The engine against MVA at the same `c`** (3157 ms, think time 28.5 s),
  which needs no measurement, **the informative test**. Per `n`, engine − MVA with
  its `t(39)` interval,
  against the same `δ`. It separates the engine's own artefacts from the plant's
  fidelity: (ii) measures engine against theory, and MVA against GKE (within
  0.004 at every `n`) measures theory against the real plant.
- **(iii) Reported:** `S*_engine − 4.14`, with `SE_g = 0.15` (the review's
  0.13–0.15, not 0.115), and the engine's between-run sds beside GKE's.
- **Disclosed before the run:** the calibrated engine's `S*` is predicted at about
  4.47–4.49 (MVA's 4.33 plus the engine's offset of about 0.16).

**What moves at the freeze.** Both drain plants are re-frozen from their
forty-seed mean curves under the GKE protocol, not from seed 9001. For the long
drain that means:

- `S*'` and the band: `θ⌊S*'⌋` to `S*'(1 + 3r̄)`, with the restart-edge floor's
  `S*'` the same value;
- `D*'`, `X'_cap`, `n_above`, the count `⌈(D'_cap + |W'|)/Δt⌉ + 1` and the floor's
  `C_W`. `X'_cap` falls with the CPU per reconcile, from 0.086/s to about
  0.082/s, so the count rises from 166 to about 174.

From `drain0`:

- `S*₀` falls from 6.95 to about 6.65 (MVA 7.11 → 6.65 at 1500 → 1657 ms), so
  `N₀ = ⌈S*₀⌉` stays 7, to be confirmed at the freeze;
- `D*₀` rises from 46.4 s to about 49.0 s (Little, `S*₀/X*₀`), and with it
  `τ_ĉ = D*₀/r̄`, the long drain's precondition, from 928 s to about 980 s;
- `nStart` and `nFrom` (62 now) and `X_cap,0` (0.162/s, to about 0.147/s);
- the "false restarts, 30s" case, whose plant is `drain0` itself, and its band.

Each moved value is reported old against new. If `N₀` isn't 7, the freeze stops
for review.

**The determinacy rule, for every plant** (the reviewer's, held until this
measurement). At the freeze, each case plant's `S*` is computed by the freeze's
method on seeds 9001, 9002 and 9003. A case's band in `S` is judged only when the
three values' range (largest minus smallest) is under the band's half-width
above, `S*·3r̄`, with `S*` the frozen value; otherwise the band is reported, not
judged. The short plants' frozen values are unchanged (seed 9001's). On the
engine at `b2e5757`:

| Plant | S*, seeds 9001 / 9002 / 9003 | range | S*·3r̄ | determined |
|---|---|---|---|---|
| base | 4.717 / 4.729 / 4.740 | 0.023 | 0.71 | yes |
| cost46 | 2.840 / 2.828 / 2.877 | 0.049 | 0.43 | yes |
| limit125 | 2.824 / 2.831 / 2.849 | 0.025 | 0.42 | yes |
| limit500 | 8.762 / 8.749 / 8.742 | 0.020 | 1.31 | yes |
| rising | 4.610 / 4.615 / 4.631 | 0.021 | 0.69 | yes |
| drain0 | 6.945 / 7.158 / 7.059 | 0.213 | 1.04 | yes |
| drain | 3.905 / 4.822 / 4.492 | 0.917 | 0.59 | no |

The drain plants are frozen from forty-seed mean curves instead, so for them the
rule is the measurement's (decision 1): the seed bootstrap's 95% interval
narrower than `0.15·S*`. Every other case's band is judged as now. The rule is
re-applied whenever a plant is re-frozen, the calibrated drain plants included.
In the code, `TestFreeze` applies the rule to every plant. If a short plant is
undetermined, or `N₀` isn't 7, it stops for review and writes nothing. Each drain
plant's decision is frozen with its values, and the long drain reports its band
instead of judging it when the drain plant's is false.

**Three controllers.** The gate runs on the cost model, the level-PI baseline
re-tuned with `K_p ∈ {0, 0.01, 0.03}` added, and the reservation-gating baseline
modelled on Temporal's tuner at its defaults (design §17.8, item 6): inside the
engine at 50 ms decisions, with its own CPU reading interval. The baselines are
judged on the numeric conditions without the cost model's mechanism assertions.
Until the reservation-gating baseline runs as item 6 describes, its column says
nothing about Temporal's design.

**Both controllers.** The gate runs these conditions on the cost model and on
the utilization-feedback baseline (PR 1's simulation, design §10), case by
case, and reports the two side by side.

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
13. **Open, tracked: an entry bound for long CPU-first reconciles.** In the long
    drain (reconciles of tens of seconds with CPU-first bursts above the quota) a
    cost change is followed in lifetimes, through ĉ's restart, with an undershoot
    for about one lifetime (design §17.10). The gate judges its safety and reports
    its entry time without judging it (the user's decision, 2026-11-10). Deriving
    a defensible entry bound for this regime is a follow-up, **due before the GKE
    re-measure** (PR M).

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
