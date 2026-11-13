---
type: Architecture
title: provider-runtime — local capacity from measured CPU use
description: Design for sizing the saturation input's local capacity from the container's measured CPU use at CPU limits of 500m and above, with the CPU pressure step as a veto and a guaranteed share of the capacity per controller.
resource: github.com/krateo-platformops/provider-runtime
tags: [design, rate-limiting, saturation, cgroup]
timestamp: 2026-11-13T00:00:00Z
---

# Local capacity from measured CPU use

**Status:** design, decided by the user on 2026-11-13; not built. It adds to
the saturation input of [design-rate-limit-governor](./design-rate-limit-governor.md)
(§7 and §11 item 19). It replaces
[design-saturation-cost-model](./design-saturation-cost-model.md), which was
stopped after its review.

## 1. What it is for

The CPU pressure step (v1.19.0) finds the CPU limit by reaching it: it cuts
when the container has stalled for half of a 10s window and then climbs again.
Measured at a 250m limit it uses 93.4% of the limit with 91.0% of the ungoverned
throughput, and `local` moves between 4 and 12 around a knee of about 5, so a
call's 20ms of CPU takes 144ms of wall time.

This design computes where the knee is from what the container measures, holds
`local` there, and leaves the step as the brake for what the computation
doesn't see. It also gives each controller a share of `local` that the others
can't take.

Decided by the user on 2026-11-13:

- the law is one ratio of live measurements (§3), not separate estimates of
  cost and duration;
- it is supported only at CPU limits of 500m and above (§5);
- the CPU pressure step stays, and may only lower what the law allows (§4);
- each controller has a guaranteed share of `local`, and lends what it doesn't
  use (§6).

## 2. What is measured

One sample per `SamplePeriod` (1s), by the sampler goroutine that already reads
`cpu.pressure`:

| Value | Source | Meaning |
|---|---|---|
| `usage` | `cpu.stat`, `usage_usec` | CPU the container used, cumulative microseconds |
| `limit` | `cpu.max`, quota ÷ period | The CPU limit in cores; `max` means none. Read at every sample: an in-place resize changes it under a running process |
| `slotSeconds` | the gated queues | Time local slots were held since the last sample, summed over the slots |
| `completions` | the gated queues | Reconciles that gave their slot back since the last sample |

The queues already know when a slot is granted and when `Done` gives it back,
so the two counts need no change to the reconciler. Each queue keeps its own
pair, for the metrics of §7.

Nothing is read from `GOMAXPROCS`: since Go 1.25 it is at least 2 whatever the
limit, so it says nothing about the quota.

## 3. The law

Every `ModelInterval` (10s), over the samples of the last interval:

- `U` = Δ`usage` ÷ Δt, the CPU used, in cores;
- `N` = Σ`slotSeconds` ÷ Δt, the mean number of reconciles in flight;
- `B` = the background: the lowest `U` among the intervals of the last 10
  minutes in which `N` was 0, and 0 while there is none. An underestimate of
  `B` charges background CPU to reconciles, which lowers the result: the safe
  side;
- `target` = `CPUTarget` × `limit`, `CPUTarget` 0.9 by default.

`wanted = N × (target − B) ÷ (U − B)`

This is the utilization law: below the limit, CPU use grows in proportion to
the reconciles in flight, so the concurrency that uses `target` is the current
one scaled by how far `U` is from `target`. It is `(target − B) × duration ÷
CPU per reconcile` with both measured at the current operating point, where
duration is `N ÷ X` and CPU per reconcile is `(U − B) ÷ X` for a throughput
`X`; the throughput cancels. It is also the rule the Horizontal Pod Autoscaler
applies to replicas.

The model's capacity `M` moves to `wanted` under these rules, in this order:

1. **No evidence, no move.** `M` is unchanged if the interval had fewer than
   `minCompletions` (20) completions, if `U − B` is under 5% of `limit`, or if
   `limit` changed during the interval.
2. **A rise needs the slots to have been used.** `M` rises only if `N` is at
   least 0.9 × the capacity in force over the interval: with slots standing
   idle, more of them would have changed nothing, and the interval says nothing
   about what more would cost.
3. **Tolerance.** `M` is unchanged if `wanted` is within 10% of it.
4. **Direction.** `M` rises only if `U` is under `target`, and falls only if
   `U` is over it.
5. **Rise cap.** A rise is at most to `2 × M`, and at most to the ceiling. A
   fall is not capped.
6. **Bounds.** `M` is at least 1 and at most the ceiling.

`M` starts at the ceiling, so until the law has evidence the step alone
governs, exactly as in v1.19.0.

`M` is a real number. `local` takes its floor, at least 1 (§4).

**At the limit.** When `U` is pinned at `limit`, the law gives `N × (target −
B) ÷ (limit − B)`, a fall of about 10% per interval: correct in direction and
slow. The step is what answers saturation (§4); the law then finds the level
from below.

**What it does not see.** A reconcile that waits on a slow external API uses
little CPU, so the law allows more of them. That is correct for the container's
CPU and wrong for the API: the rate-limit input (per-key capacity) is what
answers that, as before.

## 4. With the CPU pressure step

The step keeps its own capacity `P`, by v1.19.0's rules unchanged, with one
substitution: where it halves, it halves the published `local`, not `P`, so a
cut is a cut of what is running.

`local = min(P, floor(M))`, at least 1.

- The step may only lower: `P` above `M` changes nothing.
- After a CPU cut `P` climbs by one per `StartupRampInterval` towards the
  ceiling and stops mattering once it passes `M`.
- A cut also sets `M` to the new `local` if `M` was above it, so the law
  resumes from what the cut left and must find evidence to rise.

## 5. Where it applies

The law runs when all of these hold; otherwise `M` is the ceiling and the step
alone governs:

- `Saturation.CapacityModel` is set (off by default);
- `cpu.max` gives a limit, and it is at least `MinCPULimit` (500m by default);
- `cpu.stat` is readable.

Below 500m one reconcile's CPU is a large part of a scheduler period's quota,
so CPU use is not proportional to concurrency and the law's premise fails. The
step measured there: 88.5% of the limit at 100m, 93.4% at 250m
([configuration](./configuration.md#cpu-step-re-measured-2026-11-13)).

A limit that changes while the process runs (an in-place resize) is picked up
at the next sample. The interval it changes in moves nothing (§3 rule 1).

## 6. A share per controller

Today freed slots go round-robin to the queues with work, so one controller
with a deep backlog and long reconciles can hold every slot.

Each gated queue gets a guaranteed share `G = max(1, floor(local ÷ queues))`.

- A queue with fewer than `G` slots in flight and an item to run is *owed*.
- A freed slot goes first to an owed queue, round-robin among them; with none
  owed, to any queue with work, round-robin, as today. That second case is the
  lending: a queue may hold more than `G` while the others don't need theirs.
- Nothing is preempted. A lent slot comes back when its reconcile ends, and the
  owed queue is then first in line. The wait is at most the lender's longest
  reconcile.
- When `local` is under the number of queues, each queue's `G` is 1 and they
  can't all be met: owed queues are served round-robin, so each still runs.

Shares are equal. Weights are left out until a provider needs them.

## 7. What is exported

So that something outside the controller can size it. A controller that limits
itself hides its demand from anything that reads CPU use alone: the CPU says
"at 90% of the limit" whether ten items are waiting or ten thousand.

New instruments, beside `ratelimit.local.capacity` and `saturation.input`:

| Instrument | Kind | Attributes | Meaning |
|---|---|---|---|
| `provider_runtime.ratelimit.local.model_capacity` | gauge | none | `M`; no data point while the law doesn't apply |
| `provider_runtime.ratelimit.local.in_flight` | gauge | `controller` | Slots held now |
| `provider_runtime.ratelimit.local.guaranteed` | gauge | `controller` | `G` |
| `provider_runtime.ratelimit.local.waiting` | gauge | `controller` | Items ready to run and waiting for a slot: the demand the capacity holds back |
| `provider_runtime.ratelimit.local.cuts` | counter | `input` | Cuts by the step |
| `provider_runtime.ratelimit.cpu.limit` | gauge (cores) | none | The limit read from `cpu.max`; no data point with none |
| `provider_runtime.ratelimit.cpu.used` | gauge (cores) | none | `U` over the last interval |
| `provider_runtime.ratelimit.cpu.background` | gauge (cores) | none | `B` |
| `provider_runtime.ratelimit.cpu.per_reconcile` | gauge (seconds) | none | `(U − B) ÷ X` over the last interval with completions |
| `provider_runtime.ratelimit.cpu.required` | gauge (cores) | none | `B + (U − B) × (N + waiting) ÷ N ÷ CPUTarget`: the limit at which the work now running and waiting would all run at the target. An estimate from the same proportionality as the law; not measured |

`cpu.required` above `cpu.limit` is the statement no autoscaler can make from
CPU use: this controller would use more if it had it.

## 8. Options

In `SaturationOptions`:

| Option | Default | Meaning |
|---|---|---|
| `CapacityModel` | `false` | Turns the law on. Needs `Enabled` |
| `CPUTarget` | `0.9` | The share of the limit the law aims at, in (0, 1] |
| `MinCPULimit` | `0.5` | Cores. Under it the law doesn't run |
| `ModelInterval` | `10s` | How often `M` is recomputed |

The share per controller (§6) and the instruments that don't depend on the law
(§7: `in_flight`, `guaranteed`, `waiting`, `cuts`, `cpu.limit`) apply whenever
saturation is on.

## 9. Tests

- **The law**, as a pure function of an interval's values: each rule of §3
  removed in turn fails a test. Cases: the worked example (12 in flight, 380m
  of 500m, `B` 20m gives 14.3); at the target; over it; pinned at the limit;
  slots idle; too few completions; `U` barely above `B`; the rise cap; a limit
  change.
- **With the step:** `local` is the lower of the two; a cut halves `local` and
  pulls `M` down; `P` climbing past `M` changes nothing.
- **Where it applies:** no limit, a limit under 500m, `cpu.stat` missing: `M`
  is the ceiling and `local` behaves exactly as v1.19.0 (the existing tests
  pass unchanged with the option off).
- **Shares:** a deep-backlog queue and a light one: the light one gets a slot
  within one completion; with one queue idle the other takes every slot; with
  `local` under the number of queues every queue still runs; drain and shutdown
  give every slot back (the existing queue tests, with shares).
- **Stress and race:** the existing gated-queue stress tests, with several
  queues and a moving `local`, under the race detector.

## 10. Acceptance, on a cluster

On the harness of `hack/rlgov-measure` (20ms of CPU and a 400ms wait per
Observe, 16 workers), at 500m and at 1 CPU, three arms: governor off, the step
alone (v1.19.0), the step with the law. Bars, written before the first run:

- with the law, `local` settles within 20% of the knee computed from the
  ungoverned arm's cost (`target × limit ÷ CPU per reconcile × (CPU + wait)`);
- CPU used and throughput are not worse than the step alone;
- a call's wall time is lower than under the step alone;
- no restarts.

A second workload with two controllers, one with a backlog of long reconciles
and one light, checks §6: the light controller's queue wait stays under one
long reconcile.

If the law is no better than the step alone, it does not ship.
