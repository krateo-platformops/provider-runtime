---
type: Architecture
title: provider-runtime — local capacity from measured CPU use
description: Design for sizing the saturation input's local capacity from the container's measured CPU use at CPU limits of 500m and above, with the CPU pressure step as a veto and a guaranteed share of the capacity per controller.
resource: github.com/krateo-platformops/provider-runtime
tags: [design, rate-limiting, saturation, cgroup]
timestamp: 2026-11-13T00:00:00Z
---

# Local capacity from measured CPU use

**Status:** built, not measured on a cluster. Decided by the user on
2026-11-13. It adds to
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
| `arrivals` | the gated queues | Items that became ready to run since the last sample, for §7 |

The queues already know when a slot is granted and when `Done` gives it back,
so the two counts need no change to the reconciler. Each queue keeps its own
arrivals and completions, for the metrics of §7. The Governor, which grants
every slot and takes it back, adds up `slotSeconds` once for the process: the
slots in flight summed over time. That stays exact when a queue shuts down with
reconciles still running.

Nothing is read from `GOMAXPROCS`: since Go 1.25 it is at least 2 whatever the
limit, so it says nothing about the quota.

## 3. The law

The law is evaluated over a window of whole `ModelInterval`s (10s each). At
the end of each interval the window ends, and the law is evaluated over all of
it, if it holds at least `minCompletions` (20) completions. Otherwise it grows
by another interval, up to `maxWindow` (30 intervals, 5 minutes), where it ends
and is evaluated with what it has. The completions only choose the window's
length: a controller whose reconciles are slow is measured over a longer
window, not never. The next window starts where the last one ended.

An interval in which nothing was in flight (`N` is 0) never enters a window. It
feeds `B` and nothing else: it neither starts a window nor is added to one in
progress, and it doesn't count towards the 30. So a window holds only intervals
in which something ran, its length is the sum of theirs, and a burst after idle
time is judged on its own intervals instead of being averaged with the idle
ones before it.

Over the window:

- `U` = Δ`usage` ÷ Δt, the CPU used, in cores;
- `N` = Σ`slotSeconds` ÷ Δt, the mean number of reconciles in flight;
- `B` = the background: the lowest `U` among the intervals of the last 10
  minutes in which `N` was 0, and 0 while there is none. An underestimate of
  `B` charges background CPU to reconciles, which lowers the result: the safe
  side;
- `target` = `CPUTarget` × `limit`, `CPUTarget` 0.9 by default.

`r = (target − B) ÷ (U − B)`, and `wanted = N × r`

**CPU barely used.** If `U − B` is under 5% of `limit`, `r` divides by almost
nothing and is not usable, but the direction is certain: CPU does not bind. Then
`wanted = 2 × M`, the rise cap, and rule 3 is skipped. It is still a rise, so
rules 2 and 4 to 6 apply. One case where "does not bind" is not certain: with a
background within 5% of `limit` under the target, doubling can take CPU use
over the target, by up to 5% of `limit`. The law then falls back at the next
window.

The capacity in force (rule 2) and the arrival rate (§7) are taken over the
same window.

This is the utilization law: below the limit, CPU use grows in proportion to
the reconciles in flight, so the concurrency that uses `target` is the current
one scaled by how far `U` is from `target`. It is `(target − B) × duration ÷
CPU per reconcile` with both measured at the current operating point, where
duration is `N ÷ X` and CPU per reconcile is `(U − B) ÷ X` for a throughput
`X`; the throughput cancels. It is also the rule the Horizontal Pod Autoscaler
applies to replicas.

The model's capacity `M` moves to `wanted` under these rules, in this order:

1. **The window.** `M` is unchanged if nothing was in flight in the window
   (`N` is 0), or if `limit` changed during it. A limit change ends the window
   at the end of the interval it happened in, so the next window is all at the
   new limit.
2. **A rise needs the slots to have been used.** `M` rises only if `N` is at
   least 0.9 × the capacity in force over the window, which is the published
   `local` averaged over its time: with slots standing idle, more of them would
   have changed nothing, and the window says nothing about what more would cost.
   At a small capacity that is a high bar: at `local` 1 the one slot must be in
   use for 9 seconds in 10.
3. **Tolerance.** `M` is unchanged if `r` is within 5% of 1, that is if
   `|r − 1| ≤ 0.05`: CPU use that close to the target moves nothing. Skipped
   with the CPU barely used, where there is no `r`. It is the
   Horizontal Pod Autoscaler's tolerance, on the same ratio. It is on the ratio
   and not on `M`, because a tolerance of 10% on `M` sits exactly on the case
   of a container pinned at its limit, where `wanted` is 0.9 × `M`.
4. **Direction.** `M` rises only if `U` is under `target` and `wanted` is above
   `M`, and falls only if `U` is over `target` and `wanted` is below `M`.
5. **Rise cap.** A rise is at most to `2 × M`, and at most to the ceiling. A
   fall is not capped.
6. **Bounds.** `M` is at least 1 and at most the ceiling.

`M` starts at the ceiling, so until the law has evidence the step alone
governs, exactly as in v1.19.0.

`M` is a real number. `local` takes its floor, at least 1 (§4). At a small `M`
the floor costs up to one slot (an `M` of 1.9 runs 1): that is the safe side,
because the slot it withholds would take CPU use over the target.

**At the limit.** When `U` is pinned at `limit`, `r` is `(target − B) ÷ (limit
− B)`: `CPUTarget` (0.9) with no background, and less with one. That is outside
the tolerance, so `M` falls to `N × r`, by 10% or a little more at every
evaluation: correct in direction and slow. The step is what answers saturation
(§4); the law then finds the level from below.

**After a cut.** A cut by the step sets `M` to the `local` it left (§4), and
from there `M` rises only by the law: at most one doubling per evaluated
window. With 20 completions per interval or more a window is 10s, so from 1 to
16 takes four windows, 40s, against the step's own 30s at +1 per
`StartupRampInterval`. With slow reconciles a window is as long as 20
completions take, at most 30 intervals: from 1 to 16 is then up to four windows
of 5 minutes, 20 minutes, where the step alone would take 30s. That is the
price of asking for evidence before each rise. Two things keep a cut from
pinning `local` for good: the window is evaluated at 30 intervals whatever it
completed, and CPU barely used is a rise, not "no move" (one reconcile at
`local` 1 on a 1 CPU limit can use under 5% of it).

**What does not rise.** `M` stays where it is while the slots it allows are not
used (rule 2), which is not a shortage: nothing waits for a slot then. It also
stays while the background alone holds CPU use at the target or over it, where
there is nothing to rise into.

**How old the measurements are.** `U`, `B`, the cost per reconcile and the
required CPU (§7) are those of the last window that ended. With slow reconciles
a window is up to 30 busy intervals, and with idle time between them longer
still, so the gauges are as old as that.

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
  resumes from what the cut left and must find evidence to rise. This holds for
  any cut by the step, the experimental memory input's included. A cut counts
  (§7, `local.cuts`) when it lowered `local`. How long the rise back can
  take, and why a cut cannot pin `local` for good, is in §3, "After a cut".

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
at the next sample. The window it changes in moves nothing (§3 rule 1). When
the law stops applying it forgets what it knew, the background included.

## 6. A share per controller

Today freed slots go round-robin to the queues with work, so one controller
with a deep backlog and long reconciles can hold every slot.

Each gated queue gets a guaranteed share `G = max(1, floor(local ÷ queues))`.

- A queue with fewer than `G` slots in flight and an item to run is *owed*.
- A freed slot goes first to an owed queue, round-robin among them; with none
  owed, to any queue with work, round-robin, as today. That second case is the
  lending: a queue may hold more than `G` while the others don't need theirs.
  Each of the two has its own place in the round: a grant to an owed queue does
  not move where lending goes on from.
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
| `provider_runtime.ratelimit.cpu.used` | gauge (cores) | none | `U` over the last window that ended |
| `provider_runtime.ratelimit.cpu.background` | gauge (cores) | none | `B`, as that window was judged with |
| `provider_runtime.ratelimit.cpu.per_reconcile` | gauge (seconds) | none | `(U − B) ÷ X` over the last window with completions, for a throughput `X` |
| `provider_runtime.ratelimit.local.arrivals` | counter | `controller` | Items that became ready to run (an item re-added while it waits counts once) |
| `provider_runtime.ratelimit.cpu.required` | gauge (cores) | none | `(B + per_reconcile × arrivals per second) ÷ CPUTarget`, over the last window: the limit at which the work arriving now would run at the target. An estimate from the same proportionality as the law; not measured |

`cpu.required` above `cpu.limit` is the statement no autoscaler can make from
CPU use: this controller would use more if it had it.

The two counters are observable counters, read from the Governor's snapshot
like the gauges. The instruments that depend on the law (`model_capacity`,
`cpu.used`, `cpu.background`, `cpu.per_reconcile`, `cpu.required`) have no data
point while the law doesn't apply (§5).

## 8. Options

In `SaturationOptions`:

| Option | Default | Meaning |
|---|---|---|
| `CapacityModel` | `false` | Turns the law on. Needs `Enabled` |
| `CPUTarget` | `0.9` | The share of the limit the law aims at, in (0, 1] |
| `MinCPULimit` | `0.5` | Cores. Under it the law doesn't run |
| `ModelInterval` | `10s` | The length of one interval of the window `M` is recomputed over (§3) |

The share per controller (§6) and the instruments that don't depend on the law
(§7: `in_flight`, `guaranteed`, `waiting`, `arrivals`, `cuts`, `cpu.limit`) apply whenever
saturation is on.

## 9. Tests

- **The law**, as a pure function of a window's values: each rule of §3
  removed in turn fails a test. Cases: the worked example (12 in flight, 380m
  of 500m, `B` 20m gives 14.3); at the target; within 5% of it on either side;
  over it; pinned at the limit, with no background and every slot used, falling
  10% per evaluation; slots idle; nothing in flight; CPU barely used, which
  doubles `M` (one slot in use on a 1 CPU limit at 4.8% of it), and doesn't
  with the slots idle; the rise cap; a limit change.
- **The window:** at 5 completions per interval `M` moves at the fourth
  interval, over all four; a window that never holds 20 is evaluated at 30
  intervals with what it has; a limit change inside a window discards it; after
  idle intervals a busy interval pinned at the limit falls at its own
  evaluation; idle intervals inside a window dilute neither `N` nor `U` and
  don't count towards its length.
- **With the step:** `local` is the lower of the two; a cut halves `local` and
  pulls `M` down; `P` climbing past `M` changes nothing; after a cut `local`
  rises again with slow reconciles, and from 1 on a 1 CPU limit.
- **Where it applies:** no limit, a limit under 500m, `cpu.stat` missing: `M`
  is the ceiling and `local` behaves exactly as v1.19.0 (the existing tests
  pass unchanged with the option off).
- **Shares:** lent slots alternate between two deep queues while a third takes
  its share between them; a deep-backlog queue and a light one: the light one
  gets a slot within one completion; with one queue idle the other takes every slot; with
  `local` under the number of queues every queue still runs; drain and shutdown
  give every slot back (the existing queue tests, with shares).
- **Stress and race:** the existing gated-queue stress tests, with several
  queues and a moving `local`, under the race detector.

## 10. Acceptance, on a cluster

On the harness of `hack/rlgov-measure` (20ms of CPU and a 400ms wait per
Observe, 16 workers), at 500m and at 1 CPU, three arms: governor off, the step
alone (v1.19.0), the step with the law. Bars, written before the first run:

- a call's wall time is at least 20% lower than under the step alone;
- throughput is at least 95% of the step alone's;
- CPU used is at least 85% of the limit;
- `local` settles within 20% of the knee computed from the ungoverned arm's
  cost (`target × limit ÷ CPU per reconcile × (CPU + wait)`);
- no restarts.

The user chose these on 2026-11-13, "a latency win at near-equal throughput":
the law aims at 90% of the limit, and the step alone runs hotter (93.4% at
250m) at the cost of latency, so "not worse on CPU" would fail a law that does
what it is for.

A second workload with two controllers, one with a backlog of long reconciles
and one light, checks §6: the light controller's queue wait stays under one
long reconcile.

If the law is no better than the step alone, it does not ship.
