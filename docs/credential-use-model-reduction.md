---
type: Architecture
title: provider-runtime — credential-use model, a sound state-space reduction
description: Spec, under soundness review and not implemented, for a partial-order reduction in pkg/credentialuse/internal/apimodel's scheduler, so that the two-process scenario of the credential-use model can be searched exhaustively in CI.
resource: github.com/krateo-platformops/provider-runtime
tags: [spec, model-checking, credential-use, tests]
timestamp: 2026-11-09T00:00:00Z
---

# Credential-use model: a sound state-space reduction

**Status: spec, under soundness review, not implemented** (revision 2). It
is item 2 of the user's decision on #59: make TwoProcessesAcquireOneHolder
exhaustive in CI, with no cut in scope.
- The user has decided the base semantics (§3): event-driven time with up to
  two jumps to deadlines, and crashes that lose only units that have made a
  call.
- The implementation goes in this order:
  1. the new base, behind the gate the user required (§3.3);
  2. the reduction, with its differential test (§7);
  3. the two-process scenario, exhaustive in CI.

A throwaway measurement prototype ran in a scratch copy, outside the
repository (§9). It is how two of the obligations below were found. The
spec sits under PR 2 of
[the implementation plan](./plan-credential-use-implementation.md), and the
scheduler it changes is `pkg/credentialuse/internal/apimodel/sched.go`.

## 1. The scheduler today

- A schedule is a sequence of **steps**. A step picks one enabled actor and
  resumes it. If the actor was parked at a `Call`, the step first runs the
  call's op atomically, or injects a fault (error, lost response, crash).
  Then the actor runs its own Go code until its next park.
- An actor parks in one of three places:
  - at a `Call`, which is always enabled, and faultable if the actor is
    `Faulty`;
  - at a `Wait(cond, notBefore)`, which is enabled when cond holds and the
    clock has reached notBefore, and is never faultable;
  - at "start", before its first line runs, which is always enabled.
- The logical clock ticks 1ms on every step, and jumps to the earliest
  notBefore when nothing is enabled.
- A **preemption** is a step that picks an actor other than the last one
  while the last one is still enabled. A schedule has at most P preemptions
  and F faults. `Explore` is a DFS over every choice within (P, F).
- **A crash** is FaultCrash on a call of an actor in process Π. It marks
  every actor of Π crashed, and replaces Π's memory (`*process`) with a fresh
  one. A crashed actor's calls return ErrCrashed until it calls Restart (each
  holder pass and each batch starts with one) or Wait (which clears the mark
  before and after parking).

A step that resumes an actor from a Wait or from start is a **resume step**.
The code it runs, up to the next park, is its **segment**.

## 2. Where the schedules go

In TwoProcessesAcquireOneHolder (2,625,480 schedules with one fault), many
choice points are resume steps whose segment does nothing visible:
- an actor starting, then parking at its first Wait or call;
- a holder's or releaser's Wait being satisfied, then the actor building its
  protocol and parking at its first call.

Each is its own step, so the DFS orders it against every other step. Those
orders differ in nothing observable.

## 3. The new base semantics (decided by the user)

Both searches, reduced and unreduced, run on this base. It replaces §1's
clock and crash rules. Each change is needed to make §5's reduction exact.
The user decided the choices below.

### 3.1 Event-driven time

- **Only calls move time.** Each call step ticks the clock 1ms, and no other
  step does: not resumes, not informer deliveries, not relists. A delivery
  carries the time of the last call before it. Equal times can only stop a
  skip, since `canSkip` needs `t_obs < t_issue` strictly.
- **Up to T = 2 jumps per schedule.** Any pending deadline later than the
  clock can be chosen at any step, as an explicit step of the environment.
  The clock jumps to it, and no actor runs. The pending deadlines are:
  - every actor's notBefore;
  - every grace end, for each P being deleted and each process: the later of
    P's deletionTimestamp and that process's first sight, plus the grace;
  - the thresholds each scenario declares (its "into the grace" moments).

  A jump isn't a preemption, since no actor is switched away from. T counts
  jumps.
- **When nothing is enabled**, the clock still jumps to the earliest
  notBefore, as today. That jump is forced, and doesn't count against T.

**Why deadlines are enough.** Every time-dependent decision compares the
clock with a deadline:
- GraceOver is "now ≥ grace end";
- a requeue or a timer is "now ≥ notBefore";
- a scenario threshold.

The remaining time comparisons (t_obs, t_issue, delivery times,
deletionTimestamps) compare step times with each other, and only their order
matters, which calls already fix. So any assignment of real times to a
schedule's steps is equivalent, for every check, to one where time moves 1ms
per call and jumps only to deadlines. Each jump crosses one deadline the
schedule needs crossed early. T = 2 covers the scenarios' needs, one grace
and one timer. A scenario that needs more raises T, and the vacuity counters
show which.

### 3.2 Crashes lose only work that has made a call

- **(i) A restart resets process memory in place.** The same `*verified`,
  `*firstSight` and cache objects are cleared and relisted, not replaced.
  Code that read the process pointer before the crash sees the new process's
  memory, just as code that read it after the crash does.
- **(ii) A unit loses its work only if it has made a call.** Each actor
  records the process generation at the first call of its unit of work: a
  holder pass, a release batch, a deleter call. A call returns ErrCrashed if
  and only if the process has restarted since then. Restart and Wait end the
  unit. A unit with no call yet is unaffected: it becomes the new process's
  work, as in a real restart, where the reconcile runs again.
- **The obligation (the reviewer's): a unit whose generation is stale
  doesn't mutate process memory.**
  - In tests, `verified` and `firstSight` get an unexported mutation hook,
    which is nil in production. The harness's cache gets the same hook.
  - The harness sets it to panic if the running actor's unit generation is
    stale.
  - Today's protocol code mutates the verified set only after a successful
    call, so a stale unit, whose calls all fail, mutates nothing. The hook
    makes that a checked property.
- **The pass budget changes.** Under the old base, a crash between a pass's
  segment and its first call lost the pass, and the lost pass counted
  against the holder's live or deletion passes. Under (ii), the pass goes on
  in the new process. So in those schedules a holder has one more pass that
  does work than under the old base. The gate in §3.3 compares the two bases
  with this difference in place.
- **A stale R0 view is legitimate.** A releaser with caches whose unit began
  before a crash, and has made no call yet, may decide R0 on the old cache's
  view, read before the in-place reset. Caches may lag by any amount, and R0
  can only keep. So this is a stale cache, which the design already allows
  (§6.4, "What the proof assumes").

### 3.3 The gate before the reduction goes in (M-b, the user's)

Before any reduction, on the new base, unreduced:
- **The revert check must kill the same mutants.** That is the 44 killed on
  the old base, now 45 with mutant 2 replaced ("an unparseable record equal
  to the refs isn't rewritten"), with 1 equivalent.
- **The reach counters may not drop.** For each scenario and each
  exhaustive profile, the counts of schedules reaching U5, L6, H1's state
  and a deleting read must be no lower than the old base's. Both are
  reported side by side.

If any count is lower, the work stops and is reported. It is not adjusted
to pass.

## 4. The reduction

**Quiet segments.** A segment is **quiet** if it meets three conditions:
- (a) it reads only actor-local variables, immutable values, and the process
  pointer;
- (b) it writes only actor-local variables: its own crash state, through
  Restart, and the protocol value it builds;
- (c) it ends at a park (a Call, a Wait, or the actor's end) without making
  a call.

The harness **declares** quietness at the park that precedes the segment:
`WaitQuiet(...)` instead of `Wait(...)`, and a `QuietStart` flag on `Go`.

**Rule R.** Before computing each step's options, the scheduler resumes, at
once, every actor parked at a quiet park whose Wait is ready. It does so in
the order the actors were added, and repeats until none is left. This is a
**silent transition**: no choice point, no clock change, no preemption, no
change to `last`, and no trace entry. The segment runs to the actor's next
park. If that park is a Call, the Call is an ordinary step later, with all
its fault options.

**Unchanged:** every Call stays a step, with its faults, and so does every
time jump. So does every resume from a park that isn't quiet, including the
informer's watch and the relister's relist, whose segments are visible.

**(a) is checked, not trusted (M-a).** In tests, the scheduler marks the
running actor as inside a quiet segment, and these panic there:
- the clock's `Now()` (passing the clock along, as `newProtocol` does, is
  allowed);
- every accessor of the model and of the harness's caches;
- the world's counters and records.

The guard also panics on (b)'s side: any mutation of the model, the process
memory or the world inside a quiet segment.

## 5. Why it is sound

**Claim, forward.** For every schedule σ of the unreduced search within
(P, F, T), there is a schedule σ' of the reduced search within (P, F, T),
with:
- the same calls in the same order, with the same faults and results;
- the same jumps and the same informer deliveries, at the same points
  between calls;
- the same terminal state and the same checked observations, and so the
  same violations.

**The forward mapping.** σ' is σ with each quiet resume step removed. Its
segment runs silently at the point R runs it: right after the step that made
its Wait ready, or at the start.

**Claim, converse.** For every schedule ρ of the reduced search within
(P, F, T), there is an unreduced schedule within (P, F, T) with the same
calls, jumps, deliveries, terminal state and observations. So the reduced
search adds no behaviour.

**The converse mapping.** Put each of X's quiet resumes into the unreduced
schedule as a step **right before X's next step in ρ** (its next call or
non-quiet resume). By C-stable, X's Wait is still ready there. By
C-invisible and C-independent, running the segment there instead of where R
ran it changes nothing. The switch into X now happens at the inserted
resume, and costs what the switch into X's next step cost in ρ. The resume
followed by X's next step is no switch at all. So the bounds hold.

**The conditions** (partial-order reduction terms: a singleton ample set made
of an invisible transition, independent of every other, with no cycle to
ignore):

- **C-invisible.** By (b), checked by M-a's guard, a quiet segment changes
  nothing that any of these read:
  - any check: the safety invariant, checkHeld, the grace check, the
    deletion-phase check, convergence, the vacuity counters;
  - any other actor's step;
  - any Wait condition.

  *Harness obligations:*
  - The counters scenario Waits read (`started`, `batchesStarted`) move into
    the op of the unit's first call (a "began" hook on modelServer). The
    measurement prototype (§9) left them in segments, and lost every
    schedule in H1's state.
  - A releaser with caches is never quiet: its segment can run a whole R0
    batch against the cache without a call. The prototype marked one quiet,
    and lost every release in Teardown.
- **C-independent.** By (a), checked by M-a's guard, no other step changes
  what a quiet segment does, with two exceptions, both closed by §3:
  - **the clock:** resumes never move time on the new base, so removing or
    moving one shifts no time;
  - **crashes:** with §3.2's (i) and (ii), resume→crash→call and
    crash→resume→call reach the same state.
- **C-enabledness.** A quiet segment enables or disables nothing, by
  C-invisible. A ready quiet park can't be disabled before R runs it, since R
  runs it before the next step.
- **C-stable.** A quiet Wait's condition must stay true once it is true.
  *Obligation:* every quiet condition is either:
  - a monotone counter threshold (`started`, `batchesStarted`,
    `livePasses`, `batches` ≥ k);
  - or a model fact no later step undoes in that scenario: "the Secret is
    deleting or gone", "the holder is deleting or gone", "the Secret's UID
    changed".

  The rotation actor's "gone" wait is undone only by that actor's own later
  recreate, so it holds while the actor waits on it. Any other Wait stays
  non-quiet.
- **C-progress.** Every quiet segment reaches a park, and no cycle of quiet
  parks runs without a call. So R terminates.

**C-bounds for the forward mapping.**
- *Faults:* σ' has every call of σ, with the same fault.
- *Jumps:* σ' has the same jumps, at the same points between calls.
- *Preemptions:* removing a resume step s_X of actor X between steps of Y
  and Z can only remove switches. The switch into s_X and the switch out of
  it each cost at most one preemption in σ. In σ', the step Y→Z costs at
  most one. The switch into X's later call has the same predecessor in both
  schedules, and X is enabled in both: in σ parked at its call after its
  resume, in σ' parked at the same call after its silent segment. So σ' has
  no more preemptions than σ.

**Faults still apply at every original point.** Faults apply only at calls,
and every call is still a step. A crash that, unreduced, fell between a
segment and its call is reached in σ' with the crash before the silent
segment. §3.2 makes the two equivalent.

## 6. What it does not do

- It doesn't reduce calls or jumps: every call, every fault, every jump,
  and every order of calls between actors is still enumerated.
- It doesn't reduce informer deliveries, which are visible. Their timing
  against calls is searched in full, and they are likely the largest factor
  left. Reducing them needs a dependence relation (DPOR), which is out of
  scope.

## 7. Tests

- **The guards.** These run in every model test, both searches:
  - M-a's quiet-segment guard;
  - §3.2's stale-generation guard.
- **The gate** (§3.3), on the new base, before the reduction.
- **The differential test.** Compare the reduced and unreduced searches on
  the new base, for scenarios small enough to search both ways: Teardown ×3,
  AcquireAgainstRelease, AwayAndBack with 3 and 4 live passes,
  TwoReleasers, DeletionsAroundTheGrace, Recreate, and a shrunk two-process
  scenario. Two comparisons:
  - with bounds high enough that neither search is cut by them: the sets of
    terminal states must be equal, and so must the sets of violation
    messages. A terminal state is hashed canonically from the model's
    objects, the process memories, and the world's recorded observations
    (reads, releases, holds, deletion phases);
  - at CI's bounds, (1,0), (0,1) and (1,1) with T = 2: the two sets must be
    equal too, by §5's two mappings, and any difference is reported.
- **Mutants.** The same comparison on revert-check mutants the model kills:
  - H1's skip without the exact version;
  - M4's late clock;
  - a deletion pass that may skip;
  - R3 without the array test.

  Both searches must find violations, with the same set of messages.
- **Then:** the two-process scenario exhaustive at (0,1), (1,0) and (1,1)
  in CI, and the revert check with it included.

## 8. What changes for the existing results

The new base changes every published schedule count. The gate (§3.3) is
what keeps the change honest. The mutants killed and the reach counters are
compared with the old base, side by side, and any drop stops the work.

## 9. Measurement (the throwaway prototype)

The prototype, in a scratch copy outside the repository, has R and quiet
declarations, but none of §3's base changes, and it still breaks
C-invisible as described in §5. Its counts, with one fault:

| Scenario | Today | Prototype |
|---|---|---|
| Teardown | 1,314 | 567 |
| AwayAndBack, 3 live passes | 19,555 | 7,270 |
| TwoReleasers | 20,060 | 7,438 |
| DeletionsAroundTheGrace | 36,135 | 4,113 |
| Recreate | 43,788 | 7,260 |
| TwoProcessesAcquireOneHolder | 2,625,480 | 46,284 |

So R cuts the searches by about 3 to 57 times. But these counts **aren't
evidence of soundness**: the prototype lost behaviours (U5 in interleaving A,
releases in Teardown, H1's state in the two-process scenario), each traced
to an obligation above. The real numbers come after the base changes, with
the differential test passing.
