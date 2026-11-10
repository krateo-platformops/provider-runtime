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
- The new base multiplies every search by 70 to 110, so the user decided on
  a stateful search with a time-abstract state hash (§10). §10 is under
  review, before any code.
- The implementation goes in this order:
  1. the new base and the stateful search;
  2. the gate (§3.3, adjusted as §10.4 proposes);
  3. the quiet-segment reduction, if it's still needed (§4);
  4. the two-process scenario, exhaustive in CI.

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

**Ticks must be negligible next to every deadline** (the reviewer's
Medium). The argument below fails if call ticks can cross a deadline. With
today's 5ms test grace and 1ms per call, any five calls end the grace with
no jump, so "many calls, still inside the grace" can't be represented, and
time would move through uncounted ticks as well as through the jumps the
user decided. So:
- **The test grace is 1h**, against 1ms per call. A schedule makes at most a
  few hundred calls, so ticks never come near a deadline.
- **Every "into the grace" moment a scenario needs is a declared
  deadline**, reached by a jump, not by counting calls.
- **A call's tick never reaches a pending deadline.** Deadlines built from
  event times lie only a few ticks apart: a deletion and its first sight, or
  two processes' first sights. So after a jump to one, a 1ms tick would
  cross the next within a few calls. The tick is therefore at most 1/1024
  of the time left to the next pending deadline, which leaves room for
  hundreds of calls. **A run-time check panics if a tick can't fit**, that
  is, if a call would have to cross a deadline. Only jumps (and the forced
  jump when nothing is enabled) cross deadlines.

Decoupled time reaches everything today's base reaches: each place where a
tick crossed a deadline becomes a jump at the same point. The reach
counters then show whether T = 2 is enough. The case to watch is Teardown
interleaving A's 8 U5 schedules at one preemption. If T = 2 loses any of
them, the work stops and is reported; T is not raised to make it pass.

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
  doesn't touch process memory, neither mutating nor reading it.** Reads
  matter too: a stale unit's acquire could take the skip path, which makes
  no call, read the new process's memory, and report Held.
  - In tests, `verified` and `firstSight` get an unexported access hook,
    called on every read and every mutation, and nil in production. The
    harness's cache gets the same hook.
  - The harness sets it to panic if the running actor's unit generation is
    stale.
  - Today's protocol code reads and mutates process memory only in the same
    step as a successful call, or before the unit's first call. A stale
    unit's calls all fail, and acquire and release return at the first
    failure. So the hook should never fire, and it makes that a checked
    property.
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

In this order:
1. **The old base, first.** Rerun the revert check on the old base to
   establish its result: 45 killed and 1 equivalent, with mutant 2
   replaced. The run also produces a **per-mutant matrix**: for each mutant,
   every test and model scenario it is killed in, found without `-failfast`
   so that every scenario runs. Its output replaces the stale one from
   6f22bd6.
2. **Then the new base, unreduced, with the same two runs.** The results go
   side by side:
   - **the revert check must kill the same mutants**, and the matrix shows,
     per mutant, the scenarios that kill it on each base;
   - **the reach counters may not drop:** for each scenario and each
     exhaustive profile, the number of schedules reaching U5, L6, H1's state
     and a deleting read must be no lower than the old base's.

If any count is lower, the work stops and is reported. It is not adjusted
to pass.

**What the gate does not show.**
- Schedule counts aren't a behaviour-inclusion check: the two bases'
  schedules aren't comparable one to one.
- The reach counters cover only U5, L6, H1's state and deleting reads.
  Other states are compared only through the mutants each base kills.

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

## 10. Stateful search with a time-abstract state hash (the user's decision, under review)

**Why.** The new base (§3) is what the user decided, but T = 2 jumps multiply
every search by 70 to 110. AwayAndBack with 3 live passes goes from 19,555
to 1,538,761 schedules with one fault, and from 29,833 to 3,307,693 with one
preemption. Its search with one preemption and one fault would be about 100
million. Each step offers a jump to each pending deadline, and two jumps can
sit at any two positions. Most of those positions are equivalent: nothing
between them reads the clock except to compare it with a deadline. The user
chose the timed-model-checking answer: region equivalence (Alur and Dill;
UPPAAL's zones) and a stateful search that remembers visited states (SPIN,
TLC). Two paths that reach the same abstract state are searched once.

### 10.1 What a state is

An abstract state has seven parts. Each is hashed canonically.

1. **Every actor's control point and local memory.** A Go coroutine's stack
   can't be inspected. So an actor's state is made of:
   - **its declared persistent locals:** the variables it keeps across units
     of work, which it registers explicitly. For example, a holder's pass
     number, a releaser's batch number, a deleter's next key;
   - **a digest of its inputs in the current unit:** every call's label,
     fault and result, and every value its own code reads from shared state
     (views, the verified set, first sights, the clock, the world's
     counters). The digest is reset when a unit ends (Restart, Wait).

   Times in the digest are recorded by their place in the order of §10.2,
   not by value.

   An actor is deterministic given its code and inputs. So two actors with
   equal declared locals and equal unit inputs have equal stacks, and merging
   them is sound. The representation can miss merges (it is finer than the
   stack), which costs search time, never soundness.

   *Obligation:* everything an actor's code reads from shared state goes
   through a hooked accessor that feeds the digest. These are the access
   hooks of §3.2, extended with the value read: the harness views, the
   verified set, first sights and the clock. A read that bypasses them would
   be invisible to the hash. M-a's quiet-segment guard (§4) already forbids
   such reads in quiet segments, and a check in §10.2 catches the rest.
2. **The API model:** every object's metadata, and the event log after the
   oldest cache's position, since the older part can no longer be delivered.
   UIDs, resourceVersions and epochs are compared only for equality, so they
   are **renamed canonically** in first-occurrence order. The model's
   counters only need to allocate fresh names, which renaming keeps.
3. **The caches:** their objects, and where each stands in the log.
4. **Process memory:** the verified sets and first sights, with their times
   abstracted (§10.2), and each process's generation, relative to each
   actor's unit. What the hash needs is whether a unit is stale or hasn't
   begun, not the counter.
5. **The scheduler:**
   - each actor's park (its label and whether it is done; a Wait's condition
     is a function of the rest of the state, and its notBefore is a time
     term);
   - the last actor, because whether a switch costs a preemption depends on
     it.
6. **The checks, as monitors with small state.**
   - Today the safety invariant is checked at the end, over the whole
     history of reads and releases. A history would make almost every state
     distinct.
   - So each check becomes an **on-the-fly monitor**, evaluated at the step
     where its event happens. A violation is recorded then, with the path
     that reached it.
   - What a monitor keeps is part of the state:
     - per holder and P, the earliest unbroken hold, as a time term;
     - whether an allowed U5 release has happened, with the time terms its
       grace and namespace checks compare;
     - the namespace deletion time;
     - the deletion-phase flags;
     - the scenario's counters.
   - Convergence is checked at terminal states, as now.
7. **Time**, abstracted as §10.2 says.

### 10.2 Time: region equivalence for a discrete clock

**Terms.** The time terms of a state are:
- every time value stored anywhere in it: now, every actor's notBefore, the
  model's creation and deletion timestamps, the verified set's `written`,
  `recordSince`, `observed` and `lastList`, the first sights, and the
  monitors' times;
- every pending deadline (§3.1): grace ends and declared thresholds.

**The abstraction.** A state keeps **the order of its terms, ties
included**, and drops their values. That covers which deadlines have passed
(they are ordered before now) and the order of the pending ones. It is
region equivalence for this clock. The constants the code adds to times
(the grace, `restoredEpochRequeue`, a scenario's threshold offsets) appear
only inside deadlines, which are terms themselves.

**Why equal abstract states have equal futures.** This holds if no code uses
a time except to compare it with another term, or to store it, or to build a
deadline from it with a constant. Then:
- every comparison a future step makes gives the same answer in both
  states;
- a new now lands in the same place in the order: a tick stays strictly
  between now and the next pending deadline (§3.1), and a jump lands on a
  deadline;
- every new stored time is now, so it lands in the same place too.

**Every reader of time, and what it does with the value:**

| Reader | Use | How the abstraction handles it |
|---|---|---|
| `acquire.go` A1, `at := clock.Now()` | stored as `observed` | a stored term; compared only (t_obs order) |
| `acquire.go` A2, `issued := clock.Now()` (×2) | stored as `written` | a stored term; compared only |
| `release.go` R1, `now` | stored as a first sight; `GraceEnd` = max(deletionTimestamp, first sight) + grace; compared with now (`now.Before(graceEnd)`); `requeueAt(now, graceEnd)` | the first sight is a term, and the grace end a deadline. `RequeueAfter = graceEnd − now` is a duration, used only to schedule the requeue's wait at `now + RequeueAfter = graceEnd` (a deadline), and tested only for > 0 (an order fact) |
| `release.go` R1, restored epoch: `requeueAt(now, now + restoredEpochRequeue)` | a requeue 1s later | the wait's notBefore is a deadline (term plus constant) |
| `release.go` R2, `now` before the reads | GraceOver is now ≥ grace end | a comparison with a deadline |
| `release.go` R2, `nsReadAt = clock.Now()` | stored in the decision, read by the monitor | a stored term |
| `decide.go` `GraceEnd` | `deletionTimestamp.Sub(firstSeen)` clamped at 0, plus grace | equals max(dt, fs) + grace: a deadline |
| the model: Create and Delete | creation and deletion timestamps | stored terms; never changed by patches (`Patch` keeps them from the stored object), so no truncation |
| the harness: deliveries (`holderEvent`, `listCompleted`) | stored as `recordSince` and `lastList` | stored terms |
| the harness: holds, `nsDeleted`, a release's `at` | the monitors' terms | stored terms |
| the harness: waits | `dt + grace/2` (the late deletions' threshold); `at + RequeueAfter` | deadlines |
| the harness: `converge`'s clock, `now + 3 × grace` | after the search, outside it | not part of the search |
| the scheduler: `tick` | `min(1ms, gap/1024)` uses the gap to the next deadline as a value | the result only places the new now strictly between now and the next deadline, the same place in any state with the same order |

**What enforces "time is only compared, never used as a value".**
- *M-a's guard (§4):* a quiet segment can't read the clock at all.
- *A new check, order-preserving perturbation:* in tests, each schedule
  (each sequence of choices) is replayed with the clock perturbed in a way
  that preserves order:
  - a different start;
  - ticks scaled by a random factor that still fits under the gap rule.

  The sequence of abstract states must be identical. Any code that used a
  time as a value would see different values, and its effect would show in
  a call, a decision or a monitor. Every schedule in the differential test
  (§10.5) gets one perturbed replay.
- *Review:* the table above is the list of readers. A new reader of
  `Clock.Now()` must be added to it, and the perturbation check catches one
  that isn't.

### 10.3 The bounds stay exact under caching

A visited state is stored with the **budgets it was reached with**: the
preemptions, faults and jumps it had left. A state reached again is pruned
only if a stored budget dominates the new one, meaning it is at least as
large in all three. Every future the new path could take within its budget
has then already been searched from that state. Otherwise the state is
searched again, and the new budget joins the stored set, which keeps only
budgets no other dominates.

The last actor is part of the state (§10.1), so the preemption accounting is
the same on both paths. The search stays a replay DFS: a pruned path ends at
the visited state, and the search backtracks as at a terminal state.

### 10.4 What is reported

- **Counts are of distinct abstract states, not schedules.** The model
  reports:
  - states visited;
  - distinct terminal states;
  - for each reach property (U5, L6, H1's state, a deleting read), the
    number of distinct states in which it holds.

  Paths merge, so the number of schedules is no longer known, and none is
  reported.
- **A violation** is reported once per distinct violating state, with the
  first path found to it as its witness trace.
- **The vacuity checks** stay, on distinct-state counts: zero still means
  never reached.
- **The gate (§3.3) compares the two bases**, and counts from a stateful and
  a stateless search are not comparable. Nor are counts from two different
  abstractions: the old base's ticks carry time (its 5ms grace is crossed
  by counting calls), so it can only be hashed with concrete times. So the
  "no lower" comparison of reach counters becomes:
  - for each scenario, profile and property, reached on the old base implies
    reached on the new;
  - the per-mutant matrix is equal or larger.

  **This changes the gate the user approved. It is flagged for the user's
  decision, not assumed.**

### 10.5 The differential test

Two comparisons, run on scenarios small enough to search both ways (Teardown
×3, AcquireAgainstRelease, and the smaller AwayAndBack, at (0,1) and (1,0)
with T = 2):
- the stateful and stateless searches give the same set of terminal abstract
  states, and the same set of violation messages;
- each stateless schedule's sequence of abstract states equals that of its
  perturbed replay (§10.2).

The same comparison runs on revert-check mutants the model kills: H1's skip
without the exact version, M4's late clock, a deletion pass that may skip,
and R3 without the array test. Both searches must find violations, with the
same messages.

### 10.6 Risks, stated before the code

- **How much merges is an empirical question.** Swapping two steps that both
  store times changes the order of their terms, and so the state. Often
  that is correct: an A2's issue time against a delivery time is exactly
  what t_obs < t_issue compares. The gain is expected mostly from jump
  positions and from steps that store no time. A measurement after the
  review will show it. If the gain isn't enough for the two-process
  scenario in CI, that will be reported, not worked around.
- **The per-actor digest** is only as good as its hooks. A missed read is a
  soundness bug. The perturbation check and the differential test are the
  guards against it.
