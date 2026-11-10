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

- **Only calls move time.** Each call step ticks the clock, by at most 1µs
  (§10.2), and no other step does: not resumes, not informer deliveries, not relists. A delivery
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
- **The test grace is 1h**, against at most 1µs per call. A schedule makes
  at most a few thousand calls, so ticks never come near a deadline.
- **Every "into the grace" moment a scenario needs is a declared
  deadline**, reached by a jump, not by counting calls.
- **A call's tick never reaches a pending deadline.** Deadlines built from
  event times lie only a few ticks apart: a deletion and its first sight, or
  two processes' first sights. So after a jump to one, a fixed tick would
  cross the next within a few calls. The tick is therefore at most 1/1024
  of the time left to the next pending deadline, and at most 1µs, which
  leaves room for hundreds of calls. **A run-time check panics if a tick can't fit**, that
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

**Superseded by the user's decision (§10.4).** With the stateful search,
counts from the two bases aren't comparable. The gate is inclusion of
terminal states, S_old ⊆ S_new, together with the per-mutant matrix and
equal violation messages under each killed mutant.

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

*Revision 2: answers the soundness review of 87f333b (M1 to M3, and the Lows)
and records the user's decision on the gate.*

**Why.** The new base (§3) is what the user decided, but T = 2 jumps multiply
every search by 70 to 110. AwayAndBack with 3 live passes goes from 19,555
to 1,538,761 schedules with one fault, and from 29,833 to 3,307,693 with one
preemption. Its search with one preemption and one fault would be about 100
million. Each step offers a jump to each pending deadline, and two jumps can
sit at any two positions, most of them equivalent. The user chose the
timed-model-checking answer: region equivalence (Alur and Dill; UPPAAL's
zones) and a stateful search that remembers visited states (SPIN, TLC). Two
paths that reach the same abstract state are searched once.

### 10.1 What a state is

An abstract state has seven parts. Each is hashed canonically.

1. **Every actor's control point and local memory.** A Go coroutine's stack
   can't be inspected. So an actor's state is made of three things:
   - **Its declared persistent locals:** the variables it keeps across units
     of work, registered explicitly. Examples: a holder's pass number, a
     releaser's batch number, a deleter's next key. **Names captured in a
     closure count as locals.** Recreate's wait condition captures the
     first Secret's UID, so that UID is declared and renamed like any other
     name (point 2).
   - **A digest of its inputs in the current unit:** every call's label,
     fault and result, and every value its own code reads from shared state
     through a hooked accessor (views, the verified set, first sights, the
     clock, the world's counters). The digest resets when a unit ends
     (Restart, Wait).
   - **The time values it holds, as live time terms** (§10.2), not frozen
     ranks. R2's now is held across the Namespace GET and the list. An A2's
     issue time is held across the call. Their relation to the clock and to
     deadlines changes while they are held, so they take part in the time
     abstraction like any stored time. The digest refers to them by term.

   An actor is deterministic given its code and its inputs. So two actors
   with equal locals, equal digests and equal held terms have stacks that
   behave alike, and merging them is sound. The representation is finer
   than the stack, so it can miss merges, which costs search time and never
   soundness. *Obligation:* every read of shared state goes through a hooked
   accessor. The merge audit (§10.5) is the check on this.
2. **The API model:** every object's metadata, and the event log after the
   oldest cache's position, since the older part can no longer be delivered.
   **Names are renamed canonically:** UIDs, resourceVersions and epochs are
   compared only for equality, so each is replaced by its index of first
   occurrence in a canonical traversal of the state.
   - The traversal orders objects by key (kind, namespace, name, which the
     scenario fixes), and actors by registration. It never sorts by raw
     UIDs, resourceVersions or epochs.
   - Every place a name lives is renamed together: objects, caches, the
     verified set, monitors, and actors' locals and held values.
   - The model's counters only need to allocate fresh names, which renaming
     keeps.
   - Log positions (cache sequence numbers) are stored relative to the
     retained suffix.
3. **The caches:** their objects, and where each stands in the retained log.
4. **Process memory:** the verified sets and first sights, with their times
   as terms (§10.2), and, for each actor, whether its unit is stale or not
   begun. That is relative to the process's generation, not the counter.
5. **The scheduler:** each actor's park (its label and whether it's done, its
   notBefore as a term; a Wait's condition is a function of the state), and
   the last actor, because whether a switch costs a preemption depends on
   it.
6. **The checks, as monitors with small state.** Checking over the whole
   history of reads and releases at the end would make almost every state
   distinct. So each check runs **on the fly, at the event it is about**, and
   records a violation then, with the path that reached it.
   - **The hold monitor** updates on every model event of each holder, made
     by any actor, through the model's event hook. A version whose record
     drops P ends that holder's hold on P. It keeps the earliest unbroken
     hold per holder and P, as a term. It no longer replays the log from an
     old position (today's `namedSince`).
   - **The read monitor** keeps the latest allowed U5 release's
     `NamespaceReadAt`, `GraceEnd` and `at`, as terms, and the namespace
     deletion's time. With them it checks each deleting holder's read as it
     happens.
   - **The other parts:** the deletion-phase flags, the scenario's counters,
     and **flags for event-type reach**. A U5 release, an L6 pass, an H1
     pass and a deleting read are events, so each sets a monitor flag
     that becomes part of the state.
   - Convergence is checked at terminal states, as now.
7. **Time,** as §10.2 says.

### 10.2 Time: region equivalence for a discrete clock

**Order alone is not enough.** Code compares a term built as t + c (a grace
end, a requeue) with another term u, which amounts to comparing u − t with c.
Order keeps neither the integer parts nor the differences. Today's code has
four such cases:
- (a) R2's WaitForGrace requeue becomes a wait until `at + RequeueAfter` in
  the harness. That is the grace end plus the ticks between R2's now and
  `at`, not the grace end itself.
- (b) `requeueAt` falls back to 1ms when the grace end is already past.
- (c) a restored epoch's requeue at now + 1s, which can fall either side of a
  pending grace end;
- (d) the late deletions' threshold, dt + grace/2, created after dt.

**The representation: values relative to the earliest live term, with tick
parts reduced to their order.**
- **Every constant the code adds to a time is an integer multiple of a unit
  U:** grace = 1h, grace/2 = 30min, `restoredEpochRequeue` = 1s, so U = 1s.
- **A call's tick is at most 1µs** (`min(1µs, gap/1024)`). A schedule makes
  at most a few thousand calls, so its total ticks stay below U/2.
- Every time value is then a jump-built coarse part plus a fine part: an
  earlier term plus constants, from jumps to deadlines, plus a sum of ticks.
  Written relative to the earliest live term, a value is n·U + f with
  |f| < U/2.
- **A term is represented by the pair (n, the rank of f among all live
  terms' fine parts).** Then t + c against u is decided lexicographically:
  n_t + c/U against n_u, then f_t against f_u. Both are kept, so every
  comparison the code can make has the same answer in two states with equal
  representations.
- The shift to the earliest live term is sound because every comparison is
  a difference.
- **Why not zones (DBMs).** Zones represent symbolic clocks and constraints
  between them. Here the clock is concrete and discrete, the only
  constraints are u − t ≷ c with c a multiple of U, and fine parts are
  bounded below U/2. The integer part with fine-part order is the region
  construction, and it is exact for this case. Zones would be needed only
  if ticks could add up to U, which the assertion below rules out.

**The assertion, a run-time check.** It panics if either of these happens:
- a tick crosses a pending deadline, or brings now from outside U of a
  deadline to inside it, which would mean the coarse part moved by ticks;
- a schedule's total ticks reach U/2.

Only jumps move coarse parts, and only jumps cross deadlines.

**Two code changes make every requeue a term** (they land with the
implementation):
- **Requeues become absolute.** `ReleaseResult` returns `RequeueAt`, the
  grace end itself or now + `restoredEpochRequeue`, instead of a duration,
  and the harness waits until exactly that time. That removes (a).
- **The 1ms fallback becomes "now".** A grace end already past requeues at
  once. That removes (b).
- (c) and (d) are deadlines built as a term plus a constant, which the
  representation handles.

**Every reader of time, and what it does with the value:**

| Reader | Use | How it's handled |
|---|---|---|
| `acquire.go` A1, `at := clock.Now()` | stored as `observed` | a term, compared only |
| `acquire.go` A2, `issued := clock.Now()` (×2) | held across the call, then stored as `written` | a held term (§10.1), then a stored term |
| `release.go` R1, `now` | stored as a first sight; `GraceEnd` = max(dt, first sight) + grace; `now.Before(graceEnd)`; the requeue | the first sight is a term, the grace end a deadline; the requeue is absolute (`RequeueAt = graceEnd`) |
| `release.go` R1, a restored epoch | `RequeueAt = now + restoredEpochRequeue` | case (c): a deadline, term plus 1s |
| `release.go` R2, `now` before the reads | held across the Namespace GET and the list; GraceOver is now ≥ grace end | a held term, compared with a deadline |
| `release.go` R2, `nsReadAt = clock.Now()` | stored in the decision, read by the read monitor | a term |
| `release.go` `requeueAt` | the earliest requeue | an absolute minimum of terms; a past grace end gives now (case (b), removed) |
| `decide.go` `GraceEnd` | max(dt, first sight) + grace, through a clamped `Sub` | a deadline, term plus constant |
| the model: Create and Delete | creation and deletion timestamps | terms; `Patch` keeps them from the stored object, so no truncation |
| the harness: deliveries (`holderEvent`, `listCompleted`) | `recordSince`, `lastList` | terms |
| the harness: holds, `nsDeleted`, a release's `at` | the monitors | terms |
| the harness: requeue waits | until `RequeueAt` | case (a), removed: exactly the grace end |
| the harness: late deletions | until dt + grace/2 | case (d): a deadline, term plus constant |
| the harness: `converge` | its clock at now + 3 × grace, after the search | outside the search |
| the scheduler: `tick` | `min(1µs, gap/1024)` | a fine part; only its order is kept, and the assertion bounds the total |

**What enforces "time is used only by comparison, storage, or adding a
constant":**
- **M-a's guard (§4):** a quiet segment can't read the clock at all.
- **The run-time assertion above.**
- **The merge audit (§10.5).** It catches what a perturbation check can't:
  behaviour that depends on differences in history between two merged
  states.
- **Review:** the table above is the list of readers. A new reader must be
  added to it, and the audit catches one that isn't.

### 10.3 The bounds stay exact under caching

A visited state is stored with the **budgets it was reached with**: the
preemptions, faults and jumps it had left. A state reached again is pruned
only if a stored budget dominates the new one, meaning at least as large in
all three. Every future the new path could take within its budget has then
been searched from that state already. Otherwise the state is searched
again, and the new budget joins the stored set, which keeps only budgets no
other dominates. The review confirmed this dominance rule as sound.

The last actor is part of the state, so the preemption accounting is the
same on both paths. The search stays a replay DFS: a pruned path ends at the
visited state, and the search backtracks as at a terminal state.

### 10.4 What is reported, and the gate (the user's decision)

**Reporting.**
- **Counts are of distinct abstract states, not schedules:** states visited,
  distinct terminal states, and, for each reach flag (U5, L6, H1's state, a
  deleting read), the distinct states in which it is set. Paths merge, so
  the number of schedules is no longer known, and none is reported.
- **A violation** is reported once per distinct violating state, with the
  first path found to it as its witness.
- **The vacuity checks** stay, on distinct-state counts.

**The gate (§3.3), as the user decided: inclusion of terminal states.**
- **The old base** is enumerated statelessly, offline, at each gate profile.
- **Each of its terminal states is mapped through the new abstraction:**
  times to their relative representation, names renamed canonically,
  monitors as §10.1 defines them.
- **The requirement is S_old ⊆ S_new**, per scenario and profile: every
  terminal state the old base reaches, the new base reaches too.
- **The two intended differences go through a declared mapping, or are
  reported for triage, never silently dropped:**
  - crash rule (ii)'s extra pass (§3.2): an old-base terminal state where
    a pass was lost before its first call maps to the new-base state in
    which that pass ran;
  - the redefined thresholds: the late deletions' "into the grace" moves
    from start + grace − 1ms to dt + grace/2.

  An old terminal state that no declared mapping takes into S_new is
  listed for triage.
- **The per-mutant matrix is equal or larger**: every scenario that kills a
  mutant on the old base kills it on the new.
- **Under each killed mutant, the old and new bases report equal sets of
  violation messages.**

### 10.5 Tests of the stateful search

- **The differential test.** On scenarios small enough to search both ways,
  stateful against stateless: the same set of terminal abstract states, and
  the same set of violation messages. The scenarios are Teardown ×3,
  AcquireAgainstRelease, the smaller AwayAndBack, and **the two-process
  scenario, shrunk** (one live pass for B, one deletion pass each), at
  (0,1) and (1,0) with T = 2. The same comparison runs on the revert-check
  mutants the model kills: H1's skip without the exact version, M4's late
  clock, a deletion pass that may skip, and R3 without the array test. Both
  searches must find violations, with the same messages.
- **The merge audit.** In a test mode, at every prune (or a sample of prunes,
  on the big scenarios), keep the pruned path, a concrete state as a replay
  prefix, and the stored witness path. Run both under the same sequences of
  choices, to the end, or for k steps. The abstract traces and the
  violations must be identical.
  - This is the check that the abstraction loses nothing the futures depend
    on: a read that bypasses a hook, a time used as a value, a name the
    renaming misses.
  - It runs on every prune in the differential test's scenarios, and on a
    sample in the others.

### 10.6 Risks and traps, stated before the code

- **How much merges is an empirical question.** Swapping two steps that both
  store times can change the state, and often should: an A2's issue time
  against a delivery time is exactly what t_obs < t_issue compares. The
  gain is expected from jump positions and from steps that store no time. A
  measurement after the review will show it. If it isn't enough for the
  two-process scenario in CI, that will be reported, not worked around.
- **Traps the audit must catch, and the review named:**
  - **time:** a duration computed as a difference and used later (cases (a)
    to (d), removed or made terms); ticks adding up toward U;
  - **actor representation:**
    - a held time frozen as a rank instead of a live term;
    - a name in a closure that the renaming misses;
    - a monitor that reads from an old log position instead of updating on
      events;
    - a canonical traversal that sorts by raw names.
- **The per-actor digest is only as good as its hooks.** A missed read is a
  soundness bug, and the merge audit is the guard against it.
