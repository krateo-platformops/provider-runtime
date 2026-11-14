---
type: Review
title: provider-runtime — credential-use model, re-review of spec §12 (sleep sets) at 80656bf
description: Independent re-review of docs/credential-use-model-reduction.md §12, revision 2: sound as far as the hand search went, with fixes R1 to R6, the read side by construction, the proof's outline, and the order of the work.
resource: github.com/krateo-platformops/provider-runtime
tags: [review, model-checking, credential-use, sleep-sets]
timestamp: 2026-11-12T00:00:00Z
---

# Re-review: §12 revision 2 (80656bf)

Worktree agent-a56ab3373faf6abc1, head 80656bf. Read only. The review copy
(docs/credential-use-model-reduction-review-12.md) matches my d7fa08b write-up,
plus a frontmatter header.

## Verdict

- **The rule is now sound up to its stated assumptions, as far as my search
  goes.**
  - F1 closes CE1, F2 closes CE2, and F3 closes the gap left by interrupted
    runs.
  - I found no new counterexample at the level of the rule. The search was by
    hand, not exhaustive.
- **Two new defects would make the footprints, as specified, collapse the
  reduction to roughly nothing:** R1 and R2. Neither is a soundness defect,
  but both are traps.
  - R1's natural workaround (record only when `running != nil`) reopens D3
    exactly.
- **The proof sketch is now correct in outline but not yet a proof.** One step
  is false as stated (R4). Section 4 lists what turns it into a proof.

## 1. The counterexamples, and a search for new ones

**CE1 is closed by F1.** After c0 the last actor c is blocked by guard
`started >= 1`. F1 evaluates that guard with read-recording, meets A's write,
and drops the entry. The branch c c0 a a1 b b1 is explored and reaches
R = (1, 0).

**CE2 is closed by F2.** After l1, L is blocked by its notBefore T1. The point
is not firm, so nothing is applied, and σ is explored.

**The lost-state example (§12.1) is handled.** b2 reads V, which is in A's
union, so the entry is removed.

**What I searched, and found sound:**
- *A that is wholly quiet.* `last` is unchanged in both orders. M, the last
  actor at s, stays disabled because quiet writes nothing.
- *A ending time-blocked.* Step 4 covers it: the pruned order may cost more,
  never less.
- *A crash inside A or inside π, in a's process.* The generation read by
  `Stale` makes it dependent.
- *π's actors parking at deadlines that A's ticks could pass.* Either the
  deadline is pending (ticks never reach it) or it is a deadline change
  (conflicts with ticks).
- *Two entries at one s'.* Both are deferred, so no state has every option
  asleep.
- *Entry sets in ExploreParallel.* Fewer entries means fewer prunes, which is
  monotone, so a smaller entry set is sound.
- *Fault variants at s'.* They are a subset of those at s, because the budget
  only shrinks.
- *Interrupted deferred runs* (by a preemption, a quiet resume or a jump).
  They are explored.
- *The firm point and jumps or quiet resumes.* Guards don't read the clock,
  jumps write only the clock, and quiet writes nothing shared. So a firm L
  cannot be woken invisibly.

**Residual edge, low.** A guard that reads another actor's *closure-captured*
local is invisible to F1 and to the write detector. Firmness then fails if that
other actor's quiet resume writes the local. The harness has no such guard
today, but nothing forbids one.
- *Fix:* every variable shared between actors lives in the world struct or
  the process struct; enforce it by review or an analyzer.

### R3 (medium; wording that could zero the factor)

§12.3 says: "No entry is made ... for an actor any of whose searched variants
did not end with the actor blocked or done."

- a's subtree at s always contains runs interrupted by a jump (when T > 0) or a
  preemption (when P > 0), and those don't end blocked.
- Read literally, no actor with a multi-step segment ever gets an entry at
  T ≥ 1.
- *Fix:* variants are the uninterrupted runs only. The union is over those,
  and D7's exclusion is for uninterrupted runs that never block, such as
  always-ready loops.

### R4 (low; proof step 1 is false as stated, but a cover exists)

Step 1 says "A from s' is a variant of a's segment from s." That fails if π
writes what A's **final blocking guard** reads.
- That guard belongs to a's next transition, so it is not in A's union, and π
  stays independent.
- At s', a then runs past where A(s) blocked: it runs A(s)·X, and F3 prunes
  only once that ends.
- A cover still exists: A(s)·π·X·ρ.
  - Its first step of π is free.
  - a is enabled during π but never last.
  - X starts after L, which is firm.
- *Fix (pick one):*
  - add the final guard's reads to the union, which is conservative and
    simplest;
  - or state this extended case in the proof.

### R5 (low)

- **a's own steps in the deferred run.** They must not remove a's entry under
  the "same actor" rule. Say so.
- **Assert the realized footprint.** When the deferred run reaches its end,
  assert that its realized footprint is within the union. It is cheap, and it
  catches non-determinism in the footprints.
- **Guard evaluations.** F1 adds guard evaluations, so guards must be
  *idempotent*: their side effects must be a function of what they read, as
  `dt`'s is (model_test.go:1135-1141).

## 2. The D3 list: what is still missing

### R1 (high, effectiveness): instrumentation runs through the accessors that get recording hooks

- **`Terms()` reads the clock and the whole model.** It is `w.encode(nil).terms`
  (state_test.go:301). It calls `w.s.Now()` (state_test.go:307), which is
  `Sched.Now`, now hooked. It also calls `w.m.Objects()` → `Snapshot`
  (state_test.go:311), now hooked, as well as `EventsAfter` and
  `Cache.Objects` (state_test.go:316-331).
- **`deadlines()` reads the model too.** It calls `w.m.List` (model_test.go:345),
  which goes through `m.guard()`.
- **Both run inside every call transition.** `tick()` calls `pending()` and
  `Terms()` (sched.go:841, 857) before the op. `Key` runs between choices
  (sched.go:704-723). The diffs of §12.2 call `Terms()` and `pending()` twice
  per transition, and the write detector snapshots everything.
- **If all of that is recorded, every call reads the clock and every key.** So
  no two calls are ever independent, and the factor is about 1.
- **The obvious fix is wrong.** "Record only when running ≠ nil" drops the
  restart's reads and the guard reads, which also run with `running == nil`.
  That is D3 again.
- **The same accessor serves both purposes.** `Cache.Objects` serves the
  monitor's `peekCache` (to be recorded) and the state hash (not to be
  recorded).
- *Fix:* an explicit recording context, set by the scheduler.
  - It is on for a transition's body, for crash restarts (attributed to the
    crashing call), and for guard evaluation (attributed to the guarded actor
    at that state).
  - It is off for `Key`, `Terms`, `Deadlines`, the diffs and the detector.
  - This is sound because the effect of `Terms` and `Deadlines` on ticks is
    already captured by the term and deadline diff.

### R2 (high, effectiveness): `Terms()` includes now

- `encode` writes `e.time(w.s.Now())` first (state_test.go:306-307), and
  `time()` appends to `terms` (state_test.go:111).
- So a diff of `Terms()` "changes" on every call, because the tick moves now.
  Every call then conflicts with every tick.
- *Fix:* diff the term set without now. `tick()` itself only uses terms whose
  fine part is above now's (sched.go:858).

### R6 (medium)

- **Snapshot scope.** The write detector's snapshot must include
  `apimodel.Proc` (`gen`, `crashes`; sched.go:148-153, 498-499). It is not the
  harness's process struct, so "every process's memory" does not cover it.
- **Shared closure variables.** These are invisible to any snapshot (see the
  residual edge in section 1).
- **The `Sched.Now` hook.** It is documented as "for the state hash, without
  recording a read" (sched.go:404-405). Hooking it changes that contract.
  Better to give the model its own clock function, or to rely on R1's context.

### What is now complete

Against sched.go, model.go, cache.go and the harness, I found no other omission:
- generation reads;
- the crash's clock read and relist;
- `Snapshot`, `FinalizeNamespace`, `Cache.List`;
- the model timestamps;
- the deadline sources;
- the world fields and the peeks;
- guard attribution.

Two items are fine as listed: the log already covers `uid` and the
namespace read in Create, and `errs` is now ordered.

## 3. Can the write detector catch a missing read?

**No.** A snapshot diff sees only changes, so a missing read hook is
invisible to it. That covers, for example, the `Stale` generation read, the
restart's relist, `Snapshot`, `Sched.Now`, and actor code reading a world
counter.

**The stratified read audit is necessary but not sufficient as specified:**
1. **Stratify by pair, not by transition.** A missing read is exposed only when
   the reader is adjacent to a writer of the same object.
   - The strata should be (reader kind × writer kind × same process or not).
     The bad pairs: crash with a mid-unit call of the same process, restart
     with a model write, delivery with a write, Create or Delete with a timed
     park, guard-only with a counter write.
   - Six one-sided strata will rarely draw them.
2. **Compare at the end, not right after the pair.**
   - Run both orders on to a terminal state with the same continuation (by
     label, through `ReplayLabels`) and compare there.
   - A read whose value goes only into a Go local is invisible right after
     the pair. Not every read is a `RecordInput`, and closure locals not
     listed in `Locals` are not snapshotted.
3. **Size the samples.** To see, with confidence 1 − δ, a defect that a
   fraction p of a stratum's pairs exposes, a stratum needs
   n ≥ ln(1/δ)/p samples.
   - For example, δ = 0.05 and p = 10⁻³ need about 3,000 per stratum.
   - p is unknown, so report n per stratum and the bound it gives.
4. **Make it exhaustive where it is affordable.**
   - Run every adjacent independent pair of every schedule on CE1, CE2, the
     lost-state test and the guards-and-timers scenario.
   - Sample on the large scenarios.

**What actually closes the read side is completeness by construction.**
- Put every shared field (world, process memory, counters) behind a tracked
  accessor type that records reads in the R1 context.
- Add a small go/analysis check that forbids direct access outside the
  accessors.
- The audit then becomes a backstop rather than the guarantee.

## 4. Is the proof sketch enough to rely on?

**Not yet.** The outline is right and every step I checked holds with the
assumptions, except step 1 (R4). Two of the assumptions are carrying real
weight: footprint completeness, and the correctness of the rule's
implementation.

**What would turn it into a proof:**
1. **A precise model of a step.**
   - The state: model, processes, world, actors, clock, `last` and the bound
     counters.
   - The transition kinds as sched.go has them, including forced jumps and
     the tick of sched.go:833-873.
   - The cost function of sched.go:664 and 742.
2. **A diamond lemma per pair of transition kinds.** Independent t and u, in
   either order, give equal states and preserve each other's enabledness.
   - This includes a tick lemma: with no term or deadline change, n ticks
     from now give fⁿ(now), whatever actors make them.
   - This is where the footprint-completeness hypothesis enters, stated once.
3. **The swap lemma, by induction on |π|.**
   - It moves A past one transition at a time and accounts for the cost at
     each step: F1 at that state, and the time argument.
   - It includes the quiet-A case and R4's extended segment.
4. **The search theorem, as an invariant of the DFS with entries.**
   - The invariant: for every reduced node with entry set Z, every unreduced
     schedule from it within the remaining budget is either explored or has
     a cover in an earlier subtree that costs no more.
   - The proof is well-founded induction on DFS order. It must cover
     inherited entries (a cover can itself be pruned by an older entry: apply
     the lemma from that entry's origin), and show that any subset of
     entries, as in ExploreParallel, is still sound.
5. **Evidence for the rule itself, separate from the footprints.** This is a
   generated differential:
   - random tiny scenarios: at most 4 actors and about 4 steps each, built
     from the scheduler's primitives, namely calls with declared read and
     write sets, guards on counters, timed waits and thresholds, quiet
     starts, faults and crashes, and daemons;
   - for each, reduced against unreduced, exhaustively, comparing terminal
     sets at P ∈ {0, 1} and T ∈ {0, 1};
   - it is cheap, and it would have found CE1 and CE2 mechanically. The
     hand-built tests are only three points.
6. **MSR-TR-2007-12, read before the code**, as §12.5 plans. It is the closest
   prior work on this exact combination.

With items 1 to 4 written out, and item 5 run on a few thousand scenarios with
no mismatch, I would rely on it. Without them it is a plausible argument that
has already been wrong twice.
