---
type: Review
title: provider-runtime — credential-use model, third review of spec §12 (sleep sets) at 14c30a3
description: Independent re-review of docs/credential-use-model-reduction.md §12, revision 3: the rule unchanged and stage B ready; spec items for stage A (the recording context as a stack, three clock reads, the analyzer's five rules, the recorder API) and corrections to the MSR-TR reading.
resource: github.com/krateo-platformops/provider-runtime
tags: [review, model-checking, credential-use, sleep-sets]
timestamp: 2026-11-12T00:00:00Z
---

# Re-review: §12 revision 3 (14c30a3)

Worktree agent-a56ab3373faf6abc1, head 14c30a3. I also read e5d5af9, the
terms-only walk. Read only.

I fetched MSR-TR-2007-12 and read it as text. All claims about it below are
from that text.

## Verdict

**The rule (§12.3) needs no change. R1 to R6 are resolved as I meant them,
with four exceptions:**

- **Q1:** three actor-body clock reads still use `Sched.Now`, which revision 3
  keeps unrecorded.
- **Q2:** the recording context must be stack-shaped and live on each `Sched`.
- **Q3:** the analyzer, as written, misses aliases and package-level state,
  and the harness already has both.
- **Q3, epochs:** one package-global counter would make the read audit's
  concrete comparison fail on every pair.

None of these is a rule-level defect. Stage B is ready to code. Stage A is
ready once these are written into §12.2 and §12.4. They are spec edits, not
redesigns.

## 1. The R-items

- **R1, the recording context.** Resolved: on and off are as specified, and
  `running` is not used for attribution. Two refinements are in Q2.
- **R2, the terms diff.** Resolved: the term set is diffed without now.
  - e5d5af9's terms-only walk still calls `w.s.Now()` (state_test.go:337).
    That is harmless with the context off.
  - `TestTimeTermsMatchTheEncoding` checks that the walk's term set equals
    the full encoding's.
- **R3, variants.** Resolved: variants are uninterrupted runs.
- **R4, the final blocking guard.** Resolved: its reads are in the union, and
  proof step 1 is amended.
  - A time-blocked final Wait never has its guard evaluated, because
    `enabled` short-circuits on notBefore (sched.go:620). That is fine. A's
    pending notBefore is a deadline change, which conflicts with π's ticks;
    without ticks the clock is the same at s and s'.
- **R5.** Resolved: the same-actor exemption, the realized ⊆ union assert,
  idempotent guards, and shared state only in structs.
- **R6.** Partly resolved.
  - `apimodel.Proc` is now in the detector snapshot.
  - The model has its own clock function, and `Sched.Now` stays unrecorded.
- **R6, still open: three monitor clock reads use `Sched.Now`.** §12.2 lists
  "the read monitor" among the recorded clock reads. But the holder's body
  reads `w.s.Now()` at three call sites:
  - `holds[id] = hold{at: w.s.Now()}` (model_test.go:527);
  - `w.onRead(spec.name, w.s.Now(), …)` (model_test.go:540);
  - `at := w.s.Now()` before `onRelease` (model_test.go:617).

  These reads would be silently dropped.
  - Each value feeds a safety verdict: `h.at` against the deletion timestamp,
    and `at` against `nsDeleted` and `GraceEnd`.
  - In these call sites another conflict happens to cover it: the
    transitions also touch `nsDeleted`, `holds` and the model.
  - *Fix:* move all three to the recorded clock, and have the analyzer forbid
    `Sched.Now` outside `Key`, `Terms` and setup.

## 2. Can the recording context drop a read silently?

**No guard is evaluated during `Key` or `Terms` today.** The encoder reads
`Views()` and `Locals()`, never a condition (state_test.go:335-440). `Locals`
runs with the context off, which is correct: those are actor-local values for
the hash.

**Restart attribution is unambiguous.** The only crash path is `FaultCrash`
inside `Call` (sched.go:489). It runs inside the crashing call's body, and
`restart(a.s.Clock.Now())` evaluates now after `running = nil`
(sched.go:502-505). A context keyed on the transition, not on `running`,
records it.

**What the spec must still say:**

1. **The context nests, so it is a stack.**
   - "Off" scopes run *inside* "on" scopes: `tick()` calls `pending()` →
     `Deadlines()` → `m.List`, and `Terms()`, at the start of a call step
     (sched.go:752-753, 841, 857).
   - Guard evaluation, which is "on", happens inside the scheduler's option
     loop and in F1's extra evaluation. Whether the loop itself is
     "on" or "off" is not specified.
   - A flag set at the step's boundaries would either record the tick's
     instrumentation or switch off the body that follows. Specify a stack:
     push and pop, with the innermost scope deciding. Assert that the stack
     is empty between steps.
2. **The context lives on the `Sched`, not in a package variable.**
   `ExploreParallel` runs Scheds concurrently. A global context would race,
   and reads would be attributed to another worker's transition. Tracked
   accessors must reach the context through their world, and the world holds
   its `Sched`.
3. **Guard attribution names a state, and the scheduler evaluates guards at
   two kinds of point.**
   - Recorded: the option loop (sched.go:659) and F1's evaluation of last.
   - Not needed for dependence, and should be off: `liveActors`
     (sched.go:885). It is harmless either way.
   - Say which, so that an implementation doesn't record guard reads into the
     *previous* transition's footprint.

## 3. Does the analyzer close the read side?

The rule as written: "forbids any access to those fields outside the
accessors", and closure variables shared between actors. It leaves these gaps:

- **Aliases.**
  - `holds := w.holds[spec.name]` (model_test.go:464) holds a map from the
    world for the whole lifetime of the holder actor. Writes and reads go
    through `holds[...]` at model_test.go:523-541, and onModelEvent does the
    same (287). A field-access check sees one access at setup and none of the
    later ones.
  - Rule: tracked accessors return values or immutable copies, never inner
    maps, slices or pointers. Flag any local that stores a tracked
    container.
  - The cache is safe here: it never mutates an object in place.
    `Cache.Get` and `Cache.In` return pointers, but `Next` and `List` store
    fresh objects (cache.go:94-98, 47-71). A stale pointer is a value
    captured at a recorded read.
- **Package-level mutable state.** `epochSeq atomic.Int64`
  (harness_test.go:272) is taken by every `testEpoch()` that the model's
  protocols call (harness_test.go:275, 288).
  - It is shared by all actors, all schedules and all parallel workers.
  - It is invisible to the footprints and to the snapshot.
  - It is renamed in the abstract key, so abstract comparisons survive.
  - But §12.4's read audit compares *concrete* terminal states. Two replays
    of the same schedule already get different epoch names, so every pair
    would mismatch.
  - *Fix:* a per-world epoch counter, tracked; or compare after the key's
    renaming. The analyzer should forbid writes to package-level variables
    from model code.
- **Production types.** `verified` and `firstSight` (verified.go) are the
  process memory, and their accessors are their own methods.
  - Today `onAccess` is called in each method (verified.go:96, 134, 149,
    161, 174, 190, 203, 242, 256), but without a mode.
  - The tracked version needs a read or write mode per method. The analyzer
    should check that every method of a tracked type calls the hook with a
    mode on entry.
  - A wrongly declared mode is caught for writes by the detector. For reads
    it is caught only by the audit.
- **reflect, unsafe and fmt.**
  - Code using `reflect` can read every field, unexported ones included,
    bypassing every accessor.
  - `fmt`'s `%v` on a tracked struct reads every field reflectively. Into
    `errs`, through `w.fail`, that becomes state.
  - Allowlist the detector and the encoder, and forbid `reflect` and
    `unsafe` elsewhere in the model harness. Also forbid passing tracked
    types to `fmt` verbs.
- **Interfaces.** These are fine, provided every implementation reaches the
  state through accessors. `cacheViews`, passed to the protocol, uses the
  `Cache` methods.
- **Closures.** "Shared between actors" must include closures reached
  through helpers, such as specs passed to `w.holder` or guard closures built
  in scenario code. Variables written only during setup are exempt; that is
  checkable as "never written after `s.Run` begins".

With these five rules (no aliases, no package state, a hook with a mode in
every method, no reflect or unsafe or fmt on tracked types, and the closure
rule), the read side is closed by construction within the model packages.
The remaining dependency is that the accessors themselves name the right
object, which the audit checks.

## 4. Is the MSR-TR reading accurate?

Checked against the report's text. The reading is accurate in substance.

**What is right:**
- **Figure 1.** "the execution y := 2, x := 1, y := 1 has context-bound 0"
  and is lost under sleep sets.
- **The quote.** "we believe that a similar issue would arise even with other
  partial-order reduction algorithms" is exact.
- **The bound.** CB(α→t) adds ite(t = last(α) ∨ last(α) ∉ E(final(α)), 0, 1).
- **States.** "We represent a state as a happens-before graph".
- **Algorithms 1 and 2.** Each partial order, with all its prefixes, is
  explored "precisely once". Algorithm 2 visits each terminal partial order
  once.
- **Theorem 2.** s ∈ H iff some execution reaches s with CB(s) ≤ c.
- **CB(s, t).** It is computed by dynamic programming with memoization.

**Corrections:**
- **"Figure 1 is our §12.1 example."** It is the same phenomenon, not the
  same program: their T1 is *disabled*, rather than staying enabled.
- **"NP-hard (Theorem 5)."** The report proves NP-complete.
- **Theorem 2's "state" is a happens-before graph, a trace, not a program
  state.** The mapping to our terminal global states holds, since a global
  state is reachable within the bound iff one of its traces is. It should be
  said explicitly.

**What the fallback paragraph should add:**
- **The edges are per variable.** Their happens-before edges are reads and
  writes per variable (§3.3). They have no commuting-write class.
  - Our ticks would be writes to the clock, so every call would be ordered
    after every other, with almost no reduction.
  - The fallback needs §12.2's tick relation, not plain reads and writes.
- **No invisible transitions.** Their model has no jumps and no quiet steps.
  `last(α)` is always the previous step's thread.
  - With our jumps, which don't change `last`, the CB(s, t) recursion over
    the frontier has to carry the last visible actor through jumps.
  - Otherwise CE2's mechanism reappears in the bound computation.
- **It is stateful and keyed on traces.** That makes it independent of §10's
  abstract key, which is useful for its role as a cross-check. But it shares
  §12.2's footprints, so the footprint work is a prerequisite either way.
- **Only P needs per-state handling.** F and T are the same in every
  linearization of a trace, so its CB-per-state applies to P only.

## 5. Are stages A and B ready to code?

**Stage A: ready once these spec items are added.**
- Q1: the three `Sched.Now` call sites.
- Q2: the context is a stack, lives on the `Sched`, and its on and off sites
  are named.
- Q3: the five analyzer rules, and a per-world epoch counter.
- **Where the recorder lives.** The CE tests in sched_test.go and the
  generated differential use calls with *declared* read and write sets, at
  the `apimodel` level, while the harness uses tracked accessors.
  - Put the recorder API in `apimodel`: `Sched.Read`, `Sched.Write`,
    `Sched.Tick`, plus object naming. Both sources call it.
  - Define the object-name scheme once, and use it for the detector's
    snapshot components too.
- **Sequencing.** The CE unit tests compare reduced with unreduced, so they
  can't pass before stage B.
  - Either move them to B, or in A record their unreduced terminal sets as
    fixtures.
- **Detector cost.** A deep copy of the model and processes twice per
  transition, at about 450 KB per schedule already. On the T = 2 soundness
  differential (millions of schedules) that is a large multiple of the VM
  time, and it is not in the cost table.
  - Either run it on every transition in the tests, the generated
    differential and Teardown, and sampled per schedule elsewhere;
  - or cost it.

**Stage B: ready.**
- The rule and its bookkeeping are fully specified. One point should be
  written down: a's options at s are contiguous (its fault variants), so
  "a's subtree is searched" is the moment explore()'s backtracking moves past
  a's last option at that depth.
- The union is accumulated per depth from every run whose first steps from s
  are a's uninterrupted complete segment.
- Deferred pruning returns a pruned status that explore() and
  ExploreParallel count as neither a failure nor a terminal state.
- Because nothing is filtered, choice indices are stable under replay. That
  is a useful property of F3; it should be stated and tested.
