package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/utils/clock"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

// The model tests (§14, "Unit tests for the races") run the protocol's own
// code against the in-memory API model, on every schedule within the bounds
// below, and check the safety invariant on every read:
//
//	a holder H that is deleting (with the owner finalizer), whose pass held P
//	before H's deletion began, never reads P as NotFound, unless one of these
//	happened first: P was released while its namespace was live and no user
//	of P was deleting (U5's release); Disown ran; a third party stripped the
//	finalizer.
//
// Disown and third-party stripping don't occur in these scenarios, so the
// only allowed exception is U5's release, and it is checked to be a real one:
// the release read P's Namespace live, before any deletionTimestamp it got
// (§6.4's unrecoverable window, L3, is a namespace deletion that starts after
// that read). Convergence is checked separately: once no user of P is left,
// the finalizer goes within a bounded number of batches.
//
// The bounds are a number of preemptions and of injected faults (see
// apimodel). CREDENTIALUSE_MODEL_DEEP=1 raises them for a longer run.
//
// What the model doesn't cover, by construction (PR 3's and PR 7's stress
// tests do):
//
//   - There is no enqueue model: a batch runs when its actor is chosen, not
//     when an event would enqueue it, so neither #52-M1's lost batches nor
//     the index-prune race are reachable.
//   - Releasers are never faulted, so a lost R3 response (the patch landed,
//     the releaser saw an error) isn't searched; its next batch reads P
//     afresh, which R1 handles like any other state.
//   - A batch holds one P, of one kind, in one namespace.
//   - The holder informer's handling of a holder whose UID changed under the
//     same name (harness_test.go's deliver: a deletion of the old UID, then
//     the new one) is the harness's; PR 3's informer must do the same.
//   - firstSight is pruned without a cache, from what R1 and R3 see; that is
//     memory hygiene only, and no decision depends on it.

// testGrace is long next to the at most 1s a call ticks, so that only a jump
// crosses a grace end (docs/credential-use-model-reduction.md §3.1). The
// scheduler panics if a tick would cross a pending deadline. It and every
// other constant the model adds to a time are multiples of the time unit U,
// 1h (§10.2): the grace is 1000 U, half of it 500 U.
const testGrace = 1000 * apimodel.TimeUnit

// testEpochRequeue is the model's restored-epoch requeue: one U, where
// production's is 1s (restoredEpochRequeue), so that now plus it is a term
// plus a multiple of U (§10.2, case (c)).
const testEpochRequeue = apimodel.TimeUnit

// modelProtocol is newProtocol for the model's actors: the test grace, and
// the model's restored-epoch requeue.
func modelProtocol(srv server, v views, ver *verified, seen *firstSight, c clock.PassiveClock) *protocol {
	p := newProtocol(srv, v, ver, seen, c, testGrace)
	p.restoredEpochRequeue = testEpochRequeue
	return p
}

// testJumps is how many times a schedule may jump to a deadline (§3.1, the
// user's T).
const testJumps = 2

// A profile is one search of a scenario's schedules: exhaustive within
// bounds, or a seeded random walk of n schedules.
type profile struct {
	name   string
	bounds apimodel.Bounds
	walk   int
}

// A scenario's flags: what it needs searched, and what it must reach.
type scenarioFlags int

const (
	// bothBounds: by default, also search every schedule with one
	// preemption and one fault together (M2 of #59's review), for the
	// scenarios with a second process.
	bothBounds scenarioFlags = 1 << iota
	// needsU5: some schedule of the exhaustive searches must release P over
	// a live user after the grace (§5's last row). Without a preemption the
	// deleter deletes the holder right after the Secret, inside the grace,
	// so it is the preemption searches that reach it.
	needsU5
	// needsL6: some schedule of each exhaustive search must run a pass
	// inside §6.5's L6 window.
	needsL6
	// needsH1: some schedule of the exhaustive searches must start a live
	// pass in the state H1 of #59's review is about: the holder's record
	// names P, so the pass's A1 won't add it; P has lost its finalizer; and
	// the process's cache still shows P with it, under a written entry of
	// its own. A skip there would report P held with no finalizer.
	needsH1
	// deepOnly: the scenario's exhaustive searches run only under
	// CREDENTIALUSE_MODEL_DEEP or CREDENTIALUSE_MODEL_BOUNDS; by default and
	// under the race detector it runs its random walks only. Its one-fault
	// search alone is 2.6 million schedules, too many for CI until the
	// scheduler reduces its state space.
	deepOnly
)

// profiles are the searches a scenario runs.
//
//   - By default: every schedule with no preemption and one fault on the
//     holders' calls, every schedule with one preemption and no fault, with
//     bothBounds every schedule with one of each, and 400 random schedules
//     with two preemptions and one fault. CI runs these in a step of their
//     own, without the race detector.
//   - Under the race detector, which slows each schedule several times and
//     finds nothing in the model (one coroutine runs at a time), random walks
//     only: 300 with one fault, 300 with two preemptions and one fault. A
//     deepOnly scenario runs these walks by default too.
//   - CREDENTIALUSE_MODEL_DEEP=1: every schedule with one preemption and one
//     fault, and with two preemptions and none, and a long walk with three
//     and two, with informers delivering one event per step.
//   - CREDENTIALUSE_MODEL_BOUNDS=p,f,n[;p,f,n...]: exhaustive searches with
//     those preemptions, faults and run caps, for measuring and for the
//     revert check.
//
// The random walks' seed changes from run to run, and is logged;
// CREDENTIALUSE_MODEL_SEED replays one.
func profiles(t *testing.T, flags scenarioFlags) []profile {
	if v := os.Getenv("CREDENTIALUSE_MODEL_BOUNDS"); v != "" {
		var out []profile
		for _, part := range strings.Split(v, ";") {
			b := apimodel.Bounds{Jumps: testJumps}
			fields := strings.Split(part, ",")
			if len(fields) == 4 {
				if _, err := fmt.Sscanf(part, "%d,%d,%d,%d", &b.Preemptions, &b.Faults, &b.MaxRuns, &b.Jumps); err != nil {
					t.Fatalf("CREDENTIALUSE_MODEL_BOUNDS=%q: %v", v, err)
				}
			} else if _, err := fmt.Sscanf(part, "%d,%d,%d", &b.Preemptions, &b.Faults, &b.MaxRuns); err != nil {
				t.Fatalf("CREDENTIALUSE_MODEL_BOUNDS=%q: %v", v, err)
			}
			out = append(out, profile{name: fmt.Sprintf("%d preemptions, %d faults", b.Preemptions, b.Faults), bounds: b})
		}
		return out
	}
	if os.Getenv("CREDENTIALUSE_MODEL_DEEP") != "" {
		return []profile{
			{name: "1 preemption, 1 fault", bounds: apimodel.Bounds{Preemptions: 1, Faults: 1, Jumps: testJumps, MaxRuns: 5_000_000}},
			{name: "2 preemptions", bounds: apimodel.Bounds{Preemptions: 2, Jumps: testJumps, MaxRuns: 5_000_000}},
			{name: "random: 3 preemptions, 2 faults", bounds: apimodel.Bounds{Preemptions: 3, Faults: 2, Jumps: testJumps}, walk: 50_000},
		}
	}
	if raceEnabled || flags&deepOnly != 0 {
		return []profile{
			{name: "random: 1 fault", bounds: apimodel.Bounds{Faults: 1, Jumps: testJumps}, walk: 300},
			{name: "random: 2 preemptions, 1 fault", bounds: apimodel.Bounds{Preemptions: 2, Faults: 1, Jumps: testJumps}, walk: 300},
		}
	}
	out := []profile{
		{name: "1 fault", bounds: apimodel.Bounds{Faults: 1, Jumps: testJumps, MaxRuns: 200_000}},
		{name: "1 preemption", bounds: apimodel.Bounds{Preemptions: 1, Jumps: testJumps, MaxRuns: 200_000}},
	}
	if flags&bothBounds != 0 {
		out = append(out, profile{name: "1 preemption, 1 fault", bounds: apimodel.Bounds{Preemptions: 1, Faults: 1, Jumps: testJumps, MaxRuns: 5_000_000}})
	}
	return append(out, profile{name: "random: 2 preemptions, 1 fault", bounds: apimodel.Bounds{Preemptions: 2, Faults: 1, Jumps: testJumps}, walk: 400})
}

// auditEvery is how often a stateful search audits a prune:
// CREDENTIALUSE_MODEL_AUDIT_EVERY, or every 100th, which keeps CI's model step
// within its timeout.
func auditEvery() int {
	if v := os.Getenv("CREDENTIALUSE_MODEL_AUDIT_EVERY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 100
}

var walkSeed = sync.OnceValue(func() int64 {
	if v := os.Getenv("CREDENTIALUSE_MODEL_SEED"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return time.Now().UnixNano()
})

// world is one schedule's model, processes and record of what happened.
type world struct {
	s  *apimodel.Sched
	m  *apimodel.Model
	ns string
	p  ID // the protected Secret

	// The monitors (docs/credential-use-model-reduction.md §10.1): the checks
	// run on the fly, at the events they are about, and keep only this.
	releasedAny, u5Any, deletingRead bool
	// lastAllowed is the latest release after which the safety invariant
	// allows a deleting holder to read P as NotFound: U5's.
	lastAllowed *timedDecision
	nsDeleted   time.Time
	errs        []error
	// l6 counts passes inside §6.5's L6 window.
	l6 int
	// h1 counts live passes that start in needsH1's state.
	h1 int
	// onDone, if set, runs after the schedule, before the checks.
	onDone func()
	// livePasses counts each holder actor's live passes that ran, however
	// they ended.
	livePasses map[string]int
	// batches counts each releaser's batches that ran, and batchesStarted
	// those that began; started counts each holder actor's live passes that
	// began.
	batches, batchesStarted, started map[string]int
	// holds[holder][P]: the earliest live pass that held P, in the current
	// unbroken run of the holder's versions naming P.
	holds map[string]map[ID]hold
	// deletionPhase[actor]: the holder actor ran a pass on its deleting
	// holder, or found it gone (M3 of #59's review).
	deletionPhase map[string]bool
	procs         map[**process]*apimodel.Proc
	// thresholds are deadlines a scenario declares (§3.1).
	thresholds []time.Time
	// procOrder is the processes in the order they joined, for the state
	// hash; pinned are names a scenario's closures captured, which the
	// state's renaming must cover (§10.1).
	procOrder []**process
	pinned    []any
}

// hold is a live pass's hold of P, from when it began. The model's event
// hook ends it at the first version of the holder whose record drops P
// (onModelEvent).
type hold struct {
	at time.Time
}

type timedDecision struct {
	ReleaseDecision
	at time.Time
}

func newWorld(s *apimodel.Sched) *world {
	w := &world{s: s, ns: "rlgov-probe", livePasses: map[string]int{}, batches: map[string]int{}, batchesStarted: map[string]int{}, started: map[string]int{}, holds: map[string]map[ID]hold{},
		deletionPhase: map[string]bool{}, procs: map[**process]*apimodel.Proc{}}
	w.m = apimodel.New(s.Now) // the API server's clock: not an actor's read
	w.p = SecretRef(w.ns, "probe-dummy-creds").ID()
	s.Deadlines = w.deadlines
	s.Terms = w.timeTerms
	// The quiet-segment reduction (§4) is work in progress, not reviewed, and
	// off by default: CREDENTIALUSE_MODEL_QUIET turns it on.
	s.NoQuiet = defaultQuiet().off
	w.m.OnEvent = w.onModelEvent
	w.m.Guard = func() {
		if s.InQuiet() {
			panic(fmt.Sprintf("model: a quiet segment touched the API model (§4, M-a)\n%s", debug.Stack()))
		}
	}
	return w
}

// onModelEvent is the hold monitor (§10.1): every version of a holder, by
// any actor, whose record no longer names P ends that holder's hold on P.
// The protection lives in the record, so a hold lasts only while every
// version since names the ref; neither a lost A1 nor another process's
// write can leave a stale one (M1 of #59's review).
func (w *world) onModelEvent(ev apimodel.Event) {
	if ev.Key.GK != widgetGK {
		return
	}
	holds := w.holds[ev.Key.Name]
	for id := range holds {
		if ev.Type == watch.Deleted || !ReadRecord(&ev.Obj).Names(id) {
			delete(holds, id)
		}
	}
}

// onRead is the read monitor: a deleting holder that held P before its
// deletion began must not read P as NotFound, unless U5's release came
// first, and that release read the namespace live before any deletion of it
// (§6.4, L3).
func (w *world) onRead(holder string, at time.Time, notFound, held bool) {
	if !held {
		return
	}
	w.deletingRead = true
	if !notFound {
		return
	}
	d := w.lastAllowed
	if d == nil {
		w.fail("SAFETY: deleting holder %s read %s as NotFound at %s, after holding it, with no U5 release before",
			holder, w.p, at.Format(time.TimeOnly))
		return
	}
	if !w.nsDeleted.IsZero() && !w.nsDeleted.After(d.NamespaceReadAt) && !d.NamespaceReadAt.IsZero() {
		w.fail("SAFETY: a release read namespace %s as live at %s, after its deletion at %s",
			w.ns, d.NamespaceReadAt.Format(time.TimeOnly), w.nsDeleted.Format(time.TimeOnly))
	}
}

// onRelease is the release monitor. A release over a live user after the
// grace (U5) that follows a namespace deletion begun inside the grace is a
// violation (§6.4, M4 of #59's review); and an allowed release becomes the
// one later reads are judged against.
func (w *world) onRelease(d ReleaseDecision, at time.Time) {
	w.releasedAny = true
	in := d.Input
	if isU5(in) {
		w.u5Any = true
		if !w.nsDeleted.IsZero() && !w.nsDeleted.After(at) && w.nsDeleted.Before(d.GraceEnd) {
			w.fail("SAFETY: %s was released over a live user at %s, with its namespace deleted at %s, inside the grace that ended at %s",
				d.P, at.Format(time.TimeOnly), w.nsDeleted.Format(time.TimeOnly), d.GraceEnd.Format(time.TimeOnly))
		}
	}
	if in.Deleting && !in.NamespaceDeleting && in.GraceOver && in.DeletingUsers == 0 {
		w.lastAllowed = &timedDecision{d, at}
	}
}

// deadlines are the scenario's pending deadlines besides the actors' own
// notBefores (§3.1): for every protected object being deleted, the end of
// its grace in each process, from that process's first sight, and from the
// deletion itself for a process that hasn't seen it; and the thresholds the
// scenario declares. It reads first sights directly, as instrumentation.
func (w *world) deadlines() []time.Time {
	ds := slices.Clone(w.thresholds)
	for _, o := range w.m.List(secretGroupKind, w.ns) {
		if o.DeletionTimestamp == nil {
			continue
		}
		dt := o.DeletionTimestamp.Time
		ds = append(ds, GraceEnd(dt, dt, testGrace))
		for p := range w.procs {
			if seen, ok := (*p).seen.at[o.UID]; ok {
				ds = append(ds, GraceEnd(dt, seen, testGrace))
			}
		}
	}
	return ds
}

func (w *world) fail(format string, args ...any) {
	w.errs = append(w.errs, fmt.Errorf(format, args...))
}

// proc is the scheduler's process for p: every actor of p crashes together,
// and the crash replaces p with a fresh process, whose caches list the model
// then (M3 of #59's review).
func (w *world) proc(p **process) *apimodel.Proc {
	if pr, ok := w.procs[p]; ok {
		return pr
	}
	pr := w.s.Proc((*p).name, func(now time.Time) { (*p).restart(now) })
	w.procs[p] = pr
	// A unit of work lost to a crash must not touch the process's memory,
	// not even to read it (§3.2).
	guard := func() {
		if w.s.InQuiet() {
			panic(fmt.Sprintf("model: a quiet segment touched process %s's memory (§4, M-a)\n%s", (*p).name, debug.Stack()))
		}
		if a := w.s.Running(); a != nil && a.Stale() {
			panic(fmt.Sprintf("model: %s, a unit of work lost to a crash, touched process %s's memory\nschedule: %s\n%s", a.Name, (*p).name, w.s.Trace(), debug.Stack()))
		}
	}
	(*p).ver.onAccess = guard
	(*p).seen.onAccess = guard
	(*p).cache.OnAccess = guard
	// What a read of the process's memory returns is an input of the
	// reading actor (§10.1).
	read := func(v any) {
		if a := w.s.Running(); a != nil {
			a.RecordInput(v)
		}
	}
	(*p).ver.onRead = read
	(*p).seen.onRead = read
	(*p).sched = w.s
	w.procOrder = append(w.procOrder, p)
	return pr
}

// A process that only runs a releaser has no caches at all (releaser with
// cacheless): R0 then never settles a batch, so every batch runs R1 to R3
// against the live state, the hardest case for safety. A stale cache could
// only keep a hold longer, and an informer would multiply the schedules.
//
// informer runs a process's watch: a daemon that, each time it is chosen,
// delivers every event pending, or with oneByOne (CREDENTIALUSE_MODEL_DEEP)
// one event per step. Either way the cache lags the model until the informer
// runs, so every read through it can be stale; one by one also lets a cache
// hold some events of a burst and not others, at a far larger state space.
// The process may be replaced by a restart.
func (w *world) informer(name string, p **process) {
	oneByOne := os.Getenv("CREDENTIALUSE_MODEL_DEEP") != ""
	w.s.GoIn(w.proc(p), name, apimodel.Daemon|apimodel.QuietStart, func(a *apimodel.Actor) {
		for {
			a.Wait("watch", time.Time{}, func() bool { return (*p).cache.Pending() })
			for (*p).cache.Pending() {
				if ch, ok := (*p).cache.Next(); ok {
					(*p).deliver(ch, a.Now())
				}
				if oneByOne {
					break
				}
			}
		}
	})
}

// relister relists a process's caches once, at any point (§6.5 L2).
func (w *world) relister(name string, p **process) {
	w.s.GoIn(w.proc(p), name, apimodel.QuietStart, func(a *apimodel.Actor) {
		a.Wait("relist", time.Time{}, nil)
		(*p).relist(a.Now())
	})
}

// holderSpec is one holder's lifecycle, as one process's runtime runs it.
type holderSpec struct {
	name string
	// actor names the actor, when two processes run the same holder; it
	// defaults to name.
	actor string
	// refs returns what RefsFunc names on each pass, numbered from 0, live
	// passes first; nil means the Secret.
	refs func(pass int) []Ref
	// livePasses run before the holder is deleted; deletionPasses after.
	livePasses, deletionPasses int
	// before, if set, returns what live pass number n waits for, or nil.
	before func(n int) func() bool
}

// holder runs one holder's passes in process p, as §7.2 says a runtime
// does: Acquire, Connect (a read of each held ref), then BeforeEstablish and
// the owner finalizer on a live pass, or Forget with the owner finalizer's
// removal, in one write, on a deletion pass. A crash of p loses the pass in
// flight; the next pass runs in the restarted process.
func (w *world) holder(spec holderSpec, p **process) {
	actor := spec.actor
	if actor == "" {
		actor = spec.name
	}
	if w.holds[spec.name] == nil {
		w.holds[spec.name] = map[ID]hold{}
	}
	holds := w.holds[spec.name]
	w.deletionPhase[actor] = false
	w.s.GoIn(w.proc(p), actor, apimodel.Faulty|apimodel.QuietStart, func(a *apimodel.Actor) {
		ctx := context.Background()
		pass, liveIdx, delIdx := 0, 0, 0
		deletionPhaseLoop := false
		a.Locals = func() []any { return []any{pass, liveIdx, delIdx, deletionPhaseLoop} }
		run := func() (gone bool) {
			a.Restart()
			pr := modelProtocol(modelServer{w.m, a}, cacheViews{p}, (*p).ver, (*p).seen, a.Clock())
			srv := modelServer{w.m, a}
			obj, err := srv.Get(ctx, widgetGK, w.ns, spec.name)
			if !deletionPhaseLoop {
				w.inc(w.started, actor) // the pass began: its first call ran
			}
			if Classify(err) == ClassNotFound {
				w.deletionPhase[actor] = true // gone: nothing left to do
				return true
			}
			if err != nil {
				return false
			}
			deleting := obj.DeletionTimestamp != nil
			if deleting {
				w.deletionPhase[actor] = true
			}
			established := IsEstablished(obj, ownerFinalizers)
			if deleting && !established {
				return false // it goes once nothing holds it; nothing to do
			}
			refs := []Ref{SecretRef(w.ns, w.p.Name)}
			if spec.refs != nil {
				refs = spec.refs(pass)
			}
			pass++
			if !deleting {
				w.countH1(obj, refs, *p)
			}
			acq, err := pr.acquire(ctx, obj, refs)
			if a.Crashed() {
				// The process died under the pass: acquire turned the lost
				// call into an error it fails open on, but nothing of the pass
				// goes on in a dead process (§3.2).
				return false
			}
			if err != nil || acq.MustNotConnect {
				return false
			}
			// A live pass holds each ref it reports held whose record names
			// it now (§14's invariant: the protection lives in the record).
			// The hold lasts while every later version of the holder, by any
			// process, names the ref; namedSince checks that from the
			// model's log, so neither a lost A1 nor another process's write
			// can leave a stale hold (M1 of #59's review).
			if !deleting {
				if live, err := w.m.Get(widgetKey(w.ns, spec.name)); err == nil {
					rec := ReadRecord(live)
					for _, r := range acq.Held {
						id := r.ID()
						if _, ok := holds[id]; ok {
							continue // the earliest unbroken hold stays
						}
						if rec.Names(id) {
							holds[id] = hold{at: w.s.Now()}
						}
					}
				}
			}
			w.checkHeld(spec.name, acq, *p)
			// Connect: read every ref.
			for _, r := range refs {
				_, err := srv.Get(ctx, r.GroupKind, r.Namespace, r.Name)
				if errors.Is(err, apimodel.ErrCrashed) {
					return false
				}
				h, ok := holds[r.ID()]
				w.onRead(spec.name, w.s.Now(), Classify(err) == ClassNotFound,
					deleting && ok && h.at.Before(obj.DeletionTimestamp.Time))
			}
			switch {
			case deleting:
				pr.forget(obj)
				_, err := srv.Patch(ctx, widgetGK, w.ns, spec.name, types.MergePatchType, finalizerPatch(obj, false))
				return err == nil
			case !established:
				if pr.beforeEstablish(ctx, obj, acq) == nil {
					_, _ = srv.Patch(ctx, widgetGK, w.ns, spec.name, types.MergePatchType, finalizerPatch(obj, true))
				}
			}
			return false
		}
		for i := range spec.livePasses {
			liveIdx = i
			if spec.before != nil {
				if c := spec.before(i); c != nil {
					a.WaitQuiet("await", time.Time{}, c)
				}
			}
			gone := run()
			w.inc(w.livePasses, actor) // run, however it ended
			if gone {
				return
			}
		}
		a.WaitQuiet("await deletion", time.Time{}, func() bool {
			o, err := w.m.Get(widgetKey(w.ns, spec.name))
			return err != nil || o.DeletionTimestamp != nil
		})
		deletionPhaseLoop = true
		for i := range spec.deletionPasses {
			delIdx = i + 1
			if run() {
				return
			}
		}
	})
}

// releaser runs up to n release batches for the Secret in process p,
// waiting out each requeue. With cacheless, the protocol has no caches; with
// after, the first batch waits until it holds, as a batch is enqueued by the
// event it waits for.
func (w *world) releaser(name string, p **process, n int, cacheless bool, after func() bool) {
	// A releaser with caches is never quiet: its segment can run a whole R0
	// batch against the cache without a call (§5, C-invisible). A cacheless
	// one's waits are quiet when their condition is stable (C-stable).
	var flags apimodel.Flags
	if cacheless {
		flags = apimodel.QuietStart
	}
	_, stable := any(after).(stableCond)
	w.s.GoIn(w.proc(p), name, flags, func(a *apimodel.Actor) {
		if after != nil {
			if cacheless && stable {
				a.WaitQuiet("await trigger", time.Time{}, after)
			} else {
				a.Wait("await trigger", time.Time{}, after)
			}
		}
		batch := 0
		a.Locals = func() []any { return []any{batch} }
		for i := range n {
			batch = i
			a.Restart()
			var v views = cacheViews{p}
			if cacheless {
				v = noViews{}
			}
			began := false
			srv := firstCall{modelServer{w.m, a}, &began, func() { w.inc(w.batchesStarted, name) }}
			pr := modelProtocol(srv, v, (*p).ver, (*p).seen, a.Clock())
			res, err := pr.release(context.Background(), w.ns, []ID{w.p})
			w.inc(w.batches, name)
			at := w.s.Now()
			for _, d := range res.Decisions {
				for _, r := range res.Released {
					if r == d.P && d.Decision == Release {
						w.onRelease(d, at)
					}
				}
			}
			if errors.Is(err, ErrReleaseRejected) {
				w.fail("%s: %v", name, err)
			}
			if !res.RequeueAt.IsZero() {
				if cacheless {
					a.WaitQuiet("requeue", res.RequeueAt, nil)
				} else {
					a.Wait("requeue", res.RequeueAt, nil)
				}
			}
		}
	})
}

// call is one API call by an actor, retried on an injected fault until it
// lands (a deleter, say, is a person or a controller that tries again). Its
// attempt counter needs no declaration in the state: each attempt is a call
// among the actor's inputs, so the count is part of them.
func (w *world) call(a *apimodel.Actor, label string, op func() error) {
	for range 3 {
		err := a.Call(label, op)
		if errors.Is(err, apimodel.ErrCrashed) {
			a.Restart()
			continue
		}
		if err == nil || errors.Is(err, apimodel.ErrLost) || Classify(err) == ClassNotFound {
			return
		}
	}
}

// deleter deletes keys in order, each when it is chosen; nsFirst also
// records when the namespace got its deletionTimestamp.
func (w *world) deleter(name string, keys ...apimodel.Key) {
	w.s.Go(name, apimodel.QuietStart, func(a *apimodel.Actor) {
		idx := 0
		a.Locals = func() []any { return []any{idx} }
		for i, k := range keys {
			idx = i
			w.call(a, "delete "+k.Name, func() error {
				err := w.m.Delete(k)
				if err == nil && k.GK == apimodel.NamespaceKind && w.nsDeleted.IsZero() {
					w.nsDeleted = a.Now()
				}
				return err
			})
		}
	})
}

// countH1 counts a live pass that starts in needsH1's state, as
// instrumentation, outside the schedule. The cache must hold the same
// incarnation of P as the model: a different or deleted one is §6.5's L6
// window, which checkHeld counts apart.
func (w *world) countH1(obj metav1.Object, refs []Ref, p *process) {
	f, _ := FinalizerName(widgetGK, w.ns)
	rec := ReadRecord(obj)
	for _, r := range refs {
		k := keyOf(r.ID())
		if !rec.Names(r.ID()) {
			continue // this pass's A1 adds it, and never skips it
		}
		live, err := w.m.Get(k)
		if err != nil || hasFinalizer(live, f) {
			continue // gone (L6's territory, not H1's), or held
		}
		cached, ok := peekCache(p, k)
		if ok && cached.UID == live.UID && hasFinalizer(cached, f) && peekWritten(p, cached.UID, w.ns) {
			w.h1++
		}
	}
}

// checkHeld runs right after an Acquire, with no step in between: every ref
// it reports held must carry the finalizer now. The one exception is §6.5's
// L6 window, a recreate behind a cached UID: the pass skipped A2 on its
// process's written entry for an incarnation that is gone, because the
// process hasn't been delivered its deletion yet. That window must close on
// delivery, so outside it a held ref without the finalizer is a violation.
func (w *world) checkHeld(holder string, acq Acquired, p *process) {
	f, _ := FinalizerName(widgetGK, w.ns)
	for _, r := range acq.Held {
		k := keyOf(r.ID())
		live, err := w.m.Get(k)
		if err == nil && hasFinalizer(live, f) {
			continue
		}
		cached, ok := peekCache(p, k)
		if ok && (err != nil || cached.UID != live.UID) && hasFinalizer(cached, f) && peekWritten(p, cached.UID, w.ns) {
			w.l6++
			continue
		}
		w.fail("SAFETY: %s's pass reported %s held, and it isn't, with no older incarnation in the cache", holder, r.ID())
	}
}

// check is what remains for the end of a schedule: the monitors have
// checked safety on the fly, so here every holder must have run its
// deletion phase, and convergence must hold.
func (w *world) check() error {
	for actor, ran := range w.deletionPhase {
		if !ran {
			w.fail("DELETION PHASE: %s never ran a pass on its deleting holder", actor)
		}
	}
	if err := w.converge(); err != nil {
		w.fail("CONVERGENCE: %v", err)
	}
	return errors.Join(w.errs...)
}

// converge runs release batches with no caches and the grace long past,
// until nothing changes; then, with no user left, the finalizer must be gone.
func (w *world) converge() error {
	later := clockAt(w.s.Clock.Now().Add(3 * testGrace))
	pr := modelProtocol(modelServer{m: w.m}, noViews{}, newVerified(widgetGK), newFirstSight(), later)
	for range 3 {
		if _, err := pr.release(context.Background(), w.ns, []ID{w.p}); err != nil {
			return err
		}
	}
	users := 0
	for _, h := range w.m.List(widgetGK, w.ns) {
		if IsUser(&h, w.p, ownerFinalizers) {
			users++
		}
	}
	o, err := w.m.Get(keyOf(w.p))
	if err != nil {
		return nil // gone
	}
	f, _ := FinalizerName(widgetGK, w.ns)
	if users == 0 && hasFinalizer(o, f) {
		return fmt.Errorf("no user of %s is left, and it still carries %s", w.p, f)
	}
	return nil
}

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time                  { return c.t }
func (c fixedClock) Since(t time.Time) time.Duration { return c.t.Sub(t) }

func clockAt(t time.Time) clock.PassiveClock { return fixedClock{t} }

// isU5 says whether a release was over a live user, after the grace: §5's
// last row.
func isU5(in ReleaseInput) bool {
	return in.Deleting && !in.NamespaceDeleting && in.GraceOver && in.DeletingUsers == 0 && in.Users > 0
}

// explore runs scenario under every profile, and checks each run. A scenario
// in which no schedule ever releases anything, or no deleting holder ever
// reads, tests nothing: that is a failure too, and so, in an exhaustive
// search, is missing a state flags say it must reach.
func explore(t *testing.T, name string, flags scenarioFlags, scenario func(w *world)) {
	t.Helper()
	var releasing, deletingReads, u5, l6, h1 int
	// A stateful search (docs/credential-use-model-reduction.md §10) counts
	// distinct abstract states, per reach flag, not schedules.
	// It is the default for the exhaustive searches; CREDENTIALUSE_MODEL_STATELESS
	// turns it off, for comparison.
	stateful := os.Getenv("CREDENTIALUSE_MODEL_STATELESS") == ""
	var flagged [5]map[string]struct{}
	run := func(s *apimodel.Sched) error {
		w := newWorld(s)
		scenario(w)
		if stateful {
			s.Key = func() string {
				k := w.stateKey()
				for i, f := range []bool{w.releasedAny, w.u5Any, w.l6 > 0, w.h1 > 0, w.deletingRead} {
					if f && flagged[i] != nil {
						flagged[i][k] = struct{}{}
					}
				}
				return k
			}
		}
		if err := s.Run(); err != nil {
			if errors.Is(err, apimodel.ErrPruned) {
				// The violations found on the way stand; the rest of the
				// run was searched from the visited state.
				return errors.Join(w.errs...)
			}
			return err
		}
		if w.onDone != nil {
			w.onDone()
		}
		if w.releasedAny {
			releasing++
		}
		if w.u5Any {
			u5++
		}
		if w.l6 > 0 {
			l6++
		}
		if w.h1 > 0 {
			h1++
		}
		if w.deletingRead {
			deletingReads++
		}
		return w.check()
	}
	exhaustive, u5Total, h1Total := 0, 0, 0
	for _, p := range profiles(t, flags) {
		releasing, deletingReads, u5, l6, h1 = 0, 0, 0, 0, 0
		start := time.Now()
		var res apimodel.Result
		switch {
		case p.walk > 0:
			seed := walkSeed()
			t.Logf("%s, %s: seed %d (CREDENTIALUSE_MODEL_SEED=%d replays it)", name, p.name, seed, seed)
			res = apimodel.Walk(p.bounds, p.walk, seed, run)
		case stateful:
			for i := range flagged {
				flagged[i] = map[string]struct{}{}
			}
			// The sampled merge audit (§10.5): every Nth prune, from a seeded
			// offset, replays the pruned path and its witness to the end
			// under the same continuations.
			every, seed := auditEvery(), walkSeed()
			offset := int((uint64(seed) * 0x9E3779B97F4A7C15 >> 32) % uint64(every))
			prunes, audits, divergences := 0, 0, 0
			audit := func(pruned, witness apimodel.Witness) {
				prunes++
				if (prunes-1)%every != offset {
					return
				}
				audits++
				if msg := auditPrune(p.bounds, scenario, pruned, witness, seed, defaultQuiet()); msg != "" {
					divergences++
					if divergences <= 3 {
						t.Errorf("%s, %s: merge audit: %s\npruned: %v\nwitness: %v", name, p.name, msg, pruned.Choices, witness.Choices)
					}
				}
			}
			res = apimodel.ExploreAudited(p.bounds, audit, run)
			t.Logf("%s, %s: merge audit: every %d prunes from offset %d (seed %d): %d audits, %d divergences",
				name, p.name, every, offset, seed, audits, divergences)
			releasing, u5, l6, h1, deletingReads = len(flagged[0]), len(flagged[1]), len(flagged[2]), len(flagged[3]), len(flagged[4])
			for i := range flagged {
				flagged[i] = nil
			}
			t.Logf("%s, %s: stateful: %d distinct states, %d runs, %d pruned; the counts below are distinct states", name, p.name, res.States, res.Runs, res.Pruned)
		default:
			res = apimodel.Explore(p.bounds, run)
		}
		report(t, name+", "+p.name, res)
		t.Logf("%s, %s: %s; %d schedules released, %d released over a live user (U5), %d reached L6, %d reached H1's state, %d had a deleting holder read what it held",
			name, p.name, time.Since(start).Round(time.Millisecond), releasing, u5, l6, h1, deletingReads)
		if releasing == 0 || deletingReads == 0 {
			t.Errorf("%s, %s: vacuous: %d schedules released, %d had a deleting holder read what it held", name, p.name, releasing, deletingReads)
		}
		if p.walk == 0 {
			exhaustive++
			u5Total += u5
			h1Total += h1
		}
		if p.walk == 0 && flags&needsL6 != 0 && l6 == 0 {
			t.Errorf("%s, %s: vacuous: no schedule reached the L6 window", name, p.name)
		}
	}
	if exhaustive > 0 && flags&needsH1 != 0 && h1Total == 0 {
		t.Errorf("%s: vacuous: no exhaustively searched schedule started a live pass in H1's state", name)
	}
	if exhaustive > 0 && flags&needsU5 != 0 && u5Total == 0 {
		t.Errorf("%s: vacuous: no exhaustively searched schedule released over a live user (U5)", name)
	}
}

func report(t *testing.T, name string, res apimodel.Result) {
	t.Helper()
	t.Logf("%s: %d schedules, finished=%v", name, res.Runs, res.Finished)
	t.Logf("%s: capacity used (§10.2): at most %d calls between jumps (K = %d), %d jumps chosen or forced, smallest tick %s",
		name, res.Usage.MaxCallsBetweenJumps, apimodel.MaxCallsBetweenJumps, res.Usage.MaxJumps, res.Usage.MinTick)
	for _, f := range res.Failures {
		t.Errorf("%s: %v\nschedule: %s", name, f.Err, f.Trace)
	}
	if !res.Finished {
		t.Errorf("%s: the search didn't finish within %d schedules", name, res.Runs)
	}
	if res.Runs < 2 {
		t.Errorf("%s: only %d schedule ran", name, res.Runs)
	}
}

// The krateo-057 teardown (§1, §14): the Secret is deleted first, then the
// holders, in a namespace being deleted, with the release loop running.
// Every holder must finish its deletion with the Secret readable.
func TestModelTeardown(t *testing.T) {
	for name, order := range map[string][]string{
		"krateo-057: namespace, Secret, holder":           {"ns", "secret", "holder"},
		"interleaving B: Secret, namespace, holder":       {"secret", "ns", "holder"},
		"interleaving A: Secret, holder (namespace live)": {"secret", "holder"},
	} {
		var flags scenarioFlags
		if len(order) == 2 {
			flags = needsU5 // the namespace stays live
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			explore(t, name, flags, func(w *world) {
				seed(w.m, w.ns, w.p.Name, []string{"h1"}, true)
				pa := newProcess("A", w.m, w.s.Clock.Now())
				w.informer("informerA", &pa)
				w.holder(holderSpec{name: "h1", livePasses: 1, deletionPasses: 3}, &pa)
				w.releaser("releaserA", &pa, 3, false, nil)
				var keys []apimodel.Key
				for _, o := range order {
					switch o {
					case "ns":
						keys = append(keys, nsKey(w.ns))
					case "secret":
						keys = append(keys, keyOf(w.p))
					case "holder":
						keys = append(keys, widgetKey(w.ns, "h1"))
					}
				}
				w.deleter("teardown", keys...)
			})
		})
	}
}

// A new holder acquires while a releaser in another process removes the
// finalizer its former holder left (§6.4's three cases, §6.5), with an error,
// a lost response or a crash on any step (HIGH-1), and a restart that clears
// the verified set.
func TestModelAcquireAgainstRelease(t *testing.T) {
	t.Parallel()
	explore(t, "acquire against release", bothBounds, func(w *world) {
		seed(w.m, w.ns, w.p.Name, nil, true) // held by a former holder, now gone
		must(w.m.Create(widgetKey(w.ns, "h2"), metav1.ObjectMeta{}))
		pa := newProcess("A", w.m, w.s.Clock.Now())
		pb := newProcess("B", w.m, w.s.Clock.Now())
		w.informer("informerA", &pa)
		w.holder(holderSpec{name: "h2", livePasses: 2, deletionPasses: 3}, &pa)
		w.releaser("releaserB", &pb, 2, true, nil)
		w.deleter("teardown", keyOf(w.p), widgetKey(w.ns, "h2"))
	})
}

// Two releasers in two processes are serialized by R3's tests (§6.5, §6.8),
// while a holder of P is deleted.
func TestModelTwoReleasers(t *testing.T) {
	t.Parallel()
	explore(t, "two releasers", 0, func(w *world) {
		seed(w.m, w.ns, w.p.Name, []string{"h1"}, true)
		pa := newProcess("A", w.m, w.s.Clock.Now())
		pb := newProcess("B", w.m, w.s.Clock.Now())
		pc := newProcess("C", w.m, w.s.Clock.Now())
		w.informer("informerA", &pa)
		w.holder(holderSpec{name: "h1", livePasses: 1, deletionPasses: 2}, &pa)
		w.releaser("releaserB", &pb, 1, true, nil)
		w.releaser("releaserC", &pc, 1, true, nil)
		w.deleter("teardown", keyOf(w.p), widgetKey(w.ns, "h1"))
	})
}

// A ref goes away and comes back (M1): the pass whose A1 re-adds it must
// write A2, even with a written entry from the earlier run, and even when
// the informer never delivers the version in between. A relist of the
// holder informer at any point (L2) must not let a pass skip on a stale
// recordSince. With four live passes, a pass after the re-add runs no A1,
// and must not skip on the first run (H1 of #59's review).
func TestModelAwayAndBack(t *testing.T) {
	for _, live := range []int{3, 4} {
		t.Run(fmt.Sprintf("%d live passes", live), func(t *testing.T) {
			t.Parallel()
			explore(t, fmt.Sprintf("away and back, %d live passes", live), bothBounds|needsU5, func(w *world) {
				seed(w.m, w.ns, w.p.Name, []string{"h1"}, true)
				pa := newProcess("A", w.m, w.s.Clock.Now())
				pb := newProcess("B", w.m, w.s.Clock.Now())
				w.informer("informerA", &pa)
				if live == 3 {
					w.relister("relistA", &pa)
				}
				secret := SecretRef(w.ns, w.p.Name)
				w.holder(holderSpec{
					name: "h1",
					refs: func(pass int) []Ref {
						if pass == 1 {
							return nil // away
						}
						return []Ref{secret} // and back
					},
					livePasses: live, deletionPasses: 2,
				}, &pa)
				w.releaser("releaserB", &pb, 2, true, nil)
				w.deleter("teardown", keyOf(w.p), widgetKey(w.ns, "h1"))
			})
		})
	}
}

// Two processes run one holder (a leader handover, or two replicas without
// leader election, §6.8), each with its own verified set: H1(b) of #59's
// review. A holds the ref, and its informer runs. B takes the ref away; a
// releaser releases P, which then has no user; B puts the ref back, with any
// fault on its calls, and B's informer never delivers past its first list.
// A's next pass must not skip on what its informer delivered of the first
// run. Every exhaustive search must start a pass of A in that state
// (needsH1); then the Secret and the holder are deleted.
//
// Its exhaustive searches run only in deep mode (deepOnly): with one fault
// it has 2,625,480 schedules, all safe, 41,328 of them in H1's state; with
// one preemption, and with one of each, it didn't finish within 3 and 6
// million, with no failure. By default it runs random walks, which reach
// H1's state rarely if at all (none in 6,000 here), so H1(b) is covered by
// review_test.go's deterministic reproductions.
//
// The free-running form of this scenario (no waits, a second releaser) did
// not finish one fault's search in an hour here, so four waits order its
// phases. Each waits for a step to begin, not to end, so a preemption can
// still put the steps on either side anywhere inside it. What they rule out:
//
//  1. B's away pass begins only once A's first live pass has begun. Ruled
//     out: B's removal before A's first pass begins. A then reads
//     a record without P, and its own A1 adds P back, a ref that never
//     skips in that pass: the single-process away and back
//     (TestModelAwayAndBack), with no cross-process observation for A to
//     lean on.
//  2. The releaser's batch begins only once B's away pass has ended. B's
//     away pass makes one call, its A1, so what is ruled out is a batch
//     whose R1 reads P before that A1 lands. Its R2 list comes after R1:
//     before the A1 it sees B's record naming P and keeps P, which leaves no
//     state H1 is about; after it, the batch releases from the same P that
//     a later R1 would read, unless an A2 lands in between, which changes
//     the epoch, fails R3, and sends the batch back to R1, the ordering the
//     wait allows.
//  3. B's re-add pass begins only once the releaser's batch has begun.
//     Ruled out: B's re-add before the batch's R1. Its R2 then sees the
//     record naming P and keeps P: no finalizer is removed, so no state H1
//     is about. A re-add between R1 and R3 is in the search.
//  4. A's second live pass begins only once B's re-add pass has begun.
//     Ruled out: A's second pass beginning before B's re-add pass does. It
//     then reads B's away version, which doesn't name P, so its A1 adds P,
//     a ref that never skips; that is case 1 with the roles swapped.
//
// The teardown waits for both holders' live passes to end: deletion passes
// never skip (§6.3), so this rules out no skip, and the deletions of a live
// phase are the other scenarios'.
func TestModelTwoProcessesAcquireOneHolder(t *testing.T) {
	t.Parallel()
	explore(t, "two processes acquire one holder", bothBounds|needsH1|deepOnly, func(w *world) {
		seed(w.m, w.ns, w.p.Name, []string{"h1"}, true)
		pa := newProcess("A", w.m, w.s.Clock.Now())
		pb := newProcess("B", w.m, w.s.Clock.Now())
		pc := newProcess("C", w.m, w.s.Clock.Now())
		w.informer("informerA", &pa)
		secret := SecretRef(w.ns, w.p.Name)
		w.holder(holderSpec{name: "h1", actor: "h1@A", livePasses: 2, deletionPasses: 2, before: func(n int) func() bool {
			if n != 1 {
				return nil
			}
			return func() bool { return w.started["h1@B"] >= 2 } // (4)
		}}, &pa)
		w.holder(holderSpec{
			name: "h1", actor: "h1@B",
			refs: func(pass int) []Ref {
				if pass == 0 {
					return nil // away
				}
				return []Ref{secret} // and back
			},
			livePasses: 2, deletionPasses: 2,
			before: func(n int) func() bool {
				switch n {
				case 0:
					return func() bool { return w.started["h1@A"] >= 1 } // (1)
				case 1:
					return func() bool { return w.batchesStarted["releaserC"] >= 1 } // (3)
				}
				return nil
			},
		}, &pb)
		w.releaser("releaserC", &pc, 1, true, stableCond(func() bool { return w.livePasses["h1@B"] >= 1 })) // (2)
		w.s.Go("teardown", apimodel.QuietStart, func(a *apimodel.Actor) {
			a.WaitQuiet("live passes done", time.Time{}, func() bool {
				return w.livePasses["h1@A"] >= 2 && w.livePasses["h1@B"] >= 2
			})
			w.call(a, "delete secret", func() error { return w.m.Delete(keyOf(w.p)) })
			w.call(a, "delete h1", func() error { return w.m.Delete(widgetKey(w.ns, "h1")) })
		})
	})
}

// A holder and the namespace are deleted during the grace, just after it,
// and between R2 and R3 (§6.4): the schedules cover every position of the
// two deletions relative to a release batch that runs after the Secret's
// deletion.
func TestModelDeletionsAroundTheGrace(t *testing.T) {
	t.Parallel()
	explore(t, "deletions around the grace", 0, func(w *world) {
		seed(w.m, w.ns, w.p.Name, []string{"h1"}, true)
		pa := newProcess("A", w.m, w.s.Clock.Now())
		w.informer("informerA", &pa)
		w.holder(holderSpec{name: "h1", livePasses: 1, deletionPasses: 3}, &pa)
		w.releaser("releaserA", &pa, 3, false, nil)
		w.deleter("secret", keyOf(w.p))
		w.s.Go("late deletions", apimodel.QuietStart, func(a *apimodel.Actor) {
			// Wait into the grace, then delete the holder and the namespace.
			// "Into the grace" is half of it after the Secret's deletion: a
			// deadline (§3.1), reached by a jump, or after the grace by a
			// jump to its end first.
			var dt time.Time
			a.Locals = func() []any { return []any{dt} }
			a.Wait("secret deleting", time.Time{}, func() bool {
				o, err := w.m.Get(keyOf(w.p))
				if err == nil && o.DeletionTimestamp != nil {
					dt = o.DeletionTimestamp.Time
				}
				return err != nil || o.DeletionTimestamp != nil
			})
			if dt.IsZero() {
				dt = a.Now()
			}
			a.WaitQuiet("into the grace", dt.Add(testGrace/2), nil)
			w.call(a, "delete ns", func() error {
				err := w.m.Delete(nsKey(w.ns))
				if err == nil {
					w.nsDeleted = a.Now()
				}
				return err
			})
			w.call(a, "delete h1", func() error { return w.m.Delete(widgetKey(w.ns, "h1")) })
		})
	})
}

// A Secret released and recreated under the same name while a holder's
// process still caches the old incarnation (§6.5, L6): the only window in
// which a pass can report a ref held that isn't, closed by the deletion's
// delivery. checkHeld asserts both on every pass.
func TestModelRecreateBehindACachedUID(t *testing.T) {
	t.Parallel()
	windows := 0
	explore(t, "recreate behind a cached UID", needsU5|needsL6, func(w *world) {
		seed(w.m, w.ns, w.p.Name, []string{"h1"}, true)
		pa := newProcess("A", w.m, w.s.Clock.Now())
		pb := newProcess("B", w.m, w.s.Clock.Now())
		w.informer("informerA", &pa)
		// The first pass writes, so that the process has a written entry; the
		// last live pass runs once the Secret has been recreated, whether or
		// not the process has been delivered the old one's deletion.
		var first types.UID
		if o, err := w.m.Get(keyOf(w.p)); err == nil {
			first = o.UID
			w.pinned = append(w.pinned, first)
		}
		w.holder(holderSpec{name: "h1", livePasses: 3, deletionPasses: 2, before: func(n int) func() bool {
			if n != 2 {
				return nil
			}
			return func() bool { o, err := w.m.Get(keyOf(w.p)); return err == nil && o.UID != first }
		}}, &pa)
		// The Secret's deletion enqueues the batch that releases it after the
		// grace (§5's last row: a live namespace, a live user).
		w.releaser("releaserB", &pb, 3, true, func() bool {
			o, err := w.m.Get(keyOf(w.p))
			return err != nil || o.DeletionTimestamp != nil
		})
		w.s.Go("rotation", apimodel.QuietStart, func(a *apimodel.Actor) {
			w.call(a, "delete secret", func() error { return w.m.Delete(keyOf(w.p)) })
			// Stable while waited on: only this actor recreates the Secret.
			a.WaitQuiet("gone", time.Time{}, func() bool { _, err := w.m.Get(keyOf(w.p)); return err != nil })
			w.call(a, "recreate secret", func() error { return w.m.Create(keyOf(w.p), metav1.ObjectMeta{}) })
			// The holder is deleted after its last live pass.
			a.WaitQuiet("last live pass", time.Time{}, func() bool { return w.livePasses["h1"] >= 3 })
			w.call(a, "delete h1", func() error { return w.m.Delete(widgetKey(w.ns, "h1")) })
		})
		w.onDone = func() { windows += w.l6 }
	})
	t.Logf("passes inside the L6 window: %d", windows)
}

func TestModelBoundsAreExhausted(t *testing.T) {
	// A sanity check of the enumerator itself: two actors with one call
	// each, no preemption, no faults, give exactly the two orders.
	res := apimodel.Explore(apimodel.Bounds{}, func(s *apimodel.Sched) error {
		for _, n := range []string{"a", "b"} {
			s.Go(n, apimodel.Faulty, func(a *apimodel.Actor) {
				_ = a.Call(n, func() error { return nil })
			})
		}
		return s.Run()
	})
	if !res.Finished || res.Runs != 2 {
		t.Errorf("two actors, one call each: %d schedules, finished=%v; want 2, true", res.Runs, res.Finished)
	}
}

// peekCache and peekWritten read a process's memory as instrumentation:
// outside the access hooks, which guard what the protocol's own units touch
// (docs/credential-use-model-reduction.md §3.2), and outside any actor's
// inputs.
func peekCache(p *process, k apimodel.Key) (*metav1.PartialObjectMetadata, bool) {
	objs, _ := p.cache.Objects()
	for _, ev := range objs {
		if ev.Key == k {
			o := ev.Obj
			return &o, true
		}
	}
	return nil, false
}

func peekWritten(p *process, uid types.UID, ns string) bool {
	_, ok := p.ver.written[uid][ns]
	return ok
}

// inc counts in one of the scenario's counters, which Wait conditions read:
// never from a quiet segment (§4, M-a).
func (w *world) inc(m map[string]int, k string) {
	if w.s.InQuiet() {
		panic("model: a quiet segment wrote a scenario counter (§4, M-a)")
	}
	m[k]++
}

// stableCond marks a releaser's trigger whose condition stays true once true
// (§5, C-stable), so that a cacheless releaser's wait for it can be quiet.
type stableCond func() bool
