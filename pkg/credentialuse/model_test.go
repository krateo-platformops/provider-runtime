package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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

const testGrace = 5 * time.Millisecond

// A profile is one search of a scenario's schedules: exhaustive within
// bounds, or a seeded random walk of n schedules.
type profile struct {
	name   string
	bounds apimodel.Bounds
	walk   int
}

// profiles are the searches every scenario runs.
//
//   - By default: every schedule with no preemption and one fault on the
//     holder's calls, every schedule with one preemption and no fault, and
//     400 random schedules with two preemptions and one fault. CI runs these
//     in a step of their own, without the race detector.
//   - Under the race detector, which slows each schedule several times and
//     finds nothing in the model (one coroutine runs at a time), random walks
//     only: 300 with one fault, 300 with two preemptions and one fault.
//   - CREDENTIALUSE_MODEL_DEEP=1: every schedule with one preemption and one
//     fault, and with two preemptions and none, and a long walk with three
//     and two, with informers delivering one event per step.
//   - CREDENTIALUSE_MODEL_BOUNDS=p,f,n[;p,f,n...]: exhaustive searches with
//     those preemptions, faults and run caps, for measuring and for the
//     revert check.
func profiles(t *testing.T) []profile {
	if v := os.Getenv("CREDENTIALUSE_MODEL_BOUNDS"); v != "" {
		var out []profile
		for _, part := range strings.Split(v, ";") {
			var b apimodel.Bounds
			if _, err := fmt.Sscanf(part, "%d,%d,%d", &b.Preemptions, &b.Faults, &b.MaxRuns); err != nil {
				t.Fatalf("CREDENTIALUSE_MODEL_BOUNDS=%q: %v", v, err)
			}
			out = append(out, profile{name: fmt.Sprintf("%d preemptions, %d faults", b.Preemptions, b.Faults), bounds: b})
		}
		return out
	}
	if os.Getenv("CREDENTIALUSE_MODEL_DEEP") != "" {
		return []profile{
			{name: "1 preemption, 1 fault", bounds: apimodel.Bounds{Preemptions: 1, Faults: 1, MaxRuns: 5_000_000}},
			{name: "2 preemptions", bounds: apimodel.Bounds{Preemptions: 2, MaxRuns: 5_000_000}},
			{name: "random: 3 preemptions, 2 faults", bounds: apimodel.Bounds{Preemptions: 3, Faults: 2}, walk: 50_000},
		}
	}
	if raceEnabled {
		return []profile{
			{name: "random: 1 fault", bounds: apimodel.Bounds{Faults: 1}, walk: 300},
			{name: "random: 2 preemptions, 1 fault", bounds: apimodel.Bounds{Preemptions: 2, Faults: 1}, walk: 300},
		}
	}
	return []profile{
		{name: "1 fault", bounds: apimodel.Bounds{Faults: 1, MaxRuns: 200_000}},
		{name: "1 preemption", bounds: apimodel.Bounds{Preemptions: 1, MaxRuns: 200_000}},
		{name: "random: 2 preemptions, 1 fault", bounds: apimodel.Bounds{Preemptions: 2, Faults: 1}, walk: 400},
	}
}

// world is one schedule's model, processes and record of what happened.
type world struct {
	s  *apimodel.Sched
	m  *apimodel.Model
	ns string
	p  ID // the protected Secret

	reads     []connectRead
	released  []timedDecision
	nsDeleted time.Time
	errs      []error
	// l6 counts passes inside §6.5's L6 window.
	l6 int
	// onDone, if set, runs after the schedule, before the checks.
	onDone func()
	// livePasses counts each holder's live passes that ran, however they
	// ended.
	livePasses map[string]int
}

// connectRead is a holder's Connect reading P.
type connectRead struct {
	holder   string
	at       time.Time
	notFound bool
	// held: an earlier pass of this holder held P before its deletion
	// began, and this pass is a deletion pass with the owner finalizer.
	held bool
}

type timedDecision struct {
	ReleaseDecision
	at time.Time
}

func newWorld(s *apimodel.Sched) *world {
	w := &world{s: s, ns: "rlgov-probe", livePasses: map[string]int{}}
	w.m = apimodel.New(s.Clock.Now)
	w.p = SecretRef(w.ns, "probe-dummy-creds").ID()
	return w
}

func (w *world) fail(format string, args ...any) {
	w.errs = append(w.errs, fmt.Errorf(format, args...))
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
	w.s.Go(name, apimodel.Daemon, func(a *apimodel.Actor) {
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
	w.s.Go(name, 0, func(a *apimodel.Actor) {
		a.Wait("relist", time.Time{}, nil)
		(*p).relist(a.Now())
	})
}

// holderSpec is one holder's lifecycle.
type holderSpec struct {
	name string
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
// removal, in one write, on a deletion pass. A crash restarts the process.
func (w *world) holder(spec holderSpec, p **process) {
	w.s.Go(spec.name, apimodel.Faulty, func(a *apimodel.Actor) {
		ctx := context.Background()
		heldAt := map[ID]time.Time{}
		pass := 0
		run := func(deletionPhase bool) (gone bool) {
			if a.Crashed() {
				*p = newProcess((*p).name, w.m, a.Now())
				a.Restart()
			}
			pr := newProtocol(modelServer{w.m, a}, cacheViews{p}, (*p).ver, (*p).seen, a.Clock(), testGrace)
			srv := modelServer{w.m, a}
			obj, err := srv.Get(ctx, widgetGK, w.ns, spec.name)
			if Classify(err) == ClassNotFound {
				return true
			}
			if err != nil {
				return false
			}
			deleting := obj.DeletionTimestamp != nil
			established := IsEstablished(obj, ownerFinalizers)
			if deleting && !established {
				return false // it goes once nothing holds it; nothing to do
			}
			refs := []Ref{SecretRef(w.ns, w.p.Name)}
			if spec.refs != nil {
				refs = spec.refs(pass)
			}
			pass++
			acq, err := pr.acquire(ctx, obj, refs)
			if err != nil || acq.MustNotConnect {
				return false
			}
			// heldAt is when a live pass held each ref, for as long as the
			// holder's record has named it since: the protection lives in
			// the record, so a pass that drops a ref from it forgets the hold,
			// and one that keeps it (a Degraded pass, say) doesn't (§14's
			// invariant).
			rec := ReadRecord(obj)
			for id := range heldAt {
				if !rec.Names(id) {
					delete(heldAt, id)
				}
			}
			if !deleting {
				for _, r := range acq.Held {
					if _, ok := heldAt[r.ID()]; !ok {
						heldAt[r.ID()] = a.Now()
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
				at, wasHeld := heldAt[r.ID()]
				w.reads = append(w.reads, connectRead{
					holder:   spec.name,
					at:       a.Now(),
					notFound: Classify(err) == ClassNotFound,
					held:     deleting && wasHeld && at.Before(obj.DeletionTimestamp.Time),
				})
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
			if spec.before != nil {
				if c := spec.before(i); c != nil {
					a.Wait("await", time.Time{}, c)
				}
			}
			gone := run(false)
			w.livePasses[spec.name]++ // run, however it ended
			if gone {
				return
			}
		}
		a.Wait("await deletion", time.Time{}, func() bool {
			o, err := w.m.Get(widgetKey(w.ns, spec.name))
			return err != nil || o.DeletionTimestamp != nil
		})
		for range spec.deletionPasses {
			if run(true) {
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
	w.s.Go(name, 0, func(a *apimodel.Actor) {
		if after != nil {
			a.Wait("await trigger", time.Time{}, after)
		}
		for range n {
			if a.Crashed() {
				*p = newProcess((*p).name, w.m, a.Now())
				a.Restart()
			}
			var v views = cacheViews{p}
			if cacheless {
				v = noViews{}
			}
			pr := newProtocol(modelServer{w.m, a}, v, (*p).ver, (*p).seen, a.Clock(), testGrace)
			res, err := pr.release(context.Background(), w.ns, []ID{w.p})
			at := a.Now()
			for _, d := range res.Decisions {
				for _, r := range res.Released {
					if r == d.P && d.Decision == Release {
						w.released = append(w.released, timedDecision{d, at})
					}
				}
			}
			if errors.Is(err, ErrReleaseRejected) {
				w.fail("%s: %v", name, err)
			}
			if res.RequeueAfter > 0 {
				a.Wait("requeue", at.Add(res.RequeueAfter), nil)
			}
		}
	})
}

// call is one API call by an actor, retried on an injected fault until it
// lands (a deleter, say, is a person or a controller that tries again).
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
	w.s.Go(name, 0, func(a *apimodel.Actor) {
		for _, k := range keys {
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

// checkHeld runs right after an Acquire, with no step in between: every ref
// it reports held must carry the finalizer now. The one exception is §6.5's
// L6 window, a recreate behind a cached UID: the pass skipped A2 on a written
// entry for an incarnation that is gone, because the process hasn't been
// delivered its deletion yet. That window must close on delivery, so outside
// it a held ref without the finalizer is a violation.
func (w *world) checkHeld(holder string, acq Acquired, p *process) {
	f, _ := FinalizerName(widgetGK, w.ns)
	for _, r := range acq.Held {
		k := keyOf(r.ID())
		live, err := w.m.Get(k)
		if err == nil && hasFinalizer(live, f) {
			continue
		}
		cached, ok := p.cache.Get(k)
		inWindow := ok && (err != nil || cached.UID != live.UID)
		if inWindow {
			w.l6++
			continue
		}
		w.fail("SAFETY: %s's pass reported %s held, and it isn't, with no older incarnation in the cache", holder, r.ID())
	}
}

// check is the safety invariant, then convergence.
func (w *world) check() error {
	for _, r := range w.reads {
		if !r.notFound || !r.held {
			continue
		}
		var allowed *timedDecision
		for i, d := range w.released {
			in := d.Input
			if d.at.After(r.at) {
				continue
			}
			if in.Deleting && !in.NamespaceDeleting && in.GraceOver && in.DeletingUsers == 0 {
				allowed = &w.released[i]
			}
		}
		if allowed == nil {
			w.fail("SAFETY: deleting holder %s read %s as NotFound at %s, after holding it, with no U5 release before (releases: %+v)",
				r.holder, w.p, r.at.Format("05.000"), w.released)
			continue
		}
		// U5's release read the Namespace live; a deletionTimestamp it got
		// must have come after that read (§6.4, L3).
		if !w.nsDeleted.IsZero() && !w.nsDeleted.After(allowed.NamespaceReadAt) && !allowed.NamespaceReadAt.IsZero() {
			w.fail("SAFETY: a release read namespace %s as live at %s, after its deletion at %s",
				w.ns, allowed.NamespaceReadAt.Format("05.000"), w.nsDeleted.Format("05.000"))
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
	later := clockAt(w.s.Clock.Now().Add(time.Hour))
	pr := newProtocol(modelServer{m: w.m}, noViews{}, newVerified(widgetGK), newFirstSight(), later, testGrace)
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

// explore runs scenario under every profile, and checks each run. A scenario
// in which no schedule ever releases anything, or no deleting holder ever
// reads, tests nothing: that is a failure too.
func explore(t *testing.T, name string, scenario func(w *world)) {
	t.Helper()
	var releasing, deletingReads int
	run := func(s *apimodel.Sched) error {
		w := newWorld(s)
		scenario(w)
		if err := s.Run(); err != nil {
			return err
		}
		if w.onDone != nil {
			w.onDone()
		}
		if len(w.released) > 0 {
			releasing++
		}
		for _, r := range w.reads {
			if r.held {
				deletingReads++
				break
			}
		}
		return w.check()
	}
	for _, p := range profiles(t) {
		releasing, deletingReads = 0, 0
		var res apimodel.Result
		if p.walk > 0 {
			res = apimodel.Walk(p.bounds, p.walk, 1, run)
		} else {
			res = apimodel.Explore(p.bounds, run)
		}
		report(t, name+", "+p.name, res)
		t.Logf("%s, %s: %d schedules released, %d had a deleting holder read what it held", name, p.name, releasing, deletingReads)
		if releasing == 0 || deletingReads == 0 {
			t.Errorf("%s, %s: vacuous: %d schedules released, %d had a deleting holder read what it held", name, p.name, releasing, deletingReads)
		}
	}
}

func report(t *testing.T, name string, res apimodel.Result) {
	t.Helper()
	t.Logf("%s: %d schedules, finished=%v", name, res.Runs, res.Finished)
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
		t.Run(name, func(t *testing.T) {
			explore(t, name, func(w *world) {
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
	explore(t, "acquire against release", func(w *world) {
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
	explore(t, "two releasers", func(w *world) {
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
// recordSince.
func TestModelAwayAndBack(t *testing.T) {
	explore(t, "away and back", func(w *world) {
		seed(w.m, w.ns, w.p.Name, []string{"h1"}, true)
		pa := newProcess("A", w.m, w.s.Clock.Now())
		pb := newProcess("B", w.m, w.s.Clock.Now())
		w.informer("informerA", &pa)
		w.relister("relistA", &pa)
		secret := SecretRef(w.ns, w.p.Name)
		w.holder(holderSpec{
			name: "h1",
			refs: func(pass int) []Ref {
				if pass == 1 {
					return nil // away
				}
				return []Ref{secret} // and back
			},
			livePasses: 3, deletionPasses: 3,
		}, &pa)
		w.releaser("releaserB", &pb, 2, true, nil)
		w.deleter("teardown", keyOf(w.p), widgetKey(w.ns, "h1"))
	})
}

// A holder and the namespace are deleted during the grace, just after it,
// and between R2 and R3 (§6.4): the schedules cover every position of the
// two deletions relative to a release batch that runs after the Secret's
// deletion.
func TestModelDeletionsAroundTheGrace(t *testing.T) {
	explore(t, "deletions around the grace", func(w *world) {
		seed(w.m, w.ns, w.p.Name, []string{"h1"}, true)
		pa := newProcess("A", w.m, w.s.Clock.Now())
		w.informer("informerA", &pa)
		w.holder(holderSpec{name: "h1", livePasses: 1, deletionPasses: 3}, &pa)
		w.releaser("releaserA", &pa, 3, false, nil)
		w.deleter("secret", keyOf(w.p))
		w.s.Go("late deletions", 0, func(a *apimodel.Actor) {
			// Wait into the grace, then delete the holder and the namespace.
			a.Wait("into the grace", a.Now().Add(testGrace-time.Millisecond), nil)
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
	windows := 0
	explore(t, "recreate behind a cached UID", func(w *world) {
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
		w.s.Go("rotation", 0, func(a *apimodel.Actor) {
			w.call(a, "delete secret", func() error { return w.m.Delete(keyOf(w.p)) })
			a.Wait("gone", time.Time{}, func() bool { _, err := w.m.Get(keyOf(w.p)); return err != nil })
			w.call(a, "recreate secret", func() error { return w.m.Create(keyOf(w.p), metav1.ObjectMeta{}) })
			// The holder is deleted after its last live pass.
			a.Wait("last live pass", time.Time{}, func() bool { return w.livePasses["h1"] >= 3 })
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
