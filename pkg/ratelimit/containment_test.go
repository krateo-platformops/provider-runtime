package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"

	utilclock "k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/controller/priorityqueue"
)

// returnsWithin reports whether f returns within d.
func returnsWithin(d time.Duration, f func()) bool {
	done := make(chan struct{})
	go func() { f(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// stuckPop makes queue a's record count one item its inner queue doesn't hold,
// the defect a broken record would be, and starts a worker that is granted for
// it and waits inside inner holding a's lock. It returns once that pop has
// reached inner. unstick ends the pop by adding the item to inner, and lets the
// worker finish; it acts once however often it is called, so a test can both
// register it as a cleanup and call it.
func stuckPop(t *testing.T, g *Governor, a *gatedQueue[string]) (unstick func()) {
	t.Helper()
	entered := make(chan struct{})
	var enterOnce sync.Once
	h := &hookedInner{PriorityQueue: a.inner}
	h.beforeGet = func() { enterOnce.Do(func() { close(entered) }) }
	lockWithin(t, a, "making the record wrong")
	a.inner = h
	a.free++ // the defect: counts an item inner doesn't hold
	a.mu.Unlock()
	popped := make(chan string, 1)
	go func() { k, _, _ := a.GetWithPriority(); popped <- k }()
	// The hook runs with the pop holding a's lock, as it calls into inner,
	// which holds no item for it. (No new field is read here, so this helper
	// also runs on the code before containment, where the test fails.)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("a's pop never reached inner")
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			a.inner.Add("unstick") // the item the record counted, straight into inner
			select {
			case k := <-popped:
				a.Done(k)
			case <-time.After(5 * time.Second):
				t.Error("a's pop did not return after its item was added to inner")
			}
		})
	}
}

// A broken record that leaves one queue's pop stuck inside inner must stop that
// queue only. Before containment it froze the whole process: every dispatch
// waited on that queue's lock holding the Governor's, so other controllers'
// adds and pops, every reconcile's Acquire, the metrics callback and shutdown
// all blocked, with health checks still green.
func TestAStuckPopFreezesOnlyItsQueue(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	g.setLocalCapacity(4)
	a := newGatedQueue[string](g, "a", nil, utilclock.RealClock{})
	b := newGatedQueue[string](g, "b", nil, utilclock.RealClock{})
	before := brokenBooks.Load()
	// Cleanups run last first: unstick a's pop, shut both queues down, then
	// clear the report the stuck pop makes on purpose. A t.Fatal below then
	// leaves no pop stuck behind it.
	t.Cleanup(func() { brokenBooks.Store(before) })
	t.Cleanup(func() { returnsWithin(5*time.Second, b.ShutDown) })
	t.Cleanup(func() { returnsWithin(5*time.Second, a.ShutDown) })
	unstick := stuckPop(t, g, a)
	t.Cleanup(unstick)

	const d = time.Second
	if !returnsWithin(d, func() { b.Add("x") }) {
		t.Fatal("another queue's Add did not return")
	}
	var x string
	if !returnsWithin(d, func() { x, _, _ = b.GetWithPriority() }) || x != "x" {
		t.Fatalf("another queue's pop did not return x (got %q)", x)
	}
	if !returnsWithin(d, func() { b.Done(x) }) {
		t.Fatal("another queue's Done did not return")
	}
	if !returnsWithin(d, func() {
		release, _ := g.Acquire(context.Background(), "an-unrelated-endpoint")
		if release != nil {
			release()
		}
	}) {
		t.Fatal("an unrelated Acquire did not return")
	}
	if !returnsWithin(d, func() { g.Snapshot() }) {
		t.Fatal("Snapshot, the metrics callback, did not return")
	}
	if !returnsWithin(d, b.ShutDown) {
		t.Fatal("another queue's ShutDown did not return")
	}

	unstick()
	within(t, "a's ShutDown", a.ShutDown)
	// The stuck pop's item wasn't in the record: that pop reported it, and the
	// cleanup clears that report.
}

// The watchdog: a pop still waiting inside inner after popStuckAfter is
// reported, and logged, while it waits.
func TestAStuckPopIsReported(t *testing.T) {
	old := popStuckAfter
	popStuckAfter = 200 * time.Millisecond
	t.Cleanup(func() { popStuckAfter = old })
	logs := &recordingLog{}
	g, err := New(Options{MaxConcurrentReconciles: 4, Logger: logs, Saturation: SaturationOptions{Enabled: true, CgroupDir: fixture("healthy")}})
	if err != nil {
		t.Fatal(err)
	}
	g.setLocalCapacity(1)
	a := newGatedQueue[string](g, "a", nil, utilclock.RealClock{})
	before := brokenBooks.Load()
	unstick := stuckPop(t, g, a)
	t.Cleanup(unstick)

	// reportBrokenBooks counts, then logs: wait for both, or the log check can
	// land between the two.
	deadline := time.Now().Add(2 * time.Second)
	for {
		counted := brokenBooks.Load() != before
		logged := logs.count("ERROR The local capacity's records disagree") > 0
		if counted && logged {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("a pop stuck for %s past the watchdog's %s was not reported (counted %v, logged as an error %v)", 2*time.Second, popStuckAfter, counted, logged)
		}
		time.Sleep(10 * time.Millisecond)
	}
	unstick()
	within(t, "a's ShutDown", a.ShutDown)
	brokenBooks.Store(before) // these were on purpose
}

// A grant skipped while a queue pops is not lost: a slot that frees during the
// pop, and that only the popping queue can use, is granted to it after the pop.
func TestAGrantSkippedDuringAPopIsGrantedAfterIt(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	g.setLocalCapacity(2)
	a := gated(t, g, "a")
	b := gated(t, g, "b")
	gotB, _ := workers(b, 1)
	b.Add("b1")
	bItem := receive(t, gotB) // b holds one of the two slots

	h := &hookedInner{PriorityQueue: a.inner}
	a.inner = h
	var once sync.Once
	doneReturned := make(chan time.Duration, 1)
	h.beforeGet = func() {
		once.Do(func() {
			// While a pops a1, b finishes and frees its slot. Only a has work.
			// The pop waits here, holding a's lock, until b's Done has returned,
			// so the freed slot's dispatch happens during the pop. Without
			// containment that Done would wait for a's lock; the 1s bound keeps
			// the test from deadlocking then.
			returned := make(chan struct{})
			go func() {
				start := time.Now()
				b.Done(bItem.key)
				doneReturned <- time.Since(start)
				close(returned)
			}()
			select {
			case <-returned:
			case <-time.After(time.Second):
			}
		})
	}
	// Bound workers, as controller-runtime's are: the one that popped a1 keeps
	// it until released, so it doesn't come back and dispatch by itself.
	gotA, _ := boundWorkers(a, 2)
	// Both idle first: a worker going idle dispatches, which would grant a2
	// by itself and hide a lost grant.
	eventually(t, "a's workers waiting", func() bool { return idleWorkers(a) == 2 })
	// One call, so a2 is there before a1's pop starts: the slot b frees then
	// has work waiting, whatever the timing.
	a.AddWithOpts(priorityqueue.AddOpts{}, "a1", "a2")
	var first heldItem
	select {
	case first = <-gotA:
	case <-time.After(5 * time.Second):
		t.Fatal("a1 was never popped")
	}
	defer close(first.release)
	select {
	case took := <-doneReturned:
		if took > 200*time.Millisecond {
			t.Errorf("b's Done took %s while a was popping: a dispatch waited on a's pop", took)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("b's Done never returned")
	}
	select {
	case second := <-gotA:
		defer close(second.release)
		if second.key == first.key {
			t.Errorf("popped %s twice", second.key)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("the slot b freed during a's pop was never granted to a: a's second item waits with a slot free (in flight %d)", g.inFlight())
	}
}

// The re-check in tryGrant: the pop finishes, and finds no skip to act on,
// between tryGrant seeing it in progress and recording the skip. tryGrant must
// then see the pop is over and grant, or the grant is lost: nobody dispatches
// again. The hook runs the pop's ending at exactly that point.
func TestAPopEndingDuringTheSkipLosesNoGrant(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	g.setLocalCapacity(0)
	q := gated(t, g, "skip-race")
	got, _ := workers(q, 1)
	eventually(t, "the worker waiting", func() bool { return idleWorkers(q) == 1 })
	q.Add("k")            // available, but no capacity yet
	q.popping.Store(true) // a pop in progress, as far as tryGrant can tell
	hook := func() {
		// The pop ends now: clear, unlock, then look for a skip; there is none yet.
		q.popping.Store(false)
		if q.skipped.Swap(false) {
			t.Error("the pop found a skip before tryGrant recorded one")
		}
		skipRaceHook.Store(nil) // once
	}
	skipRaceHook.Store(&hook)
	t.Cleanup(func() { skipRaceHook.Store(nil) })
	g.setLocalCapacity(1) // dispatch: tryGrant sees the pop, the hook ends it
	select {
	case p := <-got:
		if p.key != "k" {
			t.Errorf("popped %s, want k", p.key)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("the grant was lost: k waits with a slot free (in flight %d)", g.inFlight())
	}
}
