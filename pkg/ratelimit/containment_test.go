package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"

	utilclock "k8s.io/utils/clock"
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
// it and waits inside inner holding a's lock. unstick ends that pop by adding
// the item to inner, and lets the worker finish.
func stuckPop(t *testing.T, g *Governor, a *gatedQueue[string]) (unstick func()) {
	t.Helper()
	lockWithin(t, a, "making the record wrong")
	a.free++ // the defect: counts an item inner doesn't hold
	a.mu.Unlock()
	popped := make(chan string, 1)
	go func() { k, _, _ := a.GetWithPriority(); popped <- k }()
	// The pop holds a's lock, so its state can't be read: give it time to get
	// inside inner. (No new field is read here, so this test also runs on the
	// code before containment, where it fails.)
	time.Sleep(300 * time.Millisecond)
	return func() {
		t.Helper()
		a.inner.Add("unstick") // the item the record counted, straight into inner
		select {
		case k := <-popped:
			a.Done(k)
		case <-time.After(5 * time.Second):
			t.Error("a's pop did not return after its item was added to inner")
		}
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
	unstick := stuckPop(t, g, a)

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
	// The stuck pop's item wasn't in the record: that pop reported it.
	brokenBooks.Store(before)
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

	deadline := time.Now().Add(2 * time.Second)
	for brokenBooks.Load() == before {
		if time.Now().After(deadline) {
			t.Fatalf("a pop stuck for %s past the watchdog's %s was not reported", 2*time.Second, popStuckAfter)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if logs.count("ERROR The local capacity's records disagree") == 0 {
		t.Errorf("the stuck pop was not logged as an error: %v", logs.lines)
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
			go func() {
				start := time.Now()
				b.Done(bItem.key)
				doneReturned <- time.Since(start)
			}()
			time.Sleep(300 * time.Millisecond)
		})
	}
	gotA, _ := workers(a, 2)
	a.Add("a1")
	a.Add("a2")
	first := receive(t, gotA)
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
