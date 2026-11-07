package ratelimit

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	utilclock "k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/controller/priorityqueue"
)

// returnsWithin reports whether f returns, without panicking, within d. A
// panic counts as not returning, so it fails the caller's check rather than
// ending the package's run.
func returnsWithin(d time.Duration, f func()) bool {
	done := make(chan struct{})
	ok := false
	go func() {
		defer close(done)
		defer func() { _ = recover() }()
		f()
		ok = true
	}()
	select {
	case <-done:
		return ok
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

// ShutDown ends a stuck pop: it shuts inner down before it takes the queue's
// lock, which the pop holds, and inner's ShutDown is what wakes the pop. Before,
// graceful shutdown hung in exactly the case the watchdog exists for.
// ShuttingDown and Len take no lock, so they answer meanwhile. The pop that
// ShutDown ends is no broken record, and it gives back its grant's slot.
func TestShutDownEndsAStuckPop(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	g.setLocalCapacity(4)
	a := newGatedQueue[string](g, "a", nil, utilclock.RealClock{})
	before := brokenBooks.Load()
	t.Cleanup(func() { brokenBooks.Store(before) })
	t.Cleanup(func() { returnsWithin(5*time.Second, a.ShutDown) })
	unstick := stuckPop(t, g, a)
	t.Cleanup(unstick)

	const d = time.Second
	var shutting bool
	if !returnsWithin(d, func() { shutting = a.ShuttingDown() }) {
		t.Error("ShuttingDown did not return while a pop was stuck")
	} else if shutting {
		t.Error("ShuttingDown reported true before ShutDown")
	}
	if !returnsWithin(d, func() { a.Len() }) {
		t.Error("Len did not return while a pop was stuck")
	}
	if !returnsWithin(2*time.Second, a.ShutDown) {
		t.Fatal("ShutDown did not return while a pop was stuck: graceful shutdown hangs")
	}
	if !a.ShuttingDown() {
		t.Error("ShuttingDown reported false after ShutDown")
	}
	if got := brokenBooks.Load() - before; got != 0 {
		t.Errorf("the pop ShutDown ended was reported as a broken record (%d reports)", got)
	}
	eventually(t, "the stuck pop's slot given back", func() bool { return g.inFlight() == 0 })
}

// A normal pop racing ShutDown: inner either hands the item out or ends the pop
// with shutdown, and neither is a broken record. A popped item's Done after the
// shutdown still gives its slot back, and no slot stays in flight.
func TestAPopRacingShutDownReportsNoBrokenRecord(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	g.setLocalCapacity(1)
	before := brokenBooks.Load()
	gotItem, gotShutdown := 0, 0
	for i := range 200 {
		q := newGatedQueue[string](g, "race", nil, utilclock.RealClock{})
		h := &hookedInner{PriorityQueue: q.inner}
		q.inner = h
		shutDown := make(chan struct{})
		// ShutDown starts as the pop enters inner, a little later each run, so
		// some runs end the pop inside inner and some find it handed out.
		h.beforeGet = func() {
			go func() {
				for range i % 40 {
					runtime.Gosched()
				}
				q.ShutDown()
				close(shutDown)
			}()
		}
		q.Add("k")
		type result struct {
			key  string
			shut bool
		}
		res := make(chan result, 1)
		go func() { k, _, s := q.GetWithPriority(); res <- result{k, s} }()
		var r result
		select {
		case r = <-res:
		case <-time.After(5 * time.Second):
			t.Fatalf("run %d: the pop racing ShutDown never returned", i)
		}
		select {
		case <-shutDown:
		case <-time.After(5 * time.Second):
			t.Fatalf("run %d: ShutDown racing a pop never returned", i)
		}
		if r.shut {
			gotShutdown++
		} else {
			gotItem++
			if !returnsWithin(time.Second, func() { q.Done(r.key) }) {
				t.Fatalf("run %d: Done after ShutDown did not return", i)
			}
		}
		if n := g.inFlight(); n != 0 {
			t.Fatalf("run %d: %d slots in flight after the pop and its Done (popped %v)", i, n, !r.shut)
		}
		if got := brokenBooks.Load() - before; got != 0 {
			t.Fatalf("run %d: a pop racing ShutDown was reported as a broken record (popped %v)", i, !r.shut)
		}
	}
	t.Logf("200 runs: the pop got the item %d times, shutdown %d times", gotItem, gotShutdown)
}

// wedgingInner stands for upstream's priority queue after a shutdown that
// raced a hand-out: its hand-out goroutine sends on the unbuffered get channel
// holding the locks Done and Len take (priorityqueue.go:369-400), the pop it
// was for has left on done (:436-444), and the send never completes. From then
// on its Done and Len never return.
type wedgingInner struct {
	priorityqueue.PriorityQueue[string]
	shut   atomic.Bool
	wedged chan struct{}
}

func (w *wedgingInner) ShutDown() { w.shut.Store(true); w.PriorityQueue.ShutDown() }

func (w *wedgingInner) Done(key string) {
	if w.shut.Load() {
		<-w.wedged
	}
	w.PriorityQueue.Done(key)
}

func (w *wedgingInner) Len() int {
	if w.shut.Load() {
		<-w.wedged
	}
	return w.PriorityQueue.Len()
}

// Once ShutDown has begun the queue calls neither inner's Done nor its Len, so
// a worker that finishes after the shutdown gives its slot back and returns.
func TestDoneAfterShutDownSkipsInner(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	g.setLocalCapacity(1)
	q := gated(t, g, "wedge")
	w := &wedgingInner{PriorityQueue: q.inner, wedged: make(chan struct{})}
	t.Cleanup(func() { close(w.wedged) })
	q.inner = w
	got, _ := workers(q, 1)
	q.Add("k")
	k := receive(t, got)
	within(t, "ShutDown", q.ShutDown)
	if !returnsWithin(time.Second, func() { q.Done(k.key) }) {
		t.Fatal("Done after ShutDown waited on inner")
	}
	if n := g.inFlight(); n != 0 {
		t.Errorf("%d slots in flight after the held item's Done", n)
	}
	if !returnsWithin(time.Second, func() { q.Len() }) {
		t.Error("Len after ShutDown waited on inner")
	}
}

// ShutDown may be called more than once, and at once: inner's ShutDown, which
// panics the second time, runs once.
func TestShutDownTwiceAndAtOnce(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	q := gated(t, g, "twice")
	shut := func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("ShutDown panicked: %v", r)
			}
		}()
		q.ShutDown()
	}
	var wg sync.WaitGroup
	wg.Go(shut)
	wg.Go(shut)
	within(t, "two ShutDowns at once", wg.Wait)
	within(t, "a third ShutDown", shut)
}

// Len counts what the priority queue counts, ready items and not delayed ones,
// from the record.
func TestLenCountsReadyItems(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	g.setLocalCapacity(1)
	q := gated(t, g, "len")
	q.AddWithOpts(priorityqueue.AddOpts{}, "a", "b")
	q.AddAfter("later", time.Hour)
	if got := q.Len(); got != 2 {
		t.Errorf("Len = %d with two ready items and one delayed, want 2", got)
	}
	got, _ := workers(q, 1)
	receive(t, got)
	if got := q.Len(); got != 1 {
		t.Errorf("Len = %d after one was popped, want 1", got)
	}
}
