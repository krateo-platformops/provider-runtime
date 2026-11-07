package ratelimit

import (
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	utilclock "k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/controller/priorityqueue"
)

// The tests in this file come from an independent review of the first version
// of the gated queue, which called the inner queue outside the queue's lock and
// updated its record in a separate critical section. They failed on that
// version (hack/review-2026-11-07 keeps them as written). The interleaving tests
// are adapted only where the fix makes the reviewer's interleaving impossible:
// the hook now starts its call and waits briefly for it instead of requiring it
// to finish inside the window, because the window no longer exists. What they
// assert is unchanged.

// TestMain fails the package when any test found the records disagreeing with
// what happened (reportBrokenBooks).
func TestMain(m *testing.M) {
	code := m.Run()
	if n := brokenBooks.Load(); n != 0 && code == 0 {
		fmt.Fprintf(os.Stderr, "FAIL: the local capacity's records disagreed %d time(s) during the tests\n", n)
		code = 1
	}
	os.Exit(code)
}

// hookedInner runs a hook right after the real inner call returns.
type hookedInner struct {
	priorityqueue.PriorityQueue[string]
	beforeGet func()
	afterGet  func(key string)
	afterDone func(key string)
	getOnce   sync.Once
	doneOnce  sync.Once
}

func (h *hookedInner) GetWithPriority() (string, int, bool) {
	if h.beforeGet != nil {
		h.beforeGet()
	}
	k, p, s := h.PriorityQueue.GetWithPriority()
	if h.afterGet != nil {
		h.getOnce.Do(func() { h.afterGet(k) })
	}
	return k, p, s
}

func (h *hookedInner) Done(key string) {
	h.PriorityQueue.Done(key)
	if h.afterDone != nil {
		h.doneOnce.Do(func() { h.afterDone(key) })
	}
}

func getWithin(q *gatedQueue[string], d time.Duration) (string, bool) {
	ch := make(chan string, 1)
	go func() { k, _, _ := q.GetWithPriority(); ch <- k }()
	select {
	case k := <-ch:
		return k, true
	case <-time.After(d):
		return "", false
	}
}

// Worker A's Done(k) unlocks k in inner. Worker B, granted for j, pops in that
// moment. If B could pop the re-added k before A's Done recorded it, B's own
// Done would find k unrecorded, never give its slot back, and strand j.
func TestDoneRacingAPopLeaksNoSlot(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	g.setLocalCapacity(2)
	q := gated(t, g, "done-race")
	h := &hookedInner{PriorityQueue: q.inner}
	q.inner = h

	q.Add("k")
	a := popWithin(t, q)
	if a != "k" {
		t.Fatalf("A popped %q", a)
	}
	q.Add("k") // re-added while A processes it
	q.Add("j")

	bPopped := make(chan string, 1)
	h.afterDone = func(string) {
		// B tries to pop now. In the first version it popped inside this
		// window; with the fix it waits for A's Done to finish.
		go func() { k, _, _ := q.GetWithPriority(); bPopped <- k }()
		time.Sleep(200 * time.Millisecond)
	}
	q.Done("k") // A finishes
	var b string
	select {
	case b = <-bPopped:
	case <-time.After(2 * time.Second):
		t.Fatal("B got nothing")
	}

	q.Done(b) // B finishes
	if n := g.inFlight(); n != 0 {
		t.Errorf("slot leaked: %d local slots in flight with nothing processing", n)
	}
	if _, ok := getWithin(q, time.Second); !ok {
		lockWithin(t, q, "the test")
		t.Errorf("an item stranded: no worker got it within 1s; free=%d ready=%v inner.Len=%d", q.free, q.ready, q.inner.Len())
		q.mu.Unlock()
	}
}

// An Add(k) that lands after inner handed k out, but before the pop recorded
// it, must not be lost: in the first version the record dropped it while inner
// still held k, so k was never handed out again.
func TestAddDuringAPopIsNotLost(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	g.setLocalCapacity(1)
	q := gated(t, g, "add-race")
	h := &hookedInner{PriorityQueue: q.inner}
	q.inner = h
	added := make(chan struct{})
	h.afterGet = func(key string) { // an event for k arrives now
		go func() { q.Add(key); close(added) }()
		select {
		case <-added:
		case <-time.After(200 * time.Millisecond):
		}
	}

	q.Add("k")
	a := popWithin(t, q)
	select {
	case <-added:
	case <-time.After(2 * time.Second):
		t.Fatal("the Add never returned")
	}
	q.Done(a)
	if _, ok := getWithin(q, time.Second); !ok {
		lockWithin(t, q, "the test")
		t.Errorf("re-added k never handed out again: free=%d ready=%v inner.Len=%d", q.free, q.ready, q.inner.Len())
		q.mu.Unlock()
	}
}

// The delayed version of the same window: an AddAfter(k) that lands while k is
// being popped must wait its delay, not become an immediate add that the record
// doesn't know of.
func TestAddAfterDuringAPopKeepsItsDelay(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	g.setLocalCapacity(1)
	clk := clocktesting.NewFakeClock(time.Now())
	q := newGatedQueue[string](g, "add-after-race", nil, clk)
	defer shutDownWithin(t, q)
	h := &hookedInner{PriorityQueue: q.inner}
	q.inner = h
	added := make(chan struct{})
	h.afterGet = func(key string) {
		go func() { q.AddAfter(key, time.Minute); close(added) }()
		select {
		case <-added:
		case <-time.After(200 * time.Millisecond):
		}
	}

	q.Add("k")
	a := popWithin(t, q)
	select {
	case <-added:
	case <-time.After(2 * time.Second):
		t.Fatal("the AddAfter never returned")
	}
	q.Done(a)
	got, _ := workers(q, 1)
	select {
	case p := <-got:
		t.Fatalf("%s was handed out at once: the AddAfter lost its delay", p.key)
	case <-time.After(200 * time.Millisecond):
	}
	stepWithin(t, clk, time.Minute)
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		lockWithin(t, q, "the test")
		t.Errorf("k was not handed out after its delay: waiting=%v ready=%v free=%d inner.Len=%d", q.waiting, q.ready, q.free, q.inner.Len())
		q.mu.Unlock()
	}
}

// drainDeadline is how long the stress tests wait for a queue to drain once the
// adds stop, the same for the gated queue and upstream's. It was set from data:
// in 200 runs of upstream's priority queue under -race, at host load 55 to 65,
// three took longer than 2s and the slowest 2.62s; every one drained. 10s leaves
// about four times that.
const drainDeadline = 10 * time.Second

// stressLoad runs workers that pop, hold briefly and call Done, and adders that
// re-add a few keys, for a while; then it stops the adders. Every worker runs as
// controller-runtime's do. It returns when the adders have stopped.
func stressLoad(q priorityqueue.PriorityQueue[string], workerCount int, running *atomic.Int32) (stopWorkers func(t *testing.T)) {
	var wg sync.WaitGroup
	for range workerCount {
		wg.Go(func() {
			for {
				k, _, shut := q.GetWithPriority()
				if shut {
					return
				}
				if running != nil {
					running.Add(1)
				}
				time.Sleep(time.Duration(rand.IntN(50)) * time.Microsecond)
				if running != nil {
					running.Add(-1)
				}
				q.Done(k)
			}
		})
	}
	stop := make(chan struct{})
	var adders sync.WaitGroup
	for range 4 {
		adders.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				q.Add("k" + strconv.Itoa(rand.IntN(6)))
			}
		})
	}
	time.Sleep(300 * time.Millisecond)
	close(stop)
	adders.Wait()
	return func(t *testing.T) {
		t.Helper()
		within(t, "stopping the workers", func() { q.ShutDown(); wg.Wait() })
	}
}

// No hooks: 8 workers, 4 adders re-adding 6 keys, capacity 4. After the adds
// stop, everything must drain within drainDeadline and every slot come back.
// The first version failed this 5 of 5 times in review.
func TestGatedQueueDrainsUnderConcurrentAddsAndDones(t *testing.T) {
	for run := range 20 {
		g, _ := saturationGovernor(t, 8, "healthy", false)
		g.setLocalCapacity(4)
		q := gated(t, g, "stress"+strconv.Itoa(run))
		var running atomic.Int32
		stopWorkers := stressLoad(q, 8, &running)
		deadline := time.Now().Add(drainDeadline)
		for {
			lockWithin(t, q, "draining")
			drained := len(q.ready) == 0 && len(q.processing) == 0 && q.inner.Len() == 0
			q.mu.Unlock()
			if drained && g.inFlight() == 0 {
				break
			}
			if time.Now().After(deadline) {
				lockWithin(t, q, "the test")
				t.Errorf("run %d: did not drain: ready=%v processing=%v free=%d inner.Len=%d", run, q.ready, q.processing, q.free, q.inner.Len())
				q.mu.Unlock()
				t.Errorf("run %d: local slots in flight %d, reconciles running %d", run, g.inFlight(), running.Load())
				break
			}
			time.Sleep(time.Millisecond)
		}
		stopWorkers(t)
		if t.Failed() {
			break // the next runs would only repeat it
		}
	}
}

// The control: the same load on controller-runtime's own priority queue drains,
// so the load itself is sound.
func TestUpstreamPriorityQueueDrainsUnderTheSameLoad(t *testing.T) {
	for run := range 20 {
		q := priorityqueue.New[string]("ctl"+strconv.Itoa(run), func(o *priorityqueue.Opts[string]) { o.Log = logr.Discard() })
		stopWorkers := stressLoad(q, 4, nil)
		deadline := time.Now().Add(drainDeadline)
		for q.Len() != 0 {
			if time.Now().After(deadline) {
				t.Errorf("run %d: upstream did not drain: Len=%d", run, q.Len())
				break
			}
			time.Sleep(time.Millisecond)
		}
		stopWorkers(t)
		if t.Failed() {
			break // the next runs would only repeat it
		}
	}
}

// checkBooks verifies, under q.mu, what the record must always say: free is the
// ready keys not processing, a grant is for a free item, and inner holds exactly
// the ready keys.
func checkBooks(q *gatedQueue[string]) error {
	for deadline := time.Now().Add(5 * time.Second); !q.mu.TryLock(); time.Sleep(100 * time.Microsecond) {
		if time.Now().After(deadline) {
			return fmt.Errorf("the queue's lock was not free within 5s")
		}
	}
	defer q.mu.Unlock()
	free := 0
	for k := range q.ready {
		if _, ok := q.processing[k]; !ok {
			free++
		}
	}
	switch {
	case q.free != free:
		return fmt.Errorf("free = %d, the record has %d ready keys not processing", q.free, free)
	case q.granted < 0 || q.idle < 0 || q.granted > q.free:
		return fmt.Errorf("granted %d, idle %d, free %d", q.granted, q.idle, q.free)
	case q.inner.Len() != len(q.ready):
		return fmt.Errorf("inner holds %d ready items, the record %d", q.inner.Len(), len(q.ready))
	}
	return nil
}

// The model test checks the record at quiet points, so it can't reach a race.
// This is its concurrent mode: real timers, adds of every kind, Done from the
// workers themselves and capacity changes, all at once, with the record checked
// while they run and the queue required to drain at the end.
//
// RLGOV_CONCURRENT_RUNS sets the number of runs (default 30).
func TestGatedQueueRecordStaysExactUnderConcurrency(t *testing.T) {
	runs := 30
	if v, err := strconv.Atoi(os.Getenv("RLGOV_CONCURRENT_RUNS")); err == nil && v > 0 {
		runs = v
	}
	before := brokenBooks.Load()
	for run := range runs {
		if !t.Run(strconv.Itoa(run), func(t *testing.T) { concurrentRun(t, run) }) {
			break
		}
	}
	if n := brokenBooks.Load() - before; n != 0 {
		t.Errorf("the record disagreed with what happened %d time(s)", n)
	}
}

func concurrentRun(t *testing.T, run int) {
	g, _ := saturationGovernor(t, 8, "healthy", false)
	g.setLocalCapacity(1 + run%4)
	limiter := fixedLimiter{delays: map[string]time.Duration{}}
	keys := make([]string, 8)
	for i := range keys {
		keys[i] = "k" + strconv.Itoa(i)
		limiter.delays[keys[i]] = time.Duration(i%3) * time.Millisecond
	}
	q := newGatedQueue[string](g, "concurrent"+strconv.Itoa(run), limiter, utilclock.RealClock{})

	var workers sync.WaitGroup
	for range 1 + run%6 {
		workers.Go(func() {
			for {
				k, _, shut := q.GetWithPriority()
				if shut {
					return
				}
				time.Sleep(time.Duration(rand.IntN(100)) * time.Microsecond)
				q.Done(k)
			}
		})
	}
	stop := make(chan struct{})
	var load sync.WaitGroup
	for range 3 {
		load.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				k := keys[rand.IntN(len(keys))]
				p := rand.IntN(5) - 2
				switch rand.IntN(4) {
				case 0, 1:
					q.AddWithOpts(priorityqueue.AddOpts{Priority: &p}, k)
				case 2:
					q.AddWithOpts(priorityqueue.AddOpts{After: time.Duration(rand.IntN(3000)) * time.Microsecond, Priority: &p}, k)
				default:
					q.AddRateLimited(k)
				}
				time.Sleep(time.Duration(rand.IntN(20)) * time.Microsecond)
			}
		})
	}
	load.Go(func() { // the sampler's stand-in
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(1+rand.IntN(5)) * time.Millisecond):
				g.setLocalCapacity(1 + rand.IntN(4))
			}
		}
	})
	var checked atomic.Int64
	checkErr := make(chan error, 1)
	load.Go(func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(500 * time.Microsecond):
				if err := checkBooks(q); err != nil {
					select {
					case checkErr <- err:
					default:
					}
					return
				}
				checked.Add(1)
			}
		}
	})
	time.Sleep(200 * time.Millisecond)
	close(stop)
	load.Wait()
	select {
	case err := <-checkErr:
		t.Fatalf("while running: %v", err)
	default:
	}

	g.setLocalCapacity(2)
	deadline := time.Now().Add(drainDeadline)
	for {
		lockWithin(t, q, "draining")
		drained := len(q.waiting) == 0 && len(q.ready) == 0 && len(q.processing) == 0 && q.free == 0 && q.granted == 0 && q.inner.Len() == 0
		state := fmt.Sprintf("waiting %d ready %v processing %v free %d granted %d inner.Len %d", len(q.waiting), q.ready, q.processing, q.free, q.granted, q.inner.Len())
		q.mu.Unlock()
		if drained && g.inFlight() == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("did not drain within %s: %s; local slots in flight %d", drainDeadline, state, g.inFlight())
		}
		time.Sleep(time.Millisecond)
	}
	if err := checkBooks(q); err != nil {
		t.Fatalf("after draining: %v", err)
	}
	if checked.Load() == 0 {
		t.Error("the record was never checked while the load ran")
	}
	within(t, "ShutDown", q.ShutDown)
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("workers did not return after ShutDown")
	}
}

// A slot given back with none in flight is a broken record: it is reported, not
// clamped away in silence.
func TestReleasingASlotThatIsNotHeldIsReported(t *testing.T) {
	logs := &recordingLog{}
	g, err := New(Options{MaxConcurrentReconciles: 1, Logger: logs, Saturation: SaturationOptions{Enabled: true, CgroupDir: fixture("healthy")}})
	if err != nil {
		t.Fatal(err)
	}
	before := brokenBooks.Load()
	g.releaseLocal()
	if n := brokenBooks.Load() - before; n != 1 {
		t.Errorf("reported %d broken records, want 1", n)
	}
	if logs.count("ERROR The local capacity's records disagree") != 1 {
		t.Errorf("logs = %v, want one error", logs.lines)
	}
	if n := g.inFlight(); n != 0 {
		t.Errorf("%d local slots in flight, want 0", n)
	}
	brokenBooks.Store(before) // this one was on purpose
}

// A pop waits inside inner, holding the queue's lock, while ShutDown is called:
// ShutDown waits for the lock, inner's own dispatcher still hands the counted
// item out, the pop returns it, and then ShutDown runs. Neither hangs.
func TestShutDownDuringAPopInsideInnerReturns(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	q := newGatedQueue[string](g, "shutdown-in-pop", nil, clocktesting.NewFakeClock(time.Now()))
	h := &hookedInner{PriorityQueue: q.inner}
	q.inner = h
	shutdownCalled := make(chan struct{})
	shutdownDone := make(chan struct{})
	h.beforeGet = func() {
		go func() {
			close(shutdownCalled)
			q.ShutDown()
			close(shutdownDone)
		}()
		<-shutdownCalled
		time.Sleep(100 * time.Millisecond) // ShutDown is now waiting for the lock
	}
	q.Add("k")
	popped := make(chan string, 1)
	go func() { k, _, _ := q.GetWithPriority(); popped <- k }()
	var k string
	select {
	case k = <-popped:
	case <-time.After(5 * time.Second):
		t.Fatal("the pop did not return while ShutDown was waiting")
	}
	if k != "k" {
		t.Errorf("popped %q, want k", k)
	}
	select {
	case <-shutdownDone:
	case <-time.After(5 * time.Second):
		t.Fatal("ShutDown did not return after the pop")
	}
	q.Done(k)
	if n := g.inFlight(); n != 0 {
		t.Errorf("%d local slots in flight after Done, want 0", n)
	}
}

// A pop that finds the record disagreeing with what inner handed out is
// reported. Here the record is made wrong on purpose: k is free in inner, but
// the record says a worker holds it.
func TestAPopTheRecordDisagreesWithIsReported(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	q := gated(t, g, "disagreeing-record")
	g.setLocalCapacity(0)
	q.Add("k")
	lockWithin(t, q, "the test")
	q.processing["k"] = struct{}{} // the record now says k is held, inner doesn't
	q.mu.Unlock()
	before := brokenBooks.Load()
	got, _ := workers(q, 1)
	eventually(t, "the worker waiting", func() bool { return idleWorkers(q) == 1 })
	g.mu.Lock()
	if !q.tryGrant() { // q.free still counts k, so the grant goes through
		g.mu.Unlock()
		t.Fatal("no grant")
	}
	g.localInFlight++
	g.mu.Unlock()
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("nothing was popped")
	}
	if n := brokenBooks.Load() - before; n != 1 {
		t.Errorf("reported %d broken records, want 1", n)
	}
	brokenBooks.Store(before) // this one was on purpose
}
