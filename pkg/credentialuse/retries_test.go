package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/utils/clock"
	testingclock "k8s.io/utils/clock/testing"

	"golang.org/x/time/rate"
)

// Retries: what fails is retried, with backoff, and nothing else is, nor
// waits for it. An object of a kind the API server doesn't serve fails for as
// long as that lasts (§6.2), which may be for good.

// recheck is one try of the startup recheck, from nothing.
func (ix *index) recheck(ctx context.Context, enqueue func(id ID, n string)) error {
	return ix.recheckRest(ctx, map[ID]bool{}, enqueue)
}

// timersOf is what of k waits for a retry, and how many timers q has.
func timersOf(q *batchQueue, k batchKey) (retry []ID, timers int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for id, f := range q.failing[k] {
		if f.waits != nil {
			retry = append(retry, id)
		}
	}
	return SortedIDs(retry), len(q.timers)
}

// failuresOf is how often in a row each object of k failed.
func failuresOf(q *batchQueue, k batchKey) map[ID]int {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := map[ID]int{}
	for id, f := range q.failing[k] {
		out[id] = f.n
	}
	return out
}

func TestFailKeepsOneRetryPerObject(t *testing.T) {
	clk := testingclock.NewFakeClock(time.Now())
	q := newBatchQueue(nil)
	q.clock = &unlockedClock{FakeClock: clk, t: t, q: q}
	other := batchKey{widgetGK, "other"}
	a, b, c := idOf("a"), idOf("b"), idOf("c")
	take := func(want ...ID) {
		t.Helper()
		if q.Len() != 1 {
			t.Fatalf("Len = %d, want 1", q.Len())
		}
		k, ids, _ := q.Get(context.Background())
		q.Done(k)
		if k != batchNS || !slices.Equal(ids, want) {
			t.Fatalf("Get = %v %v, want %v", k, ids, want)
		}
	}
	timers := func(want int) {
		t.Helper()
		if _, n := timersOf(q, batchNS); n != want {
			t.Fatalf("%d timers, want %d", n, want)
		}
	}
	// Those that fail together wait together; each, once.
	if d := q.Fail(batchNS, a, b, a); d != time.Second {
		t.Errorf("the first delay is %s, want 1s", d)
	}
	timers(1)
	if d := q.Fail(batchNS, b, a); d != 0 {
		t.Errorf("objects that wait were given %s", d)
	}
	timers(1)
	clk.Step(time.Second)
	take(a, b)
	timers(0)
	// Each on its own count: the soonest is returned.
	if d := q.Fail(batchNS, a, c); d != time.Second {
		t.Errorf("the soonest delay is %s, want 1s", d)
	}
	timers(2)
	clk.Step(time.Second)
	take(c)
	// A grace's end is another timer, and another key's failures its own.
	q.AddAfter(batchNS, 3*time.Second, b)
	q.Fail(other, a)
	if got := failuresOf(q, batchNS); got[a] != 2 || got[b] != 1 || got[c] != 1 || len(got) != 3 {
		t.Errorf("failures %v, want a twice, b and c once", got)
	}
	clk.Step(time.Second)
	if k, ids, _ := q.Get(context.Background()); k != batchNS || !slices.Equal(ids, []ID{a}) {
		t.Fatalf("Get = %v %v, want the second retry of a, 2s after its first", k, ids)
	}
	// While its batch runs, a fails again, and something else adds it again:
	// both hold.
	q.Add(batchNS, a)
	if d := q.Fail(batchNS, a); d != 4*time.Second {
		t.Errorf("the third delay is %s, want 4s", d)
	}
	q.Done(batchNS)
	if k, ids, _ := q.Get(context.Background()); k != other || !slices.Equal(ids, []ID{a}) {
		t.Fatalf("Get = %v %v, want the other key's", k, ids)
	}
	q.Done(other)
	take(a)
	// What is decided is forgotten, with its timer, unless others wait for it.
	q.Fail(batchNS, c, b) // both once before: 2s
	timers(3)             // a's, theirs, and the grace's end
	q.Settle(batchNS, c)
	timers(3)
	q.Settle(batchNS, b, c, idOf("never"))
	timers(2)
	// One that failed, was decided, and failed again isn't retried at the old
	// time, shared with another that still waits for it.
	d1, d2 := idOf("d1"), idOf("d2")
	q.Settle(batchNS, a)
	q.Fail(batchNS, d1, d2) // 1s
	q.Settle(batchNS, d2)
	clk.Step(500 * time.Millisecond)
	q.Fail(batchNS, d2)
	clk.Step(500 * time.Millisecond)
	take(d1)
	clk.Step(500 * time.Millisecond)
	take(d2)
	clk.Step(500 * time.Millisecond)
	take(b) // the grace's end
	// ShutDown ends it all.
	q.Fail(batchNS, a, d1)
	q.ShutDown()
	if ids, n := timersOf(q, batchNS); n != 0 || ids != nil || clk.HasWaiters() || len(q.failing) != 0 {
		t.Errorf("after ShutDown, %d timers, %v waiting, waiters %v", n, ids, clk.HasWaiters())
	}
	if d := q.Fail(batchNS, a); d != 0 || clk.HasWaiters() || len(q.failing) != 0 {
		t.Errorf("a Fail after ShutDown returned %s", d)
	}
}

// Fail and Settle race the timers, the workers and ShutDown, on a clock that
// fires under its own lock (D1 of #109), and on the real one: no deadlock, no
// object lost, and no more timers than objects that wait.
func TestFailUnderStress(t *testing.T) {
	t.Run("a clock that fires under its lock", func(t *testing.T) {
		clk := testingclock.NewFakeClock(time.Now())
		stressQueue(t, clk, func() { clk.Step(50 * time.Microsecond) })
		if clk.HasWaiters() {
			t.Error("a timer outlived ShutDown")
		}
	})
	t.Run("the real clock", func(t *testing.T) { stressQueue(t, clock.RealClock{}, nil) })
}

func stressQueue(t *testing.T, clk clock.WithDelayedExecution, step func()) {
	const keys, adders, each = 3, 4, 1200
	q := newBatchQueue(clk)
	unit := 50 * time.Microsecond
	q.backoff = func(n int) time.Duration { return time.Duration(1+n%5) * unit }
	key := func(i int) batchKey { return batchKey{widgetGK, fmt.Sprintf("ns%d", i%keys)} }

	// A one-off object is added, or failed, once, and is in some later batch,
	// whose worker may fail it again: each time it is owed again, by the only
	// one who can add it. A shared one is added again and again, also while its
	// batch is worked on or being taken, each time with a fresh one-off object:
	// one call adds both or neither, so one of those additions that is dropped
	// is seen, which a look at the shared one alone can forgive. An addition
	// that skipped only the objects being worked on isn't: the fresh one is
	// not; TestBatchQueueEveryOrdering has that. Its worker
	// fails it now and then. (Fail and Settle of one object are its worker's
	// alone, as in the release loop: a Settle that raced another's Fail would
	// forget it.) A settled one fails and is settled; it owes nothing.
	var mu sync.Mutex
	owed := map[batchKey]map[ID]bool{} // one-off objects not yet in a batch
	owe := func(k batchKey, id ID) {
		mu.Lock()
		defer mu.Unlock()
		if owed[k] == nil {
			owed[k] = map[ID]bool{}
		}
		owed[k][id] = true
	}
	lost := func() (out []string) {
		mu.Lock()
		defer mu.Unlock()
		for k, m := range owed {
			for id := range m {
				out = append(out, fmt.Sprint(k, id))
			}
		}
		return out
	}
	invariant := func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		waiting, left, retries := 0, 0, 0
		for _, m := range q.failing {
			for _, f := range m {
				if f.waits != nil {
					waiting++
				}
			}
		}
		for e := range q.timers {
			if e.retry {
				retries++
				left += e.left
			}
		}
		if left != waiting || retries > waiting {
			t.Errorf("%d retry timers for %d objects, %d wait", retries, left, waiting)
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var workers, adding, stepping sync.WaitGroup
		var quiet atomic.Bool // the workers fail nothing any more
		for range 2 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for turn := 0; ; turn++ {
					k, ids, ok := q.Get(ctx)
					if !ok {
						return
					}
					if len(ids) == 0 {
						t.Errorf("a batch of %v with no objects, which asks for the whole namespace", k)
					}
					mu.Lock()
					for _, id := range ids {
						delete(owed[k], id)
					}
					mu.Unlock()
					time.Sleep(20 * time.Microsecond) // a batch takes time
					var failed, decided []ID
					for i, id := range ids {
						switch again := (turn+i)%3 == 0 && !quiet.Load(); {
						case again && strings.HasPrefix(id.Name, "one"):
							owe(k, id)
							failed = append(failed, id)
						case again && strings.HasPrefix(id.Name, "shared"):
							failed = append(failed, id)
						default:
							decided = append(decided, id)
						}
					}
					q.Settle(k, decided...)
					q.Fail(k, failed...)
					q.Done(k)
				}
			}()
		}
		stop := make(chan struct{})
		stepping.Add(1)
		go func() {
			defer stepping.Done()
			for {
				select {
				case <-stop:
					return
				default:
					if step == nil {
						time.Sleep(unit)
					} else {
						step()
					}
				}
			}
		}()
		for a := range adders {
			adding.Add(1)
			go func() {
				defer adding.Done()
				for i := range each {
					k := key(i)
					one := idOf(fmt.Sprintf("one%d-%d", a, i))
					shared, with := idOf(fmt.Sprintf("shared%d", i%keys)), idOf(fmt.Sprintf("with%d-%d", a, i))
					switch i % 6 {
					case 0:
						owe(k, one)
						q.AddAfter(k, time.Duration(i%7)*unit, one)
					case 1, 2:
						owe(k, one)
						q.Fail(k, one)
					case 3:
						owe(k, with)
						q.Add(k, shared, with)
					case 4:
						// Times that several additions share, so that the
						// shared object's is often there already.
						owe(k, with)
						q.AddAt(k, clk.Now().Truncate(8*unit).Add(time.Duration(i%3)*8*unit), shared, with)
					case 5:
						id := idOf(fmt.Sprintf("settled%d-%d", a, i%40))
						q.Fail(k, id)
						q.Settle(k, id)
					}
					if i%50 == 0 {
						invariant()
					}
				}
			}()
		}
		adding.Wait()
		// Nothing is added or fails any more, so everything there is drains:
		// nothing is owed, waits, or is remembered. One look a turn, and the
		// last one counts: while the workers still failed some, one was owed
		// between two looks.
		quiet.Store(true)
		left := func() (out []string) {
			out = lost()
			q.mu.Lock()
			defer q.mu.Unlock()
			if len(q.timers)+len(q.failing)+len(q.asked)+len(q.pending)+len(q.working) > 0 {
				out = append(out, fmt.Sprintf("%d timers, %d keys with failures, %d times asked for, %d waiting, %d worked on", len(q.timers), len(q.failing), len(q.asked), len(q.pending), len(q.working)))
			}
			return out
		}
		deadline := time.Now().Add(30 * time.Second)
		l := left()
		for len(l) > 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
			l = left()
		}
		if len(l) > 0 {
			t.Errorf("%d left after 30s of quiet: %v", len(l), l[:min(len(l), 5)])
		}
		invariant()

		// ShutDown against failures and their timers.
		var last sync.WaitGroup
		last.Add(2)
		go func() {
			defer last.Done()
			for i := range 500 {
				q.Fail(key(i), idOf(fmt.Sprintf("x%d", i)))
				if i%3 == 0 {
					q.Settle(key(i), idOf(fmt.Sprintf("x%d", i)))
				}
			}
		}()
		go func() { defer last.Done(); time.Sleep(time.Millisecond); q.ShutDown() }()
		last.Wait()
		close(stop)
		stepping.Wait()
		cancel()
		workers.Wait()
	}()
	select {
	case <-done:
	case <-time.After(120 * time.Second):
		t.Fatal("deadlocked")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.timers) != 0 || len(q.failing) != 0 || len(q.asked) != 0 {
		t.Errorf("%d timers, %d keys with failures and %d times asked for after ShutDown", len(q.timers), len(q.failing), len(q.asked))
	}
}

// duringClock runs during, once, in the next AfterFunc, before or after the
// timer is the clock's: what may happen between Fail's two steps.
type duringClock struct {
	*testingclock.FakeClock
	before, after func(d time.Duration)
}

func (c *duringClock) AfterFunc(d time.Duration, f func()) clock.Timer {
	once := func(p *func(time.Duration)) {
		if h := *p; h != nil {
			*p = nil
			h(d)
		}
	}
	once(&c.before)
	t := c.FakeClock.AfterFunc(d, f)
	once(&c.after)
	return t
}

// What a worker fails at is owed until its retry's batch is taken, and so each
// time it fails again: nothing is owed, then one is, then nothing.
func TestARetryThatFailsIsTakenAgain(t *testing.T) {
	ctx := context.Background()
	clk := testingclock.NewFakeClock(time.Now())
	q := newBatchQueue(clk)
	id := idOf("s")
	q.Fail(batchNS, id)
	for _, wait := range []time.Duration{1, 2, 4, 8} {
		clk.Step(wait*time.Second - 1)
		if q.Len() != 0 {
			t.Fatalf("the retry is there before %ds", wait)
		}
		clk.Step(1)
		k, ids, ok := q.Get(ctx)
		if !ok || k != batchNS || !slices.Equal(ids, []ID{id}) {
			t.Fatalf("after %ds, Get = %v %v %v", wait, k, ids, ok)
		}
		if waiting, n := timersOf(q, k); len(waiting)+n != 0 {
			t.Fatalf("taken, and %v still waits, with %d timers", waiting, n)
		}
		q.Fail(k, id) // owed again, while its batch is worked on
		if waiting, n := timersOf(q, k); !slices.Equal(waiting, []ID{id}) || n != 1 {
			t.Fatalf("failed, and %v waits, with %d timers", waiting, n)
		}
		q.Done(k)
		if q.Len() != 0 {
			t.Fatal("its batch is queued before its time")
		}
	}
	clk.Step(16 * time.Second)
	k, _, _ := q.Get(ctx)
	q.Settle(k, id)
	q.Done(k)
	if _, n := timersOf(q, k); n != 0 || len(q.failing) != 0 || clk.HasWaiters() {
		t.Errorf("decided, and %d timers, %d keys with failures", n, len(q.failing))
	}
}

// Every schedule of one thing that happens while Fail is between its two
// steps, and of some that happen one after the other: the object is retried once, at its time, or decided; never lost, and
// nothing is left.
func TestFailBetweenItsTwoSteps(t *testing.T) {
	ctx := context.Background()
	k, id := batchNS, idOf("a")
	run := func(t *testing.T, q *batchQueue, settle bool) {
		t.Helper()
		q.Add(k, id)
		got, ids, _ := q.Get(ctx)
		if got != k || !slices.Equal(ids, []ID{id}) {
			t.Fatalf("Get = %v %v", got, ids)
		}
		if settle {
			q.Settle(k, id)
		} else if d := q.Fail(k, id); d != 0 {
			t.Errorf("failing while it waits gave it %s", d)
		}
		q.Done(k)
	}
	for name, c := range map[string]struct {
		during func(t *testing.T, q *batchQueue, clk *testingclock.FakeClock, d time.Duration)
		early  bool // it can only happen once the clock has the timer
		// When the retry is taken: at once, after the base delay, or never.
		now, later bool
		n          int // how often it failed, then
	}{
		"nothing":         {during: func(*testing.T, *batchQueue, *testingclock.FakeClock, time.Duration) {}, later: true, n: 1},
		"the timer fires": {during: func(_ *testing.T, _ *batchQueue, clk *testingclock.FakeClock, d time.Duration) { clk.Step(d) }, early: true, now: true, n: 1},
		"it is settled":   {during: func(_ *testing.T, q *batchQueue, _ *testingclock.FakeClock, _ time.Duration) { q.Settle(k, id) }},
		"the timer fires, and it settles": {during: func(_ *testing.T, q *batchQueue, clk *testingclock.FakeClock, d time.Duration) {
			clk.Step(d)
			q.Settle(k, id)
		}, early: true, now: true},
		// The clock has the timer and Settle doesn't: it fires once nothing
		// waits for it.
		"it is settled, and the timer fires": {during: func(_ *testing.T, q *batchQueue, clk *testingclock.FakeClock, d time.Duration) {
			q.Settle(k, id)
			clk.Step(d)
		}, early: true},
		"it is settled, and fails again": {during: func(t *testing.T, q *batchQueue, _ *testingclock.FakeClock, _ time.Duration) {
			q.Settle(k, id)
			if d := q.Fail(k, id); d != time.Second {
				t.Errorf("failing after it was decided gave it %s", d)
			}
		}, later: true, n: 1},
		"it fails again": {during: func(t *testing.T, q *batchQueue, _ *testingclock.FakeClock, _ time.Duration) {
			if d := q.Fail(k, id); d != 0 {
				t.Errorf("failing while it waits gave it %s", d)
			}
		}, later: true, n: 1},
		"an event's batch decides it":  {during: func(t *testing.T, q *batchQueue, _ *testingclock.FakeClock, _ time.Duration) { run(t, q, true) }},
		"an event's batch fails at it": {during: func(t *testing.T, q *batchQueue, _ *testingclock.FakeClock, _ time.Duration) { run(t, q, false) }, later: true, n: 1},
		"the queue is shut down":       {during: func(_ *testing.T, q *batchQueue, _ *testingclock.FakeClock, _ time.Duration) { q.ShutDown() }},
		"the timer fires, and shut down": {during: func(_ *testing.T, q *batchQueue, clk *testingclock.FakeClock, d time.Duration) {
			clk.Step(d)
			q.ShutDown()
		}, early: true},
	} {
		for _, before := range []bool{true, false} {
			if before && c.early {
				continue
			}
			t.Run(fmt.Sprintf("%s, before %v", name, before), func(t *testing.T) {
				clk := testingclock.NewFakeClock(time.Now())
				q := newBatchQueue(nil)
				dc := &duringClock{FakeClock: clk}
				q.clock = dc
				during := func(d time.Duration) { c.during(t, q, clk, d) }
				if before {
					dc.before = during
				} else {
					dc.after = during
				}
				q.Fail(k, id)
				taken := func() bool {
					if q.Len() == 0 {
						return false
					}
					got, ids, _ := q.Get(ctx)
					q.Done(got)
					if got != k || !slices.Equal(ids, []ID{id}) {
						t.Errorf("the batch of %v is %v, want the object alone: none asks for the whole namespace", got, ids)
					}
					return true
				}
				if got := taken(); got != c.now {
					t.Errorf("taken at once: %v, want %v", got, c.now)
				}
				clk.Step(releaseBackoffBase - 1)
				if taken() {
					t.Error("taken before its time")
				}
				clk.Step(1)
				if got := taken(); got != c.later {
					t.Errorf("taken after its delay: %v, want %v", got, c.later)
				}
				if got := failuresOf(q, k)[id]; got != c.n {
					t.Errorf("it failed %d times, want %d", got, c.n)
				}
				if ids, n := timersOf(q, k); n != 0 || len(ids) != 0 || clk.HasWaiters() {
					t.Errorf("%d timers, %v waiting, the clock's waiters %v; want none", n, ids, clk.HasWaiters())
				}
				clk.Step(time.Hour)
				if taken() {
					t.Error("taken again")
				}
			})
		}
	}
}

// A Fail of nothing remembers nothing.
func TestFailOfNothing(t *testing.T) {
	q := newBatchQueue(testingclock.NewFakeClock(time.Now()))
	if d := q.Fail(batchNS); d != 0 || len(q.failing) != 0 || len(q.timers) != 0 {
		t.Errorf("a Fail of nothing gave %s, %d keys, %d timers", d, len(q.failing), len(q.timers))
	}
}

// A batch of a kind that is no longer tracked is dropped, and what it remembered
// with it.
func TestADroppedBatchForgetsItsFailures(t *testing.T) {
	r := newFakeRig(t)
	k := batchKey{widgetGK, "ns"} // nobody tracks it
	r.s.queue.Fail(k, secretID("ns", "s"), secretID("ns", "t"))
	r.clk.Step(releaseBackoffBase)
	got, ids, _ := r.s.queue.Get(context.Background())
	r.s.releaseBatch(context.Background(), got, ids)
	r.s.queue.Done(got)
	if f := failuresOf(r.s.queue, k); len(f) != 0 || len(r.s.queue.failing) != 0 || r.queueTimers() != 0 {
		t.Errorf("failures %v, %d timers; want none", f, r.queueTimers())
	}
}

// release says which objects it couldn't decide, and only those, wherever it
// fails; the others are decided.
func TestReleaseSaysWhatItCouldntDecide(t *testing.T) {
	ctx := context.Background()
	boom := apierrors.NewInternalError(errors.New("etcd is unhappy"))
	s1, s2 := SecretRef("ns", "s1").ID(), SecretRef("ns", "s2").ID()
	two := func(t *testing.T, change func(name string, m *metav1.ObjectMeta)) *rig {
		r := newRig(t)
		for _, name := range []string{"s1", "s2"} {
			m := metav1.ObjectMeta{Finalizers: []string{fNS}, Annotations: map[string]string{fNS: "e0"}, Labels: map[string]string{LabelHeld: "true", MarkerLabel(widgetGK): "true"}}
			if change != nil {
				change(name, &m)
			}
			must(r.m.Create(secretKey("ns", name), m))
		}
		return r
	}
	failOn := func(op, name string) func(string, string, string) error {
		return func(o, _, n string) error {
			if o == op && (n == name || name == "") {
				return boom
			}
			return nil
		}
	}
	noEpoch := func(name string, m *metav1.ObjectMeta) {
		if name == "s1" {
			delete(m.Annotations, fNS)
		}
	}
	for what, c := range map[string]struct {
		change  func(string, *metav1.ObjectMeta)
		before  func(op, kind, name string) error
		failed  []ID
		decided []ID
	}{
		"R1's read":          {nil, failOn("get", "s1"), []ID{s1}, []ID{s2}},
		"R3's patch":         {nil, failOn("patch", "s1"), []ID{s1}, []ID{s2}},
		"the epoch's repair": {noEpoch, failOn("patch", "s1"), []ID{s1}, []ID{s2}},
		"R2's list":          {nil, failOn("list", ""), []ID{s1, s2}, nil},
		"a patch rejected, the object unchanged": {nil, func(op, _, n string) error {
			if op == "patch" && n == "s1" {
				return apierrors.NewGenericServerResponse(422, "", schema.GroupResource{}, "", "test failed", 0, false)
			}
			return nil
		}, []ID{s1}, []ID{s2}},
		"nothing": {nil, nil, nil, []ID{s1, s2}},
	} {
		r := two(t, c.change)
		r.srv.before = c.before
		res, err := r.p.release(ctx, "ns", []ID{s1, s2})
		if (err != nil) != (len(c.failed) > 0) || !slices.Equal(SortedIDs(res.Failed), c.failed) && len(res.Failed)+len(c.failed) > 0 {
			t.Errorf("%s: failed %v, %v; want %v", what, res.Failed, err, c.failed)
		}
		if !slices.Equal(SortedIDs(res.Released), c.decided) && len(res.Released)+len(c.decided) > 0 {
			t.Errorf("%s: released %v, want %v", what, res.Released, c.decided)
		}
		if !res.RequeueAt.IsZero() {
			t.Errorf("%s: requeue at %s, want none: what failed has its backoff", what, res.RequeueAt)
		}
	}
	// An object that changes under every release.
	r := two(t, nil)
	n := 0
	r.srv.before = func(op, _, name string) error {
		if op != "patch" || name != "s1" {
			return nil
		}
		n++
		if _, err := r.m.Patch(secretKey("ns", "s1"), types.MergePatchType, []byte(fmt.Sprintf(`{"metadata":{"annotations":{"n":"%d"}}}`, n))); err != nil {
			t.Error(err)
		}
		return apierrors.NewGenericServerResponse(422, "", schema.GroupResource{}, "", "test failed", 0, false)
	}
	if res, err := r.p.release(ctx, "ns", []ID{s1, s2}); err == nil || n != maxReleaseRounds || !slices.Equal(res.Failed, []ID{s1}) || !slices.Equal(res.Released, []ID{s2}) {
		t.Errorf("an object that changes under %d releases: %+v, %v; want s1 failed, s2 released", n, res, err)
	}
	// A deleting object whose Namespace can't be read.
	r = two(t, nil)
	must(r.m.Delete(secretKey("ns", "s1")))
	r.tick(time.Hour)
	r.srv.before = func(op, kind, _ string) error {
		if op == "get" && kind == namespaceGroupKind.Kind {
			return boom
		}
		return nil
	}
	if res, err := r.p.release(ctx, "ns", []ID{s1, s2}); err == nil || !slices.Equal(res.Failed, []ID{s1}) || !slices.Equal(res.Released, []ID{s2}) {
		t.Errorf("the Namespace's read: %+v, %v; want s1 failed, s2 released", res, err)
	}
}

// failingGets fails the GETs of the Secrets named name while on.
func (r *fakeRig) failingGets(name string) (on *atomic.Bool) {
	on = &atomic.Bool{}
	on.Store(true)
	r.s.hooks.wrapServer = func(srv server) server {
		return hookServer{server: srv, before: func(_ context.Context, c Call) error {
			if c.Op == "get" && c.Name == name && on.Load() {
				return unservedError()
			}
			return nil
		}}
	}
	return on
}

// An object that can't be read is retried with backoff, in its key's one
// pending retry, however often its batch runs; the rest of its batch isn't
// retried with it, and is looked at again when it is due: here, at the end of
// its grace.
func TestAFailingObjectIsRetriedAlone(t *testing.T) {
	ctx := context.Background()
	inGrace := deleting(heldObject(secretGVK, "ns", "grace", "u2", widgetGK, "ns"), time.Now())
	r := newFakeRig(t, namespaceObject("ns"), heldObject(secretGVK, "ns", "fails", "u1", widgetGK, "ns"), inGrace,
		heldObject(secretGVK, "ns", "free", "u3", widgetGK, "ns"), holderObject("ns", "h", secretID("ns", "grace")))
	failing := r.failingGets("fails")
	r.track(t, widgetGK)
	r.run(t)
	r.drain()
	k := batchKey{widgetGK, "ns"}
	_, before := timersOf(r.s.queue, k)
	r.s.releaseBatch(ctx, k, []ID{secretID("ns", "fails"), secretID("ns", "free"), secretID("ns", "grace")})
	if hasFinalizer(r.secretMeta(t, "ns", "free"), fWidgetNS) || !hasFinalizer(r.secretMeta(t, "ns", "fails"), fWidgetNS) {
		t.Error("want the free Secret released, and the one that can't be read kept")
	}
	retry, n := timersOf(r.s.queue, k)
	if !slices.Equal(retry, []ID{secretID("ns", "fails")}) {
		t.Errorf("the retry covers %v, want what failed alone", retry)
	}
	if n-before != 2 {
		t.Errorf("%d timers more, want two: the retry, and the grace's end", n-before)
	}
	var atGrace []ID
	r.s.queue.mu.Lock()
	for e := range r.s.queue.timers {
		if !e.retry {
			atGrace = append(atGrace, e.ids...)
		}
	}
	r.s.queue.mu.Unlock()
	if slices.Contains(atGrace, secretID("ns", "fails")) || !slices.Contains(atGrace, secretID("ns", "grace")) {
		t.Errorf("the grace's end covers %v, want the Secret in its grace, without the one that failed", atGrace)
	}
	// Run again and again, as events do: one retry.
	for range 20 {
		r.s.releaseBatch(ctx, k, []ID{secretID("ns", "fails")})
	}
	if _, again := timersOf(r.s.queue, k); again != n {
		t.Errorf("%d timers after twenty more failures, want %d", again, n)
	}
	// Other batches of the namespace, which succeed, don't start the backoff
	// again.
	r.s.releaseBatch(ctx, k, []ID{secretID("ns", "free")})
	if f := failuresOf(r.s.queue, k); len(f) != 1 || f[secretID("ns", "fails")] != 1 {
		t.Errorf("failures %v; want one, of the Secret that can't be read: it waited for its retry", f)
	}
	// The retry, in its time. (The released Secret's event may add it too.)
	r.clk.Step(releaseBackoffBase)
	if got := r.queued(widgetGK, "ns"); !slices.Contains(got, secretID("ns", "fails")) {
		t.Errorf("the retry's batch is %v, want what failed", got)
	}
	// Readable again, the retry releases it, and the backoff starts again.
	failing.Store(false)
	got, ids, _ := r.s.queue.Get(ctx)
	r.s.releaseBatch(ctx, got, ids)
	r.s.queue.Done(got)
	if f := failuresOf(r.s.queue, k); len(f) != 0 || hasFinalizer(r.secretMeta(t, "ns", "fails"), fWidgetNS) {
		t.Errorf("failures %v remembered, want the Secret released and none", f)
	}
}

// An object outside the caches that was counted as waiting for its users
// stays counted while it can't be read.
func TestAFailingObjectStaysCounted(t *testing.T) {
	ctx := context.Background()
	r := newFakeRigWith(t, SharedOptions{Namespaces: []string{"in"}}, namespaceObject("in"), namespaceObject("out"),
		deleting(heldObject(secretGVK, "out", "s", "u1", widgetGK, "out"), time.Now()), holderObject("out", "h", secretID("out", "s")))
	failing := r.failingGets("s")
	failing.Store(false)
	r.track(t, widgetGK)
	r.run(t)
	k, kind := batchKey{widgetGK, "out"}, kindLabel(widgetGK)
	r.s.releaseBatch(ctx, k, []ID{secretID("out", "s")})
	if n := r.metrics.waitingGauge(kind); n != 1 {
		t.Fatalf("%d waiting, want 1", n)
	}
	failing.Store(true)
	r.s.releaseBatch(ctx, k, []ID{secretID("out", "s")})
	if n := r.metrics.waitingGauge(kind); n != 1 {
		t.Errorf("%d waiting once it can't be read, want 1 still", n)
	}
}

// Nothing to look at again: no timer, and no batch of the whole namespace.
func TestAFailureAloneSetsNoOtherTimer(t *testing.T) {
	obj := heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns")
	delete(obj.Annotations, fWidgetNS) // its epoch is repaired, which asks for a recheck
	r := newFakeRig(t, namespaceObject("ns"), obj)
	r.s.hooks.wrapServer = func(srv server) server {
		return hookServer{server: srv, before: func(_ context.Context, c Call) error {
			if c.Op == "patch" {
				return unservedError()
			}
			return nil
		}}
	}
	r.track(t, widgetGK)
	r.run(t)
	r.drain()
	k := batchKey{widgetGK, "ns"}
	r.s.releaseBatch(context.Background(), k, []ID{secretID("ns", "s")})
	if retry, n := timersOf(r.s.queue, k); n != 1 || !slices.Equal(retry, []ID{secretID("ns", "s")}) {
		t.Errorf("%d timers, the retry's objects %v; want the retry alone", n, retry)
	}
}

// unwritable is a memStore whose updates fail while down.
type unwritable struct {
	*memStore
	down bool
}

func (s *unwritable) update(ctx context.Context, cur indexVersion, lines []string, mark kindSet) (indexVersion, error) {
	if s.down {
		return indexVersion{}, apierrors.NewInternalError(errors.New("etcd is unhappy"))
	}
	return s.memStore.update(ctx, cur, lines, mark)
}

// A prune that fails retries what it would have pruned, and nothing else.
func TestAFailedPruneIsRetriedAlone(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"), heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"),
		heldObject(secretGVK, "ns", "kept", "u2", widgetGK, "ns"), holderObject("ns", "h", secretID("ns", "kept")))
	tr := r.track(t, widgetGK)
	r.run(t)
	r.drain()
	st := &unwritable{memStore: &memStore{exists: true, lines: linesOf([]ID{secretID("ns", "s"), secretID("ns", "kept")})}, down: true}
	tr.idx = testIndex(st, r.s.newServer(tr.o.Holder))
	k := batchKey{widgetGK, "ns"}
	r.s.releaseBatch(context.Background(), k, []ID{secretID("ns", "s"), secretID("ns", "kept")})
	if retry, _ := timersOf(r.s.queue, k); !slices.Equal(retry, []ID{secretID("ns", "s")}) {
		t.Errorf("the retry covers %v, want the released Secret, whose line wasn't pruned", retry)
	}
}

// A prune behind a backlog of its client's rate limiter is cut, and its bare
// object retried with backoff, its line kept meanwhile; refused at once, the
// object was settled with its line back, and nothing pruned it (§10.6.2).
func TestAPruneBehindALimiterBacklogIsRetried(t *testing.T) {
	s := secretID("ns", "s")
	r := newFakeRig(t, namespaceObject("ns"), heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"))
	tr := r.track(t, widgetGK)
	r.run(t)
	r.drain()
	st := &memStore{exists: true, lines: linesOf([]ID{s})}
	tr.idx = testIndex(st, limitedServer{r.s.newServer(tr.o.Holder), backlogged(100*time.Millisecond, 20)})
	tr.idx.bound, tr.idx.checkBound = 2*time.Second, 200*time.Millisecond
	k := batchKey{widgetGK, "ns"}
	r.s.releaseBatch(context.Background(), k, []ID{s})
	if retry, _ := timersOf(r.s.queue, k); !slices.Equal(retry, []ID{s}) {
		t.Errorf("the retry covers %v, want the released Secret, whose line the cut check put back", retry)
	}
	if !slices.Equal(st.lines, linesOf([]ID{s})) {
		t.Errorf("lines %q, want the Secret's, until its retry", st.lines)
	}
}

// As the write loops stop they make the live checks the index holds in
// memory, a re-add a prune couldn't write among them, whatever their
// backoff, waiting their turn at the client's limiter, within their bound;
// what the bound leaves is logged, with how many (§10.6.3, decided
// 2026-11-25).
func TestAStopDrainsTheIndexsChecks(t *testing.T) {
	s := secretID("ns", "s")
	for name, tc := range map[string]struct {
		backlog int // calls ahead, of 100ms each
		written bool
	}{
		"behind a backlog shorter than the bound": {3, true},
		"behind one longer than the bound":        {40, false},
	} {
		t.Run(name, func(t *testing.T) {
			r := newFakeRig(t, namespaceObject("ns"), heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"), holderObject("ns", "h", s))
			w := &warnings{}
			o := r.options(widgetGK)
			o.Logger = w
			tr, err := r.s.Track(context.Background(), o)
			if err != nil {
				t.Fatal(err)
			}
			r.run(t)
			r.drain()
			st := &memStore{exists: true, rebuilt: true} // marked: the startup recheck rebuilds nothing
			lim := rate.NewLimiter(rate.Every(100*time.Millisecond), 1)
			tr.idx = testIndex(st, limitedServer{r.s.newServer(tr.o.Holder), lim})
			bound := 2 * time.Second
			r.s.hooks.drainBound = &bound
			stop := r.runWritesStarted(t)
			tr.idx.queue([]ID{s}, false)
			tr.idx.mu.Lock()
			tr.idx.pending[s].due = time.Now().Add(time.Hour) // its backoff
			tr.idx.mu.Unlock()
			for range tc.backlog {
				lim.Reserve()
			}
			start := time.Now()
			if err := stop(); err != nil {
				t.Fatal(err)
			}
			took := time.Since(start)
			if got := slices.Equal(st.lines, linesOf([]ID{s})); got != tc.written {
				t.Errorf("lines %q after the stop; want the Secret's written %v", st.lines, tc.written)
			}
			w.mu.Lock()
			said := slices.ContainsFunc(w.got, func(g string) bool { return strings.Contains(g, "left undone") && strings.Contains(g, "left 1 ") })
			w.mu.Unlock()
			if said == tc.written {
				t.Errorf("warnings %q; want one of the check left undone %v", w.got, !tc.written)
			}
			if took > bound+time.Second {
				t.Errorf("the stop took %s, past its bound of %s", took, bound)
			}
		})
	}
}

// runWritesStarted is runWrites, once the loops have started. A stop before
// then can end RunWriteLoops at its first select, which runs no loop and so
// drains nothing: a select with its context ended and the informers synced
// takes either.
func (r *fakeRig) runWritesStarted(t *testing.T) (stop func() error) {
	t.Helper()
	stop = r.runWrites(t)
	eventually(t, "the write loops to start", func() bool {
		r.s.mu.Lock()
		defer r.s.mu.Unlock()
		return r.s.writeCtx != nil
	})
	return stop
}

// drainRig is a rig whose Widgets hold Secret ns/s, recorded, with an index of
// the test's own, whose drain on a stop has bound.
func drainRig(t *testing.T, bound time.Duration) (*fakeRig, *Tracker, *memStore, *warnings) {
	t.Helper()
	r := newFakeRig(t, namespaceObject("ns"), heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"), holderObject("ns", "h", secretID("ns", "s")))
	w := &warnings{}
	o := r.options(widgetGK)
	o.Logger = w
	tr, err := r.s.Track(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	r.run(t)
	r.drain()
	st := &memStore{exists: true, rebuilt: true} // marked: the startup recheck rebuilds nothing
	tr.idx = testIndex(st, r.s.newServer(tr.o.Holder))
	r.s.hooks.drainBound = &bound
	return r, tr, st, w
}

// drainWarned reports whether w has the drain's warning, with what.
func drainWarned(w *warnings, what string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.ContainsFunc(w.got, func(g string) bool { return strings.Contains(g, "left undone") && strings.Contains(g, what) })
}

// An acquirer call begun before the stop goes on after the write loops end,
// up to callGrace, and may queue a check: the drain waits for it, and makes
// that check, its bound from when the call ended; one that outlives the
// drain's cap is said (§10.6.3, §7.2).
func TestAStopWaitsForTheCallsInFlight(t *testing.T) {
	s := secretID("ns", "s")
	t.Run("a call that queues a check after the loops ended", func(t *testing.T) {
		r, tr, st, w := drainRig(t, 500*time.Millisecond)
		stop := r.runWritesStarted(t)
		end := r.s.calls.begin()
		go func() {
			time.Sleep(800 * time.Millisecond) // past the bound from the loops' end
			tr.idx.queue([]ID{s}, false)       // its line check failed
			end()
		}()
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(st.lines, linesOf([]ID{s})) || drainWarned(w, "") {
			t.Errorf("lines %q, warnings %q; want the line written before the loops returned, and nothing left", st.lines, w.got)
		}
	})
	t.Run("a call that outlives the cap", func(t *testing.T) {
		r, _, _, w := drainRig(t, 300*time.Millisecond)
		grace := 300 * time.Millisecond
		r.s.hooks.callGrace = &grace
		stop := r.runWritesStarted(t)
		end := r.s.calls.begin()
		defer end()
		start := time.Now()
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		if took := time.Since(start); took < grace || took > 2*time.Second {
			t.Errorf("the stop took %s; want it to end at the cap of 600ms", took)
		}
		if !drainWarned(w, "callsInFlight 1 ") {
			t.Errorf("warnings %q; want one naming the call in flight", w.got)
		}
	})
	t.Run("a call begun after the stop", func(t *testing.T) {
		r, tr, _, w := drainRig(t, 300*time.Millisecond)
		grace := 2 * time.Second
		r.s.hooks.callGrace = &grace
		tr.idx.srv = &objServer{fail: map[ID]bool{s: true}} // the drain goes on
		stop := r.runWritesStarted(t)
		tr.idx.queue([]ID{s}, false)
		var mu sync.Mutex
		var end func()
		go func() {
			time.Sleep(50 * time.Millisecond)
			mu.Lock()
			end = r.s.calls.begin() // its controller wasn't stopped
			mu.Unlock()
		}()
		start := time.Now()
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		took := time.Since(start)
		mu.Lock()
		if end != nil {
			end()
		}
		mu.Unlock()
		if took > time.Second || !drainWarned(w, "left 1 ") || !drainWarned(w, "callsInFlight 0 ") {
			t.Errorf("the stop took %s, warnings %q; want it to end at its bound, with the check left and no call waited for", took, w.got)
		}
	})
}

// blockingStore is a memStore whose first update blocks until its context
// ends, as a write stuck in the API server's queue would.
type blockingStore struct {
	*memStore
	entered chan struct{}
	once    sync.Once
}

func (s *blockingStore) update(ctx context.Context, cur indexVersion, lines []string, mark kindSet) (indexVersion, error) {
	first := false
	s.once.Do(func() { first = true })
	if first {
		s.entered <- struct{}{}
		<-ctx.Done()
		return indexVersion{}, ctx.Err()
	}
	return s.memStore.update(ctx, cur, lines, mark)
}

// The drain's cap counts from the loops' end, and not from its own start: a
// prune's write still running at the stop doesn't push the stop past the cap
// (§7.2).
func TestADrainsCapCountsFromTheLoopsEnd(t *testing.T) {
	s := secretID("ns", "s")
	r := newFakeRig(t, namespaceObject("ns"), heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"))
	tr := r.track(t, widgetGK)
	r.run(t)
	st := &blockingStore{memStore: &memStore{exists: true, rebuilt: true, lines: linesOf([]ID{s})}, entered: make(chan struct{}, 1)}
	tr.idx = testIndex(st, r.s.newServer(tr.o.Holder))
	tr.idx.bound, tr.idx.checkBound = time.Second, 500*time.Millisecond
	quiet, d, grace := time.Duration(0), 300*time.Millisecond, 300*time.Millisecond
	r.s.hooks.pruneQuiet, r.s.hooks.drainBound, r.s.hooks.callGrace = &quiet, &d, &grace
	stop := r.runWrites(t)
	select {
	case <-st.entered: // the batch's prune of the released Secret is in its write
	case <-time.After(10 * time.Second):
		t.Fatal("no prune began")
	}
	end := r.s.calls.begin() // a call that outlives the cap
	defer end()
	start := time.Now()
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	// The prune's write runs on to its 500ms; the cap is at 600ms from the
	// stop, where one from the drain's start would be at about 1.1s.
	if took := time.Since(start); took < 400*time.Millisecond || took > 850*time.Millisecond {
		t.Errorf("the stop took %s; want it to end at the cap, 600ms after the loops ended", took)
	}
}

// The loops end with the informers, which a manager stops first, before the
// controllers: the drain waits for the loops' own stop, and makes what was
// queued meanwhile (§7.2).
func TestADrainAfterTheInformersEndedWaitsForTheStop(t *testing.T) {
	s := secretID("ns", "s")
	r := newFakeRig(t, namespaceObject("ns"), heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"), holderObject("ns", "h", secretID("ns", "s")))
	tr := r.track(t, widgetGK)
	stopInformers := r.run(t)
	r.drain()
	st := &memStore{exists: true, rebuilt: true} // marked: the startup recheck rebuilds nothing
	tr.idx = testIndex(st, r.s.newServer(tr.o.Holder))
	bound := 300 * time.Millisecond
	r.s.hooks.drainBound = &bound
	stopWrites := r.runWritesStarted(t)
	if err := stopInformers(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * bound) // the loops have ended, and the controllers still run
	tr.idx.queue([]ID{s}, false)
	if err := stopWrites(); err != nil && !errors.Is(err, errInformersEnded) {
		t.Fatal(err)
	}
	if !slices.Equal(st.lines, linesOf([]ID{s})) {
		t.Errorf("lines %q; want the check queued before the stop made", st.lines)
	}
}

// panicking is a server whose writes panic.
type panicking struct{ server }

func (panicking) Patch(context.Context, schema.GroupKind, string, string, types.PatchType, []byte) (*metav1.PartialObjectMetadata, error) {
	panic("the server is gone")
}

// A call is counted from its detach to its stop, however it ends: a panic and
// an early return included (§7.2).
func TestACallIsCountedWhileInFlight(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"))
	tr := r.track(t, widgetGK)
	inFlight := func() int { return r.s.calls.before(time.Now().Add(time.Second)) }
	cc := tr.proto.detach(context.Background())
	if n := inFlight(); n != 1 {
		t.Errorf("%d calls in flight during one, want 1", n)
	}
	cc.stop()
	cc.stop()
	if n := inFlight(); n != 0 {
		t.Errorf("%d calls in flight after it, stopped twice, want 0", n)
	}
	srv := tr.proto.srv
	tr.proto.srv = panicking{srv}
	panicked := func() (p bool) {
		defer func() { p = recover() != nil }()
		_, _ = tr.proto.acquire(context.Background(), holderObject("ns", "h"), []Ref{SecretRef("ns", "s")})
		return false
	}()
	tr.proto.srv = srv
	if !panicked || inFlight() != 0 {
		t.Errorf("a call that panicked (%v) leaves %d in flight, want 0", panicked, inFlight())
	}
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = tr.proto.acquire(ended, holderObject("ns", "h"), []Ref{SecretRef("ns", "s")})
	if n := inFlight(); n != 0 {
		t.Errorf("a call under an ended context leaves %d in flight, want 0", n)
	}
}

// A kind that can't be listed doesn't stop the others, in a batch that asks
// for a whole namespace outside the caches, nor in the startup recheck, which
// lists nothing twice that it could list once.
func TestAKindThatCantBeListedStopsNoOther(t *testing.T) {
	protected := []ProtectedKind{{GVK: secretGVK}, {GVK: configMapGVK}}
	unlisted := func(r *fakeRig) (on *atomic.Bool) {
		on = &atomic.Bool{}
		r.md.PrependReactor("list", "secrets", func(a clienttesting.Action) (bool, runtime.Object, error) {
			if !on.Load() || a.GetNamespace() == "in" {
				return false, nil, nil
			}
			return true, nil, unservedError()
		})
		return on
	}
	// rechecks is how many warnings of a failed recheck w has.
	rechecks := func(w *warnings) int {
		w.mu.Lock()
		defer w.mu.Unlock()
		return len(slices.DeleteFunc(slices.Clone(w.got), func(s string) bool { return !strings.Contains(s, "the recheck of the held objects") }))
	}
	configMapsGVR := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	t.Run("a batch", func(t *testing.T) {
		r := newFakeRigWith(t, SharedOptions{Namespaces: []string{"in"}}, namespaceObject("in"), namespaceObject("out"),
			heldObject(configMapGVK, "out", "c", "u1", widgetGK, "out"))
		failing := unlisted(r)
		r.track(t, widgetGK, protected...)
		r.run(t)
		failing.Store(true)
		r.drain()
		k := batchKey{widgetGK, "out"}
		r.s.releaseBatch(context.Background(), k, nil)
		o, err := r.md.Tracker().Get(configMapsGVR, "out", "c")
		if err != nil {
			t.Fatal(err)
		}
		if hasFinalizer(o.(*metav1.PartialObjectMetadata), mustFinalizer(widgetGK, "out")) {
			t.Error("the ConfigMap wasn't released: the Secrets' failed list stopped it")
		}
		if retry, _ := timersOf(r.s.queue, k); !slices.Equal(retry, []ID{fillAll}) {
			t.Errorf("the retry covers %v, want the namespace's list alone", retry)
		}
		r.clk.Step(releaseBackoffBase)
		r.next(context.Background(), k)
		if f := failuresOf(r.s.queue, k); len(f) != 1 || f[fillAll] != 2 {
			t.Errorf("failures %v; want two, of the namespace's list", f)
		}
		failing.Store(false)
		r.clk.Step(2 * releaseBackoffBase)
		r.next(context.Background(), k)
		if f := failuresOf(r.s.queue, k); len(f) != 0 {
			t.Errorf("failures %v remembered once the list succeeded", f)
		}
	})
	t.Run("the startup recheck of an index", func(t *testing.T) {
		r := newFakeRig(t, namespaceObject("ns"))
		w := &warnings{}
		o := r.options(widgetGK)
		o.Logger = w
		tr, err := r.s.Track(context.Background(), o)
		if err != nil {
			t.Fatal(err)
		}
		r.run(t)
		held, fails := secretID("ns", "held"), secretID("ns", "fails")
		srv := &objServer{objs: map[ID]*metav1.PartialObjectMetadata{held: heldObj("held")}, fail: map[ID]bool{fails: true}}
		tr.idx = testIndex(&memStore{exists: true, rebuilt: true, lines: linesOf([]ID{fails, held})}, srv)
		ctx, cancel := context.WithCancel(context.Background())
		ended := make(chan struct{})
		go func() { defer close(ended); r.s.recheckUncached(ctx, []*Tracker{tr}) }()
		eventually(t, "four tries", func() bool { r.clk.Step(releaseBackoffMax); return rechecks(w) >= 4 })
		cancel()
		<-ended
		heldReads := len(slices.DeleteFunc(slices.Clone(srv.gets), func(id ID) bool { return id != held }))
		if heldReads != 1 || len(srv.gets) < 5 {
			t.Errorf("read %v; want the held one once, and the one that fails at every try", srv.gets)
		}
	})
	t.Run("the startup recheck", func(t *testing.T) {
		r := newFakeRigWith(t, SharedOptions{Namespaces: []string{"in"}}, namespaceObject("in"), namespaceObject("out"),
			deleting(heldObject(configMapGVK, "out", "c", "u1", widgetGK, "out"), time.Now().Add(-time.Hour)))
		w := &warnings{}
		o := r.options(widgetGK, protected...)
		o.Logger = w
		failing := unlisted(r)
		if _, err := r.s.Track(context.Background(), o); err != nil {
			t.Fatal(err)
		}
		r.run(t)
		failing.Store(true)
		lists := func(resource string) int {
			n := 0
			for _, a := range r.md.Actions() {
				if l, ok := a.(clienttesting.ListAction); ok && a.GetResource().Resource == resource && a.GetNamespace() == "" && !l.GetListRestrictions().Labels.Empty() {
					n++
				}
			}
			return n
		}
		configMaps, secrets := lists("configmaps"), lists("secrets")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ended := make(chan struct{})
		go func() { defer close(ended); r.s.recheckUncached(ctx, r.s.trackerList()) }()
		// Each try warns of each of the two lists that fail.
		eventually(t, "three tries", func() bool { r.clk.Step(releaseBackoffMax); return rechecks(w) >= 6 })
		if n := lists("configmaps") - configMaps; n != 2 {
			t.Errorf("%d lists of ConfigMaps, want two, the markers' and the held ones': they didn't fail", n)
		}
		if n := lists("secrets") - secrets; n < 6 {
			t.Errorf("%d lists of Secrets after three tries, want two a try", n)
		}
		if got := r.queued(widgetGK, "out"); !slices.Contains(got, ID{Kind: "ConfigMap", Namespace: "out", Name: "c"}) {
			t.Errorf("the queued batch is %v, want the ConfigMap's", got)
		}
		// The kind served again, the recheck ends.
		failing.Store(false)
		eventually(t, "the recheck to end", func() bool { r.clk.Step(releaseBackoffMax); return isClosed(ended) })
	})
}

// The startup recheck of an index reads again only the objects it couldn't
// read, and prunes again only what it couldn't prune.
func TestTheIndexsRecheckRetriesWhatFailed(t *testing.T) {
	ctx := context.Background()
	id := func(n string) ID { return SecretRef("ns", n).ID() }
	held, fails, bare, denied, vouched := id("held"), id("fails"), id("bare"), id("denied"), id("vouched")
	named := id("named") // bare too, but a record names it, so its line stays
	srv := &objServer{objs: map[ID]*metav1.PartialObjectMetadata{held: heldObj("held"), fails: heldObj("fails")},
		fail: map[ID]bool{fails: true, vouched: true}, forbid: map[ID]bool{denied: true}}
	st := &unwritable{memStore: &memStore{exists: true, rebuilt: true, lines: linesOf([]ID{bare, denied, fails, held, named, vouched}), vouched: unheldRef(vouched)}}
	ix := testIndex(st, srv)
	ix.recorded = func(id ID) bool { return id == named }
	done := map[ID]bool{}
	var enqueued []ID
	try := func() (gets []ID, err error) {
		srv.gets, enqueued = nil, nil
		err = ix.recheckRest(ctx, done, func(id ID, _ string) { enqueued = append(enqueued, id) })
		return SortedIDs(slices.Clone(srv.gets)), err
	}
	// The prune fails too.
	st.down = true
	if _, err := try(); err == nil || !slices.Equal(enqueued, []ID{held}) {
		t.Fatalf("the first try enqueued %v, %v; want the held one, and the failures", enqueued, err)
	}
	st.down = false
	gets, err := try()
	if err == nil || len(enqueued) != 0 || slices.Contains(gets, held) || slices.Contains(gets, denied) || slices.Contains(gets, vouched) || !slices.Contains(gets, fails) || !slices.Contains(gets, bare) {
		t.Errorf("the second try read %v and enqueued %v, %v; want what failed, and what wasn't pruned", gets, enqueued, err)
	}
	if slices.Contains(st.lines, bare.String()) || !slices.Contains(st.lines, named.String()) {
		t.Errorf("lines %q; want the bare object's gone, and the one a record names", st.lines)
	}
	srv.fail[fails] = false
	gets, err = try()
	if err != nil || !slices.Equal(gets, []ID{fails}) || !slices.Equal(enqueued, []ID{fails}) {
		t.Errorf("the third try read %v and enqueued %v, %v; want the one that had failed", gets, enqueued, err)
	}
}

// The rules the review of 9db7909 and the release-cost proposal asked for, by
// what the queue holds at which time.

// failWhile fails the GETs of the Secrets that fails says.
func (r *fakeRig) failWhile(fails func(name string) bool) {
	r.s.hooks.wrapServer = func(srv server) server {
		return hookServer{server: srv, before: func(_ context.Context, c Call) error {
			if c.Op == "get" && c.GK == secretGroupKind && fails(c.Name) {
				return unservedError()
			}
			return nil
		}}
	}
}

// next takes what is queued for k, runs it, and returns it.
func (r *fakeRig) next(ctx context.Context, k batchKey) []ID {
	ids := r.queued(k.Holder, k.Namespace)
	if ids == nil {
		return nil
	}
	got, ids, _ := r.s.queue.Get(ctx)
	r.s.releaseBatch(ctx, got, ids)
	r.s.queue.Done(got)
	return ids
}

// outside is a rig whose namespace "out" no cache covers, so that nothing but
// batches and their timers queues there.
func outside(t *testing.T, objs ...runtime.Object) (*fakeRig, batchKey) {
	t.Helper()
	r := newFakeRigWith(t, SharedOptions{Namespaces: []string{"in"}}, append(objs, namespaceObject("in"), namespaceObject("out"))...)
	return r, batchKey{widgetGK, "out"}
}

// One object that fails for good doesn't put another's first failure off: it
// is retried after 1s, however long the other's backoff is.
func TestAnHonestFailureBesideAPermanentOne(t *testing.T) {
	ctx := context.Background()
	r, k := outside(t, heldObject(secretGVK, "out", "honest", "u1", widgetGK, "out"), heldObject(secretGVK, "out", "forged", "u2", widgetGK, "out"))
	honestFails := true
	r.failWhile(func(name string) bool { return name == "forged" || name == "honest" && honestFails })
	r.track(t, widgetGK)
	r.run(t)
	r.drain()
	forged, honest := secretID("out", "forged"), secretID("out", "honest")
	r.s.releaseBatch(ctx, k, []ID{forged})
	for range 11 { // to the longest backoff
		r.clk.Step(releaseBackoffMax)
		if got := r.next(ctx, k); !slices.Equal(got, []ID{forged}) {
			t.Fatalf("the retry's batch is %v", got)
		}
	}
	r.clk.Step(time.Minute)
	r.s.releaseBatch(ctx, k, []ID{forged, honest})
	r.clk.Step(releaseBackoffBase)
	if got := r.queued(widgetGK, "out"); !slices.Equal(got, []ID{honest}) {
		t.Fatalf("after 1s the batch is %v, want the honest one alone", got)
	}
	r.next(ctx, k)
	r.clk.Step(2*releaseBackoffBase - 1)
	if got := r.queued(widgetGK, "out"); got != nil {
		t.Errorf("before its second backoff ends, the batch is %v", got)
	}
	honestFails = false
	r.clk.Step(1)
	if got := r.next(ctx, k); !slices.Equal(got, []ID{honest}) || r.heldBy(t, widgetGK, "out", "honest") {
		t.Errorf("the second retry's batch is %v, want the honest one, released", got)
	}
	// The other's retry is where it was: 5 minutes after its last.
	r.clk.Step(releaseBackoffMax - time.Minute - 3*releaseBackoffBase - 1)
	if got := r.queued(widgetGK, "out"); got != nil {
		t.Errorf("before its backoff ends, the batch is %v", got)
	}
	r.clk.Step(1)
	if got := r.queued(widgetGK, "out"); !slices.Equal(got, []ID{forged}) {
		t.Errorf("the batch is %v, want the other's retry", got)
	}
}

// A grace's end never waits for a backoff: not its own namespace's, nor that of
// the very batch that asks for it.
func TestAGracesEndBesideAPermanentFailure(t *testing.T) {
	ctx := context.Background()
	r, k := outside(t, holderObject("out", "h", secretID("out", "grace")))
	r.failWhile(func(name string) bool { return name == "forged" })
	r.setRefs("h", SecretRef("out", "grace"))
	r.track(t, widgetGK)
	r.run(t)
	r.drain()
	forged, grace := secretID("out", "forged"), secretID("out", "grace")
	r.s.releaseBatch(ctx, k, []ID{forged})
	for range 11 {
		r.clk.Step(releaseBackoffMax)
		r.next(ctx, k)
	}
	if err := r.md.Tracker().Add(deleting(heldObject(secretGVK, "out", "grace", "u1", widgetGK, "out"), r.clk.Now())); err != nil {
		t.Fatal(err)
	}
	r.s.releaseBatch(ctx, k, []ID{forged, grace})
	r.clk.Step(DefaultDeletionGrace - 1)
	if got := r.queued(widgetGK, "out"); got != nil {
		t.Errorf("inside the grace the batch is %v", got)
	}
	r.clk.Step(1)
	if got := r.next(ctx, k); !slices.Equal(got, []ID{grace}) || r.heldBy(t, widgetGK, "out", "grace") {
		t.Errorf("at the grace's end the batch is %v, want the object, released", got)
	}
}

// A holder edited to name one object after another that can't be read: each is
// retried on its own backoff, and one named long ago isn't retried sooner.
func TestFreshNamesThatKeepFailing(t *testing.T) {
	ctx := context.Background()
	r, k := outside(t)
	r.failWhile(func(string) bool { return true })
	r.track(t, widgetGK)
	r.run(t)
	r.drain()
	old := secretID("out", "old")
	r.s.releaseBatch(ctx, k, []ID{old})
	oldRuns := 0
	for i := range 600 { // ten minutes, one fresh name a second
		r.s.releaseBatch(ctx, k, []ID{secretID("out", fmt.Sprintf("n%d", i))})
		r.clk.Step(time.Second)
		for got := r.next(ctx, k); got != nil; got = r.next(ctx, k) {
			if slices.Contains(got, old) {
				oldRuns++
			}
		}
	}
	// 1, 2, 4, ..., 256: nine within 511s, and the next 300s later.
	if oldRuns != 9 {
		t.Errorf("the old name was retried %d times in ten minutes, want 9", oldRuns)
	}
}

// What an event runs meanwhile isn't a retry: it doesn't lengthen the backoff.
func TestEventsDontLengthenTheBackoff(t *testing.T) {
	ctx := context.Background()
	r, k := outside(t)
	r.failWhile(func(string) bool { return true })
	r.track(t, widgetGK)
	r.run(t)
	r.drain()
	id := secretID("out", "s")
	for _, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second} {
		for range 10 {
			r.s.releaseBatch(ctx, k, []ID{id})
		}
		r.clk.Step(want - 1)
		if got := r.queued(widgetGK, "out"); got != nil {
			t.Fatalf("before %s the batch is %v", want, got)
		}
		r.clk.Step(1)
		if got := r.queued(widgetGK, "out"); !slices.Equal(got, []ID{id}) {
			t.Fatalf("after %s the batch is %v, want the retry", want, got)
		}
		// The worker takes it, and it fails again below.
		got, _, _ := r.s.queue.Get(ctx)
		r.s.queue.Done(got)
	}
}

// A grace's end runs what asked for it again, not the rest of its batch.
func TestOnlyWhatAskedIsLookedAtAgain(t *testing.T) {
	ctx := context.Background()
	r, k := outside(t, deleting(heldObject(secretGVK, "out", "grace", "u1", widgetGK, "out"), time.Now()),
		heldObject(secretGVK, "out", "used", "u2", widgetGK, "out"),
		holderObject("out", "h", secretID("out", "grace"), secretID("out", "used")))
	r.track(t, widgetGK)
	r.run(t)
	r.drain()
	r.s.releaseBatch(ctx, k, []ID{secretID("out", "free"), secretID("out", "grace"), secretID("out", "used")})
	r.clk.Step(DefaultDeletionGrace)
	if got := r.queued(widgetGK, "out"); !slices.Equal(got, []ID{secretID("out", "grace")}) {
		t.Errorf("at the grace's end the batch is %v, want the object in its grace alone", got)
	}
	// Two graces, each at its own end.
	r, k = outside(t, deleting(heldObject(secretGVK, "out", "g1", "u1", widgetGK, "out"), time.Now().Add(-2*time.Second)),
		deleting(heldObject(secretGVK, "out", "g2", "u2", widgetGK, "out"), time.Now()),
		holderObject("out", "h", secretID("out", "g1"), secretID("out", "g2")))
	r.track(t, widgetGK)
	r.run(t)
	r.drain()
	// A grace begins when the deletion is first seen.
	proto := r.s.trackerList()[0].proto
	if _, err := proto.release(ctx, "out", []ID{secretID("out", "g1")}); err != nil {
		t.Fatal(err)
	}
	r.clk.Step(2 * time.Second)
	res, err := proto.release(ctx, "out", []ID{secretID("out", "g1"), secretID("out", "g2")})
	if err != nil || len(res.Asked) != 2 || !res.Asked[0].At.Before(res.Asked[1].At) || !res.RequeueAt.Equal(res.Asked[0].At) {
		t.Fatalf("asked %+v, the soonest %s, %v; want each at its own time", res.Asked, res.RequeueAt, err)
	}
	r.s.releaseBatch(ctx, k, []ID{secretID("out", "g1"), secretID("out", "g2")})
	r.clk.Step(res.Asked[0].At.Sub(r.clk.Now()))
	if got := r.next(ctx, k); !slices.Equal(got, []ID{secretID("out", "g1")}) {
		t.Errorf("at the first grace's end the batch is %v, want its object alone", got)
	}
	r.clk.Step(res.Asked[1].At.Sub(r.clk.Now()))
	if got := r.next(ctx, k); !slices.Equal(got, []ID{secretID("out", "g2")}) {
		t.Errorf("at the second grace's end the batch is %v, want its object alone", got)
	}
	// A repaired epoch's recheck too.
	o := heldObject(secretGVK, "out", "epoch", "u3", widgetGK, "out")
	delete(o.Annotations, mustFinalizer(widgetGK, "out"))
	r, k = outside(t, o, heldObject(secretGVK, "out", "used", "u2", widgetGK, "out"), holderObject("out", "h", secretID("out", "used")))
	r.track(t, widgetGK)
	r.run(t)
	r.drain()
	r.s.releaseBatch(ctx, k, []ID{secretID("out", "epoch"), secretID("out", "used")})
	r.clk.Step(restoredEpochRequeue)
	if got := r.queued(widgetGK, "out"); !slices.Equal(got, []ID{secretID("out", "epoch")}) {
		t.Errorf("at the repaired epoch's recheck the batch is %v, want its object alone", got)
	}
}

// Batches that find an object in its grace again and again, as events run them,
// set one timer for its end.
func TestAGracesTimersDontAddUp(t *testing.T) {
	ctx := context.Background()
	r, k := outside(t, deleting(heldObject(secretGVK, "out", "g1", "u1", widgetGK, "out"), time.Now()),
		deleting(heldObject(secretGVK, "out", "g2", "u2", widgetGK, "out"), time.Now()),
		holderObject("out", "h", secretID("out", "g1"), secretID("out", "g2")))
	r.track(t, widgetGK)
	r.run(t)
	r.drain()
	for range 10 {
		r.s.releaseBatch(ctx, k, []ID{secretID("out", "g1"), secretID("out", "g2")})
		r.s.releaseBatch(ctx, k, []ID{secretID("out", "g2")})
	}
	if n := r.queueTimers(); n != 1 {
		t.Errorf("%d timers after twenty batches, want one, of both objects", n)
	}
	r.clk.Step(DefaultDeletionGrace)
	if got := r.queued(widgetGK, "out"); !slices.Equal(got, []ID{secretID("out", "g1"), secretID("out", "g2")}) || r.queueTimers() != 0 {
		t.Errorf("at the grace's end the batch is %v, with %d timers; want both, and none", got, r.queueTimers())
	}
	// Past it, the same time asked for again is now.
	r.drain()
	r.s.queue.AddAt(k, r.clk.Now().Add(-time.Second), secretID("out", "g1"))
	if got := r.queued(widgetGK, "out"); !slices.Equal(got, []ID{secretID("out", "g1")}) {
		t.Errorf("a time that is past: the batch is %v, want its object at once", got)
	}
	r.drain()
	r.s.queue.AddAt(k, r.clk.Now().Add(time.Second))
	r.s.queue.AddAt(k, r.clk.Now())
	if r.queueTimers() != 0 || r.s.queue.Len() != 0 {
		t.Error("a time asked for by no object did something")
	}
	// What is armed is forgotten with its timer.
	r.s.queue.AddAt(k, r.clk.Now().Add(time.Second), secretID("out", "g1"))
	r.clk.Step(time.Second)
	if len(r.s.queue.asked) != 0 {
		t.Errorf("%d times remembered after their timers", len(r.s.queue.asked))
	}
	r.s.queue.AddAt(k, r.clk.Now().Add(time.Second), secretID("out", "g1"))
	r.s.queue.ShutDown()
	if len(r.s.queue.asked) != 0 || r.queueTimers() != 0 {
		t.Errorf("%d times remembered after ShutDown", len(r.s.queue.asked))
	}
	r.s.queue.AddAt(k, r.clk.Now().Add(time.Second), secretID("out", "g1"))
	if r.queueTimers() != 0 {
		t.Error("an AddAt after ShutDown set a timer")
	}
}

// The timer of one object's time is no other object's, time's or key's.
func TestATimeIsItsObjects(t *testing.T) {
	clk := testingclock.NewFakeClock(time.Now())
	q := newBatchQueue(nil)
	q.clock = &unlockedClock{FakeClock: clk, t: t, q: q}
	a, b, other := idOf("a"), idOf("b"), batchKey{widgetGK, "other"}
	soon, later := clk.Now().Add(time.Second), clk.Now().Add(2*time.Second)
	for range 3 {
		q.AddAt(batchNS, soon, a)
		q.AddAt(batchNS, soon, a, b)
		q.AddAt(batchNS, later, a)
		q.AddAt(other, soon, a)
	}
	if _, n := timersOf(q, batchNS); n != 4 {
		t.Fatalf("%d timers, want four: a's, b's, a's later one, and the other key's", n)
	}
	got := map[batchKey][]ID{}
	take := func() {
		for q.Len() > 0 {
			k, ids, _ := q.Get(context.Background())
			got[k] = append(got[k], ids...)
			q.Done(k)
		}
	}
	clk.Step(time.Second)
	take()
	if !slices.Equal(got[batchNS], []ID{a, b}) || !slices.Equal(got[other], []ID{a}) {
		t.Errorf("at the first time, %v", got)
	}
	clk.Step(time.Second)
	take()
	if !slices.Equal(got[batchNS], []ID{a, b, a}) || len(got[other]) != 1 {
		t.Errorf("at the second time, %v", got)
	}
}

// The write loops' stop, under a batch that decided some: the next ones get
// what failed, at once, and no more; and the backoff isn't counted.
func TestAStoppedBatchLeavesWhatFailed(t *testing.T) {
	r, k := outside(t, heldObject(secretGVK, "out", "free", "u1", widgetGK, "out"), heldObject(secretGVK, "out", "fails", "u2", widgetGK, "out"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.failWhile(func(name string) bool {
		if name == "fails" {
			cancel() // the loops stop under this read
		}
		return name == "fails"
	})
	r.track(t, widgetGK)
	r.run(t)
	r.drain()
	r.s.releaseBatch(ctx, k, []ID{secretID("out", "free"), secretID("out", "fails")})
	if got := r.queued(widgetGK, "out"); !slices.Equal(got, []ID{secretID("out", "fails")}) {
		t.Errorf("the next write loops' batch is %v, want what failed", got)
	}
	if n := r.queueTimers(); n != 0 {
		t.Errorf("%d timers, want none", n)
	}
	if r.heldBy(t, widgetGK, "out", "free") {
		t.Error("what could be decided wasn't")
	}
}

// A batch that fails at no object, for a namespace that can't be one, queues
// nothing: not its whole namespace.
func TestABatchThatFailsAtNothing(t *testing.T) {
	r := newFakeRig(t)
	w := &warnings{}
	o := r.options(widgetGK)
	o.Logger = w
	if _, err := r.s.Track(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	r.run(t)
	r.drain()
	stopped, cancel := context.WithCancel(context.Background())
	cancel()
	for _, ctx := range []context.Context{context.Background(), stopped} {
		r.s.releaseBatch(ctx, batchKey{widgetGK, "Not A Namespace"}, nil)
		if r.s.queue.Len() != 0 || r.queueTimers() != 0 || len(r.s.queue.failing) != 0 {
			t.Errorf("stopped %v: %d batches, %d timers, %d keys with failures; want none", ctx.Err() != nil, r.s.queue.Len(), r.queueTimers(), len(r.s.queue.failing))
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.got) != 1 || !strings.Contains(w.got[0], "never") {
		t.Errorf("warnings %q; want one, that says it isn't retried", w.got)
	}
}

// An error that forgets to say which objects it left undecided loses none of
// them: the whole batch is retried.
func TestAnErrorThatNamesNoObjectLosesNone(t *testing.T) {
	a, b := secretID("out", "a"), secretID("out", "b")
	for _, stopped := range []bool{false, true} {
		r, k := outside(t, heldObject(secretGVK, "out", "a", "u1", widgetGK, "out"), heldObject(secretGVK, "out", "b", "u2", widgetGK, "out"))
		ctx, cancel := context.WithCancel(context.Background())
		r.failWhile(func(name string) bool { return name == "a" })
		r.s.hooks.released = func(res *ReleaseResult) {
			res.Failed = nil
			if stopped {
				cancel()
			}
		}
		r.track(t, widgetGK)
		r.run(t)
		r.drain()
		r.s.releaseBatch(ctx, k, []ID{a, b})
		retry, n := timersOf(r.s.queue, k)
		queued := r.queued(widgetGK, "out")
		if !stopped && (!slices.Equal(retry, []ID{a, b}) || n != 1 || queued != nil) {
			t.Errorf("%v waits for a retry, with %d timers, and %v is queued; want the whole batch to wait", retry, n, queued)
		}
		if stopped && (n != 0 || !slices.Equal(queued, []ID{a, b})) {
			t.Errorf("stopped: %d timers, and %v is queued; want the whole batch queued", n, queued)
		}
		if !stopped {
			// The second round's delay is the second's: the count isn't reset.
			r.clk.Step(releaseBackoffBase)
			if got := r.next(ctx, k); !slices.Equal(got, []ID{a, b}) {
				t.Errorf("the first retry's batch is %v", got)
			}
			r.clk.Step(2*releaseBackoffBase - 1)
			if got := r.queued(widgetGK, "out"); got != nil {
				t.Errorf("before its second delay ends, the batch is %v", got)
			}
			r.clk.Step(1)
			if got := r.queued(widgetGK, "out"); !slices.Equal(got, []ID{a, b}) {
				t.Errorf("after its second delay, the batch is %v, want both", got)
			}
		}
		cancel()
	}
}

// Objects left undecided without an error are retried too, and the log says
// that nothing says why.
func TestWhatFailedWithoutAnErrorIsRetried(t *testing.T) {
	a, b := secretID("out", "a"), secretID("out", "b")
	for _, stopped := range []bool{false, true} {
		r, k := outside(t)
		ctx, cancel := context.WithCancel(context.Background())
		r.s.hooks.released = func(res *ReleaseResult) {
			res.Failed = []ID{a}
			if stopped {
				cancel()
			}
		}
		w := &warnings{}
		o := r.options(widgetGK)
		o.Logger = w
		if _, err := r.s.Track(context.Background(), o); err != nil {
			t.Fatal(err)
		}
		r.run(t)
		r.drain()
		r.s.releaseBatch(ctx, k, []ID{a, b})
		retry, n := timersOf(r.s.queue, k)
		queued := r.queued(widgetGK, "out")
		w.mu.Lock()
		said := len(w.got) == 1 && strings.Contains(w.got[0], "undecided")
		w.mu.Unlock()
		if !stopped && (!slices.Equal(retry, []ID{a}) || n != 1 || queued != nil || !said) {
			t.Errorf("%v waits for a retry, with %d timers, %v is queued, said %v; want the one that failed to wait, and said", retry, n, queued, said)
		}
		if stopped && (n != 0 || !slices.Equal(queued, []ID{a})) {
			t.Errorf("stopped: %d timers, and %v is queued; want the one that failed queued", n, queued)
		}
		cancel()
	}
}

// Objects an error doesn't name back off as those one names do: they aren't
// settled first, which would start their backoff again at every failure.
func TestAnErrorThatNamesNoObjectBacksOff(t *testing.T) {
	ctx := context.Background()
	for _, named := range []bool{true, false} {
		r, k := outside(t)
		r.failWhile(func(string) bool { return true })
		if !named {
			r.s.hooks.released = func(res *ReleaseResult) { res.Failed = nil }
		}
		r.track(t, widgetGK)
		r.run(t)
		r.drain()
		a, b := secretID("out", "a"), secretID("out", "b")
		r.s.releaseBatch(ctx, k, []ID{a, b})
		for n, d := range []time.Duration{1, 2, 4, 8, 16} {
			if f := failuresOf(r.s.queue, k); f[a] != n+1 || f[b] != n+1 {
				t.Errorf("named %v: failures %v, want %d of each", named, f, n+1)
			}
			r.clk.Step(d*time.Second - 1)
			if got := r.queued(widgetGK, "out"); got != nil {
				t.Errorf("named %v: before %ds the batch is %v", named, d, got)
			}
			r.clk.Step(1)
			if got := r.next(ctx, k); !slices.Equal(got, []ID{a, b}) {
				t.Errorf("named %v: after %ds the batch is %v, want both retried", named, d, got)
			}
		}
	}
}

// A batch that fails at many objects says how many, and why the first few did, in
// each of its turns.
func TestAFailedBatchsLogIsBounded(t *testing.T) {
	ctx := context.Background()
	r, k := outside(t)
	r.failWhile(func(string) bool { return true })
	w := &warnings{}
	o := r.options(widgetGK)
	o.Logger = w
	if _, err := r.s.Track(ctx, o); err != nil {
		t.Fatal(err)
	}
	r.run(t)
	r.drain()
	var ids []ID
	for i := range 500 {
		ids = append(ids, secretID("out", fmt.Sprintf("a-name-of-some-length-%03d", i)))
	}
	// One line a turn, of 64 objects at most.
	r.s.queue.Add(k, ids...)
	turns := 0
	for range ids {
		if _, ok := r.turn(ctx); !ok {
			break
		}
		turns++
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if turns != 8 || len(w.got) != 8 {
		t.Fatalf("%d warnings in %d turns, want one in each of 8", len(w.got), turns)
	}
	for i, line := range w.got {
		objects, more := 64, 61
		if i == 7 {
			objects, more = 52, 49
		}
		if len(line) > 4096 || !strings.Contains(line, fmt.Sprintf("objects %d ", objects)) || !strings.Contains(line, fmt.Sprintf("a-name-of-some-length-%03d", 64*i)) || strings.Contains(line, fmt.Sprintf("a-name-of-some-length-%03d", 64*i+40)) {
			t.Errorf("warning %d of %d bytes: %q; want it of %d objects, with the first few errors", i, len(line), line, objects)
		}
		if !strings.Contains(line, fmt.Sprintf("and %d more", more)) {
			t.Errorf("%q doesn't say how many more there are", line)
		}
	}
	if got := errorSample(errors.New(strings.Repeat("é", 5000))); len(got) > maxLoggedBytes+3 || !strings.HasSuffix(got, "é...") {
		t.Errorf("a long error is cut to %d bytes, ending %q", len(got), got[max(0, len(got)-8):])
	}
	if got := errorSample(joined([]error{errors.New("a"), errors.New("b"), errors.New("c")})); got != "a\nb\nc" {
		t.Errorf("a short error is %q", got)
	}
}

func isClosed(c chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}
