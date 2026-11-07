package ratelimit

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/client-go/util/workqueue"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/controller/priorityqueue"

	"github.com/krateo-platformops/provider-runtime/pkg/logging"
)

// popped is what a test worker took from a gated queue.
type popped struct {
	key      string
	priority int
}

// workers runs n workers on q, the way controller-runtime's do: each pops and
// reports what it popped, and the test calls Done for it. A worker exits when
// the queue shuts down; the returned func waits for all of them.
func workers(q priorityqueue.PriorityQueue[string], n int) (<-chan popped, func()) {
	out := make(chan popped, 1024)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			for {
				key, priority, shutdown := q.GetWithPriority()
				if shutdown {
					return
				}
				out <- popped{key, priority}
			}
		})
	}
	return out, wg.Wait
}

// heldItem is an item a bound worker popped and is processing: like a
// controller-runtime worker, it calls Done itself, when the test closes release,
// and closes done after.
type heldItem struct {
	popped
	release, done chan struct{}
}

// boundWorkers runs n workers on q that each process one item at a time, as
// controller-runtime's do: n bounds the reconciles in flight.
func boundWorkers(q priorityqueue.PriorityQueue[string], n int) (<-chan heldItem, func()) {
	out := make(chan heldItem, 1024)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			for {
				key, priority, shutdown := q.GetWithPriority()
				if shutdown {
					return
				}
				h := heldItem{popped{key, priority}, make(chan struct{}), make(chan struct{})}
				out <- h
				<-h.release
				q.Done(key)
				close(h.done)
			}
		})
	}
	return out, wg.Wait
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within 5s", what)
		}
	}
}

func receive(t *testing.T, ch <-chan popped) popped {
	t.Helper()
	select {
	case p := <-ch:
		return p
	case <-time.After(5 * time.Second):
		t.Fatal("no item was popped within 5s")
		return popped{}
	}
}

func none(t *testing.T, ch <-chan popped) {
	t.Helper()
	select {
	case p := <-ch:
		t.Fatalf("%s was popped, want nothing", p.key)
	case <-time.After(50 * time.Millisecond):
	}
}

func (g *Governor) inFlight() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.localInFlight
}

// setLocalCapacity stands in for the sampler.
func (g *Governor) setLocalCapacity(c int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.local.Store(&localState{capacity: c})
	g.dispatchLocked()
}

func gated(t *testing.T, g *Governor, name string) *gatedQueue[string] {
	t.Helper()
	q := newGatedQueue[string](g, name, nil, clocktesting.NewFakeClock(time.Now()))
	t.Cleanup(q.ShutDown)
	return q
}

func priority(p int) priorityqueue.AddOpts { return priorityqueue.AddOpts{Priority: &p} }

// A worker pops only with a local slot: capacity 1 and three workers pop one
// item at a time, and Done hands the slot to the next.
func TestGatedQueuePopsOnlyWithALocalSlot(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	q := gated(t, g, "one-slot")
	got, _ := workers(q, 3)
	q.Add("a")
	q.Add("b")
	q.Add("c")
	first := receive(t, got)
	none(t, got)
	if n := g.inFlight(); n != 1 {
		t.Fatalf("%d local slots in flight, want 1", n)
	}
	q.Done(first.key)
	second := receive(t, got)
	none(t, got)
	q.Done(second.key)
	q.Done(receive(t, got).key)
	eventually(t, "every slot given back", func() bool { return g.inFlight() == 0 })
}

// When the slot frees, the highest-priority item runs next, ahead of
// lower-priority items queued before it.
func TestGatedQueueHandsTheFreedSlotToTheHighestPriority(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	q := gated(t, g, "priority")
	got, _ := workers(q, 4)
	q.Add("busy")
	busy := receive(t, got)
	for _, k := range []string{"low-a", "low-b", "low-c"} {
		q.AddWithOpts(priority(-100), k)
	}
	q.AddWithOpts(priority(100), "high")
	none(t, got)
	q.Done(busy.key)
	if p := receive(t, got); p.key != "high" || p.priority != 100 {
		t.Errorf("after the slot freed %s (priority %d) was popped, want high at 100", p.key, p.priority)
	}
}

// A rise in local capacity wakes a waiting worker at once.
func TestCapacityRiseWakesAWaitingWorker(t *testing.T) {
	g, c := saturationGovernor(t, 4, "healthy", false)
	q := gated(t, g, "rise")
	got, _ := workers(q, 2)
	q.Add("a")
	q.Add("b")
	receive(t, got)
	none(t, got)
	g.sample() // starts the ramp's clock
	c.add(DefaultStartupRampInterval)
	g.sample()
	if g.LocalCapacity() != 2 {
		t.Fatalf("local capacity = %d, want 2", g.LocalCapacity())
	}
	receive(t, got)
}

// Idle workers hold nothing: one queue's four idle workers don't stop another
// queue's item, with capacity 1.
func TestIdleWorkersHoldNoLocalSlot(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	idle := gated(t, g, "idle")
	busy := gated(t, g, "busy")
	workers(idle, 4)
	got, _ := workers(busy, 1)
	eventually(t, "the idle queue's workers waiting", func() bool {
		idle.mu.Lock()
		defer idle.mu.Unlock()
		return idle.idle == 4
	})
	busy.Add("b")
	if p := receive(t, got); p.key != "b" {
		t.Errorf("popped %s, want b", p.key)
	}
	if n := g.inFlight(); n != 1 {
		t.Errorf("%d local slots in flight, want 1", n)
	}
}

func idleWorkers(q *gatedQueue[string]) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.idle
}

// Across queues with an idle worker and work, freed slots go round-robin.
func TestFreedSlotsGoRoundRobinAcrossQueues(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	a, b := gated(t, g, "rr-a"), gated(t, g, "rr-b")
	gotA, _ := workers(a, 1)
	gotB, _ := workers(b, 1)
	bothIdle := func() bool { return idleWorkers(a) == 1 && idleWorkers(b) == 1 }
	eventually(t, "both queues' workers waiting", bothIdle)
	for i := range 3 {
		a.Add("a" + strconv.Itoa(i))
		b.Add("b" + strconv.Itoa(i))
	}
	var order []string
	queueOf := map[string]*gatedQueue[string]{}
	for range 6 {
		var p popped
		select {
		case p = <-gotA:
			queueOf[p.key] = a
		case p = <-gotB:
			queueOf[p.key] = b
		case <-time.After(5 * time.Second):
			t.Fatalf("stuck after %v", order)
		}
		order = append(order, p.key[:1])
		// Both queues are eligible again before the slot frees.
		eventually(t, "both queues' workers waiting", bothIdle)
		queueOf[p.key].Done(p.key)
	}
	for i := 1; i < len(order); i++ {
		if order[i] == order[i-1] {
			t.Fatalf("grant order %v: a queue got two slots in a row while the other had work", order)
		}
	}
}

// Done runs on every exit path, panics included: controller-runtime defers it
// in processNextWorkItem, so a panic unwinding the reconcile gives the slot back.
func TestAPanickingReconcileGivesItsSlotBack(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	q := gated(t, g, "panic")
	q.Add("a")
	q.Add("b")
	first := make(chan string, 1)
	go func() { key, _, _ := q.GetWithPriority(); first <- key }()
	var key string
	select {
	case key = <-first:
	case <-time.After(5 * time.Second):
		t.Fatal("no item was popped within 5s")
	}
	func() {
		defer func() { _ = recover() }()
		defer q.Done(key)
		panic("provider bug")
	}()
	got, _ := workers(q, 1)
	if p := receive(t, got); p.key != "b" {
		t.Errorf("popped %s, want b", p.key)
	}
}

// Done for a key no worker holds, or a second Done, gives back no slot.
func TestDoneGivesBackOneSlotPerPop(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	g.setLocalCapacity(2)
	q := gated(t, g, "done-once")
	got, _ := workers(q, 2)
	q.Add("a")
	q.Add("b")
	first, second := receive(t, got), receive(t, got)
	q.Done(first.key)
	q.Done(first.key)
	q.Done("never-popped")
	if n := g.inFlight(); n != 1 {
		t.Errorf("%d local slots in flight, want 1, %s's", n, second.key)
	}
}

// A key re-added while it is processed waits for its Done, then runs again.
func TestReAddWhileProcessingRunsAfterDone(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	g.setLocalCapacity(2)
	q := gated(t, g, "re-add")
	got, _ := workers(q, 2)
	q.Add("a")
	first := receive(t, got)
	q.Add("a")
	none(t, got)
	q.Done(first.key)
	if p := receive(t, got); p.key != "a" {
		t.Errorf("popped %s, want a again", p.key)
	}
}

// Delays and rate limiting follow the priority queue's rules: a delayed item is
// due at its time, an immediate add of a delayed key folds the delay in at the
// higher priority, a delayed add of a ready key leaves it ready, and the rate
// limiter decides AddRateLimited's delay.
func TestGatedQueueDelaysAndRateLimits(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	g.setLocalCapacity(4)
	clk := clocktesting.NewFakeClock(time.Now())
	q := newGatedQueue[string](g, "delays", workqueue.NewTypedItemExponentialFailureRateLimiter[string](time.Second, time.Minute), clk)
	defer q.ShutDown()
	got, _ := workers(q, 1)

	q.AddAfter("later", 10*time.Second)
	none(t, got)
	clk.Step(9 * time.Second)
	none(t, got)
	clk.Step(time.Second)
	q.Done(receive(t, got).key)

	q.AddWithOpts(priorityqueue.AddOpts{After: time.Hour, Priority: ptrTo(5)}, "folded")
	q.Add("folded")
	if p := receive(t, got); p.key != "folded" || p.priority != 5 {
		t.Errorf("popped %+v, want folded at the delayed add's priority 5, now", p)
	}
	q.Done("folded")
	clk.Step(time.Hour)
	none(t, got)

	q.AddRateLimited("limited") // first failure: 1s
	if q.NumRequeues("limited") != 1 {
		t.Errorf("NumRequeues = %d, want 1", q.NumRequeues("limited"))
	}
	clk.Step(999 * time.Millisecond)
	none(t, got)
	clk.Step(time.Millisecond)
	q.Done(receive(t, got).key)
	q.Forget("limited")
	if q.NumRequeues("limited") != 0 {
		t.Errorf("NumRequeues after Forget = %d, want 0", q.NumRequeues("limited"))
	}
}

func ptrTo(i int) *int { return &i }

// Shutdown wakes every waiting worker, and a reconcile still running gives its
// slot back with Done.
func TestShutDownWakesEveryWaitingWorker(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	q := newGatedQueue[string](g, "shutdown", nil, clocktesting.NewFakeClock(time.Now()))
	got, wait := workers(q, 4)
	q.Add("running")
	q.Add("waiting")
	running := receive(t, got)
	q.AddAfter("delayed", time.Hour)
	q.ShutDown()
	done := make(chan struct{})
	go func() { wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a worker did not return after ShutDown")
	}
	if _, _, shutdown := q.GetWithPriority(); !shutdown {
		t.Error("GetWithPriority after ShutDown did not report shutdown")
	}
	q.Done(running.key)
	if n := g.inFlight(); n != 0 {
		t.Errorf("%d local slots in flight after the last Done, want 0", n)
	}
	q.Add("after") // must not panic or block
}

// recordingLog keeps Warn and Error lines.
type recordingLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *recordingLog) add(s string)         { l.mu.Lock(); l.lines = append(l.lines, s); l.mu.Unlock() }
func (l *recordingLog) Info(string, ...any)  {}
func (l *recordingLog) Debug(string, ...any) {}
func (l *recordingLog) Warn(msg string, _ ...any) {
	l.add("WARN " + msg)
}
func (l *recordingLog) Error(_ error, msg string, _ ...any) { l.add("ERROR " + msg) }
func (l *recordingLog) WithName(string) logging.Logger      { return l }
func (l *recordingLog) WithValues(...any) logging.Logger    { return l }
func (l *recordingLog) count(prefix string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, s := range l.lines {
		if strings.HasPrefix(s, prefix) {
			n++
		}
	}
	return n
}

func startSampler(t *testing.T, g *Governor) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = g.Start(ctx); close(done) }()
	eventually(t, "the sampler's first sample", func() bool { return g.sat.last.Load() != nil })
	return func() { cancel(); <-done }
}

// Two Governors with saturation in one process each allow their full capacity
// on the same container, so the second to start warns.
func TestASecondSaturationGovernorWarns(t *testing.T) {
	first := &recordingLog{}
	second := &recordingLog{}
	a, err := New(Options{MaxConcurrentReconciles: 1, Logger: first, Saturation: SaturationOptions{Enabled: true, CgroupDir: fixture("healthy")}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(Options{MaxConcurrentReconciles: 1, Logger: second, Saturation: SaturationOptions{Enabled: true, CgroupDir: fixture("healthy")}})
	if err != nil {
		t.Fatal(err)
	}
	stopA := startSampler(t, a)
	stopB := startSampler(t, b)
	stopB()
	stopA()
	if first.count("WARN Another Governor") != 0 || second.count("WARN Another Governor") != 1 {
		t.Errorf("warnings: first %v, second %v; want one, from the second", first.lines, second.lines)
	}
	third := &recordingLog{}
	c, _ := New(Options{MaxConcurrentReconciles: 1, Logger: third, Saturation: SaturationOptions{Enabled: true, CgroupDir: fixture("healthy")}})
	startSampler(t, c)()
	if third.count("WARN") != 0 {
		t.Errorf("a Governor started alone warned: %v", third.lines)
	}
}

// A reconciler whose controller wasn't built with ForControllerRuntime has no
// local bound, and Start says so, once.
func TestAReconcilerWithoutAGatedQueueLogsAnError(t *testing.T) {
	for _, tc := range []struct {
		reconcilers, gated, want int
	}{{2, 1, 1}, {2, 2, 0}, {1, 2, 0}} {
		t.Run(fmt.Sprintf("%d reconcilers, %d gated", tc.reconcilers, tc.gated), func(t *testing.T) {
			logs := &recordingLog{}
			g, err := New(Options{MaxConcurrentReconciles: 1, Logger: logs, Saturation: SaturationOptions{Enabled: true, CgroupDir: fixture("healthy")}})
			if err != nil {
				t.Fatal(err)
			}
			for range tc.reconcilers {
				g.RegisterReconciler()
			}
			for range tc.gated {
				g.RegisterGatedController()
			}
			startSampler(t, g)()
			if got := logs.count("ERROR More reconcilers"); got != tc.want {
				t.Errorf("logged %d errors, want %d: %v", got, tc.want, logs.lines)
			}
		})
	}
}

// The model test: random interleavings of immediate, delayed and rate-limited
// adds with priorities, pops by real workers, Done, clock steps and capacity
// changes, checked after each step against a reference model of the priority
// queue's rules. The queue's own view (ready, processing, waiting, the count
// of items it can hand out) must match the model exactly, and the inner queue
// must hold what the model says. And it must be live and safe at every quiet
// point: no idle worker while an item and a slot are both free, never more
// slots taken than capacity allows, and every pop the highest priority
// available.
//
// RLGOV_MODEL_RUNS sets the number of runs (default defaultModelRuns: 200, or 20
// under the race detector), each of 120 steps.
func TestGatedQueueMatchesTheModel(t *testing.T) {
	runs := defaultModelRuns
	if v, err := strconv.Atoi(os.Getenv("RLGOV_MODEL_RUNS")); err == nil && v > 0 {
		runs = v
	}
	var total modelStats
	for run := range runs {
		seed := uint64(run)*7919 + 17
		if !t.Run(strconv.Itoa(run), func(t *testing.T) { total.add(modelRun(t, seed, 120)) }) {
			break
		}
	}
	t.Logf("%d runs: %d pops, %d of them while another key was available at a lower priority, %d delayed items came due, %d re-adds while processing, %d capacity changes, %d Done",
		runs, total.pops, total.contested, total.due, total.reAdds, total.capacityChanges, total.dones)
	// The test proves nothing if the interesting cases never happen.
	if runs >= 20 && (total.contested == 0 || total.due == 0 || total.reAdds == 0 || total.capacityChanges == 0) {
		t.Errorf("the runs never exercised a case: %+v", total)
	}
}

type modelStats struct{ pops, contested, due, reAdds, capacityChanges, dones int }

func (s *modelStats) add(o modelStats) {
	s.pops += o.pops
	s.contested += o.contested
	s.due += o.due
	s.reAdds += o.reAdds
	s.capacityChanges += o.capacityChanges
	s.dones += o.dones
}

// fixedLimiter delays every rate-limited add of key k by delays[k].
type fixedLimiter struct{ delays map[string]time.Duration }

func (f fixedLimiter) When(k string) time.Duration { return f.delays[k] }
func (fixedLimiter) Forget(string)                 {}
func (fixedLimiter) NumRequeues(string) int        { return 0 }

type modelWaiting struct {
	at       time.Time
	priority int
}

func modelRun(t *testing.T, seed uint64, steps int) (stats modelStats) {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	keys := []string{"k0", "k1", "k2", "k3", "k4", "k5", "k6"}
	limiter := fixedLimiter{delays: map[string]time.Duration{}}
	for _, k := range keys {
		limiter.delays[k] = time.Duration(rng.IntN(3)) * time.Second
	}
	g, err := New(Options{MaxConcurrentReconciles: 8, Saturation: SaturationOptions{Enabled: true, CgroupDir: fixture("healthy")}})
	if err != nil {
		t.Fatal(err)
	}
	capacity := 1 + rng.IntN(3)
	g.setLocalCapacity(capacity)
	clk := clocktesting.NewFakeClock(time.Unix(1_000_000, 0))
	q := newGatedQueue[string](g, "model", limiter, clk)
	nWorkers := 1 + rng.IntN(4)
	got, wait := boundWorkers(q, nWorkers)

	ready := map[string]int{} // key -> priority, in inner
	processing := map[string]bool{}
	waiting := map[string]modelWaiting{}
	var held []heldItem // popped, not Done, in pop order
	addReady := func(k string, p int) {
		if w, ok := waiting[k]; ok {
			p = max(p, w.priority)
			delete(waiting, k)
		}
		if old, ok := ready[k]; ok {
			p = max(p, old)
		}
		ready[k] = p
	}
	addAfter := func(k string, d time.Duration, p int) {
		if d <= 0 {
			addReady(k, p)
			return
		}
		if old, ok := ready[k]; ok {
			ready[k] = max(old, p)
			return
		}
		at := clk.Now().Add(d)
		if w, ok := waiting[k]; ok {
			w.priority = max(w.priority, p)
			if at.Before(w.at) {
				w.at = at
			}
			waiting[k] = w
			return
		}
		waiting[k] = modelWaiting{at: at, priority: p}
	}
	available := func() []string {
		var out []string
		for k := range ready {
			if !processing[k] {
				out = append(out, k)
			}
		}
		return out
	}
	settle := func(step int, op string) {
		t.Helper()
		// Collect pops until the queue is quiet: no grant pending, every worker
		// accounted for, and nothing left that a free slot and an idle worker could take.
		// Pops of one step happen together, and workers report them in any order, so
		// the priority rule is checked on the batch.
		var batch []heldItem
		deadline := time.Now().Add(5 * time.Second)
		for {
			for drained := false; !drained; {
				select {
				case p := <-got:
					av := available()
					if !slices.Contains(av, p.key) {
						t.Fatalf("step %d (%s): popped %s, which the model says isn't available (available %v)", step, op, p.key, av)
					}
					if p.priority != ready[p.key] {
						t.Fatalf("step %d (%s): %s popped at priority %d, model says %d", step, op, p.key, p.priority, ready[p.key])
					}
					for _, k := range av {
						if ready[k] < p.priority {
							stats.contested++
							break
						}
					}
					stats.pops++
					batch = append(batch, p)
					delete(ready, p.key)
					processing[p.key] = true
					held = append(held, p)
				default:
					drained = true
				}
			}
			q.mu.Lock()
			quiet := q.granted == 0 && q.reserved == 0 && q.idle == nWorkers-len(q.processing) && len(q.processing) == len(held)
			q.mu.Unlock()
			if quiet && !(len(held) < g.LocalCapacity() && len(held) < nWorkers && len(available()) > 0) {
				break
			}
			if time.Now().After(deadline) {
				q.mu.Lock()
				state := fmt.Sprintf("granted %d reserved %d idle %d processing %d free %d", q.granted, q.reserved, q.idle, len(q.processing), q.free)
				q.mu.Unlock()
				t.Fatalf("step %d (%s): never quiet: held %d, capacity %d, workers %d, available %v; queue %s", step, op, len(held), g.LocalCapacity(), nWorkers, available(), state)
			}
			time.Sleep(200 * time.Microsecond)
		}
		for _, p := range batch {
			for _, k := range available() {
				if ready[k] > p.priority {
					t.Fatalf("step %d (%s): popped %s at %d while %s at %d stayed available", step, op, p.key, p.priority, k, ready[k])
				}
			}
		}
		// The governor's count first: its lock comes before the queue's.
		if n := g.inFlight(); n != len(held) {
			t.Fatalf("step %d (%s): %d local slots in flight, %d items held", step, op, n, len(held))
		}
		// The queue's view is the model's.
		q.mu.Lock()
		defer q.mu.Unlock()
		if q.free != len(available()) {
			t.Fatalf("step %d (%s): free = %d, model has %d available", step, op, q.free, len(available()))
		}
		for k := range ready {
			if _, ok := q.ready[k]; !ok {
				t.Fatalf("step %d (%s): %s is ready in the model, not in the queue", step, op, k)
			}
		}
		if len(q.ready) != len(ready) || len(q.processing) != len(processing) || len(q.waiting) != len(waiting) {
			t.Fatalf("step %d (%s): queue has %d ready, %d processing, %d waiting; model %d, %d, %d",
				step, op, len(q.ready), len(q.processing), len(q.waiting), len(ready), len(processing), len(waiting))
		}
		for k, w := range waiting {
			if qw, ok := q.waiting[k]; !ok || !qw.at.Equal(w.at) || qw.priority != w.priority {
				t.Fatalf("step %d (%s): waiting %s = %+v in the queue, model %+v", step, op, k, q.waiting[k], w)
			}
		}
		if n := q.inner.Len(); n != len(ready) {
			t.Fatalf("step %d (%s): the inner queue holds %d ready items, model %d", step, op, n, len(ready))
		}
	}

	settle(-1, "start")
	for step := range steps {
		k := keys[rng.IntN(len(keys))]
		p := rng.IntN(5) - 2
		var op string
		switch r := rng.IntN(100); {
		case r < 25:
			op = fmt.Sprintf("add %s@%d", k, p)
			if processing[k] {
				stats.reAdds++
			}
			q.AddWithOpts(priority(p), k)
			addReady(k, p)
		case r < 40:
			d := time.Duration(1+rng.IntN(3)) * time.Second
			op = fmt.Sprintf("add %s@%d after %s", k, p, d)
			q.AddWithOpts(priorityqueue.AddOpts{After: d, Priority: &p}, k)
			addAfter(k, d, p)
		case r < 48:
			op = "rate-limited add " + k
			q.AddRateLimited(k)
			addAfter(k, limiter.delays[k], 0)
		case r < 75:
			if len(held) == 0 {
				continue
			}
			i := rng.IntN(len(held))
			h := held[i]
			op = "done " + h.key
			held = slices.Delete(held, i, i+1)
			delete(processing, h.key)
			close(h.release)
			<-h.done
			stats.dones++
		case r < 88:
			// To the next deadline, so one step makes due only the keys due at
			// the same time; or a short step when nothing waits.
			d := time.Duration(rng.IntN(500)) * time.Millisecond
			var next time.Time
			for _, w := range waiting {
				if next.IsZero() || w.at.Before(next) {
					next = w.at
				}
			}
			if !next.IsZero() {
				d = next.Sub(clk.Now())
			}
			op = "step " + d.String()
			clk.Step(d)
			for wk, w := range waiting {
				if !w.at.After(clk.Now()) {
					delete(waiting, wk)
					addReady(wk, w.priority)
					stats.due++
				}
			}
		default:
			capacity = 1 + rng.IntN(4)
			op = "capacity " + strconv.Itoa(capacity)
			g.setLocalCapacity(capacity)
			stats.capacityChanges++
		}
		settle(step, op)
	}

	q.ShutDown()
	for _, h := range held {
		close(h.release)
		<-h.done
	}
	finished := make(chan struct{})
	go func() { wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("workers did not return after ShutDown")
	}
	if n := g.inFlight(); n != 0 {
		t.Fatalf("%d local slots in flight after shutdown and the last Done, want 0", n)
	}
	return stats
}

// Every key due by the time a timer fires becomes ready in that one step, not
// one per timer: items due together are handed out by priority. Two timers due
// at the same instant can't be ordered from outside, so this calls the timer's
// callback directly.
func TestKeysDueTogetherBecomeReadyTogether(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	g.setLocalCapacity(0) // nothing is handed out, so the test sees the queue's state
	clk := clocktesting.NewFakeClock(time.Now())
	q := newGatedQueue[string](g, "due-together", nil, clk)
	defer q.ShutDown()
	q.AddWithOpts(priorityqueue.AddOpts{After: time.Second, Priority: ptrTo(-1)}, "low")
	q.AddWithOpts(priorityqueue.AddOpts{After: time.Second, Priority: ptrTo(1)}, "high")
	q.mu.Lock()
	w, gen := q.waiting["low"], q.waiting["low"].gen
	q.mu.Unlock()

	q.due("low", w, gen)
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.waiting) != 0 || len(q.ready) != 2 || q.free != 2 {
		t.Errorf("after one timer: %d waiting, %d ready, %d free; want 0, 2, 2", len(q.waiting), len(q.ready), q.free)
	}
}

// A timer from before a delay was shortened does nothing when it fires late.
func TestALateTimerDoesNothing(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	g.setLocalCapacity(0)
	clk := clocktesting.NewFakeClock(time.Now())
	q := newGatedQueue[string](g, "late-timer", nil, clk)
	defer q.ShutDown()
	q.AddAfter("k", time.Hour)
	q.mu.Lock()
	w, stale := q.waiting["k"], q.waiting["k"].gen
	q.mu.Unlock()
	q.AddAfter("k", time.Minute) // the same entry, rescheduled

	q.due("k", w, stale)
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.waiting["k"]; !ok || len(q.ready) != 0 {
		t.Errorf("a stale timer made k ready: waiting %v, ready %v", q.waiting, q.ready)
	}
}

// A worker that was granted a slot but hadn't popped when the queue shut down
// gives the slot back. Between a grant and its pop is too short to hit from
// outside, so this puts a waiting worker in that state the way dispatchLocked
// does, without the signal, and then shuts the queue down.
func TestShutDownReturnsASlotGrantedButNotPopped(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	g.setLocalCapacity(0)
	q := newGatedQueue[string](g, "granted-at-shutdown", nil, clocktesting.NewFakeClock(time.Now()))
	_, wait := workers(q, 1)
	eventually(t, "the worker waiting", func() bool { return idleWorkers(q) == 1 })

	g.mu.Lock()
	q.mu.Lock()
	q.idle--
	q.granted++
	q.reserved++
	g.localInFlight++
	q.mu.Unlock()
	g.mu.Unlock()

	q.ShutDown()
	done := make(chan struct{})
	go func() { wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the worker did not return after ShutDown")
	}
	if n := g.inFlight(); n != 0 {
		t.Errorf("%d local slots in flight, want 0: the granted slot was kept", n)
	}
}
