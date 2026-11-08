package reconciler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/util/workqueue"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/priorityqueue"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/controller"
	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
)

// These tests run a real controller-runtime controller, over the queue
// controller.Options.ForControllerRuntime builds, so the worker loop is
// controller-runtime's own (docs/design-local-slot-before-pop.md, §7). The
// governor's local capacity is held at 1: one sample starts the startup ramp's
// clock, and with the sampler stopped nothing raises it.

// slotWorld is one controller over a managed reconciler whose keyless provider
// records the order Observe is entered in and can hold an Observe open.
type slotWorld struct {
	t       *testing.T
	srv     *apiServer
	entered chan string

	mu   sync.Mutex
	hold map[string]chan struct{}
}

func newSlotWorld(t *testing.T) *slotWorld {
	return &slotWorld{t: t, srv: newAPIServer(map[string]string{}), entered: make(chan string, 256), hold: map[string]chan struct{}{}}
}

// holdObserve makes name's Observe wait until the returned func is called.
func (w *slotWorld) holdObserve(name string) (release func()) {
	ch := make(chan struct{})
	w.mu.Lock()
	w.hold[name] = ch
	w.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { close(ch) }) }
}

func (w *slotWorld) reconciler(g *ratelimit.Governor) *Reconciler {
	conn := ExternalConnectorFn(func(context.Context, resource.Managed) (ExternalClient, error) {
		c := countingClient(&clientCalls{}, func(ctx context.Context, mg resource.Managed) (ExternalObservation, error) {
			w.entered <- mg.GetName()
			w.mu.Lock()
			ch := w.hold[mg.GetName()]
			w.mu.Unlock()
			if ch != nil {
				select {
				case <-ch:
				case <-ctx.Done():
				}
			}
			return upToDate(ctx, mg)
		})
		return &c, nil
	})
	return newRateLimitedReconciler(w.srv, WithExternalConnecter(conn), WithRateLimitGovernor(g), WithPollInterval(time.Hour))
}

// next returns the next name to enter Observe, or "" after d.
func (w *slotWorld) next(d time.Duration) string {
	select {
	case n := <-w.entered:
		return n
	case <-time.After(d):
		return ""
	}
}

// runningController starts a controller with workers workers over the queue
// o.ForControllerRuntime builds, reconciling with r. add enqueues a request
// at a priority; the returned stop cancels the controller and waits for
// Start to return, failing the test if it doesn't within 5s.
func runningController(t *testing.T, name string, workers int, o controller.Options, r reconcile.Reconciler) (add func(req reconcile.Request, priority int), stop func()) {
	t.Helper()
	o.MaxConcurrentReconciles = workers
	opts := o.ForControllerRuntime()
	opts.Reconciler = r
	opts.MaxConcurrentReconciles = workers
	opts.SkipNameValidation = func() *bool { b := true; return &b }()
	c, err := ctrlcontroller.NewTypedUnmanaged(name, opts)
	if err != nil {
		t.Fatal(err)
	}
	queueCh := make(chan workqueue.TypedRateLimitingInterface[reconcile.Request], 1)
	if err := c.Watch(source.Func(func(_ context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
		queueCh <- q
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()
	var q workqueue.TypedRateLimitingInterface[reconcile.Request]
	select {
	case q = <-queueCh:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("the controller never started its queue")
	}
	pq, ok := q.(priorityqueue.PriorityQueue[reconcile.Request])
	if !ok {
		cancel()
		t.Fatalf("the controller's queue is a %T, not a priority queue", q)
	}
	var stopped sync.Once
	stop = func() {
		stopped.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("the controller's Start returned %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("the controller's Start did not return within 5s of its context being cancelled")
			}
		})
	}
	t.Cleanup(stop)
	return func(req reconcile.Request, priority int) {
		pq.AddWithOpts(priorityqueue.AddOpts{Priority: &priority}, req)
	}, stop
}

func slotOptions(g *ratelimit.Governor) controller.Options {
	o := controller.DefaultOptions()
	o.RateLimitGovernor = g
	return o
}

// Test 1. Capacity 1, one reconcile running, low-priority items queued, then a
// high-priority one: when the slot frees, the high-priority item runs next, at
// once. With the gate after the pop, the high-priority item is popped by an
// idle worker, deferred for 1s to 30s, and sleeps through the freed slot.
func TestHighPriorityRunsNextWhenTheLocalSlotFrees(t *testing.T) {
	g := saturatedGovernor(t, 4)
	w := newSlotWorld(t)
	add, _ := runningController(t, "priority-next", 4, slotOptions(g), w.reconciler(g))

	releaseBusy := w.holdObserve("busy")
	add(request("busy"), 0)
	if got := w.next(5 * time.Second); got != "busy" {
		t.Fatalf("first Observe = %q, want busy", got)
	}
	for _, n := range []string{"low-a", "low-b", "low-c", "low-d", "low-e", "low-f"} {
		add(request(n), -100)
	}
	add(request("high"), 100)
	time.Sleep(300 * time.Millisecond) // every idle worker has had time to pop
	releaseBusy()

	if got := w.next(time.Second); got != "high" {
		t.Errorf("after the slot freed, %q entered Observe first (\"\" means none within 1s), want high", got)
	}
}

// Test 2. When the slot frees, the next reconcile starts at once, not after a
// requeue delay.
func TestNextReconcileStartsWhenTheLocalSlotFrees(t *testing.T) {
	g := saturatedGovernor(t, 2)
	w := newSlotWorld(t)
	add, _ := runningController(t, "next-at-once", 2, slotOptions(g), w.reconciler(g))

	releaseA := w.holdObserve("a")
	add(request("a"), 0)
	if got := w.next(5 * time.Second); got != "a" {
		t.Fatalf("first Observe = %q, want a", got)
	}
	add(request("b"), 0)
	time.Sleep(200 * time.Millisecond)
	releaseA()
	start := time.Now()
	if got := w.next(time.Second); got != "b" {
		t.Fatalf("after the slot freed, %q entered Observe (\"\" means none within 1s), want b", got)
	}
	if d := time.Since(start); d > 300*time.Millisecond {
		t.Errorf("b started %s after the slot freed, want within 300ms", d)
	}
}

// concurrentReconciles wraps r and records the most Reconcile calls in flight
// at once, and every result.
type concurrentReconciles struct {
	r              reconcile.Reconciler
	in, max        atomic.Int64
	mu             sync.Mutex
	shortRequeues  int
	shortThreshold time.Duration
}

func (c *concurrentReconciles) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	now := c.in.Add(1)
	for m := c.max.Load(); now > m && !c.max.CompareAndSwap(m, now); m = c.max.Load() {
	}
	defer c.in.Add(-1)
	res, err := c.r.Reconcile(ctx, req)
	if res.RequeueAfter > 0 && res.RequeueAfter < c.shortThreshold {
		c.mu.Lock()
		c.shortRequeues++
		c.mu.Unlock()
	}
	return res, err
}

// Test 3. The local slot covers the whole reconcile, not only the gated calls:
// with capacity 1 and four workers, never more than one Reconcile is in flight.
// With the gate inside the reconcile, work outside the gated calls, here the
// whole Reconcile, isn't bounded.
func TestLocalSlotCoversTheWholeReconcile(t *testing.T) {
	g := saturatedGovernor(t, 4)
	w := newSlotWorld(t)
	cr := &concurrentReconciles{r: w.reconciler(g), shortThreshold: time.Minute}
	add, _ := runningController(t, "whole-reconcile", 4, slotOptions(g), cr)

	releases := map[string]func(){}
	for _, n := range []string{"a", "b", "c", "d"} {
		releases[n] = w.holdObserve(n)
		add(request(n), 0)
	}
	for range 4 {
		n := w.next(5 * time.Second)
		if n == "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
		releases[n]()
	}
	if m := cr.max.Load(); m != 1 {
		t.Errorf("at most %d Reconcile calls were in flight at once, want 1 with local capacity 1", m)
	}
}

// Test 4. The controller's own saturation is not a state of the object: no
// reconcile defers for local capacity, and nothing writes ReconcileDeferred.
func TestLocalCapacityWritesNothingOnTheObject(t *testing.T) {
	g := saturatedGovernor(t, 4)
	w := newSlotWorld(t)
	cr := &concurrentReconciles{r: w.reconciler(g), shortThreshold: time.Minute}
	add, stop := runningController(t, "nothing-written", 4, slotOptions(g), cr)

	// Each Observe lasts 50ms, so the four reconciles overlap: four workers pop
	// at once and only one slot is free.
	names := []string{"a", "b", "c", "d"}
	releases := map[string]func(){}
	for _, n := range names {
		releases[n] = w.holdObserve(n)
		add(request(n), 0)
	}
	seen := map[string]bool{}
	for deadline := time.Now().Add(3 * time.Second); len(seen) < len(names) && time.Now().Before(deadline); {
		n := w.next(time.Until(deadline))
		if n == "" {
			break
		}
		seen[n] = true
		time.Sleep(50 * time.Millisecond)
		releases[n]()
	}
	stop()
	if len(seen) != len(names) {
		t.Errorf("only %d of %d objects reached Observe within 3s", len(seen), len(names))
	}
	cr.mu.Lock()
	short := cr.shortRequeues
	cr.mu.Unlock()
	if short != 0 {
		t.Errorf("%d reconciles requeued in under a minute: local capacity must not defer a popped item", short)
	}
	for _, n := range names {
		if obj := w.srv.persistedObject(n); obj != nil {
			if s := obj.GetCondition(prv1.TypeSynced); s.Reason == prv1.ReasonReconcileDeferred {
				t.Errorf("%s was written Synced=%s %q: local capacity must not be written on the object", n, s.Reason, s.Message)
			}
		}
	}
}

// Test 5. Two controllers on one governor, capacity 1: controller A's idle
// workers hold no slot, so controller B's item runs at once. A design that takes
// the slot before waiting for an item would let A's four idle workers starve B.
func TestIdleWorkersOfOneControllerDoNotStarveAnother(t *testing.T) {
	g := saturatedGovernor(t, 4)
	idle := newSlotWorld(t)
	busy := newSlotWorld(t)
	runningController(t, "idle-controller", 4, slotOptions(g), idle.reconciler(g))
	add, _ := runningController(t, "busy-controller", 1, slotOptions(g), busy.reconciler(g))
	time.Sleep(100 * time.Millisecond) // the idle controller's workers are all waiting

	add(request("b"), 0)
	if got := busy.next(time.Second); got != "b" {
		t.Errorf("controller B's item %q entered Observe within 1s (\"\" means it didn't), want b: another controller's idle workers must not hold the slot", got)
	}
}

// Test 6. A reconcile that panics releases its slot: controller-runtime
// recovers the panic, and the next item still runs.
func TestPanickingReconcileReleasesTheLocalSlot(t *testing.T) {
	g := saturatedGovernor(t, 2)
	w := newSlotWorld(t)
	inner := w.reconciler(g)
	r := reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
		if req.Name == "panics" {
			panic("provider bug")
		}
		return inner.Reconcile(ctx, req)
	})
	add, _ := runningController(t, "panic-releases", 2, slotOptions(g), r)
	add(request("panics"), 0)
	time.Sleep(100 * time.Millisecond)
	add(request("after"), 0)
	if got := w.next(time.Second); got != "after" {
		t.Errorf("after a panicking reconcile, %q entered Observe (\"\" means none within 1s), want after", got)
	}
}

// Test 7. Shutdown with workers waiting, idle and blocked on a full local
// capacity: Start returns, so every worker exited.
func TestShutdownWithWorkersWaitingForTheLocalSlot(t *testing.T) {
	g := saturatedGovernor(t, 4)
	w := newSlotWorld(t)
	add, stop := runningController(t, "shutdown-waiters", 4, slotOptions(g), w.reconciler(g))
	w.holdObserve("held") // released only by the context
	add(request("held"), 0)
	if got := w.next(5 * time.Second); got != "held" {
		t.Fatalf("first Observe = %q, want held", got)
	}
	add(request("waits-a"), 0)
	add(request("waits-b"), 0)
	time.Sleep(100 * time.Millisecond)
	stop() // fails the test if Start doesn't return within 5s
}

// Test 8. A governor whose sampler never started keeps local capacity at 1, so
// reconciles run one at a time with no sign on the object. It shows on the
// metrics instead: every saturation input reports as disabled. Mirrors
// core-provider's TestUnstartedSamplerShowsAsADisabledInput.
func TestUnstartedSamplerShowsAsADisabledInput(t *testing.T) {
	g := unstartedSaturatedGovernor(t, 4)
	s := g.Snapshot()
	if s.LocalCapacity != 1 {
		t.Errorf("local capacity = %d, want 1", s.LocalCapacity)
	}
	if len(s.Inputs) == 0 {
		t.Fatal("the snapshot reports no inputs")
	}
	for _, in := range s.Inputs {
		if in.Enabled {
			t.Errorf("input %s reports as enabled before the sampler ever ran", in.Input)
		}
	}
}
