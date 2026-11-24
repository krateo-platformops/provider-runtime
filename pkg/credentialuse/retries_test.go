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
	testingclock "k8s.io/utils/clock/testing"
)

// Retries: what fails is retried, with backoff, and nothing else is, nor
// waits for it. An object of a kind the API server doesn't serve fails for as
// long as that lasts (§6.2), which may be for good.

// recheck is one try of the startup recheck, from nothing.
func (ix *index) recheck(ctx context.Context, enqueue func(id ID, n string)) error {
	return ix.recheckRest(ctx, map[ID]bool{}, enqueue)
}

// timersOf is what q's timers of k hold: the pending retry's objects, and
// the others', and how many there are.
func timersOf(q *batchQueue, k batchKey) (retry []ID, timers int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if e := q.retries[k]; e != nil {
		retry = e.ids
	}
	return retry, len(q.timers)
}

func TestRetryIsOnePerKey(t *testing.T) {
	clk := testingclock.NewFakeClock(time.Now())
	q := newBatchQueue(clk)
	other := batchKey{widgetGK, "other"}
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
	// A later one joins; a sooner one takes over, with what there was.
	q.Retry(batchNS, 4*time.Second, idOf("a"))
	q.Retry(batchNS, 8*time.Second, idOf("b"))
	if _, n := timersOf(q, batchNS); n != 1 {
		t.Fatalf("%d timers for two retries of one key, want 1", n)
	}
	clk.Step(time.Second)
	q.Retry(batchNS, time.Second, idOf("c"), idOf("a"))
	if ids, n := timersOf(q, batchNS); n != 1 || !slices.Equal(ids, []ID{idOf("a"), idOf("b"), idOf("c")}) {
		t.Fatalf("%d timers, the retry's objects %v; want one, with a, b and c", n, ids)
	}
	clk.Step(time.Second)
	take(idOf("a"), idOf("b"), idOf("c"))
	clk.Step(time.Minute)
	if q.Len() != 0 {
		t.Fatal("the retry that was taken over added a batch too")
	}
	// Another key's, and a grace's, are their own.
	q.Retry(batchNS, time.Second, idOf("a"))
	q.AddAfter(batchNS, 2*time.Second, idOf("g"))
	q.Retry(other, time.Minute)
	if _, n := timersOf(q, batchNS); n != 3 {
		t.Errorf("%d timers, want three", n)
	}
	if ids, _ := timersOf(q, other); !slices.Equal(ids, []ID{fillAll}) {
		t.Errorf("a retry with no objects covers %v, want the namespace", ids)
	}
	clk.Step(time.Second)
	take(idOf("a"))
	clk.Step(time.Second)
	take(idOf("g"))
	// At once.
	q.Retry(batchNS, 0, idOf("z"))
	take(idOf("z"))
	q.ShutDown()
	if ids, n := timersOf(q, other); n != 0 || ids != nil || clk.HasWaiters() {
		t.Errorf("after ShutDown, %d timers, a retry of %v, waiters %v", n, ids, clk.HasWaiters())
	}
	q.Retry(batchNS, time.Second, idOf("a"))
	if _, n := timersOf(q, batchNS); n != 0 || clk.HasWaiters() {
		t.Error("a Retry after ShutDown set a timer")
	}
}

// Retry races the timers it replaces, the workers and ShutDown: no deadlock
// against a clock that fires under its own lock (D1 of #109), no object lost,
// and never two pending retries of one key.
func TestRetryUnderStress(t *testing.T) {
	const keys, adders, each = 3, 4, 1500
	clk := testingclock.NewFakeClock(time.Now())
	q := newBatchQueue(clk)
	key := func(i int) batchKey { return batchKey{widgetGK, fmt.Sprintf("ns%d", i%keys)} }
	var mu sync.Mutex
	owed := map[batchKey]map[ID]int{} // additions not followed by a batch that began after them
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var workers, adding, stepping sync.WaitGroup
		for range 2 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for {
					k, ids, ok := q.Get(ctx)
					if !ok {
						return
					}
					mu.Lock()
					for _, id := range ids {
						delete(owed[k], id)
					}
					mu.Unlock()
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
					clk.Step(time.Millisecond)
				}
			}
		}()
		for a := range adders {
			adding.Add(1)
			go func() {
				defer adding.Done()
				for i := range each {
					k, id := key(i), idOf(fmt.Sprintf("o%d-%d", a, i))
					mu.Lock()
					if owed[k] == nil {
						owed[k] = map[ID]int{}
					}
					owed[k][id]++
					mu.Unlock()
					switch i % 5 {
					case 0:
						q.AddAfter(k, time.Duration(i%7)*time.Millisecond, id)
					default:
						q.Retry(k, time.Duration(i%11)*time.Millisecond, id)
					}
					if i%50 == 0 {
						q.mu.Lock()
						n := 0
						for e := range q.timers {
							if !e.at.IsZero() {
								n++
							}
						}
						if n > keys || n != len(q.retries) {
							t.Errorf("%d pending retries of %d keys, %d known", n, keys, len(q.retries))
						}
						q.mu.Unlock()
					}
				}
			}()
		}
		adding.Wait()
		close(stop)
		stepping.Wait()
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			clk.Step(time.Second)
			mu.Lock()
			left := 0
			for _, m := range owed {
				left += len(m)
			}
			mu.Unlock()
			if left == 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		mu.Lock()
		for k, m := range owed {
			if len(m) != 0 {
				t.Errorf("%d objects of %v were never in a batch", len(m), k)
			}
		}
		mu.Unlock()
		// ShutDown against retries and their timers.
		var last sync.WaitGroup
		last.Add(3)
		go func() {
			defer last.Done()
			for i := range 500 {
				q.Retry(key(i), time.Duration(1+(500-i)%9)*time.Millisecond, idOf("x"))
			}
		}()
		go func() {
			defer last.Done()
			for range 200 {
				clk.Step(time.Millisecond)
			}
		}()
		go func() { defer last.Done(); time.Sleep(time.Millisecond); q.ShutDown() }()
		last.Wait()
		cancel()
		workers.Wait()
	}()
	select {
	case <-done:
	case <-time.After(90 * time.Second):
		t.Fatal("deadlocked")
	}
	if clk.HasWaiters() {
		t.Error("a timer outlived ShutDown")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.timers) != 0 || len(q.retries) != 0 {
		t.Errorf("%d timers and %d retries after ShutDown", len(q.timers), len(q.retries))
	}
}

// The backoff starts again once everything that failed is decided, not at any
// batch of the namespace that succeeds.
func TestBackoffLastsWhileAnObjectFails(t *testing.T) {
	var b batchBackoff
	k := batchKey{widgetGK, "ns"}
	fails, other := secretID("ns", "fails"), secretID("ns", "other")
	for _, want := range []time.Duration{1, 2, 4} {
		if d := b.fail(k, fails); d != want*time.Second {
			t.Errorf("backoff %s, want %ds", d, want)
		}
		b.settle(k, other)
	}
	b.fail(k, fails, fillAll)
	b.settle(k, fails)
	if d := b.fail(k, other); d != 16*time.Second {
		t.Errorf("with the namespace's list failing, backoff %s, want 16s", d)
	}
	b.settle(batchKey{widgetGK, "elsewhere"}, other, fillAll)
	b.settle(k, other, fillAll)
	if d := b.fail(k, fails); d != time.Second {
		t.Errorf("after everything was decided, backoff %s, want 1s", d)
	}
	b.settle(k, fails)
	if len(b.failures) != 0 {
		t.Errorf("%d keys remembered, want none", len(b.failures))
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

// failuresOf is how many times in a row k's batch failed, and at what that
// wasn't decided since.
func failuresOf(b *batchBackoff, k batchKey) (int, []ID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	f := b.failures[k]
	if f == nil {
		return 0, nil
	}
	return f.n, SortedIDs(keysOf(f.ids))
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
		if e.at.IsZero() {
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
	if n, ids := failuresOf(&r.s.backoff, k); n != 21 || !slices.Equal(ids, []ID{secretID("ns", "fails")}) {
		t.Errorf("%d failures, at %v; want 21, at the Secret that can't be read", n, ids)
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
	if n, _ := failuresOf(&r.s.backoff, k); n != 0 || hasFinalizer(r.secretMeta(t, "ns", "fails"), fWidgetNS) {
		t.Errorf("%d failures remembered, want the Secret released and none", n)
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

func (s *unwritable) update(ctx context.Context, cur indexVersion, lines []string, rebuilt bool) (indexVersion, error) {
	if s.down {
		return indexVersion{}, apierrors.NewInternalError(errors.New("etcd is unhappy"))
	}
	return s.memStore.update(ctx, cur, lines, rebuilt)
}

// A prune that fails retries what it would have pruned, and nothing else.
func TestAFailedPruneIsRetriedAlone(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"), heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"),
		heldObject(secretGVK, "ns", "kept", "u2", widgetGK, "ns"), holderObject("ns", "h", secretID("ns", "kept")))
	tr := r.track(t, widgetGK)
	r.run(t)
	r.drain()
	st := &unwritable{memStore: &memStore{exists: true, lines: linesOf([]ID{secretID("ns", "s"), secretID("ns", "kept")})}, down: true}
	tr.idx = testIndex(st, r.s.newServer())
	k := batchKey{widgetGK, "ns"}
	r.s.releaseBatch(context.Background(), k, []ID{secretID("ns", "s"), secretID("ns", "kept")})
	if retry, _ := timersOf(r.s.queue, k); !slices.Equal(retry, []ID{secretID("ns", "s")}) {
		t.Errorf("the retry covers %v, want the released Secret, whose line wasn't pruned", retry)
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
		r.s.releaseBatch(context.Background(), k, []ID{fillAll})
		if n, ids := failuresOf(&r.s.backoff, k); n != 2 || !slices.Equal(ids, []ID{fillAll}) {
			t.Errorf("%d failures, at %v; want two, at the namespace's list", n, ids)
		}
		failing.Store(false)
		r.s.releaseBatch(context.Background(), k, []ID{fillAll})
		if n, ids := failuresOf(&r.s.backoff, k); n != 0 {
			t.Errorf("%d failures remembered, at %v, once the list succeeded", n, ids)
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
	srv := &objServer{objs: map[ID]*metav1.PartialObjectMetadata{held: heldObj("held"), fails: heldObj("fails")},
		fail: map[ID]bool{fails: true, vouched: true}, forbid: map[ID]bool{denied: true}}
	st := &unwritable{memStore: &memStore{exists: true, rebuilt: true, lines: linesOf([]ID{bare, denied, fails, held, vouched}), vouched: unheldRef(vouched)}}
	ix := testIndex(st, srv)
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
	if slices.Contains(st.lines, bare.String()) {
		t.Error("the bare object's line stays")
	}
	srv.fail[fails] = false
	gets, err = try()
	if err != nil || !slices.Equal(gets, []ID{fails}) || !slices.Equal(enqueued, []ID{fails}) {
		t.Errorf("the third try read %v and enqueued %v, %v; want the one that had failed", gets, enqueued, err)
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
