package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// The wait before the release of an object being deleted (§6.4, H1), on fake
// clients and a fake clock.

// draws is a random source that a test sets: it gives out waits in turn, and
// the last of them again.
type draws struct {
	mu    sync.Mutex
	waits []time.Duration
	n     int
}

func (d *draws) next() time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()
	w := d.waits[min(d.n, len(d.waits)-1)]
	d.n++
	return w
}

func (d *draws) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.n
}

// graceTimer is the timer that the protected informer sets for the end of one
// deleting object's grace, counted from when this process first saw it.
const graceTimer = 1

// deletingSecret is a held Secret being deleted, that another's finalizer keeps
// after its release, so that the release is seen.
func deletingSecret(ns, name string, deletedAt time.Time) *metav1.PartialObjectMetadata {
	m := deleting(heldObject(secretGVK, ns, name, types.UID("u-"+name), widgetGK, "ns"), deletedAt)
	m.Finalizers = append(m.Finalizers, "example.com/other")
	return m
}

// waitingRig is a rig whose waits are waits, with calls counted.
func waitingRig(t *testing.T, waits []time.Duration, objs ...*metav1.PartialObjectMetadata) (*fakeRig, *draws) {
	t.Helper()
	r := newFakeRig(t, namespaceObject("ns"))
	for _, o := range objs {
		if err := r.md.Tracker().Add(o); err != nil {
			t.Fatal(err)
		}
	}
	d := &draws{waits: waits}
	r.s.releaseWait = d.next
	return r, d
}

func (r *fakeRig) heldSecret(t *testing.T, name string) bool {
	t.Helper()
	return hasFinalizer(r.secretMeta(t, "ns", name), fWidgetNS)
}

// waits reports whether a wait of id is kept in k, ended or not, of whichever
// incarnation.
func (r *fakeRig) waits(k batchKey, id ID) bool {
	r.s.waits.mu.Lock()
	defer r.s.waits.mu.Unlock()
	_, ok := r.s.waits.ends[k][id]
	return ok
}

// quiet waits until no batch is queued or worked on.
func (r *fakeRig) quiet(t *testing.T) {
	t.Helper()
	eventually(t, "the queue to be empty", func() bool {
		return r.s.queue.Len() == 0 && !r.working(batchKey{widgetGK, "ns"})
	})
}

// An object being deleted is released when its wait, of the length drawn, ends,
// and not before. A batch that comes meanwhile neither releases it nor draws
// again, and its release after the wait isn't delayed once more.
func TestAWaitBeforeReleasingADeletingObject(t *testing.T) {
	const wait = 700 * time.Millisecond
	r, d := waitingRig(t, []time.Duration{wait, time.Hour}, deletingSecret("ns", "s", time.Now().Add(-time.Hour)))
	r.track(t, widgetGK)
	r.run(t)
	r.runWrites(t)
	eventually(t, "the wait", func() bool { return d.count() == 1 && r.queueTimers() == 1+graceTimer })
	r.quiet(t)
	r.s.queue.Add(batchKey{widgetGK, "ns"}, secretID("ns", "s"))
	r.quiet(t)
	r.clk.Step(wait - time.Millisecond)
	r.s.queue.Add(batchKey{widgetGK, "ns"}, secretID("ns", "s"))
	r.quiet(t)
	if !r.heldSecret(t, "s") || d.count() != 1 || r.queueTimers() != 1+graceTimer {
		t.Fatalf("inside its wait: held %v, %d waits drawn, %d timers", r.heldSecret(t, "s"), d.count(), r.queueTimers())
	}
	r.clk.Step(time.Millisecond)
	eventually(t, "the release at the wait's end", func() bool { return !r.heldSecret(t, "s") })
	r.quiet(t)
	if n := r.queueTimers(); n != graceTimer {
		t.Errorf("%d timers are left, want the grace's", n)
	}
	if len(r.s.waits.ends) != 0 {
		t.Errorf("waits are left: %v", r.s.waits.ends)
	}
}

// An object that isn't being deleted never waits, whatever its batch has beside.
func TestOnlyADeletingObjectWaits(t *testing.T) {
	live := heldObject(secretGVK, "ns", "live", "u-live", widgetGK, "ns")
	r, d := waitingRig(t, []time.Duration{time.Minute}, live, deletingSecret("ns", "s", time.Now().Add(-time.Hour)))
	r.track(t, widgetGK)
	r.run(t)
	r.runWrites(t)
	eventually(t, "the live object's release", func() bool { return !r.heldSecret(t, "live") })
	eventually(t, "the wait of the other", func() bool { return d.count() == 1 && r.queueTimers() == 1+graceTimer })
	r.quiet(t)
	if !r.heldSecret(t, "s") {
		t.Error("the object being deleted was released at once")
	}
}

// In a namespace being deleted nothing can be created again, so nothing waits,
// and a teardown's batches stay whole (§6.6).
func TestNoWaitInANamespaceBeingDeleted(t *testing.T) {
	now := metav1.Now()
	r := newFakeRig(t)
	ns := namespaceObject("ns")
	ns.DeletionTimestamp = &now
	if err := r.md.Tracker().Add(ns); err != nil {
		t.Fatal(err)
	}
	if err := r.md.Tracker().Add(deletingSecret("ns", "s", time.Now())); err != nil {
		t.Fatal(err)
	}
	r.s.releaseWait = func() time.Duration { t.Error("a wait was drawn"); return time.Hour }
	r.track(t, widgetGK)
	r.run(t)
	r.runWrites(t)
	eventually(t, "the release", func() bool { return !r.heldSecret(t, "s") })
}

// A grace's end keeps its time: only then, and only once the object may be
// released, its wait begins.
func TestTheWaitBeginsAtTheGracesEnd(t *testing.T) {
	const wait = 900 * time.Millisecond
	r, d := waitingRig(t, []time.Duration{wait}, holderObject("ns", "h", secretID("ns", "s")))
	if err := r.md.Tracker().Add(deletingSecret("ns", "s", r.clk.Now())); err != nil {
		t.Fatal(err)
	}
	r.track(t, widgetGK)
	r.run(t)
	r.runWrites(t)
	eventually(t, "the grace's timer", func() bool { return r.queueTimers() >= 1 })
	r.quiet(t)
	if d.count() != 0 {
		t.Fatal("a wait began inside the grace")
	}
	r.clk.Step(DefaultDeletionGrace + 2*time.Second) // deletionTimestamp has whole seconds
	eventually(t, "the wait, at the grace's end", func() bool { return d.count() == 1 })
	r.quiet(t)
	if !r.heldSecret(t, "s") {
		t.Fatal("released without waiting")
	}
	r.clk.Step(wait)
	eventually(t, "the release", func() bool { return !r.heldSecret(t, "s") })
}

// A release that fails after its wait is retried at its backoff, without another
// wait.
func TestARetryDoesNotWaitAgain(t *testing.T) {
	const wait = 500 * time.Millisecond
	r, d := waitingRig(t, []time.Duration{wait, time.Hour}, deletingSecret("ns", "s", time.Now().Add(-time.Hour)))
	var fails atomic.Int32
	r.s.hooks.wrapServer = func(srv server) server {
		return hookServer{server: srv, before: func(_ context.Context, c Call) error {
			if c.Op == "patch" && c.GK == secretGroupKind && fails.Add(1) == 1 {
				return apierrors.NewInternalError(errors.New("test: once"))
			}
			return nil
		}}
	}
	r.track(t, widgetGK)
	r.run(t)
	r.runWrites(t)
	eventually(t, "the wait", func() bool { return d.count() == 1 && r.queueTimers() == 1+graceTimer })
	r.clk.Step(wait)
	eventually(t, "the failed release, and its retry's timer", func() bool { return fails.Load() == 1 && r.queueTimers() == 1+graceTimer })
	r.quiet(t)
	r.clk.Step(releaseBackoffBase)
	eventually(t, "the retry's release", func() bool { return !r.heldSecret(t, "s") })
	if d.count() != 1 {
		t.Errorf("%d waits were drawn, want 1", d.count())
	}
}

// An object that was kept waits again before its release, whenever that is: the
// event that brings it comes to every process at once, as the first did.
func TestAKeptObjectWaitsAgain(t *testing.T) {
	const wait = time.Second
	h := deleting(holderObject("ns", "h", secretID("ns", "s")), time.Now())
	r, d := waitingRig(t, []time.Duration{wait}, h, deletingSecret("ns", "s", time.Now().Add(-time.Hour)))
	// The caches don't show the user yet, so that R0 doesn't keep it.
	tr := r.track(t, widgetGK)
	k := batchKey{widgetGK, "ns"}
	r.s.releaseBatch(context.Background(), k, []ID{secretID("ns", "s")})
	if d.count() != 1 {
		t.Fatalf("%d waits were drawn, want 1", d.count())
	}
	r.clk.Step(wait)
	r.s.releaseBatch(context.Background(), k, []ID{secretID("ns", "s")})
	if !r.heldSecret(t, "s") {
		t.Fatal("released with a deleting user")
	}
	if err := r.md.Tracker().Delete(tr.gvr, "ns", "h"); err != nil {
		t.Fatal(err)
	}
	r.s.releaseBatch(context.Background(), k, []ID{secretID("ns", "s")})
	if d.count() != 2 || !r.heldSecret(t, "s") {
		t.Fatalf("once its user is gone: %d waits drawn, held %v; want a wait of its own", d.count(), r.heldSecret(t, "s"))
	}
	r.clk.Step(wait)
	r.s.releaseBatch(context.Background(), k, []ID{secretID("ns", "s")})
	if r.heldSecret(t, "s") {
		t.Error("not released after its wait")
	}
}

// An object released keeps no wait, though another of its batch fails, so that the
// next object of its name waits.
func TestAReleasedObjectKeepsNoWait(t *testing.T) {
	r, _ := waitingRig(t, []time.Duration{time.Second},
		deletingSecret("ns", "a", time.Now().Add(-time.Hour)), deletingSecret("ns", "b", time.Now().Add(-time.Hour)))
	r.s.hooks.wrapServer = func(srv server) server {
		return hookServer{server: srv, before: func(_ context.Context, c Call) error {
			if c.Op == "patch" && c.Name == "b" {
				return apierrors.NewInternalError(errors.New("test"))
			}
			return nil
		}}
	}
	r.track(t, widgetGK)
	k := batchKey{widgetGK, "ns"}
	ids := []ID{secretID("ns", "a"), secretID("ns", "b")}
	r.s.releaseBatch(context.Background(), k, ids)
	r.clk.Step(time.Second)
	r.s.releaseBatch(context.Background(), k, ids)
	if r.heldSecret(t, "a") || !r.heldSecret(t, "b") {
		t.Fatalf("a held %v, b held %v", r.heldSecret(t, "a"), r.heldSecret(t, "b"))
	}
	if r.waits(k, ids[0]) || !r.waits(k, ids[1]) {
		t.Errorf("waits kept: a %v, b %v; want only b's, whose release failed", r.waits(k, ids[0]), r.waits(k, ids[1]))
	}
}

// Nor does an object that the batch kept: its next release waits again, whatever
// else of its batch failed.
func TestAKeptObjectKeepsNoWaitThoughItsBatchFails(t *testing.T) {
	h := deleting(holderObject("ns", "h", secretID("ns", "a")), time.Now())
	r, _ := waitingRig(t, []time.Duration{time.Second}, h,
		deletingSecret("ns", "a", time.Now().Add(-time.Hour)), deletingSecret("ns", "b", time.Now().Add(-time.Hour)))
	r.s.hooks.wrapServer = func(srv server) server {
		return hookServer{server: srv, before: func(_ context.Context, c Call) error {
			if c.Op == "patch" && c.Name == "b" {
				return apierrors.NewInternalError(errors.New("test"))
			}
			return nil
		}}
	}
	r.track(t, widgetGK) // the caches don't show the user yet, so that R0 doesn't keep it
	k := batchKey{widgetGK, "ns"}
	ids := []ID{secretID("ns", "a"), secretID("ns", "b")}
	r.s.releaseBatch(context.Background(), k, ids)
	r.clk.Step(time.Second)
	r.s.releaseBatch(context.Background(), k, ids)
	if !r.heldSecret(t, "a") || !r.heldSecret(t, "b") {
		t.Fatalf("a held %v, b held %v", r.heldSecret(t, "a"), r.heldSecret(t, "b"))
	}
	if r.waits(k, ids[0]) || !r.waits(k, ids[1]) {
		t.Errorf("waits kept: a %v, b %v; want only b's, whose release failed", r.waits(k, ids[0]), r.waits(k, ids[1]))
	}
}

// Nor does an object that a batch decided without keeping or releasing it as one
// being deleted: here a new one of its name, live and in use.
func TestAReplacedObjectKeepsNoWait(t *testing.T) {
	r, _ := waitingRig(t, []time.Duration{time.Second}, holderObject("ns", "h", secretID("ns", "s")),
		deletingSecret("ns", "s", time.Now().Add(-time.Hour)))
	r.track(t, widgetGK)
	k := batchKey{widgetGK, "ns"}
	id := secretID("ns", "s")
	r.s.releaseBatch(context.Background(), k, []ID{id})
	if !r.waits(k, id) {
		t.Fatal("precondition: no wait began")
	}
	r.clk.Step(time.Second)
	if err := r.md.Tracker().Update(secretsGVR, heldObject(secretGVK, "ns", "s", "u-new", widgetGK, "ns"), "ns"); err != nil {
		t.Fatal(err)
	}
	r.s.releaseBatch(context.Background(), k, []ID{id})
	if !r.heldSecret(t, "s") {
		t.Fatal("the new object was released, with a live user")
	}
	if r.waits(k, id) {
		t.Error("the new object has the old one's wait: its release, when it is deleted, wouldn't wait")
	}
}

// Nor does an object that was released and whose index line couldn't be pruned,
// though it is retried.
func TestAFailedPruneKeepsNoWait(t *testing.T) {
	r, _ := waitingRig(t, []time.Duration{time.Second}, deletingSecret("ns", "s", time.Now().Add(-time.Hour)))
	tr := r.track(t, widgetGK)
	id, k := secretID("ns", "s"), batchKey{widgetGK, "ns"}
	tr.idx = testIndex(&unwritable{memStore: &memStore{exists: true, lines: linesOf([]ID{id})}, down: true}, r.s.newServer(tr.o.Holder))
	r.s.releaseBatch(context.Background(), k, []ID{id})
	if !r.waits(k, id) {
		t.Fatal("precondition: no wait began")
	}
	r.clk.Step(time.Second)
	r.s.releaseBatch(context.Background(), k, []ID{id})
	if retry, _ := timersOf(r.s.queue, k); r.heldSecret(t, "s") || !slices.Equal(retry, []ID{id}) {
		t.Fatalf("held %v, and %v is retried; want it released, and retried for its line", r.heldSecret(t, "s"), retry)
	}
	if r.waits(k, id) {
		t.Error("the released object keeps its wait: the next of its name wouldn't wait")
	}
}

// A wait is neither a decision nor a failure of the batch: what failed before it
// backs off from there after it, and its wait isn't begun again.
func TestAWaitLeavesTheBackoff(t *testing.T) {
	const wait = 5 * time.Second
	ctx := context.Background()
	r, d := waitingRig(t, []time.Duration{wait, time.Hour}, deletingSecret("ns", "s", time.Now().Add(-time.Hour)))
	var fails atomic.Value
	fails.Store("get")
	r.s.hooks.wrapServer = func(srv server) server {
		return hookServer{server: srv, before: func(_ context.Context, c Call) error {
			if c.Op == fails.Load() && c.GK == secretGroupKind {
				return apierrors.NewInternalError(errors.New("test"))
			}
			return nil
		}}
	}
	r.track(t, widgetGK)
	id, k := secretID("ns", "s"), batchKey{widgetGK, "ns"}
	r.s.releaseBatch(ctx, k, []ID{id})
	fails.Store("patch")
	r.clk.Step(releaseBackoffBase)
	if got := r.next(ctx, k); !slices.Equal(got, []ID{id}) || d.count() != 1 {
		t.Fatalf("the retry's batch is %v, and %d waits were drawn; want the Secret's, and one", got, d.count())
	}
	if f := failuresOf(r.s.queue, k); f[id] != 1 {
		t.Errorf("failures %v in the wait, want the one before it", f)
	}
	if retry, n := timersOf(r.s.queue, k); len(retry) != 0 || n != 1 {
		t.Errorf("%v is retried, with %d timers; want the wait's alone", retry, n)
	}
	r.clk.Step(wait)
	if got := r.next(ctx, k); !slices.Equal(got, []ID{id}) || !r.heldSecret(t, "s") {
		t.Fatalf("the batch at the wait's end is %v, held %v", got, r.heldSecret(t, "s"))
	}
	if f := failuresOf(r.s.queue, k); f[id] != 2 {
		t.Errorf("failures %v after the release failed, want 2", f)
	}
	fails.Store("")
	r.clk.Step(2*releaseBackoffBase - 1)
	if got := r.queued(widgetGK, "ns"); got != nil {
		t.Errorf("before its second delay ends, the batch is %v", got)
	}
	r.clk.Step(1)
	r.next(ctx, k)
	if r.heldSecret(t, "s") || d.count() != 1 || len(failuresOf(r.s.queue, k)) != 0 {
		t.Errorf("after its second delay: held %v, %d waits drawn, failures %v", r.heldSecret(t, "s"), d.count(), failuresOf(r.s.queue, k))
	}
}

// An error that names no object retries what waits too, which neither ends its
// wait nor begins another.
func TestAnErrorThatNamesNoObjectLeavesTheWait(t *testing.T) {
	const wait = 5 * time.Second
	ctx := context.Background()
	live := heldObject(secretGVK, "ns", "live", "u-live", widgetGK, "ns")
	r, d := waitingRig(t, []time.Duration{wait, time.Hour}, live, deletingSecret("ns", "s", time.Now().Add(-time.Hour)))
	var fails atomic.Bool
	fails.Store(true)
	r.s.hooks.wrapServer = func(srv server) server {
		return hookServer{server: srv, before: func(_ context.Context, c Call) error {
			if c.Op == "get" && c.Name == "live" && fails.Load() {
				return apierrors.NewInternalError(errors.New("test"))
			}
			return nil
		}}
	}
	r.s.hooks.released = func(res *ReleaseResult) { res.Failed = nil }
	r.track(t, widgetGK)
	s, k := secretID("ns", "s"), batchKey{widgetGK, "ns"}
	r.s.releaseBatch(ctx, k, []ID{secretID("ns", "live"), s})
	fails.Store(false)
	r.clk.Step(releaseBackoffBase)
	if got := r.next(ctx, k); !slices.Equal(got, []ID{secretID("ns", "live"), s}) {
		t.Fatalf("the retry's batch is %v, want both", got)
	}
	if !r.heldSecret(t, "s") || r.heldSecret(t, "live") || d.count() != 1 {
		t.Fatalf("in the wait: s held %v, live held %v, %d waits drawn; want s waiting", r.heldSecret(t, "s"), r.heldSecret(t, "live"), d.count())
	}
	r.clk.Step(wait - releaseBackoffBase - 1)
	if got := r.queued(widgetGK, "ns"); got != nil {
		t.Errorf("before the wait's end the batch is %v", got)
	}
	r.clk.Step(1)
	r.next(ctx, k)
	if r.heldSecret(t, "s") || d.count() != 1 {
		t.Errorf("at the wait's end: s held %v, %d waits drawn", r.heldSecret(t, "s"), d.count())
	}
}

// A batch of a kind that is no longer tracked forgets its objects' waits.
func TestADroppedBatchForgetsItsWaits(t *testing.T) {
	r := newFakeRig(t)
	id, k := secretID("ns", "s"), batchKey{widgetGK, "ns"} // nobody tracks it
	r.s.waits.begin(k, Waiting{id, "u-s"}, r.clk.Now())
	r.s.releaseBatch(context.Background(), k, []ID{id})
	if len(r.s.waits.ends) != 0 {
		t.Errorf("waits are left: %v", r.s.waits.ends)
	}
}

// Nor does one that a Track of its kind parks: what the Track brings back waits
// once, from its start, though its earlier wait had ended, and is released.
func TestAParkedBatchWaitsAgain(t *testing.T) {
	r, d := waitingRig(t, []time.Duration{time.Second}, deletingSecret("ns", "s", time.Now().Add(-time.Hour)))
	id, k := secretID("ns", "s"), batchKey{widgetGK, "ns"}
	tr := r.track(t, widgetGK)
	// The Track runs: its kind isn't tracked yet.
	mine := &inflight{done: make(chan struct{})}
	r.s.mu.Lock()
	delete(r.s.trackers, widgetGK)
	r.s.tracking[widgetGK] = mine
	r.s.mu.Unlock()
	r.s.waits.begin(k, Waiting{id, "u-s"}, r.clk.Now())
	r.s.releaseBatch(context.Background(), k, []ID{id})
	if r.waits(k, id) || len(mine.parked) != 1 {
		t.Fatalf("a wait is kept: %v, and %d batches are parked; want none, and one", r.waits(k, id), len(mine.parked))
	}
	// The Track ends, with its kind tracked.
	r.s.mu.Lock()
	r.s.trackers[widgetGK] = tr
	r.s.mu.Unlock()
	r.s.endTracking(widgetGK, mine)
	if got := r.next(context.Background(), k); !slices.Equal(got, []ID{id}) || d.count() != 1 || !r.heldSecret(t, "s") {
		t.Fatalf("once tracked, the batch is %v, %d waits were drawn, held %v; want one wait", got, d.count(), r.heldSecret(t, "s"))
	}
	r.clk.Step(time.Second)
	r.next(context.Background(), k)
	if r.heldSecret(t, "s") || d.count() != 1 {
		t.Errorf("at the wait's end: held %v, %d waits drawn", r.heldSecret(t, "s"), d.count())
	}
}

// A wait is of one incarnation. Another of the name, held and being deleted while
// the first's ended wait is kept for its retry, waits for itself; so does one that
// comes while the first's wait runs, and the first's timer doesn't end it.
func TestARecreatedObjectWaitsOfItsOwn(t *testing.T) {
	for _, ended := range []bool{true, false} {
		ctx := context.Background()
		r, d := waitingRig(t, []time.Duration{time.Second, 5 * time.Second}, deletingSecret("ns", "s", time.Now().Add(-time.Hour)))
		var fails atomic.Bool
		fails.Store(true)
		r.s.hooks.wrapServer = func(srv server) server {
			return hookServer{server: srv, before: func(_ context.Context, c Call) error {
				if c.Op == "patch" && c.GK == secretGroupKind && fails.Load() {
					return apierrors.NewInternalError(errors.New("test"))
				}
				return nil
			}}
		}
		r.track(t, widgetGK)
		id, k := secretID("ns", "s"), batchKey{widgetGK, "ns"}
		r.s.releaseBatch(ctx, k, []ID{id})
		if ended {
			r.clk.Step(time.Second)
			r.next(ctx, k) // its release fails, and is retried in 1s
		}
		fails.Store(false)
		next := deletingSecret("ns", "s", time.Now().Add(-time.Hour))
		next.UID = "u-next"
		if err := r.md.Tracker().Update(secretsGVR, next, "ns"); err != nil {
			t.Fatal(err)
		}
		// Either way the batch that comes 1s later finds the next one.
		if !ended {
			r.s.releaseBatch(ctx, k, []ID{id})
		}
		r.clk.Step(time.Second)
		if got := r.next(ctx, k); !slices.Equal(got, []ID{id}) || !r.heldSecret(t, "s") || d.count() != 2 {
			t.Errorf("ended %v: the batch is %v, the next one is held %v, and %d waits were drawn; want it to wait", ended, got, r.heldSecret(t, "s"), d.count())
			continue
		}
		rest := 5 * time.Second
		if !ended {
			rest -= time.Second
		}
		r.clk.Step(rest - 1)
		if got := r.queued(widgetGK, "ns"); got != nil {
			t.Errorf("ended %v: before its wait ends the batch is %v", ended, got)
		}
		r.clk.Step(1)
		r.next(ctx, k)
		if r.heldSecret(t, "s") || d.count() != 2 {
			t.Errorf("ended %v: at its wait's end: held %v, %d waits drawn", ended, r.heldSecret(t, "s"), d.count())
		}
	}
}

// A namespace that begins to be deleted inside a wait ends it: R1 takes it from the
// cache, and R2 reads it live. The object is released without a step of the clock,
// or kept, if it has a user.
func TestANamespaceDeletedInsideAWait(t *testing.T) {
	for _, used := range []bool{false, true} {
		r, d := waitingRig(t, []time.Duration{time.Hour}, deletingSecret("ns", "s", time.Now().Add(-time.Hour)))
		var reads atomic.Int32
		r.s.hooks.wrapServer = func(srv server) server {
			return hookServer{server: srv, before: func(_ context.Context, c Call) error {
				if c.Op == "get" && c.GK == namespaceGroupKind && c.Name == "ns" {
					reads.Add(1)
				}
				return nil
			}}
		}
		tr := r.track(t, widgetGK)
		r.run(t)
		r.drain()
		id, k := secretID("ns", "s"), batchKey{widgetGK, "ns"}
		r.s.releaseBatch(context.Background(), k, []ID{id})
		if d.count() != 1 || !r.waits(k, id) || reads.Load() != 0 {
			t.Fatalf("precondition: %d waits drawn, kept %v, %d live reads of the namespace", d.count(), r.waits(k, id), reads.Load())
		}
		if used {
			if err := r.md.Tracker().Add(holderObject("ns", "h", id)); err != nil {
				t.Fatal(err)
			}
			eventually(t, "the holder, in the cache", func() bool { return len(tr.proto.views.HoldersNaming(id, "ns")) == 1 })
		}
		ns := deleting(namespaceObject("ns"), time.Now())
		if err := r.md.Tracker().Update(namespacesGVR, ns, ""); err != nil {
			t.Fatal(err)
		}
		eventually(t, "the namespace's deletion, in the cache", func() bool { return tr.proto.namespaceDeletingCached("ns") })
		r.s.releaseBatch(context.Background(), k, []ID{id})
		if r.heldSecret(t, "s") != used || d.count() != 1 || reads.Load() != 1 {
			t.Errorf("used %v: held %v, %d waits drawn, %d live reads of the namespace; want it decided at once, on one", used, r.heldSecret(t, "s"), d.count(), reads.Load())
		}
		if r.waits(k, id) {
			t.Errorf("used %v: its wait is kept", used)
		}
	}
}

// An object outside the caches that was counted as waiting for its users stays
// counted while its release waits, and leaves the gauge when it is released.
func TestAWaitingObjectStaysCounted(t *testing.T) {
	ctx := context.Background()
	r, k := outside(t, deleting(heldObject(secretGVK, "out", "s", "u1", widgetGK, "out"), time.Now().Add(-time.Hour)),
		holderObject("out", "h", secretID("out", "s")))
	tr := r.track(t, widgetGK)
	r.run(t)
	kind := kindLabel(widgetGK)
	gauge := func(want int, when string) {
		t.Helper()
		if n := r.metrics.waitingGauge(kind); n != want {
			t.Errorf("%s: %d counted, want %d", when, n, want)
		}
	}
	r.s.releaseBatch(ctx, k, []ID{secretID("out", "s")})
	gauge(1, "in its grace")
	if err := r.md.Tracker().Delete(widgetsGVR, "out", "h"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the holder to leave the cache", func() bool { return len(tr.proto.views.HoldersNaming(secretID("out", "s"), "out")) == 0 })
	d := &draws{waits: []time.Duration{time.Second}}
	r.s.releaseWait = d.next
	r.s.releaseBatch(ctx, k, []ID{secretID("out", "s")})
	if d.count() != 1 || !r.heldBy(t, widgetGK, "out", "s") {
		t.Fatalf("precondition: %d waits drawn, held %v", d.count(), r.heldBy(t, widgetGK, "out", "s"))
	}
	gauge(1, "in its wait")
	r.clk.Step(time.Second)
	r.s.releaseBatch(ctx, k, []ID{secretID("out", "s")})
	if r.heldBy(t, widgetGK, "out", "s") {
		t.Error("not released after its wait")
	}
	gauge(0, "released")
}

// Objects of one batch wait each its own time, so that two processes don't meet on
// all of them.
func TestEachObjectWaitsItsOwnTime(t *testing.T) {
	r, d := waitingRig(t, []time.Duration{time.Second, 1900 * time.Millisecond},
		deletingSecret("ns", "a", time.Now().Add(-time.Hour)), deletingSecret("ns", "b", time.Now().Add(-time.Hour)))
	r.track(t, widgetGK)
	k := batchKey{widgetGK, "ns"}
	ids := []ID{secretID("ns", "a"), secretID("ns", "b")}
	r.s.releaseBatch(context.Background(), k, ids)
	if d.count() != 2 {
		t.Fatalf("%d waits were drawn, want 2", d.count())
	}
	r.clk.Step(time.Second)
	r.s.releaseBatch(context.Background(), k, ids)
	if r.heldSecret(t, "a") || !r.heldSecret(t, "b") {
		t.Fatalf("after 1s: a held %v, b held %v", r.heldSecret(t, "a"), r.heldSecret(t, "b"))
	}
	r.clk.Step(900 * time.Millisecond)
	r.s.releaseBatch(context.Background(), k, ids)
	if r.heldSecret(t, "b") {
		t.Error("b isn't released after its wait")
	}
}

// The waits are dropped when the write loops stop: the next ones wait again,
// and their timers hold nothing up.
func TestAStopDropsTheWaits(t *testing.T) {
	r, d := waitingRig(t, []time.Duration{time.Second}, deletingSecret("ns", "s", time.Now().Add(-time.Hour)))
	r.track(t, widgetGK)
	r.run(t)
	stop := r.runWrites(t)
	eventually(t, "the wait", func() bool { return d.count() == 1 && r.queueTimers() == 1+graceTimer })
	r.quiet(t)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if len(r.s.waits.ends) != 0 {
		t.Errorf("waits are left after the stop: %v", r.s.waits.ends)
	}
	r.clk.Step(time.Minute)
	if !r.heldSecret(t, "s") {
		t.Fatal("released with no write loops")
	}
	r.runWrites(t)
	eventually(t, "another wait", func() bool { return d.count() == 2 })
	r.quiet(t)
	if !r.heldSecret(t, "s") {
		t.Fatal("released without waiting")
	}
	r.clk.Step(time.Second)
	eventually(t, "the release", func() bool { return !r.heldSecret(t, "s") })
}

// Two processes whose waits differ: the second finds the object released, and
// makes no R3.
func TestTwoProcessesWithTwoWaits(t *testing.T) {
	r, d := waitingRig(t, []time.Duration{300 * time.Millisecond}, deletingSecret("ns", "s", time.Now().Add(-time.Hour)))
	other := newShared(r.s.c, SharedOptions{}, r.clk)
	e := &draws{waits: []time.Duration{1200 * time.Millisecond}}
	other.releaseWait = e.next
	var r3s [2]atomic.Int32
	for i, s := range []*Shared{r.s, other} {
		s.hooks.wrapServer = func(srv server) server {
			return hookServer{server: srv, before: func(_ context.Context, c Call) error {
				if c.Op == "patch" && c.GK == secretGroupKind {
					r3s[i].Add(1)
				}
				return nil
			}}
		}
		if _, err := s.Track(context.Background(), r.options(widgetGK)); err != nil {
			t.Fatal(err)
		}
	}
	k := batchKey{widgetGK, "ns"}
	both := func() {
		for _, s := range []*Shared{other, r.s} {
			s.releaseBatch(context.Background(), k, []ID{secretID("ns", "s")})
		}
	}
	both()
	if d.count() != 1 || e.count() != 1 {
		t.Fatalf("waits drawn: %d and %d", d.count(), e.count())
	}
	r.clk.Step(300 * time.Millisecond)
	both()
	if r3s[0].Load() != 1 || r3s[1].Load() != 0 || r.heldSecret(t, "s") {
		t.Fatalf("after the first wait: R3s %d and %d, held %v; want only the first's", r3s[0].Load(), r3s[1].Load(), r.heldSecret(t, "s"))
	}
	r.clk.Step(900 * time.Millisecond)
	both()
	if r3s[1].Load() != 0 {
		t.Errorf("the second made %d R3s of an object that was released", r3s[1].Load())
	}
}

// The wait in production is random, below releaseWaitMax, and every Shared has
// it.
func TestTheProductionWait(t *testing.T) {
	s := NewShared(Clients{}, SharedOptions{})
	if s.releaseWait == nil {
		t.Fatal("no wait")
	}
	lo, hi := releaseWaitMax, time.Duration(0)
	for range 10000 {
		w := s.releaseWait()
		if w < 0 || w >= releaseWaitMax {
			t.Fatalf("a wait of %s", w)
		}
		lo, hi = min(lo, w), max(hi, w)
	}
	if lo > releaseWaitMax/10 || hi < releaseWaitMax*9/10 {
		t.Errorf("10000 waits from %s to %s: want them spread over %s", lo, hi, releaseWaitMax)
	}
	if s := fmt.Sprint(releaseWaitMax); s != "2s" {
		t.Errorf("the wait is below %s, want 2s (the user's decision)", s)
	}
}
