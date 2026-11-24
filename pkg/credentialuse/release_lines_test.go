package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// What is known to carry the finalizer stays in its line whatever becomes of it
// (§6.2): what a namespace's list found, what a read showed, what failed, what a
// Track held up, and what the write loops' end left.

// flood adds 3,000 names that nothing carries, more than a turn takes of a line.
func (r *fakeRig) flood(k batchKey, prefix string) {
	r.s.queue.Add(k, someIDs(k.Namespace, prefix, 3000)...)
}

// found are n held Secrets of namespace out, as a list of it finds them.
func found(n int, more ...string) (ids []ID, objs []runtime.Object) {
	ids = someIDs("out", "found", n)
	for _, name := range more {
		ids = append(ids, secretID("out", name))
	}
	for _, id := range ids {
		objs = append(objs, heldObject(secretGVK, "out", id.Name, types.UID("u-"+id.Name), widgetGK, "out"))
	}
	return ids, objs
}

// What a namespace's list found is read again, behind no name of its key: once it
// failed, once it was put back though a record had named it after a flood, and
// beside names taken with it.
func TestWhatANamespacesListFindsStaysKnown(t *testing.T) {
	ctx := context.Background()
	k, x := batchKey{widgetGK, "out"}, secretID("out", "x")
	t.Run("once it failed", func(t *testing.T) {
		_, objs := found(0, "x")
		r, _, l := quiet(t, objs...)
		var failed atomic.Bool
		l.setBefore(func(_ context.Context, c Call) error {
			if c.Op == "get" && c.Name == "x" && failed.CompareAndSwap(false, true) {
				return unservedError()
			}
			return nil
		})
		r.flood(k, "a")
		r.s.queue.Add(k)
		r.turn(ctx)
		if !failed.Load() {
			t.Fatal("the list's find wasn't read in the first turn")
		}
		r.clk.Step(releaseBackoffBase)
		l.take()
		r.turn(ctx)
		if got := l.take(); !slices.Contains(got, x) || r.heldBy(t, widgetGK, "out", "x") {
			t.Errorf("its retry read it: %v; want it read in its turn, and released", slices.Contains(got, x))
		}
	})
	t.Run("put back, and named before", func(t *testing.T) {
		_, objs := found(70, "x")
		r, _, l := quiet(t, objs...)
		r.flood(k, "a")
		r.s.queue.Add(k, x)
		r.s.queue.Add(k)
		for range 2 {
			r.turn(ctx)
		}
		if got := l.take(); !slices.Contains(got, x) {
			t.Errorf("two turns read %d objects, and not the one that the list found and a record named", len(got))
		}
	})
	t.Run("beside names taken with it", func(t *testing.T) {
		_, objs := found(70, "x")
		r, _, l := quiet(t, objs...)
		r.s.queue.Add(k, someIDs("out", "a", 100)...)
		r.s.queue.Add(k, x)
		r.s.queue.Add(k)
		r.turn(ctx)
		if got := l.take(); len(got) == 0 || got[0] != x {
			t.Errorf("the turn read %d objects, the one known longest %v; want it first", len(got), slices.Contains(got, x))
		}
	})
}

// An object that a read showed carrying the finalizer is known to from then on: its
// retry, as after R2's list failed, waits behind no name.
func TestWhatAReadShowsHeldStaysKnown(t *testing.T) {
	ctx := context.Background()
	k, x := batchKey{widgetGK, "out"}, secretID("out", "x")
	_, objs := found(0, "x")
	r, _, l := quiet(t, objs...)
	var failed atomic.Bool
	l.setBefore(func(_ context.Context, c Call) error {
		if c.Op == "list" && failed.CompareAndSwap(false, true) {
			return errors.New("etcd is unhappy")
		}
		return nil
	})
	r.s.queue.Add(k, x)
	r.turn(ctx)
	if !failed.Load() {
		t.Fatal("the list wasn't made")
	}
	r.flood(k, "a")
	r.clk.Step(releaseBackoffBase)
	l.take()
	r.turn(ctx)
	if got := l.take(); !slices.Contains(got, x) || r.heldBy(t, widgetGK, "out", "x") {
		t.Errorf("its retry read it: %v; want it read in its turn, and released", slices.Contains(got, x))
	}
}

// waitsHeld is whether id waits in k, and is known to carry the finalizer.
func (r *fakeRig) waitsHeld(k batchKey, id ID) (waits, held bool) {
	r.s.queue.mu.Lock()
	defer r.s.queue.mu.Unlock()
	p, waits := r.s.queue.pending[k][id]
	return waits, p.held
}

// A batch that a Track of its kind holds up goes back with what was known of it.
func TestAParkedBatchKeepsWhatIsKnown(t *testing.T) {
	r, tr, _ := quiet(t)
	k, x, y := batchKey{widgetGK, "out"}, secretID("out", "x"), secretID("out", "y")
	mine := &inflight{done: make(chan struct{})}
	r.s.mu.Lock()
	delete(r.s.trackers, widgetGK)
	r.s.tracking[widgetGK] = mine
	r.s.mu.Unlock()
	r.s.queue.AddHeld(k, x)
	r.s.queue.Add(k, y)
	r.turn(context.Background())
	r.s.mu.Lock()
	r.s.trackers[widgetGK] = tr
	r.s.mu.Unlock()
	r.s.endTracking(widgetGK, mine)
	if waits, held := r.waitsHeld(k, x); !waits || !held {
		t.Errorf("the one known to be held waits %v, known %v; want both", waits, held)
	}
	if waits, held := r.waitsHeld(k, y); !waits || held {
		t.Errorf("the other waits %v, known %v; want it to wait, unknown", waits, held)
	}
}

// What failed as the write loops ended goes back with what was known of it, where it
// was: before what was added since.
func TestAStoppedBatchsFailuresKeepTheirPlace(t *testing.T) {
	_, objs := found(0, "x")
	r, _, l := quiet(t, objs...)
	k, x := batchKey{widgetGK, "out"}, secretID("out", "x")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l.setBefore(func(_ context.Context, c Call) error {
		if c.Op == "get" && c.Name == "x" {
			r.s.queue.AddHeld(k, secretID("out", "later"))
			cancel()
			return ctx.Err()
		}
		return nil
	})
	r.s.queue.AddHeld(k, x)
	r.turn(ctx)
	if waits, held := r.waitsHeld(k, x); !waits || !held {
		t.Errorf("what failed waits %v, known %v; want both", waits, held)
	}
	if _, ids, _ := r.s.queue.Get(context.Background()); len(ids) != 2 || ids[0] != x {
		t.Errorf("the next batch is %v, want what failed first", ids)
	}
}

// What a turn puts back is in the order its batch was in, whatever kept it from being
// read: a namespace's list shares its place, so nothing else keeps it.
func TestTheRestIsInItsBatchsOrder(t *testing.T) {
	r := newRig(t)
	r.p.turnTime = 1 // none but the first is read
	ids := someIDs("ns", "s", 70)
	slices.Reverse(ids)
	res, err := r.p.release(context.Background(), "ns", ids)
	if err != nil || !slices.Equal(res.Rest, ids[1:]) {
		t.Errorf("the rest is %v, %v\nwant %v", res.Rest, err, ids[1:])
	}
}

// Each of three objects whose release meets a change is read again in its own turn,
// however long that takes: put back, it would lose which resourceVersion its patch was
// built from. The first of its sort would be read past the turn's time in any case, so
// the third is the one that shows it: the turn's time is over when its second read
// comes, and wasn't at its first.
func TestAConflictsReadIsItsTurnsPastItsTime(t *testing.T) {
	r := newRig(t)
	r.p.turnTime = 2 * time.Second
	ids := []ID{secretID("ns", "a"), secretID("ns", "b"), secretID("ns", "c")}
	for _, id := range ids {
		addHeld(r, id)
	}
	changed, reads := map[string]bool{}, map[string]int{}
	r.p.srv = hookServer{server: r.srv, before: func(_ context.Context, c Call) error {
		switch {
		case c.Op == "patch" && !changed[c.Name]:
			changed[c.Name] = true
			_, err := r.m.Patch(secretKey("ns", c.Name), types.MergePatchType, []byte(fmt.Sprintf(`{"metadata":{"finalizers":[%q,"example.com/other"]}}`, fNS)))
			must(err)
		case c.Op == "get":
			if reads[c.Name]++; reads[c.Name] == 2 {
				time.Sleep(1100 * time.Millisecond)
			}
		}
		return nil
	}}
	res, err := r.p.release(context.Background(), "ns", ids)
	if err != nil || len(res.Released) != 3 || len(res.Rest) != 0 {
		t.Errorf("released %v, rest %v, %v; want all three released", res.Released, res.Rest, err)
	}
	for _, id := range ids {
		if reads[id.Name] != 2 {
			t.Errorf("%s was read %d times, want twice", id, reads[id.Name])
		}
	}
}

// A later round leaves no Namespace for another turn either, however long they take.
func TestAConflictsNamespacesAreItsTurns(t *testing.T) {
	r := newRig(t)
	r.p.turnTime = 2 * time.Second
	ids := []ID{secretID("a", "s"), secretID("b", "s"), secretID("c", "s")}
	for _, id := range ids {
		must(r.m.Create(nsKey(id.Namespace), metav1.ObjectMeta{}))
		addHeld(r, id)
		must(r.m.Delete(keyOf(id)))
	}
	changed := map[string]bool{}
	r.p.srv = hookServer{server: r.srv, before: func(_ context.Context, c Call) error {
		switch {
		case c.Op == "patch" && !changed[c.Namespace]:
			changed[c.Namespace] = true
			_, err := r.m.Patch(secretKey(c.Namespace, c.Name), types.MergePatchType, []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:"e1"}}}`, fNS)))
			must(err)
		case c.Op == "get" && c.GK == namespaceGroupKind && changed[c.Name]:
			time.Sleep(1100 * time.Millisecond) // its second read
		}
		return nil
	}}
	res, err := r.p.release(context.Background(), "ns", ids)
	if err != nil || len(res.Released) != 3 || len(res.Rest) != 0 {
		t.Errorf("released %v, rest %v, %v; want all three released", res.Released, res.Rest, err)
	}
}

// The first of each sort is decided, not only read: the turn's time over, its
// Namespace is read too.
func TestEachSortsFirstIsDecided(t *testing.T) {
	r, ctx := limitsRig(t, 1, func(c Call) bool { return c.GK == namespaceGroupKind && c.Name == "a" })
	known, other := secretID("a", "s"), secretID("b", "s")
	for _, id := range []ID{known, other} {
		must(r.m.Create(nsKey(id.Namespace), metav1.ObjectMeta{}))
		addHeld(r, id)
		must(r.m.Delete(keyOf(id)))
	}
	r.p.known = func(_ string, id ID) bool { return id == known }
	res, err := r.p.release(ctx, "ns", []ID{known, other})
	if err == nil || !slices.Equal(res.Failed, []ID{known}) || !slices.Equal(res.Released, []ID{other}) {
		t.Errorf("failed %v, released %v, rest %v, %v; want the first of the one sort failed, and of the other released", res.Failed, res.Released, res.Rest, err)
	}
}

// A failed batch's log line builds the texts of the errors it has, and no other's.
func TestALogLineBuildsFewErrors(t *testing.T) {
	var built atomic.Int32
	var errs []error
	for range 64 {
		errs = append(errs, countedError{&built})
	}
	got := errorSample(joined([]error{errors.New("first"), joined(errs)}))
	if n := built.Load(); n > maxLoggedErrors {
		t.Errorf("the texts of %d errors were built, want %d at most", n, maxLoggedErrors)
	}
	if want := "first\ncounted\ncounted\nand 62 more"; got != want {
		t.Errorf("the line is %q, want %q", got, want)
	}
}

type countedError struct{ n *atomic.Int32 }

func (e countedError) Error() string {
	e.n.Add(1)
	return "counted"
}
