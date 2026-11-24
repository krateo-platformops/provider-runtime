package credentialuse

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/flowcontrol"
)

// The bounded turn (§6.6): a batch reads 64 objects at most, and what it didn't
// get to goes back where it was in the queue, behind the other keys. Whatever is
// added meanwhile, every object is read within a number of turns that what was
// queued before it bounds.

// readIn runs up to most turns, with before each of them, until one reads
// id, and returns which did, from 1, or 0.
func readIn(r *fakeRig, l *costLog, id ID, most int, before func(turn int)) int {
	for turn := 1; turn <= most; turn++ {
		before(turn)
		l.take()
		if _, ok := r.turn(context.Background()); !ok {
			return 0
		}
		if slices.Contains(l.take(), id) {
			return turn
		}
	}
	return 0
}

// 65 objects of a key, and one of another: the other key's turn comes before the
// 65th read, and the 65th, the last added, is the only one of the next.
func TestAKeysTurnIsBounded(t *testing.T) {
	r, _, l := quiet(t)
	a, b := batchKey{widgetGK, "out"}, batchKey{widgetGK, "out2"}
	// Added in no order of their names.
	ids := someIDs("out", "s", 65)
	slices.Reverse(ids)
	other := secretID("out2", "s")
	for _, id := range ids {
		r.s.queue.Add(a, id)
	}
	r.s.queue.Add(b, other)
	var keys []batchKey
	for {
		k, ok := r.turn(context.Background())
		if !ok || len(keys) > 10 {
			break
		}
		keys = append(keys, k)
	}
	if want := []batchKey{a, b, a}; !slices.Equal(keys, want) {
		t.Errorf("the turns were of %v, want %v", keys, want)
	}
	if got, want := l.take(), slices.Concat(ids[:64], []ID{other}, ids[64:]); !slices.Equal(got, want) {
		t.Errorf("the reads were %v\nwant %v", got, want)
	}
}

// What is added before each turn, and an object's being added again, also while
// its key is worked on, don't move one that waits.
func TestWhatIsAddedDoesntDelayWhatWaits(t *testing.T) {
	r, _, l := quiet(t)
	k := batchKey{widgetGK, "out"}
	old := secretID("out", "z")
	r.s.queue.Add(k, someIDs("out", "a", 100)...)
	r.s.queue.Add(k, old)
	l.setBefore(func(_ context.Context, c Call) error {
		r.s.queue.Add(k, old) // while its key is worked on
		return nil
	})
	got := readIn(r, l, old, 20, func(turn int) {
		r.s.queue.Add(k, old)
		// These sort before it, as an edited holder's names may.
		r.s.queue.Add(k, someIDs("out", fmt.Sprintf("b%02d-", turn), 64)...)
	})
	if got != 2 {
		t.Errorf("the 101st object was read in turn %d (0 is never), want 2", got)
	}
}

// The caches first show those that are being deleted, in their grace and with a
// user: each of their reads changes nothing, so the same 128 can be added before
// every turn. shownRig is a rig whose caches show them, and nothing else.
func shownRig(t *testing.T, objs ...runtime.Object) (r *fakeRig, l *costLog, k batchKey, shown []ID) {
	t.Helper()
	shown = someIDs("ns", "held", 128)
	objs = append(objs, namespaceObject("ns"), holderObject("ns", "h", shown...))
	for _, id := range shown {
		objs = append(objs, deleting(heldObject(secretGVK, "ns", id.Name, types.UID("u-"+id.Name), widgetGK, "ns"), time.Now()))
	}
	r = newFakeRig(t, objs...)
	l = r.log()
	// The holder's event waits for the Secrets', so that the queue shows when
	// each of them has queued what it queues, and nothing queues any more.
	gate := make(chan struct{})
	r.s.hooks.holderEvent = func(*Tracker, *metav1.PartialObjectMetadata) { <-gate }
	r.track(t, widgetGK)
	r.run(t)
	eventually(t, "the Secrets' batch", func() bool { return len(r.queued(widgetGK, "ns")) == len(shown) })
	r.drain()
	close(gate)
	eventually(t, "the holder's batch", func() bool { return len(r.queued(widgetGK, "ns")) == len(shown) })
	r.drain()
	return r, l, batchKey{widgetGK, "ns"}, shown
}

// A real hold that the caches don't show, as outside SharedOptions.Namespaces
// or behind a per-name watch that hasn't synced, is read, and released, in the
// third turn, though 40 others of its sort were queued before it and 128 that
// the caches show are queued before each turn.
func TestAHoldTheCachesDontShowIsReadInTime(t *testing.T) {
	unseen := heldObject(secretGVK, "ns", "unseen", "u-unseen", widgetGK, "ns")
	unseen.Labels = nil // the caches select by the label
	r, l, k, shown := shownRig(t, unseen)
	real := secretID("ns", "unseen")
	r.s.queue.Add(k, someIDs("ns", "absent", 40)...)
	r.s.queue.Add(k, real)
	if got := readIn(r, l, real, 20, func(int) { r.s.queue.Add(k, shown...) }); got != 3 {
		t.Errorf("the hold the caches don't show was read in turn %d (0 is never), want 3", got)
	}
	if r.heldBy(t, widgetGK, "ns", "unseen") {
		t.Error("the hold the caches don't show wasn't released")
	}
}

// Those the caches show carrying the finalizer are read first, however many
// others wait, and with room for at least 16 of the others.
func TestWhatTheCachesShowIsReadFirst(t *testing.T) {
	for _, c := range []struct{ others, shown, wantShown, wantOthers int }{
		{100, 10, 10, 54},
		{100, 128, 48, 16},
		{5, 128, 59, 5},
		{0, 128, 64, 0},
		{15, 50, 49, 15},
	} {
		r, l, k, shown := shownRig(t)
		others := someIDs("ns", "absent", c.others)
		for _, id := range others {
			r.s.queue.Add(k, id)
		}
		r.s.queue.Add(k, shown[:c.shown]...)
		l.take()
		r.turn(context.Background())
		want := slices.Concat(shown[:c.wantShown], others[:c.wantOthers])
		if got := l.take(); !slices.Equal(got, want) {
			t.Errorf("%d others and %d shown: %d reads %v\nwant the first %d shown and the first %d others", c.others, c.shown, len(got), got, c.wantShown, c.wantOthers)
		}
	}
}

// What is retried counts among the 64 as what an event queued does.
func TestRetriesAreBoundedToo(t *testing.T) {
	r, _, l := quiet(t)
	k := batchKey{widgetGK, "out"}
	r.s.queue.Fail(k, someIDs("out", "s", 70)...)
	r.clk.Step(releaseBackoffBase)
	for _, want := range []int{64, 6} {
		r.turn(context.Background())
		if got := l.take(); len(got) != want {
			t.Errorf("a turn read %d of the 70 retried, want %d", len(got), want)
		}
	}
}

// What a turn didn't get to is neither decided nor failed: its failures, its
// pending retry, and what is counted for it are as they were; and an error that
// names no object is of the turn's alone.
func TestTheRestIsNeitherDecidedNorFailed(t *testing.T) {
	waiting := deleting(heldObject(secretGVK, "out", "waits", "u1", widgetGK, "out"), time.Now())
	r, _, l := quiet(t, waiting, holderObject("out", "h", secretID("out", "waits")))
	k, kind := batchKey{widgetGK, "out"}, kindLabel(widgetGK)
	waits := secretID("out", "waits")
	r.s.releaseBatch(context.Background(), k, []ID{waits})
	if n := r.metrics.waitingGauge(kind); n != 1 {
		t.Fatalf("%d waiting, want the Secret", n)
	}
	before, fresh := secretID("out", "z-failed"), secretID("out", "y-fresh")
	r.s.queue.Fail(k, before)
	first := someIDs("out", "s", 64)
	l.setBefore(func(_ context.Context, c Call) error {
		if c.Op == "get" && strings.HasPrefix(c.Name, "s") {
			return unservedError()
		}
		return nil
	})
	r.s.hooks.released = func(res *ReleaseResult) { res.Failed = nil } // the error names none
	for _, id := range append(slices.Clone(first), waits, before, fresh) {
		r.s.queue.Add(k, id)
	}
	l.take()
	r.turn(context.Background())
	if got := l.take(); !slices.Equal(got, first) {
		t.Fatalf("the turn read %v, want the first 64", got)
	}
	f := failuresOf(r.s.queue, k)
	for _, id := range first {
		if f[id] != 1 {
			t.Errorf("%s failed %d times, want once", id, f[id])
		}
	}
	if len(f) != 65 || f[before] != 1 {
		t.Errorf("%d objects have failed, and %s %d times; want the turn's 64, and the one that had, once", len(f), before, f[before])
	}
	if retry, _ := timersOf(r.s.queue, k); !slices.Contains(retry, before) || slices.Contains(retry, waits) || slices.Contains(retry, fresh) {
		t.Errorf("%d objects wait for a retry; want the one that did before among them, and none of the rest else", len(retry))
	}
	if n := r.metrics.waitingGauge(kind); n != 1 {
		t.Errorf("%d waiting, want the Secret still: the turn didn't get to it", n)
	}
	// The next turn gets to them: the one that had failed is decided.
	r.s.hooks.released = nil
	r.turn(context.Background())
	if got, want := l.take(), []ID{waits, before, fresh}; !slices.Equal(got, want) {
		t.Errorf("the next turn read %v, want %v", got, want)
	}
	if f := failuresOf(r.s.queue, k); f[before] != 0 {
		t.Errorf("the one that had failed is remembered after a turn decided it: %d", f[before])
	}
}

// A batch of a whole namespace lists it once, whatever the turns its objects
// take, and they go behind what was queued before it.
func TestAWholeNamespacesTurns(t *testing.T) {
	var objs []runtime.Object
	ids := someIDs("out", "s", 70)
	for _, id := range ids {
		objs = append(objs, heldObject(secretGVK, "out", id.Name, types.UID("u-"+id.Name), widgetGK, "out"))
	}
	r, _, l := quiet(t, objs...)
	k := batchKey{widgetGK, "out"}
	first, last := secretID("out", "z-first"), secretID("out", "a-last")
	r.s.queue.Add(k, first)
	r.s.queue.Add(k)
	r.s.queue.Add(k, last)
	for _, want := range [][]ID{append([]ID{first}, ids[:63]...), append(slices.Clone(ids[63:]), last)} {
		r.turn(context.Background())
		if got := l.take(); !slices.Equal(got, want) {
			t.Errorf("a turn read %v\nwant %v", got, want)
		}
	}
	lists := 0
	for _, a := range r.md.Actions() {
		if a.Matches("list", "secrets") && a.GetNamespace() == "out" {
			lists++
		}
	}
	if lists != 1 {
		t.Errorf("%d lists of the namespace's held Secrets, want one", lists)
	}
	for _, id := range ids {
		if r.heldBy(t, widgetGK, "out", id.Name) {
			t.Errorf("%s wasn't released", id)
		}
	}
}

// An object whose release patch meets a change is read again in its own turn,
// past the 64: put back, it would lose which resourceVersion its patch was built
// from, and with it the report of one rejected by an unchanged object.
func TestAConflictsReadIsItsTurns(t *testing.T) {
	r := newRig(t)
	ids := someIDs("ns", "s", 64)
	for _, id := range ids {
		addHeld(r, id)
	}
	changed := false
	r.srv.before = func(op, _, name string) error {
		if op != "patch" || name != ids[0].Name || changed {
			return nil
		}
		changed = true
		_, err := r.m.Patch(keyOf(ids[0]), types.MergePatchType, []byte(fmt.Sprintf(`{"metadata":{"finalizers":[%q,"example.com/other"]}}`, fNS)))
		must(err)
		return nil
	}
	res, err := r.p.release(context.Background(), "ns", ids)
	if err != nil || len(res.Released) != 64 || len(res.Rest) != 0 {
		t.Errorf("%d released, rest %v, %v; want all 64", len(res.Released), res.Rest, err)
	}
	if got := strings.Count(strings.Join(r.srv.calls, "\n")+"\n", "get Secret/"); got != 65 {
		t.Errorf("%d reads, want 65: the one whose patch met a change twice", got)
	}
}

// client-go's own rate limiter refuses a read at once when its wait would pass
// the read's deadline, with an error that isn't the context's. So what a turn's
// time leaves unread isn't asked for at all: no read of a slow client fails
// unread.
func TestASlowClientsTurnFailsNothingUnread(t *testing.T) {
	r := newRig(t)
	limiter := flowcontrol.NewTokenBucketRateLimiter(20, 1)
	defer limiter.Stop()
	r.p.srv = hookServer{server: r.srv, before: func(ctx context.Context, _ Call) error { return limiter.Wait(ctx) }}
	r.p.turnTime = 100 * time.Millisecond
	ids := someIDs("ns", "s", 10) // 450ms at 20 a second
	res, err := r.p.release(context.Background(), "ns", ids)
	if err != nil || len(res.Failed) != 0 {
		t.Errorf("failed %v, %v; want none: none was read", res.Failed, err)
	}
	if len(res.Bare) == 0 || len(res.Rest) == 0 || !slices.Equal(slices.Concat(res.Bare, res.Rest), ids) {
		t.Errorf("read %v, rest %v; want the first few, and the others", res.Bare, res.Rest)
	}
}
