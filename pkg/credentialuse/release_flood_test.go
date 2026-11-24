package credentialuse

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testingclock "k8s.io/utils/clock/testing"
)

// A flood of names, as in the records of one holder that is edited again and
// again: what a turn costs doesn't grow with what waits (§6.6), and what is known
// to carry the finalizer doesn't wait behind it.

// namingViews count the objects that R0 reads.
type namingViews struct {
	views
	n *int
}

func (v namingViews) HoldersNaming(id ID, ns string) []*metav1.PartialObjectMetadata {
	*v.n++
	return v.views.HoldersNaming(id, ns)
}

// With 5,000 names waiting, every turn reads the caches for 2,048 at most, and
// 64 objects live; and each is read once, in the order they were added.
func TestATurnsWorkDoesntGrowWithWhatWaits(t *testing.T) {
	r, tr, l := quiet(t)
	inR0 := 0
	tr.proto.views = namingViews{tr.proto.views, &inR0}
	ids := someIDs("out", "s", 5000)
	slices.Reverse(ids)
	for _, id := range ids {
		r.s.queue.Add(batchKey{widgetGK, "out"}, id)
	}
	var got []ID
	for turn := 1; ; turn++ {
		inR0 = 0
		if _, ok := r.turn(context.Background()); !ok || turn > len(ids) {
			break
		}
		gets := l.take()
		if inR0 > 2048 || len(gets) > 64 {
			t.Fatalf("turn %d read the caches for %d objects, and %d live; want 2048 and 64 at most", turn, inR0, len(gets))
		}
		got = append(got, gets...)
	}
	if !slices.Equal(got, ids) {
		t.Errorf("%d reads of %d objects, want each of the %d once, in the order they were added", len(got), len(SortedIDs(got)), len(ids))
	}
}

// A held Secret whose last holder goes, behind 5,000 names of its own key, is
// released in the next turn.
func TestAHeldObjectDoesntWaitBehindAFlood(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"), heldObject(secretGVK, "ns", "held", "u1", widgetGK, "ns"), holderObject("ns", "h", secretID("ns", "held"), secretID("ns", "missing")))
	l := r.log()
	r.track(t, widgetGK)
	gate := make(chan struct{})
	r.s.hooks.holderEvent = func(*Tracker, *metav1.PartialObjectMetadata) { <-gate }
	r.run(t)
	held, k := secretID("ns", "held"), batchKey{widgetGK, "ns"}
	queued := func() bool { return slices.Contains(r.queued(widgetGK, "ns"), held) }
	eventually(t, "the Secret's batch", queued)
	r.drain()
	close(gate)
	named := []ID{held, secretID("ns", "missing")}
	eventually(t, "what the holder names to be queued, and no more", func() bool { return slices.Equal(r.queued(widgetGK, "ns"), named) })
	r.drain()

	r.s.queue.Add(k, someIDs("ns", "absent", 5000)...)
	if err := r.md.Tracker().Delete(widgetsGVR, "ns", "h"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the holder's going to queue its Secret", queued)
	l.take()
	r.turn(context.Background())
	if got := l.take(); len(got) == 0 || got[0] != held {
		t.Errorf("the turn read %d objects, the Secret %v; want it first", len(got), slices.Contains(got, held))
	}
	if r.heldBy(t, widgetGK, "ns", "held") {
		t.Error("the Secret wasn't released")
	}
}

// The queue's two lines: a worker takes 1,024 of each at most, in their order,
// and what it doesn't take, or puts back, is taken next.
func TestTheQueuesLines(t *testing.T) {
	ctx := context.Background()
	k, names := batchKey{widgetGK, "ns"}, someIDs("ns", "a", 3000)
	x, y := secretID("ns", "x"), secretID("ns", "y")
	setup := func() (*batchQueue, *testingclock.FakeClock) {
		clk := testingclock.NewFakeClock(time.Now())
		q := newBatchQueue(nil)
		q.clock = &unlockedClock{FakeClock: clk, t: t, q: q}
		q.Add(k, names...)
		return q, clk
	}
	get := func(q *batchQueue) []ID {
		t.Helper()
		if q.Len() != 1 {
			t.Fatalf("%d batches wait, want one", q.Len())
		}
		_, ids, _ := q.Get(ctx)
		return ids
	}
	want := func(what string, got, want []ID) {
		t.Helper()
		if !slices.Equal(got, want) {
			t.Errorf("%s: a batch of %d, want %d; %v %v", what, len(got), len(want), got[:min(3, len(got))], want[:min(3, len(want))])
		}
	}

	q, _ := setup()
	q.AddHeld(k, x)
	first := get(q)
	want("with one held", first, append(slices.Clone(names[:1024]), x))
	if !q.held(k, x) || q.held(k, names[0]) {
		t.Error("want the one added so known to be held, and no other")
	}
	// What is put back, and one added again meanwhile, at their old places.
	q.Add(k, names[1023], y)
	q.PutBack(k, names[1020:1024]...)
	q.PutBack(k)
	q.Done(k)
	second := get(q)
	want("after four were put back", second, names[1020:2044])
	q.PutBack(k, second[1:]...)
	q.Done(k)
	want("after all but the first was", get(q), append(slices.Clone(second[1:]), names[2044]))
	q.Done(k)
	rest := get(q)
	want("the rest", rest, append(slices.Clone(names[2045:]), y))
	q.Done(k)
	if q.Len() != 0 || len(q.pending)+len(q.lines)+len(q.taken) != 0 {
		t.Errorf("%d batches wait, %d pending, %d lines, %d taken; want none: none is taken twice", q.Len(), len(q.pending), len(q.lines), len(q.taken))
	}

	// Found to be held once it waits, it is in both lines, and is taken once.
	q, _ = setup()
	q.Add(k, x)
	q.AddHeld(k, x)
	q.Add(k, x)
	want("one found to be held", get(q), append(slices.Clone(names[:1024]), x))
	q.Done(k)
	var after []ID
	for q.Len() > 0 {
		after = append(after, get(q)...)
		q.Done(k)
	}
	want("then", after, names[1024:])

	// One taken by one line, and added again, is taken at its new place, not at its
	// old one in the other line.
	q = newBatchQueue(testingclock.NewFakeClock(time.Now()))
	q.Add(k, names[:1500]...)
	q.Add(k, x)
	q.Add(k, names[1500:]...)
	q.AddHeld(k, x)
	get(q)
	q.Done(k)
	q.Add(k, x)
	want("one added again", get(q), names[1024:2048])
	q.Done(k)

	// A namespace's list is of the held line, and so is what it found.
	q, clk := setup()
	q.Add(k)
	if got := get(q); !slices.Contains(got, fillAll) || q.held(k, x) {
		t.Error("want the namespace's list in the first batch, and nothing it didn't find known")
	}
	q.Known(k, x)
	if !q.held(k, x) || q.held(k, names[0]) {
		t.Error("want what the list found known to be held, and no other")
	}
	q.Fail(k, fillAll) // its retry too
	q.PutBack(k, x)
	q.Done(k)
	want("what the list found, put back", get(q), append(slices.Clone(names[1024:2048]), x)) // at the list's place, after the names
	q.Done(k)
	clk.Step(releaseBackoffBase)
	want("the list's retry", get(q), append(slices.Clone(names[2048:]), fillAll))
	q.Done(k)

	// More than a worker takes of it, what was put back of one list is taken in the
	// list's order, though it shares a place.
	q = newBatchQueue(testingclock.NewFakeClock(time.Now()))
	q.Add(k)
	get(q)
	q.Known(k, names...)
	q.PutBack(k, names...)
	q.Done(k)
	for i := 0; i < 2048; i += 1024 {
		want(fmt.Sprintf("a list's objects from %d", i), get(q), SortedIDs(names[i:i+1024]))
		q.Done(k)
	}

	// A failure's retry is of the line its object was in; a time asked for, and a
	// grace's end, are of the held one.
	q, clk = setup()
	q.AddHeld(k, x)
	get(q)
	q.Fail(k, x, names[0])
	q.AddAt(k, clk.Now().Add(2*time.Second), y)
	q.Done(k)
	clk.Step(releaseBackoffBase)
	want("a held object's retry", get(q), append(slices.Clone(names[1024:2048]), x))
	q.Done(k)
	q.Add(k, names[:1024]...)
	q.Add(k, someIDs("ns", "b", 1024)...)
	q.AddAfter(k, time.Second, secretID("ns", "z"))
	clk.Step(time.Second)
	if got := get(q); !slices.Contains(got, y) || !slices.Contains(got, secretID("ns", "z")) || len(got) != 1026 {
		t.Errorf("a batch of %d without the time asked for or the grace's end, want both with 1024 others", len(got))
	}
	q.ShutDown()
	if len(q.pending)+len(q.lines)+len(q.taken) != 0 {
		t.Error("ShutDown leaves something")
	}
	q.AddHeld(k, x)
	q.PutBack(k, x)
	if q.Len() != 0 || len(q.pending) != 0 {
		t.Error("something is queued after ShutDown")
	}
}
