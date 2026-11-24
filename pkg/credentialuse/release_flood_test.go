package credentialuse

import (
	"context"
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	r := newFakeRig(t, namespaceObject("ns"), heldObject(secretGVK, "ns", "held", "u1", widgetGK, "ns"), holderObject("ns", "h", secretID("ns", "held")))
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
	eventually(t, "the holder's batch", queued)
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
