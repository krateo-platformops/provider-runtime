package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
	testingclock "k8s.io/utils/clock/testing"
)

// What the write loops' start makes up for, of the names that the queue of a process
// without them dropped (§6.2).

// floodedRig is a rig on objs whose write loops don't run, with a flood in
// namespace ns.
func floodedRig(t *testing.T, o SharedOptions, ns string, objs ...runtime.Object) *fakeRig {
	t.Helper()
	r := newFakeRigWith(t, o, objs...)
	r.track(t, widgetGK)
	r.run(t)
	for i := range 20 {
		r.s.queue.Add(batchKey{widgetGK, ns}, someIDs(ns, fmt.Sprintf("flood%d-", i), 4000)...)
	}
	return r
}

// unlabelled is m without its labels, as a third party's replace leaves it: the
// caches and the labelled lists don't show it.
func unlabelled(m *metav1.PartialObjectMetadata) *metav1.PartialObjectMetadata {
	m.Labels = nil
	return m
}

// An election right after a flood releases every hold that nothing uses, whether
// the caches show it, only a live list does, or neither does: one whose labels were
// stripped, in the flood's namespace and outside the caches, and one that a
// holder's record still names. A hold with a user is kept.
func TestAnElectionAfterAFloodReleasesWhatIsHeld(t *testing.T) {
	old := time.Now().Add(-time.Hour)
	for _, c := range []struct {
		what string
		o    SharedOptions
		objs []runtime.Object
		gone map[string]string // the namespace of each Secret to be released
	}{
		{"cached", SharedOptions{}, []runtime.Object{heldObject(secretGVK, "ns", "s", "u", widgetGK, "ns")}, map[string]string{"s": "ns"}},
		{"its labels stripped", SharedOptions{}, []runtime.Object{unlabelled(heldObject(secretGVK, "ns", "s", "u", widgetGK, "ns"))}, map[string]string{"s": "ns"}},
		{"outside the caches", SharedOptions{Namespaces: []string{"in"}}, []runtime.Object{deleting(heldObject(secretGVK, "out", "s", "u", widgetGK, "ns"), old)}, map[string]string{"s": "out"}},
		{"outside the caches, its labels stripped", SharedOptions{Namespaces: []string{"in"}}, []runtime.Object{unlabelled(heldObject(secretGVK, "out", "s", "u", widgetGK, "ns"))}, map[string]string{"s": "out"}},
		{"two, beside one with a user", SharedOptions{}, []runtime.Object{
			unlabelled(heldObject(secretGVK, "ns", "s", "u", widgetGK, "ns")),
			unlabelled(heldObject(secretGVK, "other", "s2", "u2", widgetGK, "ns")),
			unlabelled(heldObject(secretGVK, "ns", "used", "u3", widgetGK, "ns")),
			holderObject("ns", "h", secretID("ns", "used")),
		}, map[string]string{"s": "ns", "s2": "other"}},
	} {
		t.Run(c.what, func(t *testing.T) {
			objs := append([]runtime.Object{namespaceObject("ns"), namespaceObject("in"), namespaceObject("out"), namespaceObject("other")}, c.objs...)
			r := floodedRig(t, c.o, "ns", objs...)
			// The names these holds' holders had, dropped with the flood's.
			for name, ns := range c.gone {
				r.s.queue.Add(batchKey{widgetGK, "ns"}, secretID(ns, name))
			}
			r.runWrites(t)
			for name, ns := range c.gone {
				releasedStepping(t, r, "Secret "+ns+"/"+name+" to be released", func() bool {
					return !hasFinalizer(r.secretMeta(t, ns, name), fWidgetNS)
				})
			}
			if c.what == "two, beside one with a user" && !hasFinalizer(r.secretMeta(t, "ns", "used"), fWidgetNS) {
				t.Error("the hold with a user was released")
			}
		})
	}
}

// fullLists is how many lists of Secrets in every namespace, with no selector, were
// made.
func (r *fakeRig) fullLists() int {
	n := 0
	for _, a := range r.md.Actions() {
		if l, ok := a.(clienttesting.ListAction); ok && a.GetResource().Resource == "secrets" && a.GetNamespace() == "" && l.GetListRestrictions().Labels.Empty() {
			n++
		}
	}
	return n
}

// Nothing is listed without a selector unless names were dropped; and then once,
// however many namespaces were collapsed, and not again at the loops' next start.
func TestTheFullListIsForAnOverflow(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"))
	r.track(t, widgetGK)
	r.run(t)
	r.s.queue.Add(batchKey{widgetGK, "ns"}, someIDs("ns", "s", 100)...)
	stop := r.runWrites(t)
	eventually(t, "the queue to be drained", func() bool { _, _, all := waitingNames(r.s.queue); return all == 0 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if n := r.fullLists(); n != 0 {
		t.Fatalf("%d full lists, and nothing was dropped", n)
	}
	for _, ns := range []string{"ns", "ns2"} {
		for i := range 20 {
			r.s.queue.Add(batchKey{widgetGK, ns}, someIDs(ns, fmt.Sprintf("flood%d-", i), 4000)...)
		}
	}
	stop = r.runWrites(t)
	eventually(t, "the full list", func() bool { return r.fullLists() == 1 && len(r.s.queue.collapsedKeys(widgetGK)) == 0 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	stop = r.runWrites(t)
	eventually(t, "the queue to be drained again", func() bool { _, _, all := waitingNames(r.s.queue); return all == 0 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if n := r.fullLists(); n != 1 {
		t.Errorf("%d full lists, want one", n)
	}
}

// A full list that fails is retried; and one that the write loops' end interrupts
// is made by the next ones.
func TestTheFullListIsRetried(t *testing.T) {
	r := floodedRig(t, SharedOptions{}, "ns", namespaceObject("ns"), unlabelled(heldObject(secretGVK, "ns", "s", "u", widgetGK, "ns")))
	r.s.queue.Add(batchKey{widgetGK, "ns"}, secretID("ns", "s"))
	var tries atomic.Int32
	r.md.PrependReactor("list", "secrets", func(a clienttesting.Action) (bool, runtime.Object, error) {
		if l := a.(clienttesting.ListAction); a.GetNamespace() == "" && l.GetListRestrictions().Labels.Empty() && tries.Add(1) <= 2 {
			return true, nil, apierrors.NewInternalError(errors.New("etcd is unhappy"))
		}
		return false, nil, nil
	})
	stop := r.runWrites(t)
	eventually(t, "the first try", func() bool { return tries.Load() == 1 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if len(r.s.queue.collapsedKeys(widgetGK)) != 1 {
		t.Fatal("the next write loops won't list: the collapse is forgotten")
	}
	r.runWrites(t)
	releasedStepping(t, r, "the hold to be released", func() bool { return !hasFinalizer(r.secretMeta(t, "ns", "s"), fWidgetNS) })
}

// A kind that can't be listed in full doesn't hold up the others, which aren't listed
// again with it.
func TestAKindThatCantBeListedInFullStopsNoOther(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"), unlabelled(heldObject(secretGVK, "ns", "s", "u", widgetGK, "ns")))
	var tries atomic.Int32
	r.md.PrependReactor("list", "configmaps", func(a clienttesting.Action) (bool, runtime.Object, error) {
		if l := a.(clienttesting.ListAction); a.GetNamespace() == "" && l.GetListRestrictions().Labels.Empty() {
			tries.Add(1)
			return true, nil, apierrors.NewInternalError(errors.New("etcd is unhappy"))
		}
		return false, nil, nil
	})
	r.track(t, widgetGK, ProtectedKind{GVK: configMapGVK}, ProtectedKind{GVK: secretGVK})
	r.run(t)
	for i := range 20 {
		r.s.queue.Add(batchKey{widgetGK, "ns"}, someIDs("ns", fmt.Sprintf("flood%d-", i), 4000)...)
	}
	r.runWrites(t)
	releasedStepping(t, r, "the Secret to be released", func() bool { return !hasFinalizer(r.secretMeta(t, "ns", "s"), fWidgetNS) })
	releasedStepping(t, r, "the other kind's list to be retried", func() bool { return tries.Load() >= 3 })
	if n := r.fullLists(); n != 1 {
		t.Errorf("%d full lists of Secrets, want one", n)
	}
	if len(r.s.queue.collapsedKeys(widgetGK)) != 1 {
		t.Error("the collapse is settled, and a kind wasn't listed")
	}
}

// What a cached holder's record names, and the flood dropped, is queued again: a
// missing Secret it names is read, and so would one of a kind that no list covers.
func TestTheRecordsOfACollapsedNamespaceAreQueuedAgain(t *testing.T) {
	named, elsewhere := secretID("ns", "missing"), secretID("ns2", "missing")
	cm := ID{Kind: "ConfigMap", Namespace: "ns", Name: "cm"}
	r := newFakeRig(t, namespaceObject("ns"), namespaceObject("ns2"), holderObject("ns", "h", named, cm), holderObject("ns2", "h2", elsewhere))
	l := r.log()
	r.track(t, widgetGK)
	r.run(t)
	eventually(t, "the records' names", func() bool {
		return len(r.queued(widgetGK, "ns")) == 2 && len(r.queued(widgetGK, "ns2")) == 1
	})
	r.drain()
	for i := range 20 {
		r.s.queue.Add(batchKey{widgetGK, "ns"}, someIDs("ns", fmt.Sprintf("flood%d-", i), 4000)...)
	}
	r.runWrites(t)
	var got []ID
	eventually(t, "what the collapsed namespace's record names to be read", func() bool {
		got = append(got, l.take()...)
		return slices.Contains(got, named) && slices.Contains(got, cm)
	})
	eventually(t, "the queue to be idle", func() bool {
		r.s.queue.mu.Lock()
		defer r.s.queue.mu.Unlock()
		return len(r.s.queue.pending)+len(r.s.queue.working) == 0
	})
	if got = append(got, l.take()...); slices.Contains(got, elsewhere) {
		t.Error("what another namespace's record names was queued too")
	}
}

// A kind whose Track ends after the write loops' start has what was dropped for it
// made up then.
func TestATrackAfterTheStartMakesUpForItsFlood(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"), unlabelled(heldObject(secretGVK, "ns", "s", "u", gadgetGK, "ns")))
	r.track(t, widgetGK)
	r.run(t)
	for i := range 20 {
		r.s.queue.Add(batchKey{gadgetGK, "ns"}, someIDs("ns", fmt.Sprintf("flood%d-", i), 4000)...)
	}
	r.runWrites(t)
	eventually(t, "the write loops to start", func() bool {
		r.s.mu.Lock()
		defer r.s.mu.Unlock()
		return r.s.writeCtx != nil
	})
	if _, err := r.s.Track(context.Background(), r.options(gadgetGK)); err != nil {
		t.Fatal(err)
	}
	releasedStepping(t, r, "the hold to be released", func() bool {
		return !hasFinalizer(r.secretMeta(t, "ns", "s"), mustFinalizer(gadgetGK, "ns"))
	})
}

// While the write loops run, no name is dropped; and once they stop, what waits
// over the bound is.
func TestTheBoundIsForAProcessWithoutWriteLoops(t *testing.T) {
	q := newBatchQueue(testingclock.NewFakeClock(time.Now()))
	k, small := batchKey{widgetGK, "ns"}, batchKey{widgetGK, "small"}
	q.setDraining(true)
	q.Add(small, someIDs("small", "s", 100)...)
	for i := range 20 {
		q.Add(k, someIDs("ns", fmt.Sprintf("s%d-", i), 4000)...)
	}
	if names, _, _ := waitingNames(q); names != 80100 {
		t.Errorf("%d names wait while the loops run, want all 80,100", names)
	}
	if got := q.collapsedKeys(widgetGK); len(got) != 0 {
		t.Errorf("%v collapsed while the loops run", got)
	}
	q.setDraining(false)
	if _, by, _ := waitingNames(q); by[k] != 0 || by[small] != 100 {
		t.Errorf("after they stop, %d and %d names wait, want none of the many, and the 100", by[k], by[small])
	}
	if _, waits := q.pending[k][fillAll]; !waits {
		t.Error("the list of the collapsed namespace doesn't wait")
	}
	if got := q.collapsedKeys(widgetGK); !slices.Equal(got, []batchKey{k}) || len(q.collapsedKeys(gadgetGK)) != 0 {
		t.Errorf("collapsed %v, want %v", got, k)
	}
	q.setDraining(true)
	q.Add(k, secretID("ns", "again"))
	if _, by, _ := waitingNames(q); by[k] != 1 {
		t.Error("a name added while the loops run isn't kept, for a key that had collapsed")
	}
	q.settleCollapsed(k)
	if len(q.collapsedKeys(widgetGK)) != 0 {
		t.Error("the collapse is remembered once it is made up for")
	}
}

// The count of the names that wait is right whatever happens to them: taken, put
// back, promoted, added again, collapsed, dropped, or lost with ShutDown.
func TestTheCountOfWaitingNames(t *testing.T) {
	ctx := context.Background()
	q := newBatchQueue(testingclock.NewFakeClock(time.Now()))
	k := batchKey{widgetGK, "ns"}
	check := func(what string) {
		t.Helper()
		want, by, _ := waitingNames(q)
		q.mu.Lock()
		defer q.mu.Unlock()
		if q.names != want {
			t.Errorf("%s: %d names counted, want %d", what, q.names, want)
		}
		for k, ls := range q.lines {
			if ls.names != by[k] {
				t.Errorf("%s: %d names counted for %v, want %d", what, ls.names, k, by[k])
			}
		}
	}
	names := someIDs("ns", "s", 3000)
	q.Add(k, names...)
	check("added")
	q.Add(k, names[:10]...)
	check("added again")
	q.AddHeld(k, names[:20]...)
	q.AddHeld(k, secretID("ns", "held"))
	check("promoted")
	_, ids, _ := q.Get(ctx)
	check("taken")
	q.Add(k, names[100:110]...)
	q.AddHeld(k, names[110:115]...)
	check("added again while worked on")
	q.PutBack(k, ids...)
	check("put back")
	q.PutBack(k, ids...)
	check("put back twice")
	q.Done(k)
	for q.Len() > 0 {
		_, _, _ = q.Get(ctx)
		q.Done(k)
		check("taken again")
	}
	q.Add(k)
	check("the namespace's list")
	for i := range 20 {
		q.Add(k, someIDs("ns", fmt.Sprintf("s%d-", i), 4000)...)
		check("a flood")
	}
	q.Add(k, names...)
	check("dropped")
	q.AddHeld(k, names[0])
	check("held, of a collapsed key")
	q.ShutDown()
	check("shut down")
}

// A PerName kind isn't listed, which its RBAC would refuse: the index names all that
// is held, whatever its labels.
func TestAPerNameKindIsntListedAfterAFlood(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"))
	tr := r.track(t, widgetGK)
	tr.o.Protected = []ProtectedKind{{GVK: secretGVK, Watch: PerName}}
	for i := range 20 {
		r.s.queue.Add(batchKey{widgetGK, "ns"}, someIDs("ns", fmt.Sprintf("flood%d-", i), 4000)...)
	}
	p, ok := r.s.collapsedPart([]*Tracker{tr})
	if !ok {
		t.Fatal("nothing to make up for")
	}
	if err := p.run(context.Background()); err != nil || r.fullLists() != 0 || len(r.s.queue.collapsedKeys(widgetGK)) != 0 {
		t.Errorf("%v, %d full lists, and %v still collapsed; want none of them", err, r.fullLists(), r.s.queue.collapsedKeys(widgetGK))
	}
}
