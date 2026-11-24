package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clienttesting "k8s.io/client-go/testing"
	testingclock "k8s.io/utils/clock/testing"
)

// What the write loops' start makes up for, of the names that the queue of a process
// without them dropped (§6.2).

// collapsedOf are the keys of holder kind gk that collapsed, without the kind.
func collapsedOf(q *batchQueue, gk schema.GroupKind) []batchKey {
	keys, _ := q.collapsedOf(gk)
	return keys
}

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
	eventually(t, "the full list", func() bool { return r.fullLists() == 1 && len(collapsedOf(r.s.queue, widgetGK)) == 0 })
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
	if len(collapsedOf(r.s.queue, widgetGK)) != 1 {
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
	if len(collapsedOf(r.s.queue, widgetGK)) != 1 {
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
	if got := collapsedOf(q, widgetGK); len(got) != 0 {
		t.Errorf("%v collapsed while the loops run", got)
	}
	q.setDraining(false)
	if _, by, _ := waitingNames(q); by[k] != 0 || by[small] != 100 {
		t.Errorf("after they stop, %d and %d names wait, want none of the many, and the 100", by[k], by[small])
	}
	if _, waits := q.pending[k][fillAll]; !waits {
		t.Error("the list of the collapsed namespace doesn't wait")
	}
	if got := collapsedOf(q, widgetGK); !slices.Equal(got, []batchKey{k}) || len(collapsedOf(q, gadgetGK)) != 0 {
		t.Errorf("collapsed %v, want %v", got, k)
	}
	q.setDraining(true)
	q.Add(k, secretID("ns", "again"))
	if _, by, _ := waitingNames(q); by[k] != 1 {
		t.Error("a name added while the loops run isn't kept, for a key that had collapsed")
	}
	q.settleCollapsed(widgetGK, []batchKey{k}, false)
	if len(collapsedOf(q, widgetGK)) != 0 {
		t.Error("the collapse is remembered once it is made up for")
	}
}

// counted checks that q counts what waits.
func counted(t *testing.T, what string, q *batchQueue) {
	t.Helper()
	q.mu.Lock()
	defer q.mu.Unlock()
	names, bytes := 0, 0
	for k, set := range q.pending {
		own := 0
		for id := range set {
			if id != fillAll {
				names++
				own += costOf(id)
			}
		}
		bytes += own
		if q.lines[k] == nil || q.lines[k].bytes != own {
			t.Errorf("%s: %+v counted for %v, want %d bytes", what, q.lines[k], k, own)
		}
	}
	if q.names != names || q.bytes != bytes || len(q.lines) != len(q.pending) {
		t.Errorf("%s: %d names and %d bytes counted, %d lines; want %d, %d and %d", what, q.names, q.bytes, len(q.lines), names, bytes, len(q.pending))
	}
}

// The count of what waits is right whatever happens to it: taken, put back, promoted,
// added again, collapsed, dropped, or lost with ShutDown.
func TestTheCountOfWaitingNames(t *testing.T) {
	ctx := context.Background()
	q := newBatchQueue(testingclock.NewFakeClock(time.Now()))
	k := batchKey{widgetGK, "ns"}
	check := func(what string) { t.Helper(); counted(t, what, q) }
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
	q.AddHeld(k, names[0])
	check("dropped, of a collapsed key")
	q.max.keys = 2
	for _, ns := range []string{"a", "b", "c"} {
		q.Add(batchKey{gadgetGK, ns}, names[0])
		check("a kind's collapse")
	}
	q.setDraining(true)
	q.Add(k, names...)
	q.Fail(k, names[0], secretID("ns", "failed"))
	q.AddAt(k, time.Now().Add(time.Hour), secretID("ns", "asked"))
	check("failed and asked for")
	q.setDraining(false)
	check("the loops' end")
	q.ShutDown()
	check("shut down")
}

// The end of the write loops leaves no timer for a retry or a time, and no failure:
// what they were for waits, as known as it was.
func TestTheLoopsEndLeavesNoRetries(t *testing.T) {
	clk := testingclock.NewFakeClock(time.Now())
	q := newBatchQueue(nil)
	q.clock = &unlockedClock{FakeClock: clk, t: t, q: q}
	k, a, b, c := batchKey{widgetGK, "ns"}, secretID("ns", "a"), secretID("ns", "b"), secretID("ns", "c")
	q.setDraining(true)
	q.AddHeld(k, a)
	_, _, _ = q.Get(context.Background())
	q.Fail(k, a, b)
	q.AddAt(k, clk.Now().Add(time.Hour), c)
	q.AddAfter(k, time.Hour, secretID("ns", "grace"))
	q.Done(k)
	q.setDraining(false)
	if len(q.failing)+len(q.asked) != 0 || len(q.timers) != 1 {
		t.Errorf("%d keys with failures, %d times asked for, %d timers; want none, and the grace's timer", len(q.failing), len(q.asked), len(q.timers))
	}
	for id, held := range map[ID]bool{a: true, b: false, c: true} {
		if p, waits := q.pending[k][id]; !waits || p.held != held {
			t.Errorf("%s waits %v, known %v; want it to wait, known %v", id, waits, p.held, held)
		}
	}
	counted(t, "the loops' end", q)
	clk.Step(30 * time.Minute)
	if got := len(q.pending[k]); got != 3 {
		t.Errorf("%d wait once the retries' times passed, want the three", got)
	}
}

// Every order of adding to three keys of two kinds, in either line, of taking a batch
// and giving it back or not, and of the write loops' start and end, with a bound of
// two names and two keys: the counts are right; the bound holds without the loops;
// and nothing is lost but what a collapse dropped, which is remembered.
func TestTheBoundInEveryOrder(t *testing.T) {
	keys := map[string]batchKey{"A": {widgetGK, "a"}, "B": {widgetGK, "b"}, "C": {gadgetGK, "c"}}
	for _, spec := range []string{
		"add:A1 add:A2 held:A3 add:B1 get1 loops ends",
		"add:A1 held:B1 add:C1 add:C2 get1 back1 loops ends",
		"held:A1 held:A2 add:A3 add:B1 add:C1 loops get1 ends",
		"add:A1 add:A1 held:A1 add:A2 loops ends get1 back1",
	} {
		orders := permutations(ops(spec), func(seq []enumOp) {
			q := newBatchQueue(testingclock.NewFakeClock(time.Now()))
			q.max = queueMax{names: 2, bytes: 1 << 20, keys: 2}
			owed := map[batchKey]map[ID]bool{}
			var held batchKey
			var has []ID
			name := ""
			for _, o := range seq {
				name += " " + o.String()
				k := keys[o.key]
				switch o.kind {
				case "add", "held":
					id := secretID(k.Namespace, o.id)
					if o.kind == "add" {
						q.Add(k, id)
					} else {
						q.AddHeld(k, id)
					}
					if owed[k] == nil {
						owed[k] = map[ID]bool{}
					}
					owed[k][id] = true
				case "loops":
					q.setDraining(true)
				case "ends":
					q.setDraining(false)
				case "get":
					if has == nil && q.Len() > 0 {
						held, has, _ = q.Get(context.Background())
						for _, id := range has {
							delete(owed[held], id)
						}
					}
				case "back":
					if has != nil {
						q.PutBack(held, has...)
						for _, id := range has {
							if owed[held] == nil {
								owed[held] = map[ID]bool{}
							}
							if id != fillAll {
								owed[held][id] = true
							}
						}
					}
				case "done":
					if has != nil {
						q.Done(held)
						has = nil
					}
				}
				counted(t, name, q)
				// What a worker puts back isn't kept out: it does so while they run.
				if !q.draining && o.kind != "back" && !strings.Contains(name, "back") && (q.names > 2 || len(q.pending) > 2) {
					t.Fatalf("%s: %d names of %d keys wait without the write loops, want two of each at most", name, q.names, len(q.pending))
				}
				for k, ids := range owed {
					for id := range ids {
						if _, waits := q.pending[k][id]; !waits && !q.collapsed[k] && !q.kinds[k.Holder] {
							t.Fatalf("%s: %s of %v neither waits nor was dropped", name, id, k)
						}
					}
				}
				for k := range q.collapsed {
					if _, waits := q.pending[k][fillAll]; !waits && !slices.Contains(has, fillAll) && !strings.Contains(name, "get") {
						t.Fatalf("%s: %v collapsed, and its namespace's list doesn't wait", name, k)
					}
				}
			}
		})
		t.Logf("%s: %d orders", spec, orders)
	}
}

// The full list, which a flood causes and its size delays, is made after everything
// else of the recheck, of any kind.
func TestTheFullListIsMadeLast(t *testing.T) {
	r := floodedRig(t, SharedOptions{Namespaces: []string{"in"}}, "ns", namespaceObject("ns"), namespaceObject("in"))
	parts := r.s.recheckParts(r.s.trackerList())
	for i, p := range parts {
		if n := r.fullLists(); n != 0 {
			t.Errorf("part %d of %d comes after the full list", i+1, len(parts))
		}
		if err := p.run(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if n := r.fullLists(); n != 1 {
		t.Errorf("%d full lists, want one", n)
	}
}

// A holder kind that collapsed as a whole, with more keys than are kept, is made up for
// in every namespace: a hold whose labels were stripped, and what a record names.
func TestAnElectionAfterAKindsCollapse(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"), unlabelled(heldObject(secretGVK, "ns", "s", "u", widgetGK, "ns")), holderObject("ns", "h", secretID("ns", "missing")))
	l := r.log()
	r.track(t, widgetGK)
	r.run(t)
	eventually(t, "the record's name", func() bool { return len(r.queued(widgetGK, "ns")) == 1 })
	for i := range standbyMax.keys + 1 {
		ns := fmt.Sprintf("forged%d", i)
		r.s.queue.AddHeld(batchKey{widgetGK, ns}, secretID("ns", "s"))
	}
	if keys, all := r.s.queue.collapsedOf(widgetGK); !all || len(keys) != 0 || r.s.queue.Len() != 0 {
		t.Fatalf("%v collapsed, the kind %v, with %d batches; want the kind, and nothing to wait", keys, all, r.s.queue.Len())
	}
	r.s.queue.Add(batchKey{widgetGK, "ns"}, secretID("ns", "s"))
	r.s.queue.Add(batchKey{widgetGK, "ns"})
	if n := r.s.queue.Len(); n != 0 {
		t.Fatalf("%d batches of the collapsed kind wait", n)
	}
	r.runWrites(t)
	releasedStepping(t, r, "the hold to be released", func() bool { return !hasFinalizer(r.secretMeta(t, "ns", "s"), fWidgetNS) })
	var got []ID
	eventually(t, "what the record names to be read", func() bool {
		got = append(got, l.take()...)
		return slices.Contains(got, secretID("ns", "missing"))
	})
	eventually(t, "the collapse to be settled", func() bool { _, all := r.s.queue.collapsedOf(widgetGK); return !all })
}

// A PerName kind isn't listed, which its RBAC would refuse. (That the index names all
// that is held, whatever its labels, is the envtest's,
// TestAnElectionAfterAFloodReleasesWhatTheIndexNames.)
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
	if err := p.run(context.Background()); err != nil || r.fullLists() != 0 || len(collapsedOf(r.s.queue, widgetGK)) != 0 {
		t.Errorf("%v, %d full lists, and %v still collapsed; want none of them", err, r.fullLists(), collapsedOf(r.s.queue, widgetGK))
	}
}
