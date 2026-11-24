package credentialuse

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

// A holder whose record can't be parsed counts as a user of every object held
// for its namespace (§4, §6.1). Its record names nothing, so nothing is queued
// for what it kept, unless its ceasing to be such a user queues the whole
// namespace (§10.2).

// unparseable gives m a record that can't be parsed.
func unparseable(m *metav1.PartialObjectMetadata) *metav1.PartialObjectMetadata {
	m.Annotations = map[string]string{AnnotationRefs: "oops"}
	return m
}

// updateHolder stores holder m in the rig's API server.
func (r *fakeRig) updateHolder(t *testing.T, m *metav1.PartialObjectMetadata) {
	t.Helper()
	if err := r.md.Tracker().Update(widgetsGVR, m, m.Namespace); err != nil {
		t.Fatal(err)
	}
}

func (r *fakeRig) deleteHolder(t *testing.T, ns, name string) {
	t.Helper()
	if err := r.md.Tracker().Delete(widgetsGVR, ns, name); err != nil {
		t.Fatal(err)
	}
}

// settled gives the write loops time to take what is queued, on both clocks:
// a batch that keeps an object makes no call to wait for.
func (r *fakeRig) settled() {
	for range 5 {
		time.Sleep(50 * time.Millisecond)
		r.clk.Step(10 * time.Minute)
	}
	time.Sleep(50 * time.Millisecond)
}

func TestAnUnparseableHolderThatGoesReleasesWhatItKept(t *testing.T) {
	past := time.Now().Add(-time.Hour)
	s := func() *metav1.PartialObjectMetadata {
		return heldObject(secretGVK, "ns", "s", "u-s", widgetGK, "ns")
	}
	id := secretID("ns", "s")
	for name, tc := range map[string]struct {
		secret, namespace, holder *metav1.PartialObjectMetadata
		// other is a second holder.
		other *metav1.PartialObjectMetadata
		// act ends the unparseable holder's use.
		act func(t *testing.T, r *fakeRig)
		// kept: act leaves the hold, which then's the control releases.
		kept bool
		then func(t *testing.T, r *fakeRig)
	}{
		"in a teardown, deleted": {
			secret: deleting(s(), past), namespace: deleting(namespaceObject("ns"), past),
			holder: deleting(unparseable(holderObject("ns", "h")), past),
			act:    func(t *testing.T, r *fakeRig) { r.deleteHolder(t, "ns", "h") },
		},
		"in a teardown, without its owner finalizer": {
			secret: deleting(s(), past), namespace: deleting(namespaceObject("ns"), past),
			holder: deleting(unparseable(holderObject("ns", "h")), past),
			act: func(t *testing.T, r *fakeRig) {
				h := deleting(unparseable(holderObject("ns", "h")), past)
				h.Finalizers = []string{"example.com/else"}
				r.updateHolder(t, h)
			},
		},
		"deleted": {
			holder: unparseable(holderObject("ns", "h")),
			act:    func(t *testing.T, r *fakeRig) { r.deleteHolder(t, "ns", "h") },
		},
		"deleting, never with an owner finalizer": {
			holder: func() *metav1.PartialObjectMetadata {
				h := unparseable(holderObject("ns", "h"))
				h.Finalizers = []string{"example.com/else"}
				return h
			}(),
			act: func(t *testing.T, r *fakeRig) {
				h := deleting(unparseable(holderObject("ns", "h")), past)
				h.Finalizers = []string{"example.com/else"}
				r.updateHolder(t, h)
			},
		},
		"its record removed": {
			holder: unparseable(holderObject("ns", "h")),
			act:    func(t *testing.T, r *fakeRig) { r.updateHolder(t, holderObject("ns", "h")) },
		},
		"its record repaired, naming another": {
			holder: unparseable(holderObject("ns", "h")),
			act: func(t *testing.T, r *fakeRig) {
				r.updateHolder(t, holderObject("ns", "h", secretID("ns", "another")))
			},
		},
		"its record repaired, naming it": {
			holder: unparseable(holderObject("ns", "h")),
			act:    func(t *testing.T, r *fakeRig) { r.updateHolder(t, holderObject("ns", "h", id)) },
			kept:   true,
			then:   func(t *testing.T, r *fakeRig) { r.deleteHolder(t, "ns", "h") },
		},
		"its record still unparseable": {
			holder: unparseable(holderObject("ns", "h")),
			act: func(t *testing.T, r *fakeRig) {
				h := holderObject("ns", "h")
				h.Annotations = map[string]string{AnnotationRefs: "[oops"}
				r.updateHolder(t, h)
			},
			kept: true,
			then: func(t *testing.T, r *fakeRig) { r.deleteHolder(t, "ns", "h") },
		},
		"deleted, with another that names it": {
			holder: unparseable(holderObject("ns", "h")), other: holderObject("ns", "h2", id),
			act:  func(t *testing.T, r *fakeRig) { r.deleteHolder(t, "ns", "h") },
			kept: true,
			then: func(t *testing.T, r *fakeRig) { r.deleteHolder(t, "ns", "h2") },
		},
		"deleted, with another that is unparseable too": {
			holder: unparseable(holderObject("ns", "h")), other: unparseable(holderObject("ns", "h2")),
			act:  func(t *testing.T, r *fakeRig) { r.deleteHolder(t, "ns", "h") },
			kept: true,
			then: func(t *testing.T, r *fakeRig) { r.deleteHolder(t, "ns", "h2") },
		},
		"deleted, and recreated under its name without the record": {
			holder: unparseable(holderObject("ns", "h")),
			act: func(t *testing.T, r *fakeRig) {
				r.deleteHolder(t, "ns", "h")
				h := holderObject("ns", "h")
				h.UID = "w-again"
				if err := r.md.Tracker().Add(h); err != nil {
					t.Fatal(err)
				}
			},
		},
		"deleted, with a record the API server's cap allows": {
			holder: func() *metav1.PartialObjectMetadata {
				h := holderObject("ns", "h")
				h.Annotations = map[string]string{AnnotationRefs: "[" + strings.Repeat(`{"group":"","kind":"Secret","namespace":"a","name":"b"},`, 4000)}
				return h
			}(),
			act: func(t *testing.T, r *fakeRig) { r.deleteHolder(t, "ns", "h") },
		},
	} {
		t.Run(name, func(t *testing.T) {
			if tc.secret == nil {
				tc.secret, tc.namespace = s(), namespaceObject("ns")
			}
			r := newFakeRig(t, tc.namespace, tc.secret, tc.holder)
			if tc.other != nil {
				if err := r.md.Tracker().Add(tc.other); err != nil {
					t.Fatal(err)
				}
			}
			r.track(t, widgetGK)
			r.run(t)
			r.runWrites(t)
			r.settled()
			held := func() bool { return r.heldBy(t, widgetGK, "ns", "s") }
			if !held() {
				t.Fatal("the unparseable holder keeps nothing: the case shows nothing")
			}
			tc.act(t, r)
			if tc.kept {
				r.settled()
				if !held() {
					t.Fatal("released, though it has a user")
				}
				tc.then(t, r)
			}
			eventually(t, "the hold to be released", func() bool { return !held() })
		})
	}
}

// What a holder event queues, when a side of it can't be parsed.
func TestWhatAnUnparseableRecordsEventQueues(t *testing.T) {
	gone := time.Now()
	named := secretID("ns", "named")
	bad := func() *metav1.PartialObjectMetadata { return unparseable(holderObject("ns", "h")) }
	noOwner := func(m *metav1.PartialObjectMetadata) *metav1.PartialObjectMetadata {
		m.Finalizers = nil
		return m
	}
	recreated := func(m *metav1.PartialObjectMetadata) *metav1.PartialObjectMetadata {
		m.UID = "w-again"
		return m
	}
	for name, tc := range map[string]struct {
		old, cur *metav1.PartialObjectMetadata
		want     []ID
	}{
		"added":                              {nil, bad(), nil},
		"gone":                               {bad(), nil, []ID{fillAll}},
		"gone, deleting with its finalizer":  {deleting(bad(), gone), nil, []ID{fillAll}},
		"gone, which wasn't a user any more": {noOwner(deleting(bad(), gone)), nil, nil},
		"becomes unparseable":                {holderObject("ns", "h", named), bad(), []ID{named}},
		"repaired":                           {bad(), holderObject("ns", "h", named), []ID{fillAll, named}},
		"its record removed":                 {bad(), holderObject("ns", "h"), []ID{fillAll}},
		"starts to delete, with its owner finalizer":              {bad(), deleting(bad(), gone), nil},
		"starts to delete, without one":                           {noOwner(bad()), noOwner(deleting(bad(), gone)), []ID{fillAll}},
		"loses its owner finalizer, deleting":                     {deleting(bad(), gone), noOwner(deleting(bad(), gone)), []ID{fillAll}},
		"loses its owner finalizer, not deleting":                 {bad(), noOwner(bad()), nil},
		"recreated, unparseable":                                  {bad(), recreated(bad()), nil},
		"recreated, without a record":                             {bad(), recreated(holderObject("ns", "h")), []ID{fillAll}},
		"recreated, deleting, unparseable, without its finalizer": {bad(), recreated(noOwner(deleting(bad(), gone))), []ID{fillAll}},
	} {
		t.Run(name, func(t *testing.T) {
			r := newFakeRig(t)
			h := holderHandler{r.track(t, widgetGK)}
			switch {
			case tc.old == nil:
				h.OnAdd(tc.cur, false)
			case tc.cur == nil:
				h.OnDelete(cache.DeletedFinalStateUnknown{Key: "ns/h", Obj: tc.old})
			default:
				h.OnUpdate(tc.old, tc.cur)
			}
			if got := r.queued(widgetGK, "ns"); !slices.Equal(got, SortedIDs(tc.want)) {
				t.Errorf("queued %q, want %q", got, tc.want)
			}
		})
	}
	t.Run("a cluster-scoped holder", func(t *testing.T) {
		r := newFakeRig(t)
		tr := r.track(t, widgetGK)
		h := bad()
		h.Namespace = ""
		holderHandler{tr}.OnDelete(h)
		if got := r.queued(widgetGK, ""); !slices.Equal(got, []ID{fillAll}) {
			t.Errorf("queued %q, want the whole of its kind", got)
		}
		p := heldObject(secretGVK, "ns", "s", "u-s", widgetGK, "")
		if err := r.protectedOf(secretGroupKind).inf.GetStore().Add(p); err != nil {
			t.Fatal(err)
		}
		if got, err := r.s.heldIn(context.Background(), tr, ""); err != nil || !slices.Equal(got, []ID{secretID("ns", "s")}) {
			t.Errorf("heldIn = %v, %v; want what its kind holds", got, err)
		}
	})
}

// A hold in another namespace is released too, where the caches cover it.
// Outside them, nothing lists it: a limit (§10.2), which the write loops'
// next start ends.
func TestAnUnparseableHoldersHoldsInOtherNamespaces(t *testing.T) {
	for name, tc := range map[string]struct {
		so       SharedOptions
		holderNS string
		secretNS string
		released bool
	}{
		"cached":                              {SharedOptions{}, "ns", "other", true},
		"outside the caches":                  {SharedOptions{Namespaces: []string{"ns"}}, "ns", "other", false},
		"outside the caches, the holder too":  {SharedOptions{Namespaces: []string{"in"}}, "ns", "ns", true},
		"cached, with the holder outside it":  {SharedOptions{Namespaces: []string{"other"}}, "ns", "other", true},
		"outside the caches, in a third one":  {SharedOptions{Namespaces: []string{"in"}}, "ns", "other", false},
		"the holder's namespace, in no cache": {SharedOptions{Namespaces: []string{"other"}}, "ns", "ns", true},
	} {
		t.Run(name, func(t *testing.T) {
			r := newFakeRigWith(t, tc.so, namespaceObject("in"), namespaceObject("ns"), namespaceObject("other"),
				heldObject(secretGVK, tc.secretNS, "s", "u-s", widgetGK, tc.holderNS), unparseable(holderObject(tc.holderNS, "h")))
			r.track(t, widgetGK)
			r.run(t)
			stop := r.runWrites(t)
			r.settled()
			held := func() bool {
				return hasFinalizer(r.secretMeta(t, tc.secretNS, "s"), mustFinalizer(widgetGK, tc.holderNS))
			}
			if !held() {
				t.Fatal("the unparseable holder keeps nothing")
			}
			r.deleteHolder(t, tc.holderNS, "h")
			if tc.released {
				eventually(t, "the hold to be released", func() bool { return !held() })
				return
			}
			r.settled()
			if !held() {
				t.Fatal("released: the limit that §10.2 documents is gone")
			}
			if err := stop(); err != nil {
				t.Fatal(err)
			}
			r.runWrites(t)
			eventually(t, "the next start's recheck to release it", func() bool { return !held() })
		})
	}
}

// What such an event costs, however big the record and however often it
// changes: one batch of the namespace at a time, and no call but what that
// batch's own objects need.
func TestWhatAnUnparseableRecordsEventCosts(t *testing.T) {
	const others = 200
	r := newFakeRig(t, namespaceObject("ns"), holderObject("ns", "user", secretID("ns", "used")))
	tr := r.track(t, widgetGK)
	r.run(t)
	store := r.protectedOf(secretGroupKind).inf.GetStore()
	add := func(ns, name string) {
		t.Helper()
		if err := store.Add(heldObject(secretGVK, ns, name, types.UID("u-"+ns+name), widgetGK, ns)); err != nil {
			t.Fatal(err)
		}
	}
	add("ns", "used")
	for i := range others {
		add("elsewhere", "s"+strconv.Itoa(i))
	}
	r.drain()
	h := holderHandler{tr}
	bad := unparseable(holderObject("ns", "h"))
	good := holderObject("ns", "h")
	for range 100 {
		h.OnUpdate(bad, good)
		h.OnUpdate(good, bad)
	}
	k := batchKey{widgetGK, "ns"}
	if n, got := r.s.queue.Len(), r.queued(widgetGK, "ns"); n != 1 || !slices.Equal(got, []ID{fillAll}) {
		t.Fatalf("100 changes queue %d batches, covering %q; want one, of the namespace", n, got)
	}
	_, ids, _ := r.s.queue.Get(context.Background())
	r.md.ClearActions()
	r.s.releaseBatch(context.Background(), k, ids)
	r.s.queue.Done(k)
	for _, a := range r.md.Actions() {
		if a.GetResource().Resource == "secrets" || a.GetVerb() == "list" {
			t.Errorf("the batch of a namespace whose objects have users calls %s %s in %q", a.GetVerb(), a.GetResource().Resource, a.GetNamespace())
		}
	}
	if n := r.s.queue.Len(); n != 0 {
		t.Errorf("%d batches queued by the batch", n)
	}
	// What reading the caches takes: for the record, not a bound.
	for i := range 10000 {
		add("many", "s"+strconv.Itoa(i))
	}
	start := time.Now()
	got, err := r.s.heldIn(context.Background(), tr, "ns")
	t.Logf("heldIn over %d cached objects: %v", len(store.List()), time.Since(start).Round(100*time.Microsecond))
	if err != nil || !slices.Equal(got, []ID{secretID("ns", "used")}) {
		t.Errorf("heldIn = %v, %v", got, err)
	}
}
