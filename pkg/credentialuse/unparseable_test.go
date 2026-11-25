package credentialuse

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clienttesting "k8s.io/client-go/testing"
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
		"a parseable one, gone":              {holderObject("ns", "h", named), nil, []ID{named}},
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
		"replaced in one event, unparseable":                      {bad(), recreated(bad()), []ID{fillAll}},
		"replaced in one event, both parseable":                   {holderObject("ns", "h", named), recreated(holderObject("ns", "h")), []ID{fillAll, named}},
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
				h.OnDelete(tc.old)
			default:
				h.OnUpdate(tc.old, tc.cur)
			}
			if got := r.queued(widgetGK, "ns"); !slices.Equal(got, SortedIDs(tc.want)) {
				t.Errorf("queued %q, want %q", got, tc.want)
			}
		})
	}
	t.Run("found gone by a relist, whatever its last version", func(t *testing.T) {
		for _, old := range []*metav1.PartialObjectMetadata{holderObject("ns", "h", named), holderObject("ns", "h"), bad(), noOwner(deleting(bad(), gone))} {
			r := newFakeRig(t)
			holderHandler{r.track(t, widgetGK)}.OnDelete(cache.DeletedFinalStateUnknown{Key: "ns/h", Obj: old})
			if got := r.queued(widgetGK, "ns"); !slices.Contains(got, fillAll) {
				t.Errorf("queued %q, want the whole namespace", got)
			}
		}
	})
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

// The batch of the namespace releases by §5 and nothing else: what has a user
// that counts stays.
func TestWhatTheBatchOfTheNamespaceKeeps(t *testing.T) {
	id := secretID("ns", "s")
	held := func(r *fakeRig) bool { return hasFinalizer(r.secretMetaOf("ns", "s"), mustFinalizer(widgetGK, "ns")) }
	t.Run("a deleting object with a live user, in a live namespace, for its grace", func(t *testing.T) {
		s := deleting(heldObject(secretGVK, "ns", "s", "u-s", widgetGK, "ns"), time.Now())
		r := newFakeRig(t, namespaceObject("ns"), s, unparseable(holderObject("ns", "h")), holderObject("ns", "h2", id))
		r.track(t, widgetGK)
		r.run(t)
		r.runWrites(t)
		time.Sleep(100 * time.Millisecond)
		r.deleteHolder(t, "ns", "h")
		for range 5 {
			time.Sleep(50 * time.Millisecond)
			r.clk.Step(5 * time.Second)
		}
		if !held(r) {
			t.Fatal("released inside its grace, with a live user")
		}
		r.clk.Step(10 * time.Minute)
		eventually(t, "§5's last row to release it", func() bool { return !held(r) })
	})
	t.Run("a pass's own repair", func(t *testing.T) {
		r := newFakeRig(t, namespaceObject("ns"), heldObject(secretGVK, "ns", "s", "u-s", widgetGK, "ns"), unparseable(holderObject("ns", "h")))
		r.setRefs("h", SecretRef("ns", "s"))
		tr := r.track(t, widgetGK)
		r.run(t)
		r.runWrites(t)
		r.settled()
		obj, err := r.md.Tracker().Get(widgetsGVR, "ns", "h")
		if err != nil {
			t.Fatal(err)
		}
		if a, err := tr.Acquire(context.Background(), obj.(*metav1.PartialObjectMetadata).DeepCopy()); err != nil || len(a.Held) != 1 {
			t.Fatalf("Acquire = %+v, %v", a, err)
		}
		r.settled()
		if !held(r) {
			t.Error("the pass that repaired the record lost its own ref")
		}
	})
	t.Run("a switch for one kind leaves the record to its holder's pass", func(t *testing.T) {
		// Were it stripped, the batch would release the other kinds' objects.
		u := unparseable(holderObject("ns", "h"))
		if data, ok := disownHolderPatch(u, parseDisown("ConfigMap")); ok {
			t.Errorf("patch %s, want none", data)
		}
	})
	t.Run("toggles while its batch works", func(t *testing.T) {
		r := newFakeRig(t, namespaceObject("ns"), heldObject(secretGVK, "ns", "s", "u-s", widgetGK, "ns"), heldObject(secretGVK, "ns", "b", "u-b", widgetGK, "ns"),
			unparseable(holderObject("ns", "h")))
		entered, release := make(chan struct{}), make(chan struct{})
		var gets atomic.Int64
		r.md.PrependReactor("get", "secrets", func(a clienttesting.Action) (bool, runtime.Object, error) {
			if a.(clienttesting.GetAction).GetName() == "b" && gets.Add(1) == 1 {
				close(entered)
				<-release
			}
			return false, nil, nil
		})
		tr := r.track(t, widgetGK)
		r.run(t)
		stop := r.runWrites(t)
		r.settled() // kept by the unparseable holder, from the cache
		h, bad, good := holderHandler{tr}, unparseable(holderObject("ns", "h")), holderObject("ns", "h")
		r.updateHolder(t, good)
		<-entered
		k := batchKey{widgetGK, "ns"}
		for range 100 {
			h.OnUpdate(bad, good)
			h.OnUpdate(good, bad)
		}
		if n := r.s.queue.Len(); n != 0 || !r.working(k) {
			t.Errorf("%d batches ready while the namespace's works (%v)", n, r.working(k))
		}
		close(release)
		eventually(t, "the holds to be released", func() bool { return !held(r) })
		eventually(t, "the queue to be empty", func() bool { return r.s.queue.Len() == 0 && !r.working(k) })
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		// One more batch for all the toggles: b is read once more at most, and
		// again after a release that found it changed.
		if n := gets.Load(); n > 3 {
			t.Errorf("b was read %d times for 200 changes", n)
		}
	})
	t.Run("a PerName name whose watch has nothing: a limit", func(t *testing.T) {
		r := newFakeRig(t, namespaceObject("ns"))
		tr := r.track(t, widgetGK)
		tr.idx, tr.names = testIndex(&memStore{}, nil), newPerNames(tr)
		p := &protectedInformer{gk: secretGroupKind, ns: "ns", name: "s", inf: syncedInformer(t, make(chan struct{}))}
		tr.names.by[id] = &nameWatch{p: p, stop: func() {}}
		if got, err := r.s.heldIn(context.Background(), tr, "ns"); err != nil || len(got) != 0 {
			t.Errorf("heldIn = %v, %v: the limit that §10.2 documents is gone", got, err)
		}
		if err := p.inf.GetStore().Add(heldObject(secretGVK, "ns", "s", "u-s", widgetGK, "ns")); err != nil {
			t.Fatal(err)
		}
		if got, err := r.s.heldIn(context.Background(), tr, "ns"); err != nil || !slices.Equal(got, []ID{id}) {
			t.Errorf("heldIn = %v, %v once its watch delivered it", got, err)
		}
	})
}

// A pass repairs the record with all the refs it names, in one write, where
// they are all direct, so the batch that its event queues finds them recorded.
// A chain's pass writes none, as this release refuses it (§9); where the tests
// follow one, its first write has the chain so far.
func TestAPassThatRepairsAnUnparseableRecord(t *testing.T) {
	ctx := context.Background()
	holder := func(r *rig) *metav1.PartialObjectMetadata {
		must(r.m.Create(widgetKey("ns", "h"), metav1.ObjectMeta{Finalizers: ownerFinalizers, Annotations: map[string]string{AnnotationRefs: "oops"}}))
		return r.get(t, widgetKey("ns", "h"))
	}
	record := func(r *rig) Record { return ReadRecord(r.get(t, widgetKey("ns", "h"))) }
	t.Run("direct refs", func(t *testing.T) {
		r := newRig(t)
		must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
		must(r.m.Create(secretKey("ns", "t"), metav1.ObjectMeta{}))
		h := holder(r)
		r.srv.before = func(op, kind, name string) error {
			if op == "patch" && kind == "Secret" {
				if rec := record(r); rec.Unparseable || len(rec.IDs) != 2 {
					t.Errorf("the A2 of %s: record %+v, want both refs", name, rec)
				}
			}
			return nil
		}
		if a, err := r.p.acquire(ctx, h, []Ref{SecretRef("ns", "s"), SecretRef("ns", "t")}); err != nil || len(a.Held) != 2 {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
		if n := countCalls(r.srv.calls, "patch Widget/h"); n != 1 {
			t.Errorf("%d record writes, want one", n)
		}
	})
	t.Run("a refused chain", func(t *testing.T) {
		r := newRig(t)
		must(r.m.Create(targetKey("kt"), metav1.ObjectMeta{}))
		heldSecret(r)
		h := holder(r)
		a, err := r.p.acquire(ctx, h, []Ref{targetRef("kt"), SecretRef("ns", "s")})
		if err != nil || !a.Degraded || !errors.Is(a.Err, ErrChainsNotSupported) {
			t.Fatalf("acquire = %+v, %v; want it refused", a, err)
		}
		if n := countCalls(r.srv.calls, "patch Widget/h"); n != 0 || !record(r).Unparseable {
			t.Errorf("%d record writes, record %+v; want it left as it is", n, record(r))
		}
	})
	t.Run("limit: a chain that is followed", func(t *testing.T) {
		r, fc := chainRig(t)
		r.target(fc, "kt", "Secret", "s")
		heldSecret(r)
		h, s := holder(r), SecretRef("ns", "s").ID()
		var released []ID
		r.srv.before = func(op, kind, _ string) error {
			if op == "patch" && kind == "Target" { // after the first level's A1
				r.srv.before = nil
				res, err := r.p.release(ctx, "ns", []ID{s})
				if err != nil {
					t.Error(err)
				}
				released = res.Released
			}
			return nil
		}
		if a, err := r.p.acquire(ctx, h, []Ref{targetRef("kt")}); err != nil || len(a.Held) != 2 {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
		if !slices.Equal(released, []ID{s}) || !hasFinalizer(r.get(t, secretKey("ns", "s")), fNS) {
			t.Errorf("released %v between the levels; the limit that §10.2 documents has changed", released)
		}
	})
}

// toggles is a holder whose record is spoiled and repaired, over and over, by
// its events; what the API server and the caches have of it is set.
type toggles struct {
	r         *fakeRig
	tr        *Tracker
	bad, good *metav1.PartialObjectMetadata
}

func newToggles(r *fakeRig, tr *Tracker) toggles {
	return toggles{r, tr, unparseable(holderObject("ns", "h")), holderObject("ns", "h")}
}

// burst is 100 toggles, the last of which leaves it unparseable. They queue one
// batch, of the whole namespace.
func (g toggles) burst(t *testing.T) {
	t.Helper()
	for range 100 {
		holderHandler{g.tr}.OnUpdate(g.bad, g.good)
		holderHandler{g.tr}.OnUpdate(g.good, g.bad)
	}
	if got := g.r.queued(widgetGK, "ns"); !slices.Contains(got, fillAll) || g.r.s.queue.Len() != 1 {
		t.Fatalf("%d batches, of %q; want one, of the whole namespace", g.r.s.queue.Len(), got)
	}
}

// set makes it m, and waits for what its event queues when that is its repair.
func (g toggles) set(t *testing.T, m *metav1.PartialObjectMetadata) {
	t.Helper()
	g.r.updateHolder(t, m)
	eventually(t, "the holder's cache", func() bool {
		o, ok, err := g.tr.holders.GetIndexer().GetByKey("ns/h")
		return err == nil && ok && o.(*metav1.PartialObjectMetadata).Annotations[AnnotationRefs] == m.Annotations[AnnotationRefs]
	})
	if m == g.good {
		eventually(t, "the batch of its repair", func() bool { return slices.Contains(g.r.queued(widgetGK, "ns"), fillAll) })
	}
}

// work runs the batch that waits, and fails t unless it reads the Secret once,
// and leaves it held or not, and timers timers.
func (g toggles) work(t *testing.T, what string, held bool, timers int) {
	t.Helper()
	if n := g.r.secretGets(t, g.tr, true); n != 1 {
		t.Errorf("%s: the batch read the Secret %d times, want once", what, n)
	}
	if got := hasFinalizer(g.r.secretMetaOf("ns", "s"), mustFinalizer(widgetGK, "ns")); got != held {
		t.Fatalf("%s: held %v, want %v", what, got, held)
	}
	if n := g.r.queueTimers(); n != timers {
		t.Errorf("%s: %d timers, want %d", what, n, timers)
	}
}

// Toggles while a deleting object waits for its grace, or for its namespace's
// teardown: no batch releases it while the record is unparseable, but §5's last
// row, whoever uses it; each reads it once, and sets no timer that there isn't;
// and the repair releases it.
func TestTogglesDuringAGrace(t *testing.T) {
	rig := func(t *testing.T, ns *metav1.PartialObjectMetadata) toggles {
		t.Helper()
		r := newFakeRig(t, ns, deleting(heldObject(secretGVK, "ns", "s", "u-s", widgetGK, "ns"), time.Now()), unparseable(holderObject("ns", "h")))
		tr := r.track(t, widgetGK)
		r.run(t)
		eventually(t, "the Secret's batch, and its grace's timer", func() bool { return r.queued(widgetGK, "ns") != nil && r.queueTimers() == 1 })
		return newToggles(r, tr)
	}
	live, going := namespaceObject("ns"), func() *metav1.PartialObjectMetadata { return deleting(namespaceObject("ns"), time.Now()) }
	// In a live namespace the batch asks for the grace's end too.
	t.Run("in a live namespace, repaired", func(t *testing.T) {
		g := rig(t, live)
		g.work(t, "before any", true, 2)
		for range 3 {
			g.burst(t)
			g.work(t, "toggled", true, 2)
		}
		g.set(t, g.good)
		g.work(t, "repaired", false, 2)
	})
	t.Run("in a live namespace, to the grace's end", func(t *testing.T) {
		g := rig(t, live)
		g.burst(t)
		g.work(t, "toggled", true, 2)
		g.r.clk.Step(DefaultDeletionGrace - time.Second)
		g.burst(t)
		g.work(t, "toggled a second before the end", true, 2)
		g.r.clk.Step(2 * time.Second)
		g.work(t, "§5's last row", false, 0)
	})
	t.Run("in a namespace that is being deleted", func(t *testing.T) {
		g := rig(t, going())
		g.work(t, "before any", true, 1)
		g.burst(t)
		g.work(t, "toggled", true, 1)
		g.r.clk.Step(2 * DefaultDeletionGrace)
		g.burst(t)
		g.work(t, "toggled after the grace", true, 0)
		g.set(t, g.good)
		g.work(t, "repaired", false, 0)
	})
}

// Toggles while the batch fails, at the object or at the namespace's list: they
// put off no retry and set no other, none of them releases it while the record
// is unparseable, and a repair that a batch failed at is made good by the retry,
// without another event.
func TestTogglesWhileABatchFails(t *testing.T) {
	t.Run("at the object", func(t *testing.T) {
		r := newFakeRig(t, deleting(namespaceObject("ns"), time.Now()),
			deleting(heldObject(secretGVK, "ns", "s", "u-s", widgetGK, "ns"), time.Now()), unparseable(holderObject("ns", "h")))
		var fails atomic.Bool
		r.md.PrependReactor("get", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) {
			if fails.Load() {
				return true, nil, apierrors.NewServiceUnavailable("overloaded")
			}
			return false, nil, nil
		})
		tr := r.track(t, widgetGK)
		r.run(t)
		eventually(t, "the Secret's batch, and its grace's timer", func() bool { return r.queued(widgetGK, "ns") != nil && r.queueTimers() == 1 })
		g := newToggles(r, tr)
		fails.Store(true)
		g.work(t, "failing", true, 2) // the grace's, and the retry
		for range 3 {
			g.burst(t)
			g.work(t, "toggled, failing", true, 2)
		}
		r.clk.Step(releaseBackoffMax) // both fire, in one batch
		g.work(t, "its retry, failing", true, 1)
		// Answered, and unparseable.
		fails.Store(false)
		r.clk.Step(releaseBackoffMax)
		g.work(t, "its retry, answered", true, 0)
		// Repaired in a batch that fails.
		fails.Store(true)
		g.set(t, g.good)
		g.work(t, "repaired, failing", true, 1)
		// Spoiled again before the retry: it keeps.
		g.set(t, g.bad)
		fails.Store(false)
		r.clk.Step(releaseBackoffMax)
		g.work(t, "its retry, spoiled again", true, 0)
		// And repaired for good, in a batch that fails.
		fails.Store(true)
		g.set(t, g.good)
		g.work(t, "repaired again, failing", true, 1)
		fails.Store(false)
		r.clk.Step(releaseBackoffMax)
		g.work(t, "its retry, repaired", false, 0)
	})
	t.Run("at the namespace's list, outside the caches", func(t *testing.T) {
		r := newFakeRigWith(t, SharedOptions{Namespaces: []string{"in"}}, namespaceObject("in"), namespaceObject("ns"),
			heldObject(secretGVK, "ns", "s", "u-s", widgetGK, "ns"), unparseable(holderObject("ns", "h")))
		tr := r.track(t, widgetGK)
		r.run(t)
		r.drain()
		g := newToggles(r, tr)
		// Without the list there is nothing to read.
		failing := func(what string) {
			t.Helper()
			if n := r.secretGets(t, tr, true); n != 0 || !hasFinalizer(r.secretMetaOf("ns", "s"), mustFinalizer(widgetGK, "ns")) || r.queueTimers() != 1 {
				t.Fatalf("%s: %d reads, %d timers; want none, the list's retry, and it held", what, n, r.queueTimers())
			}
		}
		r.forbidList.Store("secrets", true)
		for range 3 {
			g.burst(t)
			failing("toggled, failing")
		}
		r.clk.Step(releaseBackoffMax)
		failing("its retry, failing")
		r.forbidList.Delete("secrets")
		r.clk.Step(releaseBackoffMax)
		g.work(t, "its retry, answered", true, 0)
		r.forbidList.Store("secrets", true)
		g.set(t, g.good)
		failing("repaired, failing")
		g.set(t, g.bad)
		r.forbidList.Delete("secrets")
		r.clk.Step(releaseBackoffMax)
		g.work(t, "its retry, spoiled again", true, 0)
		r.forbidList.Store("secrets", true)
		g.set(t, g.good)
		failing("repaired again, failing")
		r.forbidList.Delete("secrets")
		r.clk.Step(releaseBackoffMax)
		g.work(t, "its retry, repaired", false, 0)
	})
}

// A holder that another replaces in one event leaves nothing behind: its UID
// never comes back.
func TestAHolderReplacedInOneEventLeavesNothing(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"))
	tr := r.track(t, widgetGK)
	old, again := holderObject("ns", "h"), holderObject("ns", "h")
	again.UID = "w-again"
	key := consentKey{old.UID, secretID("other", "s")}
	tr.proto.written.set(old.UID, map[string]string{AnnotationRefs: "[]"})
	tr.proto.backoff.fail(key)
	for _, s := range []*reportedSet{tr.proto.reported, tr.unrecorded, tr.degradedLogged, tr.refused, tr.misplaced, tr.unparseable} {
		s.first(key)
		s.first(consentKey{again.UID, key.ref})
	}
	tr.proto.backoff.fail(consentKey{again.UID, key.ref})
	for _, uid := range []types.UID{old.UID, again.UID} {
		tr.proto.gateTurns.next(uid, []ID{key.ref}, 1)
		tr.proto.conflicts.tell(uid, key.ref, asks{hold: true})
	}
	holderHandler{tr}.OnUpdate(old, again)
	if _, ok := tr.proto.gateTurns.at[again.UID]; !ok || len(tr.proto.gateTurns.at) != 1 {
		t.Errorf("turns kept: %v, want the new one's", tr.proto.gateTurns.at)
	}
	if tr.proto.conflicts.told(again.UID, key.ref) != (asks{hold: true}) || len(tr.proto.conflicts.m) != 1 {
		t.Errorf("conflicts kept: %v, want the new one's", tr.proto.conflicts.m)
	}
	if n := len(tr.proto.backoff.at); n != 1 {
		t.Errorf("%d backoffs kept, want the new one's", n)
	}
	for i, s := range []*reportedSet{tr.proto.reported, tr.unrecorded, tr.degradedLogged, tr.refused, tr.misplaced, tr.unparseable} {
		if s.m[key] || len(s.m) != 1 {
			t.Errorf("set %d keeps %v, want the new one's", i, s.m)
		}
	}
	if _, ok := tr.proto.written.get(old.UID); ok {
		t.Error("its last record write is kept")
	}
}

// secretMetaOf reads Secret ns/name from the fake API server, or nothing.
func (r *fakeRig) secretMetaOf(ns, name string) *metav1.PartialObjectMetadata {
	obj, err := r.md.Tracker().Get(secretsGVR, ns, name)
	if err != nil {
		return &metav1.PartialObjectMetadata{}
	}
	return obj.(*metav1.PartialObjectMetadata)
}

// take returns the warnings since the last take.
func (w *warnings) take() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	got := w.got
	w.got = nil
	return got
}

// An unparseable record is logged once per holder, however often it changes,
// with the holder's namespace, name and kind, and never what it says; and
// nothing is kept of a holder that is forgotten, or gone (§4).
func TestAnUnparseableRecordIsLoggedOnce(t *testing.T) {
	const spoiled = "spoiled-by-someone"
	record := func(v string) *metav1.PartialObjectMetadata {
		h := holderObject("ns", "h")
		h.Annotations = map[string]string{AnnotationRefs: v}
		return h
	}
	world := func(t *testing.T) (*fakeRig, *Tracker, *warnings) {
		t.Helper()
		r := newFakeRig(t, namespaceObject("ns"))
		log := &warnings{}
		o := r.options(widgetGK)
		o.Logger = log
		tr, err := r.s.Track(context.Background(), o)
		if err != nil {
			t.Fatal(err)
		}
		return r, tr, log
	}
	logged := func(t *testing.T, what string, log *warnings, want int) {
		t.Helper()
		w := log.take()
		if len(w) != want {
			t.Errorf("%s: %d warnings, want %d: %q", what, len(w), want, w)
		}
		for _, s := range w {
			if !strings.Contains(s, "ns/h") || !strings.Contains(s, widgetGK.String()) || strings.Contains(s, spoiled) {
				t.Errorf("%s: warning %q; want the holder and its kind, and nothing of the record", what, s)
			}
		}
	}
	t.Run("its events", func(t *testing.T) {
		_, tr, log := world(t)
		h := holderHandler{tr}
		one, two, good := record(spoiled), record(spoiled+" 2"), holderObject("ns", "h", secretID("ns", "s"))
		h.OnAdd(good, true)
		logged(t, "a good record", log, 0)
		h.OnUpdate(good, one)
		logged(t, "spoiled", log, 1)
		relabelled := one.DeepCopy()
		relabelled.ResourceVersion, relabelled.Generation = "8", 8
		h.OnUpdate(one, relabelled)
		h.OnUpdate(relabelled, relabelled)
		h.OnAdd(relabelled, true)
		logged(t, "changed otherwise, resynced, and replayed", log, 0)
		h.OnUpdate(relabelled, good)
		h.OnUpdate(good, one)
		logged(t, "repaired, and spoiled the same way", log, 0)
		h.OnUpdate(one, two)
		logged(t, "spoiled another way", log, 0)
		h.OnDelete(two)
		if len(tr.unparseable.m) != 0 {
			t.Errorf("%d holders kept once it is gone", len(tr.unparseable.m))
		}
		again := record(spoiled)
		again.UID = "w-again"
		h.OnAdd(again, false)
		logged(t, "gone, and recreated", log, 1)
		h.OnUpdate(again, one) // delivered as one event, after a gap
		logged(t, "replaced, in one event", log, 1)
		if len(tr.unparseable.m) != 1 {
			t.Errorf("%d holders kept, want the one there is", len(tr.unparseable.m))
		}
	})
	t.Run("one that isn't a user", func(t *testing.T) {
		// Forget, and then the update that takes the owner finalizer: it is
		// unparseable still, and counts as a user of nothing.
		_, tr, log := world(t)
		h := holderHandler{tr}
		going := deleting(record(spoiled), time.Now())
		h.OnAdd(going, false)
		logged(t, "deleting, with its owner finalizer", log, 1)
		tr.Forget(going.DeepCopy())
		done := going.DeepCopy()
		done.Finalizers = []string{"example.com/else"}
		h.OnUpdate(going, done)
		logged(t, "forgotten, and without it", log, 0)
		h.OnAdd(done, true)
		logged(t, "replayed", log, 0)
		if len(tr.unparseable.m) != 0 {
			t.Errorf("%d holders kept, want none", len(tr.unparseable.m))
		}
	})
	t.Run("through the informer, and what is kept", func(t *testing.T) {
		r, tr, log := world(t)
		if err := r.md.Tracker().Add(record(spoiled)); err != nil {
			t.Fatal(err)
		}
		r.run(t)
		kept := func() int {
			tr.unparseable.mu.Lock()
			defer tr.unparseable.mu.Unlock()
			return len(tr.unparseable.m)
		}
		eventually(t, "the record to be logged", func() bool { return kept() == 1 })
		logged(t, "its first list", log, 1)
		for i := range 5 {
			r.updateHolder(t, record(spoiled+strconv.Itoa(i)))
		}
		eventually(t, "the last of 5 other records", func() bool {
			o, ok, err := tr.holders.GetIndexer().GetByKey("ns/h")
			return err == nil && ok && o.(*metav1.PartialObjectMetadata).Annotations[AnnotationRefs] == spoiled+"4"
		})
		time.Sleep(50 * time.Millisecond)
		logged(t, "5 other records", log, 0)
		tr.Forget(record(spoiled))
		if n := kept(); n != 0 {
			t.Errorf("%d holders kept after Forget", n)
		}
		r.updateHolder(t, record(spoiled))
		eventually(t, "the record to be logged again", func() bool { return kept() == 1 })
		r.deleteHolder(t, "ns", "h")
		eventually(t, "nothing to be kept of a holder that is gone", func() bool { return kept() == 0 })
	})
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
