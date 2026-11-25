package credentialuse

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

// A ghost: a holder that is gone, and stays in the holder cache, since its
// kind's list is a 404 for as long as the CRD is gone (§6.2 R0, §10.2).

// ghostViews are caches of p, and of a ghost that names it.
type ghostViews struct {
	noViews
	p, ghost *metav1.PartialObjectMetadata
}

func (v ghostViews) Protected(ID) (*metav1.PartialObjectMetadata, bool) { return v.p, true }
func (v ghostViews) HoldersNaming(ID, string) []*metav1.PartialObjectMetadata {
	return []*metav1.PartialObjectMetadata{v.ghost}
}

// R0 and R1's grace shortcut take nothing from the holder cache while the
// kind's CRD is absent: the batch goes on to R1 and R2, which are live.
func TestR0WithoutTheHolderKind(t *testing.T) {
	ctx := context.Background()
	id := SecretRef("ns", "s").ID()
	there, absent := func() bool { return false }, func() bool { return true }
	for name, c := range map[string]struct {
		absent        func() bool
		deleting      bool // P is, inside its grace
		ghostDeleting bool
		released      bool
	}{
		"the CRD is there":                     {absent: there},
		"nothing says":                         {},
		"it is absent":                         {absent: absent, released: true},
		"there, P in its grace":                {absent: there, deleting: true},
		"absent, P in its grace":               {absent: absent, deleting: true, released: true},
		"there, P and the ghost deleting":      {absent: there, deleting: true, ghostDeleting: true},
		"nothing says, P and it deleting":      {deleting: true, ghostDeleting: true},
		"absent, P and the ghost deleting":     {absent: absent, deleting: true, ghostDeleting: true, released: true},
		"absent, the ghost deleting, P is not": {absent: absent, ghostDeleting: true, released: true},
	} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			heldSecret(r)
			if c.deleting {
				must(r.m.Delete(secretKey("ns", "s")))
			}
			ghost := holderObject("ns", "ghost", id)
			if c.ghostDeleting {
				deleting(ghost, r.now)
			}
			r.views = ghostViews{p: r.get(t, secretKey("ns", "s")), ghost: ghost}
			r.p.kindAbsent = c.absent
			r.p.noHolders = func(context.Context, error) error { return nil }
			r.srv.before = func(op, kind, _ string) error {
				if op == "list" && kind == widgetGK.Kind {
					return errTest404
				}
				return nil
			}
			res, err := r.p.release(ctx, "ns", []ID{id})
			if err != nil {
				t.Fatal(err)
			}
			if got := len(res.Released) == 1; got != c.released {
				t.Errorf("released %v, want %v; calls %q", res.Released, c.released, r.srv.calls)
			}
			if c.released && len(res.Kept) != 0 {
				t.Errorf("kept %+v for a ghost", res.Kept)
			}
		})
	}
}

// ghostRig is a rig with one Secret held for Widgets in namespace ns, and its
// holder, in the holder cache. vanish makes the holder a ghost: it is gone,
// and its kind's lists and watches fail, with the CRD there.
func ghostRig(t *testing.T, o SharedOptions, ns string, deletingP bool) (r *fakeRig, tr *Tracker, out *outage, vanish func()) {
	t.Helper()
	s := heldObject(secretGVK, ns, "s", "u1", widgetGK, ns)
	if deletingP {
		deleting(s, time.Now())
	}
	r = newFakeRigWith(t, o, namespaceObject("ns"), namespaceObject("out"), s, holderObject(ns, "ghost", secretID(ns, "s")))
	out = r.breakable("widgets")
	tr = r.trackLogged(t, &sinkLines{})
	r.run(t)
	eventually(t, "the holder's watch", func() bool { return out.watching(1) })
	return r, tr, out, func() {
		t.Helper()
		out.fail(errTest404)
		if err := r.md.Tracker().Delete(widgetsGVR, ns, "ghost"); err != nil {
			t.Fatal(err)
		}
	}
}

// idle waits until no batch of k waits or runs.
func (r *fakeRig) idle(t *testing.T, k batchKey) {
	t.Helper()
	eventually(t, "the batches to run", func() bool { return r.s.queue.Len() == 0 && !r.working(k) })
}

// The CRD's deletion queues the kind's held objects, and their batches
// release them, though the holder cache shows a user for good. Nothing else
// touches the Secret.
func TestTheCRDsDeletionReleasesWhatAGhostHolds(t *testing.T) {
	restricted := SharedOptions{Namespaces: []string{"ns"}}
	for name, c := range map[string]struct {
		o        SharedOptions
		ns       string
		deleting bool // the Secret is, inside its grace
		standby  bool // the write loops start after the CRD went
	}{
		"while the write loops run":            {ns: "ns"},
		"a Secret being deleted, in its grace": {ns: "ns", deleting: true},
		"on a standby":                         {ns: "ns", standby: true},
		"outside the caches":                   {o: restricted, ns: "out"},
		"outside the caches, on a standby":     {o: restricted, ns: "out", standby: true},
	} {
		t.Run(name, func(t *testing.T) {
			r, tr, _, vanish := ghostRig(t, c.o, c.ns, c.deleting)
			if !c.standby {
				r.runWrites(t)
				if len(c.o.Namespaces) > 0 {
					eventually(t, "the start's recheck outside the caches", r.clusterHeldListed)
				}
				r.idle(t, batchKey{widgetGK, c.ns})
			}
			vanish()
			if !r.heldBy(t, widgetGK, c.ns, "s") {
				t.Fatal("released with the CRD there")
			}
			r.removeCRD(t, widgetGK)
			eventually(t, "the CRD informer to see it go", tr.crdAbsent)
			if c.standby {
				r.runWrites(t)
			}
			// The clock stands still: no grace ends, and no retry is due.
			eventually(t, "the hold to be released", func() bool { return !r.heldBy(t, widgetGK, c.ns, "s") })
			if len(tr.holders.GetStore().List()) != 1 {
				t.Error("the ghost left the cache: the test shows nothing")
			}
		})
	}
}

// While the CRD is there, a ghost keeps its hold, as before, by R0, without a
// call: it goes when the holder informer lists again. So it does when the CRD
// comes back before the batch that its deletion queued.
func TestAGhostOfAKindThatIsThereGoesWithTheRelist(t *testing.T) {
	for name, recreated := range map[string]bool{"the CRD stays": false, "it comes back": true} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r, tr, out, vanish := ghostRig(t, SharedOptions{}, "ns", false)
			vanish()
			if recreated {
				r.removeCRD(t, widgetGK)
				slowly(t, "the CRD informer to see it go", tr.crdAbsent)
				r.restoreCRD(t, widgetGK)
				slowly(t, "the CRD informer to see it back", func() bool { return !tr.crdAbsent() })
			}
			// The write loops start only now, so every batch, the one its
			// deletion queued among them, runs with the CRD there.
			r.md.ClearActions()
			r.runWrites(t)
			r.idle(t, batchKey{widgetGK, "ns"})
			for _, a := range r.md.Actions() {
				if a.GetResource().Resource == "secrets" && a.GetVerb() != "list" && a.GetVerb() != "watch" {
					t.Errorf("a %s of the Secret: R0 didn't settle its batch", a.GetVerb())
				}
			}
			if !r.heldBy(t, widgetGK, "ns", "s") {
				t.Fatal("released though its holder is cached, and its kind is there")
			}
			out.heal()
			slowly(t, "the relist to release the hold", func() bool { return !r.heldBy(t, widgetGK, "ns", "s") })
		})
	}
}

// The CRD counts as absent only to an informer that has synced: one that
// hasn't knows nothing.
func TestCRDAbsent(t *testing.T) {
	r := newFakeRig(t)
	tr := r.track(t, widgetGK)
	if tr.crdAbsent() {
		t.Error("absent, before the informer runs")
	}
	if (&Tracker{}).crdAbsent() {
		t.Error("absent, without an informer")
	}
	r.run(t)
	if tr.crdAbsent() {
		t.Error("absent, with the CRD there")
	}
	r.removeCRD(t, widgetGK)
	eventually(t, "the CRD informer to see it go", tr.crdAbsent)
	if g := r.track(t, gadgetGK); g.crdAbsent() {
		t.Error("another kind's CRD is absent too")
	}
	r.restoreCRD(t, widgetGK)
	eventually(t, "the CRD informer to see it back", func() bool { return !tr.crdAbsent() })
}

// The CRD's deletion queues the batch of every (K, N) finalizer its kind has
// in the caches, the per-name ones included, and no other kind's.
func TestTheCRDsDeletionQueuesTheKindsHolds(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"),
		heldObject(secretGVK, "ns", "a", "u-a", widgetGK, "ns", "other"),
		heldObject(secretGVK, "ns", "b", "u-b", gadgetGK, "ns"))
	tr := r.track(t, widgetGK)
	r.track(t, gadgetGK)
	// c is in a per-name cache only.
	a, c := secretID("ns", "a"), secretID("ns", "c")
	p := &protectedInformer{gk: secretGroupKind, ns: "ns", name: "c",
		inf: cache.NewSharedIndexInformer(&cache.ListWatch{}, &metav1.PartialObjectMetadata{}, 0, cache.Indexers{})}
	must(p.inf.GetStore().Add(heldObject(secretGVK, "ns", "c", "u-c", widgetGK, "ns")))
	tr.names.by[c] = &nameWatch{p: p, stop: func() {}}
	r.run(t)
	eventually(t, "the initial list's batches", func() bool {
		return len(r.queued(widgetGK, "other")) == 1 && len(r.queued(gadgetGK, "ns")) == 1
	})
	r.drain()
	crdHandler{tr}.OnDelete(cache.DeletedFinalStateUnknown{Obj: crdMeta(widgetGK)})
	if got := r.queued(widgetGK, "ns"); len(got) != 2 || got[0] != a || got[1] != c {
		t.Errorf("queued for Widgets in ns: %v, want a and c", got)
	}
	if got := r.queued(widgetGK, "other"); len(got) != 1 || got[0] != a {
		t.Errorf("queued for Widgets in other: %v, want a", got)
	}
	if got := r.queued(gadgetGK, "ns"); got != nil {
		t.Errorf("queued for Gadgets, whose CRD is there: %v", got)
	}
}

// A ghost's record keeps no line in the index while the kind is gone
// (§10.6.2): the batch that released its object prunes it.
func TestAGhostsRecordKeepsNoLine(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"), holderObject("ns", "ghost", secretID("ns", "s")))
	tr := r.track(t, widgetGK)
	r.run(t)
	if !tr.recorded(secretID("ns", "s")) {
		t.Fatal("the holder's record isn't seen")
	}
	r.removeCRD(t, widgetGK)
	eventually(t, "the CRD informer to see it go", tr.crdAbsent)
	if tr.recorded(secretID("ns", "s")) {
		t.Error("a ghost's record keeps its line")
	}
}

// perNameFakeRig is a rig of Widgets in PerName mode, whose index names ids,
// and whose Secrets' watches the API server forbids: its per-name caches are
// empty (§13 shape 1). before, if set, runs before its Track.
func perNameFakeRig(t *testing.T, objs []runtime.Object, before func(r *fakeRig), ids ...ID) (*fakeRig, *sinkLines, *outage) {
	t.Helper()
	ctx := context.Background()
	r := newFakeRigWith(t, SharedOptions{IndexNamespace: "idx"}, append([]runtime.Object{namespaceObject("ns"), namespaceObject("idx")}, objs...)...)
	cm := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap",
		"data": map[string]any{indexDataKey: encodeIndex(linesOf(ids))}}}
	cm.SetNamespace("idx")
	cm.SetName(IndexName(widgetGK))
	cm.SetAnnotations(map[string]string{AnnotationRebuilt: "true"})
	if _, err := r.dyn.Resource(configMapGVR).Namespace("idx").Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	r.breakable("secrets").fail(errTest403)
	out, sink := r.breakable("widgets"), &sinkLines{}
	if before != nil {
		before(r)
	}
	r.trackLogged(t, sink, ProtectedKind{GVK: secretGVK, Watch: PerName})
	r.run(t)
	return r, sink, out
}

// indexedIDs are the objects the fake index names.
func (r *fakeRig) indexedIDs(t *testing.T) []ID {
	t.Helper()
	cm, err := r.dyn.Resource(configMapGVR).Namespace("idx").Get(context.Background(), IndexName(widgetGK), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var out []ID
	for _, l := range versionOf(cm).lines {
		if id, ok := parseIndexLine(l); ok {
			out = append(out, id)
		}
	}
	return out
}

// In PerName mode a hold whose per-name watch is refused is in no cache, so
// the CRD's deletion finds it through the index, as the start does, and a
// ghost's lines go, the one of a ref that is Missing too (§10.6.2).
func TestTheCRDsDeletionRechecksTheIndex(t *testing.T) {
	s, missing := secretID("ns", "s"), secretID("ns", "missing")
	r, _, out := perNameFakeRig(t, []runtime.Object{
		heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"), holderObject("ns", "ghost", s, missing)}, nil, s, missing)
	r.runWrites(t)
	eventually(t, "the start's recheck to read the Secret, and its batch", func() bool {
		n := 0
		for _, a := range r.md.Actions() {
			if g, ok := a.(clienttesting.GetAction); ok && a.GetResource() == secretsGVR && g.GetName() == "s" {
				n++
			}
		}
		return n >= 2 && r.s.queue.Len() == 0 && !r.working(batchKey{widgetGK, "ns"})
	})
	if got := r.indexedIDs(t); len(got) != 2 || !r.heldBy(t, widgetGK, "ns", "s") {
		t.Fatalf("before: the index names %v, held %v", got, r.heldBy(t, widgetGK, "ns", "s"))
	}
	eventually(t, "the holder's watch", func() bool { return out.watching(1) })
	out.fail(errTest404)
	must(r.md.Tracker().Delete(widgetsGVR, "ns", "ghost"))
	r.removeCRD(t, widgetGK)
	// The clock stands still: only the CRD's deletion can have queued it.
	eventually(t, "the hold to be released", func() bool { return !r.heldBy(t, widgetGK, "ns", "s") })
	eventually(t, "the ghost's lines to go", func() bool { return len(r.indexedIDs(t)) == 0 })
}

// heldLists are the cluster-wide lists of held Secrets made.
func (r *fakeRig) heldLists() int {
	n := 0
	for _, a := range r.md.Actions() {
		l, ok := a.(clienttesting.ListAction)
		if ok && a.GetResource().Resource == "secrets" && a.GetNamespace() == "" && l.GetListRestrictions().Labels.String() == heldSelector() {
			n++
		}
	}
	return n
}

// The write loops' end waits for a recheck; none starts once they are ending.
func TestTheWriteLoopsWaitForTheRecheck(t *testing.T) {
	r, tr, _, vanish := ghostRig(t, SharedOptions{Namespaces: []string{"ns"}}, "out", false)
	vanish()
	stop := r.runWrites(t)
	eventually(t, "the start's recheck", r.clusterHeldListed)
	r.idle(t, batchKey{widgetGK, "out"})
	entered, release := r.gateList("secrets")
	crdHandler{tr}.OnDelete(crdMeta(widgetGK))
	within(t, "the recheck's list", entered)
	crdHandler{tr}.OnDelete(crdMeta(widgetGK)) // and one asked for meanwhile
	stopped := make(chan error, 1)
	go func() { stopped <- stop() }()
	// Every wait below is bounded: a write loops' end that doesn't wait fails
	// here, within a second, and not at the binary's timeout.
	select {
	case err := <-stopped:
		close(release)
		t.Fatalf("RunWriteLoops ended (%v) with a recheck running", err)
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-stopped:
		if err != nil {
			t.Errorf("RunWriteLoops = %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("RunWriteLoops didn't end once the recheck did")
	}
	// As between the write loops' cancel and their end.
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	r.s.mu.Lock()
	r.s.writeCtx = ended
	r.s.mu.Unlock()
	r.md.ClearActions()
	crdHandler{tr}.OnDelete(crdMeta(widgetGK))
	time.Sleep(100 * time.Millisecond)
	// Any list: a recheck begun now makes its first part's call, whose ended
	// context the fake doesn't honour, and stops before the held objects'.
	lists := 0
	for _, a := range r.md.Actions() {
		if a.GetVerb() == "list" && a.GetResource().Resource == "secrets" && a.GetNamespace() == "" {
			lists++
		}
	}
	r.s.mu.Lock()
	rechecking := tr.rechecking
	r.s.mu.Unlock()
	if lists != 0 || rechecking {
		t.Errorf("a recheck began while the write loops were ending: %d lists, rechecking %v", lists, rechecking)
	}
	r.s.mu.Lock()
	r.s.writeCtx = nil
	r.s.mu.Unlock()
}

// The recheck outside the caches that a CRD's deletion starts ends with the
// write loops, which wait for it; a standby makes none, nor does a process
// whose write loops have ended.
func TestTheRecheckAfterACRDsDeletionEndsWithTheWriteLoops(t *testing.T) {
	r, tr, _, vanish := ghostRig(t, SharedOptions{Namespaces: []string{"ns"}}, "out", false)
	vanish()
	r.md.ClearActions()
	crdHandler{tr}.OnDelete(crdMeta(widgetGK))
	if r.clusterHeldListed() {
		t.Error("a standby listed")
	}
	stop := r.runWrites(t)
	eventually(t, "the start's recheck outside the caches", r.clusterHeldListed)
	r.forbidList.Store("secrets", true) // the next recheck retries for as long as it lasts
	r.md.ClearActions()
	crdHandler{tr}.OnDelete(crdMeta(widgetGK))
	eventually(t, "the CRD's recheck outside the caches", r.clusterHeldListed)
	if err := stop(); err != nil {
		t.Errorf("RunWriteLoops = %v", err)
	}
	r.md.ClearActions()
	crdHandler{tr}.OnDelete(crdMeta(widgetGK))
	if r.clusterHeldListed() {
		t.Error("a recheck began after the write loops ended")
	}
}
