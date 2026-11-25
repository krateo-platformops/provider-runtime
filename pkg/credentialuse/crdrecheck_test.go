package credentialuse

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
)

// The recheck that a CRD's deletion asks for while one runs (recheckSoon): it
// cuts the run's wait short. The clock stands still in each test, so that no
// retry's backoff ends: what the tests see, only an ask did.

// within waits for ch to close, for at most 20 seconds.
func within(t *testing.T, what string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(20 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// A recheck whose part fails for good, as a protected kind's list that is
// forbidden, waits to retry it. The CRD's deletion while it waits starts every
// part again at once: the hold outside the caches, which its first try found
// while the CRD was there, is found again, and released.
func TestTheCRDsDeletionCutsARecheckWaitShort(t *testing.T) {
	r := newFakeRigWith(t, SharedOptions{Namespaces: []string{"ns"}}, namespaceObject("ns"), namespaceObject("out"),
		heldObject(secretGVK, "out", "s", "u1", widgetGK, "out"), holderObject("out", "ghost", secretID("out", "s")))
	out, sink := r.breakable("widgets"), &sinkLines{}
	tr := r.trackLogged(t, sink, ProtectedKind{GVK: secretGVK}, ProtectedKind{GVK: configMapGVK})
	r.run(t)
	eventually(t, "the holder's watch", func() bool { return out.watching(1) })
	r.runWrites(t)
	eventually(t, "the start's recheck", r.clusterHeldListed)
	r.idle(t, batchKey{widgetGK, "out"})

	r.forbidList.Store("configmaps", true) // its parts fail for as long as the test runs
	out.fail(errTest404)
	must(r.md.Tracker().Delete(widgetsGVR, "out", "ghost"))
	r.md.ClearActions()
	failed := sink.warned("a release batch failed")
	crdHandler{tr}.OnDelete(crdMeta(widgetGK)) // the CRD is still there
	eventually(t, "the first try, and its batch to fail", func() bool {
		return r.heldLists() >= 1 && sink.warned("a release batch failed") > failed
	})
	if !r.heldBy(t, widgetGK, "out", "s") {
		t.Fatal("released with the CRD there")
	}
	r.removeCRD(t, widgetGK)
	eventually(t, "the hold to be released", func() bool { return !r.heldBy(t, widgetGK, "out", "s") })
}

// So too through the index, in PerName mode, whose part keeps what it dealt
// with across its retries (done): after an ask it reads again what a try before
// it read held.
func TestTheCRDsDeletionRereadsTheIndex(t *testing.T) {
	s, x := secretID("ns", "s"), secretID("ns", "x")
	var failX atomic.Bool
	r, sink, out := perNameFakeRig(t, []runtime.Object{
		heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"), holderObject("ns", "ghost", s)}, func(r *fakeRig) {
		r.md.PrependReactor("get", "secrets", func(a clienttesting.Action) (bool, runtime.Object, error) {
			if failX.Load() && a.(clienttesting.GetAction).GetName() == "x" {
				return true, nil, errors.New("test: x can't be read")
			}
			return false, nil, nil
		})
	}, s, x)
	tr := r.s.trackers[widgetGK]
	failX.Store(true) // the index's part fails for as long as the test runs
	r.runWrites(t)
	eventually(t, "the start's recheck to read the Secret", func() bool { return r.getsOf("s") >= 1 })
	r.idle(t, batchKey{widgetGK, "ns"})
	eventually(t, "the holder's watch", func() bool { return out.watching(1) })
	out.fail(errTest404)
	must(r.md.Tracker().Delete(widgetsGVR, "ns", "ghost"))
	r.md.ClearActions()
	failed := sink.warned("a release batch failed")
	crdHandler{tr}.OnDelete(crdMeta(widgetGK)) // the CRD is still there
	eventually(t, "the first try to read it, and its batch to fail", func() bool {
		return r.getsOf("s") >= 1 && sink.warned("a release batch failed") > failed
	})
	if !r.heldBy(t, widgetGK, "ns", "s") {
		t.Fatal("released with the CRD there")
	}
	r.removeCRD(t, widgetGK)
	eventually(t, "the hold to be released", func() bool { return !r.heldBy(t, widgetGK, "ns", "s") })
}

// getsOf counts the GETs of Secret ns/name made.
func (r *fakeRig) getsOf(name string) int {
	n := 0
	for _, a := range r.md.Actions() {
		if g, ok := a.(clienttesting.GetAction); ok && a.GetResource() == secretsGVR && g.GetName() == name {
			n++
		}
	}
	return n
}

// One recheck runs at a time; asks made while one of its parts runs are one,
// and run all the parts once more, whether that part then fails or succeeds.
func TestARecheckAskedForMeanwhile(t *testing.T) {
	for _, c := range []struct {
		name  string
		fails bool
		asks  int
	}{{"succeeds, one ask", false, 1}, {"succeeds, five asks", false, 5}, {"fails, one ask", true, 1}, {"fails, five asks", true, 5}} {
		t.Run(c.name, func(t *testing.T) {
			r, tr, _, vanish := ghostRig(t, SharedOptions{Namespaces: []string{"ns"}}, "out", false)
			vanish()
			r.runWrites(t)
			eventually(t, "the start's recheck", r.clusterHeldListed)
			r.idle(t, batchKey{widgetGK, "out"})
			if c.fails {
				r.forbidList.Store("secrets", true)
			}
			r.md.ClearActions()
			entered, release := r.gateList("secrets")
			crdHandler{tr}.OnDelete(crdMeta(widgetGK))
			within(t, "the recheck's first list", entered)
			for range c.asks {
				crdHandler{tr}.OnDelete(crdMeta(widgetGK))
			}
			time.Sleep(200 * time.Millisecond)
			// The gated list isn't recorded until it is let go: any list
			// recorded now is a second recheck's.
			if n := r.heldLists(); n != 0 {
				t.Errorf("%d lists while the first recheck's waits: two rechecks run", n)
			}
			close(release)
			eventually(t, "the recheck asked for", func() bool { return r.heldLists() >= 2 })
			time.Sleep(200 * time.Millisecond)
			if n := r.heldLists(); n != 2 {
				t.Errorf("%d rechecks, want the first and one more", n)
			}
			if !c.fails {
				eventually(t, "the recheck to end", func() bool {
					r.s.mu.Lock()
					defer r.s.mu.Unlock()
					return !tr.rechecking
				})
				return
			}
			// Its retry, once the list works, ends it.
			r.forbidList.Delete("secrets")
			r.md.ClearActions()
			r.clk.Step(releaseBackoffBase)
			eventually(t, "the retry", func() bool { return r.heldLists() >= 1 })
			eventually(t, "the recheck to end", func() bool {
				r.s.mu.Lock()
				defer r.s.mu.Unlock()
				return !tr.rechecking
			})
			if n := r.heldLists(); n != 1 {
				t.Errorf("%d rechecks once the list works, want its retry alone", n)
			}
		})
	}
}

// The CRD's deletion seen by a tracker that is no longer its kind's, as one
// that a Track rolled back while the CRD went, queues nothing and rechecks
// nothing: as endTracking, it acts only for the kind's tracker.
func TestARolledBackTrackersCRDDeletion(t *testing.T) {
	rollBack := func(t *testing.T, r *fakeRig, tr *Tracker) {
		r.s.mu.Lock()
		delete(r.s.trackers, widgetGK)
		r.s.mu.Unlock()
		t.Cleanup(func() {
			r.s.mu.Lock()
			r.s.trackers[widgetGK] = tr
			r.s.mu.Unlock()
		})
	}
	t.Run("what the caches show", func(t *testing.T) {
		r, tr, _, _ := ghostRig(t, SharedOptions{}, "ns", false)
		eventually(t, "the Secret's cache", func() bool { _, ok := (trackerViews{tr}).Protected(secretID("ns", "s")); return ok })
		r.drain()
		rollBack(t, r, tr)
		crdHandler{tr}.OnDelete(crdMeta(widgetGK))
		if got := r.queued(widgetGK, "ns"); len(got) != 0 {
			t.Errorf("queued %v for a tracker that isn't the kind's", got)
		}
	})
	t.Run("what they can't", func(t *testing.T) {
		r, tr, _, _ := ghostRig(t, SharedOptions{Namespaces: []string{"ns"}}, "out", false)
		r.runWrites(t)
		eventually(t, "the start's recheck", r.clusterHeldListed)
		r.idle(t, batchKey{widgetGK, "out"})
		rollBack(t, r, tr)
		r.md.ClearActions()
		// recheckSoon's own check, under mu: the rollback may come after
		// requeueHeld's.
		r.s.recheckSoon(tr)
		time.Sleep(200 * time.Millisecond)
		r.s.mu.Lock()
		rechecking := tr.rechecking
		r.s.mu.Unlock()
		if n := r.heldLists(); n != 0 || rechecking {
			t.Errorf("a recheck for a tracker that isn't the kind's: %d lists, rechecking %v", n, rechecking)
		}
	})
}

// A rebuild of the index while the kind is gone, as for a lost index or a
// stale marker, finds the union of the records empty, keeps every line, and
// marks the index: it doesn't fail for as long as the CRD is gone. While the CRD
// is there, the list's 404 fails it.
func TestARebuildWhileTheKindIsGone(t *testing.T) {
	ctx := context.Background()
	s, x := secretID("ns", "s"), secretID("ns", "x")
	r, _, out := perNameFakeRig(t, []runtime.Object{heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns")}, nil, s, x)
	tr := r.s.trackers[widgetGK]
	out.fail(errTest404)
	if err := tr.idx.rebuild(ctx); err == nil {
		t.Error("a rebuild with the CRD there, and the holders' list a 404, succeeded")
	}
	r.removeCRD(t, widgetGK)
	if err := tr.idx.rebuild(ctx); err != nil {
		t.Fatalf("a rebuild with the kind gone: %v", err)
	}
	cur, err := tr.idx.store.get(ctx)
	if err != nil || tr.idx.needsRebuild(cur) {
		t.Errorf("the index after the rebuild: %+v, %v; want it marked", cur, err)
	}
	if got := r.indexedIDs(t); len(got) != 2 {
		t.Errorf("the index names %v; want both lines kept", got)
	}
}

// A ghost's line, which the CRD's deletion's recheck finds bare, waits for the
// write loops' quiet start to end before it is pruned, as any line does
// (§10.6.2); the hold itself doesn't wait.
func TestAGhostsLineWaitsForTheQuietStart(t *testing.T) {
	s, missing := secretID("ns", "s"), secretID("ns", "missing")
	r, _, out := perNameFakeRig(t, []runtime.Object{
		heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"), holderObject("ns", "ghost", s, missing)}, nil, s, missing)
	r.s.SetPruneQuiet(time.Minute)
	r.runWrites(t)
	eventually(t, "the start's recheck to read the Secret, and its batch", func() bool {
		return r.getsOf("s") >= 2 && r.s.queue.Len() == 0 && !r.working(batchKey{widgetGK, "ns"})
	})
	eventually(t, "the holder's watch", func() bool { return out.watching(1) })
	out.fail(errTest404)
	must(r.md.Tracker().Delete(widgetsGVR, "ns", "ghost"))
	r.removeCRD(t, widgetGK)
	eventually(t, "the hold to be released", func() bool { return !r.heldBy(t, widgetGK, "ns", "s") })
	time.Sleep(200 * time.Millisecond)
	if got := r.indexedIDs(t); len(got) != 2 {
		t.Fatalf("the index names %v within the quiet start; want both lines kept", got)
	}
	deadline := time.Now().Add(20 * time.Second)
	for len(r.indexedIDs(t)) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the ghost's lines stay after the quiet start: %v", r.indexedIDs(t))
		}
		r.clk.Step(indexTick)
		time.Sleep(10 * time.Millisecond)
	}
}
