package credentialuse

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clienttesting "k8s.io/client-go/testing"
)

// The recheck that a CRD's deletion asks for while one runs (recheckSoon): it
// cuts the run's wait short, and never its work. The clock stands still in
// each test, so that no retry's backoff ends: what the tests see, only an ask
// did.

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
	gets := func() int {
		n := 0
		for _, a := range r.md.Actions() {
			if g, ok := a.(clienttesting.GetAction); ok && a.GetResource() == secretsGVR && g.GetName() == "s" {
				n++
			}
		}
		return n
	}
	r.runWrites(t)
	eventually(t, "the start's recheck to read the Secret", func() bool { return gets() >= 1 })
	r.idle(t, batchKey{widgetGK, "ns"})
	eventually(t, "the holder's watch", func() bool { return out.watching(1) })
	out.fail(errTest404)
	must(r.md.Tracker().Delete(widgetsGVR, "ns", "ghost"))
	r.md.ClearActions()
	failed := sink.warned("a release batch failed")
	crdHandler{tr}.OnDelete(crdMeta(widgetGK)) // the CRD is still there
	eventually(t, "the first try to read it, and its batch to fail", func() bool {
		return gets() >= 1 && sink.warned("a release batch failed") > failed
	})
	if !r.heldBy(t, widgetGK, "ns", "s") {
		t.Fatal("released with the CRD there")
	}
	r.removeCRD(t, widgetGK)
	eventually(t, "the hold to be released", func() bool { return !r.heldBy(t, widgetGK, "ns", "s") })
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

// ctxCallServer and ctxIndexStore fail a call whose context has ended, as a real
// client does, and the fake ones don't.
type ctxCallServer struct{ server }

func (s ctxCallServer) Get(ctx context.Context, gk schema.GroupKind, ns, name string) (*metav1.PartialObjectMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.server.Get(ctx, gk, ns, name)
}

func (s ctxCallServer) Patch(ctx context.Context, gk schema.GroupKind, ns, name string, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.server.Patch(ctx, gk, ns, name, pt, data)
}

func (s ctxCallServer) List(ctx context.Context, gk schema.GroupKind, ns string) ([]metav1.PartialObjectMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.server.List(ctx, gk, ns)
}

// ctxIndexStore holds the write that removes line until release closes, once
// gate is set, after the write landed.
type ctxIndexStore struct {
	indexStore
	line    string
	gate    *atomic.Bool
	once    *sync.Once
	entered chan struct{}
	release chan struct{}
}

func (s ctxIndexStore) get(ctx context.Context) (indexVersion, error) {
	if err := ctx.Err(); err != nil {
		return indexVersion{}, err
	}
	return s.indexStore.get(ctx)
}

func (s ctxIndexStore) update(ctx context.Context, cur indexVersion, lines []string, mark kindSet) (indexVersion, error) {
	if err := ctx.Err(); err != nil {
		return indexVersion{}, err
	}
	v, err := s.indexStore.update(ctx, cur, lines, mark)
	if err == nil && s.gate.Load() && slices.Contains(cur.lines, s.line) && !slices.Contains(lines, s.line) {
		s.once.Do(func() {
			close(s.entered)
			<-s.release
		})
	}
	return v, err
}

func (s ctxIndexStore) create(ctx context.Context, lines []string) (indexVersion, error) {
	if err := ctx.Err(); err != nil {
		return indexVersion{}, err
	}
	return s.indexStore.create(ctx, lines)
}

// An ask while the recheck's index prune is between its write and its check of
// what it removed cuts nothing: the check is made, and the line of an object
// that an A2 held meanwhile comes back at once, without a report of tampering
// (§10.6.2).
func TestAnAskCutsNoPrune(t *testing.T) {
	s, x := secretID("ns", "s"), secretID("ns", "x")
	var gate atomic.Bool
	entered, release := make(chan struct{}), make(chan struct{})
	r, sink, _ := perNameFakeRig(t, []runtime.Object{
		heldObject(secretGVK, "ns", "x", "u-x", widgetGK, "ns"), holderObject("ns", "h", x),
		heldObject(secretGVK, "ns", "s", "u-s", widgetGK)}, func(r *fakeRig) {
		r.s.hooks.wrapServer = func(srv server) server { return ctxCallServer{srv} }
		r.s.hooks.wrapIndex = func(st indexStore) indexStore {
			return ctxIndexStore{indexStore: st, line: s.String(), gate: &gate, once: &sync.Once{}, entered: entered, release: release}
		}
	}, x)
	tr := r.s.trackers[widgetGK]
	r.runWrites(t)
	eventually(t, "the start's recheck to read x", func() bool {
		for _, a := range r.md.Actions() {
			if g, ok := a.(clienttesting.GetAction); ok && a.GetResource() == secretsGVR && g.GetName() == "x" {
				return true
			}
		}
		return false
	})
	r.idle(t, batchKey{widgetGK, "ns"})
	// The line of s, which carries nothing, and which no record names: the
	// recheck prunes it.
	cm, err := r.dyn.Resource(configMapGVR).Namespace("idx").Get(context.Background(), IndexName(widgetGK), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cm.Object["data"] = map[string]any{indexDataKey: encodeIndex(linesOf([]ID{s, x}))}
	if _, err := r.dyn.Resource(configMapGVR).Namespace("idx").Update(context.Background(), cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	gate.Store(true)
	crdHandler{tr}.OnDelete(crdMeta(widgetGK))
	within(t, "the prune's write", entered)
	// An A2 of a new holder lands after the prune read s, and the CRD's
	// deletion asks again. The holder keeps the hold, and the line, after.
	must(r.md.Tracker().Create(widgetsGVR, holderObject("ns", "h2", s), "ns"))
	must(r.md.Tracker().Update(secretsGVR, heldObject(secretGVK, "ns", "s", "u-s", widgetGK, "ns"), "ns"))
	crdHandler{tr}.OnDelete(crdMeta(widgetGK))
	close(release)
	eventually(t, "the line of s to come back", func() bool { return slices.Contains(r.indexedIDs(t), s) })
	if n := sink.warned("a line dropped from the index named an object"); n != 0 {
		t.Errorf("tampering reported %d times: %q", n, sink.warns)
	}
}
