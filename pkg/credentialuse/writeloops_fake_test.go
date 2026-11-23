package credentialuse

// The write loops on client-go's fake clients and a fake clock (plan, PR
// 3b): the release loop and its requeues, the rescan, the reports of §12,
// the label janitor, the missing-ref prober, and the namespace-restricted
// caches.

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clienttesting "k8s.io/client-go/testing"
)

var (
	secretsGVR = schema.GroupVersionResource{Version: "v1", Resource: "secrets"}
	widgetsGVR = schema.GroupVersionResource{Group: widgetGK.Group, Version: "v1", Resource: "widgets"}
)

// namespaceObject is a live Namespace.
func namespaceObject(name string) *metav1.PartialObjectMetadata {
	m := &metav1.PartialObjectMetadata{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}}
	m.Name, m.UID = name, types.UID("ns-"+name)
	return m
}

// holderObject is an established Widget whose record names ids.
func holderObject(ns, name string, ids ...ID) *metav1.PartialObjectMetadata {
	m := &metav1.PartialObjectMetadata{TypeMeta: metav1.TypeMeta{APIVersion: widgetGK.Group + "/v1", Kind: "Widget"}}
	m.Namespace, m.Name, m.UID = ns, name, types.UID("w-"+ns+"-"+name)
	m.Finalizers = []string{ownerFinalizer}
	if len(ids) > 0 {
		rec, _ := EncodeRecord(ids)
		m.Annotations = map[string]string{AnnotationRefs: rec}
	}
	return m
}

// deleting marks m as being deleted at t.
func deleting(m *metav1.PartialObjectMetadata, t time.Time) *metav1.PartialObjectMetadata {
	ts := metav1.NewTime(t)
	m.DeletionTimestamp = &ts
	return m
}

// runWrites runs the write loops until the test ends, and returns a stop
// that ends them and waits.
func (r *fakeRig) runWrites(t *testing.T) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- r.s.RunWriteLoops(ctx) }()
	var once sync.Once
	var err error
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case err = <-errc:
			case <-time.After(20 * time.Second):
				err = errors.New("RunWriteLoops didn't return after its context ended")
			}
		})
		return err
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

// secretMeta reads Secret ns/name from the fake API server.
func (r *fakeRig) secretMeta(t *testing.T, ns, name string) *metav1.PartialObjectMetadata {
	t.Helper()
	obj, err := r.md.Tracker().Get(secretsGVR, ns, name)
	if err != nil {
		t.Fatalf("reading secret %s/%s: %v", ns, name, err)
	}
	return obj.(*metav1.PartialObjectMetadata)
}

// takeEvents returns the events recorded so far.
func (r *fakeRig) takeEvents() []string {
	var out []string
	for {
		select {
		case e := <-r.events.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

// A Call is one API call the protocol or the janitor makes, as a hook sees
// it.
type Call struct {
	Op        string // get, patch or list
	GK        schema.GroupKind
	Namespace string
	Name      string
	PatchType types.PatchType
}

// hookServer lets a test act before a server call, or fail it, and see its
// result.
type hookServer struct {
	server
	before func(ctx context.Context, c Call) error
	after  func(c Call, err error)
}

func (h hookServer) call(ctx context.Context, c Call, f func() error) error {
	if h.before != nil {
		if err := h.before(ctx, c); err != nil {
			return err
		}
	}
	err := f()
	if h.after != nil {
		h.after(c, err)
	}
	return err
}

func (h hookServer) Get(ctx context.Context, gk schema.GroupKind, ns, name string) (out *metav1.PartialObjectMetadata, err error) {
	err = h.call(ctx, Call{Op: "get", GK: gk, Namespace: ns, Name: name}, func() error {
		out, err = h.server.Get(ctx, gk, ns, name)
		return err
	})
	return out, err
}

func (h hookServer) Patch(ctx context.Context, gk schema.GroupKind, ns, name string, pt types.PatchType, data []byte) (out *metav1.PartialObjectMetadata, err error) {
	err = h.call(ctx, Call{Op: "patch", GK: gk, Namespace: ns, Name: name, PatchType: pt}, func() error {
		out, err = h.server.Patch(ctx, gk, ns, name, pt, data)
		return err
	})
	return out, err
}

func (h hookServer) List(ctx context.Context, gk schema.GroupKind, ns string) (out []metav1.PartialObjectMetadata, err error) {
	err = h.call(ctx, Call{Op: "list", GK: gk, Namespace: ns}, func() error {
		out, err = h.server.List(ctx, gk, ns)
		return err
	})
	return out, err
}

var fWidgetNS = func() string { f, _ := FinalizerName(widgetGK, "ns"); return f }()

// The release loop releases a hold that has no users: the initial list's
// batch, through R0 to R3.
func TestReleaseLoopReleasesAnUnusedHold(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"), heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"))
	r.track(t, widgetGK)
	r.run(t)
	r.runWrites(t)
	eventually(t, "the hold to be released", func() bool {
		return !hasFinalizer(r.secretMeta(t, "ns", "s"), fWidgetNS)
	})
	if s := r.secretMeta(t, "ns", "s"); s.Labels[LabelHeld] != "" || s.Labels[MarkerLabel(widgetGK)] != "" {
		t.Errorf("labels after the release: %v", s.Labels)
	}
}

// A Secret being deleted, with a live user, is kept for the grace, with
// §12's reports: WaitingForCredentialUsers on the Secret and
// CredentialDeleting on the user, and the waiting gauge. The batch comes
// back at the grace's end, on the queue's clock, and releases it then.
func TestReleaseLoopKeepsForTheGraceAndReports(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"),
		deleting(heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"), time.Now().Add(-time.Hour)),
		holderObject("ns", "h", secretID("ns", "s")))
	// The Secret carries another finalizer, as a Secret being deleted that
	// the API server keeps after the release would.
	obj := r.secretMeta(t, "ns", "s")
	obj.Finalizers = append(obj.Finalizers, "example.com/other")
	if err := r.md.Tracker().Update(secretsGVR, obj, "ns"); err != nil {
		t.Fatal(err)
	}
	r.track(t, widgetGK)
	r.run(t)
	r.runWrites(t)
	kind := kindLabel(widgetGK)

	eventually(t, "the Secret to be reported waiting", func() bool {
		evs := strings.Join(r.peekEvents(), "\n")
		return strings.Contains(evs, EventWaitingForUsers) && strings.Contains(evs, EventProtectedDeleting)
	})
	evs := r.takeEvents()
	var waiting, deletingEv string
	for _, e := range evs {
		switch {
		case strings.Contains(e, EventWaitingForUsers):
			waiting = e
		case strings.Contains(e, EventProtectedDeleting):
			deletingEv = e
		}
	}
	if !strings.HasPrefix(waiting, "Normal ") || !strings.Contains(waiting, "waiting for 1 user(s) of kind "+kind+" in namespace ns") ||
		!strings.Contains(waiting, string(KeptGrace)) {
		t.Errorf("WaitingForCredentialUsers = %q", waiting)
	}
	if !strings.HasPrefix(deletingEv, "Warning ") || !strings.Contains(deletingEv, "Secret ns/s is being deleted") ||
		!strings.Contains(deletingEv, string(KeptGrace)) {
		t.Errorf("CredentialDeleting = %q", deletingEv)
	}
	if n := r.metrics.waitingGauge(kind); n != 1 {
		t.Errorf("waiting gauge = %d, want 1", n)
	}
	if !hasFinalizer(r.secretMeta(t, "ns", "s"), fWidgetNS) {
		t.Fatal("the Secret was released inside its grace")
	}

	r.clk.Step(DefaultDeletionGrace)
	eventually(t, "the release at the grace's end", func() bool {
		return !hasFinalizer(r.secretMeta(t, "ns", "s"), fWidgetNS)
	})
	eventually(t, "the waiting gauge to drop", func() bool { return r.metrics.waitingGauge(kind) == 0 })
}

// peekEvents returns the events recorded so far, and keeps them.
func (r *fakeRig) peekEvents() []string {
	evs := r.takeEvents()
	for _, e := range evs {
		r.events.Events <- e
	}
	return evs
}

// A batch for a kind no tracker has, as a rolled-back Track may leave, is
// dropped with no call.
func TestReleaseLoopDropsBatchesOfUntrackedKinds(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"), heldObject(secretGVK, "ns", "s", "u1", gadgetGK, "ns"))
	r.track(t, widgetGK)
	r.run(t)
	r.md.ClearActions()
	r.s.queue.Add(batchKey{gadgetGK, "ns"}, secretID("ns", "s"))
	r.runWrites(t)
	eventually(t, "the batch to be taken", func() bool { return r.s.queue.Len() == 0 && !r.working(batchKey{gadgetGK, "ns"}) })
	for _, a := range r.md.Actions() {
		if a.GetResource().Resource == "secrets" && a.GetVerb() != "list" && a.GetVerb() != "watch" {
			t.Errorf("a batch of an untracked kind made a call: %s %s", a.GetVerb(), a.GetResource().Resource)
		}
	}
	if !hasFinalizer(r.secretMeta(t, "ns", "s"), mustFinalizer(gadgetGK, "ns")) {
		t.Error("the untracked kind's hold was touched")
	}
}

func mustFinalizer(gk schema.GroupKind, ns string) string {
	f, err := FinalizerName(gk, ns)
	if err != nil {
		panic(err)
	}
	return f
}

func (r *fakeRig) working(k batchKey) bool {
	r.s.queue.mu.Lock()
	defer r.s.queue.mu.Unlock()
	return r.s.queue.working[k]
}

// A batch that fails is retried after its backoff, on the queue's clock;
// the backoff doubles per (K, N) to its cap, and a success resets it.
func TestReleaseLoopBacksOff(t *testing.T) {
	var b batchBackoff
	k := batchKey{widgetGK, "ns"}
	var got []time.Duration
	for range 11 {
		got = append(got, b.fail(k))
	}
	want := []time.Duration{1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300}
	for i := range want {
		want[i] *= time.Second
	}
	if !slices.Equal(got, want) {
		t.Errorf("backoff = %v, want %v", got, want)
	}
	b.reset(k)
	if d := b.fail(k); d != time.Second {
		t.Errorf("after a reset, backoff = %s, want 1s", d)
	}

	r := newFakeRig(t, namespaceObject("ns"), heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"))
	var gets atomic.Int32
	r.s.hooks.wrapServer = func(srv server) server {
		return hookServer{server: srv, before: func(_ context.Context, c Call) error {
			if c.Op == "get" && c.GK == secretGroupKind && gets.Add(1) == 1 {
				return apierrors.NewInternalError(errors.New("test: once"))
			}
			return nil
		}}
	}
	r.track(t, widgetGK)
	r.run(t)
	r.runWrites(t)
	eventually(t, "the first attempt", func() bool { return gets.Load() == 1 })
	eventually(t, "the retry's timer", func() bool { return r.queueTimers() == 1 })
	if !hasFinalizer(r.secretMeta(t, "ns", "s"), fWidgetNS) {
		t.Fatal("released despite the failure")
	}
	r.clk.Step(releaseBackoffBase)
	eventually(t, "the retry to release the hold", func() bool { return !hasFinalizer(r.secretMeta(t, "ns", "s"), fWidgetNS) })
}

func (r *fakeRig) queueTimers() int {
	r.s.queue.mu.Lock()
	defer r.s.queue.mu.Unlock()
	return len(r.s.queue.timers)
}

// The write loops' Start rescans the protected caches: a batch the queue
// lost, as on a standby whose informers saw the hold before this process
// was elected, is enqueued again, and released.
func TestWriteLoopsRescan(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"), heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"))
	r.track(t, widgetGK)
	r.run(t)
	r.drain()
	if r.queued(widgetGK, "ns") != nil {
		t.Fatal("the batch is still queued after the drain")
	}
	r.runWrites(t)
	eventually(t, "the rescan's batch to release the hold", func() bool {
		return !hasFinalizer(r.secretMeta(t, "ns", "s"), fWidgetNS)
	})
}

// When the write loops' context ends under a batch, the batch goes back to
// the queue at once, for the next write loops, rather than after a backoff.
func TestReleaseLoopKeepsAnInterruptedBatch(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"), heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"))
	inGet := make(chan struct{})
	var once sync.Once
	r.s.hooks.wrapServer = func(srv server) server {
		return hookServer{server: srv, before: func(ctx context.Context, c Call) error {
			if c.Op == "get" && c.GK == secretGroupKind {
				once.Do(func() { close(inGet) })
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}}
	}
	r.track(t, widgetGK)
	r.run(t)
	stop := r.runWrites(t)
	select {
	case <-inGet:
	case <-time.After(20 * time.Second):
		t.Fatal("the batch never read the Secret")
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if got := r.queued(widgetGK, "ns"); !slices.Equal(got, []ID{secretID("ns", "s")}) {
		t.Errorf("after the write loops ended, the queued batch covers %v, want the Secret", got)
	}
	if r.queueTimers() != 0 {
		t.Error("the interrupted batch was put behind a backoff")
	}
}

// The janitor removes the labels a stripped object kept without finalizers
// (§10.1), and a janitor patch rejected by an unchanged object is reported,
// not retried as contention (§6.2 L1).
func TestJanitor(t *testing.T) {
	stripped := func() *metav1.PartialObjectMetadata {
		m := heldObject(secretGVK, "ns", "s", "u1", widgetGK)
		m.Labels[MarkerLabel(widgetGK)] = "true"
		return m
	}
	cleaned := func(r *fakeRig) bool {
		l := r.secretMeta(t, "ns", "s").Labels
		return l[LabelHeld] == "" && l[MarkerLabel(widgetGK)] == ""
	}
	t.Run("an object the informer delivers", func(t *testing.T) {
		r := newFakeRig(t, namespaceObject("ns"))
		r.track(t, widgetGK)
		r.run(t)
		r.runWrites(t)
		if err := r.md.Tracker().Create(secretsGVR, stripped(), "ns"); err != nil {
			t.Fatal(err)
		}
		eventually(t, "the labels to go", func() bool { return cleaned(r) })
	})
	t.Run("an object the rescan finds", func(t *testing.T) {
		r := newFakeRig(t, namespaceObject("ns"), stripped())
		r.track(t, widgetGK)
		r.run(t)
		// What the informers queued before the write loops is lost, as on a
		// process whose queue started after its informers had seen it.
		for r.s.janitor.Len() > 0 {
			id, _ := r.s.janitor.Get()
			r.s.janitor.Forget(id)
			r.s.janitor.Done(id)
		}
		r.runWrites(t)
		eventually(t, "the labels to go", func() bool { return cleaned(r) })
	})
	t.Run("a patch whose object changed is retried on a new read", func(t *testing.T) {
		r := newFakeRig(t, namespaceObject("ns"), stripped())
		var once sync.Once
		r.md.PrependReactor("patch", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) {
			var changed bool
			once.Do(func() {
				// Another writer changes the object under the patch, so its
				// resourceVersion test fails against a newer object.
				obj := r.secretMeta(t, "ns", "s")
				obj.Annotations["example.com/touched"] = "yes"
				obj.ResourceVersion = "2"
				if err := r.md.Tracker().Update(secretsGVR, obj, "ns"); err != nil {
					t.Error(err)
				}
				changed = true
			})
			if changed {
				return true, nil, apierrors.NewGenericServerResponse(422, "", schema.GroupResource{}, "", "", 0, false)
			}
			return false, nil, nil
		})
		var gets atomic.Int32
		r.md.PrependReactor("get", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) {
			gets.Add(1)
			return false, nil, nil
		})
		// One clean, with no informer running: nothing else can queue the
		// object again, so only clean's own retry can clean it.
		r.track(t, widgetGK)
		if err := r.s.clean(context.Background(), secretID("ns", "s")); err != nil {
			t.Fatal(err)
		}
		if !cleaned(r) || gets.Load() != 2 {
			t.Errorf("cleaned %v after %d reads; want cleaned on the second read", cleaned(r), gets.Load())
		}
	})
	t.Run("a failed read is retried with backoff", func(t *testing.T) {
		r := newFakeRig(t, namespaceObject("ns"), stripped())
		var failed atomic.Bool
		r.md.PrependReactor("get", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) {
			if failed.CompareAndSwap(false, true) {
				return true, nil, apierrors.NewInternalError(errors.New("etcd is unhappy"))
			}
			return false, nil, nil
		})
		r.track(t, widgetGK)
		r.run(t)
		r.runWrites(t)
		eventually(t, "the labels to go after the backoff", func() bool {
			r.clk.Step(janitorBackoffBase)
			return failed.Load() && cleaned(r)
		})
	})
	t.Run("an object that kept only a marker is swept at the write loops' start", func(t *testing.T) {
		// The held label is gone, so no cache holds it; the marker is left,
		// and scaletozero selects on it (§11.6).
		m := stripped()
		delete(m.Labels, LabelHeld)
		r := newFakeRig(t, namespaceObject("ns"), m)
		r.track(t, widgetGK)
		r.run(t)
		r.runWrites(t)
		eventually(t, "the marker to go", func() bool { return r.secretMeta(t, "ns", "s").Labels[MarkerLabel(widgetGK)] == "" })
	})
	t.Run("a rejected patch is reported", func(t *testing.T) {
		r := newFakeRig(t, namespaceObject("ns"), stripped())
		r.md.PrependReactor("patch", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) {
			// What the API server's patch handler returns for a failed test
			// op (pkg/endpoints/handlers/patch.go).
			return true, nil, apierrors.NewGenericServerResponse(422, "", schema.GroupResource{}, "", "", 0, false)
		})
		r.track(t, widgetGK)
		r.run(t)
		r.runWrites(t)
		eventually(t, "the rejection to be counted", func() bool {
			return r.metrics.count("release_rejected "+kindLabel(widgetGK)) > 0
		})
		// The metric is counted before the event is recorded
		// (coreReports.ReleaseRejected), so the event is waited for, not
		// taken in one drain right after the metric, which missed it
		// intermittently under -race.
		var seen []string
		eventually(t, "the CredentialReleaseRejected event", func() bool {
			seen = append(seen, r.takeEvents()...)
			return slices.ContainsFunc(seen, func(e string) bool { return strings.Contains(e, EventReleaseRejected) })
		})
	})
}

// The prober (§10.3): a ref a pass found Missing is probed on its own
// backoff, from ProbeInterval doubling to 5 minutes, by GET with few names
// and one LIST with many, falling back to GETs when RBAC refuses the LIST;
// a ref it finds enqueues its holders, and a pass that reports the ref
// Missing again resets its backoff.
func TestProber(t *testing.T) {
	ctx := context.Background()
	type rigWithCounts struct {
		*fakeRig
		t                *Tracker
		gets, plainLists atomic.Int32
		holder           *metav1.PartialObjectMetadata
	}
	setup := func(t *testing.T, names ...string) *rigWithCounts {
		holder := holderObject("ns", "h")
		r := &rigWithCounts{fakeRig: newFakeRig(t, namespaceObject("ns"), holder), holder: holder}
		var refs []Ref
		for _, n := range names {
			refs = append(refs, SecretRef("ns", n))
		}
		r.setRefs("h", refs...)
		r.md.PrependReactor("get", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) {
			r.gets.Add(1)
			return false, nil, nil
		})
		r.md.PrependReactor("list", "secrets", func(a clienttesting.Action) (bool, runtime.Object, error) {
			if a.(clienttesting.ListAction).GetListRestrictions().Labels.Empty() {
				r.plainLists.Add(1)
			}
			return false, nil, nil
		})
		r.t = r.track(t, widgetGK)
		a, err := r.t.Acquire(ctx, r.holder.DeepCopy())
		if err != nil || len(a.Unheld) != len(names) {
			t.Fatalf("Acquire = %+v, %v; want every ref Missing", a, err)
		}
		r.gets.Store(0)
		return r
	}
	interval := DefaultProbeInterval
	h := types.NamespacedName{Namespace: "ns", Name: "h"}

	t.Run("by GET, with backoff", func(t *testing.T) {
		r := setup(t, "new")
		r.t.probe(ctx)
		if r.gets.Load() != 0 {
			t.Fatal("probed before the ref was due")
		}
		r.clk.Step(interval)
		r.t.probe(ctx)
		if r.gets.Load() != 1 {
			t.Fatalf("%d GETs when due, want 1", r.gets.Load())
		}
		r.clk.Step(interval) // the backoff is now twice the interval
		r.t.probe(ctx)
		if r.gets.Load() != 1 {
			t.Fatal("probed again before the doubled backoff")
		}
		if err := r.md.Tracker().Create(secretsGVR, plainSecret("ns", "new"), "ns"); err != nil {
			t.Fatal(err)
		}
		r.clk.Step(interval)
		r.t.probe(ctx)
		if r.gets.Load() != 2 || r.enq.take()[h] != 1 {
			t.Errorf("%d GETs, holders enqueued: want the found ref's holder enqueued", r.gets.Load())
		}
	})
	t.Run("a pass that finds it Missing again resets the backoff", func(t *testing.T) {
		r := setup(t, "new")
		r.clk.Step(interval)
		r.t.probe(ctx) // backoff now doubled
		if _, err := r.t.Acquire(ctx, r.currentHolder(t)); err != nil {
			t.Fatal(err)
		}
		r.gets.Store(0)
		r.clk.Step(interval)
		r.t.probe(ctx)
		if r.gets.Load() != 1 {
			t.Errorf("%d GETs one interval after a pass reported it Missing, want 1", r.gets.Load())
		}
	})
	t.Run("by one LIST with many names", func(t *testing.T) {
		r := setup(t, "a", "b", "c", "d")
		if err := r.md.Tracker().Create(secretsGVR, plainSecret("ns", "c"), "ns"); err != nil {
			t.Fatal(err)
		}
		r.clk.Step(interval)
		r.t.probe(ctx)
		if r.plainLists.Load() != 1 || r.gets.Load() != 0 {
			t.Errorf("%d LISTs and %d GETs, want one LIST", r.plainLists.Load(), r.gets.Load())
		}
		if r.enq.take()[h] != 1 {
			t.Error("the found ref's holder wasn't enqueued")
		}
	})
	t.Run("GETs when RBAC refuses the LIST", func(t *testing.T) {
		r := setup(t, "a", "b", "c", "d")
		r.md.PrependReactor("list", "secrets", func(a clienttesting.Action) (bool, runtime.Object, error) {
			if a.(clienttesting.ListAction).GetListRestrictions().Labels.Empty() {
				// The authorizer's own message, under resourceNames RBAC.
				return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "",
					errors.New(`User "system:serviceaccount:ns:p" cannot list resource "secrets" in API group "" in the namespace "ns"`))
			}
			return false, nil, nil
		})
		r.clk.Step(interval)
		r.t.probe(ctx)
		if r.gets.Load() != 4 {
			t.Errorf("%d GETs after a refused LIST, want 4", r.gets.Load())
		}
	})
	t.Run("a holder's deletion drops its refs", func(t *testing.T) {
		r := setup(t, "new")
		r.run(t)
		if err := r.md.Tracker().Delete(widgetsGVR, "ns", "h"); err != nil {
			t.Fatal(err)
		}
		eventually(t, "the deleted holder's refs to be dropped", func() bool {
			return len(r.t.missing.due(r.clk.Now().Add(time.Hour))) == 0
		})
		r.clk.Step(interval)
		r.t.probe(ctx)
		if r.gets.Load() != 0 {
			t.Errorf("%d GETs for the refs of a deleted holder", r.gets.Load())
		}
	})
	t.Run("Forget drops its refs", func(t *testing.T) {
		r := setup(t, "new")
		r.t.Forget(r.currentHolder(t))
		r.clk.Step(interval)
		r.t.probe(ctx)
		if r.gets.Load() != 0 {
			t.Errorf("%d GETs for the refs of a forgotten holder", r.gets.Load())
		}
	})
	t.Run("a LIST that fails otherwise is retried at the same backoff", func(t *testing.T) {
		r := setup(t, "a", "b", "c", "d")
		var fail atomic.Bool
		fail.Store(true)
		r.md.PrependReactor("list", "secrets", func(a clienttesting.Action) (bool, runtime.Object, error) {
			if fail.Load() && a.(clienttesting.ListAction).GetListRestrictions().Labels.Empty() {
				// This reactor answers first, so it counts the LIST itself.
				r.plainLists.Add(1)
				return true, nil, apierrors.NewInternalError(errors.New("etcd is unhappy"))
			}
			return false, nil, nil
		})
		r.clk.Step(interval)
		r.t.probe(ctx)
		r.t.probe(ctx)
		if r.plainLists.Load() != 1 {
			t.Fatalf("%d LISTs: a failed LIST was retried before its backoff", r.plainLists.Load())
		}
		fail.Store(false)
		r.clk.Step(interval) // the backoff didn't double: due again after one interval
		r.t.probe(ctx)
		if r.plainLists.Load() != 2 {
			t.Errorf("%d LISTs one interval after the failure, want 2: the backoff is unchanged", r.plainLists.Load())
		}
	})
	t.Run("the write loops run it on their clock", func(t *testing.T) {
		r := setup(t, "new")
		if err := r.md.Tracker().Create(secretsGVR, plainSecret("ns", "new"), "ns"); err != nil {
			t.Fatal(err)
		}
		r.run(t)
		r.runWrites(t)
		eventually(t, "the prober's ticker", func() bool { return r.clk.HasWaiters() })
		r.clk.Step(interval)
		eventually(t, "the holder to be enqueued", func() bool {
			r.clk.Step(probeTick)
			return r.enq.count(h) > 0
		})
	})
}

func (r *fakeRig) currentHolder(t *testing.T) *metav1.PartialObjectMetadata {
	t.Helper()
	obj, err := r.md.Tracker().Get(widgetsGVR, "ns", "h")
	if err != nil {
		t.Fatal(err)
	}
	return obj.(*metav1.PartialObjectMetadata).DeepCopy()
}

func (e *enqueueLog) count(nn types.NamespacedName) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.got[nn]
}

// plainSecret is a Secret with no credential-use metadata, as one created
// for rotation is.
func plainSecret(ns, name string) *metav1.PartialObjectMetadata {
	m := &metav1.PartialObjectMetadata{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"}}
	m.Namespace, m.Name, m.UID = ns, name, types.UID("s-"+ns+"-"+name)
	return m
}

// SharedOptions.Namespaces restricts the protected caches (§10.5): an
// object outside them isn't cached, so it is read live, and its deletion,
// which no informer delivers, is noticed by the next pass of a holder that
// holds it, which enqueues its batch.
func TestNamespaceRestrictedCaches(t *testing.T) {
	ctx := context.Background()
	out := deleting(heldObject(secretGVK, "out", "s", "u-out", widgetGK, "out"), time.Now())
	out.Finalizers = append(out.Finalizers, "example.com/other")
	r := newFakeRigWith(t, SharedOptions{Namespaces: []string{"in"}},
		namespaceObject("in"), namespaceObject("out"), out,
		heldObject(secretGVK, "in", "s", "u-in", widgetGK, "in"),
		holderObject("out", "h", secretID("out", "s")))
	r.setRefs("h", SecretRef("out", "s"))
	tr := r.track(t, widgetGK)
	r.run(t)

	r.s.mu.Lock()
	var keys []protKey
	for k := range r.s.protected {
		keys = append(keys, k)
	}
	r.s.mu.Unlock()
	if !slices.Equal(keys, []protKey{{secretGroupKind, "in"}}) {
		t.Errorf("protected informers %v, want one for Secrets in \"in\"", keys)
	}
	views := trackerViews{tr}
	if _, ok := views.Protected(secretID("out", "s")); ok {
		t.Error("an object outside the namespaces is cached")
	}
	if _, ok := views.Protected(secretID("in", "s")); !ok {
		t.Error("an object inside the namespaces isn't cached")
	}

	r.drain()
	holder, err := r.md.Tracker().Get(widgetsGVR, "out", "h")
	if err != nil {
		t.Fatal(err)
	}
	a, err := tr.Acquire(ctx, holder.(*metav1.PartialObjectMetadata).DeepCopy())
	if err != nil || len(a.Held) != 1 {
		t.Fatalf("Acquire = %+v, %v; want the Secret held, read live", a, err)
	}
	if got := r.queued(widgetGK, "out"); !slices.Equal(got, []ID{secretID("out", "s")}) {
		t.Errorf("after a pass that held a deleting Secret outside the caches, the batch covers %v, want it", got)
	}
	tr.heldDeleting(secretID("in", "s"), "in")
	if got := r.queued(widgetGK, "in"); got != nil {
		t.Errorf("a deleting Secret inside the caches was enqueued by the pass (%v): its informer does that", got)
	}
}

// A batch added with no objects covers every cached object that carries
// the kind's finalizer for its namespace.
func TestReleaseBatchFillsAnEmptyBatch(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"), heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"))
	r.track(t, widgetGK)
	r.run(t)
	r.s.releaseBatch(context.Background(), batchKey{widgetGK, "ns"}, nil)
	if hasFinalizer(r.secretMeta(t, "ns", "s"), fWidgetNS) {
		t.Error("an empty batch released nothing")
	}
}

// CredentialDeleting goes to the live users of a kept object only: the
// deleting ones are named in WaitingForCredentialUsers.
func TestReportKeptWarnsTheLiveUsers(t *testing.T) {
	r := newFakeRig(t)
	tr := r.track(t, widgetGK)
	live, gone := types.NamespacedName{Namespace: "ns", Name: "live"}, types.NamespacedName{Namespace: "ns", Name: "going"}
	r.s.reportKept(tr, batchKey{widgetGK, "ns"}, []Kept{{P: secretID("ns", "s"), Why: KeptUserDeleting,
		Users: []types.NamespacedName{live, gone}, DeletingUsers: []types.NamespacedName{gone}}})
	evs := r.takeEvents()
	var warned, waiting int
	for _, e := range evs {
		switch {
		case strings.Contains(e, EventProtectedDeleting):
			warned++
		case strings.Contains(e, EventWaitingForUsers):
			waiting++
			if !strings.Contains(e, "being deleted: ns/going") || !strings.Contains(e, "2 user(s)") {
				t.Errorf("WaitingForCredentialUsers = %q; want both users counted and the deleting one named", e)
			}
		}
	}
	if warned != 1 || waiting != 1 {
		t.Errorf("events %q; want one CredentialDeleting, for the live user, and one WaitingForCredentialUsers", evs)
	}
}

// RunWriteLoops waits for the informers' first sync before it rescans and
// takes batches: a batch queued while the informers are still syncing
// waits. It is held for two seconds; the write loops that don't wait take
// it at once.
func TestWriteLoopsWaitForTheInformers(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"), heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"))
	r.track(t, widgetGK)
	entered, release := r.gateList("namespaces")
	defer close(release)
	r.s.queue.Add(batchKey{widgetGK, "ns"}, secretID("ns", "s"))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = r.s.RunInformers(ctx) }()
	<-entered
	r.runWrites(t)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.s.queue.Len() == 0 {
			t.Fatal("the write loops took a batch before the informers had synced")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A batch that asks to run again at a grace's end does, on the queue's
// clock: outside the cached namespaces no informer sets a timer for the
// deletion, so the batch's own RequeueAt is the only way back.
func TestReleaseLoopRequeuesAtTheGracesEnd(t *testing.T) {
	ctx := context.Background()
	out := heldObject(secretGVK, "out", "s", "u-out", widgetGK, "out")
	out.Finalizers = append(out.Finalizers, "example.com/other")
	r := newFakeRigWith(t, SharedOptions{Namespaces: []string{"in"}},
		namespaceObject("in"), namespaceObject("out"), out, holderObject("out", "h", secretID("out", "s")))
	r.setRefs("h", SecretRef("out", "s"))
	tr := r.track(t, widgetGK)
	r.run(t)
	r.runWrites(t)
	// The holder's initial list enqueued the Secret's batch, which keeps
	// it for its user, with no timer.
	k := batchKey{widgetGK, "out"}
	eventually(t, "the initial batch", func() bool {
		return r.s.queue.Len() == 0 && !r.working(k) && slices.ContainsFunc(r.md.Actions(), func(a clienttesting.Action) bool {
			return a.GetVerb() == "list" && a.GetResource().Resource == "widgets" && a.GetNamespace() == "out"
		})
	})
	if r.queueTimers() != 0 {
		t.Fatalf("%d timers after a batch that kept a live Secret", r.queueTimers())
	}

	// The Secret starts deleting. Nothing caches it, so only the holder's
	// next pass, which holds it, enqueues its batch.
	obj := r.secretMeta(t, "out", "s")
	if err := r.md.Tracker().Update(secretsGVR, deleting(obj, r.clk.Now()), "out"); err != nil {
		t.Fatal(err)
	}
	holder, err := r.md.Tracker().Get(widgetsGVR, "out", "h")
	if err != nil {
		t.Fatal(err)
	}
	if a, err := tr.Acquire(ctx, holder.(*metav1.PartialObjectMetadata).DeepCopy()); err != nil || len(a.Held) != 1 {
		t.Fatalf("Acquire = %+v, %v", a, err)
	}
	f := mustFinalizer(widgetGK, "out")
	// At least one timer: the write loops' recheck outside the caches
	// (§10.5), which runs beside the release loop from its start, may
	// enqueue the Secret's batch late, after the deletion, so that it runs
	// once more and sets a second timer for the same grace's end. A
	// duplicate requeue is harmless by design: R1 re-checks the grace
	// (§10.2). Waiting for exactly one timer failed intermittently under
	// -race, with two timers.
	eventually(t, "the batch to wait for the grace", func() bool { return r.queueTimers() >= 1 })
	if !hasFinalizer(r.secretMeta(t, "out", "s"), f) {
		t.Fatal("released inside the grace")
	}
	r.clk.Step(DefaultDeletionGrace)
	eventually(t, "the release at the grace's end", func() bool { return !hasFinalizer(r.secretMeta(t, "out", "s"), f) })
}

// A release batch for a kind that a Track is still adding waits for that
// Track (the review of #114): a release worker that takes it parks it, and
// it goes back to the queue when the Track succeeds, or is dropped when the
// Track fails. A Track after start replays the shared caches to its
// handlers, and those batches, the recheck of every hold already cached,
// reach the queue before the kind is tracked; dropping them stranded the
// hold of a holder deleted while the process was down. The Secret carries
// Gadget's finalizer and no Gadget holder exists, so once Gadget is tracked
// its batch releases it.
func TestReleaseLoopParksBatchesOfAKindBeingTracked(t *testing.T) {
	k := batchKey{gadgetGK, "ns"}
	held := func(r *fakeRig) bool {
		return hasFinalizer(r.secretMeta(t, "ns", "s"), mustFinalizer(gadgetGK, "ns"))
	}
	// start tracks Gadget after the write loops run, with its holder list
	// held, so the Track is in flight; queues Gadget's batch; and waits for
	// a worker to take it.
	start := func(t *testing.T) (r *fakeRig, release chan struct{}, done chan error) {
		r = newFakeRig(t, namespaceObject("ns"), heldObject(secretGVK, "ns", "s", "u1", gadgetGK, "ns"))
		r.track(t, widgetGK)
		r.run(t)
		r.runWrites(t)
		entered, release := r.gateList("gadgets")
		done = make(chan error, 1)
		go func() {
			_, err := r.s.Track(context.Background(), r.options(gadgetGK))
			done <- err
		}()
		<-entered
		r.s.queue.Add(k, secretID("ns", "s"))
		eventually(t, "a worker to take the batch", func() bool { return r.s.queue.Len() == 0 && !r.working(k) })
		r.s.mu.Lock()
		parked := len(r.s.tracking[gadgetGK].parked)
		r.s.mu.Unlock()
		if parked != 1 {
			t.Fatalf("the batch of a kind being tracked wasn't parked: %d parked", parked)
		}
		if !held(r) {
			t.Fatal("the hold was released before its kind was tracked")
		}
		return r, release, done
	}

	t.Run("the Track succeeds", func(t *testing.T) {
		r, release, done := start(t)
		close(release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		eventually(t, "the parked batch to release the hold", func() bool { return !held(r) })
	})

	t.Run("the Track fails", func(t *testing.T) {
		r, release, done := start(t)
		r.forbidList.Store("gadgets", true)
		close(release)
		if err := <-done; err == nil {
			t.Fatal("the Track didn't fail")
		}
		r.s.mu.Lock()
		_, inFlight := r.s.tracking[gadgetGK]
		r.s.mu.Unlock()
		if inFlight || r.s.queue.Len() != 0 || len(r.queued(gadgetGK, "ns")) != 0 {
			t.Fatalf("a failed Track left its parked batch behind: in flight %v, queued %v", inFlight, r.queued(gadgetGK, "ns"))
		}
		if !held(r) {
			t.Fatal("a failed Track's parked batch was worked")
		}
		// A later Track replays the caches again, and its batch releases the
		// hold.
		r.forbidList.Delete("gadgets")
		if _, err := r.s.Track(context.Background(), r.options(gadgetGK)); err != nil {
			t.Fatal(err)
		}
		eventually(t, "the later Track's batch to release the hold", func() bool { return !held(r) })
	})
}

// --- §10.5: held objects outside the protected caches (the user's
// decision of 2026-11-19) ---

var namespacesGVR = schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}

// releasedStepping steps the clock a grace at a time until released holds,
// so a release that waits for a grace measured from its first sight gets
// there; it fails after 20s of wall time.
func releasedStepping(t *testing.T, r *fakeRig, what string, released func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !released() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		r.clk.Step(DefaultDeletionGrace)
		time.Sleep(20 * time.Millisecond)
	}
}

// clusterHeldListed reports whether a live, label-selected list of secrets
// in every namespace has been made: §10.5's recheck outside the caches.
func (r *fakeRig) clusterHeldListed() bool {
	for _, a := range r.md.Actions() {
		l, ok := a.(clienttesting.ListAction)
		if ok && a.GetResource().Resource == "secrets" && a.GetNamespace() == "" &&
			l.GetListRestrictions().Labels.String() == heldSelector() {
			return true
		}
	}
	return false
}

func (r *fakeRig) heldBy(t *testing.T, gk schema.GroupKind, ns, name string) bool {
	return hasFinalizer(r.secretMeta(t, ns, name), mustFinalizer(gk, ns))
}

// A deleting Secret outside the cached namespaces keeps Widget's finalizer,
// and no Widget holder names it any more: its holder was deleted while the
// process was down, or the leader died between Forget and R3. No informer
// and no holder pass reaches it; the write loops' start rechecks it with a
// live list, and releases it.
func TestUncachedHoldsAreRecheckedWhenTheWriteLoopsStart(t *testing.T) {
	out := deleting(heldObject(secretGVK, "out", "s", "u-out", widgetGK, "out"), time.Now().Add(-time.Hour))
	r := newFakeRigWith(t, SharedOptions{Namespaces: []string{"in"}}, namespaceObject("in"), namespaceObject("out"), out)
	r.track(t, widgetGK)
	r.run(t)
	r.runWrites(t)
	releasedStepping(t, r, "the start's recheck to release the stranded hold", func() bool { return !r.heldBy(t, widgetGK, "out", "s") })
}

// A kind tracked after the write loops started gets the same recheck
// outside the caches: the replay of the caches covers only what they hold.
func TestATrackAfterTheWriteLoopsStartRechecksOutsideTheCaches(t *testing.T) {
	out := deleting(heldObject(secretGVK, "out", "s", "u-out", gadgetGK, "out"), time.Now().Add(-time.Hour))
	r := newFakeRigWith(t, SharedOptions{Namespaces: []string{"in"}}, namespaceObject("in"), namespaceObject("out"), out)
	r.track(t, widgetGK)
	r.run(t)
	r.runWrites(t)
	if _, err := r.s.Track(context.Background(), r.options(gadgetGK)); err != nil {
		t.Fatal(err)
	}
	releasedStepping(t, r, "the late Track's recheck to release the stranded hold", func() bool { return !r.heldBy(t, gadgetGK, "out", "s") })
}

// A held Secret outside the caches that appears after the start's recheck
// is deleted with its namespace. No informer delivers its deletion and no
// holder names it: the namespace's deletion enqueues each tracker's batch
// for it, which a live list fills, and the hold is released. Without that,
// the Secret's finalizer keeps the namespace Terminating, the krateo-057
// wedge (the review of #114, defect 2).
func TestAnUncachedNamespaceDeletionReleasesItsHolds(t *testing.T) {
	r := newFakeRigWith(t, SharedOptions{Namespaces: []string{"in"}}, namespaceObject("in"), namespaceObject("out"))
	r.track(t, widgetGK)
	r.run(t)
	r.runWrites(t)
	eventually(t, "the start's recheck outside the caches", r.clusterHeldListed)

	when := time.Now().Add(-time.Hour)
	if err := r.md.Tracker().Create(secretsGVR, deleting(heldObject(secretGVK, "out", "s", "u-out", widgetGK, "out"), when), "out"); err != nil {
		t.Fatal(err)
	}
	ns := deleting(namespaceObject("out"), when)
	if err := r.md.Tracker().Update(namespacesGVR, ns, ""); err != nil {
		t.Fatal(err)
	}
	releasedStepping(t, r, "the namespace's deletion to release the hold outside the caches", func() bool { return !r.heldBy(t, widgetGK, "out", "s") })
}

// A restricted process needs list on each protected kind in every
// namespace, for the rechecks outside the caches: without it, Run fails at
// once and says which grant is missing.
func TestRestrictedCachesNeedAClusterList(t *testing.T) {
	r := newFakeRigWith(t, SharedOptions{Namespaces: []string{"in"}}, namespaceObject("in"))
	r.denied.Store("list secrets in *", true)
	r.track(t, widgetGK)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := r.s.RunInformers(ctx)
	if err == nil || !strings.Contains(err.Error(), "grant list on secrets in every namespace") {
		t.Fatalf("RunInformers = %v, want the cluster-wide list refused", err)
	}
	// With the cluster-wide list granted, the same process runs.
	r2 := newFakeRigWith(t, SharedOptions{Namespaces: []string{"in"}}, namespaceObject("in"))
	r2.track(t, widgetGK)
	r2.run(t)
}

// The waiting gauge counts a deleting object outside the caches that the
// release loop keeps for its users, and stops counting it once released.
func TestTheWaitingGaugeCountsKeptObjectsOutsideTheCaches(t *testing.T) {
	out := deleting(heldObject(secretGVK, "out", "s", "u-out", widgetGK, "out"), time.Now().Add(-time.Hour))
	r := newFakeRigWith(t, SharedOptions{Namespaces: []string{"in"}}, namespaceObject("in"), namespaceObject("out"), out,
		holderObject("out", "h", secretID("out", "s")))
	r.track(t, widgetGK)
	r.run(t)
	r.runWrites(t)
	kind := kindLabel(widgetGK)
	eventually(t, "the kept object outside the caches to be counted", func() bool { return r.metrics.waitingGauge(kind) == 1 })
	if err := r.md.Tracker().Delete(widgetsGVR, "out", "h"); err != nil {
		t.Fatal(err)
	}
	releasedStepping(t, r, "the hold to be released once its user goes", func() bool { return !r.heldBy(t, widgetGK, "out", "s") })
	eventually(t, "the released object to leave the gauge", func() bool { return r.metrics.waitingGauge(kind) == 0 })
}

// An Add with no objects asks for every held object of the namespace, and
// that survives the merge of objects added before the batch is taken (the
// re-review of #114): during a teardown the holders' events fill (K, N) all
// the time, and a merged batch that ran on their objects alone never listed
// the namespace, so a hold nobody names stayed.
func TestAnEmptyBatchMergedWithObjectsStillCoversItsNamespace(t *testing.T) {
	k := batchKey{widgetGK, "out"}
	s1 := secretID("out", "s1")

	q := newBatchQueue(newFakeRig(t).clk)
	q.Add(k)
	q.Add(k, s1)
	if _, ids, ok := q.Get(context.Background()); !ok || !slices.Contains(ids, fillAll) || !slices.Contains(ids, s1) {
		t.Fatalf("Get = %v, %v: the merge lost the namespace's list or the object", ids, ok)
	}

	// s1 has a user, s2 none: the merged batch releases s2 and keeps s1.
	r := newFakeRigWith(t, SharedOptions{Namespaces: []string{"in"}}, namespaceObject("in"), namespaceObject("out"),
		heldObject(secretGVK, "out", "s1", "u1", widgetGK, "out"), holderObject("out", "h", s1),
		heldObject(secretGVK, "out", "s2", "u2", widgetGK, "out"))
	r.track(t, widgetGK)
	r.run(t)
	r.s.releaseBatch(context.Background(), k, []ID{s1, fillAll})
	if r.heldBy(t, widgetGK, "out", "s2") {
		t.Error("a batch merged with an object didn't list its namespace: the hold nobody names stayed")
	}
	if !r.heldBy(t, widgetGK, "out", "s1") {
		t.Error("the batch released an object that has a user")
	}
}

// The recheck outside the caches retries a list that failed, on the release
// loop's backoff: a recheck that is skipped can strand a hold for good.
func TestTheRecheckOutsideTheCachesIsRetried(t *testing.T) {
	out := deleting(heldObject(secretGVK, "out", "s", "u-out", widgetGK, "out"), time.Now().Add(-time.Hour))
	r := newFakeRigWith(t, SharedOptions{Namespaces: []string{"in"}}, namespaceObject("in"), namespaceObject("out"), out)
	var failed atomic.Bool
	r.md.PrependReactor("list", "secrets", func(a clienttesting.Action) (bool, runtime.Object, error) {
		if a.GetNamespace() == "" && a.(clienttesting.ListAction).GetListRestrictions().Labels.String() == heldSelector() &&
			failed.CompareAndSwap(false, true) {
			return true, nil, apierrors.NewInternalError(errors.New("etcd is unhappy"))
		}
		return false, nil, nil
	})
	r.track(t, widgetGK)
	r.run(t)
	r.runWrites(t)
	eventually(t, "the first recheck to fail", failed.Load)
	releasedStepping(t, r, "the retried recheck to release the hold", func() bool { return !r.heldBy(t, widgetGK, "out", "s") })
}

// One process runs one set of write loops: a second RunWriteLoops while the
// first runs fails at once, and leaves the first's context alone.
func TestRunWriteLoopsRunsOnce(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"))
	r.track(t, widgetGK)
	r.run(t)
	r.runWrites(t)
	eventually(t, "the write loops to start", func() bool {
		r.s.mu.Lock()
		defer r.s.mu.Unlock()
		return r.s.writeCtx != nil
	})
	// A second RunWriteLoops that ran would block until its ctx ends, or
	// for good, since two sets of loops can deadlock on the shared queues;
	// the refusal returns at once.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.s.RunWriteLoops(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, errWriteLoopsRunning) {
			t.Fatalf("a second RunWriteLoops = %v, want errWriteLoopsRunning", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a second RunWriteLoops neither was refused nor returned: two sets of write loops ran")
	}
	r.s.mu.Lock()
	running := r.s.writeCtx != nil
	r.s.mu.Unlock()
	if !running {
		t.Error("the refused RunWriteLoops cleared the running one's context")
	}
}

// What the release loop counted outside the caches leaves the waiting gauge
// when the write loops stop: a replica that keeps running without them
// mustn't report it.
func TestTheWaitingGaugeOutsideTheCachesClearsWhenTheWriteLoopsStop(t *testing.T) {
	out := deleting(heldObject(secretGVK, "out", "s", "u-out", widgetGK, "out"), time.Now().Add(-time.Hour))
	r := newFakeRigWith(t, SharedOptions{Namespaces: []string{"in"}}, namespaceObject("in"), namespaceObject("out"), out,
		holderObject("out", "h", secretID("out", "s")))
	r.track(t, widgetGK)
	r.run(t)
	stop := r.runWrites(t)
	kind := kindLabel(widgetGK)
	eventually(t, "the kept object outside the caches to be counted", func() bool { return r.metrics.waitingGauge(kind) == 1 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if n := r.metrics.waitingGauge(kind); n != 0 {
		t.Errorf("waiting gauge %d after the write loops stopped, want 0", n)
	}
}
