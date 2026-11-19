package credentialuse

// The core's client-go runtime on client-go's fake clients and a fake clock
// (the review of #109): Track's concurrency and rollback (D2, D3), the
// recheck of a shared cache by a Track after Run (D4), the protected
// handler (G2, G4, G5, G6, N3, N5), the queue's shutdown (G6), publishing
// the settings (G3), and the RBAC checks before an informer starts (N2).
// The fake metadata client says it can't stream lists, so the reflectors
// list with a List.

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/metadata"
	metadatafake "k8s.io/client-go/metadata/fake"
	clienttesting "k8s.io/client-go/testing"
	testingclock "k8s.io/utils/clock/testing"

	"github.com/krateo-platformops/provider-runtime/pkg/logging"
)

var (
	gadgetGK     = schema.GroupKind{Group: widgetGK.Group, Kind: "Gadget"}
	configMapGVK = schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}
	configMapGK  = configMapGVK.GroupKind()
)

// enqueueLog records the holders the core enqueues.
type enqueueLog struct {
	mu  sync.Mutex
	got map[types.NamespacedName]int
}

func (e *enqueueLog) add(nn types.NamespacedName) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.got == nil {
		e.got = map[types.NamespacedName]int{}
	}
	e.got[nn]++
}

func (e *enqueueLog) take() map[types.NamespacedName]int {
	e.mu.Lock()
	defer e.mu.Unlock()
	got := e.got
	e.got = nil
	return got
}

// fakeRig is a Shared on fake clients and a fake clock.
type fakeRig struct {
	s       *Shared
	clk     *testingclock.FakeClock
	md      *metadatafake.FakeMetadataClient
	dyn     *dynamicfake.FakeDynamicClient
	metrics *countingMetrics
	enq     *enqueueLog

	// denied are the "verb resource" pairs the access reviews refuse;
	// forbidList, the resources whose lists the API server forbids.
	denied, forbidList sync.Map
	// listGate, if set, runs before each list, outside the fake client:
	// its reactors run under one lock, so a reactor that blocks blocks
	// every call.
	listGate atomic.Pointer[func(resource string)]
}

// gatedMetadata is the fake metadata client, with the rig's list gate.
type gatedMetadata struct {
	*metadatafake.FakeMetadataClient
	r *fakeRig
}

func (g gatedMetadata) Resource(gvr schema.GroupVersionResource) metadata.Getter {
	return gatedGetter{Getter: g.FakeMetadataClient.Resource(gvr), resource: gvr.Resource, r: g.r}
}

type gatedGetter struct {
	metadata.Getter
	resource string
	r        *fakeRig
}

func (g gatedGetter) List(ctx context.Context, opts metav1.ListOptions) (*metav1.PartialObjectMetadataList, error) {
	if f := g.r.listGate.Load(); f != nil {
		(*f)(g.resource)
	}
	return g.Getter.List(ctx, opts)
}

// gateList makes the first list of resource wait until release is closed,
// and closes entered when it starts waiting.
func (r *fakeRig) gateList(resource string) (entered, release chan struct{}) {
	entered, release = make(chan struct{}), make(chan struct{})
	var once sync.Once
	f := func(res string) {
		if res != resource {
			return
		}
		var first bool
		once.Do(func() { first = true })
		if first {
			close(entered)
			<-release
		}
	}
	r.listGate.Store(&f)
	return entered, release
}

func crdOf(gk schema.GroupKind) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("apiextensions.k8s.io/v1")
	u.SetKind("CustomResourceDefinition")
	u.SetName(strings.ToLower(gk.Kind) + "s." + gk.Group)
	u.SetResourceVersion("7")
	return u
}

func newFakeRig(t *testing.T, objs ...runtime.Object) *fakeRig {
	t.Helper()
	mapper := meta.NewDefaultRESTMapper(nil)
	for _, gvk := range []schema.GroupVersionKind{widgetGK.WithVersion("v1"), gadgetGK.WithVersion("v1"), secretGVK, configMapGVK} {
		mapper.Add(gvk, meta.RESTScopeNamespace)
	}
	r := &fakeRig{clk: testingclock.NewFakeClock(time.Now()), metrics: &countingMetrics{}, enq: &enqueueLog{}}
	scheme := metadatafake.NewTestScheme()
	if err := metav1.AddMetaToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	r.md = metadatafake.NewSimpleMetadataClient(scheme, objs...)
	r.md.PrependReactor("list", "*", func(a clienttesting.Action) (bool, runtime.Object, error) {
		if _, ok := r.forbidList.Load(a.GetResource().Resource); ok {
			return true, nil, apierrors.NewForbidden(a.GetResource().GroupResource(), "", errors.New("credential-use test: forbidden"))
		}
		return false, nil, nil
	})
	r.dyn = dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{crdGVR: "CustomResourceDefinitionList"}, crdOf(widgetGK), crdOf(gadgetGK))
	r.dyn.PrependReactor("create", "selfsubjectaccessreviews", func(a clienttesting.Action) (bool, runtime.Object, error) {
		obj := a.(clienttesting.CreateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
		verb, _, _ := unstructured.NestedString(obj.Object, "spec", "resourceAttributes", "verb")
		res, _, _ := unstructured.NestedString(obj.Object, "spec", "resourceAttributes", "resource")
		_, denied := r.denied.Load(verb + " " + res)
		_ = unstructured.SetNestedField(obj.Object, !denied, "status", "allowed")
		return true, obj, nil
	})
	r.s = newShared(Clients{Metadata: gatedMetadata{r.md, r}, Dynamic: r.dyn, Mapper: mapper}, SharedOptions{}, r.clk)
	r.s.syncTimeout.Store(int64(20 * time.Second))
	return r
}

func (r *fakeRig) options(gk schema.GroupKind, protected ...ProtectedKind) Options {
	return Options{
		Holder:          gk.WithVersion("v1"),
		OwnerFinalizers: ownerFinalizers,
		Refs:            func(metav1.Object) ([]Ref, error) { return nil, nil },
		Enqueue:         r.enq.add,
		TimeoutBound:    90 * time.Second,
		Metrics:         r.metrics,
		Protected:       protected,
	}
}

func (r *fakeRig) track(t *testing.T, gk schema.GroupKind, protected ...ProtectedKind) *Tracker {
	t.Helper()
	tr, err := r.s.Track(context.Background(), r.options(gk, protected...))
	if err != nil {
		t.Fatalf("Track(%s): %v", gk, err)
	}
	return tr
}

// run runs the informers until the test ends, and waits for their sync.
func (r *fakeRig) run(t *testing.T) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- r.s.RunInformers(ctx) }()
	var once sync.Once
	var runErr error
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case runErr = <-errc:
			case <-time.After(20 * time.Second):
				runErr = errors.New("RunInformers didn't return after its context ended")
			}
		})
		return runErr
	}
	t.Cleanup(func() { _ = stop() })
	eventually(t, "RunInformers to sync", func() bool {
		select {
		case err := <-errc:
			errc <- err
			t.Fatalf("RunInformers returned before it synced: %v", err)
		default:
		}
		return r.s.HasSynced()
	})
	return stop
}

// eventually polls cond until it holds, failing t after 20 seconds.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func (r *fakeRig) queued(gk schema.GroupKind, ns string) []ID {
	q := r.s.queue
	q.mu.Lock()
	defer q.mu.Unlock()
	k := batchKey{gk, ns}
	if !q.queued[k] {
		return nil
	}
	return SortedIDs(keysOf(q.pending[k]))
}

func (r *fakeRig) drain() {
	for r.s.queue.Len() > 0 {
		k, _, ok := r.s.queue.Get(context.Background())
		if !ok {
			return
		}
		r.s.queue.Done(k)
	}
}

func (r *fakeRig) tracked(gk schema.GroupKind) bool {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	_, ok := r.s.trackers[gk]
	return ok
}

func (r *fakeRig) protectedOf(gk schema.GroupKind) *protectedInformer {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	return r.s.protected[gk]
}

// heldObject is a protected object of kind gvk carrying finalizer and epoch
// "e1" for holder kind k in each of namespaces, with the held label.
func heldObject(gvk schema.GroupVersionKind, ns, name string, uid types.UID, k schema.GroupKind, namespaces ...string) *metav1.PartialObjectMetadata {
	m := &metav1.PartialObjectMetadata{TypeMeta: metav1.TypeMeta{APIVersion: gvk.GroupVersion().String(), Kind: gvk.Kind}}
	m.Namespace, m.Name, m.UID = ns, name, uid
	m.Labels = map[string]string{LabelHeld: "true"}
	m.Annotations = map[string]string{}
	for _, n := range namespaces {
		f, _ := FinalizerName(k, n)
		m.Finalizers = append(m.Finalizers, f)
		m.Annotations[f] = "e1"
	}
	return m
}

func secretID(ns, name string) ID { return SecretRef(ns, name).ID() }

// D2: a Track for a kind another Track is adding waits for that Track to
// end, and gets its tracker once it has synced.
func TestConcurrentTracksWaitForTheSync(t *testing.T) {
	r := newFakeRig(t)
	r.run(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	r.md.PrependReactor("list", "widgets", func(clienttesting.Action) (bool, runtime.Object, error) {
		once.Do(func() {
			close(entered)
			<-release
		})
		return false, nil, nil
	})
	type result struct {
		t      *Tracker
		err    error
		synced bool
	}
	track := func(out chan<- result) {
		tr, err := r.s.Track(context.Background(), r.options(widgetGK))
		res := result{t: tr, err: err}
		if tr != nil {
			res.synced = tr.holders.HasSynced()
		}
		out <- res
	}
	first, second := make(chan result, 1), make(chan result, 1)
	go track(first)
	select {
	case <-entered:
	case <-time.After(20 * time.Second):
		t.Fatal("the first Track never listed the holders")
	}
	go track(second)
	var early *result
	eventually(t, "the second Track to wait or return", func() bool {
		select {
		case res := <-second:
			early = &res
			return true
		default:
			return r.s.trackWaiting.Load() == 1
		}
	})
	close(release)
	if early != nil {
		t.Fatalf("the second Track returned (%+v) while the first was still syncing", *early)
	}
	a, b := <-first, <-second
	if a.err != nil || b.err != nil {
		t.Fatalf("Tracks: %v, %v", a.err, b.err)
	}
	if a.t != b.t || !b.synced {
		t.Errorf("the second Track got %p (synced %v), want the first's %p, synced", b.t, b.synced, a.t)
	}
}

// A slow Track of one kind doesn't delay a Track of another: no lock is
// held across an API call (the re-review of #109).
func TestTrackOfOneKindDoesntWaitForAnother(t *testing.T) {
	r := newFakeRig(t)
	r.run(t)
	entered, release := r.gateList("gadgets")
	gadget := make(chan error, 1)
	go func() {
		_, err := r.s.Track(context.Background(), r.options(gadgetGK))
		gadget <- err
	}()
	select {
	case <-entered:
	case <-time.After(20 * time.Second):
		t.Fatal("the Gadget's Track never listed its holders")
	}
	widget := make(chan error, 1)
	go func() {
		_, err := r.s.Track(context.Background(), r.options(widgetGK))
		widget <- err
	}()
	select {
	case err := <-widget:
		if err != nil {
			t.Errorf("Track(Widget) = %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Error("Track(Widget) waited for the slow Track(Gadget)")
	}
	close(release)
	if err := <-gadget; err != nil {
		t.Errorf("Track(Gadget) = %v", err)
	}
}

// A Track after start that waits for a protected informer RunInformers
// should start stops waiting as soon as RunInformers fails, not after its
// timeout (the re-review of #109).
func TestTrackStopsWaitingWhenRunInformersFails(t *testing.T) {
	r := newFakeRig(t)
	r.s.syncTimeout.Store(int64(10 * time.Minute))
	r.track(t, widgetGK) // the Secret informer is RunInformers'
	// RunInformers' first list of the Secrets waits, and is then
	// forbidden, which fails RunInformers.
	entered, release := r.gateList("secrets")
	r.forbidList.Store("secrets", true)
	run := make(chan error, 1)
	go func() { run <- r.s.RunInformers(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(20 * time.Second):
		t.Fatal("RunInformers never listed the Secrets")
	}
	tracked := make(chan error, 1)
	go func() {
		_, err := r.s.Track(context.Background(), r.options(gadgetGK))
		tracked <- err
	}()
	secrets := r.protectedOf(secretGroupKind)
	eventually(t, "the Gadget's Track to join the Secret informer", func() bool {
		return len(secrets.snapshot()) == 2
	})
	close(release)
	if err := <-run; err == nil {
		t.Fatal("RunInformers succeeded without namespaces")
	}
	select {
	case err := <-tracked:
		if err == nil || !strings.Contains(err.Error(), "RunInformers has ended") {
			t.Errorf("Track = %v, want it stopped by RunInformers' end", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Track kept waiting after RunInformers failed")
	}
	if r.tracked(gadgetGK) {
		t.Error("the failed Track left its tracker")
	}
}

// claimsOf is p's claims.
func (r *fakeRig) claimsOf(p *protectedInformer) int {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	return p.claims
}

// Two kinds share a protected informer a Track after Run creates. It runs
// under RunInformers' context, and each Track waits for it on its own
// budget: the first Track's budget running out fails that Track only, the
// informer stays while the second claims it, and the second syncs (the
// re-review of #109).
func TestSharedInformerWaitersKeepTheirOwnBudget(t *testing.T) {
	r := newFakeRig(t)
	r.run(t)
	entered, release := r.gateList("configmaps")
	cm := []ProtectedKind{{GVK: configMapGVK}}
	r.s.syncTimeout.Store(int64(2 * time.Second))
	first := make(chan error, 1)
	go func() {
		_, err := r.s.Track(context.Background(), r.options(gadgetGK, cm...))
		first <- err
	}()
	select {
	case <-entered:
	case <-time.After(20 * time.Second):
		t.Fatal("the ConfigMap informer never listed")
	}
	p := r.protectedOf(configMapGK)
	r.s.syncTimeout.Store(int64(10 * time.Second))
	second := make(chan error, 1)
	go func() {
		_, err := r.s.Track(context.Background(), r.options(widgetGK, cm...))
		second <- err
	}()
	eventually(t, "both Tracks to claim the ConfigMap informer", func() bool { return r.claimsOf(p) == 2 })
	select {
	case err := <-first:
		if err == nil {
			t.Fatal("the first Track synced through a held list")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the first Track outlived its budget")
	}
	if r.protectedOf(configMapGK) != p || r.claimsOf(p) != 1 {
		t.Fatal("the first Track's failure took the informer the second still claims")
	}
	close(release)
	select {
	case err := <-second:
		if err != nil {
			t.Errorf("the second Track = %v, want it synced on its own budget", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the second Track didn't return")
	}
}

// Two kinds share a protected informer a Track after Run creates, and its
// start fails: the Track that started it and the one waiting for it both
// get the start's error at once, not at the end of their budgets, and the
// informer goes once neither claims it.
func TestSharedInformerStartFailureReachesEveryTrack(t *testing.T) {
	r := newFakeRig(t)
	r.s.syncTimeout.Store(int64(10 * time.Minute))
	r.run(t)
	entered, release := r.gateList("configmaps")
	r.forbidList.Store("configmaps", true)
	cm := []ProtectedKind{{GVK: configMapGVK}}
	errs := make(chan error, 2)
	go func() {
		_, err := r.s.Track(context.Background(), r.options(gadgetGK, cm...))
		errs <- err
	}()
	select {
	case <-entered:
	case <-time.After(20 * time.Second):
		t.Fatal("the ConfigMap informer never listed")
	}
	p := r.protectedOf(configMapGK)
	go func() {
		_, err := r.s.Track(context.Background(), r.options(widgetGK, cm...))
		errs <- err
	}()
	eventually(t, "both Tracks to claim the ConfigMap informer", func() bool { return r.claimsOf(p) == 2 })
	close(release)
	for range 2 {
		select {
		case err := <-errs:
			if err == nil || !strings.Contains(err.Error(), "forbidden") {
				t.Errorf("Track = %v, want the informer's start error", err)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("a Track kept waiting after the shared informer's start failed")
		}
	}
	if r.protectedOf(configMapGK) != nil {
		t.Error("the informer whose start failed stayed after both Tracks gave it up")
	}
}

// A Track after Run joins the shared protected informers only once its
// holder informer has listed, so that an object leaving a shared cache
// enqueues its holders from a full index (the re-review of #109).
func TestTrackJoinsAfterItsHolderSync(t *testing.T) {
	r := newFakeRig(t)
	r.track(t, widgetGK)
	r.run(t)
	secrets := r.protectedOf(secretGroupKind)
	var joined atomic.Int32 // at the Gadget's first holder list: 1 not joined, 2 joined
	var once sync.Once
	r.md.PrependReactor("list", "gadgets", func(clienttesting.Action) (bool, runtime.Object, error) {
		once.Do(func() {
			if len(secrets.snapshot()) == 2 {
				joined.Store(2)
			} else {
				joined.Store(1)
			}
		})
		return false, nil, nil
	})
	r.track(t, gadgetGK)
	switch joined.Load() {
	case 0:
		t.Fatal("the Gadget's holders were never listed")
	case 2:
		t.Error("the Gadget's tracker joined the Secret informer before its holder informer listed")
	}
	if len(secrets.snapshot()) != 2 {
		t.Error("the Gadget's tracker didn't join the Secret informer")
	}
}

// The caller's ctx aborts a pending Track, whether it is working or waiting
// for another Track of its kind: it returns at once with an error that
// wraps ctx.Err(), and leaves nothing behind. A Track cancelled before Run
// isn't held against Run.
func TestTrackHonoursTheCallersContext(t *testing.T) {
	t.Run("after Run", func(t *testing.T) {
		r := newFakeRig(t)
		r.s.syncTimeout.Store(int64(10 * time.Minute))
		r.run(t)
		entered, release := r.gateList("gadgets")
		defer close(release)
		workCtx, cancelWork := context.WithCancel(context.Background())
		waitCtx, cancelWait := context.WithCancel(context.Background())
		working, waiting := make(chan error, 1), make(chan error, 1)
		go func() {
			_, err := r.s.Track(workCtx, r.options(gadgetGK))
			working <- err
		}()
		select {
		case <-entered:
		case <-time.After(20 * time.Second):
			t.Fatal("the Gadget's holders were never listed")
		}
		go func() {
			_, err := r.s.Track(waitCtx, r.options(gadgetGK))
			waiting <- err
		}()
		eventually(t, "the second Track to wait", func() bool { return r.s.trackWaiting.Load() == 1 })
		for _, c := range []struct {
			what   string
			cancel context.CancelFunc
			errs   chan error
		}{{"waiting", cancelWait, waiting}, {"working", cancelWork, working}} {
			c.cancel()
			select {
			case err := <-c.errs:
				if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "abandoned") {
					t.Errorf("the %s Track = %v, want it abandoned, wrapping context.Canceled", c.what, err)
				}
			case <-time.After(20 * time.Second):
				t.Fatalf("the %s Track ignored its caller's cancellation", c.what)
			}
		}
		if r.tracked(gadgetGK) {
			t.Error("a cancelled Track left its tracker")
		}
	})
	t.Run("before Run", func(t *testing.T) {
		r := newFakeRig(t)
		ctx, cancel := context.WithCancel(context.Background())
		// The caller gives up while the settings are being published, and
		// the read fails as a real client's would.
		r.dyn.PrependReactor("get", "customresourcedefinitions", func(clienttesting.Action) (bool, runtime.Object, error) {
			cancel()
			return true, nil, context.Canceled
		})
		if _, err := r.s.Track(ctx, r.options(gadgetGK)); !errors.Is(err, context.Canceled) {
			t.Fatalf("Track cancelled while publishing = %v, want context.Canceled", err)
		}
		if _, err := r.s.Track(ctx, r.options(gadgetGK)); !errors.Is(err, context.Canceled) {
			t.Fatalf("Track with an ended ctx = %v, want context.Canceled", err)
		}
		r.run(t) // fails the test if Run refuses to start
	})
}

// D3: a Track after Run that fails rolls back everything it created, on
// each way it can fail: the holder informer can't sync, a protected
// informer it created can't sync, and the RBAC check of the holders or of
// a protected kind fails. A later Track of the kind then works.
func TestFailedTrackLeavesNothing(t *testing.T) {
	cases := []struct {
		name      string
		protected []ProtectedKind
		fail      func(r *fakeRig)
		allow     func(r *fakeRig)
		want      string
	}{{
		name:  "the holder informer",
		fail:  func(r *fakeRig) { r.forbidList.Store("gadgets", true) },
		allow: func(r *fakeRig) { r.forbidList.Delete("gadgets") },
		want:  "forbidden",
	}, {
		name:      "a new protected informer",
		protected: []ProtectedKind{{GVK: secretGVK}, {GVK: configMapGVK}},
		fail:      func(r *fakeRig) { r.forbidList.Store("configmaps", true) },
		allow:     func(r *fakeRig) { r.forbidList.Delete("configmaps") },
		want:      "forbidden",
	}, {
		name:  "the holders' RBAC check",
		fail:  func(r *fakeRig) { r.denied.Store("watch gadgets", true) },
		allow: func(r *fakeRig) { r.denied.Delete("watch gadgets") },
		want:  "grant list and watch on gadgets",
	}, {
		name:      "the RBAC check",
		protected: []ProtectedKind{{GVK: secretGVK}, {GVK: configMapGVK}},
		fail:      func(r *fakeRig) { r.denied.Store("watch configmaps", true) },
		allow:     func(r *fakeRig) { r.denied.Delete("watch configmaps") },
		want:      "grant list and watch on configmaps",
	}}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newFakeRig(t)
			w := r.track(t, widgetGK)
			r.run(t)
			secrets := r.protectedOf(secretGroupKind)

			// Catch what the failing Track started, from inside the
			// informers it runs.
			var mu sync.Mutex
			var started []interface{ IsStopped() bool }
			catch := func(a clienttesting.Action) (bool, runtime.Object, error) {
				r.s.mu.Lock()
				defer r.s.mu.Unlock()
				mu.Lock()
				defer mu.Unlock()
				switch a.GetResource().Resource {
				case "gadgets":
					if g := r.s.tracking[gadgetGK]; g != nil && g.t != nil {
						started = append(started, g.t.holders)
					}
				case "configmaps":
					if p := r.s.protected[configMapGK]; p != nil {
						started = append(started, p.inf)
					}
				}
				return false, nil, nil
			}
			r.md.PrependReactor("list", "gadgets", catch)
			r.md.PrependReactor("list", "configmaps", catch)

			c.fail(r)
			_, err := r.s.Track(context.Background(), r.options(gadgetGK, c.protected...))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Track = %v, want an error containing %q", err, c.want)
			}
			if r.tracked(gadgetGK) || r.protectedOf(configMapGK) != nil {
				t.Errorf("after the failure: Gadget tracked %v, a ConfigMap informer %v", r.tracked(gadgetGK), r.protectedOf(configMapGK) != nil)
			}
			if got := secrets.snapshot(); !slices.Equal(got, []*Tracker{w}) {
				t.Errorf("the Secret informer's trackers are %v, want only the Widget's", got)
			}
			mu.Lock()
			informers := slices.Clone(started)
			mu.Unlock()
			eventually(t, "the failed Track's informers to stop", func() bool {
				for _, inf := range informers {
					if !inf.IsStopped() {
						return false
					}
				}
				return true
			})
			if n, set := r.metrics.heldGauge(kindLabel(gadgetGK)); set && n != 0 {
				t.Errorf("the failed kind's held gauge is %d", n)
			}

			// A held Secret added after the failure: the next Track must
			// find it in the shared cache.
			late := heldObject(secretGVK, "ns", "late", "u-late", gadgetGK, "ns")
			if err := r.md.Tracker().Create(schema.GroupVersionResource{Version: "v1", Resource: "secrets"}, late, "ns"); err != nil {
				t.Fatal(err)
			}
			eventually(t, "the Secret informer to cache the late Secret", func() bool {
				_, ok := secrets.get("ns", "late")
				return ok
			})

			c.allow(r)
			g, err := r.s.Track(context.Background(), r.options(gadgetGK, c.protected...))
			if err != nil {
				t.Fatalf("Track after the cause is gone: %v", err)
			}
			if !g.holders.HasSynced() {
				t.Error("the Track after the failure returned before its holder informer synced")
			}
			if got := r.queued(gadgetGK, "ns"); !slices.Contains(got, secretID("ns", "late")) {
				t.Errorf("the Gadget's batch covers %v, want the late Secret: its handler didn't recheck the shared cache", got)
			}
		})
	}
}

// D4, the restart scenario: a kind tracked after Run finds Secrets that
// already carry its finalizers, from before the restart, in the cache the
// Widget's informer filled. Its Track rechecks each of them before it
// returns, the deleting one's grace included, and counts them in its held
// gauge.
func TestTrackAfterRunRechecksTheSharedCache(t *testing.T) {
	r := newFakeRig(t,
		heldObject(secretGVK, "ns", "a", "u-a", gadgetGK, "ns"),
		func() runtime.Object {
			m := heldObject(secretGVK, "ns", "b", "u-b", gadgetGK, "ns", "other")
			now := metav1.NewTime(time.Now().Add(-time.Hour))
			m.DeletionTimestamp = &now
			return m
		}(),
	)
	r.track(t, widgetGK)
	// Hold the Gadget's replay until the test has looked at the queue,
	// for at most two seconds: a Track that waits for the replay sees each
	// object rechecked when it returns, and one that doesn't returns first.
	looked := make(chan struct{})
	var lookOnce sync.Once
	look := func() { lookOnce.Do(func() { close(looked) }) }
	r.s.hooks.protectedEvent = func(t *Tracker, _ *metav1.PartialObjectMetadata) {
		if t.gk == gadgetGK {
			select {
			case <-looked:
			case <-time.After(2 * time.Second):
			}
		}
	}
	r.run(t)
	time.AfterFunc(2*time.Second, look)
	r.track(t, gadgetGK)
	gotNS, gotOther := r.queued(gadgetGK, "ns"), r.queued(gadgetGK, "other")
	look()
	if want := []ID{secretID("ns", "a"), secretID("ns", "b")}; !slices.Equal(gotNS, want) {
		t.Errorf("when Track returned, the Gadget's batch in ns covered %v, want %v", gotNS, want)
	}
	if want := []ID{secretID("ns", "b")}; !slices.Equal(gotOther, want) {
		t.Errorf("when Track returned, the Gadget's batch in other covered %v, want %v", gotOther, want)
	}
	if n, _ := r.metrics.heldGauge(kindLabel(gadgetGK)); n != 3 {
		t.Errorf("the Gadget's held gauge is %d, want 3: a, and b for two namespaces", n)
	}
	r.drain()
	r.clk.Step(DefaultDeletionGrace - time.Second)
	if got := r.queued(gadgetGK, "ns"); got != nil {
		t.Errorf("before the grace ends, the batch covers %v", got)
	}
	r.clk.Step(time.Second)
	if want := []ID{secretID("ns", "b")}; !slices.Equal(r.queued(gadgetGK, "ns"), want) || !slices.Equal(r.queued(gadgetGK, "other"), want) {
		t.Errorf("at the grace's end the batches cover %v and %v, want b in both", r.queued(gadgetGK, "ns"), r.queued(gadgetGK, "other"))
	}
}

// The protected handler, one event at a time (G2, G4, G5, G6, N3, N5).
func TestProtectedHandler(t *testing.T) {
	r := newFakeRig(t)
	tr := r.track(t, widgetGK)
	p := r.protectedOf(secretGroupKind)
	h := &protectedHandler{s: r.s, p: p, t: tr}
	kind := kindLabel(widgetGK)
	gauge := func() int { n, _ := r.metrics.heldGauge(kind); return n }
	id := secretID("ns", "s")
	fNS, _ := FinalizerName(widgetGK, "ns")
	fOther, _ := FinalizerName(widgetGK, "other")

	// Holders in the holder cache: one names the Secret, one names another,
	// and one has a record that can't be parsed.
	holder := func(name, record string) {
		m := &metav1.PartialObjectMetadata{}
		m.Namespace, m.Name = "ns", name
		m.Annotations = map[string]string{AnnotationRefs: record}
		if err := tr.holders.GetIndexer().Add(m); err != nil {
			t.Fatal(err)
		}
	}
	rec := func(ids ...ID) string {
		v, err := EncodeRecord(ids)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	holder("names-it", rec(id))
	holder("names-another", rec(secretID("ns", "other")))
	holder("unparseable", "not a record")
	wantHolders := map[types.NamespacedName]int{{Namespace: "ns", Name: "names-it"}: 1, {Namespace: "ns", Name: "unparseable"}: 1}

	both := heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns", "other")
	t.Run("an add enqueues each held batch and counts the pairs", func(t *testing.T) {
		h.OnAdd(both, false)
		if !slices.Equal(r.queued(widgetGK, "ns"), []ID{id}) || !slices.Equal(r.queued(widgetGK, "other"), []ID{id}) {
			t.Errorf("batches %v, %v; want the Secret in both", r.queued(widgetGK, "ns"), r.queued(widgetGK, "other"))
		}
		if gauge() != 2 {
			t.Errorf("held gauge %d, want 2", gauge())
		}
		r.drain()
	})
	t.Run("an epoch change enqueues its batch only", func(t *testing.T) {
		n := both.DeepCopy()
		n.Annotations[fNS] = "e2"
		h.OnUpdate(both, n)
		if !slices.Equal(r.queued(widgetGK, "ns"), []ID{id}) || r.queued(widgetGK, "other") != nil {
			t.Errorf("batches %v, %v; want ns's only", r.queued(widgetGK, "ns"), r.queued(widgetGK, "other"))
		}
		if got := r.enq.take(); got != nil {
			t.Errorf("holders enqueued %v on an epoch change", got)
		}
		r.drain()
	})
	t.Run("losing a finalizer, keeping the label, enqueues the holders", func(t *testing.T) {
		tr.ver.wrote("u1", "other", r.clk.Now())
		tr.ver.wrote("u1", "ns", r.clk.Now())
		n := both.DeepCopy()
		n.Finalizers = []string{fNS}
		delete(n.Annotations, fOther)
		h.OnUpdate(both, n)
		if tr.ver.hasWritten("u1", "other") || !tr.ver.hasWritten("u1", "ns") {
			t.Errorf("written: other %v, ns %v; want other's dropped only", tr.ver.hasWritten("u1", "other"), tr.ver.hasWritten("u1", "ns"))
		}
		if !slices.Equal(r.queued(widgetGK, "other"), []ID{id}) {
			t.Errorf("other's batch %v, want the Secret", r.queued(widgetGK, "other"))
		}
		if got := r.enq.take(); !maps.Equal(got, wantHolders) {
			t.Errorf("holders enqueued %v, want %v", got, wantHolders)
		}
		if gauge() != 1 {
			t.Errorf("held gauge %d, want 1", gauge())
		}
		h.OnUpdate(n, both)
		r.drain()
		r.enq.take()
	})
	deleting := both.DeepCopy()
	now := metav1.NewTime(r.clk.Now())
	deleting.DeletionTimestamp = &now
	t.Run("a new deletionTimestamp enqueues now and at the grace's end", func(t *testing.T) {
		h.OnUpdate(both, deleting)
		if r.queued(widgetGK, "ns") == nil || r.queued(widgetGK, "other") == nil {
			t.Error("the deletion didn't enqueue its batches")
		}
		r.drain()
		r.clk.Step(DefaultDeletionGrace)
		if !slices.Equal(r.queued(widgetGK, "ns"), []ID{id}) || !slices.Equal(r.queued(widgetGK, "other"), []ID{id}) {
			t.Errorf("at the grace's end, batches %v, %v", r.queued(widgetGK, "ns"), r.queued(widgetGK, "other"))
		}
		r.drain()
	})
	t.Run("a relist that brings a deleting object under a new UID starts its grace", func(t *testing.T) {
		replaced := deleting.DeepCopy()
		replaced.UID = "u2"
		h.OnUpdate(deleting, replaced)
		r.drain()
		r.enq.take()
		if _, ok := r.s.seen.at["u1"]; ok {
			t.Error("the replaced UID's first sight is kept")
		}
		r.clk.Step(DefaultDeletionGrace - time.Second)
		if r.queued(widgetGK, "ns") != nil {
			t.Error("enqueued before the new object's grace ends")
		}
		r.clk.Step(time.Second)
		if !slices.Equal(r.queued(widgetGK, "ns"), []ID{id}) {
			t.Errorf("at the new object's grace end, the batch is %v", r.queued(widgetGK, "ns"))
		}
		r.drain()
		deleting = replaced
	})
	t.Run("leaving the cache enqueues the holders and forgets the first sight", func(t *testing.T) {
		tr.ver.wrote("u2", "ns", r.clk.Now())
		if _, ok := r.s.seen.at["u2"]; !ok {
			t.Fatal("the deleting object's first sight wasn't recorded")
		}
		h.OnDelete(deleting)
		if tr.ver.hasWritten("u2", "ns") {
			t.Error("the written entry survived the deletion")
		}
		if _, ok := r.s.seen.at["u2"]; ok {
			t.Error("the first sight survived the deletion")
		}
		if got := r.enq.take(); !maps.Equal(got, wantHolders) {
			t.Errorf("holders enqueued %v, want %v", got, wantHolders)
		}
		if r.queued(widgetGK, "ns") == nil || gauge() != 0 {
			t.Errorf("batch %v, held gauge %d; want the batch, and 0", r.queued(widgetGK, "ns"), gauge())
		}
	})
}

// G6: the queue shuts down when RunInformers' context ends, which stops
// the grace timers.
func TestRunInformersShutsTheQueueDown(t *testing.T) {
	r := newFakeRig(t)
	stop := r.run(t)
	r.s.queue.AddAfter(batchKey{widgetGK, "ns"}, time.Minute)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if r.clk.HasWaiters() {
		t.Error("a grace timer outlived RunInformers")
	}
	r.s.queue.Add(batchKey{widgetGK, "ns"})
	if r.s.queue.Len() != 0 {
		t.Error("the queue took a batch after RunInformers ended")
	}
}

// N2: RunInformers fails, before it starts anything, without the access
// it needs: without a dynamic client it can't check, and each informer
// needs list and watch on what it caches.
func TestRunInformersChecksAccess(t *testing.T) {
	t.Run("no dynamic client", func(t *testing.T) {
		r := newFakeRig(t)
		r.s.c.Dynamic = nil
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := r.s.RunInformers(ctx); err == nil || !strings.Contains(err.Error(), "Clients.Dynamic is required") {
			t.Errorf("RunInformers = %v, want it refused", err)
		}
	})
	for _, c := range []struct{ deny, want string }{
		{"list namespaces", "grant get, list and watch on namespaces"},
		{"watch widgets", "grant list and watch on widgets." + widgetGK.Group},
		{"list secrets", "grant list and watch on secrets"},
	} {
		t.Run(c.deny, func(t *testing.T) {
			r := newFakeRig(t)
			r.track(t, widgetGK)
			r.denied.Store(c.deny, true)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := r.s.RunInformers(ctx); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("RunInformers = %v, want %q", err, c.want)
			}
			for _, a := range r.md.Actions() {
				if a.GetVerb() == "list" || a.GetVerb() == "watch" {
					t.Errorf("an informer started before the check failed: %s %s", a.GetVerb(), a.GetResource().Resource)
					break
				}
			}
		})
	}
}

// G3: publishing the settings on the CRD.
func TestPublishConfig(t *testing.T) {
	type rig struct {
		*fakeRig
		o       Options
		gets    int
		patches [][]byte
	}
	newRig := func(t *testing.T) *rig {
		r := &rig{fakeRig: newFakeRig(t)}
		o, err := r.s.validate(r.options(widgetGK))
		if err != nil {
			t.Fatal(err)
		}
		r.o = o
		r.dyn.PrependReactor("get", "customresourcedefinitions", func(clienttesting.Action) (bool, runtime.Object, error) {
			r.gets++
			return false, nil, nil
		})
		return r
	}
	conflict := apierrors.NewConflict(crdGVR.GroupResource(), "widgets", errors.New("test: conflict"))
	patching := func(r *rig, f func(n int) error) {
		r.dyn.PrependReactor("patch", "customresourcedefinitions", func(a clienttesting.Action) (bool, runtime.Object, error) {
			r.patches = append(r.patches, a.(clienttesting.PatchAction).GetPatch())
			if err := f(len(r.patches)); err != nil {
				return true, nil, err
			}
			return false, nil, nil
		})
	}
	published := func(r *rig) string {
		crd, err := r.dyn.Resource(crdGVR).Get(context.Background(), crdOf(widgetGK).GetName(), metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return crd.GetAnnotations()[AnnotationConfig]
	}
	// publishElsewhere writes the annotation as another process would.
	publishElsewhere := func(r *rig, value string) {
		u := crdOf(widgetGK)
		u.SetAnnotations(map[string]string{AnnotationConfig: value})
		if err := r.dyn.Tracker().Update(crdGVR, u, ""); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("the patch is guarded by the resourceVersion read", func(t *testing.T) {
		r := newRig(t)
		patching(r, func(int) error { return nil })
		if err := r.s.publishConfig(context.Background(), r.o); err != nil {
			t.Fatal(err)
		}
		var body struct {
			Metadata struct {
				ResourceVersion string            `json:"resourceVersion"`
				Annotations     map[string]string `json:"annotations"`
			} `json:"metadata"`
		}
		if len(r.patches) != 1 || json.Unmarshal(r.patches[0], &body) != nil || body.Metadata.ResourceVersion != "7" {
			t.Fatalf("patches %q, want one carrying resourceVersion 7", r.patches)
		}
		if got := published(r); got != `{"deletionGrace":"60s","disownPasses":5,"disownInterval":"60s","timeoutBound":"90s"}` {
			t.Errorf("published %s", got)
		}
	})
	t.Run("only a conflict is retried", func(t *testing.T) {
		r := newRig(t)
		patching(r, func(int) error {
			return apierrors.NewForbidden(crdGVR.GroupResource(), "widgets", errors.New("test: no"))
		})
		if err := r.s.publishConfig(context.Background(), r.o); err == nil || !strings.Contains(err.Error(), "publishing the settings") {
			t.Errorf("publishConfig = %v, want the patch's error", err)
		}
		if len(r.patches) != 1 {
			t.Errorf("%d patches, want 1: a forbidden patch isn't retried", len(r.patches))
		}
	})
	t.Run("a conflict reads again, and finds another process's settings", func(t *testing.T) {
		r := newRig(t)
		patching(r, func(n int) error {
			if n == 2 {
				publishElsewhere(r, `{"deletionGrace":"1m0s","disownPasses":5,"disownInterval":"60s","timeoutBound":"90s"}`)
				return conflict
			}
			return conflict
		})
		if err := r.s.publishConfig(context.Background(), r.o); err != nil {
			t.Fatalf("publishConfig = %v, want the agreeing settings accepted", err)
		}
		if len(r.patches) != 2 || r.gets != 3 {
			t.Errorf("%d patches and %d reads, want 2 and 3", len(r.patches), r.gets)
		}
	})
	t.Run("after the last conflict, a read refuses disagreeing settings", func(t *testing.T) {
		r := newRig(t)
		patching(r, func(n int) error {
			if n == maxCRDWriteAttempts {
				publishElsewhere(r, `{"deletionGrace":"30s","disownPasses":5,"disownInterval":"60s","timeoutBound":"90s"}`)
			}
			return conflict
		})
		if err := r.s.publishConfig(context.Background(), r.o); err == nil || !strings.Contains(err.Error(), "deletionGrace") {
			t.Errorf("publishConfig = %v, want the disagreement", err)
		}
		if len(r.patches) != maxCRDWriteAttempts || r.gets != maxCRDWriteAttempts+1 {
			t.Errorf("%d patches and %d reads, want %d and %d", len(r.patches), r.gets, maxCRDWriteAttempts, maxCRDWriteAttempts+1)
		}
		if n := r.metrics.count("crd_write_starved " + kindLabel(widgetGK)); n != 0 {
			t.Errorf("counted %d starved writes, want none: the read decided", n)
		}
	})
	t.Run("conflicts every time wait for the next start, counted", func(t *testing.T) {
		r := newRig(t)
		patching(r, func(int) error { return conflict })
		if err := r.s.publishConfig(context.Background(), r.o); err != nil {
			t.Errorf("publishConfig = %v, want nil", err)
		}
		if len(r.patches) != maxCRDWriteAttempts || r.gets != maxCRDWriteAttempts+1 {
			t.Errorf("%d patches and %d reads, want %d and %d", len(r.patches), r.gets, maxCRDWriteAttempts, maxCRDWriteAttempts+1)
		}
		if n := r.metrics.count("crd_write_starved " + kindLabel(widgetGK)); n != 1 {
			t.Errorf("counted %d starved writes, want 1", n)
		}
		if got := published(r); got != "" {
			t.Errorf("published %q after only conflicts", got)
		}
	})
}

// N1, the user's decision of 2026-11-18: Options are equal when every field
// that isn't a function or a sink is. A field added to Options stops this
// unkeyed literal compiling, so that it is put in compared or left out of it
// here, as a function or a sink: Refs, Enqueue, Logger and Metrics.
var _ = Options{
	schema.GroupVersionKind{}, []string(nil), RefsFunc(nil), []ProtectedKind(nil), 0,
	func(types.NamespacedName) {}, 0, 0, 0, 0, 0, logging.Logger(nil), MetricsRecorder(nil),
}

func TestEqualOptionsComparesEveryOtherField(t *testing.T) {
	compared := []string{"Holder", "OwnerFinalizers", "Protected", "MaxDepth",
		"DeletionGrace", "ProbeInterval", "DisownInterval", "DisownPasses", "TimeoutBound"}
	r := newFakeRig(t)
	base, err := r.s.validate(r.options(widgetGK, ProtectedKind{GVK: secretGVK}, ProtectedKind{GVK: configMapGVK}))
	if err != nil {
		t.Fatal(err)
	}
	base.OwnerFinalizers = []string{"a", "b"}
	same := base
	same.OwnerFinalizers = []string{"b", "a"}
	same.Protected = []ProtectedKind{{GVK: configMapGVK}, {GVK: secretGVK}}
	same.Refs = func(metav1.Object) ([]Ref, error) { return nil, errors.New("another") }
	same.Enqueue = func(types.NamespacedName) {}
	same.Logger, same.Metrics = nil, NopMetrics{}
	if !equalOptions(base, same) {
		t.Error("Options differing in order and in functions and sinks only aren't equal")
	}
	changes := map[string]func(o *Options){
		"Holder":          func(o *Options) { o.Holder.Version = "v2" },
		"OwnerFinalizers": func(o *Options) { o.OwnerFinalizers = []string{"a"} },
		"Protected":       func(o *Options) { o.Protected = o.Protected[:1] },
		"MaxDepth":        func(o *Options) { o.MaxDepth++ },
		"DeletionGrace":   func(o *Options) { o.DeletionGrace++ },
		"ProbeInterval":   func(o *Options) { o.ProbeInterval++ },
		"DisownInterval":  func(o *Options) { o.DisownInterval++ },
		"DisownPasses":    func(o *Options) { o.DisownPasses++ },
		"TimeoutBound":    func(o *Options) { o.TimeoutBound++ },
	}
	for _, f := range compared {
		o := base
		o.OwnerFinalizers = slices.Clone(base.OwnerFinalizers)
		o.Protected = slices.Clone(base.Protected)
		changes[f](&o)
		if equalOptions(base, o) {
			t.Errorf("Options differing in %s are equal", f)
		}
	}
}

// N1: a Track with equal Options rebinds Enqueue, Logger and Metrics, while
// events flow; once it returns, no event reaches the old Enqueue, and the
// new Metrics has the held gauge. Run under -race.
func TestTrackRebindsTheSinks(t *testing.T) {
	r := newFakeRig(t)
	tr := r.track(t, widgetGK)
	p := r.protectedOf(secretGroupKind)
	h := &protectedHandler{s: r.s, p: p, t: tr}
	holderNaming := &metav1.PartialObjectMetadata{}
	holderNaming.Namespace, holderNaming.Name = "ns", "h"
	rec, err := EncodeRecord([]ID{secretID("ns", "s")})
	if err != nil {
		t.Fatal(err)
	}
	holderNaming.Annotations = map[string]string{AnnotationRefs: rec}
	if err := tr.holders.GetIndexer().Add(holderNaming); err != nil {
		t.Fatal(err)
	}
	held := heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns")
	h.OnAdd(held, false)

	// Events flow: the object leaves and comes back, enqueueing its
	// holder and moving the gauge, until the test ends.
	stop := make(chan struct{})
	flowing := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var once sync.Once
		for {
			select {
			case <-stop:
				return
			default:
			}
			h.OnDelete(held)
			h.OnAdd(held, false)
			once.Do(func() { close(flowing) })
		}
	}()
	<-flowing

	var violations atomic.Int32
	for i := range 20 {
		var retired atomic.Bool
		var calls atomic.Int32
		o := r.options(widgetGK)
		m := &countingMetrics{}
		o.Metrics = m
		o.Enqueue = func(types.NamespacedName) {
			calls.Add(1)
			if retired.Load() {
				violations.Add(1)
			}
		}
		got, err := r.s.Track(context.Background(), o)
		if err != nil || got != tr {
			t.Fatalf("rebind %d: Track = %p, %v; want the tracker %p", i, got, err, tr)
		}
		if _, set := m.heldGauge(kindLabel(widgetGK)); !set {
			t.Errorf("rebind %d: the new Metrics didn't get the held gauge", i)
		}
		eventually(t, "an event to reach the new Enqueue", func() bool { return calls.Load() > 0 })
		// The next Track retires this Enqueue: from its return on, a call
		// to it is a violation.
		o2 := r.options(widgetGK)
		o2.Enqueue = func(types.NamespacedName) {}
		if _, err := r.s.Track(context.Background(), o2); err != nil {
			t.Fatal(err)
		}
		retired.Store(true)
	}
	close(stop)
	wg.Wait()
	if n := violations.Load(); n != 0 {
		t.Errorf("%d events reached an Enqueue after the Track that replaced it returned", n)
	}
}

// N1: the rebind waits for a call to the old Enqueue that is running, so
// that once Track returns none is. The old Enqueue holds its call for two
// seconds: a rebind that waits returns after that call ends, and one that
// doesn't returns first.
func TestRebindWaitsForCallsInFlight(t *testing.T) {
	r := newFakeRig(t)
	entered := make(chan struct{})
	var mu sync.Mutex
	var order []string
	note := func(s string) { mu.Lock(); order = append(order, s); mu.Unlock() }
	var once sync.Once
	o := r.options(widgetGK)
	o.Enqueue = func(types.NamespacedName) {
		once.Do(func() {
			close(entered)
			time.Sleep(2 * time.Second)
			note("the old call ended")
		})
	}
	tr, err := r.s.Track(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	go tr.enqueue(types.NamespacedName{Namespace: "ns", Name: "h"})
	<-entered
	o2 := r.options(widgetGK)
	o2.Enqueue = func(types.NamespacedName) {}
	if _, err := r.s.Track(context.Background(), o2); err != nil {
		t.Fatal(err)
	}
	note("Track returned")
	eventually(t, "the old call to end", func() bool { mu.Lock(); defer mu.Unlock(); return len(order) == 2 })
	if want := []string{"the old call ended", "Track returned"}; !slices.Equal(order, want) {
		t.Errorf("order %q, want %q", order, want)
	}
}
