package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/recorder"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/logging"
	pmeta "github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
	"github.com/krateo-platformops/provider-runtime/pkg/resource/fake"
	"github.com/krateo-platformops/provider-runtime/pkg/telemetry"
	"github.com/krateo-platformops/provider-runtime/pkg/tracing"
)

// The adapter's unit tests (plan, PR 7): a Shared on fake clients, whose
// metadata client reads and writes the reconciler's own fake API server, so
// that the record write (A1) and the reconciler's writes share one store and
// one resourceVersion. The envtests with a manager and the real API server
// are in pkg/credentialuse/e2e.

var cuWidgetGVR = schema.GroupVersionResource{Group: widgetGVK.Group, Version: widgetGVK.Version, Resource: "widgets"}

// cuWorld is a fake API server and a Shared whose metadata client is that
// server.
type cuWorld struct {
	t       *testing.T
	srv     *traceServer
	dyn     *dynamicfake.FakeDynamicClient
	s       *credentialuse.Shared
	metrics *cuMetrics
	kinds   map[schema.GroupVersionResource]schema.GroupVersionKind

	mu sync.Mutex
	// mdPatch, if set, runs before each metadata patch; an error fails it.
	mdPatch func(gvr schema.GroupVersionResource, ns, name string) error
	// afterMDPatch, if set, runs after each metadata patch that succeeded,
	// as a mutating webhook would.
	afterMDPatch func(gvr schema.GroupVersionResource, ns, name string)
	// mdGet, if set, runs before each metadata get; an error fails it.
	mdGet func(gvr schema.GroupVersionResource, ns, name string) error
}

func cuScheme() *runtime.Scheme {
	s := widgetScheme()
	if err := corev1.AddToScheme(s); err != nil {
		panic(err)
	}
	return s
}

func newCUWorld(t *testing.T, objs ...client.Object) *cuWorld {
	t.Helper()
	objs = append(objs, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "demo"}})
	w := &cuWorld{t: t, srv: newTraceServerWithScheme(t, cuScheme(), objs...), metrics: &cuMetrics{},
		kinds: map[schema.GroupVersionResource]schema.GroupVersionKind{
			cuWidgetGVR:                             widgetGVK,
			{Version: "v1", Resource: "secrets"}:    {Version: "v1", Kind: "Secret"},
			{Version: "v1", Resource: "namespaces"}: {Version: "v1", Kind: "Namespace"},
		}}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Version: "v1"}, widgetGVK.GroupVersion()})
	mapper.Add(widgetGVK, meta.RESTScopeNamespace)
	mapper.Add(schema.GroupVersionKind{Version: "v1", Kind: "Secret"}, meta.RESTScopeNamespace)
	mapper.Add(schema.GroupVersionKind{Version: "v1", Kind: "Namespace"}, meta.RESTScopeRoot)
	mapper.Add(schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}, meta.RESTScopeNamespace)
	crd := &unstructured.Unstructured{}
	crd.SetAPIVersion("apiextensions.k8s.io/v1")
	crd.SetKind("CustomResourceDefinition")
	crd.SetName("widgets." + widgetGVK.Group)
	crd.SetResourceVersion("1")
	// A ConfigMap a Guaranteed ref names, read live through the dynamic
	// client, whose "secret" key names a Secret (§9.3).
	config := &unstructured.Unstructured{Object: map[string]any{"data": map[string]any{"secret": "behind"}}}
	config.SetAPIVersion("v1")
	config.SetKind("ConfigMap")
	config.SetNamespace("demo")
	config.SetName("config")
	w.dyn = dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}: "CustomResourceDefinitionList",
			{Version: "v1", Resource: "configmaps"}:                                               "ConfigMapList",
		}, crd, config)
	dyn := w.dyn
	w.s = credentialuse.NewShared(credentialuse.Clients{Metadata: crMeta{w}, Dynamic: dyn, Mapper: mapper}, credentialuse.SharedOptions{})
	return w
}

// crMeta is a metadata client over the world's fake API server.
type crMeta struct{ w *cuWorld }

func (m crMeta) Resource(gvr schema.GroupVersionResource) metadata.Getter {
	return crMetaResource{w: m.w, gvr: gvr}
}

type crMetaResource struct {
	w   *cuWorld
	gvr schema.GroupVersionResource
	ns  string
}

func (r crMetaResource) Namespace(ns string) metadata.ResourceInterface { r.ns = ns; return r }

func (r crMetaResource) obj(name string) *metav1.PartialObjectMetadata {
	o := &metav1.PartialObjectMetadata{}
	o.SetGroupVersionKind(r.w.kinds[r.gvr])
	o.Name, o.Namespace = name, r.ns
	return o
}

func (r crMetaResource) Get(ctx context.Context, name string, _ metav1.GetOptions, _ ...string) (*metav1.PartialObjectMetadata, error) {
	if _, ok := r.w.kinds[r.gvr]; !ok {
		// The holder's CRD, for the Disown switch: there is none here.
		return nil, kerrors.NewNotFound(r.gvr.GroupResource(), name)
	}
	r.w.mu.Lock()
	get := r.w.mdGet
	r.w.mu.Unlock()
	if get != nil {
		if err := get(r.gvr, r.ns, name); err != nil {
			return nil, err
		}
	}
	o := r.obj(name)
	if err := r.w.srv.raw.Get(ctx, client.ObjectKey{Namespace: r.ns, Name: name}, o); err != nil {
		return nil, err
	}
	return o, nil
}

func (r crMetaResource) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, _ metav1.PatchOptions, _ ...string) (*metav1.PartialObjectMetadata, error) {
	r.w.mu.Lock()
	before, after := r.w.mdPatch, r.w.afterMDPatch
	r.w.mu.Unlock()
	if before != nil {
		if err := before(r.gvr, r.ns, name); err != nil {
			return nil, err
		}
	}
	o := r.obj(name)
	if err := r.w.srv.raw.Patch(ctx, o, client.RawPatch(pt, data)); err != nil {
		return nil, err
	}
	r.w.srv.ops.add("md-patch " + r.gvr.Resource)
	if after != nil {
		after(r.gvr, r.ns, name)
		if err := r.w.srv.raw.Get(ctx, client.ObjectKey{Namespace: r.ns, Name: name}, o); err != nil {
			return nil, err
		}
	}
	return o, nil
}

func (r crMetaResource) List(ctx context.Context, _ metav1.ListOptions) (*metav1.PartialObjectMetadataList, error) {
	l := &metav1.PartialObjectMetadataList{}
	gvk := r.w.kinds[r.gvr]
	l.SetGroupVersionKind(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
	if err := r.w.srv.raw.List(ctx, l, client.InNamespace(r.ns)); err != nil {
		return nil, err
	}
	return l, nil
}

func (r crMetaResource) Watch(context.Context, metav1.ListOptions) (watch.Interface, error) {
	return nil, errors.New("crMeta: no watch")
}

func (r crMetaResource) Delete(context.Context, string, metav1.DeleteOptions, ...string) error {
	return errors.New("crMeta: no delete")
}

func (r crMetaResource) DeleteCollection(context.Context, metav1.DeleteOptions, metav1.ListOptions) error {
	return errors.New("crMeta: no delete")
}

// cuMetrics counts the core's metrics the tests read.
type cuMetrics struct {
	credentialuse.NopMetrics
	unrecorded, degraded atomic.Int64
}

func (m *cuMetrics) UnrecordedReads(string)  { m.unrecorded.Add(1) }
func (m *cuMetrics) Degraded(string, string) { m.degraded.Add(1) }

// cuManager is a test manager with a config, which records the runnables
// added to it instead of running them.
type cuManager struct {
	fake.Manager
	mu    sync.Mutex
	added []manager.Runnable
}

func newCUManager(w *cuWorld) *cuManager {
	return &cuManager{Manager: fake.Manager{Client: w.srv.c, Scheme: w.srv.scheme, Config: &rest.Config{Host: "https://127.0.0.1:1"}}}
}

// GetEventRecorder is the default Shared's recorder: none.
func (m *cuManager) GetEventRecorder(string) recorder.EventRecorder { return nil }

func (m *cuManager) Add(r manager.Runnable) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.added = append(m.added, r)
	return nil
}

func (m *cuManager) runnables() []manager.Runnable {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.added)
}

// refsConnecter is a widgetProvider that names refs.
type refsConnecter struct {
	*widgetProvider
	refs func(mg resource.Managed) ([]credentialuse.Ref, error)
}

func (c refsConnecter) CredentialRefs(mg resource.Managed) ([]credentialuse.Ref, error) {
	return c.refs(mg)
}

// secretRef names Secret demo/name.
func secretRef(name string) func(resource.Managed) ([]credentialuse.Ref, error) {
	return func(resource.Managed) ([]credentialuse.Ref, error) {
		return []credentialuse.Ref{credentialuse.SecretRef("demo", name)}, nil
	}
}

// cuReconciler builds a Reconciler over w's server that tracks Widgets in
// w's Shared, with the APIFinalizer, and starts its source, as the
// controller would.
func (w *cuWorld) reconciler(m manager.Manager, p *widgetProvider, refs func(resource.Managed) ([]credentialuse.Ref, error), o ...ReconcilerOption) *Reconciler {
	w.t.Helper()
	if m == nil {
		m = &fake.Manager{Client: w.srv.c, Scheme: w.srv.scheme}
	}
	r := NewReconciler(m, resource.ManagedKind(widgetGVK), append([]ReconcilerOption{
		WithExternalConnecter(refsConnecter{widgetProvider: p, refs: refs}),
		WithFinalizer(resource.NewAPIFinalizer(w.srv.c, FinalizerName)),
		WithCredentialUseShared(w.s),
		WithMetrics(nil),
	}, o...)...)
	return r
}

func startSource(t *testing.T, r *Reconciler) workqueue.TypedRateLimitingInterface[reconcile.Request] {
	t.Helper()
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	t.Cleanup(q.ShutDown)
	if err := r.CredentialUseSource().Start(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	return q
}

// §11.10: a connecter that doesn't implement CredentialRefsProvider changes
// nothing: the same client calls as before credential-use, no Shared, no
// runnable, and a no-op source and predicate.
func TestCredentialUseNoChangeForNonImplementers(t *testing.T) {
	cases := map[string]struct {
		w      *Widget
		exists bool
		want   []string
	}{
		// A new widget: Observe finds nothing, the finalizer, create-pending,
		// Create, the critical annotations and the status.
		"create": {w: newWidget("w", nil), exists: false,
			want: []string{"connect", "observe", "update", "update", "create", "update", "status"}},
		// A deleting widget: Observe finds it, Delete, the status.
		"delete": {w: func() *Widget {
			w := newWidget("w", nil)
			w.Finalizers = []string{FinalizerName}
			now := metav1.Now()
			w.DeletionTimestamp = &now
			return w
		}(), exists: true, want: []string{"connect", "observe", "act-delete", "status"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			srv := newTraceServerWithScheme(t, cuScheme(), tc.w)
			m := &cuManager{Manager: fake.Manager{Client: srv.c, Scheme: srv.scheme, Config: &rest.Config{Host: "https://127.0.0.1:1"}}}
			p := &widgetProvider{ops: srv.ops, exists: func(*Widget) bool { return tc.exists }}
			r := NewReconciler(m, resource.ManagedKind(widgetGVK), WithExternalConnecter(p),
				WithFinalizer(resource.NewAPIFinalizer(srv.c, FinalizerName)))
			if _, err := r.Reconcile(context.Background(), reqFor("w")); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if got := srv.ops.list(); !slices.Equal(got, tc.want) {
				t.Errorf("client and provider calls = %v, want today's %v", got, tc.want)
			}
			if n := len(m.runnables()); n != 0 {
				t.Errorf("%d runnables added, want none", n)
			}
			credentialUseRegistry.mu.Lock()
			_, built := credentialUseRegistry.defaults[m]
			credentialUseRegistry.mu.Unlock()
			if built || r.cu.enabled || r.cu.tracker != nil {
				t.Errorf("a Shared was built (%v) or credential-use is on (%v)", built, r.cu.enabled)
			}
			if _, ok := r.CredentialUseSource().(nopSource); !ok {
				t.Errorf("CredentialUseSource = %T, want the no-op source", r.CredentialUseSource())
			}
			if !r.IgnoreCredentialUseRecord().Update(event.UpdateEvent{ObjectOld: tc.w, ObjectNew: tc.w}) {
				t.Error("the predicate dropped an event without credential-use")
			}
			if w := srv.widget("w"); w.Annotations[credentialuse.AnnotationRefs] != "" {
				t.Errorf("a record was written: %v", w.Annotations)
			}
		})
	}
}

// §7.5: with a test manager's nil config, credential-use is off: no Shared,
// the source is a no-op, and passes acquire nothing.
func TestCredentialUseOffWithANilConfig(t *testing.T) {
	srv := newTraceServerWithScheme(t, cuScheme(), newWidget("w", nil))
	p := &widgetProvider{ops: srv.ops}
	r := NewReconciler(&fake.Manager{Client: srv.c, Scheme: srv.scheme}, resource.ManagedKind(widgetGVK),
		WithExternalConnecter(refsConnecter{widgetProvider: p, refs: secretRef("creds")}),
		WithFinalizer(resource.FinalizerFns{
			AddFinalizerFn:    func(context.Context, resource.Object) error { return nil },
			RemoveFinalizerFn: func(context.Context, resource.Object) error { return nil },
		}))
	if r.cu.enabled || r.cu.tracker != nil {
		t.Fatal("credential-use is on with a nil config")
	}
	if _, ok := r.CredentialUseSource().(nopSource); !ok {
		t.Errorf("CredentialUseSource = %T, want the no-op source", r.CredentialUseSource())
	}
	mustReconcile(t, r, "w")
	if slices.Contains(srv.ops.list(), "md-patch widgets") || srv.widget("w").Annotations[credentialuse.AnnotationRefs] != "" {
		t.Errorf("a pass acquired with credential-use off: %v", srv.ops.list())
	}
}

// §7.5: a custom WithFinalizer that doesn't name its finalizer, on a
// connecter that implements CredentialRefsProvider, is a setup error: the
// informer runnable's Start returns it, so the manager refuses to start, and
// a pass returns it before the Get.
func TestCredentialUseFinalizerWithoutNameFailsStart(t *testing.T) {
	w := newCUWorld(t, newWidget("w", nil))
	m := newCUManager(w)
	r := w.reconciler(m, &widgetProvider{ops: w.srv.ops}, secretRef("creds"), WithFinalizer(resource.FinalizerFns{
		AddFinalizerFn:    func(context.Context, resource.Object) error { return nil },
		RemoveFinalizerFn: func(context.Context, resource.Object) error { return nil },
	}))
	rs := m.runnables()
	if len(rs) != 2 {
		t.Fatalf("%d runnables added, want 2: the informers and the write loops", len(rs))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, rn := range rs {
		if err := rn.Start(ctx); err == nil || !strings.Contains(err.Error(), "NamedFinalizer") {
			t.Errorf("%T.Start = %v, want the NamedFinalizer setup error", rn, err)
		}
	}
	w.srv.faults.get = func(client.ObjectKey, client.Object) (bool, error) {
		t.Error("a pass read the object after a setup error")
		return false, nil
	}
	if _, err := r.Reconcile(context.Background(), reqFor("w")); err == nil || !strings.Contains(err.Error(), "NamedFinalizer") {
		t.Errorf("Reconcile = %v, want the setup error", err)
	}
}

// §8 step 1 and M4: a pass of a kind whose CredentialUseSource was never
// started does nothing, not even the Get, and fails the manager.
func TestCredentialUseSourceNotStartedFailsTheFirstReconcile(t *testing.T) {
	w := newCUWorld(t, newWidget("w", nil))
	m := newCUManager(w)
	r := w.reconciler(m, &widgetProvider{ops: w.srv.ops}, secretRef("creds"))
	if r.cu.tracker == nil {
		t.Fatalf("not tracked: %v", r.cu.setupErr)
	}
	read := false
	w.srv.faults.get = func(client.ObjectKey, client.Object) (bool, error) {
		read = true
		return false, nil
	}
	want := "credential-use source for " + r.kind + " was not started: add .WatchesRawSource(r.CredentialUseSource()) to the controller"
	if _, err := r.Reconcile(context.Background(), reqFor("w")); err == nil || err.Error() != want {
		t.Fatalf("Reconcile = %v, want %q", err, want)
	}
	if read || len(w.srv.ops.list()) != 0 {
		t.Errorf("the pass read (%v) or wrote (%v)", read, w.srv.ops.list())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, rn := range m.runnables() {
		if _, ok := rn.(informerRunnable); !ok {
			continue
		}
		if err := rn.Start(ctx); err == nil || err.Error() != want {
			t.Errorf("the informer runnable's Start = %v, want %q", err, want)
		}
	}
}

// M4's timer: with no Reconcile, the write-loop runnable fails the manager
// when a source is still unstarted sourceStartTimeout after election, and
// doesn't when every source started.
func TestCredentialUseSourceTimer(t *testing.T) {
	old := sourceStartTimeout
	sourceStartTimeout = 200 * time.Millisecond
	t.Cleanup(func() { sourceStartTimeout = old })
	loops := func(ctx context.Context) error { <-ctx.Done(); return nil }

	t.Run("unstarted", func(t *testing.T) {
		wr := newSharedWiring(nil)
		wr.register(&credentialUseSource{kind: "Widget.k", registered: time.Now()})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		start := time.Now()
		err := writeLoopsRunnable{wr}.run(ctx, loops)
		if err == nil || !strings.Contains(err.Error(), "credential-use source for Widget.k was not started") {
			t.Fatalf("run = %v, want the unstarted source's error", err)
		}
		if d := time.Since(start); d < sourceStartTimeout {
			t.Errorf("failed after %v, before the timeout %v", d, sourceStartTimeout)
		}
		select {
		case <-wr.failed:
		default:
			t.Error("the wiring wasn't failed, so the informer runnable wouldn't return")
		}
	})
	t.Run("started", func(t *testing.T) {
		wr := newSharedWiring(nil)
		src := &credentialUseSource{kind: "Widget.k", registered: time.Now()}
		src.started.Store(true)
		wr.register(src)
		ctx, cancel := context.WithTimeout(context.Background(), 4*sourceStartTimeout)
		defer cancel()
		if err := (writeLoopsRunnable{wr}).run(ctx, loops); err != nil {
			t.Errorf("run = %v, want nil once ctx ends", err)
		}
	})
}

// §8 step 5: the record write (A1) runs on a pass before Connect, and a
// conflict on it requeues through the rate limiter, as every conflict on the
// object does, before Connect.
func TestCredentialUseRecordConflictRequeues(t *testing.T) {
	w := newCUWorld(t, newWidget("w", nil))
	r := w.reconciler(nil, &widgetProvider{ops: w.srv.ops}, secretRef("creds"))
	startSource(t, r)
	w.mdPatch = func(gvr schema.GroupVersionResource, _, _ string) error {
		if gvr == cuWidgetGVR {
			return kerrors.NewConflict(gvr.GroupResource(), "w", errors.New("test"))
		}
		return nil
	}
	res, err := r.Reconcile(context.Background(), reqFor("w"))
	if err != nil || !isRateLimitedRequeue(res) {
		t.Fatalf("Reconcile = %+v, %v; want a rate-limited requeue", res, err)
	}
	if slices.Contains(w.srv.ops.list(), "connect") {
		t.Errorf("Connect ran after a conflicting record write: %v", w.srv.ops.list())
	}
}

// L2 of #52's review: after an A1 that a mutating webhook changed (here, its
// labels), the pass reads the object again, live, before its next write, and
// that write carries the webhook's change.
func TestCredentialUseRereadsAfterAWebhookChangedTheRecordWrite(t *testing.T) {
	w := newCUWorld(t, newWidget("w", nil))
	var live atomic.Int64
	apiReader := readerFn(func(ctx context.Context, key client.ObjectKey, obj client.Object) error {
		live.Add(1)
		return w.srv.raw.Get(ctx, key, obj)
	})
	r := w.reconciler(nil, &widgetProvider{ops: w.srv.ops, exists: func(*Widget) bool { return false }}, secretRef("creds"), WithAPIReader(apiReader))
	startSource(t, r)
	w.afterMDPatch = func(gvr schema.GroupVersionResource, ns, name string) {
		if gvr != cuWidgetGVR {
			return
		}
		cur := &Widget{}
		if err := w.srv.raw.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, cur); err != nil {
			t.Error(err)
			return
		}
		cur.Labels = map[string]string{"webhook": "was-here"}
		if err := w.srv.raw.Update(context.Background(), cur); err != nil {
			t.Error(err)
		}
	}
	mustReconcile(t, r, "w")
	if live.Load() != 1 {
		t.Errorf("live reads after the record write = %d, want 1", live.Load())
	}
	got := w.srv.widget("w")
	if got.Labels["webhook"] != "was-here" || got.Annotations[credentialuse.AnnotationRefs] == "" || !pmeta.FinalizerExists(got, FinalizerName) {
		t.Errorf("stored widget labels %v, annotations %v, finalizers %v; want the webhook's label, the record and the finalizer",
			got.Labels, got.Annotations, got.Finalizers)
	}
	if n := w.srv.ops.count("update-failed") + w.srv.ops.count("status-failed"); n != 0 {
		t.Errorf("%d writes failed: the pass wrote from a stale copy (%v)", n, w.srv.ops.list())
	}
}

type readerFn func(ctx context.Context, key client.ObjectKey, obj client.Object) error

func (f readerFn) Get(ctx context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	return f(ctx, key, obj)
}

func (f readerFn) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errors.New("readerFn: no list")
}

// §8 step 5 and the governor: Acquire runs before the gate, so a deferred
// pass has acquired, and the deferral compares against the pristine copy
// taken after Acquire. The second of two identical at-capacity deferrals
// writes no status, though its A1 wrote the record again, after a hand edit
// removed it: a pristine copy taken before A1 would differ, and write.
func TestCredentialUseDeferralRightAfterTheRecordWrite(t *testing.T) {
	g := newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: 1})
	held, d := g.Acquire(context.Background(), "api.example.com")
	if d != nil {
		t.Fatal(d)
	}
	defer held()
	w := newCUWorld(t, newWidget("w", nil))
	p := &widgetProvider{ops: w.srv.ops, key: "api.example.com"}
	r := NewReconciler(&fake.Manager{Client: w.srv.c, Scheme: w.srv.scheme}, resource.ManagedKind(widgetGVK),
		WithExternalConnecter(keyedRefsConnecter{refsConnecter{widgetProvider: p, refs: secretRef("creds")}}),
		WithFinalizer(resource.NewAPIFinalizer(w.srv.c, FinalizerName)),
		WithCredentialUseShared(w.s), WithRateLimitGovernor(g))
	startSource(t, r)
	for pass := 1; pass <= 2; pass++ {
		w.srv.ops.reset()
		res, err := r.Reconcile(context.Background(), reqFor("w"))
		if err != nil || res.RequeueAfter <= 0 {
			t.Fatalf("pass %d: Reconcile = %+v, %v; want a deferral", pass, res, err)
		}
		if got := w.srv.widget("w").Annotations[credentialuse.AnnotationRefs]; got == "" {
			t.Fatalf("pass %d: the deferred pass didn't acquire: no record", pass)
		}
		if slices.Contains(w.srv.ops.list(), "connect") {
			t.Fatalf("pass %d connected: %v", pass, w.srv.ops.list())
		}
		if pass == 1 {
			// A hand edit removes the record: the next pass writes it again.
			cur := w.srv.widget("w")
			delete(cur.Annotations, credentialuse.AnnotationRefs)
			if err := w.srv.raw.Update(context.Background(), cur); err != nil {
				t.Fatal(err)
			}
		}
	}
	if got := w.srv.ops.list(); !slices.Equal(got, []string{"md-patch widgets"}) {
		t.Errorf("the second deferral's calls = %v; want the record write only: a deferral that changes nothing writes no status", got)
	}
}

// keyedRefsConnecter is a refsConnecter with a connecter rate-limit key.
type keyedRefsConnecter struct{ refsConnecter }

func (c keyedRefsConnecter) RateLimitKey(resource.Managed) (string, error) { return c.key, nil }

// §8, the gated queue: with saturation on, the local slot covers the whole
// reconcile, so Acquire's API calls run inside it: with one slot and two
// objects, the second object's record write waits for the first pass.
func TestCredentialUseAcquireRunsInsideTheSlot(t *testing.T) {
	g := saturatedGovernor(t, 2)
	w := newCUWorld(t, newWidget("a", nil), newWidget("b", nil))
	r := w.reconciler(nil, &widgetProvider{ops: w.srv.ops}, secretRef("creds"))
	startSource(t, r)
	entered := make(chan string, 2)
	release := make(chan struct{})
	w.mdPatch = func(gvr schema.GroupVersionResource, _, name string) error {
		if gvr == cuWidgetGVR {
			entered <- name
			<-release
		}
		return nil
	}
	add, stop := runningController(t, "cu-slot", 2, slotOptions(g), r)
	defer stop()
	add(reqFor("a"), 0)
	add(reqFor("b"), 0)
	first := ""
	select {
	case first = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no record write started")
	}
	select {
	case second := <-entered:
		t.Fatalf("%s's record write started while %s's pass held the only slot", second, first)
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the second record write never started")
	}
}

// §12: the pointer to the unheld refs, exactly, and within 160 characters.
func TestUnheldSuffix(t *testing.T) {
	since := time.Date(2026, 11, 8, 17, 2, 11, 0, time.FixedZone("x", 3600))
	entry := func(ref, reason string, at time.Time) map[string]any {
		return map[string]any{"ref": ref, "reason": reason, "since": at.Format(time.RFC3339)}
	}
	mk := func(entries ...map[string]any) resource.Managed {
		w := newWidget("w", nil)
		if len(entries) > 0 {
			b, _ := jsonMarshal(entries)
			w.Annotations = map[string]string{credentialuse.AnnotationUnheld: b}
		}
		return w
	}
	cases := map[string]struct {
		mg   resource.Managed
		want string
	}{
		"none": {mg: mk(), want: ""},
		"empty": {mg: func() resource.Managed {
			w := newWidget("w", nil)
			w.Annotations = map[string]string{credentialuse.AnnotationUnheld: "[]"}
			return w
		}(), want: ""},
		"one": {mg: mk(entry("team-b/Secret/db", "ConsentWithdrawn", since)),
			want: "; credential-use: 1 ref not held (ConsentWithdrawn since 2026-11-08T16:02:11Z), see annotation credential-use.krateo.io/unheld"},
		"two, the most recent named": {mg: mk(entry("team-b/Secret/a", "NotConsented", since.Add(-time.Hour)), entry("team-b/Secret/b", "ConsentWithdrawn", since)),
			want: "; credential-use: 2 refs not held (ConsentWithdrawn since 2026-11-08T16:02:11Z, +1 more), see annotation credential-use.krateo.io/unheld"},
		"more counted": {mg: mk(entry("team-b/Secret/a", "NotConsented", since), map[string]any{"ref": "…", "reason": "More", "count": 20}),
			want: "; credential-use: 21 refs not held (NotConsented since 2026-11-08T16:02:11Z, +20 more), see annotation credential-use.krateo.io/unheld"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := unheldSuffix(tc.mg); got != tc.want {
				t.Errorf("unheldSuffix =\n %q\nwant\n %q", got, tc.want)
			}
		})
	}
	longest := unheldSuffix(mk(entry("x/Secret/y", "AReasonLongerThanSixteen", since), map[string]any{"ref": "…", "reason": "More", "count": 1000000}))
	if len(longest) > 160 || !strings.Contains(longest, " 9999 refs ") || !strings.Contains(longest, "(AReasonLongerTha since") {
		t.Errorf("the longest suffix is %d characters, want at most 160, with n capped at 9999 and the reason at 16: %q", len(longest), longest)
	}
}

// §12: the suffix goes on a pass that failed in Connect, with a non-empty
// unheld list, and on nothing else: not on a successful pass with the same
// list. The ref is in another namespace whose target hasn't consented, so the
// pass's own record write lists it as NotConsented (§11.3).
func TestCredentialUseSuffixOnlyOnAConnectFailure(t *testing.T) {
	w := newCUWorld(t, newWidget("w", nil),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-b"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "team-b"}})
	p := &widgetProvider{ops: w.srv.ops, connectErr: errors.New("secret team-b/db: forbidden")}
	r := w.reconciler(nil, p, func(resource.Managed) ([]credentialuse.Ref, error) {
		return []credentialuse.Ref{credentialuse.SecretRef("team-b", "db")}, nil
	})
	startSource(t, r)
	if _, err := r.Reconcile(context.Background(), reqFor("w")); err == nil {
		t.Fatal("Reconcile succeeded, want the Connect error")
	}
	got := w.srv.widget("w")
	synced := got.GetCondition(prv1.TypeSynced)
	suffix := unheldSuffix(got)
	if suffix == "" || !strings.Contains(suffix, "1 ref not held (NotConsented since ") {
		t.Fatalf("unheld list %q gives suffix %q; want one NotConsented ref", got.Annotations[credentialuse.AnnotationUnheld], suffix)
	}
	if want := "connect failed: secret team-b/db: forbidden" + suffix; synced.Message != want {
		t.Errorf("Synced message =\n %q\nwant\n %q", synced.Message, want)
	}

	p.connectErr = nil
	mustReconcile(t, r, "w")
	if got := w.srv.widget("w").GetCondition(prv1.TypeSynced); strings.Contains(got.Message, "credential-use") {
		t.Errorf("a successful pass's Synced message %q names the unheld refs", got.Message)
	}
}

func jsonMarshal(v any) (string, error) {
	b, err := json.Marshal(v)
	return string(b), err
}

// IP2: a pass that ends without an error, a rate-limited requeue or a
// deferral requeues at the smaller of Acquired.RetryAfter and the time it
// asked for.
func TestCredentialUseRetryAfter(t *testing.T) {
	cases := map[string]struct {
		p    credentialUsePass
		res  reconcile.Result
		err  error
		want reconcile.Result
	}{
		"none":         {p: credentialUsePass{}, res: reconcile.Result{RequeueAfter: time.Minute}, want: reconcile.Result{RequeueAfter: time.Minute}},
		"poll":         {p: credentialUsePass{retryAfter: 2 * time.Second}, res: reconcile.Result{RequeueAfter: time.Minute}, want: reconcile.Result{RequeueAfter: 2 * time.Second}},
		"sooner kept":  {p: credentialUsePass{retryAfter: 2 * time.Second}, res: reconcile.Result{RequeueAfter: time.Second}, want: reconcile.Result{RequeueAfter: time.Second}},
		"no requeue":   {p: credentialUsePass{retryAfter: 2 * time.Second}, res: reconcile.Result{}, want: reconcile.Result{RequeueAfter: 2 * time.Second}},
		"error":        {p: credentialUsePass{retryAfter: 2 * time.Second}, res: reconcile.Result{}, err: errors.New("x"), want: reconcile.Result{}},
		"rate-limited": {p: credentialUsePass{retryAfter: 2 * time.Second}, res: rateLimitedRequeue(), want: rateLimitedRequeue()},
		"deferred":     {p: credentialUsePass{retryAfter: 2 * time.Second, deferred: true}, res: reconcile.Result{RequeueAfter: time.Minute}, want: reconcile.Result{RequeueAfter: time.Minute}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := tc.p.requeue(tc.res, tc.err); got != tc.want {
				t.Errorf("requeue = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// M6 and L8: the predicate drops a holder update whose only change is the
// record this process wrote, and passes any other change, and a record
// change this process didn't write.
func TestIgnoreCredentialUseRecord(t *testing.T) {
	w := newCUWorld(t, newWidget("w", nil))
	r := w.reconciler(nil, &widgetProvider{ops: w.srv.ops}, secretRef("creds"))
	startSource(t, r)
	before := w.srv.widget("w")
	mustReconcile(t, r, "w")
	if !slices.Contains(w.srv.ops.list(), "md-patch widgets") {
		t.Fatalf("no record write: %v", w.srv.ops.list())
	}
	// The record write's own version: the object as A1 left it, before the
	// pass's later writes.
	written := before.DeepCopyObject().(*Widget)
	stored := w.srv.widget("w")
	written.Annotations = map[string]string{credentialuse.AnnotationRefs: stored.Annotations[credentialuse.AnnotationRefs]}
	written.ResourceVersion = "999"
	pred := r.IgnoreCredentialUseRecord()
	update := func(o, n *Widget) bool { return pred.Update(event.UpdateEvent{ObjectOld: o, ObjectNew: n}) }
	if update(before, written) {
		t.Error("the record write this process made passed the predicate")
	}
	other := written.DeepCopyObject().(*Widget)
	other.Spec.Wanted = "changed"
	if !update(before, other) {
		t.Error("a spec change with the record write was dropped")
	}
	foreign := written.DeepCopyObject().(*Widget)
	foreign.Annotations[credentialuse.AnnotationRefs] = `[{"group":"","kind":"Secret","namespace":"demo","name":"other"}]`
	if !update(before, foreign) {
		t.Error("a record this process didn't write was dropped")
	}
	if !update(before, before.DeepCopyObject().(*Widget)) {
		t.Error("an update that doesn't change the record was dropped")
	}
}

// §11.9: the checking reader logs, once per object, a read made during a
// pass of a protected object the holder's refs don't name, and nothing for a
// named one, or outside a pass.
func TestCredentialUseCheckingReader(t *testing.T) {
	w := newCUWorld(t, newWidget("w", nil),
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: "demo"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "unnamed", Namespace: "demo"}})
	rd := CredentialUseCheckingReader(w.srv.raw, w.srv.scheme)
	p := &widgetProvider{ops: w.srv.ops}
	p.observeHook = func(ctx context.Context, _ *Widget) {
		for _, name := range []string{"creds", "unnamed", "unnamed"} {
			if err := rd.Get(ctx, client.ObjectKey{Namespace: "demo", Name: name}, &corev1.Secret{}); err != nil {
				t.Error(err)
			}
		}
	}
	log := &warnLog{}
	r := w.reconciler(nil, p, secretRef("creds"), WithLogger(log))
	startSource(t, r)
	mustReconcile(t, r, "w")
	if err := rd.Get(context.Background(), client.ObjectKey{Namespace: "demo", Name: "unnamed"}, &corev1.Secret{}); err != nil {
		t.Fatal(err)
	}
	got := log.matching("that the holder's refs don't name")
	if len(got) != 1 || !strings.Contains(got[0], "/v1/Secret/demo/unnamed") && !strings.Contains(got[0], "Secret/demo/unnamed") {
		t.Errorf("unrecorded-read warnings = %v; want one, for demo/unnamed", got)
	}
}

// A recorder set with WithMetrics that isn't a credentialuse.MetricsRecorder
// records none of credential-use's metrics, and is logged, once a kind: one
// with no such methods, and one that embeds both a *telemetry.Metrics and a
// credentialuse.NopMetrics, and so has each of them twice, and none.
func TestCredentialUseMetricsRecorderThatIsNone(t *testing.T) {
	type both struct {
		*telemetry.Metrics
		credentialuse.NopMetrics
	}
	type embeds struct {
		MetricsRecorder
		credentialuse.NopMetrics
	}
	for name, tc := range map[string]struct {
		m    MetricsRecorder
		want int
	}{
		"none set":                  {},
		"no such methods":           {m: struct{ MetricsRecorder }{}, want: 1},
		"each method twice":         {m: both{}, want: 1},
		"a NopMetrics with its own": {m: embeds{}},
	} {
		t.Run(name, func(t *testing.T) {
			w := newCUWorld(t)
			log := &warnLog{}
			w.reconciler(nil, &widgetProvider{ops: w.srv.ops}, secretRef("creds"), WithLogger(log), WithMetrics(tc.m))
			if got := log.matching("isn't a credentialuse.MetricsRecorder"); len(got) != tc.want {
				t.Errorf("warnings = %q, want %d", got, tc.want)
			}
		})
	}
}

// warnLog keeps every Warn, with its values.
type warnLog struct {
	mu    sync.Mutex
	warns []string
}

func (l *warnLog) Info(string, ...any)         {}
func (l *warnLog) Debug(string, ...any)        {}
func (l *warnLog) Error(error, string, ...any) {}
func (l *warnLog) Warn(msg string, kv ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, fmt.Sprint(append([]any{msg}, kv...)...))
}
func (l *warnLog) WithValues(...any) logging.Logger { return l }
func (l *warnLog) WithName(string) logging.Logger   { return l }

func (l *warnLog) matching(sub string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, w := range l.warns {
		if strings.Contains(w, sub) {
			out = append(out, w)
		}
	}
	return out
}

// §8 step 4 (R12): on the orphan path, with the finalizer already gone and
// the record present, the record's removal is an Update of its own; with the
// finalizer present, it goes in the finalizer's Update.
func TestCredentialUseStandaloneForgetOnTheOrphanPath(t *testing.T) {
	rec := `[{"group":"","kind":"Secret","namespace":"demo","name":"creds"}]`
	orphaned := func(finalizers ...string) *Widget {
		w := newWidget("w", map[string]string{credentialuse.AnnotationRefs: rec, pmeta.AnnotationKeyDeletionPolicy: "Orphan"})
		w.Finalizers = finalizers
		now := metav1.Now()
		w.DeletionTimestamp = &now
		return w
	}
	t.Run("finalizer gone", func(t *testing.T) {
		w := newCUWorld(t, orphaned("other"))
		r := w.reconciler(nil, &widgetProvider{ops: w.srv.ops}, secretRef("creds"))
		startSource(t, r)
		mustReconcile(t, r, "w")
		if got := w.srv.ops.list(); !slices.Equal(got, []string{"update"}) {
			t.Errorf("calls = %v, want one Update, with no Connect", got)
		}
		if a := w.srv.widget("w").Annotations; a[credentialuse.AnnotationRefs] != "" {
			t.Errorf("the record is still there: %v", a)
		}
	})
	t.Run("finalizer present", func(t *testing.T) {
		w := newCUWorld(t, orphaned("other", FinalizerName))
		r := w.reconciler(nil, &widgetProvider{ops: w.srv.ops}, secretRef("creds"))
		startSource(t, r)
		mustReconcile(t, r, "w")
		if got := w.srv.ops.list(); !slices.Equal(got, []string{"update"}) {
			t.Errorf("calls = %v, want the one finalizer Update", got)
		}
		got := w.srv.widget("w")
		if got.Annotations[credentialuse.AnnotationRefs] != "" || pmeta.FinalizerExists(got, FinalizerName) {
			t.Errorf("annotations %v, finalizers %v; want both gone in one write", got.Annotations, got.Finalizers)
		}
	})
}

// §9.2: Connect's context carries what Acquire read for a ref with a Next,
// for credentialuse.Resolved; here a Guaranteed ConfigMap, read live.
func TestCredentialUseResolvedOnConnectsContext(t *testing.T) {
	w := newCUWorld(t, newWidget("w", nil))
	config := credentialuse.Ref{GroupKind: schema.GroupKind{Kind: "ConfigMap"}, Namespace: "demo", Name: "config", Guaranteed: true,
		Next: func(u *unstructured.Unstructured) ([]credentialuse.Ref, error) {
			name, _, _ := unstructured.NestedString(u.Object, "data", "secret")
			return []credentialuse.Ref{credentialuse.SecretRef("demo", name)}, nil
		}}
	var got string
	p := &widgetProvider{ops: w.srv.ops}
	r := w.reconciler(nil, p, func(resource.Managed) ([]credentialuse.Ref, error) { return []credentialuse.Ref{config}, nil })
	startSource(t, r)
	p.observeHook = func(context.Context, *Widget) {}
	conn := r.external.ExternalConnectDisconnecter
	r.external.ExternalConnectDisconnecter = ExternalConnectDisconnecterFns{
		ConnectFn: func(ctx context.Context, mg resource.Managed) (ExternalClient, error) {
			if u, ok := credentialuse.Resolved(ctx, config); ok {
				got, _, _ = unstructured.NestedString(u.Object, "data", "secret")
			}
			return conn.Connect(ctx, mg)
		},
		DisconnectFn: conn.Disconnect,
	}
	mustReconcile(t, r, "w")
	if got != "behind" {
		t.Errorf("Resolved on Connect's context gave %q, want the ConfigMap Acquire read", got)
	}
}

// §8 step 8 and §6.7: a ref that was Missing for Acquire and appears before
// the finalizer is written is held by BeforeEstablish, just before it.
func TestCredentialUseBeforeEstablishHoldsARefThatAppeared(t *testing.T) {
	w := newCUWorld(t, newWidget("w", nil))
	p := &widgetProvider{ops: w.srv.ops, exists: func(*Widget) bool { return false }}
	p.observeHook = func(context.Context, *Widget) {
		if err := w.srv.raw.Create(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: "demo"}}); err != nil {
			t.Error(err)
		}
	}
	r := w.reconciler(nil, p, secretRef("creds"))
	startSource(t, r)
	mustReconcile(t, r, "w")
	ops := w.srv.ops.list()
	held, added := slices.Index(ops, "md-patch secrets"), slices.Index(ops, "update")
	if held < 0 || added < 0 || held > added {
		t.Errorf("calls = %v; want the Secret held (md-patch secrets) before the finalizer's Update", ops)
	}
	f, _ := credentialuse.FinalizerName(widgetGVK.GroupKind(), "demo")
	s := &corev1.Secret{}
	if err := w.srv.raw.Get(context.Background(), client.ObjectKey{Namespace: "demo", Name: "creds"}, s); err != nil || !slices.Contains(s.Finalizers, f) {
		t.Errorf("the Secret's finalizers %v (%v) lack %s", s.Finalizers, err, f)
	}
}

// §12: the pointer also goes on a pass that failed in Observe.
func TestCredentialUseSuffixOnAnObserveFailure(t *testing.T) {
	w := newCUWorld(t, newWidget("w", nil),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-b"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "team-b"}})
	p := &widgetProvider{ops: w.srv.ops, observeErr: errors.New("unauthorized")}
	r := w.reconciler(nil, p, func(resource.Managed) ([]credentialuse.Ref, error) {
		return []credentialuse.Ref{credentialuse.SecretRef("team-b", "db")}, nil
	})
	startSource(t, r)
	if _, err := r.Reconcile(context.Background(), reqFor("w")); err == nil {
		t.Fatal("Reconcile succeeded, want the Observe error")
	}
	got := w.srv.widget("w")
	if want := "observe failed: unauthorized" + unheldSuffix(got); unheldSuffix(got) == "" || got.GetCondition(prv1.TypeSynced).Message != want {
		t.Errorf("Synced message = %q, want %q", got.GetCondition(prv1.TypeSynced).Message, want)
	}
}

// §7.3: a Track that fails, here on (DisownPasses − 1) × DisownInterval below
// twice TimeoutBound, with an explicit interval, is a setup error: the
// runnables return it.
func TestCredentialUseTrackErrorFailsStart(t *testing.T) {
	w := newCUWorld(t, newWidget("w", nil))
	m := newCUManager(w)
	w.reconciler(m, &widgetProvider{ops: w.srv.ops}, secretRef("creds"),
		WithCredentialUse(credentialuse.KindOptions{DisownPasses: 2, DisownInterval: time.Second}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, rn := range m.runnables() {
		if err := rn.Start(ctx); err == nil || !strings.Contains(err.Error(), "DisownPasses") {
			t.Errorf("%T.Start = %v, want the Track error", rn, err)
		}
	}
}

// §7.5, IP4: a source's Start starts the informers when nothing has yet, so
// that EnableWarmup, which starts sources before election, works.
func TestCredentialUseSourceStartStartsTheInformers(t *testing.T) {
	w := newCUWorld(t)
	wr := newSharedWiring(w.s)
	t.Cleanup(wr.cancelRun)
	src := &credentialUseSource{kind: "Widget", wiring: wr}
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	defer q.ShutDown()
	if err := src.Start(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	// The fake API server allows no access review, so RunInformers returns
	// at once: that it returned at all shows it ran.
	select {
	case <-wr.runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the source's Start didn't start the informers")
	}
}

// §7.5, L1 and A22: WaitForSync waits until the tracker has synced, and the
// tracker's Enqueue reaches the queue the source got in Start, and nothing
// before it.
func TestCredentialUseSourceWaitsAndForwards(t *testing.T) {
	w := newCUWorld(t, newWidget("w", nil))
	r := w.reconciler(nil, &widgetProvider{ops: w.srv.ops}, secretRef("creds"))
	src := r.cu.src
	src.enqueue(types.NamespacedName{Namespace: "demo", Name: "early"}) // before Start: dropped
	q := startSource(t, r)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := src.WaitForSync(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("WaitForSync on informers that never ran = %v, want it to wait until ctx ends", err)
	}
	src.enqueue(types.NamespacedName{Namespace: "demo", Name: "w"})
	if q.Len() != 1 {
		t.Fatalf("queue length = %d, want the one enqueued after Start", q.Len())
	}
	if item, _ := q.Get(); item.Name != "w" {
		t.Errorf("queued %v, want demo/w", item)
	}
}

// IP4: the informers need no leader election, and the write loops do.
func TestCredentialUseRunnablesElection(t *testing.T) {
	if (informerRunnable{}).NeedLeaderElection() {
		t.Error("the informer runnable needs leader election; a standby's informers must run")
	}
	if !(writeLoopsRunnable{}).NeedLeaderElection() {
		t.Error("the write-loop runnable doesn't need leader election")
	}
}

// A11: concurrent NewReconcilers on one manager share its one Shared, and
// its two runnables are added once.
func TestCredentialUseOneSharedPerManager(t *testing.T) {
	w := newCUWorld(t, newWidget("w", nil))
	m := newCUManager(w)
	var wg sync.WaitGroup
	rs := make([]*Reconciler, 4)
	for i := range rs {
		wg.Go(func() {
			rs[i] = NewReconciler(m, resource.ManagedKind(widgetGVK),
				WithExternalConnecter(refsConnecter{widgetProvider: &widgetProvider{ops: w.srv.ops}, refs: secretRef("creds")}),
				WithFinalizer(resource.NewAPIFinalizer(w.srv.c, FinalizerName)))
		})
	}
	wg.Wait()
	if n := len(m.runnables()); n != 2 {
		t.Errorf("%d runnables added, want 2 for the manager's one Shared", n)
	}
	credentialUseRegistry.mu.Lock()
	s := credentialUseRegistry.defaults[m]
	credentialUseRegistry.mu.Unlock()
	for i, r := range rs {
		if r.cu.wiring == nil || r.cu.wiring.s != s {
			t.Errorf("Reconciler %d doesn't use the manager's Shared", i)
		}
	}
}

// docs/design-tracing.md §4.5 (the user's decision of 2026-11-22): each
// Acquire records one credentialuse.acquire event on the reconcile span, with
// the refs named, how many were held, a count per reason not held (only those
// present) and the outcome; counts only, never a ref's name.
func TestTracingCredentialUseAcquireEvent(t *testing.T) {
	established := func() *Widget {
		w := newWidget("w", nil)
		w.Finalizers = []string{FinalizerName}
		return w
	}
	boom := func(resource.Managed) ([]credentialuse.Ref, error) { return nil, errors.New("boom") }
	cases := map[string]struct {
		widget *Widget
		objs   []client.Object
		refs   func(resource.Managed) ([]credentialuse.Ref, error)
		change func(w *cuWorld)
		want   map[string]string
	}{
		"ok, held": {
			widget: newWidget("w", nil),
			objs:   []client.Object{&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: "demo"}}},
			refs:   secretRef("creds"),
			want:   map[string]string{"refs": "1", "held": "1", "outcome": "ok"},
		},
		"ok, unheld by reason": {
			widget: newWidget("w", nil),
			objs: []client.Object{&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-b"}},
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "team-b"}}},
			refs: func(resource.Managed) ([]credentialuse.Ref, error) {
				return []credentialuse.Ref{credentialuse.SecretRef("demo", "creds"), credentialuse.SecretRef("team-b", "db")}, nil
			},
			want: map[string]string{"refs": "2", "held": "0", "outcome": "ok", "unheld.Missing": "1", "unheld.NotConsented": "1"},
		},
		"conflict": {
			widget: newWidget("w", nil),
			refs:   secretRef("creds"),
			change: func(w *cuWorld) {
				w.mdPatch = func(gvr schema.GroupVersionResource, _, name string) error {
					if gvr == cuWidgetGVR {
						return kerrors.NewConflict(gvr.GroupResource(), name, errors.New("test"))
					}
					return nil
				}
			},
			want: map[string]string{"refs": "0", "held": "0", "outcome": "conflict"},
		},
		"must not connect": {widget: newWidget("w", nil), refs: boom, want: map[string]string{"refs": "0", "held": "0", "outcome": "must_not_connect"}},
		"degraded":         {widget: established(), refs: boom, want: map[string]string{"refs": "0", "held": "0", "outcome": "degraded"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			w := newCUWorld(t, append([]client.Object{tc.widget}, tc.objs...)...)
			if tc.change != nil {
				tc.change(w)
			}
			tp, rec := tracer(t)
			r := w.reconciler(nil, &widgetProvider{ops: w.srv.ops}, tc.refs, WithTracerProvider(tp))
			startSource(t, r)
			_, _ = r.Reconcile(context.Background(), reqFor("w"))
			root := lastRoot(t, rec)
			var got map[string]string
			n := 0
			for _, e := range root.Events() {
				if e.Name != tracing.EventCredentialUseAcquire {
					continue
				}
				n++
				got = map[string]string{}
				for _, kv := range e.Attributes {
					key := strings.TrimPrefix(string(kv.Key), "provider_runtime.credentialuse.")
					got[key] = kv.Value.String()
					if strings.Contains(kv.Value.String(), "creds") || strings.Contains(kv.Value.String(), "db") {
						t.Errorf("attribute %s = %q names a ref", kv.Key, kv.Value.String())
					}
				}
			}
			if n != 1 {
				t.Fatalf("%d credentialuse.acquire events, want 1; events %v", n, events(root))
			}
			if !maps.Equal(got, tc.want) {
				t.Errorf("attributes = %v, want %v", got, tc.want)
			}
		})
	}
}

// §8 step 5 (the user's decision of 2026-11-22): a holder being deleted that
// carries none of its owner finalizers has been finalized: its pass skips
// Acquire, writes no record and holds nothing, and runs on as before. An
// established deleting holder still acquires.
func TestCredentialUseSkipsAcquireOnAFinalizedDeletingHolder(t *testing.T) {
	deleting := func(finalizers ...string) *Widget {
		w := newWidget("w", nil)
		w.Finalizers = finalizers
		now := metav1.Now()
		w.DeletionTimestamp = &now
		return w
	}
	secret := func() client.Object {
		return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: "demo"}}
	}
	t.Run("finalized", func(t *testing.T) {
		w := newCUWorld(t, deleting("other"), secret())
		tp, rec := tracer(t)
		r := w.reconciler(nil, &widgetProvider{ops: w.srv.ops, exists: func(*Widget) bool { return false }}, secretRef("creds"), WithTracerProvider(tp))
		startSource(t, r)
		mustReconcile(t, r, "w")
		ops := w.srv.ops.list()
		if slices.ContainsFunc(ops, func(op string) bool { return strings.HasPrefix(op, "md-patch") }) {
			t.Errorf("calls = %v; want no record write and no hold", ops)
		}
		if !slices.Contains(ops, "connect") {
			t.Errorf("calls = %v; want the pass to run on as before", ops)
		}
		if a := w.srv.widget("w").Annotations; a[credentialuse.AnnotationRefs] != "" {
			t.Errorf("a record was written: %v", a)
		}
		if _, ok := eventAttr(lastRoot(t, rec), tracing.EventCredentialUseAcquire, string(tracing.AttrCredentialUseOutcome)); ok {
			t.Error("a credentialuse.acquire event on a pass that skips Acquire")
		}
	})
	t.Run("established", func(t *testing.T) {
		w := newCUWorld(t, deleting("other", FinalizerName), secret())
		r := w.reconciler(nil, &widgetProvider{ops: w.srv.ops, exists: func(*Widget) bool { return false }}, secretRef("creds"))
		startSource(t, r)
		mustReconcile(t, r, "w")
		if !slices.Contains(w.srv.ops.list(), "md-patch widgets") {
			t.Errorf("calls = %v; want an established deleting holder to acquire", w.srv.ops.list())
		}
	})
}

// §7.4 (the user's decision of 2026-11-22): with default KindOptions and a
// long reconcile timeout, as git-provider's 4m, credential-use starts: the
// core derives Disown's interval from the TimeoutBound the adapter passes.
// The review's probe.
func TestCredentialUseStartsWithALongTimeout(t *testing.T) {
	w := newCUWorld(t, newWidget("w", nil))
	r := w.reconciler(newCUManager(w), &widgetProvider{ops: w.srv.ops}, secretRef("creds"),
		WithTimeout(4*time.Minute), WithCredentialUse(credentialuse.KindOptions{}))
	if r.cu.setupErr != nil || r.cu.tracker == nil {
		t.Fatalf("default KindOptions with a 4m timeout: setup error %v; want credential-use on", r.cu.setupErr)
	}
	cfg := publishedConfig(t, w)
	if cfg["timeoutBound"] != "270s" || cfg["disownInterval"] != "135s" {
		t.Errorf("published %v; want timeoutBound 270s and the derived disownInterval 135s", cfg)
	}
}

// publishedConfig reads the settings Track published on the Widget CRD.
func publishedConfig(t *testing.T, w *cuWorld) map[string]any {
	t.Helper()
	crd, err := w.dyn.Resource(schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}).
		Get(context.Background(), "widgets."+widgetGVK.Group, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var cfg map[string]any
	if err := json.Unmarshal([]byte(crd.GetAnnotations()[credentialuse.AnnotationConfig]), &cfg); err != nil {
		t.Fatalf("published settings %q: %v", crd.GetAnnotations()[credentialuse.AnnotationConfig], err)
	}
	return cfg
}

// §7.5 and A6: the adapter's TimeoutBound is its reconcile timeout plus the
// 30s grace, as published on the CRD.
func TestCredentialUseTimeoutBoundIsTheTimeoutPlusTheGrace(t *testing.T) {
	w := newCUWorld(t, newWidget("w", nil))
	w.reconciler(nil, &widgetProvider{ops: w.srv.ops}, secretRef("creds"), WithTimeout(time.Minute))
	if got := publishedConfig(t, w)["timeoutBound"]; got != "90s" {
		t.Errorf("published timeoutBound = %v, want 90s: the 1m timeout plus 30s", got)
	}
}

// §11.3 and IP2: the adapter passes its poll interval to the core, which caps
// the backoff of a failed consent read's requeues there: from 1s, doubling
// to the poll interval.
func TestCredentialUsePollIntervalCapsTheConsentBackoff(t *testing.T) {
	w := newCUWorld(t, newWidget("w", nil),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-b"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "team-b"}})
	w.mdGet = func(gvr schema.GroupVersionResource, ns, name string) error {
		if ns == "team-b" || name == "team-b" {
			return errors.New("the consent read fails")
		}
		return nil
	}
	r := w.reconciler(nil, &widgetProvider{ops: w.srv.ops}, func(resource.Managed) ([]credentialuse.Ref, error) {
		return []credentialuse.Ref{credentialuse.SecretRef("team-b", "db")}, nil
	}, WithPollInterval(1500*time.Millisecond),
		WithPollIntervalHook(func(resource.Managed, time.Duration) time.Duration { return 10 * time.Minute }))
	startSource(t, r)
	for pass, want := range []time.Duration{time.Second, 1500 * time.Millisecond} {
		res, err := r.Reconcile(context.Background(), reqFor("w"))
		if err != nil || res.RequeueAfter != want {
			t.Fatalf("pass %d: Reconcile = %+v, %v; want RequeueAfter %s", pass+1, res, err, want)
		}
	}
}

// §8 step 8: when BeforeEstablish fails, the pass writes ReconcileError and
// returns, without adding the finalizer.
func TestCredentialUseBeforeEstablishErrorAddsNoFinalizer(t *testing.T) {
	w := newCUWorld(t, newWidget("w", nil))
	reads := 0
	w.mdGet = func(gvr schema.GroupVersionResource, _, _ string) error {
		if gvr.Resource == "secrets" {
			// The first confirms A2's 404: the Secret is Missing.
			if reads++; reads > 1 {
				return errors.New("the live read fails")
			}
		}
		return nil
	}
	r := w.reconciler(nil, &widgetProvider{ops: w.srv.ops, exists: func(*Widget) bool { return false }}, secretRef("creds"))
	startSource(t, r)
	if _, err := r.Reconcile(context.Background(), reqFor("w")); err == nil {
		t.Fatal("Reconcile succeeded, want BeforeEstablish's error")
	}
	got := w.srv.widget("w")
	if pmeta.FinalizerExists(got, FinalizerName) {
		t.Error("the finalizer was added after BeforeEstablish failed")
	}
	if s := got.GetCondition(prv1.TypeSynced); s.Reason != prv1.ReasonReconcileError || !strings.Contains(s.Message, "before establishing") {
		t.Errorf("Synced = %+v, want ReconcileError naming the check", s)
	}
	if reads != 2 {
		t.Errorf("%d reads of the Secret, want two: A2's, and the check's", reads)
	}
	if slices.Contains(w.srv.ops.list(), "create") {
		t.Errorf("calls = %v; the pass created after BeforeEstablish failed", w.srv.ops.list())
	}
}

// §8 step 5 (R4): an established holder whose acquire fails proceeds to
// Connect as today (fail open), with no ReconcileError of its own.
func TestCredentialUseEstablishedHolderFailsOpen(t *testing.T) {
	wd := initialized(newWidget("w", nil))
	wd.Finalizers = []string{FinalizerName}
	w := newCUWorld(t, wd)
	r := w.reconciler(nil, &widgetProvider{ops: w.srv.ops}, func(resource.Managed) ([]credentialuse.Ref, error) {
		return nil, errors.New("boom")
	})
	startSource(t, r)
	mustReconcile(t, r, "w")
	if !slices.Contains(w.srv.ops.list(), "connect") {
		t.Errorf("calls = %v; want the degraded pass to connect", w.srv.ops.list())
	}
	if s := w.srv.widget("w").GetCondition(prv1.TypeSynced); s.Status != metav1.ConditionTrue {
		t.Errorf("Synced = %+v, want True: a degraded pass proceeds", s)
	}
}
