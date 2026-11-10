package reconciler

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
	"github.com/krateo-platformops/provider-runtime/pkg/resource/fake"
	"github.com/krateo-platformops/provider-runtime/pkg/tracing"
)

// The marker's optimistic lock is tested against envtest's kube-apiserver, so
// the API server's own resourceVersion check is what refuses the patch
// (docs/design-tracing.md §10, tests 12 and 13).

var (
	traceEnv    *envtest.Environment
	traceClient client.WithWatch // reads and writes the API server directly
)

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		os.Exit(m.Run())
	}
	traceEnv = &envtest.Environment{
		CRDs:                  []*apiextensionsv1.CustomResourceDefinition{widgetCRD()},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := traceEnv.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "envtest:", err)
		os.Exit(1)
	}
	s := widgetScheme()
	if err := corev1.AddToScheme(s); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	traceClient, err = client.NewWithWatch(cfg, client.Options{Scheme: s})
	if err != nil {
		fmt.Fprintln(os.Stderr, "client:", err)
		_ = traceEnv.Stop()
		os.Exit(1)
	}
	if err := traceClient.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "demo"}}); err != nil {
		fmt.Fprintln(os.Stderr, "namespace:", err)
		_ = traceEnv.Stop()
		os.Exit(1)
	}
	code := m.Run()
	_ = traceEnv.Stop()
	os.Exit(code)
}

// requireTraceAPIServer skips a test that needs envtest when its binaries
// aren't there, except under CI, where a skip would hide that it never ran.
func requireTraceAPIServer(t *testing.T) {
	t.Helper()
	if traceClient != nil {
		return
	}
	msg := "KUBEBUILDER_ASSETS is not set: this test needs envtest (export KUBEBUILDER_ASSETS=$(setup-envtest use -p path 1.37.0))"
	if os.Getenv("CI") != "" {
		t.Fatal(msg)
	}
	t.Skip(msg)
}

func widgetCRD() *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "widgets." + widgetGVK.Group},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: widgetGVK.Group,
			Names: apiextensionsv1.CustomResourceDefinitionNames{Kind: "Widget", ListKind: "WidgetList", Plural: "widgets", Singular: "widget"},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: widgetGVK.Version, Served: true, Storage: true,
				Subresources: &apiextensionsv1.CustomResourceSubresources{Status: &apiextensionsv1.CustomResourceSubresourceStatus{}},
				Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
					Type: "object", XPreserveUnknownFields: ptr.To(true),
				}},
			}},
		},
	}
}

// createLiveWidget creates a widget on envtest and returns its key; the test's
// cleanup deletes it.
func createLiveWidget(t *testing.T, name string, kv map[string]string) types.NamespacedName {
	t.Helper()
	w := newWidget(name, kv)
	w.UID = ""
	if err := traceClient.Create(context.Background(), w); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	t.Cleanup(func() {
		_ = traceClient.Delete(context.Background(), &Widget{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "demo"}})
	})
	return types.NamespacedName{Namespace: "demo", Name: name}
}

func liveWidget(t *testing.T, key types.NamespacedName) *Widget {
	t.Helper()
	w := &Widget{}
	if err := traceClient.Get(context.Background(), key, w); err != nil {
		t.Fatal(err)
	}
	return w
}

// liveReconciler reconciles widgets against envtest through c.
func liveReconciler(c client.Client, p *widgetProvider, o ...ReconcilerOption) *Reconciler {
	return NewReconciler(&fake.Manager{Client: c, Scheme: widgetScheme()}, resource.ManagedKind(widgetGVK), append([]ReconcilerOption{
		WithExternalConnecter(p),
		WithFinalizer(resource.FinalizerFns{
			AddFinalizerFn:    func(context.Context, resource.Object) error { return nil },
			RemoveFinalizerFn: func(context.Context, resource.Object) error { return nil },
		}),
	}, o...)...)
}

// design-tracing §10, test 12. Another writer Status().Patches the object after
// the pass's Get and before its marker patch, as alert-provider's reanalysis
// would write new scripts. The API server refuses the locked marker patch; the
// pass requeues before acting, with k8s.conflict step marker, and never writes
// status over the other writer's. The next pass is Parent.
func TestTracingEnvtestConcurrentWriterBeforeTheMarker(t *testing.T) {
	requireTraceAPIServer(t)
	key := createLiveWidget(t, "concurrent", ann(meta.AnnotationKeyTraceparent, tpA))
	tp, rec := tracer(t)
	ops := &apiOps{}
	p := &widgetProvider{ops: ops}
	var once sync.Once
	p.observeHook = func(ctx context.Context, _ *Widget) {
		once.Do(func() {
			w := liveWidget(t, key)
			patch := client.MergeFrom(w.DeepCopyObject().(*Widget))
			w.Status.Script = "from-reanalysis"
			if err := traceClient.Status().Patch(ctx, w, patch); err != nil {
				t.Errorf("the other writer's status patch: %v", err)
			}
		})
	}
	r := liveReconciler(traceClient, p, WithTracerProvider(tp))

	res, err := r.Reconcile(context.Background(), reqFor("concurrent"))
	if err != nil || !isRateLimitedRequeue(res) {
		t.Fatalf("Reconcile = %v, %v; want a rate-limited requeue", res, err)
	}
	if ops.count("act-update") != 0 {
		t.Errorf("ops = %v, want no act after the marker conflict", ops.list())
	}
	root := lastRoot(t, rec)
	if got, _ := eventAttr(root, tracing.EventConflict, string(tracing.AttrStep)); got != string(tracing.StepMarker) {
		t.Errorf("conflict step = %q, want marker; events %v", got, events(root))
	}
	stored := liveWidget(t, key)
	if stored.Status.Script != "from-reanalysis" {
		t.Errorf("status.script = %q: the other writer's status was overwritten", stored.Status.Script)
	}
	if stored.GetAnnotations()[meta.AnnotationKeyTraceparentContinued] != "" {
		t.Error("the refused marker was stored")
	}

	mustReconcile(t, r, "concurrent")
	wantMode(t, lastRoot(t, rec), tracing.Parent, tpA)
	if ops.count("act-update") != 1 {
		t.Errorf("ops = %v, want the next pass to act", ops.list())
	}
	if got := liveWidget(t, key).GetAnnotations()[meta.AnnotationKeyTraceparentContinued]; got != tpA {
		t.Errorf("marker = %q after the next pass, want %s", got, tpA)
	}
}

// design-tracing §10, test 13. A second pass reads a cached copy that doesn't
// show the marker yet. The memo's marked entry makes it Link, so it doesn't act
// as a child. With the memo cleared, as after a restart, the locked patch built
// from the stale copy conflicts, and nothing acts twice as a child.
func TestTracingEnvtestStaleCache(t *testing.T) {
	requireTraceAPIServer(t)
	key := createLiveWidget(t, "stale", ann(meta.AnnotationKeyTraceparent, tpA))
	stale := liveWidget(t, key) // before the marker
	staleClient := interceptor.NewClient(traceClient, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, k client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if k == key {
				*obj.(*Widget) = *stale.DeepCopyObject().(*Widget)
				return nil
			}
			return c.Get(ctx, k, obj, opts...)
		},
	})
	tp, rec := tracer(t)
	ops := &apiOps{}

	r := liveReconciler(traceClient, &widgetProvider{ops: ops}, WithTracerProvider(tp))
	mustReconcile(t, r, "stale") // acts and marks
	if got := liveWidget(t, key).GetAnnotations()[meta.AnnotationKeyTraceparentContinued]; got != tpA {
		t.Fatalf("marker = %q after the first pass, want %s", got, tpA)
	}

	// The same process, reading the stale copy: the memo says marked, so Link.
	lagging := liveReconciler(staleClient, &widgetProvider{ops: ops}, WithTracerProvider(tp))
	lagging.traces.entries = r.traces.entries
	_, _ = lagging.Reconcile(context.Background(), reqFor("stale"))
	wantMode(t, lastRoot(t, rec), tracing.Link, tpA)

	// A new process, reading the stale copy: Parent, but the locked patch carries
	// the stale resourceVersion and the API server refuses it before any act.
	ops.reset()
	restarted := liveReconciler(staleClient, &widgetProvider{ops: ops}, WithTracerProvider(tp))
	res, err := restarted.Reconcile(context.Background(), reqFor("stale"))
	if err != nil || !isRateLimitedRequeue(res) {
		t.Fatalf("Reconcile = %v, %v; want a rate-limited requeue", res, err)
	}
	if ops.count("act-update") != 0 {
		t.Errorf("ops = %v, want no second act as a child", ops.list())
	}
	if got, _ := eventAttr(lastRoot(t, rec), tracing.EventConflict, string(tracing.AttrStep)); got != string(tracing.StepMarker) {
		t.Errorf("conflict step = %q, want marker", got)
	}
}
