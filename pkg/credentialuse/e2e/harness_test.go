// Package e2e runs the credential-use envtests with the real reconciler:
// a controller-runtime manager reconciles a Widget kind through
// pkg/reconciler, whose connecter names the Secret it reads (plan, PR 7, A21).
// It is a test-only package with its own TestMain, so that the core's tests
// in pkg/credentialuse don't depend on pkg/reconciler.
package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
	"github.com/krateo-platformops/provider-runtime/pkg/reconciler"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
)

// env is the envtest API server, or nil when KUBEBUILDER_ASSETS is unset.
var env *cutest.Env

func TestMain(m *testing.M) {
	// A managed resource's CRD has the status subresource, which the
	// reconciler writes status through.
	e, err := cutest.StartWith(cutest.CRDWithStatus("Widget", "widgets"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	env = e
	code := m.Run()
	if err := env.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, "stopping envtest:", err)
	}
	os.Exit(code)
}

var (
	widgetGVK = cutest.WidgetGVK
	widgetGK  = widgetGVK.GroupKind()
)

// Widget is the managed resource the tests reconcile. Its spec names the
// Secret its connecter reads.
type Widget struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              WidgetSpec   `json:"spec,omitempty"`
	Status            WidgetStatus `json:"status,omitempty"`
}

// WidgetSpec names the credential Secret: in the Widget's namespace, unless
// SecretNamespace says otherwise. FailConnect makes Connect fail after it
// read the Secret.
type WidgetSpec struct {
	Secret          string `json:"secret,omitempty"`
	SecretNamespace string `json:"secretNamespace,omitempty"`
	FailConnect     bool   `json:"failConnect,omitempty"`
}

type WidgetStatus struct {
	prv1.ConditionedStatus `json:",inline"`
}

func (w *Widget) DeepCopyObject() runtime.Object {
	out := &Widget{}
	b, err := json.Marshal(w)
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(b, out); err != nil {
		panic(err)
	}
	return out
}

func (w *Widget) GetCondition(ct prv1.ConditionType) prv1.Condition { return w.Status.GetCondition(ct) }
func (w *Widget) SetConditions(c ...prv1.Condition)                 { w.Status.SetConditions(c...) }

type WidgetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Widget `json:"items"`
}

func (l *WidgetList) DeepCopyObject() runtime.Object {
	out := &WidgetList{}
	b, _ := json.Marshal(l)
	_ = json.Unmarshal(b, out)
	return out
}

func widgetScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		panic(err)
	}
	s.AddKnownTypes(widgetGVK.GroupVersion(), &Widget{}, &WidgetList{})
	metav1.AddToGroupVersion(s, widgetGVK.GroupVersion())
	return s
}

// The settings every test's managers share. They are published on the CRD
// (§7.3), so every manager of a test, and of the next, must agree on them.
// TimeoutBound is the reconcile timeout plus 30s, 40s, so (DisownPasses − 1)
// × DisownInterval must be at least 80s.
const (
	testGrace   = 3 * time.Second
	testTimeout = 10 * time.Second
)

func kindOptions() cu.KindOptions {
	return cu.KindOptions{DeletionGrace: testGrace, ProbeInterval: time.Second, DisownInterval: 500 * time.Millisecond, DisownPasses: 200}
}

// resetPublished removes the kind's published settings, so that a test leaves
// nothing behind on the shared CRD (§7.3).
func resetPublished(t *testing.T) {
	t.Helper()
	crds := env.Dynamic.Resource(schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"})
	_, err := crds.Patch(context.Background(), cutest.WidgetGVR.Resource+"."+cutest.WidgetGVR.Group, types.MergePatchType,
		[]byte(`{"metadata":{"annotations":{"`+cu.AnnotationConfig+`":null}}}`), metav1.PatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
}

// world is the external system the Widgets stand for, and what the
// connecter saw.
type world struct {
	t    *testing.T
	kube client.Client // reads the API server directly

	mu      sync.Mutex
	exists  map[types.NamespacedName]bool
	events  []string
	reads   map[types.NamespacedName]int
	connect atomic.Pointer[func(ctx context.Context, w *Widget)]
}

func newWorld(t *testing.T) *world {
	t.Helper()
	c, err := client.New(env.Config, client.Options{Scheme: widgetScheme()})
	if err != nil {
		t.Fatal(err)
	}
	resetPublished(t)
	t.Cleanup(func() { resetPublished(t) })
	return &world{t: t, kube: c, exists: map[types.NamespacedName]bool{}, reads: map[types.NamespacedName]int{}}
}

func (w *world) record(e string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.events = append(w.events, e)
}

func (w *world) recorded() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.events)
}

// connecter reads the Widget's Secret in Connect, as a provider reads its
// credentials. It doesn't implement CredentialRefsProvider: the control.
type connecter struct{ w *world }

func (c connecter) Connect(ctx context.Context, mg resource.Managed) (reconciler.ExternalClient, error) {
	wd := mg.(*Widget)
	if f := c.w.connect.Load(); f != nil {
		(*f)(ctx, wd)
	}
	if wd.Spec.Secret != "" {
		key := secretKey(wd)
		if err := c.w.kube.Get(ctx, key, &corev1.Secret{}); err != nil {
			return nil, fmt.Errorf("reading the credential Secret %s: %w", key, err)
		}
		c.w.mu.Lock()
		c.w.reads[key]++
		c.w.mu.Unlock()
	}
	if wd.Spec.FailConnect {
		return nil, errors.New("the external system refused the credentials")
	}
	return &external{w: c.w}, nil
}

func secretKey(wd *Widget) types.NamespacedName {
	ns := wd.Spec.SecretNamespace
	if ns == "" {
		ns = wd.Namespace
	}
	return types.NamespacedName{Namespace: ns, Name: wd.Spec.Secret}
}

// refsConnecter names the Secret Connect reads.
type refsConnecter struct{ connecter }

func (refsConnecter) CredentialRefs(mg resource.Managed) ([]cu.Ref, error) {
	wd := mg.(*Widget)
	if wd.Spec.Secret == "" {
		return nil, nil
	}
	key := secretKey(wd)
	return []cu.Ref{cu.SecretRef(key.Namespace, key.Name)}, nil
}

type external struct{ w *world }

func (e *external) Observe(_ context.Context, mg resource.Managed) (reconciler.ExternalObservation, error) {
	e.w.mu.Lock()
	exists := e.w.exists[client.ObjectKeyFromObject(mg)]
	e.w.mu.Unlock()
	mg.SetConditions(prv1.Available())
	return reconciler.ExternalObservation{ResourceExists: exists, ResourceUpToDate: true}, nil
}

func (e *external) Create(_ context.Context, mg resource.Managed) error {
	e.w.mu.Lock()
	e.w.exists[client.ObjectKeyFromObject(mg)] = true
	e.w.mu.Unlock()
	e.w.record("create " + client.ObjectKeyFromObject(mg).String())
	return nil
}

func (e *external) Update(context.Context, resource.Managed) error { return nil }

func (e *external) Delete(_ context.Context, mg resource.Managed) error {
	e.w.mu.Lock()
	delete(e.w.exists, client.ObjectKeyFromObject(mg))
	e.w.mu.Unlock()
	e.w.record("delete " + client.ObjectKeyFromObject(mg).String())
	return nil
}

// managerOptions configure one manager running the real reconciler.
type managerOptions struct {
	// refs: the connecter implements CredentialRefsProvider.
	refs bool
	// lease, when set, runs the manager under leader election on it.
	lease string
	// cfg is the manager's config; env.Config by default.
	cfg *rest.Config
	// noSource leaves WatchesRawSource out of the controller.
	noSource bool
	// warmup sets the controller's EnableWarmup.
	warmup bool
}

// running is a manager running the real reconciler for Widgets.
type running struct {
	mgr    manager.Manager
	r      *reconciler.Reconciler
	passes atomic.Int64
	errc   chan error
	cancel context.CancelFunc
	once   sync.Once
	err    error
}

// stop ends the manager and returns what its Start returned.
func (m *running) stop() error {
	m.once.Do(func() {
		m.cancel()
		select {
		case m.err = <-m.errc:
		case <-time.After(30 * time.Second):
			m.err = errors.New("the manager didn't stop within 30s")
		}
	})
	return m.err
}

var managerSeq atomic.Int64

// start runs a manager with the real reconciler for Widgets until the test
// ends.
func (w *world) start(t *testing.T, o managerOptions) *running {
	t.Helper()
	cfg := o.cfg
	if cfg == nil {
		cfg = env.Config
	}
	mo := manager.Options{
		Scheme:                 widgetScheme(),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller:             config.Controller{SkipNameValidation: ptr.To(true)},
	}
	if o.lease != "" {
		mo.LeaderElection = true
		mo.LeaderElectionID = o.lease
		mo.LeaderElectionNamespace = "default"
		mo.LeaderElectionReleaseOnCancel = true
		mo.LeaseDuration = ptr.To(3 * time.Second)
		mo.RenewDeadline = ptr.To(2 * time.Second)
		mo.RetryPeriod = ptr.To(250 * time.Millisecond)
	}
	mgr, err := ctrl.NewManager(cfg, mo)
	if err != nil {
		t.Fatal(err)
	}
	var c reconciler.ExternalConnecter = connecter{w: w}
	if o.refs {
		c = refsConnecter{connecter{w: w}}
	}
	r := reconciler.NewReconciler(mgr, resource.ManagedKind(widgetGVK),
		reconciler.WithExternalConnecter(c),
		reconciler.WithCredentialUse(kindOptions()),
		reconciler.WithTimeout(testTimeout),
		reconciler.WithPollInterval(10*time.Minute),
		// Observe answers at once, so a deletion needn't wait out a
		// creation grace.
		reconciler.WithCreationGracePeriod(0),
		reconciler.WithAPIReader(mgr.GetAPIReader()))
	m := &running{mgr: mgr, r: r, errc: make(chan error, 1)}
	b := ctrl.NewControllerManagedBy(mgr).
		Named(fmt.Sprintf("widget-%d", managerSeq.Add(1))).
		For(&Widget{}, builder.WithPredicates(r.IgnoreCredentialUseRecord())).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 4,
			EnableWarmup:            ptr.To(o.warmup),
			RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
				50*time.Millisecond, 2*time.Second),
		})
	if !o.noSource {
		b = b.WatchesRawSource(r.CredentialUseSource())
	}
	if err := b.Complete(reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
		m.passes.Add(1)
		return r.Reconcile(ctx, req)
	})); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	go func() { m.errc <- mgr.Start(ctx) }()
	t.Cleanup(func() { _ = m.stop() })
	return m
}

func createSecret(t *testing.T, ns, name string, finalizers ...string) {
	t.Helper()
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Finalizers: finalizers}}
	if _, err := env.Kube.CoreV1().Secrets(ns).Create(context.Background(), s, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating Secret %s/%s: %v", ns, name, err)
	}
}

func createWidget(t *testing.T, w *world, ns, name string, spec WidgetSpec, mutate ...func(*Widget)) {
	t.Helper()
	wd := &Widget{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}, Spec: spec}
	for _, f := range mutate {
		f(wd)
	}
	if err := w.kube.Create(context.Background(), wd); err != nil {
		t.Fatalf("creating Widget %s/%s: %v", ns, name, err)
	}
}

func getWidget(t *testing.T, w *world, ns, name string) *Widget {
	t.Helper()
	wd := &Widget{}
	if err := w.kube.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, wd); err != nil {
		t.Fatalf("reading Widget %s/%s: %v", ns, name, err)
	}
	return wd
}

func deleteWidget(t *testing.T, ns, name string, opts ...metav1.DeleteOptions) {
	t.Helper()
	o := metav1.DeleteOptions{}
	if len(opts) > 0 {
		o = opts[0]
	}
	if err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Delete(context.Background(), name, o); err != nil {
		t.Fatal(err)
	}
}

func deleteSecret(t *testing.T, ns, name string) {
	t.Helper()
	if err := env.Kube.CoreV1().Secrets(ns).Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
}

func secretGet(ns, name string) func() error {
	return func() error {
		_, err := env.Kube.CoreV1().Secrets(ns).Get(context.Background(), name, metav1.GetOptions{})
		return err
	}
}

func widgetGet(ns, name string) func() error {
	return func() error {
		_, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
		return err
	}
}

// gone waits until get reports NotFound.
func gone(t *testing.T, timeout time.Duration, what string, get func() error) {
	t.Helper()
	cutest.Eventually(t, timeout, func() error {
		err := get()
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		return fmt.Errorf("%s still exists: %w", what, cutest.ErrNotYet)
	})
}

// established waits until each Widget has the owner finalizer, its record
// and Synced=True, and the Secret is held for the Widgets' namespace.
func established(t *testing.T, w *world, ns string, secret types.NamespacedName, widgets ...string) {
	t.Helper()
	f, _ := cu.FinalizerName(widgetGK, ns)
	cutest.Eventually(t, time.Minute, func() error {
		for _, name := range widgets {
			wd := &Widget{}
			if err := w.kube.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, wd); err != nil {
				return err
			}
			if !slices.Contains(wd.Finalizers, reconciler.FinalizerName) || wd.Annotations[cu.AnnotationRefs] == "" ||
				wd.GetCondition(prv1.TypeSynced).Status != metav1.ConditionTrue {
				return fmt.Errorf("Widget %s not established (finalizers %v, annotations %v, synced %+v): %w",
					name, wd.Finalizers, wd.Annotations, wd.GetCondition(prv1.TypeSynced), cutest.ErrNotYet)
			}
		}
		s, err := env.Kube.CoreV1().Secrets(secret.Namespace).Get(context.Background(), secret.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !slices.Contains(s.Finalizers, f) || s.Annotations[f] == "" || s.Labels[cu.LabelHeld] != "true" {
			return fmt.Errorf("Secret %s not held (finalizers %v): %w", secret, s.Finalizers, cutest.ErrNotYet)
		}
		return nil
	})
}

func nn(ns, name string) types.NamespacedName { return types.NamespacedName{Namespace: ns, Name: name} }

// removeFinalizers empties an object's finalizers, as a human unwedging it
// would.
func removeFinalizers(t *testing.T, gvr schema.GroupVersionResource, ns, name string) {
	t.Helper()
	_, err := env.Dynamic.Resource(gvr).Namespace(ns).Patch(context.Background(), name, types.MergePatchType,
		[]byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
}

var secretGVR = schema.GroupVersionResource{Version: "v1", Resource: "secrets"}

// i1Watch checks every Widget status a watch delivers in a namespace (§14,
// I1): no Widget may ever show Ready=Deleting together with
// Synced=ReconcileError and a not-found message for its credential.
type i1Watch struct {
	mu         sync.Mutex
	violations []string
	stop       context.CancelFunc
}

func watchI1(t *testing.T, ns string) *i1Watch {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	wi, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Watch(ctx, metav1.ListOptions{})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	w := &i1Watch{stop: cancel}
	t.Cleanup(cancel)
	go func() {
		defer wi.Stop()
		for ev := range wi.ResultChan() {
			u, ok := ev.Object.(*unstructured.Unstructured)
			if !ok {
				continue
			}
			conds, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
			reason := map[string]string{}
			message := map[string]string{}
			for _, c := range conds {
				m, _ := c.(map[string]any)
				ty, _ := m["type"].(string)
				reason[ty], _ = m["reason"].(string)
				message[ty], _ = m["message"].(string)
			}
			if reason[string(prv1.TypeReady)] == string(prv1.ReasonDeleting) &&
				reason[string(prv1.TypeSynced)] == string(prv1.ReasonReconcileError) &&
				strings.Contains(message[string(prv1.TypeSynced)], "not found") {
				w.mu.Lock()
				w.violations = append(w.violations, fmt.Sprintf("%s/%s: %s", u.GetNamespace(), u.GetName(), message[string(prv1.TypeSynced)]))
				w.mu.Unlock()
			}
		}
	}()
	return w
}

func (w *i1Watch) seen() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.violations)
}

// versionWatch keeps every version of one Widget a watch delivers.
type versionWatch struct {
	mu       sync.Mutex
	versions []*unstructured.Unstructured
}

func watchVersions(t *testing.T, ns, name string) *versionWatch {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	wi, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Watch(ctx, metav1.ListOptions{FieldSelector: "metadata.name=" + name})
	if err != nil {
		t.Fatal(err)
	}
	v := &versionWatch{}
	go func() {
		defer wi.Stop()
		for ev := range wi.ResultChan() {
			if u, ok := ev.Object.(*unstructured.Unstructured); ok {
				v.mu.Lock()
				v.versions = append(v.versions, u)
				v.mu.Unlock()
			}
		}
	}()
	return v
}

// firstWithout returns the first version, after one that carried finalizer
// f, that doesn't; nil if none has been delivered.
func (v *versionWatch) firstWithout(f string) *unstructured.Unstructured {
	v.mu.Lock()
	defer v.mu.Unlock()
	had := false
	for _, u := range v.versions {
		has := slices.Contains(u.GetFinalizers(), f)
		if had && !has {
			return u
		}
		had = had || has
	}
	return nil
}
