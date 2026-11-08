package scaletozero

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// The fake client of controller-runtime v0.23.3 can't stand in for an API
// server here. Its List ignores Limit and Continue, and it never consults the
// RESTMapper, so a missing CRD lists as empty instead of failing. Its scale
// Patch patches the Scale body as an object of its own. So Exists and
// Reconcile are tested against envtest's kube-apiserver and etcd.

const testGroup = "scaletozero.test.krateo.io"

var (
	// Widget is served at v1; v1alpha1 is declared but not served.
	widgetGVK         = schema.GroupVersionKind{Group: testGroup, Version: "v1", Kind: "Widget"}
	unservedWidgetGVK = schema.GroupVersionKind{Group: testGroup, Version: "v1alpha1", Kind: "Widget"}
	// Sprocket is populated by one test only, so labels.Everything() can be
	// checked on it exactly.
	sprocketGVK = schema.GroupVersionKind{Group: testGroup, Version: "v1", Kind: "Sprocket"}
	// Gadget's CRD is deleted by a test.
	gadgetGVK = schema.GroupVersionKind{Group: testGroup, Version: "v1", Kind: "Gadget"}
	// Missing has no CRD.
	missingGVK = schema.GroupVersionKind{Group: testGroup, Version: "v1", Kind: "Missing"}
)

var (
	testEnv *envtest.Environment
	testCfg *rest.Config
	// k8s is an admin client that reads from the API server.
	k8s    client.Client
	scheme = runtime.NewScheme()
)

func TestMain(m *testing.M) {
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		os.Exit(m.Run())
	}
	testEnv = &envtest.Environment{
		CRDs: []*apiextensionsv1.CustomResourceDefinition{
			testCRD("Widget", "widgets", "v1", "v1alpha1"),
			testCRD("Sprocket", "sprockets", "v1"),
			testCRD("Gadget", "gadgets", "v1"),
		},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := testEnv.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "envtest:", err)
		os.Exit(1)
	}
	testCfg = cfg
	k8s, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fmt.Fprintln(os.Stderr, "client:", err)
		_ = testEnv.Stop()
		os.Exit(1)
	}
	code := m.Run()
	_ = testEnv.Stop()
	os.Exit(code)
}

// requireAPIServer skips a test that needs envtest when its binaries aren't
// there, except under CI, where a skip would hide that the test never ran.
func requireAPIServer(t *testing.T) {
	t.Helper()
	if testCfg != nil {
		return
	}
	msg := "KUBEBUILDER_ASSETS is not set: this test needs envtest's kube-apiserver and etcd " +
		"(export KUBEBUILDER_ASSETS=$(setup-envtest use -p path 1.37.0))"
	if os.Getenv("CI") != "" {
		t.Fatal(msg)
	}
	t.Skip(msg)
}

// testCRD builds a namespaced CRD whose first version is served and stored,
// and whose other versions are declared but not served.
func testCRD(kind, plural string, versions ...string) *apiextensionsv1.CustomResourceDefinition {
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: plural + "." + testGroup},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: testGroup,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Kind: kind, ListKind: kind + "List", Plural: plural, Singular: strings.ToLower(kind),
			},
			Scope: apiextensionsv1.NamespaceScoped,
		},
	}
	for i, v := range versions {
		crd.Spec.Versions = append(crd.Spec.Versions, apiextensionsv1.CustomResourceDefinitionVersion{
			Name:    v,
			Served:  i == 0,
			Storage: i == 0,
			Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
				Type: "object", XPreserveUnknownFields: ptr.To(true),
			}},
		})
	}
	return crd
}

var nsCounter atomic.Int64

// newNamespace creates a namespace of the test's own.
func newNamespace(t *testing.T) string {
	t.Helper()
	ns := fmt.Sprintf("stz-%d", nsCounter.Add(1))
	if err := k8s.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatalf("creating namespace %s: %v", ns, err)
	}
	return ns
}

// newInstance creates an instance of gvk with the given labels and finalizers.
func newInstance(t *testing.T, gvk schema.GroupVersionKind, ns, name string, lbls map[string]string, finalizers ...string) *unstructured.Unstructured {
	t.Helper()
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetNamespace(ns)
	u.SetName(name)
	u.SetLabels(lbls)
	u.SetFinalizers(finalizers)
	if err := k8s.Create(context.Background(), u); err != nil {
		t.Fatalf("creating %s %s/%s: %v", gvk.Kind, ns, name, err)
	}
	return u
}

// newDeployment creates a controller Deployment at the given replicas. envtest
// runs no controller manager, so no pods appear: only spec.replicas matters.
func newDeployment(t *testing.T, ns, name string, replicas int32) *appsv1.Deployment {
	t.Helper()
	sel := map[string]string{"app": name}
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: sel},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: sel},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "controller", Image: "example.invalid/controller:0"}}},
			},
		},
	}
	if err := k8s.Create(context.Background(), d); err != nil {
		t.Fatalf("creating deployment %s/%s: %v", ns, name, err)
	}
	return d
}

// deploymentReplicas reads spec.replicas of the Deployment itself, as an admin.
func deploymentReplicas(t *testing.T, ns, name string) int32 {
	t.Helper()
	d := &appsv1.Deployment{}
	if err := k8s.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, d); err != nil {
		t.Fatalf("reading deployment %s/%s: %v", ns, name, err)
	}
	return ptr.Deref(d.Spec.Replicas, -1)
}

// listRequest is one LIST request as it went on the wire.
type listRequest struct {
	path, limit, cont, labelSelector, accept string
}

// recordingClient is a client whose LIST requests to resource are recorded.
func recordingClient(t *testing.T, resource string) (client.Client, func() []listRequest) {
	t.Helper()
	var mu sync.Mutex
	var reqs []listRequest
	cfg := rest.CopyConfig(testCfg)
	cfg.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/"+resource) {
				q := r.URL.Query()
				mu.Lock()
				reqs = append(reqs, listRequest{
					path: r.URL.Path, limit: q.Get("limit"), cont: q.Get("continue"),
					labelSelector: q.Get("labelSelector"), accept: r.Header.Get("Accept"),
				})
				mu.Unlock()
			}
			return rt.RoundTrip(r)
		})
	})
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	return c, func() []listRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]listRequest(nil), reqs...)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// eventually polls cond until it holds or the deadline passes.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
