// Package cutest is the envtest harness for pkg/credentialuse's tests. It is
// used only by tests, so it may depend on controller-runtime's envtest; the
// credentialuse package itself must not (§7.1).
//
// envtest is kube-apiserver and etcd only: there is no namespace controller
// and no garbage collector (§2). Tests play those parts with the helpers here.
package cutest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// Group is the API group of the test CRDs.
const Group = "credentialuse.test.krateo.io"

var (
	// WidgetGVK is a namespaced test kind, the holder of the tests.
	WidgetGVK = schema.GroupVersionKind{Group: Group, Version: "v1", Kind: "Widget"}
	// WidgetGVR is its resource.
	WidgetGVR = schema.GroupVersionResource{Group: Group, Version: "v1", Resource: "widgets"}
	// TargetGVK is a namespaced test kind that is protected, like core-provider's
	// KubernetesTarget (§9).
	TargetGVK = schema.GroupVersionKind{Group: Group, Version: "v1", Kind: "Target"}
	// TargetGVR is its resource.
	TargetGVR = schema.GroupVersionResource{Group: Group, Version: "v1", Resource: "targets"}
	// SecretGVR is the core Secret resource.
	SecretGVR = schema.GroupVersionResource{Version: "v1", Resource: "secrets"}
)

// An Env is a running envtest API server and admin clients for it.
type Env struct {
	Config   *rest.Config
	Scheme   *runtime.Scheme
	Client   client.Client
	Kube     kubernetes.Interface
	Metadata metadata.Interface
	Dynamic  dynamic.Interface

	env *envtest.Environment
}

// Start starts envtest with the Widget and Target CRDs. It returns a nil Env
// and no error when KUBEBUILDER_ASSETS is unset, so that a test binary runs
// its other tests; Require then fails or skips the tests that need it.
func Start() (*Env, error) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		return nil, nil
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	te := &envtest.Environment{
		CRDs: []*apiextensionsv1.CustomResourceDefinition{
			CRD("Widget", "widgets"),
			CRD("Target", "targets"),
		},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := te.Start()
	if err != nil {
		return nil, fmt.Errorf("envtest: %w", err)
	}
	e := &Env{Config: cfg, Scheme: scheme, env: te}
	if err := e.clients(); err != nil {
		_ = te.Stop()
		return nil, err
	}
	return e, nil
}

func (e *Env) clients() error {
	var err error
	if e.Client, err = client.New(e.Config, client.Options{Scheme: e.Scheme}); err != nil {
		return err
	}
	if e.Kube, err = kubernetes.NewForConfig(e.Config); err != nil {
		return err
	}
	if e.Metadata, err = metadata.NewForConfig(e.Config); err != nil {
		return err
	}
	e.Dynamic, err = dynamic.NewForConfig(e.Config)
	return err
}

// Stop stops envtest. A nil Env stops nothing.
func (e *Env) Stop() error {
	if e == nil {
		return nil
	}
	return e.env.Stop()
}

// Require skips a test that needs envtest when its binaries aren't there,
// except under CI, where a skip would hide that the test never ran.
func Require(t testing.TB, e *Env) {
	t.Helper()
	if e != nil {
		return
	}
	msg := "KUBEBUILDER_ASSETS is not set: this test needs envtest's kube-apiserver and etcd " +
		"(export KUBEBUILDER_ASSETS=$(setup-envtest use -p path 1.37.0))"
	if os.Getenv("CI") != "" {
		t.Fatal(msg)
	}
	t.Skip(msg)
}

// CRD builds a namespaced CRD in Group, served and stored at v1, whose
// objects take any fields.
func CRD(kind, plural string) *apiextensionsv1.CustomResourceDefinition {
	preserve := true
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: plural + "." + Group},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: Group,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Kind: kind, ListKind: kind + "List", Plural: plural, Singular: strings.ToLower(kind),
			},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: "v1", Served: true, Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: &preserve},
				},
			}},
		},
	}
}

var nsSeq atomic.Int64

// Namespace creates a namespace with a unique name that starts with prefix,
// and returns its name. It isn't deleted afterwards: envtest has no namespace
// controller, and the API server is thrown away with the test binary.
func (e *Env) Namespace(t testing.TB, prefix string) string {
	t.Helper()
	name := fmt.Sprintf("%s-%d", prefix, nsSeq.Add(1))
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if _, err := e.Kube.CoreV1().Namespaces().Create(context.Background(), ns, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating namespace %s: %v", name, err)
	}
	return name
}

// FinalizeNamespace plays the namespace controller's last step: it empties
// the namespace's spec.finalizers through the finalize subresource, then
// deletes it again so that the API server removes it. The caller has already
// deleted, or chosen to leave, its content.
func (e *Env) FinalizeNamespace(ctx context.Context, name string) error {
	ns, err := e.Kube.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	ns.Spec.Finalizers = nil
	if _, err := e.Kube.CoreV1().Namespaces().Finalize(ctx, ns, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("finalizing namespace %s: %w", name, err)
	}
	err = e.Kube.CoreV1().Namespaces().Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// As returns a copy of the admin config that impersonates user, with groups.
// Every authenticated user is also in system:authenticated.
func (e *Env) As(user string, groups ...string) *rest.Config {
	cfg := rest.CopyConfig(e.Config)
	cfg.Impersonate = rest.ImpersonationConfig{UserName: user, Groups: append([]string{"system:authenticated"}, groups...)}
	return cfg
}

// AsServiceAccount impersonates the service account namespace/name, with the
// groups the API server gives it.
func (e *Env) AsServiceAccount(namespace, name string) *rest.Config {
	return e.As("system:serviceaccount:"+namespace+":"+name, "system:serviceaccounts", "system:serviceaccounts:"+namespace)
}

// Eventually calls f until it returns nil or timeout passes, and then fails t
// with f's last error.
func Eventually(t testing.TB, timeout time.Duration, f func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := f()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after %s: %v", timeout, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// ErrNotYet is a convenience for Eventually's f.
var ErrNotYet = errors.New("not yet")
