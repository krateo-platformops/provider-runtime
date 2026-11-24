package credentialuse_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
)

var scopeGVR = schema.GroupVersionResource{Group: cutest.Group, Version: "v1", Resource: "scopewidgets"}

// scopeCRD creates and removes the ScopeWidget CRD, whose scope each creation
// chooses, and cleans it up. In is a namespace its holders live in.
type scopeCRD struct {
	t  *testing.T
	in string
}

func newScopeCRD(t *testing.T, in string) scopeCRD {
	t.Helper()
	c := scopeCRD{t, in}
	t.Cleanup(c.remove)
	return c
}

func (c scopeCRD) kind() schema.GroupVersionKind {
	return scopeGVR.GroupVersion().WithKind("ScopeWidget")
}

func (c scopeCRD) create(scope apiextensionsv1.ResourceScope) {
	c.t.Helper()
	ctx := context.Background()
	crd := cutest.CRD("ScopeWidget", scopeGVR.Resource)
	crd.Spec.Scope = scope
	cutest.Eventually(c.t, 30*time.Second, func() error { return env.Client.Create(ctx, crd.DeepCopy()) })
	cutest.Eventually(c.t, 30*time.Second, func() error {
		_, err := env.Dynamic.Resource(scopeGVR).List(ctx, metav1.ListOptions{})
		if err == nil && scope == apiextensionsv1.NamespaceScoped {
			_, err = env.Dynamic.Resource(scopeGVR).Namespace(c.in).List(ctx, metav1.ListOptions{})
		}
		return err
	})
}

func (c scopeCRD) remove() {
	c.t.Helper()
	ctx := context.Background()
	if l, err := env.Dynamic.Resource(scopeGVR).List(ctx, metav1.ListOptions{}); err == nil {
		for i := range l.Items {
			_, _ = c.at(l.Items[i].GetNamespace()).Patch(ctx, l.Items[i].GetName(), types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{})
		}
	}
	name := scopeGVR.Resource + "." + scopeGVR.Group
	_ = env.Dynamic.Resource(crdGVR).Delete(ctx, name, metav1.DeleteOptions{})
	cutest.Eventually(c.t, 30*time.Second, func() error {
		if _, err := env.Dynamic.Resource(crdGVR).Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			return fmt.Errorf("the CRD is still there (%v): %w", err, cutest.ErrNotYet)
		}
		return nil
	})
}

func (c scopeCRD) at(ns string) dynamic.ResourceInterface {
	if ns == "" {
		return env.Dynamic.Resource(scopeGVR)
	}
	return env.Dynamic.Resource(scopeGVR).Namespace(ns)
}

// holder creates an established holder, whose record names ref.
func (c scopeCRD) holder(ns, name string, ref cu.Ref) {
	c.t.Helper()
	rec, err := cu.EncodeRecord([]cu.ID{ref.ID()})
	if err != nil {
		c.t.Fatal(err)
	}
	u := &unstructured.Unstructured{Object: map[string]any{}}
	u.SetGroupVersionKind(c.kind())
	u.SetNamespace(ns)
	u.SetName(name)
	u.SetFinalizers([]string{ownerFinalizer})
	u.SetAnnotations(map[string]string{cu.AnnotationRefs: rec})
	cutest.Eventually(c.t, 30*time.Second, func() error {
		_, err := c.at(ns).Create(context.Background(), u, metav1.CreateOptions{})
		return err
	})
}

// track tracks the kind, with the mapping a fresh mapper gives now.
func (c scopeCRD) track(so cu.SharedOptions) (*cu.Shared, *cu.Tracker) {
	c.t.Helper()
	s := cu.NewShared(clientsFor(c.t, env.Config), so)
	o := widgetOptions(func(types.NamespacedName) {})
	o.Holder = c.kind()
	tr, err := s.Track(context.Background(), o)
	if err != nil {
		c.t.Fatal(err)
	}
	return s, tr
}

// gone waits for tr's list in ns to give the API server's 404.
func (c scopeCRD) gone(tr *cu.Tracker, ref cu.Ref, ns string) {
	c.t.Helper()
	cutest.Eventually(c.t, 30*time.Second, func() error {
		_, err := tr.Users(context.Background(), ref, ns)
		var st apierrors.APIStatus
		if !errors.As(err, &st) || st.Status().Code != 404 {
			return fmt.Errorf("Users in %q = %v, want the 404: %w", ns, err, cutest.ErrNotYet)
		}
		return nil
	})
}

// A holder kind that returns namespaced, under a process that mapped it
// cluster-scoped (§6.2): Users' list, which is R2's, and the release itself. Not
// checked: an API server that still serves the deleted CRD.
func TestAHolderKindThatReturnsNamespaced(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	a, b, cached := env.Namespace(t, "scope"), env.Namespace(t, "scope"), env.Namespace(t, "scope")
	crd := newScopeCRD(t, a)
	crd.create(apiextensionsv1.ClusterScoped)
	// The Secrets of a are outside its caches, so R0 decides nothing of them,
	// and every batch reads the holders live.
	s, old := crd.track(cu.SharedOptions{Namespaces: []string{cached}})
	ref := cu.SecretRef(a, "a-used")
	if u, err := old.Users(ctx, ref, ""); err != nil || len(u) != 0 {
		t.Fatalf("before: Users = %v, %v", u, err)
	}
	crd.remove()
	crd.gone(old, ref, a)
	crd.create(apiextensionsv1.NamespaceScoped)
	for _, ns := range []string{a, b} {
		crd.holder(ns, "h", ref)
	}
	_, now := crd.track(cu.SharedOptions{})
	for who, tr := range map[string]*cu.Tracker{"the process that mapped it before": old, "one that maps it now": now} {
		for _, ns := range []string{a, b} {
			got, err := tr.Users(ctx, ref, ns)
			if err != nil || len(got) != 1 || got[0] != (types.NamespacedName{Namespace: ns, Name: "h"}) {
				t.Errorf("%s: Users in %s = %v, %v; want the one there", who, ns, got, err)
			}
		}
		if got, err := tr.Users(ctx, ref, cached); err != nil || len(got) != 0 {
			t.Errorf("%s: Users in %s = %v, %v; want none", who, cached, got, err)
		}
	}

	// The release loop of the old process: what has a user stays, in the batch
	// that releases what has none. The used one is the first of the batch, so a
	// release of it would be there when the other's is.
	holdSecret(t, crd.kind().GroupKind(), a, "a-used")
	holdSecret(t, crd.kind().GroupKind(), a, "b-unused")
	running(t, s)
	cutest.Eventually(t, 60*time.Second, func() error {
		if f := finalizersOf(t, a, "b-unused"); len(f) != 0 {
			return fmt.Errorf("the unused Secret's finalizers are %v: %w", f, cutest.ErrNotYet)
		}
		return nil
	})
	if f := finalizersOf(t, a, "a-used"); len(f) != 1 {
		t.Fatalf("the used Secret's finalizers are %v, want its hold", f)
	}
	// Its user gone, it is released.
	if _, err := crd.at(a).Patch(ctx, "h", types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := crd.at(a).Delete(ctx, "h", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	cutest.Eventually(t, 60*time.Second, func() error {
		if f := finalizersOf(t, a, "a-used"); len(f) != 0 {
			return fmt.Errorf("the used Secret's finalizers are %v: %w", f, cutest.ErrNotYet)
		}
		return nil
	})
}

// The reverse: one that returns cluster-scoped, under a process that mapped it
// namespaced. A list in a namespace is the API server's 404, never "none"; one
// without a namespace has every holder.
func TestAHolderKindThatReturnsClusterScoped(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	a := env.Namespace(t, "scope")
	crd := newScopeCRD(t, a)
	crd.create(apiextensionsv1.NamespaceScoped)
	_, old := crd.track(cu.SharedOptions{})
	ref := cu.SecretRef(a, "s")
	if u, err := old.Users(ctx, ref, a); err != nil || len(u) != 0 {
		t.Fatalf("before: Users = %v, %v", u, err)
	}
	crd.remove()
	crd.gone(old, ref, a)
	crd.create(apiextensionsv1.ClusterScoped)
	crd.holder("", "h", ref)
	_, now := crd.track(cu.SharedOptions{})
	for who, tr := range map[string]*cu.Tracker{"the process that mapped it before": old, "one that maps it now": now} {
		if got, err := tr.Users(ctx, ref, ""); err != nil || len(got) != 1 || got[0] != (types.NamespacedName{Name: "h"}) {
			t.Errorf("%s: Users = %v, %v; want the holder", who, got, err)
		}
	}
	crd.gone(old, ref, a)
	if got, err := now.Users(ctx, ref, a); err != nil || len(got) != 0 {
		t.Errorf("one that maps it now: Users in %s = %v, %v; want none", a, got, err)
	}
}
