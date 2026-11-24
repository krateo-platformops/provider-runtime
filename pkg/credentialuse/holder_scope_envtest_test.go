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

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
)

// A holder kind that returns with another scope, under a process that mapped it
// before (§6.2): its list of the holders in a namespace is R2's, and Users'.
// Not checked: an API server that still serves the deleted CRD.
func TestAHolderKindThatReturnsNamespaced(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	gvr := schema.GroupVersionResource{Group: cutest.Group, Version: "v1", Resource: "scopewidgets"}
	gvk := gvr.GroupVersion().WithKind("ScopeWidget")
	a, b := env.Namespace(t, "scope"), env.Namespace(t, "scope")
	create := func(scope apiextensionsv1.ResourceScope) {
		t.Helper()
		crd := cutest.CRD(gvk.Kind, gvr.Resource)
		crd.Spec.Scope = scope
		cutest.Eventually(t, 30*time.Second, func() error { return env.Client.Create(ctx, crd.DeepCopy()) })
		cutest.Eventually(t, 30*time.Second, func() error {
			_, err := env.Dynamic.Resource(gvr).List(ctx, metav1.ListOptions{})
			if err == nil && scope == apiextensionsv1.NamespaceScoped {
				_, err = env.Dynamic.Resource(gvr).Namespace(a).List(ctx, metav1.ListOptions{})
			}
			return err
		})
	}
	name := gvr.Resource + "." + gvr.Group
	remove := func() {
		for _, ns := range []string{a, b} {
			_, _ = env.Dynamic.Resource(gvr).Namespace(ns).Patch(ctx, "h", types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{})
		}
		_ = env.Dynamic.Resource(crdGVR).Delete(ctx, name, metav1.DeleteOptions{})
		cutest.Eventually(t, 30*time.Second, func() error {
			if _, err := env.Dynamic.Resource(crdGVR).Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				return fmt.Errorf("the CRD is still there (%v): %w", err, cutest.ErrNotYet)
			}
			return nil
		})
	}
	t.Cleanup(remove)
	track := func() *cu.Tracker {
		t.Helper()
		s := cu.NewShared(clientsFor(t, env.Config), cu.SharedOptions{})
		o := widgetOptions(func(types.NamespacedName) {})
		o.Holder = gvk
		tr, err := s.Track(ctx, o)
		if err != nil {
			t.Fatal(err)
		}
		return tr
	}
	create(apiextensionsv1.ClusterScoped)
	old := track()
	ref := cu.SecretRef(a, "s")
	if u, err := old.Users(ctx, ref, ""); err != nil || len(u) != 0 {
		t.Fatalf("before: Users = %v, %v", u, err)
	}
	remove()
	cutest.Eventually(t, 30*time.Second, func() error {
		if _, err := old.Users(ctx, ref, a); err == nil {
			return fmt.Errorf("the kind is gone, and its users are known to be none: %w", cutest.ErrNotYet)
		} else if st := apierrors.APIStatus(nil); !errors.As(err, &st) || st.Status().Code != 404 {
			return fmt.Errorf("the kind is gone, and Users = %w; want its 404", err)
		}
		return nil
	})
	create(apiextensionsv1.NamespaceScoped)
	rec, err := cu.EncodeRecord([]cu.ID{ref.ID()})
	if err != nil {
		t.Fatal(err)
	}
	for _, ns := range []string{a, b} {
		u := &unstructured.Unstructured{Object: map[string]any{}}
		u.SetGroupVersionKind(gvk)
		u.SetNamespace(ns)
		u.SetName("h")
		u.SetFinalizers([]string{ownerFinalizer})
		u.SetAnnotations(map[string]string{cu.AnnotationRefs: rec})
		cutest.Eventually(t, 30*time.Second, func() error {
			_, err := env.Dynamic.Resource(gvr).Namespace(ns).Create(ctx, u, metav1.CreateOptions{})
			return err
		})
	}
	for who, tr := range map[string]*cu.Tracker{"the process that mapped it before": old, "one that maps it now": track()} {
		for _, ns := range []string{a, b} {
			got, err := tr.Users(ctx, ref, ns)
			if err != nil || len(got) != 1 || got[0] != (types.NamespacedName{Namespace: ns, Name: "h"}) {
				t.Errorf("%s: Users in %s = %v, %v; want the one there", who, ns, got, err)
			}
		}
		if got, err := tr.Users(ctx, ref, "default"); err != nil || len(got) != 0 {
			t.Errorf("%s: Users in default = %v, %v; want none", who, got, err)
		}
	}
}
