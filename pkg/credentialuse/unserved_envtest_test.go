package credentialuse_test

// A 404 for a resource the API server doesn't serve (§6.2): what the API
// server and client-go answer, and what a tracker does when a protected kind
// stops being served while it runs.

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
)

// Multis are served at v2 and v1, and stored at v1; the mapper prefers v2,
// which a test stops serving.
var (
	multiGVK = schema.GroupVersionKind{Group: cutest.Group, Version: "v2", Kind: "Multi"}
	multiV2  = schema.GroupVersionResource{Group: cutest.Group, Version: "v2", Resource: "multis"}
	multiV1  = schema.GroupVersionResource{Group: cutest.Group, Version: "v1", Resource: "multis"}
)

// multiCRD creates the Multi CRD, and removes it when t ends. serve sets
// whether v2 is served, and waits until the API server does as it says.
func multiCRD(t *testing.T) (serve func(bool)) {
	t.Helper()
	ctx := context.Background()
	crd := cutest.CRD("Multi", "multis")
	v2 := *crd.Spec.Versions[0].DeepCopy()
	v2.Name, v2.Storage = "v2", false
	crd.Spec.Versions = append(crd.Spec.Versions, v2)
	cutest.Eventually(t, 30*time.Second, func() error { return env.Client.Create(ctx, crd.DeepCopy()) })
	t.Cleanup(func() {
		// A test that failed may leave a finalizer, which would keep the CRD.
		if l, err := env.Dynamic.Resource(multiV1).List(ctx, metav1.ListOptions{}); err == nil {
			for i := range l.Items {
				_, _ = env.Dynamic.Resource(multiV1).Namespace(l.Items[i].GetNamespace()).Patch(ctx, l.Items[i].GetName(),
					types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{})
			}
		}
		_ = env.Client.Delete(ctx, crd)
		cutest.Eventually(t, 30*time.Second, func() error {
			if _, err := env.Dynamic.Resource(crdGVR).Get(ctx, crd.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				return fmt.Errorf("the Multi CRD is still there (%v): %w", err, cutest.ErrNotYet)
			}
			return nil
		})
	})
	served := func(want bool) {
		t.Helper()
		cutest.Eventually(t, 30*time.Second, func() error {
			if _, err := env.Metadata.Resource(multiV2).List(ctx, metav1.ListOptions{}); (err == nil) != want {
				return fmt.Errorf("listing Multis at v2: %v: %w", err, cutest.ErrNotYet)
			}
			return nil
		})
	}
	served(true)
	return func(on bool) {
		t.Helper()
		p := fmt.Sprintf(`[{"op":"replace","path":"/spec/versions/1/served","value":%v}]`, on)
		if _, err := env.Dynamic.Resource(crdGVR).Patch(ctx, crd.Name, types.JSONPatchType, []byte(p), metav1.PatchOptions{}); err != nil {
			t.Fatal(err)
		}
		served(on)
	}
}

func createMulti(t *testing.T, ns, name string) {
	t.Helper()
	u := &unstructured.Unstructured{Object: map[string]any{}}
	u.SetGroupVersionKind(multiV1.GroupVersion().WithKind("Multi"))
	u.SetNamespace(ns)
	u.SetName(name)
	if _, err := env.Dynamic.Resource(multiV1).Namespace(ns).Create(context.Background(), u, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

// P19: a version the CRD no longer serves, though the object exists, and a
// CRD that is gone, are a 404 with client-go's UnexpectedServerResponse cause,
// for every call the core makes; a missing object's 404 has none.
func TestPremiseUnservedResource(t *testing.T) {
	cutest.Require(t, env)
	ctx := ctxFor(t)
	ns := env.Namespace(t, "unserved-premise")
	serve := multiCRD(t)
	createMulti(t, ns, "m")
	m, d := env.Metadata.Resource(multiV2).Namespace(ns), env.Dynamic.Resource(multiV2).Namespace(ns)
	calls := map[string]func(name string) error{
		"get": func(n string) error { _, err := m.Get(ctx, n, metav1.GetOptions{}); return err },
		"merge patch": func(n string) error {
			_, err := m.Patch(ctx, n, types.MergePatchType, []byte(`{"metadata":{"labels":{"a":"b"}}}`), metav1.PatchOptions{})
			return err
		},
		"JSON patch": func(n string) error {
			_, err := m.Patch(ctx, n, types.JSONPatchType, []byte(`[{"op":"test","path":"/metadata/name","value":"x"}]`), metav1.PatchOptions{})
			return err
		},
		"get, whole objects":  func(n string) error { _, err := d.Get(ctx, n, metav1.GetOptions{}); return err },
		"list":                func(string) error { _, err := m.List(ctx, metav1.ListOptions{}); return err },
		"list, whole objects": func(string) error { _, err := d.List(ctx, metav1.ListOptions{}); return err },
		"watch": func(string) error {
			w, err := m.Watch(ctx, metav1.ListOptions{})
			if err == nil {
				w.Stop()
			}
			return err
		},
	}
	check := func(what string) {
		t.Helper()
		for name, call := range calls {
			err := call("m")
			if err == nil {
				t.Errorf("%s: %s succeeded", what, name)
				continue
			}
			st := statusOf(t, err)
			if st.Code != http.StatusNotFound || st.Details == nil || !slices.ContainsFunc(st.Details.Causes, func(c metav1.StatusCause) bool {
				return c.Type == metav1.CauseTypeUnexpectedServerResponse
			}) {
				t.Errorf("%s: %s: %+v, want a 404 with the UnexpectedServerResponse cause", what, name, st)
			}
			if c, j := cu.Classify(err), cu.ClassifyJSONPatch(err); c != cu.ClassOther || j != cu.ClassOther {
				t.Errorf("%s: %s: Classify %v, ClassifyJSONPatch %v, want Other", what, name, c, j)
			}
		}
	}
	for _, name := range []string{"get", "merge patch", "JSON patch", "get, whole objects"} {
		err := calls[name]("missing")
		if c := cu.Classify(err); c != cu.ClassNotFound {
			t.Errorf("%s of a missing object: Classify(%v) = %v, want NotFound", name, err, c)
		}
	}
	serve(false)
	check("v2 no longer served")
	if err := env.Dynamic.Resource(crdGVR).Delete(ctx, "multis."+cutest.Group, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	cutest.Eventually(t, 30*time.Second, func() error {
		if _, err := env.Metadata.Resource(multiV1).List(ctx, metav1.ListOptions{}); err == nil {
			return fmt.Errorf("Multis are still served: %w", cutest.ErrNotYet)
		}
		return nil
	})
	m, d = env.Metadata.Resource(multiV1).Namespace(ns), env.Dynamic.Resource(multiV1).Namespace(ns)
	check("the CRD gone")
}

// A protected kind that stops being served while the process runs (PerName):
// what it holds is unknown, not gone. A new holder doesn't connect; a hold
// whose holder goes keeps its line; and Disown's count is unknown, never zero,
// while an object may carry the finalizer. Served again, everything goes on.
func TestAProtectedKindThatStopsBeingServed(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	set := disowning(t)
	idx, ns := indexNamespace(t), env.Namespace(t, "unserved")
	serve := multiCRD(t)
	for _, n := range []string{"m", "m2"} {
		createMulti(t, ns, n)
		createSecret(t, ns, n) // what the driver's Connect reads
	}
	multi := func(obj metav1.Object) (cu.Ref, bool) {
		name, _, _ := unstructured.NestedString(obj.(*unstructured.Unstructured).Object, "spec", "secret")
		return cu.Ref{GroupKind: multiGVK.GroupKind(), Namespace: obj.GetNamespace(), Name: name}, name != ""
	}
	r := perNameRig(t, idx, func(o *cu.Options) {
		disownOptions(o)
		o.Protected = []cu.ProtectedKind{{GVK: multiGVK, Watch: cu.PerName}}
		o.Refs = func(obj metav1.Object) ([]cu.Ref, error) {
			if ref, ok := multi(obj); ok {
				return []cu.Ref{ref}, nil
			}
			return nil, nil
		}
	}, nil)
	f, _ := cu.FinalizerName(widgetGK, ns)
	carries := func(name string) bool {
		u, err := env.Dynamic.Resource(multiV1).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
		return err == nil && slices.Contains(u.GetFinalizers(), f)
	}
	line := cu.ID{Group: cutest.Group, Kind: "Multi", Namespace: ns, Name: "m"}.String()
	createWidget(t, ns, "w", "m")
	r.c.enqueue(widgetNN(ns, "w"))
	cutest.Eventually(t, time.Minute, func() error {
		w, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(ctx, "w", metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !carries("m") || !slices.Contains(w.GetFinalizers(), ownerFinalizer) {
			return fmt.Errorf("w isn't established, or m isn't held: %w", cutest.ErrNotYet)
		}
		return nil
	})
	lined(t, idx, line, true)

	serve(false)
	createWidget(t, ns, "w2", "m2")
	r.c.enqueue(widgetNN(ns, "w2"))
	cutest.Eventually(t, 30*time.Second, func() error {
		if r.c.passes.Load() == 0 || len(r.c.lastResult(widgetNN(ns, "w2")).Acquired.Unheld) == 0 && r.c.lastResult(widgetNN(ns, "w2")).Acquired.Err == nil {
			return fmt.Errorf("w2 has had no pass: %w", cutest.ErrNotYet)
		}
		return nil
	})
	if res := r.c.lastResult(widgetNN(ns, "w2")); !res.Acquired.MustNotConnect || res.Connected {
		t.Errorf("a new holder of an unserved kind: MustNotConnect %v, connected %v, unheld %v; want it not to connect", res.Acquired.MustNotConnect, res.Connected, res.Acquired.Unheld)
	}
	deleteWidget(t, ns, "w")
	r.c.enqueue(widgetNN(ns, "w"))
	gone(t, 30*time.Second, "w", widgetGet(ns, "w"))
	counts := watchHeldCount(t)
	set("all")
	time.Sleep(6 * time.Second) // the batch's retries, and six passes of Disown
	if lines, _ := indexLines(t, idx); !slices.Contains(lines, line) {
		t.Errorf("the index is %q: m's line went, though m carries the finalizer", lines)
	}
	if !carries("m") {
		t.Fatal("m lost the finalizer while it couldn't be patched")
	}
	if a := widgetCRDAnnotations(t); a[cu.AnnotationHeldCount] != "unknown" || a[cu.AnnotationDisownSettled] != "" {
		t.Errorf("held-count %q, disown-settled %q; want the count unknown (it was %v)", a[cu.AnnotationHeldCount], a[cu.AnnotationDisownSettled], counts())
	}
	eventSeen(t, r, cu.EventDisownUnreadable, "isn't served")

	serve(true)
	cutest.Eventually(t, time.Minute, settledAtZero(t))
	if carries("m") {
		t.Error("Disown settled at zero while m carries the finalizer")
	}
	lined(t, idx, line, false)
}
