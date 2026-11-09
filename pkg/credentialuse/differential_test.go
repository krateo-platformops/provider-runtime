package credentialuse_test

// The differential test: the same operations against envtest's API server and
// against the in-memory model, with the same outcomes required. The model's
// schedules are only as good as its semantics, and this is the check that its
// semantics are the API server's, for everything the protocol does.

import (
	"context"
	"errors"
	"fmt"
	"maps"
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
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
)

type backend interface {
	create(k apimodel.Key, m metav1.ObjectMeta) error
	get(k apimodel.Key) (*metav1.PartialObjectMetadata, error)
	patch(k apimodel.Key, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error)
	del(k apimodel.Key) error
}

type modelBackend struct{ m *apimodel.Model }

func (b modelBackend) create(k apimodel.Key, m metav1.ObjectMeta) error { return b.m.Create(k, m) }
func (b modelBackend) get(k apimodel.Key) (*metav1.PartialObjectMetadata, error) {
	return b.m.Get(k)
}
func (b modelBackend) patch(k apimodel.Key, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error) {
	return b.m.Patch(k, pt, data)
}
func (b modelBackend) del(k apimodel.Key) error { return b.m.Delete(k) }

type envBackend struct{ e *cutest.Env }

func gvrOf(gk schema.GroupKind) schema.GroupVersionResource {
	switch gk {
	case apimodel.NamespaceKind:
		return schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	case schema.GroupKind{Kind: "Secret"}:
		return cutest.SecretGVR
	case cutest.TargetGVK.GroupKind():
		return cutest.TargetGVR
	}
	panic("no resource for " + gk.String())
}

func (b envBackend) create(k apimodel.Key, m metav1.ObjectMeta) error {
	u := &unstructured.Unstructured{}
	gv := gvrOf(k.GK).GroupVersion()
	u.SetAPIVersion(gv.String())
	u.SetKind(k.GK.Kind)
	u.SetName(k.Name)
	u.SetNamespace(k.Namespace)
	u.SetFinalizers(m.Finalizers)
	u.SetLabels(m.Labels)
	u.SetAnnotations(m.Annotations)
	r := b.e.Dynamic.Resource(gvrOf(k.GK))
	var err error
	if k.Namespace == "" {
		_, err = r.Create(context.Background(), u, metav1.CreateOptions{})
	} else {
		_, err = r.Namespace(k.Namespace).Create(context.Background(), u, metav1.CreateOptions{})
	}
	return err
}

func (b envBackend) res(k apimodel.Key) interface {
	Get(context.Context, string, metav1.GetOptions, ...string) (*metav1.PartialObjectMetadata, error)
	Patch(context.Context, string, types.PatchType, []byte, metav1.PatchOptions, ...string) (*metav1.PartialObjectMetadata, error)
	Delete(context.Context, string, metav1.DeleteOptions, ...string) error
} {
	r := b.e.Metadata.Resource(gvrOf(k.GK))
	if k.Namespace == "" {
		return r
	}
	return r.Namespace(k.Namespace)
}

func (b envBackend) get(k apimodel.Key) (*metav1.PartialObjectMetadata, error) {
	return b.res(k).Get(context.Background(), k.Name, metav1.GetOptions{})
}
func (b envBackend) patch(k apimodel.Key, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error) {
	return b.res(k).Patch(context.Background(), k.Name, pt, data, metav1.PatchOptions{})
}
func (b envBackend) del(k apimodel.Key) error {
	return b.res(k).Delete(context.Background(), k.Name, metav1.DeleteOptions{})
}

// outcome is what one step did, in terms that don't depend on the backend.
type outcome struct {
	Class       string
	Code        int32
	Exists      bool
	Deleting    bool
	Finalizers  []string
	Labels      map[string]string
	Annotations map[string]string
}

func outcomeOf(err error, m *metav1.PartialObjectMetadata, jsonPatch bool) outcome {
	o := outcome{}
	if err != nil {
		if jsonPatch {
			o.Class = cu.ClassifyJSONPatch(err).String()
		} else {
			o.Class = cu.Classify(err).String()
		}
		var st apierrors.APIStatus
		if errors.As(err, &st) {
			o.Code = st.Status().Code
		}
		return o
	}
	o.Class = cu.ClassNone.String()
	if m != nil {
		o.Exists = true
		o.Deleting = m.DeletionTimestamp != nil
		o.Finalizers = slices.Sorted(slices.Values(m.Finalizers))
		o.Labels = maps.Clone(m.Labels)
		o.Annotations = maps.Clone(m.Annotations)
	}
	return o
}

// script runs the protocol's operations on b, and returns each outcome, by
// step. ns is a fresh namespace name.
func script(t *testing.T, b backend, ns string) map[string]outcome {
	t.Helper()
	got := map[string]outcome{}
	holder := schema.GroupKind{Group: "git.krateo.io", Kind: "Repo"}
	secret := apimodel.Key{GK: schema.GroupKind{Kind: "Secret"}, Namespace: ns, Name: "s"}
	target := apimodel.Key{GK: cutest.TargetGVK.GroupKind(), Namespace: ns, Name: "t"}
	step := func(name string, jsonPatch bool, f func() (*metav1.PartialObjectMetadata, error)) *metav1.PartialObjectMetadata {
		m, err := f()
		got[name] = outcomeOf(err, m, jsonPatch)
		return m
	}
	a2 := func(name, holderNS, epoch string) {
		p, err := cu.AcquireStrategicPatch(holder, holderNS, epoch)
		if err != nil {
			t.Fatal(err)
		}
		step(name, false, func() (*metav1.PartialObjectMetadata, error) { return b.patch(secret, p.Type, p.Data) })
	}
	read := func(k apimodel.Key) *metav1.PartialObjectMetadata {
		m, err := b.get(k)
		if err != nil {
			t.Fatalf("reading %s: %v", k, err)
		}
		return m
	}
	jp := func(name string, k apimodel.Key, build func(m *metav1.PartialObjectMetadata) (cu.Patch, error), from *metav1.PartialObjectMetadata) {
		p, err := build(from)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		step(name, true, func() (*metav1.PartialObjectMetadata, error) { return b.patch(k, p.Type, p.Data) })
	}

	step("create namespace", false, func() (*metav1.PartialObjectMetadata, error) {
		return nil, b.create(apimodel.Key{GK: apimodel.NamespaceKind, Name: ns}, metav1.ObjectMeta{})
	})
	step("create secret", false, func() (*metav1.PartialObjectMetadata, error) {
		return nil, b.create(secret, metav1.ObjectMeta{Finalizers: []string{"example.com/other"}, Labels: map[string]string{"app": "x"}})
	})
	a2("A2 for a", "team-a", "e1")
	a2("A2 for b", "team-b", "e2")
	a2("A2 for a again", "team-a", "e3")
	stale := read(secret)
	a2("A2 for b, moving its epoch", "team-b", "e4")
	jp("R3 for b from a stale read", secret, func(m *metav1.PartialObjectMetadata) (cu.Patch, error) { return cu.ReleasePatch(m, holder, "team-b") }, stale)
	jp("restore an epoch from a stale read", secret, func(m *metav1.PartialObjectMetadata) (cu.Patch, error) {
		return cu.RestoreEpochPatch(m, holder, "team-a", "e9")
	}, stale)
	jp("R3 for a", secret, func(m *metav1.PartialObjectMetadata) (cu.Patch, error) { return cu.ReleasePatch(m, holder, "team-a") }, read(secret))
	step("delete secret", false, func() (*metav1.PartialObjectMetadata, error) {
		if err := b.del(secret); err != nil {
			return nil, err
		}
		return b.get(secret)
	})
	a2("A2 for c on a deleting secret", "team-c", "e5")
	a2("A2 for b on a deleting secret it holds", "team-b", "e6")
	step("labels on a deleting secret", false, func() (*metav1.PartialObjectMetadata, error) {
		return b.patch(secret, types.MergePatchType, []byte(`{"metadata":{"labels":{"late":"1"}}}`))
	})
	jp("R3 for b, the last", secret, func(m *metav1.PartialObjectMetadata) (cu.Patch, error) { return cu.ReleasePatch(m, holder, "team-b") }, read(secret))
	step("remove the last finalizer", false, func() (*metav1.PartialObjectMetadata, error) {
		return b.patch(secret, types.JSONPatchType, []byte(`[{"op":"remove","path":"/metadata/finalizers/0"}]`))
	})
	step("get the removed secret", false, func() (*metav1.PartialObjectMetadata, error) { return b.get(secret) })

	step("create target", false, func() (*metav1.PartialObjectMetadata, error) {
		return nil, b.create(target, metav1.ObjectMeta{Finalizers: []string{"example.com/other"}})
	})
	staleT := read(target)
	smp, _ := cu.AcquireStrategicPatch(holder, ns, "e7")
	step("strategic merge on a custom resource", false, func() (*metav1.PartialObjectMetadata, error) {
		return b.patch(target, smp.Type, smp.Data)
	})
	step("bump target", false, func() (*metav1.PartialObjectMetadata, error) {
		return b.patch(target, types.MergePatchType, []byte(`{"metadata":{"labels":{"bump":"1"}}}`))
	})
	g, _ := cu.AcquireGuardedPatch(staleT, holder, ns, "e8")
	step("guarded A2 from a stale read", false, func() (*metav1.PartialObjectMetadata, error) { return b.patch(target, g.Type, g.Data) })
	g, _ = cu.AcquireGuardedPatch(read(target), holder, ns, "e8")
	step("guarded A2", false, func() (*metav1.PartialObjectMetadata, error) { return b.patch(target, g.Type, g.Data) })
	step("merge patch replaces the list", false, func() (*metav1.PartialObjectMetadata, error) {
		return b.patch(target, types.MergePatchType, []byte(`{"metadata":{"finalizers":["example.com/only"]}}`))
	})
	step("a no-op patch", false, func() (*metav1.PartialObjectMetadata, error) {
		before := read(target)
		after, err := b.patch(target, types.MergePatchType, []byte(`{"metadata":{"labels":{"bump":"1"}}}`))
		if err == nil && after.ResourceVersion != before.ResourceVersion {
			return nil, fmt.Errorf("a no-op patch moved the resourceVersion")
		}
		return after, err
	})

	other := apimodel.Key{GK: schema.GroupKind{Kind: "Secret"}, Namespace: ns, Name: "s2"}
	step("create s2", false, func() (*metav1.PartialObjectMetadata, error) { return nil, b.create(other, metav1.ObjectMeta{}) })
	step("delete namespace", false, func() (*metav1.PartialObjectMetadata, error) {
		return nil, b.del(apimodel.Key{GK: apimodel.NamespaceKind, Name: ns})
	})
	step("create in a terminating namespace", false, func() (*metav1.PartialObjectMetadata, error) {
		return nil, b.create(apimodel.Key{GK: schema.GroupKind{Kind: "Secret"}, Namespace: ns, Name: "late"}, metav1.ObjectMeta{})
	})
	p, _ := cu.AcquireStrategicPatch(holder, ns, "e10")
	step("A2 in a terminating namespace", false, func() (*metav1.PartialObjectMetadata, error) { return b.patch(other, p.Type, p.Data) })
	return got
}

func TestModelAgreesWithTheAPIServer(t *testing.T) {
	cutest.Require(t, env)
	ns := fmt.Sprintf("diff-%d", time.Now().UnixNano()%1_000_000)
	real := script(t, envBackend{env}, ns)
	clock := apimodel.Clock{}
	model := script(t, modelBackend{apimodel.New(clock.Now)}, ns)
	if len(real) != len(model) {
		t.Fatalf("%d steps against envtest, %d against the model", len(real), len(model))
	}
	for _, name := range slices.Sorted(maps.Keys(real)) {
		r, m := real[name], model[name]
		t.Logf("%s: %+v", name, r)
		// Codes differ only for an outcome with no class (none is expected).
		if fmt.Sprint(r) != fmt.Sprint(m) {
			t.Errorf("%s:\n envtest %+v\n   model %+v", name, r, m)
		}
	}
	// The script must have produced each class it is about.
	want := map[string]bool{"Deleting": false, "StateChanged": false, "Conflict": false, "AdmissionDenied": false, "NotFound": false}
	for _, o := range real {
		if _, ok := want[o.Class]; ok {
			want[o.Class] = true
		}
	}
	for c, seen := range want {
		if !seen {
			t.Errorf("no step produced %s", c)
		}
	}
	if real["strategic merge on a custom resource"].Code != http.StatusUnsupportedMediaType {
		t.Errorf("strategic merge on a custom resource: %+v, want 415", real["strategic merge on a custom resource"])
	}
}
