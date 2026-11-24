package credentialuse_test

// What a metadata-only patch does to the rest of a custom resource whose CRD
// serves several versions (§6.2). A2 and R3 patch a protected custom resource,
// and A1 a holder, through the mapper's version for the kind, whatever
// version its owner writes. The API server decodes the stored object at the
// request's version, and encodes it back at the storage version: a field that
// either version's schema lacks, and no conversion keeps, is pruned, though
// the patch doesn't touch it.

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
)

const pruneGroup = "prune.credentialuse.test.krateo.io"

// pruneVersions are the CRDs' versions; the API server prefers v1, and so
// does the mapper of these tests.
var pruneVersions = []string{"v1", "v1beta1"}

// onlyIn is the field that only version v's schema has.
func onlyIn(v string) string { return "only" + v }

// pruneCRD is a CRD of kind with both versions, each with spec.both and a
// field of its own, stored at storage.
func pruneCRD(kind, storage string, conversion *apiextensionsv1.CustomResourceConversion) *apiextensionsv1.CustomResourceDefinition {
	crd := cutest.CRD(kind, strings.ToLower(kind)+"s")
	crd.Name = strings.ToLower(kind) + "s." + pruneGroup
	crd.Spec.Group, crd.Spec.Conversion, crd.Spec.Versions = pruneGroup, conversion, nil
	str := apiextensionsv1.JSONSchemaProps{Type: "string"}
	for _, v := range pruneVersions {
		crd.Spec.Versions = append(crd.Spec.Versions, apiextensionsv1.CustomResourceDefinitionVersion{
			Name: v, Served: true, Storage: v == storage,
			Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{"spec": {
					Type:       "object",
					Properties: map[string]apiextensionsv1.JSONSchemaProps{"both": str, onlyIn(v): str},
				}},
			}},
		})
	}
	return crd
}

// roundTrips is a conversion webhook that loses nothing: it keeps the other
// version's field in an annotation. Then each of edits, if any, changes what
// it answers, or fails it.
func roundTrips(t *testing.T, edits ...func(u *unstructured.Unstructured) error) *apiextensionsv1.CustomResourceConversion {
	t.Helper()
	const kept = "prune.test/"
	convert := func(raw []byte, to string) ([]byte, error) {
		u := &unstructured.Unstructured{}
		if err := u.UnmarshalJSON(raw); err != nil {
			return nil, err
		}
		from := u.GroupVersionKind().Version
		u.SetAPIVersion(to)
		_, toVersion, _ := strings.Cut(to, "/")
		if from == toVersion {
			return u.MarshalJSON()
		}
		an := u.GetAnnotations()
		if an == nil {
			an = map[string]string{}
		}
		if v, ok, _ := unstructured.NestedString(u.Object, "spec", onlyIn(from)); ok {
			an[kept+onlyIn(from)] = v
			unstructured.RemoveNestedField(u.Object, "spec", onlyIn(from))
		}
		if v, ok := an[kept+onlyIn(toVersion)]; ok {
			_ = unstructured.SetNestedField(u.Object, v, "spec", onlyIn(toVersion))
			delete(an, kept+onlyIn(toVersion))
		}
		u.SetAnnotations(an)
		for _, edit := range edits {
			if err := edit(u); err != nil {
				return nil, err
			}
		}
		return u.MarshalJSON()
	}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var review apiextensionsv1.ConversionReview
		if err := json.NewDecoder(r.Body).Decode(&review); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		res := &apiextensionsv1.ConversionResponse{UID: review.Request.UID, Result: metav1.Status{Status: metav1.StatusSuccess}}
		for _, o := range review.Request.Objects {
			out, err := convert(o.Raw, review.Request.DesiredAPIVersion)
			if err != nil {
				res.Result = metav1.Status{Status: metav1.StatusFailure, Message: err.Error()}
				break
			}
			res.ConvertedObjects = append(res.ConvertedObjects, runtime.RawExtension{Raw: out})
		}
		review.Response, review.Request = res, nil
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(review)
	}))
	t.Cleanup(srv.Close)
	return &apiextensionsv1.CustomResourceConversion{
		Strategy: apiextensionsv1.WebhookConverter,
		Webhook: &apiextensionsv1.WebhookConversion{
			ConversionReviewVersions: []string{"v1"},
			ClientConfig: &apiextensionsv1.WebhookClientConfig{
				URL:      ptr.To(srv.URL),
				CABundle: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}),
			},
		},
	}
}

// A pruneRig is one CRD and one object of it, of which the storage version's
// own field is set.
type pruneRig struct {
	t       *testing.T
	kind    string
	ns      string
	storage string
}

func (p pruneRig) gvr(v string) schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: pruneGroup, Version: v, Resource: strings.ToLower(p.kind) + "s"}
}

// pruneSeq numbers the rigs' kinds.
var pruneSeq atomic.Int64

// newPruneRig creates a CRD of its own, whose kind is kind with a number, so
// that a test can run again in one process. The CRD stays, as the rig's namespace
// does: removing it would need the finalizers that tests leave on its objects
// stripped, and those objects read through a conversion that some refuse.
func newPruneRig(t *testing.T, kind, storage string, conversion *apiextensionsv1.CustomResourceConversion) pruneRig {
	t.Helper()
	ctx := context.Background()
	kind = fmt.Sprintf("%sN%d", kind, pruneSeq.Add(1))
	if err := env.Client.Create(ctx, pruneCRD(kind, storage, conversion)); err != nil {
		t.Fatal(err)
	}
	p := pruneRig{t: t, kind: kind, ns: env.Namespace(t, "prune"), storage: storage}
	for _, v := range pruneVersions {
		cutest.Eventually(t, time.Minute, func() error {
			_, err := env.Dynamic.Resource(p.gvr(v)).Namespace(p.ns).List(ctx, metav1.ListOptions{})
			return err
		})
	}
	return p
}

func (p pruneRig) create(name string) {
	p.t.Helper()
	u := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"both": "x", onlyIn(p.storage): "y"}}}
	u.SetAPIVersion(pruneGroup + "/" + p.storage)
	u.SetKind(p.kind)
	u.SetName(name)
	u.SetNamespace(p.ns)
	if _, err := env.Dynamic.Resource(p.gvr(p.storage)).Namespace(p.ns).Create(context.Background(), u, metav1.CreateOptions{}); err != nil {
		p.t.Fatal(err)
	}
	if !p.kept(name) {
		p.t.Fatalf("precondition: %s of %s is lost at its creation", onlyIn(p.storage), name)
	}
}

// kept reports whether the object still has the field it was created with,
// read at the version it was created at.
func (p pruneRig) kept(name string) bool {
	p.t.Helper()
	u, err := env.Dynamic.Resource(p.gvr(p.storage)).Namespace(p.ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		p.t.Fatal(err)
	}
	if both, _, _ := unstructured.NestedString(u.Object, "spec", "both"); both != "x" {
		p.t.Fatalf("spec.both of %s is %q", name, both)
	}
	_, ok, _ := unstructured.NestedString(u.Object, "spec", onlyIn(p.storage))
	return ok
}

// patches are the package's own writes of a custom resource, through version
// v: A2's, R3's, and the janitor's are guarded merge and JSON patches of
// metadata, sent by the metadata client.
func (p pruneRig) patches() map[string]func(name, v string) {
	ctx := context.Background()
	get := func(name, v string) *metav1.PartialObjectMetadata {
		p.t.Helper()
		m, err := env.Metadata.Resource(p.gvr(v)).Namespace(p.ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			p.t.Fatal(err)
		}
		return m
	}
	apply := func(name, v string, pt cu.Patch, err error) {
		p.t.Helper()
		if err == nil {
			_, err = send(ctx, env.Metadata.Resource(p.gvr(v)).Namespace(p.ns), name, pt)
		}
		if err != nil {
			p.t.Fatal(err)
		}
	}
	a2 := func(name, v string) {
		p.t.Helper()
		pt, err := cu.AcquireGuardedPatch(get(name, v), widgetGK, p.ns, "e")
		apply(name, v, pt, err)
	}
	return map[string]func(name, v string){
		"A2": a2,
		"R3": func(name, v string) {
			p.t.Helper()
			a2(name, p.storage) // held through the storage version, which prunes nothing
			pt, err := cu.ReleasePatch(get(name, v), widgetGK, p.ns)
			apply(name, v, pt, err)
		},
		"a merge patch of one label": func(name, v string) {
			p.t.Helper()
			apply(name, v, cu.Patch{Type: types.MergePatchType, Data: []byte(`{"metadata":{"labels":{"a":"b"}}}`)}, nil)
		},
	}
}

func TestPremisePatchThroughAnotherVersion(t *testing.T) {
	cutest.Require(t, env)
	n := 0
	for _, webhook := range []bool{false, true} {
		for _, storage := range pruneVersions {
			conversion := "None"
			if webhook {
				conversion = "a webhook that round-trips"
			}
			t.Run(fmt.Sprintf("%s, stored at %s", conversion, storage), func(t *testing.T) {
				var conv *apiextensionsv1.CustomResourceConversion
				if webhook {
					conv = roundTrips(t)
				}
				n++
				p := newPruneRig(t, fmt.Sprintf("Prune%d", n), storage, conv)
				for what, patch := range p.patches() {
					for _, v := range pruneVersions {
						name := strings.ToLower(strings.ReplaceAll(what, " ", "-")) + "-" + v
						p.create(name)
						patch(name, v)
						kept, want := p.kept(name), webhook || v == storage
						t.Logf("PRUNE conversion %s, stored at %s, %s through %s: %s kept: %v", conversion, storage, what, v, onlyIn(storage), kept)
						if kept != want {
							t.Errorf("%s through %s: %s kept = %v, want %v", what, v, onlyIn(storage), kept, want)
						}
					}
				}
			})
		}
	}
}

// Once the storage version has moved on, no version keeps the field of an
// object still stored at the old one, if the new one's schema lacks it: any
// write of any client prunes it.
func TestPremisePatchAfterTheStorageVersionMoved(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	p := newPruneRig(t, "PruneMoved", "v1beta1", nil)
	for _, v := range pruneVersions {
		p.create("through-" + v)
	}
	crd := &apiextensionsv1.CustomResourceDefinition{}
	crd.Name = p.gvr("v1").GroupResource().String()
	cutest.Eventually(t, time.Minute, func() error {
		if err := env.Client.Get(ctx, types.NamespacedName{Name: crd.Name}, crd); err != nil {
			return err
		}
		for i := range crd.Spec.Versions {
			crd.Spec.Versions[i].Storage = crd.Spec.Versions[i].Name == "v1"
		}
		return env.Client.Update(ctx, crd)
	})
	// The API server serves with the new storage version once an object it
	// creates loses v1beta1's field, for v1 is what it stores.
	probe := 0
	cutest.Eventually(t, time.Minute, func() error {
		probe++
		name := fmt.Sprintf("probe%d", probe)
		u := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"both": "x", onlyIn("v1beta1"): "y"}}}
		u.SetAPIVersion(pruneGroup + "/v1beta1")
		u.SetKind(p.kind)
		u.SetName(name)
		u.SetNamespace(p.ns)
		if _, err := env.Dynamic.Resource(p.gvr("v1beta1")).Namespace(p.ns).Create(ctx, u, metav1.CreateOptions{}); err != nil {
			return err
		}
		if p.kept(name) {
			return cutest.ErrNotYet
		}
		return nil
	})
	for _, v := range pruneVersions {
		name := "through-" + v
		if !p.kept(name) {
			t.Fatalf("precondition: %s lost its field before any patch", name)
		}
		p.patches()["A2"](name, v)
		kept := p.kept(name)
		t.Logf("PRUNE conversion None, stored at v1beta1 and the storage version moved to v1, A2 through %s: kept: %v", v, kept)
		if kept {
			t.Errorf("A2 through %s kept %s; want it pruned", v, onlyIn("v1beta1"))
		}
	}
}
