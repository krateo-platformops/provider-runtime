package credentialuse

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

// Chains (§9.1 to §9.3), on the protocol rig: the in-memory model as the
// server, and fakeContents as the live content reader of a content kind.

// fakeContents reads a content kind's object from the model, with the spec
// the test gave it, and records each read in the rig's calls, so a test sees
// it in order with the writes.
type fakeContents struct {
	r     *rig
	specs map[ID]map[string]any
}

func (f *fakeContents) Content(_ context.Context, gk schema.GroupKind, namespace, name string) (*unstructured.Unstructured, error) {
	f.r.srv.calls = append(f.r.srv.calls, "content "+gk.Kind+"/"+name)
	m, err := f.r.m.Get(apimodel.Key{GK: gk, Namespace: namespace, Name: name})
	if err != nil {
		return nil, err
	}
	md, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&m.ObjectMeta)
	if err != nil {
		return nil, err
	}
	// A status, as the server would return, which no Next may see.
	u := &unstructured.Unstructured{Object: map[string]any{"metadata": md, "status": map[string]any{"ready": true}}}
	u.SetKind(gk.Kind)
	if spec, ok := f.specs[ID{Group: gk.Group, Kind: gk.Kind, Namespace: namespace, Name: name}]; ok {
		u.Object["spec"] = runtime.DeepCopyJSONValue(spec)
	}
	return u, nil
}

func targetID(name string) ID {
	return ID{Group: targetGK.Group, Kind: targetGK.Kind, Namespace: "ns", Name: name}
}
func targetKey(name string) apimodel.Key {
	return apimodel.Key{GK: targetGK, Namespace: "ns", Name: name}
}

// chainRig is a rig whose protocol treats Target as a content kind.
func chainRig(t *testing.T) (*rig, *fakeContents) {
	r := newRig(t)
	fc := &fakeContents{r: r, specs: map[ID]map[string]any{}}
	r.p.contentKinds = map[schema.GroupKind]bool{targetGK: true}
	r.p.contents = fc
	return r, fc
}

// target creates Target name, whose spec names Secret secret (or another
// Target, with kind "Target").
func (r *rig) target(fc *fakeContents, name, kind, ref string) {
	must(r.m.Create(targetKey(name), metav1.ObjectMeta{}))
	fc.specs[targetID(name)] = map[string]any{"ref": map[string]any{"kind": kind, "name": ref}}
}

// nextOf is the Next of a Target: the object its spec.ref names, which is
// followed in turn when it is a Target.
func nextOf(obj *unstructured.Unstructured) ([]Ref, error) {
	kind, _, _ := unstructured.NestedString(obj.Object, "spec", "ref", "kind")
	name, _, _ := unstructured.NestedString(obj.Object, "spec", "ref", "name")
	switch {
	case name == "":
		return nil, errors.New("no spec.ref")
	case kind == "Target":
		return []Ref{targetRef(name)}, nil
	}
	return []Ref{SecretRef(obj.GetNamespace(), name)}, nil
}

func targetRef(name string) Ref {
	return Ref{GroupKind: targetGK, Namespace: "ns", Name: name, Next: nextOf}
}

func chainRecord(t *testing.T, r *rig, holder string) []ID {
	t.Helper()
	return ReadRecord(r.get(t, widgetKey("ns", holder))).IDs
}

func TestChainHoldsEachHopBeforeReadingIt(t *testing.T) {
	r, fc := chainRig(t)
	r.target(fc, "kt", "Secret", "kubeconfig")
	must(r.m.Create(secretKey("ns", "kubeconfig"), metav1.ObjectMeta{}))
	h := r.holder(t, "h", false)

	a, err := r.p.acquire(context.Background(), h, []Ref{targetRef("kt")})
	if err != nil || a.MustNotConnect || a.Degraded || len(a.Held) != 2 {
		t.Fatalf("acquire = %+v, %v; want the Target and its Secret held", a, err)
	}
	want := SortedIDs([]ID{targetID("kt"), SecretRef("ns", "kubeconfig").ID()})
	if got := chainRecord(t, r, "h"); !slices.Equal(got, want) {
		t.Errorf("record = %v, want %v", got, want)
	}
	for _, k := range []apimodel.Key{targetKey("kt"), secretKey("ns", "kubeconfig")} {
		if !hasFinalizer(r.get(t, k), fNS) {
			t.Errorf("%s isn't held", k)
		}
	}
	// The Target is read only after its A2's patch, and the Secret is held
	// after the read that named it.
	calls := strings.Join(r.srv.calls, ", ")
	patchT := strings.Index(calls, "patch Target/kt")
	read := strings.Index(calls, "content Target/kt")
	patchS := strings.Index(calls, "patch Secret/kubeconfig")
	if patchT < 0 || read < patchT || patchS < read {
		t.Errorf("calls = %s; want the Target patched, then read, then the Secret patched", calls)
	}
	// Resolved hands on exactly that read.
	got, ok := Resolved(WithResolved(context.Background(), a), targetRef("kt"))
	if !ok || got.GetName() != "kt" {
		t.Fatalf("Resolved = %v, %v", got, ok)
	}
	if name, _, _ := unstructured.NestedString(got.Object, "spec", "ref", "name"); name != "kubeconfig" {
		t.Errorf("Resolved's spec.ref.name = %q", name)
	}
	if _, has := got.Object["status"]; has {
		t.Error("a live read of a hop kept its status")
	}
}

func TestChainRepointKeepsTheOldRefUntilTheLastAnnounce(t *testing.T) {
	r, fc := chainRig(t)
	r.target(fc, "kt", "Secret", "old")
	must(r.m.Create(secretKey("ns", "old"), metav1.ObjectMeta{}))
	must(r.m.Create(secretKey("ns", "new"), metav1.ObjectMeta{}))
	h := r.holder(t, "h", false)
	if _, err := r.p.acquire(context.Background(), h, []Ref{targetRef("kt")}); err != nil {
		t.Fatal(err)
	}

	// Re-point the Target. On the next pass, every record the holder is
	// written with must name the old Secret until the chain is known.
	fc.specs[targetID("kt")]["ref"] = map[string]any{"kind": "Secret", "name": "new"}
	var records [][]ID
	r.srv.before = func(op, kind, name string) error {
		if op == "patch" && kind == "Widget" {
			// Seen just before the write, so take the next record afterwards.
			records = append(records, nil)
		}
		return nil
	}
	inner := r.srv.server
	r.srv.server = recordWatch{server: inner, onWidget: func(ids []ID) { records[len(records)-1] = ids }}
	h = r.get(t, widgetKey("ns", "h"))
	a, err := r.p.acquire(context.Background(), h, []Ref{targetRef("kt")})
	if err != nil || len(a.Held) != 2 {
		t.Fatalf("acquire = %+v, %v", a, err)
	}
	old, nw := SecretRef("ns", "old").ID(), SecretRef("ns", "new").ID()
	if len(records) == 0 {
		t.Fatal("no record write")
	}
	for i, ids := range records[:len(records)-1] {
		if !slices.Contains(ids, old) {
			t.Errorf("record write %d = %v drops the old Secret before the chain is known", i, ids)
		}
	}
	if last := records[len(records)-1]; slices.Contains(last, old) || !slices.Contains(last, nw) {
		t.Errorf("the last record write = %v, want the new Secret and not the old", last)
	}
}

// recordWatch reports the record each Widget patch leaves.
type recordWatch struct {
	server
	onWidget func([]ID)
}

func (w recordWatch) Patch(ctx context.Context, gk schema.GroupKind, ns, name string, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error) {
	out, err := w.server.Patch(ctx, gk, ns, name, pt, data)
	if err == nil && gk == widgetGK {
		w.onWidget(ReadRecord(out).IDs)
	}
	return out, err
}

func TestChainFailures(t *testing.T) {
	ctx := context.Background()
	t.Run("a Next error fails closed for a new holder", func(t *testing.T) {
		r, fc := chainRig(t)
		must(r.m.Create(targetKey("kt"), metav1.ObjectMeta{}))
		fc.specs[targetID("kt")] = map[string]any{}
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{targetRef("kt")})
		if err != nil || !a.MustNotConnect || a.Err == nil || !strings.Contains(a.Err.Error(), "no spec.ref") {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
	})
	t.Run("a Next error fails open for an established holder", func(t *testing.T) {
		r, fc := chainRig(t)
		must(r.m.Create(targetKey("kt"), metav1.ObjectMeta{}))
		fc.specs[targetID("kt")] = map[string]any{}
		a, err := r.p.acquire(ctx, r.holder(t, "h", true), []Ref{targetRef("kt")})
		if err != nil || a.MustNotConnect || !a.Degraded {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
	})
	t.Run("a chain deeper than MaxDepth", func(t *testing.T) {
		r, fc := chainRig(t)
		r.p.maxDepth = 3
		r.target(fc, "t1", "Target", "t2")
		r.target(fc, "t2", "Target", "t3")
		r.target(fc, "t3", "Target", "t4")
		r.target(fc, "t4", "Secret", "s")
		must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{targetRef("t1")})
		if err != nil || !a.MustNotConnect || !errors.Is(a.Err, errChainTooDeep) {
			t.Fatalf("acquire = %+v, %v; want errChainTooDeep", a, err)
		}
		// Control: three levels are allowed.
		r2, fc2 := chainRig(t)
		r2.p.maxDepth = 3
		r2.target(fc2, "t1", "Target", "t2")
		r2.target(fc2, "t2", "Secret", "s")
		must(r2.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
		if a, err := r2.p.acquire(ctx, r2.holder(t, "h", false), []Ref{targetRef("t1")}); err != nil || a.MustNotConnect || len(a.Held) != 3 {
			t.Fatalf("a three-level chain: acquire = %+v, %v", a, err)
		}
	})
	t.Run("a cycle ends", func(t *testing.T) {
		r, fc := chainRig(t)
		r.p.maxDepth = 2
		r.target(fc, "t1", "Target", "t2")
		r.target(fc, "t2", "Target", "t1")
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{targetRef("t1")})
		if err != nil || a.MustNotConnect || len(a.Held) != 2 {
			t.Fatalf("acquire = %+v, %v; want both Targets held, once", a, err)
		}
	})
	t.Run("a Missing hop isn't followed, and BeforeEstablish won't establish when it appears", func(t *testing.T) {
		r, fc := chainRig(t)
		h := r.holder(t, "h", false)
		a, err := r.p.acquire(ctx, h, []Ref{targetRef("kt")})
		if err != nil || len(a.Unheld) != 1 || a.Unheld[0].Reason != ReasonMissing || slices.ContainsFunc(r.srv.calls, func(c string) bool { return strings.HasPrefix(c, "content") }) {
			t.Fatalf("acquire = %+v, %v, calls %v", a, err, r.srv.calls)
		}
		r.target(fc, "kt", "Secret", "s")
		if err := r.p.beforeEstablish(ctx, r.get(t, widgetKey("ns", "h")), a); err == nil {
			t.Fatal("BeforeEstablish established with a hop whose chain isn't held")
		}
		if !hasFinalizer(r.get(t, targetKey("kt")), fNS) {
			t.Error("BeforeEstablish didn't hold the hop that appeared")
		}
	})
}

func TestChainGuaranteedRefs(t *testing.T) {
	ctx := context.Background()
	guaranteed := func() Ref { r := targetRef("cd"); r.Guaranteed = true; return r }
	t.Run("not recorded, not held, its Next followed, read live", func(t *testing.T) {
		r, fc := chainRig(t)
		r.target(fc, "cd", "Secret", "password")
		must(r.m.Create(secretKey("ns", "password"), metav1.ObjectMeta{}))
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{guaranteed()})
		if err != nil || a.MustNotConnect || len(a.Held) != 1 {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
		if got := chainRecord(t, r, "h"); !slices.Equal(got, []ID{SecretRef("ns", "password").ID()}) {
			t.Errorf("record = %v, want only the Secret", got)
		}
		if hasFinalizer(r.get(t, targetKey("cd")), fNS) || slices.Contains(r.srv.calls, "patch Target/cd") {
			t.Error("a Guaranteed ref was held")
		}
		if !slices.Contains(r.srv.calls, "content Target/cd") {
			t.Errorf("calls = %v; want a live read of the Guaranteed ref", r.srv.calls)
		}
	})
	t.Run("read through the caller's lister", func(t *testing.T) {
		r, fc := chainRig(t)
		r.target(fc, "cd", "Secret", "password")
		must(r.m.Create(secretKey("ns", "password"), metav1.ObjectMeta{}))
		listed := 0
		r.p.listers = map[schema.GroupKind]Lister{targetGK: func(ns, name string) (*unstructured.Unstructured, error) {
			listed++
			return fc.Content(ctx, targetGK, ns, name)
		}}
		r.srv.calls = nil
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{guaranteed()})
		if err != nil || len(a.Held) != 1 || listed != 1 {
			t.Fatalf("acquire = %+v, %v, listed %d", a, err, listed)
		}
		if got, ok := Resolved(WithResolved(ctx, a), guaranteed()); !ok || got.Object["status"] != nil {
			t.Errorf("Resolved = %v, %v; want the lister's object without its status", got, ok)
		}
	})
	t.Run("one that can't be read fails, and isn't Missing", func(t *testing.T) {
		r, _ := chainRig(t)
		r.p.listers = map[schema.GroupKind]Lister{targetGK: func(ns, name string) (*unstructured.Unstructured, error) {
			return nil, apierrors.NewNotFound(schema.GroupResource{Group: targetGK.Group, Resource: "targets"}, name)
		}}
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{guaranteed()})
		if err != nil || !a.MustNotConnect || len(a.Unheld) != 0 {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
	})
}

// TestChainNextNeedsContent: a held ref with a Next must be of a Content
// kind (§10.4; the user's decision, 2026-11-21), since only the content cache
// sees a re-point of it. A Guaranteed ref isn't held, so it needn't be.
func TestChainNextNeedsContent(t *testing.T) {
	ctx := context.Background()
	outer := func() Ref {
		r := SecretRef("ns", "outer")
		r.Next = func(*unstructured.Unstructured) ([]Ref, error) { return []Ref{SecretRef("ns", "inner")}, nil }
		return r
	}
	t.Run("fails closed for a new holder", func(t *testing.T) {
		r, _ := chainRig(t)
		must(r.m.Create(secretKey("ns", "outer"), metav1.ObjectMeta{}))
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{outer()})
		if err != nil || !a.MustNotConnect || !errors.Is(a.Err, errNextNeedsContent) || !strings.Contains(a.Err.Error(), "set Content") {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
		if hasFinalizer(r.get(t, secretKey("ns", "outer")), fNS) {
			t.Error("the refused ref was held")
		}
	})
	t.Run("fails open for an established holder", func(t *testing.T) {
		r, _ := chainRig(t)
		must(r.m.Create(secretKey("ns", "outer"), metav1.ObjectMeta{}))
		if a, err := r.p.acquire(ctx, r.holder(t, "h", true), []Ref{outer()}); err != nil || a.MustNotConnect || !a.Degraded || !errors.Is(a.Err, errNextNeedsContent) {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
	})
	t.Run("a Guaranteed ref of any kind may have a Next", func(t *testing.T) {
		r, fc := chainRig(t)
		r.p.contentKinds = nil
		r.target(fc, "cd", "Secret", "s")
		must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
		g := targetRef("cd")
		g.Guaranteed = true
		if a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{g}); err != nil || a.MustNotConnect || len(a.Held) != 1 {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
	})
}

// TestChainKeepsWhatAnUnfollowedHopLeadsTo: a hop with a Next that this pass
// can't hold, Deleting or Missing, isn't read, so what lies behind it is
// unknown; the record keeps the refs it named, which stay held (§9.1, §8),
// while an established holder fails open and may still read through the hop.
func TestChainKeepsWhatAnUnfollowedHopLeadsTo(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		break_ func(r *rig)
		want   Reason
	}{
		{"the hop is being deleted", func(r *rig) {
			// Someone strips our finalizer, and the hop is deleted: it stays,
			// deleting, for its other finalizer.
			data := []byte(`{"metadata":{"finalizers":["example.com/keep"]}}`)
			if _, err := r.m.Patch(targetKey("kt"), types.MergePatchType, data); err != nil {
				panic(err)
			}
			must(r.m.Delete(targetKey("kt")))
		}, ReasonDeleting},
		{"the hop is missing", func(r *rig) {
			data := []byte(`{"metadata":{"finalizers":null}}`)
			if _, err := r.m.Patch(targetKey("kt"), types.MergePatchType, data); err != nil {
				panic(err)
			}
			must(r.m.Delete(targetKey("kt")))
		}, ReasonMissing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, fc := chainRig(t)
			must(r.m.Create(targetKey("kt"), metav1.ObjectMeta{Finalizers: []string{"example.com/keep"}}))
			fc.specs[targetID("kt")] = map[string]any{"ref": map[string]any{"kind": "Secret", "name": "kubeconfig"}}
			must(r.m.Create(secretKey("ns", "kubeconfig"), metav1.ObjectMeta{}))
			h := r.holder(t, "h", true)
			if a, err := r.p.acquire(ctx, h, []Ref{targetRef("kt")}); err != nil || len(a.Held) != 2 {
				t.Fatalf("pass 1: acquire = %+v, %v", a, err)
			}
			tc.break_(r)
			a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{targetRef("kt")})
			if err != nil || len(a.Unheld) != 1 || a.Unheld[0].Reason != tc.want {
				t.Fatalf("pass 2: acquire = %+v, %v", a, err)
			}
			if got := chainRecord(t, r, "h"); !slices.Contains(got, SecretRef("ns", "kubeconfig").ID()) {
				t.Errorf("record = %v: the Secret behind the unfollowed hop was dropped, so a release can take it", got)
			}
		})
	}
}

func TestResolvedIsTheHeldReadNotANewerOne(t *testing.T) {
	r, fc := chainRig(t)
	r.target(fc, "kt", "Secret", "s")
	must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
	a, err := r.p.acquire(context.Background(), r.holder(t, "h", false), []Ref{targetRef("kt")})
	if err != nil {
		t.Fatal(err)
	}
	fc.specs[targetID("kt")]["ref"] = map[string]any{"kind": "Secret", "name": "elsewhere"}
	got, ok := Resolved(WithResolved(context.Background(), a), targetRef("kt"))
	if name, _, _ := unstructured.NestedString(got.Object, "spec", "ref", "name"); !ok || name != "s" {
		t.Errorf("Resolved names %q, want the held read's \"s\"", name)
	}
	got.Object["spec"] = nil
	again, _ := Resolved(WithResolved(context.Background(), a), targetRef("kt"))
	if again.Object["spec"] == nil {
		t.Error("Resolved's object is shared with the caller")
	}
	if _, ok := Resolved(context.Background(), targetRef("kt")); ok {
		t.Error("Resolved without WithResolved found an object")
	}
}

// TestContentTransform: a content kind's cache keeps the whole object but
// managedFields and status (§10.4), and doesn't change the informer's copy.
func TestContentTransform(t *testing.T) {
	in := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "credentialuse.test.krateo.io/v1", "kind": "Target",
		"metadata": map[string]any{"name": "kt", "namespace": "ns", "labels": map[string]any{"app": "x"},
			"managedFields": []any{map[string]any{"manager": "kubectl"}}},
		"spec":   map[string]any{"secret": "s"},
		"status": map[string]any{"ready": true},
	}}
	out, err := contentTransform(in)
	if err != nil {
		t.Fatal(err)
	}
	u := out.(*unstructured.Unstructured)
	if _, has := u.Object["status"]; has {
		t.Error("status kept")
	}
	if u.GetManagedFields() != nil {
		t.Error("managedFields kept")
	}
	if s, _, _ := unstructured.NestedString(u.Object, "spec", "secret"); s != "s" || u.GetLabels()["app"] != "x" {
		t.Errorf("content lost: %v", u.Object)
	}
	if _, has := in.Object["status"]; !has || in.GetManagedFields() == nil {
		t.Error("the transform changed its input")
	}
}

// TestContentMustAgreeAcrossKinds: one informer caches a protected kind for
// every holder kind, so two holder kinds that disagree on its Content can't
// both track it (§10.4).
func TestContentMustAgreeAcrossKinds(t *testing.T) {
	r := newFakeRig(t)
	r.track(t, widgetGK, ProtectedKind{GVK: secretGVK, Content: true})
	_, err := r.s.Track(context.Background(), r.options(gadgetGK, ProtectedKind{GVK: secretGVK}))
	if err == nil || !strings.Contains(err.Error(), "Content") {
		t.Fatalf("Track with the other Content = %v, want it refused", err)
	}
	// Control: agreeing is fine.
	r2 := newFakeRig(t)
	r2.track(t, widgetGK, ProtectedKind{GVK: secretGVK, Content: true})
	if _, err := r2.s.Track(context.Background(), r2.options(gadgetGK, ProtectedKind{GVK: secretGVK, Content: true})); err != nil {
		t.Fatalf("Track with the same Content = %v", err)
	}
}

// TestChainEndingInACycleStillWritesTheExactRecord: when the last level's
// hops lead nowhere new (here, a cycle), the A1s between levels left the old
// refs in the record, so a last A1 must write exactly the chain; otherwise
// the Secret the chain no longer names stays recorded and is never released.
func TestChainEndingInACycleStillWritesTheExactRecord(t *testing.T) {
	r, fc := chainRig(t)
	r.target(fc, "t1", "Secret", "s")
	must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
	h := r.holder(t, "h", false)
	if _, err := r.p.acquire(context.Background(), h, []Ref{targetRef("t1")}); err != nil {
		t.Fatal(err)
	}
	// Re-point t1 to t2, which points back to t1: no Secret any more.
	fc.specs[targetID("t1")]["ref"] = map[string]any{"kind": "Target", "name": "t2"}
	r.target(fc, "t2", "Target", "t1")
	a, err := r.p.acquire(context.Background(), r.get(t, widgetKey("ns", "h")), []Ref{targetRef("t1")})
	if err != nil || a.MustNotConnect || len(a.Held) != 2 {
		t.Fatalf("acquire = %+v, %v", a, err)
	}
	want := SortedIDs([]ID{targetID("t1"), targetID("t2")})
	if got := chainRecord(t, r, "h"); !slices.Equal(got, want) {
		t.Errorf("record = %v, want %v: the Secret the chain no longer names is still recorded", got, want)
	}
}
