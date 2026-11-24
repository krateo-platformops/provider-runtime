package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/equality"
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

// chainRig is a rig whose protocol follows chains, which production refuses
// (chain_refused_test.go), and treats Target as a content kind.
func chainRig(t *testing.T) (*rig, *fakeContents) {
	r := newRig(t)
	fc := &fakeContents{r: r, specs: map[ID]map[string]any{}}
	r.p.followChains = true
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
	t.Run("a lister that returns neither an object nor an error fails, naming the kind", func(t *testing.T) {
		r, _ := chainRig(t)
		r.p.listers = map[schema.GroupKind]Lister{targetGK: func(string, string) (*unstructured.Unstructured, error) { return nil, nil }}
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{guaranteed()})
		if err != nil || !a.MustNotConnect || a.Err == nil || len(a.Unheld) != 0 || !strings.Contains(a.Err.Error(), "the lister for "+targetGK.String()) {
			t.Fatalf("acquire = %+v, %v", a, err)
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

// TestRefsConflicting: the first ref met of an object wins, and a later one
// that asks for more, a hold where the first is Guaranteed, or a Next where
// the first has none, is reported once per holder, object and thing asked for,
// at the same level or behind a Next (§9.1). One that asks for no more isn't.
func TestRefsConflicting(t *testing.T) {
	ctx := context.Background()
	held := targetRef("cd")
	guaranteed := targetRef("cd")
	guaranteed.Guaranteed = true
	plain := targetRef("cd")
	plain.Next = nil
	plainGuaranteed := plain
	plainGuaranteed.Guaranteed = true
	other := targetRef("cd")
	other.Next = func(*unstructured.Unstructured) ([]Ref, error) { return nil, errors.New("never called") }
	// via is a Guaranteed ref to another object, whose Next names refs.
	via := func(refs ...Ref) Ref {
		v := targetRef("via")
		v.Guaranteed = true
		v.Next = func(*unstructured.Unstructured) ([]Ref, error) { return refs, nil }
		return v
	}
	const cd = "/Target/ns/cd"
	for name, tc := range map[string]struct {
		refs   []Ref
		want   string // what is reported of cd, or ""
		held   bool   // whether cd is
		behind bool   // whether the Secret behind cd is
	}{
		"Guaranteed, then held":                  {refs: []Ref{guaranteed, held}, want: "hold=true next=false", behind: true},
		"held, then Guaranteed":                  {refs: []Ref{held, guaranteed}, held: true, behind: true},
		"no Next, then one":                      {refs: []Ref{plain, held}, want: "hold=false next=true", held: true},
		"a Next, then none":                      {refs: []Ref{held, plain}, held: true, behind: true},
		"Guaranteed with no Next, then both":     {refs: []Ref{plainGuaranteed, held}, want: "hold=true next=true"},
		"Guaranteed with a Next, then held":      {refs: []Ref{guaranteed, plain}, want: "hold=true next=false", behind: true},
		"twice the same":                         {refs: []Ref{held, held}, held: true, behind: true},
		"two Nexts, which can't be told apart":   {refs: []Ref{held, other}, held: true, behind: true},
		"three, two of which ask for more":       {refs: []Ref{guaranteed, held, held}, want: "hold=true next=false", behind: true},
		"held, Guaranteed, held":                 {refs: []Ref{held, guaranteed, held}, held: true, behind: true},
		"Guaranteed, then held behind a Next":    {refs: []Ref{guaranteed, via(held)}, want: "hold=true next=false", behind: true},
		"no Next, then one behind a Next":        {refs: []Ref{plain, via(held)}, want: "hold=false next=true", held: true},
		"held, then Guaranteed behind a Next":    {refs: []Ref{held, via(guaranteed)}, held: true, behind: true},
		"the first behind a Next, at one level":  {refs: []Ref{via(guaranteed, held)}, want: "hold=true next=false", behind: true},
		"a cycle that asks for no more than met": {refs: []Ref{held, via(held)}, held: true, behind: true},
	} {
		t.Run(name, func(t *testing.T) {
			r, fc := chainRig(t)
			r.target(fc, "cd", "Secret", "password")
			r.target(fc, "via", "Secret", "password")
			must(r.m.Create(secretKey("ns", "password"), metav1.ObjectMeta{}))
			h := r.holder(t, "h", false)
			for pass := range 2 {
				a, err := r.p.acquire(ctx, h, tc.refs)
				if err != nil || a.MustNotConnect || a.Err != nil {
					t.Fatalf("pass %d: acquire = %+v, %v", pass, a, err)
				}
			}
			if got := hasFinalizer(r.get(t, targetKey("cd")), fNS); got != tc.held {
				t.Errorf("cd held = %v, want %v: the first ref wins", got, tc.held)
			}
			if got := hasFinalizer(r.get(t, secretKey("ns", "password")), fNS); got != tc.behind {
				t.Errorf("the Secret behind cd held = %v, want %v: the first ref wins", got, tc.behind)
			}
			var want []string
			if tc.want != "" {
				want = []string{"h " + targetGK.Group + cd + " " + tc.want}
			}
			if !slices.Equal(r.rep.conflicting, want) {
				t.Errorf("reported %q, want %q", r.rep.conflicting, want)
			}
		})
	}
	t.Run("one asks for a hold, and another for a Next too", func(t *testing.T) {
		r, fc := chainRig(t)
		r.target(fc, "cd", "Secret", "password")
		if _, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{plainGuaranteed, plain, held}); err != nil {
			t.Fatal(err)
		}
		want := []string{"h " + targetGK.Group + cd + " hold=true next=false", "h " + targetGK.Group + cd + " hold=false next=true"}
		if !slices.Equal(r.rep.conflicting, want) {
			t.Errorf("reported %q, want %q", r.rep.conflicting, want)
		}
	})
	t.Run("once for each object", func(t *testing.T) {
		r, fc := chainRig(t)
		r.target(fc, "cd", "Secret", "password")
		r.target(fc, "cd2", "Secret", "password")
		plainGuaranteed2, held2 := plainGuaranteed, held
		plainGuaranteed2.Name, held2.Name = "cd2", "cd2"
		if _, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{plainGuaranteed, held, plainGuaranteed2, held2}); err != nil {
			t.Fatal(err)
		}
		want := []string{"h " + targetGK.Group + cd + " hold=true next=true", "h " + targetGK.Group + cd + "2 hold=true next=true"}
		if !slices.Equal(r.rep.conflicting, want) {
			t.Errorf("reported %q, want %q", r.rep.conflicting, want)
		}
	})
	t.Run("a holder that records nothing is forgotten too", func(t *testing.T) {
		r, fc := chainRig(t)
		r.target(fc, "cd", "Secret", "password")
		h := r.holder(t, "h", false)
		for range 2 {
			if _, err := r.p.acquire(ctx, h, []Ref{plainGuaranteed, held}); err != nil {
				t.Fatal(err)
			}
			if r.p.forget(h) {
				t.Fatal("the holder has a record")
			}
		}
		if len(r.rep.conflicting) != 2 {
			t.Errorf("reported %q, want one more after the Forget", r.rep.conflicting)
		}
	})
	t.Run("once per holder, and again after its Forget", func(t *testing.T) {
		r, fc := chainRig(t)
		r.target(fc, "cd", "Secret", "password")
		must(r.m.Create(secretKey("ns", "password"), metav1.ObjectMeta{}))
		refs := []Ref{guaranteed, held}
		one, two := r.holder(t, "one", false), r.holder(t, "two", false)
		for _, h := range []*metav1.PartialObjectMetadata{one, two, one} {
			if _, err := r.p.acquire(ctx, h, refs); err != nil {
				t.Fatal(err)
			}
		}
		if len(r.rep.conflicting) != 2 {
			t.Errorf("reported %q, want once for each holder", r.rep.conflicting)
		}
		if !r.p.forget(one) {
			t.Fatal("nothing to forget")
		}
		if _, err := r.p.acquire(ctx, one, refs); err != nil {
			t.Fatal(err)
		}
		if len(r.rep.conflicting) != 3 {
			t.Errorf("reported %q, want one more after the Forget", r.rep.conflicting)
		}
	})
}

// What was reported of a holder is forgotten when the conflict, or the holder,
// is gone, so a holder whose refs keep changing doesn't add to it for life
// (§9.1): by a pass that met all the holder's refs, though it then fails, and
// not by one that may not have.
func TestRefsConflictingForgotten(t *testing.T) {
	ctx := context.Background()
	guaranteed := targetRef("cd")
	guaranteed.Guaranteed, guaranteed.Next = true, nil
	plain := targetRef("cd")
	plain.Next = nil
	conflict := []Ref{guaranteed, plain}
	other := []Ref{SecretRef("ns", "other")}
	acquire := func(t *testing.T, r *rig, h metav1.Object, refs []Ref) Acquired {
		t.Helper()
		a, err := r.p.acquire(ctx, h, refs)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	start := func(t *testing.T) (*rig, *fakeContents, *metav1.PartialObjectMetadata) {
		t.Helper()
		r, fc := chainRig(t)
		r.target(fc, "cd", "Target", "cd")
		must(r.m.Create(secretKey("ns", "other"), metav1.ObjectMeta{}))
		h := r.holder(t, "h", false)
		acquire(t, r, h, conflict)
		acquire(t, r, h, conflict)
		if len(r.rep.conflicting) != 1 {
			t.Fatalf("reported %q, want one", r.rep.conflicting)
		}
		return r, fc, h
	}
	want := func(t *testing.T, r *rig, n int) {
		t.Helper()
		if len(r.rep.conflicting) != n {
			t.Errorf("reported %q, want %d", r.rep.conflicting, n)
		}
	}
	t.Run("when the refs no longer conflict", func(t *testing.T) {
		r, _, h := start(t)
		acquire(t, r, h, other)
		acquire(t, r, h, conflict)
		want(t, r, 2)
	})
	t.Run("each thing asked for on its own", func(t *testing.T) {
		r, _, h := start(t)
		// A hold and a Next, then the Next alone, then both again.
		followed := targetRef("cd")
		acquire(t, r, h, []Ref{guaranteed, followed})
		acquire(t, r, h, []Ref{plain, followed})
		acquire(t, r, h, []Ref{guaranteed, followed})
		group := "h " + targetGK.Group + "/Target/ns/cd "
		if got := r.rep.conflicting; !slices.Equal(got, []string{group + "hold=true next=false", group + "hold=false next=true", group + "hold=true next=false"}) {
			t.Errorf("reported %q", got)
		}
	})
	t.Run("by a pass whose last level leads back", func(t *testing.T) {
		r, _, h := start(t)
		acquire(t, r, h, []Ref{targetRef("cd")})
		acquire(t, r, h, conflict)
		want(t, r, 2)
	})
	t.Run("by a pass that then fails", func(t *testing.T) {
		r, _, h := start(t)
		r.srv.before = func(op, kind, _ string) error {
			if op == "patch" && kind == "Secret" {
				return errors.New("refused")
			}
			return nil
		}
		if a := acquire(t, r, h, other); !a.MustNotConnect {
			t.Fatalf("acquire = %+v; want a pass that fails", a)
		}
		r.srv.before = nil
		acquire(t, r, h, conflict)
		want(t, r, 2)
	})
	t.Run("not by a pass that couldn't follow a hop", func(t *testing.T) {
		r, fc, h := start(t)
		r.target(fc, "via", "Secret", "other")
		if a := acquire(t, r, h, []Ref{targetRef("absent"), targetRef("via")}); a.MustNotConnect || len(a.Unheld) != 1 {
			t.Fatalf("acquire = %+v; want one Missing hop", a)
		}
		acquire(t, r, h, conflict)
		want(t, r, 1)
	})
	t.Run("not by a pass whose Next fails", func(t *testing.T) {
		r, fc, h := start(t)
		r.target(fc, "via", "Secret", "")
		if a := acquire(t, r, h, []Ref{targetRef("via")}); !a.MustNotConnect {
			t.Fatalf("acquire = %+v; want a pass that fails", a)
		}
		acquire(t, r, h, conflict)
		want(t, r, 1)
	})
	t.Run("when the holder is deleted without a Forget", func(t *testing.T) {
		r, _, h := start(t)
		tr := newFakeRig(t).track(t, widgetGK)
		tr.proto = r.p
		holderHandler{tr}.OnDelete(h)
		acquire(t, r, h, conflict)
		want(t, r, 2)
	})
}

// A set keeps nothing of a holder that asks for nothing, so it doesn't grow
// with the holders ever seen.
func TestConflictSet(t *testing.T) {
	c := newConflictSet()
	cd := targetID("cd")
	c.tell("u1", cd, asks{hold: true})
	c.tell("u2", cd, asks{next: true})
	c.tell("u1", cd, asks{next: true})
	if got := c.told("u1", cd); got != (asks{hold: true, next: true}) {
		t.Errorf("told of u1 = %+v, want both", got)
	}
	c.keep("u1", map[ID]asks{cd: {}})
	c.forget("u2")
	if len(c.m) != 0 {
		t.Errorf("kept %v", c.m)
	}
	c.keep("u3", map[ID]asks{cd: {hold: true}})
	if len(c.m) != 0 {
		t.Errorf("keep added %v", c.m)
	}
	var none *conflictSet // remembers nothing
	none.tell("u1", cd, asks{hold: true})
	none.keep("u1", nil)
	none.forget("u1")
	if got := none.told("u1", cd); got != (asks{}) {
		t.Errorf("a nil set remembers %+v", got)
	}
}

// A hold of a kind the Disown switch covers isn't reported, since no ref of it
// is held (§7.4); its Next is. A conflict reads nothing to be reported, in
// another namespace without consent too (§9.1).
func TestRefsConflictingReported(t *testing.T) {
	ctx := context.Background()
	t.Run("a Disowned kind's hold isn't, and its Next is", func(t *testing.T) {
		r, fc := chainRig(t)
		r.target(fc, "cd", "Secret", "password")
		disowned := true
		r.p.disowned = func(gk schema.GroupKind) bool { return disowned && gk == targetGK }
		guaranteed := targetRef("cd")
		guaranteed.Guaranteed, guaranteed.Next = true, nil
		h := r.holder(t, "h", false)
		pass := func(want ...string) {
			t.Helper()
			if _, err := r.p.acquire(ctx, h, []Ref{guaranteed, targetRef("cd")}); err != nil {
				t.Fatal(err)
			}
			for i := range want {
				want[i] = "h " + targetGK.Group + "/Target/ns/cd " + want[i]
			}
			if !slices.Equal(r.rep.conflicting, want) {
				t.Errorf("reported %q, want %q", r.rep.conflicting, want)
			}
		}
		pass("hold=false next=true")
		// Back on, the hold is reported too.
		disowned = false
		pass("hold=false next=true", "hold=true next=false")
	})
	t.Run("in another namespace without consent, with no read", func(t *testing.T) {
		r, _ := consentRig(t, "")
		must(r.m.Create(secretKey("other", "s"), metav1.ObjectMeta{}))
		g := otherSecret()
		g.Guaranteed = true
		h := r.holder(t, "h", false)
		r.srv.calls = nil
		for range 2 {
			if a, err := r.p.acquire(ctx, h, []Ref{g, otherSecret()}); err != nil || a.MustNotConnect || len(a.Unheld) != 0 {
				t.Fatalf("acquire = %+v, %v", a, err)
			}
		}
		if want := []string{"h /Secret/other/s hold=true next=false"}; !slices.Equal(r.rep.conflicting, want) || len(r.srv.calls) != 0 {
			t.Errorf("reported %q, calls %q; want %q, and none", r.rep.conflicting, r.srv.calls, want)
		}
	})
}

// A conflict counts each thing asked for, and is one log line that names the
// holder, the object and what was asked.
func TestRefsConflictingReport(t *testing.T) {
	for name, tc := range map[string]struct {
		hold, next bool
		asked      string
	}{
		"a hold": {hold: true, asked: "hold"},
		"a Next": {next: true, asked: "next"},
		"both":   {hold: true, next: true, asked: "hold,next"},
	} {
		m := &countingMetrics{}
		var logged []string
		r := coreReports{metrics: m, warn: func(_ string, kv ...any) { logged = append(logged, fmt.Sprintf("%q", kv)) }}
		h := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "h"}}
		r.RefsConflicting(widgetGK, h, targetID("cd"), tc.hold, tc.next)
		for asked, want := range map[string]bool{"hold": tc.hold, "next": tc.next} {
			if got := m.count("refs_conflicting "+kindLabel(widgetGK)+" "+asked) == 1; got != want {
				t.Errorf("%s: counted for %s: %v, want %v", name, asked, got, want)
			}
		}
		want := fmt.Sprintf("%q", []any{"kind", widgetGK.String(), "holder", "ns/h", "object", targetID("cd").String(), "asked", tc.asked})
		if len(logged) != 1 || logged[0] != want {
			t.Errorf("%s: logged %s, want once %s", name, logged, want)
		}
	}
}

// A tracker's conflicts go to its own sinks: the metric of its recorder, and
// its logger; and it remembers them.
func TestRefsConflictingOfATracker(t *testing.T) {
	r := newFakeRig(t)
	tr := r.track(t, widgetGK)
	rep, ok := tr.proto.reports.(coreReports)
	if !ok || rep.warn == nil {
		t.Fatalf("the tracker's reports = %+v; want coreReports that log", tr.proto.reports)
	}
	if tr.proto.conflicts == nil {
		t.Error("the tracker remembers no conflict, so it reports each on every pass")
	}
	rep.RefsConflicting(widgetGK, &metav1.PartialObjectMetadata{}, targetID("cd"), true, false)
	if n := r.metrics.count("refs_conflicting " + kindLabel(widgetGK) + " hold"); n != 1 {
		t.Errorf("counted %d, want 1", n)
	}
}

// TestChainNextCannotChangeTheListersObject: a Next may change the object it
// is given: neither the lister's, which is the caller's cache, nor what
// Resolved returns changes with it.
func TestChainNextCannotChangeTheListersObject(t *testing.T) {
	ctx := context.Background()
	r, fc := chainRig(t)
	r.target(fc, "cd", "Secret", "password")
	must(r.m.Create(secretKey("ns", "password"), metav1.ObjectMeta{Labels: map[string]string{"app": "x"}}))
	cached, err := fc.Content(ctx, targetGK, "ns", "cd")
	if err != nil {
		t.Fatal(err)
	}
	before := cached.DeepCopy()
	r.p.listers = map[schema.GroupKind]Lister{targetGK: func(string, string) (*unstructured.Unstructured, error) { return cached, nil }}
	g := targetRef("cd")
	g.Guaranteed = true
	g.Next = func(obj *unstructured.Unstructured) ([]Ref, error) {
		refs, err := nextOf(obj)
		obj.Object["spec"].(map[string]any)["ref"].(map[string]any)["name"] = "changed"
		obj.SetLabels(map[string]string{"changed": "yes"})
		return refs, err
	}
	a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{g})
	if err != nil || a.MustNotConnect || len(a.Held) != 1 {
		t.Fatalf("acquire = %+v, %v", a, err)
	}
	if !equality.Semantic.DeepEqual(cached.Object, before.Object) {
		t.Errorf("the Next changed the lister's object: %v, was %v", cached.Object, before.Object)
	}
	delete(before.Object, "status")
	if got, ok := Resolved(WithResolved(ctx, a), g); !ok || !equality.Semantic.DeepEqual(got.Object, before.Object) {
		t.Errorf("the Next changed what Resolved returns: %v, want %v", got, before.Object)
	}
}

// TestChainNextThatLacksWhatItReads: a Next whose object lacks what it reads
// must return an error, never nil refs (§9.1). The error keeps the record,
// and so the holds, of a holder that still uses what the Next no longer
// finds; nil refs drop them.
func TestChainNextThatLacksWhatItReads(t *testing.T) {
	ctx := context.Background()
	password := SecretRef("ns", "password")
	for _, guaranteed := range []bool{false, true} {
		ref := func(next func(*unstructured.Unstructured) ([]Ref, error)) []Ref {
			r := targetRef("cd")
			r.Next, r.Guaranteed = next, guaranteed
			return []Ref{r}
		}
		lax := func(obj *unstructured.Unstructured) ([]Ref, error) {
			refs, _ := nextOf(obj)
			return refs, nil
		}
		// lost is a rig whose established holder h holds the password, and
		// whose cd then loses what nextOf reads.
		lost := func(t *testing.T) *rig {
			t.Helper()
			r, fc := chainRig(t)
			r.target(fc, "cd", "Secret", "password")
			must(r.m.Create(secretKey("ns", "password"), metav1.ObjectMeta{}))
			a, err := r.p.acquire(ctx, r.holder(t, "h", true), ref(nextOf))
			if err != nil || a.Degraded || !slices.Contains(chainRecord(t, r, "h"), password.ID()) {
				t.Fatalf("first pass: acquire = %+v, %v", a, err)
			}
			fc.specs[targetID("cd")] = map[string]any{}
			return r
		}
		name := map[bool]string{false: "held", true: "Guaranteed"}[guaranteed]
		t.Run(name+": an error keeps the record", func(t *testing.T) {
			r := lost(t)
			a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), ref(nextOf))
			if err != nil || !a.Degraded || a.Err == nil {
				t.Errorf("acquire = %+v, %v; want Degraded", a, err)
			}
			if !slices.Contains(chainRecord(t, r, "h"), password.ID()) || !hasFinalizer(r.get(t, secretKey("ns", "password")), fNS) {
				t.Errorf("record = %v: the Secret the holder still uses was dropped", chainRecord(t, r, "h"))
			}
			if a, err := r.p.acquire(ctx, r.holder(t, "new", false), ref(nextOf)); err != nil || !a.MustNotConnect {
				t.Errorf("a new holder: acquire = %+v, %v; want MustNotConnect", a, err)
			}
		})
		t.Run(name+": nil refs drop the Secret from the record", func(t *testing.T) {
			r := lost(t)
			a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), ref(lax))
			if err != nil || a.Degraded || a.Err != nil || slices.Contains(chainRecord(t, r, "h"), password.ID()) {
				t.Errorf("acquire = %+v, %v, record %v; want a pass that doesn't name the Secret", a, err, chainRecord(t, r, "h"))
			}
		})
	}
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
