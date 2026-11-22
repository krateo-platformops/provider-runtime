package credentialuse

// Disown's parts on the in-memory model (plan, PR 6; §7.4).

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/krateo-platformops/provider-runtime/pkg/logging"
)

func TestParseDisown(t *testing.T) {
	secret := schema.GroupKind{Kind: "Secret"}
	target := schema.GroupKind{Group: "core.krateo.io", Kind: "KubernetesTarget"}
	cases := []struct {
		v             string
		on, sec, targ bool
		all           bool
	}{
		{"", false, false, false, false},
		{" , ", false, false, false, false},
		{"all", true, true, true, true},
		{"Secret", true, true, false, false},
		{"KubernetesTarget.core.krateo.io", true, false, true, false},
		{" Secret , KubernetesTarget.core.krateo.io ", true, true, true, false},
		{"secret", true, false, false, false}, // kinds are case-sensitive, as everywhere
	}
	for _, c := range cases {
		d := parseDisown(c.v)
		if d.on() != c.on || d.covers(secret) != c.sec || d.covers(target) != c.targ || d.all != c.all {
			t.Errorf("parseDisown(%q) = %+v: on %v, Secret %v, Target %v; want %v, %v, %v", c.v, d, d.on(), d.covers(secret), d.covers(target), c.on, c.sec, c.targ)
		}
	}
}

// otherKindFinalizer is a credential-use finalizer of another holder kind.
var otherKindFinalizer = func() string {
	f, _ := FinalizerName(schema.GroupKind{Group: "other.krateo.io", Kind: "Gizmo"}, "ns")
	return f
}()

func TestDisownPatch(t *testing.T) {
	fA, _ := FinalizerName(widgetGK, "a")
	fB, _ := FinalizerName(widgetGK, "b")
	marker := MarkerLabel(widgetGK)
	apply := func(t *testing.T, r *rig, finalizers []string, labels map[string]string) (*metav1.PartialObjectMetadata, error) {
		t.Helper()
		ann := map[string]string{"keep": "me"}
		for _, f := range finalizers {
			if IsCredentialUseFinalizer(f) {
				ann[f] = "e-" + f
			}
		}
		k := secretKey("ns", "s")
		must(r.m.Create(k, metav1.ObjectMeta{Finalizers: finalizers, Annotations: ann, Labels: labels}))
		read := r.get(t, k)
		p, err := DisownPatch(read, widgetGK)
		if err != nil {
			return nil, err
		}
		if _, err := r.m.Patch(k, p.Type, p.Data); err != nil {
			return nil, err
		}
		return r.get(t, k), nil
	}
	t.Run("every finalizer of the kind, with its epoch, and the marker; the held label stays for another kind", func(t *testing.T) {
		r := newRig(t)
		got, err := apply(t, r, []string{fA, "foreign/f", otherKindFinalizer, fB},
			map[string]string{marker: "true", LabelHeld: "true", "x": "y"})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got.Finalizers, []string{"foreign/f", otherKindFinalizer}) {
			t.Errorf("finalizers = %v", got.Finalizers)
		}
		if _, ok := got.Annotations[fA]; ok || got.Annotations[otherKindFinalizer] == "" || got.Annotations["keep"] != "me" {
			t.Errorf("annotations = %v", got.Annotations)
		}
		if _, ok := got.Labels[marker]; ok || got.Labels[LabelHeld] != "true" || got.Labels["x"] != "y" {
			t.Errorf("labels = %v", got.Labels)
		}
	})
	t.Run("the held label goes with the last credential-use finalizer", func(t *testing.T) {
		r := newRig(t)
		got, err := apply(t, r, []string{fA}, map[string]string{marker: "true", LabelHeld: "true"})
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Finalizers) != 0 || len(got.Labels) != 0 {
			t.Errorf("finalizers %v, labels %v; want none", got.Finalizers, got.Labels)
		}
	})
	t.Run("a finalizer whose epoch is gone", func(t *testing.T) {
		r := newRig(t)
		k := secretKey("ns", "s")
		must(r.m.Create(k, metav1.ObjectMeta{Finalizers: []string{fA}}))
		p, err := DisownPatch(r.get(t, k), widgetGK)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.m.Patch(k, p.Type, p.Data); err != nil {
			t.Fatalf("a strip without the epoch: %v", err)
		}
		if len(r.get(t, k).Finalizers) != 0 {
			t.Error("the finalizer stayed")
		}
	})
	t.Run("labels only", func(t *testing.T) {
		r := newRig(t)
		got, err := apply(t, r, nil, map[string]string{marker: "true", LabelHeld: "true"})
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Labels) != 0 {
			t.Errorf("labels = %v", got.Labels)
		}
	})
	t.Run("nothing of the kind", func(t *testing.T) {
		r := newRig(t)
		_, err := apply(t, r, []string{otherKindFinalizer}, map[string]string{LabelHeld: "true"})
		if !errors.Is(err, ErrNotHeld) {
			t.Errorf("err = %v, want ErrNotHeld", err)
		}
	})
	t.Run("a write in between fails it", func(t *testing.T) {
		r := newRig(t)
		k := secretKey("ns", "s")
		must(r.m.Create(k, metav1.ObjectMeta{Finalizers: []string{fA}}))
		p, err := DisownPatch(r.get(t, k), widgetGK)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.m.Patch(k, types.MergePatchType, []byte(`{"metadata":{"labels":{"x":"y"}}}`)); err != nil {
			t.Fatal(err)
		}
		if _, err := r.m.Patch(k, p.Type, p.Data); ClassifyJSONPatch(err) != ClassStateChanged {
			t.Errorf("err = %v (%v), want ClassStateChanged", err, ClassifyJSONPatch(err))
		}
	})
}

func TestDisownHolderPatch(t *testing.T) {
	secret := SecretRef("ns", "s")
	target := targetRef("c")
	rec, _ := EncodeRecord(SortedIDs([]ID{secret.ID(), target.ID()}))
	h := func(ann map[string]string) *metav1.PartialObjectMetadata {
		return &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "7", Annotations: ann}}
	}
	decode := func(t *testing.T, data []byte) map[string]any {
		t.Helper()
		var p struct {
			Metadata struct {
				ResourceVersion string         `json:"resourceVersion"`
				Annotations     map[string]any `json:"annotations"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal(data, &p); err != nil {
			t.Fatal(err)
		}
		if p.Metadata.ResourceVersion != "7" {
			t.Errorf("the patch isn't guarded by the version read: %s", data)
		}
		return p.Metadata.Annotations
	}
	t.Run("all: the record and the unheld list", func(t *testing.T) {
		data, ok := disownHolderPatch(h(map[string]string{AnnotationRefs: rec, AnnotationUnheld: "[]"}), parseDisown("all"))
		ann := decode(t, data)
		if !ok || ann[AnnotationRefs] != nil || ann[AnnotationUnheld] != nil || len(ann) != 2 {
			t.Errorf("patch %s", data)
		}
	})
	t.Run("one kind: the record keeps the others, and the unheld list stays", func(t *testing.T) {
		data, ok := disownHolderPatch(h(map[string]string{AnnotationRefs: rec, AnnotationUnheld: "[]"}), parseDisown("Secret"))
		ann := decode(t, data)
		want, _ := EncodeRecord([]ID{target.ID()})
		if !ok || ann[AnnotationRefs] != want || len(ann) != 1 {
			t.Errorf("patch %s, want the record %s", data, want)
		}
	})
	t.Run("one kind, none recorded", func(t *testing.T) {
		only, _ := EncodeRecord([]ID{target.ID()})
		if data, ok := disownHolderPatch(h(map[string]string{AnnotationRefs: only}), parseDisown("Secret")); ok {
			t.Errorf("patch %s, want none", data)
		}
	})
	t.Run("one kind, an unparseable record goes", func(t *testing.T) {
		data, ok := disownHolderPatch(h(map[string]string{AnnotationRefs: "{"}), parseDisown("Secret"))
		if ann := decode(t, data); !ok || ann[AnnotationRefs] != nil {
			t.Errorf("patch %s", data)
		}
	})
	t.Run("nothing", func(t *testing.T) {
		if data, ok := disownHolderPatch(h(nil), parseDisown("all")); ok {
			t.Errorf("patch %s, want none", data)
		}
	})
}

// disownRig is a consent rig whose Disown switch the test sets.
func disownRig(t *testing.T) (*rig, *disownSet) {
	r, _ := consentRig(t, "")
	set := &disownSet{}
	r.p.disowned = func(gk schema.GroupKind) bool { return set.covers(gk) }
	return r, set
}

func TestAcquireWhileDisowned(t *testing.T) {
	ctx := context.Background()
	t.Run("all: nothing held or recorded, and the holder connects", func(t *testing.T) {
		r, set := disownRig(t)
		*set = parseDisown("all")
		must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
		must(r.m.Create(secretKey("other", "s"), metav1.ObjectMeta{}))
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{SecretRef("ns", "s"), otherSecret()})
		if err != nil || a.MustNotConnect || a.Degraded || len(a.Held) != 0 || len(a.Unheld) != 2 {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
		for _, u := range a.Unheld {
			if u.Reason != ReasonDisowned {
				t.Errorf("unheld %+v, want Disowned", u)
			}
		}
		h := r.get(t, widgetKey("ns", "h"))
		if ReadRecord(h).Present || h.Annotations[AnnotationUnheld] != "" {
			t.Errorf("annotations %v: Disowned is never recorded, nor written to unheld (A13)", h.Annotations)
		}
		if hasFinalizer(r.get(t, secretKey("ns", "s")), fNS) || held(r, t) {
			t.Error("a disowned ref was held")
		}
		if len(r.rep.notHeld) != 0 {
			t.Errorf("RefNotHeld for %v: a disowned ref isn't a consent refusal", r.rep.notHeld)
		}
		if err := r.p.beforeEstablish(ctx, h, a); err != nil || countCalls(r.srv.calls, "patch Secret/s") != 0 {
			t.Errorf("beforeEstablish = %v, calls %v", err, r.srv.calls)
		}
	})
	t.Run("a held holder: the record empties and unheld loses the kind's entries", func(t *testing.T) {
		r, set := disownRig(t)
		must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
		must(r.m.Create(secretKey("other", "s"), metav1.ObjectMeta{}))
		h := r.holder(t, "h", true)
		if _, err := r.p.acquire(ctx, h, []Ref{SecretRef("ns", "s"), otherSecret()}); err != nil {
			t.Fatal(err)
		}
		h = r.get(t, widgetKey("ns", "h"))
		if !ReadRecord(h).Names(SecretRef("ns", "s").ID()) || len(ReadUnheld(h.Annotations)) != 1 {
			t.Fatalf("before the switch: %v", h.Annotations)
		}
		*set = parseDisown("Secret")
		a, err := r.p.acquire(ctx, h, []Ref{SecretRef("ns", "s"), otherSecret()})
		if err != nil || a.MustNotConnect || a.Degraded || len(a.Held) != 0 {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
		h = r.get(t, widgetKey("ns", "h"))
		if ReadRecord(h).Present || h.Annotations[AnnotationUnheld] != "" {
			t.Errorf("annotations %v", h.Annotations)
		}
	})
	t.Run("one kind: the others stay held, and a disowned hop is followed", func(t *testing.T) {
		r, set := disownRig(t)
		fc := &fakeContents{r: r, specs: map[ID]map[string]any{}}
		r.p.contentKinds = map[schema.GroupKind]bool{targetGK: true}
		r.p.contents = fc
		must(r.m.Create(targetKey("c"), metav1.ObjectMeta{}))
		fc.specs[targetID("c")] = map[string]any{"ref": map[string]any{"kind": "Secret", "name": "s"}}
		must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
		c := targetRef("c")
		c.Next = func(*unstructured.Unstructured) ([]Ref, error) { return []Ref{SecretRef("ns", "s")}, nil }
		*set = parseDisown(targetGK.Kind + "." + targetGK.Group)
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{c})
		if err != nil || a.MustNotConnect || len(a.Held) != 1 || a.Held[0].ID() != SecretRef("ns", "s").ID() ||
			len(a.Unheld) != 1 || a.Unheld[0].Reason != ReasonDisowned {
			t.Fatalf("acquire = %+v, %v; want the Secret behind the disowned Target held", a, err)
		}
		if hasFinalizer(r.get(t, targetKey("c")), fNS) || !hasFinalizer(r.get(t, secretKey("ns", "s")), fNS) {
			t.Error("want the Target unheld and the Secret held")
		}
		if rec := ReadRecord(r.get(t, widgetKey("ns", "h"))); !slices.Equal(rec.IDs, []ID{SecretRef("ns", "s").ID()}) {
			t.Errorf("record %v, want only the Secret", rec.IDs)
		}
		if got, ok := Resolved(WithResolved(ctx, a), c); !ok || got.GetName() != "c" {
			t.Errorf("Resolved = %v, %v; want the disowned hop's read", got, ok)
		}

		*set = parseDisown("Secret")
		writes := countCalls(r.srv.calls, "patch Widget/h")
		a, err = r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{c})
		if n := countCalls(r.srv.calls, "patch Widget/h") - writes; n != 1 {
			t.Errorf("%d holder writes, want 1: no A1 may write back the disowned Secret the record names", n)
		}
		if err != nil || len(a.Held) != 1 || a.Held[0].ID() != targetID("c") || len(a.Unheld) != 1 || a.Unheld[0].Reason != ReasonDisowned {
			t.Fatalf("acquire = %+v, %v; want the Target held and the Secret disowned", a, err)
		}
		if rec := ReadRecord(r.get(t, widgetKey("ns", "h"))); !slices.Equal(rec.IDs, []ID{targetID("c")}) {
			t.Errorf("record %v, want only the Target", rec.IDs)
		}
	})
	t.Run("set between A1 and A2: the pass stops before its A2", func(t *testing.T) {
		r, set := disownRig(t)
		must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
		r.srv.before = func(op, kind, name string) error {
			if op == "patch" && kind == "Widget" {
				*set = parseDisown("all") // the switch lands just after A1
			}
			return nil
		}
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{SecretRef("ns", "s")})
		if err != nil || a.MustNotConnect || len(a.Held) != 0 || len(a.Unheld) != 1 || a.Unheld[0].Reason != ReasonDisowned {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
		if countCalls(r.srv.calls, "patch Secret/s") != 0 || hasFinalizer(r.get(t, secretKey("ns", "s")), fNS) {
			t.Errorf("an A2 after the switch: %v", r.srv.calls)
		}
		if ReadRecord(r.get(t, widgetKey("ns", "h"))).Present {
			t.Error("the last A1 kept the record its first A1 wrote")
		}
	})
	t.Run("Missing before the switch: BeforeEstablish doesn't hold it", func(t *testing.T) {
		r, set := disownRig(t)
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{SecretRef("ns", "s")})
		if err != nil || len(a.Unheld) != 1 || a.Unheld[0].Reason != ReasonMissing {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
		must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
		*set = parseDisown("all")
		if err := r.p.beforeEstablish(ctx, r.get(t, widgetKey("ns", "h")), a); err != nil {
			t.Fatal(err)
		}
		if hasFinalizer(r.get(t, secretKey("ns", "s")), fNS) {
			t.Error("BeforeEstablish held a ref the switch covers")
		}
	})
	t.Run("the switch off again: held as before", func(t *testing.T) {
		r, set := disownRig(t)
		must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
		*set = parseDisown("all")
		if _, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{SecretRef("ns", "s")}); err != nil {
			t.Fatal(err)
		}
		*set = disownSet{}
		a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{SecretRef("ns", "s")})
		if err != nil || len(a.Held) != 1 || !hasFinalizer(r.get(t, secretKey("ns", "s")), fNS) {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
	})
}

// TestDisownSettling: disown-settled only after DisownPasses zero passes in
// a row; a non-zero count of this process, or one read on the CRD from
// another, resets the run, as an unknown count does; the fields are written
// only when they change (§7.4).
func TestDisownSettling(t *testing.T) {
	ctx := context.Background()
	const name = "widgets.credentialuse.test.krateo.io"
	crd := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
		"metadata": map[string]any{"name": name, "resourceVersion": "1"},
	}}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{crdGVR: "CustomResourceDefinitionList"}, crd)
	s := &Shared{c: Clients{Dynamic: dyn}}
	tr := &Tracker{s: s, crdName: name, o: Options{DisownPasses: 3}, sink: sinks{logger: logging.NewNopLogger()}}
	read := func() (string, string) {
		t.Helper()
		got, err := dyn.Resource(crdGVR).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		a := got.GetAnnotations()
		return a[AnnotationHeldCount], a[AnnotationDisownSettled]
	}
	patches := func() int {
		n := 0
		for _, a := range dyn.Actions() {
			if a.GetVerb() == "patch" {
				n++
			}
		}
		return n
	}
	step := func(count int, known bool, wantCount, wantSettled string) {
		t.Helper()
		s.writeDisownCount(ctx, tr, count, known)
		if c, st := read(); c != wantCount || st != wantSettled {
			t.Fatalf("after a count of %d (known %v): held-count %q, settled %q; want %q, %q", count, known, c, st, wantCount, wantSettled)
		}
	}
	step(4, true, "4", "")
	step(0, true, "0", "")
	step(0, true, "0", "")
	before := patches()
	step(0, true, "0", "true")
	if patches() != before+1 {
		t.Errorf("%d patches to settle, want 1", patches()-before)
	}
	before = patches()
	step(0, true, "0", "true")
	if patches() != before {
		t.Error("an unchanged count was written: every CRD write rebuilds the API server's OpenAPI")
	}

	// A hold found again: the flag goes, and the run starts over.
	step(1, true, "1", "")
	step(0, true, "0", "")
	step(0, true, "0", "")
	step(0, true, "0", "true")

	// Another process's non-zero count, read on the CRD, resets this one's
	// run, and so does an unknown count.
	if _, err := dyn.Resource(crdGVR).Patch(ctx, name, types.MergePatchType,
		[]byte(`{"metadata":{"annotations":{"`+AnnotationHeldCount+`":"2","`+AnnotationDisownSettled+`":null}}}`), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	step(0, true, "0", "")
	step(0, true, "0", "")
	step(0, false, "unknown", "")
	step(0, true, "0", "")
	step(0, true, "0", "")
	step(0, true, "0", "true")
}
