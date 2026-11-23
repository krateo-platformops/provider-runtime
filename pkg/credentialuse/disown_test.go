package credentialuse

// Disown's parts on the in-memory model (plan, PR 6; §7.4).

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	metadatafake "k8s.io/client-go/metadata/fake"
	clienttesting "k8s.io/client-go/testing"
	testingclock "k8s.io/utils/clock/testing"

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

// TestStripDisownedCountsWhatItRemoved: an A2 that lands between the list
// and the strip makes the strip read again, and it reports the finalizers
// it removed, which the pass adds to its count (the review of PR 6).
func TestStripDisownedCountsWhatItRemoved(t *testing.T) {
	ctx := context.Background()
	r := newRig(t)
	fA, _ := FinalizerName(widgetGK, "a")
	fB, _ := FinalizerName(widgetGK, "b")
	k := secretKey("ns", "s")
	must(r.m.Create(k, metav1.ObjectMeta{Finalizers: []string{fA}, Labels: map[string]string{MarkerLabel(widgetGK): "true"}}))
	listed := r.get(t, k)
	// The late A2, after the list.
	if _, err := r.m.Patch(k, types.MergePatchType, []byte(`{"metadata":{"finalizers":["`+fA+`","`+fB+`"]}}`)); err != nil {
		t.Fatal(err)
	}
	wrote, removed, err := stripDisowned(ctx, r.srv, k.GK, widgetGK, listed)
	if err != nil || !wrote || removed != 2 {
		t.Fatalf("stripDisowned = %v, %d, %v; want a write that removed 2", wrote, removed, err)
	}
	if got := r.get(t, k); len(got.Finalizers) != 0 {
		t.Errorf("finalizers %v", got.Finalizers)
	}
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
	r.p.disownLive = func(context.Context) (disownSet, error) { return *set, nil }
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
	t.Run("a stale cache: the live read before the A2 stops it", func(t *testing.T) {
		r, _ := disownRig(t)
		must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
		r.p.disowned = func(schema.GroupKind) bool { return false } // the informer's watch fails
		r.p.disownLive = func(context.Context) (disownSet, error) { return parseDisown("all"), nil }
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{SecretRef("ns", "s")})
		if err != nil || a.MustNotConnect || len(a.Held) != 0 || len(a.Unheld) != 1 || a.Unheld[0].Reason != ReasonDisowned {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
		if countCalls(r.srv.calls, "patch Secret/s") != 0 {
			t.Errorf("an A2 under a stale cache: %v", r.srv.calls)
		}
		h := r.get(t, widgetKey("ns", "h"))
		must(r.m.Delete(secretKey("ns", "s")))
		a2 := Acquired{Unheld: []Unheld{{Ref: SecretRef("ns", "s"), Reason: ReasonMissing}}}
		must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
		if err := r.p.beforeEstablish(ctx, h, a2); err != nil || hasFinalizer(r.get(t, secretKey("ns", "s")), fNS) {
			t.Errorf("BeforeEstablish = %v under a stale cache, held %v", err, hasFinalizer(r.get(t, secretKey("ns", "s")), fNS))
		}
	})
	t.Run("one live read per pass, before its first A2", func(t *testing.T) {
		r, _ := disownRig(t)
		reads := 0
		r.p.disownLive = func(context.Context) (disownSet, error) { reads++; return disownSet{}, nil }
		var refs []Ref
		for _, n := range []string{"a", "b", "c"} {
			must(r.m.Create(secretKey("ns", n), metav1.ObjectMeta{}))
			refs = append(refs, SecretRef("ns", n))
		}
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), refs)
		if err != nil || len(a.Held) != 3 || reads != 1 {
			t.Fatalf("acquire = %+v, %v; %d live reads for three A2s, want 1", a, err, reads)
		}
		// A pass that writes no A2 reads nothing: the steady state, as in
		// TestAcquireSkipsInTheSteadyState.
		r.views = cacheOf{m: r.m}
		current := r.get(t, widgetKey("ns", "h"))
		r.p.ver.holderEvent(current.UID, current, r.now)
		r.tick(time.Millisecond)
		if _, err := r.p.acquire(ctx, current, refs); err != nil {
			t.Fatal(err)
		}
		reads = 0
		r.srv.calls = nil
		if _, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), refs); err != nil || len(r.srv.calls) != 0 || reads != 0 {
			t.Errorf("acquire = %v, calls %v; %d live reads for a pass that skips every A2, want 0", err, r.srv.calls, reads)
		}
	})
	t.Run("a live read that fails holds nothing", func(t *testing.T) {
		r, _ := disownRig(t)
		must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
		r.p.disownLive = func(context.Context) (disownSet, error) { return disownSet{}, errors.New("the CRD read failed") }
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{SecretRef("ns", "s")})
		if err != nil || !a.MustNotConnect || hasFinalizer(r.get(t, secretKey("ns", "s")), fNS) {
			t.Fatalf("acquire = %+v, %v; want MustNotConnect for a new holder, and no A2", a, err)
		}
	})
	t.Run("set between two A2s: the cached check stops the second", func(t *testing.T) {
		r, set := disownRig(t)
		for _, n := range []string{"a", "b"} {
			must(r.m.Create(secretKey("ns", n), metav1.ObjectMeta{}))
		}
		r.srv.before = func(op, kind, name string) error {
			if op == "patch" && kind == "Secret" && name == "a" {
				*set = parseDisown("all") // after the pass's live read, before b's A2
			}
			return nil
		}
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{SecretRef("ns", "a"), SecretRef("ns", "b")})
		if err != nil || len(a.Held) != 1 || a.Held[0].Name != "a" || len(a.Unheld) != 1 || a.Unheld[0].Reason != ReasonDisowned {
			t.Fatalf("acquire = %+v, %v; want a held and b disowned", a, err)
		}
		if countCalls(r.srv.calls, "patch Secret/b") != 0 {
			t.Errorf("b's A2 after the switch: %v", r.srv.calls)
		}

		// BeforeEstablish likewise, between its own A2s.
		*set = disownSet{}
		for _, n := range []string{"c", "d"} {
			must(r.m.Create(secretKey("ns", n), metav1.ObjectMeta{}))
		}
		r.srv.before = func(op, kind, name string) error {
			if op == "patch" && kind == "Secret" && name == "c" {
				*set = parseDisown("all")
			}
			return nil
		}
		missing := Acquired{Unheld: []Unheld{{Ref: SecretRef("ns", "c"), Reason: ReasonMissing}, {Ref: SecretRef("ns", "d"), Reason: ReasonMissing}}}
		if err := r.p.beforeEstablish(ctx, r.get(t, widgetKey("ns", "h")), missing); err != nil {
			t.Fatal(err)
		}
		if hasFinalizer(r.get(t, secretKey("ns", "d")), fNS) {
			t.Error("BeforeEstablish held d after the switch")
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
	tr := &Tracker{s: s, gk: widgetGK, crdName: name, o: Options{DisownPasses: 3}, sink: sinks{logger: logging.NewNopLogger(), metrics: NopMetrics{}}}
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
		s.writeDisownCount(ctx, tr, disownSet{}, count, known)
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
	if p := lastPatch(dyn.Actions()); p == nil || !strings.Contains(string(p.GetPatch()), `"resourceVersion"`) {
		t.Errorf("the count patch isn't guarded by the version read: %v", p)
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

	// A switch that no longer covers what the pass counted: nothing is
	// written, and the run starts over.
	step(0, true, "0", "true")
	if _, err := dyn.Resource(crdGVR).Patch(ctx, name, types.MergePatchType,
		[]byte(`{"metadata":{"annotations":{"`+AnnotationHeldCount+`":"0","`+AnnotationDisownSettled+`":null,"`+AnnotationDisown+`":"all"}}}`), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	before = patches()
	s.writeDisownCount(ctx, tr, disownSet{}, 0, true)
	if patches() != before {
		t.Error("a count for another switch was written")
	}
	all := parseDisown("all")
	for range 2 {
		s.writeDisownCount(ctx, tr, all, 0, true)
	}
	if c, st := read(); c != "0" || st != "" {
		t.Fatalf("held-count %q, settled %q after two zero passes under the new switch; want not settled", c, st)
	}
	s.writeDisownCount(ctx, tr, all, 0, true)
	if _, st := read(); st != "true" {
		t.Fatal("not settled after three zero passes under the new switch")
	}

	// A conflict reads and decides again (§7.3).
	conflicts := 1
	dyn.PrependReactor("patch", "customresourcedefinitions", func(clienttesting.Action) (bool, runtime.Object, error) {
		if conflicts > 0 {
			conflicts--
			return true, nil, apierrors.NewConflict(schema.GroupResource{Group: crdGVR.Group, Resource: crdGVR.Resource}, name, errors.New("changed"))
		}
		return false, nil, nil
	})
	s.writeDisownCount(ctx, tr, all, 1, true)
	if c, st := read(); c != "1" || st != "" {
		t.Fatalf("after a conflict: held-count %q, settled %q; want the retry to write 1", c, st)
	}

	// Every attempt conflicting, three passes in a row: starved, counted once.
	m := &countingMetrics{}
	tr.sink.metrics = m
	for i := range 3 {
		conflicts = maxCRDWriteAttempts
		s.writeDisownCount(ctx, tr, all, 2+i, true)
	}
	if n := m.count("crd_write_starved " + kindLabel(widgetGK)); n != 1 {
		t.Errorf("starved counted %d times, want 1", n)
	}
	if c, _ := read(); c != "1" {
		t.Errorf("held-count %q: a starved write landed", c)
	}
}

// TestDisownOffClearsTheCount: once the switch is removed, held-count and
// disown-settled leave the CRD, in a patch guarded by the version the
// informer has; with nothing to clear, nothing is written.
func TestDisownOffClearsTheCount(t *testing.T) {
	ctx := context.Background()
	const name = "widgets.credentialuse.test.krateo.io"
	crd := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
		"metadata": map[string]any{"name": name, "resourceVersion": "9", "annotations": map[string]any{
			AnnotationHeldCount: "0", AnnotationDisownSettled: "true",
		}},
	}}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{crdGVR: "CustomResourceDefinitionList"}, crd)
	s := &Shared{c: Clients{Dynamic: dyn}}
	inf, err := newCRDInformer(metadatafake.NewSimpleMetadataClient(metadatafake.NewTestScheme()), name, testingclock.NewFakeClock(time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	must(inf.GetStore().Add(&metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Name: name, ResourceVersion: "9",
		Annotations: map[string]string{AnnotationHeldCount: "0", AnnotationDisownSettled: "true"}}}))
	tr := &Tracker{s: s, gk: widgetGK, crd: inf, crdName: name, o: Options{DisownInterval: time.Minute}, sink: sinks{logger: logging.NewNopLogger()}}
	tr.disown.zeros, tr.disown.value = 4, "all"
	now := time.Now()
	s.disownOff(ctx, tr, now)
	got, err := dyn.Resource(crdGVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if a := got.GetAnnotations(); a[AnnotationHeldCount] != "" || a[AnnotationDisownSettled] != "" {
		t.Errorf("annotations %v, want the count cleared", a)
	}
	if p := lastPatch(dyn.Actions()); p == nil || !strings.Contains(string(p.GetPatch()), `"resourceVersion":"9"`) {
		t.Errorf("the clearing patch isn't guarded by the informer's version: %v", p)
	}
	if tr.disown.zeros != 0 || tr.disown.value != "" {
		t.Errorf("the run survived the switch's removal: zeros %d, value %q", tr.disown.zeros, tr.disown.value)
	}
	// Within the interval, no second try.
	n := len(dyn.Actions())
	s.disownOff(ctx, tr, now.Add(time.Second))
	if len(dyn.Actions()) != n {
		t.Error("a second clearing within DisownInterval")
	}
}

// TestDisownPrefixRefusesWhenTheCRDListFails: whether the CRD is gone can't
// be known, so nothing is stripped.
func TestDisownPrefixRefusesWhenTheCRDListFails(t *testing.T) {
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{crdGVR: "CustomResourceDefinitionList"})
	dyn.PrependReactor("list", "customresourcedefinitions", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("the list failed")
	})
	md := metadatafake.NewSimpleMetadataClient(metadatafake.NewTestScheme())
	c := Clients{Metadata: md, Dynamic: dyn, Mapper: meta.NewDefaultRESTMapper(nil)}
	_, err := DisownPrefix(context.Background(), c, DisownPrefixOptions{Holder: widgetGK, Names: []Ref{SecretRef("ns", "s")}})
	if err == nil || !strings.Contains(err.Error(), "the list failed") {
		t.Errorf("DisownPrefix = %v, want the list's failure", err)
	}
	for _, a := range md.Actions() {
		t.Errorf("an API call despite the refusal: %v", a)
	}
}

// lastPatch is the last patch among acts, or nil.
func lastPatch(acts []clienttesting.Action) clienttesting.PatchAction {
	for i := len(acts) - 1; i >= 0; i-- {
		if p, ok := acts[i].(clienttesting.PatchAction); ok {
			return p
		}
	}
	return nil
}

// TestDisownPassesAreAnIntervalApart: a tracker's next pass is due an
// interval after this one was about to start, not after the loop's look,
// however long another tracker's pass took in between: the passes of a run
// span its intervals (§7.4).
func TestDisownPassesAreAnIntervalApart(t *testing.T) {
	r := newFakeRig(t)
	trackers := map[string]*Tracker{"widgets": r.track(t, widgetGK), "gadgets": r.track(t, gadgetGK)}
	for _, tr := range trackers {
		must(tr.crd.GetStore().Add(&metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Name: tr.crdName,
			Annotations: map[string]string{AnnotationDisown: "all"}}}))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := map[string]time.Time{}
	r.md.PrependReactor("list", "*", func(a clienttesting.Action) (bool, runtime.Object, error) {
		res := a.GetResource().Resource
		if trackers[res] != nil && started[res].IsZero() {
			started[res] = r.clk.Now()
			r.clk.Step(30 * time.Second) // the pass takes this long
			if len(started) == len(trackers) {
				cancel()
			}
		}
		return false, nil, nil
	})
	r.s.runDisown(ctx)
	if len(started) != len(trackers) {
		t.Fatalf("passes started: %v, want one of each tracker", started)
	}
	for res, tr := range trackers {
		if want := started[res].Add(tr.o.DisownInterval); tr.disown.next.Before(want) {
			t.Errorf("%s: the next pass is due %s before an interval after this one started", res, want.Sub(tr.disown.next))
		}
	}
}

// TestDisownPassCounts: a pass counts a holder whose record names a covered
// ref, with no finalizer anywhere, and a list that fails makes the count
// unknown, never zero (§7.4).
func TestDisownPassCounts(t *testing.T) {
	ctx := context.Background()
	crdCount := func(t *testing.T, r *fakeRig) string {
		t.Helper()
		crd, err := r.dyn.Resource(crdGVR).Get(ctx, "widgets."+widgetGK.Group, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return crd.GetAnnotations()[AnnotationHeldCount]
	}
	// The pass's count is written only under the switch it counted for.
	switchOn := func(t *testing.T, r *fakeRig) {
		t.Helper()
		if _, err := r.dyn.Resource(crdGVR).Patch(ctx, "widgets."+widgetGK.Group, types.MergePatchType,
			[]byte(`{"metadata":{"annotations":{"`+AnnotationDisown+`":"all"}}}`), metav1.PatchOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("a record alone", func(t *testing.T) {
		r := newFakeRig(t, holderObject("ns", "h", SecretRef("ns", "s").ID()))
		tr := r.track(t, widgetGK)
		switchOn(t, r)
		r.s.disownPass(ctx, tr, parseDisown("all"))
		if got := crdCount(t, r); got != "1" {
			t.Errorf("held-count %q, want 1: the record is a hold to strip", got)
		}
		r.s.disownPass(ctx, tr, parseDisown("all"))
		if got := crdCount(t, r); got != "0" {
			t.Errorf("held-count %q after the strip, want 0", got)
		}
	})
	t.Run("a list that fails", func(t *testing.T) {
		r := newFakeRig(t)
		tr := r.track(t, widgetGK)
		switchOn(t, r)
		r.forbidList.Store("secrets", true)
		r.s.disownPass(ctx, tr, parseDisown("all"))
		if got := crdCount(t, r); got != "unknown" {
			t.Errorf("held-count %q, want unknown", got)
		}
	})
}
