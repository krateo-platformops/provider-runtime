package credentialuse

// Disown's parts on the in-memory model (plan, PR 6; §7.4).

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
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

// A lost index, missing or without the marker, has one report, its own: not
// also that of an index that can't be read (§7.4).
func TestALostIndexHasOneReport(t *testing.T) {
	ctx := context.Background()
	for name, st := range map[string]*memStore{"missing": {}, "without the marker": {exists: true}} {
		r := newFakeRig(t)
		tr, err := r.s.Track(ctx, r.options(widgetGK, ProtectedKind{GVK: secretGVK}))
		if err != nil {
			t.Fatal(err)
		}
		// As a PerName kind, with an index of the test's own.
		tr.o.Protected = []ProtectedKind{{GVK: secretGVK, Watch: PerName}}
		tr.idx = testIndex(st, r.s.newServer())
		r.s.disownPass(ctx, tr, parseDisown("all"))
		var got []string
		for len(r.events.Events) > 0 {
			got = append(got, <-r.events.Events)
		}
		if len(got) != 1 || !strings.Contains(got[0], EventIndexLost) {
			t.Errorf("%s: events %q, want one %s", name, got, EventIndexLost)
		}
	}
}

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
		"metadata": map[string]any{"name": name, "resourceVersion": "1", "annotations": map[string]any{AnnotationDisown: "Secret"}},
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
		s.writeDisownCount(ctx, tr, parseDisown("Secret"), count, known)
		if c, st := read(); c != wantCount || st != wantSettled {
			t.Fatalf("after a count of %d (known %v): held-count %q, settled %q; want %q, %q", count, known, c, st, wantCount, wantSettled)
		}
	}
	step(4, true, "4", "")
	step(0, true, "0", "")
	step(0, true, "0", "")
	before := patches()
	step(0, true, "0", "Secret")
	if patches() != before+1 {
		t.Errorf("%d patches to settle, want 1", patches()-before)
	}
	if p := lastPatch(dyn.Actions()); p == nil || !strings.Contains(string(p.GetPatch()), `"resourceVersion"`) {
		t.Errorf("the count patch isn't guarded by the version read: %v", p)
	}
	before = patches()
	step(0, true, "0", "Secret")
	if patches() != before {
		t.Error("an unchanged count was written: every CRD write rebuilds the API server's OpenAPI")
	}

	// A hold found again: the flag goes, and the run starts over.
	step(1, true, "1", "")
	step(0, true, "0", "")
	step(0, true, "0", "")
	step(0, true, "0", "Secret")

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
	step(0, true, "0", "Secret")

	// A switch that no longer covers what the pass counted: nothing is
	// written, and the run starts over.
	step(0, true, "0", "Secret")
	if _, err := dyn.Resource(crdGVR).Patch(ctx, name, types.MergePatchType,
		[]byte(`{"metadata":{"annotations":{"`+AnnotationHeldCount+`":"0","`+AnnotationDisownSettled+`":null,"`+AnnotationDisown+`":"all"}}}`), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	before = patches()
	s.writeDisownCount(ctx, tr, parseDisown("Secret"), 0, true)
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
	if _, st := read(); st != "all" {
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

// TestDisownSettledIsTheSwitchsValue: Disown has settled only under the
// switch's value as written, whatever the kinds it covers, and never with no
// switch; anything else left there is stale, an old "true" included. The
// index's lines stay until then (§7.4; the user's decision, 2026-11-23).
func TestDisownSettledIsTheSwitchsValue(t *testing.T) {
	ann := func(kv ...string) map[string]string {
		out := map[string]string{}
		for i := 0; i < len(kv); i += 2 {
			out[kv[i]] = kv[i+1]
		}
		return out
	}
	d, c, s := AnnotationDisown, AnnotationHeldCount, AnnotationDisownSettled
	for name, tc := range map[string]struct {
		ann            map[string]string
		settled, stale bool
	}{
		"settled":                   {ann(d, "all", c, "0", s, "all"), true, false},
		"settled, some kinds":       {ann(d, "Secret, ConfigMap", c, "0", s, "Secret, ConfigMap"), true, false},
		"not settled":               {ann(d, "all", c, "0"), false, false},
		"no switch, nothing left":   {nil, false, false},
		"no switch, a count left":   {ann(c, "0"), false, false},
		"no switch, a flag left":    {ann(c, "0", s, "all"), false, true},
		"no switch, an empty flag":  {ann(c, "0", s, ""), false, false},
		"an empty switch and flag":  {ann(d, "", c, "0", s, ""), false, false},
		"a count":                   {ann(d, "all", c, "1", s, "all"), false, false},
		"unknown":                   {ann(d, "all", c, "unknown", s, "all"), false, false},
		"widened":                   {ann(d, "all", c, "0", s, "Secret"), false, true},
		"narrowed":                  {ann(d, "Secret", c, "0", s, "all"), false, true},
		"reordered":                 {ann(d, "ConfigMap,Secret", c, "0", s, "Secret,ConfigMap"), false, true},
		"another spacing":           {ann(d, "Secret, ConfigMap", c, "0", s, "Secret,ConfigMap"), false, true},
		"an old build's true":       {ann(d, "all", c, "0", s, "true"), false, true},
		"an old true and no switch": {ann(c, "0", s, "true"), false, true},
	} {
		if got := DisownSettled(tc.ann); got != tc.settled {
			t.Errorf("%s: settled %v, want %v", name, got, tc.settled)
		}
		if got := staleSettled(tc.ann); got != tc.stale {
			t.Errorf("%s: stale %v, want %v", name, got, tc.stale)
		}
		if got, want := keptKinds(tc.ann).on(), parseDisown(tc.ann[d]).on() && !tc.settled; got != want {
			t.Errorf("%s: the lines stay: %v, want %v", name, got, want)
		}
	}
}

// TestDisownRunsOverAnUnchangedCRD: a run of zero passes is of passes over
// one version of the CRD, but for this process's own writes: a switch turned
// off and on again, whatever the informer delivered, starts it over. A
// process whose run isn't done leaves the flag of one whose run is (§7.4).
func TestDisownRunsOverAnUnchangedCRD(t *testing.T) {
	on := map[string]string{AnnotationDisown: "all", AnnotationHeldCount: "0"}
	done := map[string]string{AnnotationDisown: "all", AnnotationHeldCount: "0", AnnotationDisownSettled: "all"}
	stale := map[string]string{AnnotationDisown: "all", AnnotationHeldCount: "0", AnnotationDisownSettled: "true"}
	pass := func(d *disownState, ann map[string]string, start, end, cur string) (bool, bool) {
		if d.startRV = start; cur == "0" {
			d.zeros++
		} else {
			d.zeros = 0
		}
		settled, write := d.decide(ann, end, cur, 3)
		d.rv = end
		return settled, write
	}
	d := &disownState{}
	for i, want := range []bool{false, false, true} {
		if settled, write := pass(d, on, "5", "5", "0"); settled != want || write != want {
			t.Errorf("pass %d over an unchanged CRD: settled %v, write %v, want %v", i+1, settled, write, want)
		}
	}
	// Changed since the last pass: this one is the first of a new run.
	d = &disownState{}
	pass(d, on, "5", "5", "0")
	pass(d, on, "5", "5", "0")
	for i, want := range []bool{false, false, true} {
		if settled, _ := pass(d, on, "7", "7", "0"); settled != want {
			t.Errorf("pass %d after the CRD changed: settled %v, want %v", i+1, settled, want)
		}
	}
	// Changed during a pass: that pass is of no run.
	d = &disownState{}
	pass(d, on, "5", "5", "0")
	pass(d, on, "5", "6", "0")
	for i, want := range []bool{false, false, true} {
		if settled, _ := pass(d, on, "6", "6", "0"); settled != want {
			t.Errorf("pass %d after the CRD changed during one: settled %v, want %v", i+1, settled, want)
		}
	}
	d = &disownState{}
	if settled, write := pass(d, done, "5", "5", "0"); settled || write {
		t.Errorf("another process's flag, of this switch: settled %v, write %v; want it left alone", settled, write)
	}
	if settled, write := pass(d, stale, "5", "5", "0"); settled || !write {
		t.Errorf("a flag of another value: settled %v, write %v; want it cleared", settled, write)
	}
	if settled, write := pass(d, done, "5", "5", "1"); settled || !write {
		t.Errorf("a hold under another process's flag: settled %v, write %v; want it cleared", settled, write)
	}
}

// TestDisownsFirstPassNeedsItsCRD: a pass that couldn't read the CRD as it
// started is of no run: the switch may have been set during it (§7.4).
func TestDisownsFirstPassNeedsItsCRD(t *testing.T) {
	ctx := context.Background()
	r := newFakeRig(t)
	tr := r.track(t, widgetGK)
	if _, err := r.dyn.Resource(crdGVR).Patch(ctx, tr.crdName, types.MergePatchType,
		[]byte(`{"metadata":{"annotations":{"`+AnnotationDisown+`":"all"}}}`), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	fail := true
	r.dyn.PrependReactor("get", "customresourcedefinitions", func(clienttesting.Action) (bool, runtime.Object, error) {
		return fail, nil, errors.New("the CRD can't be read")
	})
	r.s.disownStarts(ctx, tr)
	fail = false
	for i := range tr.o.DisownPasses + 1 {
		if i > 0 {
			r.s.disownStarts(ctx, tr)
		}
		if got, want := r.s.writeDisownCount(ctx, tr, parseDisown("all"), 0, true), i == tr.o.DisownPasses; got != want {
			t.Errorf("pass %d: settled %v, want %v", i+1, got, want)
		}
	}
}

// TestDisownReportsARunThatKeepsRestarting: the CRD changes under every
// pass, as with two writers that never agree: Disown doesn't settle, and
// writes nothing; each restart is counted; and once they have gone on for two
// spans, the CRD's newest writer at them is reported, at most once per
// eventThrottle, and neither older ones nor, though it is newer, the entry of
// the count's and the flag's own writes. Once the CRD
// stays as it is, Disown settles, and nothing more is counted (§7.4; the
// user's decision, 2026-11-23).
func TestDisownReportsARunThatKeepsRestarting(t *testing.T) {
	ctx := context.Background()
	r := newFakeRig(t)
	tr := r.track(t, widgetGK)
	entry := func(manager, subresource, fields string, age time.Duration) metav1.ManagedFieldsEntry {
		at := metav1.NewTime(r.clk.Now().Add(-age))
		return metav1.ManagedFieldsEntry{Manager: manager, Operation: metav1.ManagedFieldsOperationUpdate, APIVersion: "apiextensions.k8s.io/v1",
			Time: &at, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(fields), Subresource: subresource}
	}
	crd := crdOf(widgetGK)
	crd.SetAnnotations(map[string]string{AnnotationDisown: "all", AnnotationHeldCount: "0"})
	crd.SetManagedFields([]metav1.ManagedFieldsEntry{
		entry("older", "", `{"f:spec":{"f:scope":{}}}`, time.Hour),
		entry("hub-writer", "", `{"f:spec":{"f:versions":{}}}`, time.Minute),
		entry("kube-apiserver", "status", `{"f:status":{"f:conditions":{}}}`, 2*time.Minute),
		entry("the-tracker", "", `{"f:metadata":{"f:annotations":{"f:`+AnnotationHeldCount+`":{}}}}`, 0),
		entry("spoke-writer", "", `{"f:metadata":{"f:annotations":{"f:`+AnnotationHeldCount+`":{}},"f:labels":{"f:x":{}}}}`, time.Second),
		entry("oldest", "", `{"f:spec":{"f:group":{}}}`, 2*time.Hour),
	})
	rv, changing := 1, true
	r.dyn.PrependReactor("get", "customresourcedefinitions", func(clienttesting.Action) (bool, runtime.Object, error) {
		if changing {
			rv++
		}
		u := crd.DeepCopy()
		u.SetResourceVersion(strconv.Itoa(rv))
		return true, u, nil
	})
	var patches int
	r.dyn.PrependReactor("patch", "customresourcedefinitions", func(clienttesting.Action) (bool, runtime.Object, error) {
		patches++
		u := crd.DeepCopy()
		u.SetResourceVersion(strconv.Itoa(rv))
		return true, u, nil
	})
	pass := func() bool {
		r.s.disownStarts(ctx, tr)
		settled := r.s.writeDisownCount(ctx, tr, parseDisown("all"), 0, true)
		r.clk.Step(tr.o.DisownInterval)
		return settled
	}
	reports := func() []string {
		return slices.DeleteFunc(r.takeEvents(), func(e string) bool { return !strings.Contains(e, EventDisownRestarted) })
	}
	span := time.Duration(tr.o.DisownPasses-1) * tr.o.DisownInterval
	restarts := func() int { return r.metrics.count("disown_restarts " + kindLabel(widgetGK)) }
	n := int(2 * span / tr.o.DisownInterval)
	for range n {
		if pass() {
			t.Fatal("settled under a CRD that keeps changing")
		}
	}
	if got := reports(); len(got) != 0 || restarts() != n {
		t.Errorf("before two spans: reports %v, %d restarts counted; want none, and %d", got, restarts(), n)
	}
	pass()
	got := reports()
	if len(got) != 1 {
		t.Fatalf("after two spans: %d reports %v, want one", len(got), got)
	}
	if !strings.Contains(got[0], "spoke-writer (Update,") || !strings.Contains(got[0], strconv.Itoa(n+1)+" times") {
		t.Errorf("the report %q doesn't name the newest writer, or not how often", got[0])
	}
	for _, other := range []string{"the-tracker", "hub-writer", "kube-apiserver", "older", "oldest"} {
		if strings.Contains(got[0], other) {
			t.Errorf("the report %q names %s, which made no restart", got[0], other)
		}
	}
	for range int(eventThrottle/tr.o.DisownInterval) - 1 {
		pass()
	}
	if got := reports(); len(got) != 0 {
		t.Errorf("reported again within its throttle: %v", got)
	}
	pass()
	if got := reports(); len(got) != 1 {
		t.Errorf("%d reports once the throttle is over, want one", len(got))
	}
	if patches != 0 {
		t.Errorf("%d writes of an unchanged count", patches)
	}
	changing = false
	before, settled := restarts(), false
	for range tr.o.DisownPasses + 1 {
		settled = pass()
	}
	if !settled || patches == 0 {
		t.Errorf("settled %v, with %d writes, once the CRD stays as it is", settled, patches)
	}
	// The change seen at the first of those passes was the last.
	if n := restarts() - before; n > 1 {
		t.Errorf("%d restarts counted over a CRD that stays as it is", n)
	}
}

// TestDisownReportsWritersThatOverwriteEachOther: writers of one field, each
// of which takes it, and so its entry in managedFields, from the one before:
// the report names the newest at each restart of the streak, the last three,
// each with its operation and subresource; and nobody of a streak before
// (§7.4; the user's decision, 2026-11-23).
func TestDisownReportsWritersThatOverwriteEachOther(t *testing.T) {
	ctx := context.Background()
	r := newFakeRig(t)
	tr := r.track(t, widgetGK)
	crd := crdOf(widgetGK)
	crd.SetAnnotations(map[string]string{AnnotationDisown: "all", AnnotationHeldCount: "0"})
	rv := 1
	answer := func(clienttesting.Action) (bool, runtime.Object, error) {
		rv++
		u := crd.DeepCopy()
		u.SetResourceVersion(strconv.Itoa(rv))
		return true, u, nil
	}
	r.dyn.PrependReactor("get", "customresourcedefinitions", answer)
	r.dyn.PrependReactor("patch", "customresourcedefinitions", answer)
	pass := func(writer string, count int) {
		manager, subresource, _ := strings.Cut(writer, "/")
		at := metav1.NewTime(r.clk.Now())
		crd.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: manager, Operation: metav1.ManagedFieldsOperationUpdate, Subresource: subresource,
			APIVersion: "apiextensions.k8s.io/v1", Time: &at, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:metadata":{"f:labels":{"f:x":{}}}}`)}})
		r.s.disownStarts(ctx, tr)
		r.s.writeDisownCount(ctx, tr, parseDisown("all"), count, true)
		r.clk.Step(tr.o.DisownInterval)
	}
	for _, streak := range []struct{ writers, want []string }{
		{[]string{"writer-a", "writer-b/status"}, []string{"writer-a (Update,", "writer-b (Update of status,"}},
		{[]string{"writer-c"}, []string{"writer-c (Update,"}},
		{[]string{"writer-d", "writer-e", "writer-f", "writer-g"}, []string{"writer-d (Update,", "writer-g (Update,", "writer-f (Update,"}},
	} {
		// Two spans, and the pass that reports.
		for i := range 2*(tr.o.DisownPasses-1) + 1 {
			pass(streak.writers[i%len(streak.writers)], 0)
		}
		got := slices.DeleteFunc(r.takeEvents(), func(e string) bool { return !strings.Contains(e, EventDisownRestarted) })
		if len(got) != 1 {
			t.Fatalf("%v: %d reports %v, want one", streak.writers, len(got), got)
		}
		if n := strings.Count(got[0], "writer-"); n != len(streak.want) {
			t.Errorf("%v: the report %q names %d writers, want %d", streak.writers, got[0], n, len(streak.want))
		}
		at := -1
		for _, w := range streak.want {
			i := strings.Index(got[0], w)
			if i <= at {
				t.Errorf("%v: the report %q doesn't name %v, the last first", streak.writers, got[0], streak.want)
			}
			at = i
		}
		pass("writer-x", 1) // a hold ends the streak
		r.clk.Step(eventThrottle)
	}
}

// TestAHoldClearsAStaleFlag: before a pass's first A2, a disown-settled that
// a switch of another value left is removed, guarded by the version the
// switch was read at; a flag of the value it has now stays. One that can't be
// removed fails the read, and so the A2, naming the grant (§7.4).
func TestAHoldClearsAStaleFlag(t *testing.T) {
	ctx := context.Background()
	const name = "widgets.credentialuse.test.krateo.io"
	gvk := crdGVR.GroupVersion().WithKind("CustomResourceDefinition")
	for title, tc := range map[string]struct {
		ann   map[string]string
		fail  error
		clear bool
		want  string // in the error
	}{
		"off, a flag left":      {ann: map[string]string{AnnotationHeldCount: "0", AnnotationDisownSettled: "all"}, clear: true},
		"off, an old true left": {ann: map[string]string{AnnotationHeldCount: "0", AnnotationDisownSettled: "true"}, clear: true},
		"narrowed":              {ann: map[string]string{AnnotationDisown: "Secret", AnnotationHeldCount: "0", AnnotationDisownSettled: "all"}, clear: true},
		"its own":               {ann: map[string]string{AnnotationDisown: "Secret", AnnotationHeldCount: "0", AnnotationDisownSettled: "Secret"}},
		"nothing":               {},
		"a conflict": {ann: map[string]string{AnnotationDisownSettled: "all"}, clear: true, want: "leftover",
			fail: apierrors.NewConflict(schema.GroupResource{Group: crdGVR.Group, Resource: crdGVR.Resource}, name, errors.New("changed"))},
		"no grant": {ann: map[string]string{AnnotationDisownSettled: "all"}, clear: true, want: "grant patch on customresourcedefinitions",
			fail: apierrors.NewForbidden(schema.GroupResource{Group: crdGVR.Group, Resource: crdGVR.Resource}, name,
				errors.New(`User "u" cannot patch resource "customresourcedefinitions" in API group "apiextensions.k8s.io" at the cluster scope`))},
	} {
		m := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Name: name, ResourceVersion: "9", Annotations: tc.ann}}
		m.SetGroupVersionKind(gvk)
		scheme := metadatafake.NewTestScheme()
		must(metav1.AddMetaToScheme(scheme))
		crd := &unstructured.Unstructured{Object: map[string]any{"apiVersion": gvk.GroupVersion().String(), "kind": gvk.Kind,
			"metadata": map[string]any{"name": name, "resourceVersion": "9"}}}
		dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
			map[schema.GroupVersionResource]string{crdGVR: "CustomResourceDefinitionList"}, crd)
		dyn.PrependReactor("patch", "customresourcedefinitions", func(clienttesting.Action) (bool, runtime.Object, error) {
			return tc.fail != nil, nil, tc.fail
		})
		tr := &Tracker{s: &Shared{c: Clients{Dynamic: dyn, Metadata: metadatafake.NewSimpleMetadataClient(scheme, m)}}, gk: widgetGK, crdName: name}
		set, err := tr.disownForHold(ctx)
		if (err != nil) != (tc.want != "") || err != nil && !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want an error naming %q", title, err, tc.want)
		}
		if err == nil && set.key() != parseDisown(tc.ann[AnnotationDisown]).key() {
			t.Errorf("%s: the switch read is %+v", title, set)
		}
		p := lastPatch(dyn.Actions())
		if (p != nil) != tc.clear {
			t.Errorf("%s: patched %v, want %v", title, p != nil, tc.clear)
		}
		if p != nil && !strings.Contains(string(p.GetPatch()), `"resourceVersion":"9"`) {
			t.Errorf("%s: the patch isn't guarded by the version read: %s", title, p.GetPatch())
		}
	}
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
	// The objects first, and a record only once those it names are stripped:
	// while one may be held, a record names it (§7.4).
	t.Run("the order of the strips", func(t *testing.T) {
		s := heldObject(secretGVK, "ns", "s", "uid-s", widgetGK, "ns")
		s.Labels[MarkerLabel(widgetGK)] = "true"
		r := newFakeRig(t, s, holderObject("ns", "h", secretID("ns", "s")))
		tr := r.track(t, widgetGK)
		switchOn(t, r)
		var mu sync.Mutex
		var order []string
		failing := true
		r.md.PrependReactor("patch", "*", func(a clienttesting.Action) (bool, runtime.Object, error) {
			mu.Lock()
			defer mu.Unlock()
			res := a.GetResource().Resource
			order = append(order, res)
			if failing && res == "secrets" {
				return true, nil, apierrors.NewInternalError(errors.New("down"))
			}
			return false, nil, nil
		})
		r.s.disownPass(ctx, tr, parseDisown("all"))
		if want := []string{"secrets"}; !slices.Equal(order, want) {
			t.Errorf("with the Secret's strip failing, patched %v, want %v: its record stays", order, want)
		}
		if got := crdCount(t, r); got != "2" {
			t.Errorf("held-count %q, want 2: the record and the finalizer", got)
		}
		failing, order = false, nil
		r.s.disownPass(ctx, tr, parseDisown("all"))
		if want := []string{"secrets", "widgets"}; !slices.Equal(order, want) {
			t.Errorf("patched %v, want %v", order, want)
		}
		r.s.disownPass(ctx, tr, parseDisown("all"))
		if got := crdCount(t, r); got != "0" {
			t.Errorf("held-count %q after the strips, want 0", got)
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
