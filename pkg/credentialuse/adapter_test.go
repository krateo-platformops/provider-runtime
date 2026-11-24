package credentialuse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/provider-runtime/pkg/logging"
)

// The adapter API (plan, PR 7): IsOwnRecordWrite, CheckRead, SummarizeUnheld
// and WithResolvedObjects, on the fake rig.

// newAdapterHolder is a new Widget, as the metadata fake stores it, with no
// record.
func newAdapterHolder(ns, name string) *metav1.PartialObjectMetadata {
	m := holderObject(ns, name)
	m.Finalizers = nil
	m.ResourceVersion = "1"
	return m
}

// M6 and L8: IsOwnRecordWrite is true only for the record this process's
// last A1 wrote, with nothing else changed; it is false for another change,
// another record, a record it didn't write, and once the holder is
// forgotten.
func TestIsOwnRecordWrite(t *testing.T) {
	h := newAdapterHolder("ns", "w")
	r := newFakeRig(t, h, namespaceObject("ns"))
	tr := r.track(t, widgetGK)
	r.setRefs("w", SecretRef("ns", "creds"))
	obj := h.DeepCopy()
	if _, err := tr.Acquire(context.Background(), obj); err != nil {
		t.Fatal(err)
	}
	if obj.Annotations[AnnotationRefs] == "" {
		t.Fatal("Acquire wrote no record")
	}
	before := h.DeepCopy()
	after := obj.DeepCopy()
	if !tr.IsOwnRecordWrite(before, after) {
		t.Error("the record write this process made isn't its own")
	}
	changed := after.DeepCopy()
	changed.Labels = map[string]string{"x": "y"}
	if tr.IsOwnRecordWrite(before, changed) {
		t.Error("a record write with a label change counts as the record write alone")
	}
	other := after.DeepCopy()
	other.Annotations[AnnotationRefs] = `[{"group":"","kind":"Secret","namespace":"ns","name":"other"}]`
	if tr.IsOwnRecordWrite(before, other) {
		t.Error("a record this process didn't write counts as its own")
	}
	if tr.IsOwnRecordWrite(after, after.DeepCopy()) {
		t.Error("an update that changes no record counts as a record write")
	}
	// Typed and unstructured objects compare whole.
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(widgetGK.WithVersion("v1"))
	u.SetNamespace("ns")
	u.SetName("w")
	u.SetUID(h.UID)
	u2 := u.DeepCopy()
	u2.SetAnnotations(after.Annotations)
	if !tr.IsOwnRecordWrite(u, u2) {
		t.Error("an unstructured record write isn't its own")
	}
	_ = unstructured.SetNestedField(u2.Object, "changed", "spec", "x")
	if tr.IsOwnRecordWrite(u, u2) {
		t.Error("an unstructured record write with a spec change counts as the record write alone")
	}
	tr.Forget(obj)
	if tr.IsOwnRecordWrite(before, after) {
		t.Error("a forgotten holder's record write still counts as its own")
	}
}

// §11.9: CheckRead counts every read of a protected kind that the holder's
// record, its unheld list and its refs don't name, and logs it once per
// holder and object; a named read, and a kind the tracker doesn't protect,
// count nothing.
func TestCheckRead(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"))
	tr := r.track(t, widgetGK)
	rec, _ := EncodeRecord([]ID{SecretRef("ns", "recorded").ID()})
	h := newAdapterHolder("ns", "w")
	h.Annotations = map[string]string{
		AnnotationRefs:   rec,
		AnnotationUnheld: `[{"ref":"team-b/Secret/unheld","reason":"NotConsented","since":"2026-11-08T17:02:11Z"}]`,
	}
	r.setRefs("w", SecretRef("ns", "named"), Ref{GroupKind: secretGroupKind, Namespace: "ns", Name: "guaranteed", Guaranteed: true})
	secret := schema.GroupKind{Kind: "Secret"}
	for _, read := range []struct{ ns, name string }{{"ns", "recorded"}, {"team-b", "unheld"}, {"ns", "named"}, {"ns", "guaranteed"}} {
		tr.CheckRead(h, secret, read.ns, read.name)
	}
	tr.CheckRead(h, schema.GroupKind{Kind: "ConfigMap"}, "ns", "unprotected")
	if n := r.metrics.count("unrecorded_reads " + kindLabel(widgetGK)); n != 0 {
		t.Fatalf("named reads counted %d unrecorded reads, want 0", n)
	}
	tr.CheckRead(h, secret, "ns", "unnamed")
	tr.CheckRead(h, secret, "ns", "unnamed")
	if n := r.metrics.count("unrecorded_reads " + kindLabel(widgetGK)); n != 2 {
		t.Errorf("two unnamed reads counted %d, want 2", n)
	}
}

// §12: the summary counts the entries folded into More, and names the most
// recent entry.
func TestSummarizeUnheld(t *testing.T) {
	if _, ok := SummarizeUnheld(nil); ok {
		t.Error("an absent list summarizes")
	}
	if _, ok := SummarizeUnheld(map[string]string{AnnotationUnheld: "[]"}); ok {
		t.Error("an empty list summarizes")
	}
	got, ok := SummarizeUnheld(map[string]string{AnnotationUnheld: `[` +
		`{"ref":"a/Secret/x","reason":"ConsentWithdrawn","since":"2026-11-08T17:02:11Z"},` +
		`{"ref":"a/Secret/y","reason":"NotConsented","since":"2026-11-09T10:00:00Z"},` +
		`{"ref":"…","reason":"More","count":5}]`})
	want := UnheldSummary{Count: 7, Reason: "NotConsented", Since: time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)}
	if !ok || got.Count != want.Count || got.Reason != want.Reason || !got.Since.Equal(want.Since) {
		t.Errorf("SummarizeUnheld = %+v, %v; want %+v", got, ok, want)
	}
}

// WithResolvedObjects hands Resolved copies of the objects given, by ID.
func TestWithResolvedObjects(t *testing.T) {
	target := &unstructured.Unstructured{}
	target.SetName("t")
	ref := Ref{GroupKind: schema.GroupKind{Group: "g", Kind: "Target"}, Namespace: "ns", Name: "t"}
	ctx := WithResolvedObjects(context.Background(), map[ID]*unstructured.Unstructured{ref.ID(): target})
	got, ok := Resolved(ctx, ref)
	if !ok || got.GetName() != "t" {
		t.Fatalf("Resolved = %v, %v", got, ok)
	}
	got.SetName("changed")
	if again, _ := Resolved(ctx, ref); again.GetName() != "t" || target.GetName() != "t" {
		t.Error("Resolved handed out the stored object, not a copy")
	}
}

func (m *countingMetrics) UnrecordedReads(kind string) { m.inc("unrecorded_reads " + kind) }

// §7.5, L1: a tracker has synced only once its informers ran and its
// handlers had their initial state.
func TestTrackerHasSynced(t *testing.T) {
	r := newFakeRig(t, namespaceObject("ns"))
	tr := r.track(t, widgetGK)
	if tr.HasSynced() {
		t.Fatal("a tracker whose informers never ran has synced")
	}
	r.run(t)
	eventually(t, "the tracker to sync", tr.HasSynced)
}

// The record-write memory goes with the holder: an entry is dropped when the
// holder informer sees it deleted.
func TestRecordWritesGoWithTheHolder(t *testing.T) {
	h := newAdapterHolder("ns", "w")
	r := newFakeRig(t, h, namespaceObject("ns"))
	tr := r.track(t, widgetGK)
	r.run(t)
	r.setRefs("w", SecretRef("ns", "creds"))
	if _, err := tr.Acquire(context.Background(), h.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	if _, ok := tr.proto.written.get(h.UID); !ok {
		t.Fatal("no record write remembered")
	}
	gvr := schema.GroupVersionResource{Group: widgetGK.Group, Version: "v1", Resource: "widgets"}
	if err := r.md.Resource(gvr).Namespace("ns").Delete(context.Background(), "w", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the record write to be forgotten", func() bool {
		_, ok := tr.proto.written.get(h.UID)
		return !ok
	})
}

// warnCount counts a logger's Warn calls.
type warnCount struct {
	logging.Logger
	n atomic.Int64
}

func (w *warnCount) Warn(string, ...any) { w.n.Add(1) }

// §8: a degraded pass is logged once per holder while it stays degraded; a
// pass that isn't degraded, or Forget, clears that, so the next degraded pass
// logs again.
func TestDegradedIsLoggedOncePerHolder(t *testing.T) {
	h := holderObject("ns", "w") // established
	r := newFakeRig(t, h, namespaceObject("ns"))
	var failing atomic.Bool
	log := &warnCount{Logger: logging.NewNopLogger()}
	o := r.options(widgetGK)
	o.Logger = log
	o.Refs = func(metav1.Object) ([]Ref, error) {
		if failing.Load() {
			return nil, errors.New("boom")
		}
		return nil, nil
	}
	tr, err := r.s.Track(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	pass := func(fail bool) {
		t.Helper()
		failing.Store(fail)
		a, err := tr.Acquire(context.Background(), h.DeepCopy())
		if err != nil || a.Degraded != fail {
			t.Fatalf("Acquire = %+v, %v; want Degraded %v", a, err, fail)
		}
	}
	steps := []struct {
		fail bool
		want int64
	}{{true, 1}, {true, 1}, {true, 1}, {false, 1}, {true, 2}, {true, 2}}
	for i, s := range steps {
		pass(s.fail)
		if got := log.n.Load(); got != s.want {
			t.Fatalf("after pass %d: %d degraded log lines, want %d", i+1, got, s.want)
		}
	}
	tr.Forget(h.DeepCopy())
	pass(true)
	if got := log.n.Load(); got != 3 {
		t.Errorf("after Forget, a degraded pass logged %d lines in all, want 3", got)
	}
}

// §7.4 (the user's decision of 2026-11-22): an unset
// DisownInterval is derived from TimeoutBound, max(60s, ⌈max(2 ×
// TimeoutBound, TimeoutBound + LandingMargin) / (DisownPasses − 1)⌉), and
// published on the CRD as derived, for every TimeoutBound and DisownPasses;
// one set explicitly is used as given, and refused when it is too short.
func TestDisownIntervalDerivedFromTheTimeoutBound(t *testing.T) {
	for _, tc := range []struct {
		bound     time.Duration
		passes    int // 0 is the default
		want      time.Duration
		published string
	}{
		{270 * time.Second, 0, 135 * time.Second, "135s"}, // git-provider's 4m timeout plus 30s
		{90 * time.Second, 0, 60 * time.Second, "60s"},    // the default bound keeps the default
		{271 * time.Second, 0, 136 * time.Second, "136s"}, // 542s / 4 = 135.5s, up to a whole second
		// Two passes span one interval, so a small bound needs the margin.
		{time.Second, 2, 61 * time.Second, "61s"},
		{1500 * time.Millisecond, 2, 62 * time.Second, "62s"},
		{10 * time.Second, 2, 70 * time.Second, "70s"},
		{59 * time.Second, 2, 119 * time.Second, "119s"},
		{60 * time.Second, 2, 120 * time.Second, "120s"}, // the two halves meet
		{61 * time.Second, 2, 122 * time.Second, "122s"},
		{90 * time.Second, 2, 180 * time.Second, "180s"},
		{10 * time.Second, 3, 60 * time.Second, "60s"},
		{65 * time.Second, 3, 65 * time.Second, "65s"},
	} {
		t.Run(fmt.Sprintf("%s in %d passes", tc.bound, tc.passes), func(t *testing.T) {
			r := newFakeRig(t, namespaceObject("ns"))
			o := r.options(widgetGK)
			o.TimeoutBound, o.DisownPasses = tc.bound, tc.passes
			tr, err := r.s.Track(context.Background(), o)
			if err != nil {
				t.Fatalf("Track with an unset DisownInterval and TimeoutBound %s: %v", tc.bound, err)
			}
			if tr.o.DisownInterval != tc.want {
				t.Errorf("DisownInterval = %s, want %s", tr.o.DisownInterval, tc.want)
			}
			crd, err := r.dyn.Resource(crdGVR).Get(context.Background(), crdOf(widgetGK).GetName(), metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			var got published
			if err := json.Unmarshal([]byte(crd.GetAnnotations()[AnnotationConfig]), &got); err != nil {
				t.Fatalf("the published settings %q: %v", crd.GetAnnotations()[AnnotationConfig], err)
			}
			if got.DisownInterval != tc.published {
				t.Errorf("published disownInterval = %q, want the derived %q", got.DisownInterval, tc.published)
			}
			// The published string reads back as the derived value, and the
			// bound holds for it.
			parsed, err := time.ParseDuration(got.DisownInterval)
			need := max(2*tc.bound, tc.bound+60*time.Second)
			if err != nil || parsed != tr.o.DisownInterval || time.Duration(tr.o.DisownPasses-1)*parsed < need {
				t.Errorf("published %q parses to %s (%v); want %s, spanning at least %s", got.DisownInterval, parsed, err, tr.o.DisownInterval, need)
			}
			// A second Track of the kind, and a second process, compare the
			// derived value against the derived value.
			if again, err := r.s.Track(context.Background(), o); err != nil || again != tr {
				t.Errorf("a second Track with the interval unset = %v, %v; want the same tracker", again, err)
			}
			other := newShared(r.s.c, SharedOptions{}, r.clk)
			if _, err := other.Track(context.Background(), o); err != nil {
				t.Errorf("a second process with the interval unset: %v; want it to agree with the published value", err)
			}
		})
	}
	t.Run("one pass", func(t *testing.T) {
		r := newFakeRig(t, namespaceObject("ns"))
		o := r.options(widgetGK)
		o.DisownPasses = 1
		if _, err := r.s.Track(context.Background(), o); err == nil || !strings.Contains(err.Error(), "with DisownPasses at least 2") {
			t.Errorf("Track with DisownPasses 1 and the interval unset = %v; want the refusal that names the fix", err)
		}
	})
	t.Run("explicit and too short", func(t *testing.T) {
		r := newFakeRig(t, namespaceObject("ns"))
		o := r.options(widgetGK)
		o.TimeoutBound = 270 * time.Second
		o.DisownInterval = 60 * time.Second
		if _, err := r.s.Track(context.Background(), o); err == nil || !strings.Contains(err.Error(), "DisownInterval") {
			t.Errorf("Track = %v, want an explicit interval too short for TimeoutBound refused", err)
		}
	})
}
