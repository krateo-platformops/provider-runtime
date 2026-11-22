package credentialuse

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
