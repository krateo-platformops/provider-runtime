package credentialuse

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestForeignChange is the self-write filter (§9.2, L2): which changes to a
// content kind's object enqueue its holders. The tracker's own writes, epoch,
// labels and finalizers, and what the server stamps, must not; anyone else's
// change must, however small.
func TestForeignChange(t *testing.T) {
	base := func() *unstructured.Unstructured {
		u := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "credentialuse.test.krateo.io/v1", "kind": "Target",
			"spec": map[string]any{"ref": map[string]any{"name": "s"}},
		}}
		u.SetNamespace("ns")
		u.SetName("kt")
		u.SetResourceVersion("1")
		u.SetLabels(map[string]string{"app": "x"})
		u.SetAnnotations(map[string]string{"note": "a"})
		u.SetFinalizers([]string{"example.com/other"})
		return u
	}
	f := fNS
	cases := []struct {
		name    string
		change  func(u *unstructured.Unstructured)
		foreign bool
	}{
		{"nothing but the resourceVersion", func(u *unstructured.Unstructured) { u.SetResourceVersion("2") }, false},
		{"the epoch only", func(u *unstructured.Unstructured) {
			u.SetAnnotations(map[string]string{"note": "a", f: "e2"})
		}, false},
		{"the held and marker labels only", func(u *unstructured.Unstructured) {
			u.SetLabels(map[string]string{"app": "x", LabelHeld: "true", MarkerLabel(widgetGK): "true"})
		}, false},
		{"our finalizer added, with its epoch", func(u *unstructured.Unstructured) {
			u.SetFinalizers([]string{"example.com/other", f})
			u.SetAnnotations(map[string]string{"note": "a", f: "e1"})
		}, false},
		{"our finalizer the only one, then released", func(u *unstructured.Unstructured) {
			u.SetFinalizers([]string{"example.com/other"})
		}, false},
		{"managedFields", func(u *unstructured.Unstructured) {
			u.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubectl"}})
		}, false},
		{"the spec re-pointed", func(u *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(u.Object, "other", "spec", "ref", "name")
		}, true},
		{"someone else's label, all else equal", func(u *unstructured.Unstructured) {
			u.SetLabels(map[string]string{"app": "x", "team": "y"})
		}, true},
		{"someone else's annotation, all else equal", func(u *unstructured.Unstructured) {
			u.SetAnnotations(map[string]string{"note": "b"})
		}, true},
		{"someone else's finalizer", func(u *unstructured.Unstructured) {
			u.SetFinalizers([]string{"example.com/other", "example.com/more"})
		}, true},
		{"a deletionTimestamp", func(u *unstructured.Unstructured) {
			now := metav1.Now()
			u.SetDeletionTimestamp(&now)
		}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			o, n := base(), base()
			c.change(n)
			if got := foreignChange(o, n); got != c.foreign {
				t.Errorf("foreignChange = %v, want %v", got, c.foreign)
			}
		})
	}
	t.Run("our finalizer the only one, released", func(t *testing.T) {
		o, n := base(), base()
		o.SetFinalizers([]string{f})
		n.SetFinalizers(nil)
		if foreignChange(o, n) {
			t.Error("our own release counted as someone else's change")
		}
	})
	t.Run("metadata objects aren't content", func(t *testing.T) {
		if foreignChange(&metav1.PartialObjectMetadata{}, &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Generation: 2}}) {
			t.Error("a metadata object counted")
		}
	})
}
