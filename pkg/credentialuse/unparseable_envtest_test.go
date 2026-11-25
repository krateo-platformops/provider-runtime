package credentialuse_test

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
)

// A namespace's teardown with a holder whose record can't be parsed, which no
// pass of its controller repairs, as while it is paused: it keeps the Secret
// until it is gone, the last of the holders, and then the Secret goes (§6.1,
// §10.2).
func TestNamespaceTeardownWithAnUnparseableHolder(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	r := newWriteRig(t, env.Config, cu.SharedOptions{}, func(o *cu.Options) {
		enqueue := o.Enqueue
		o.Enqueue = func(nn types.NamespacedName) {
			if nn.Name != "bad" {
				enqueue(nn)
			}
		}
	}, nil)
	ns := env.Namespace(t, "unparseable")
	createSecret(t, ns, "s")
	createWidget(t, ns, "w", "s")
	r.c.enqueue(widgetNN(ns, "w"))
	established(t, ns, "s", "w")
	bad := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}}
	bad.SetGroupVersionKind(cutest.WidgetGVK)
	bad.SetNamespace(ns)
	bad.SetName("bad")
	bad.SetFinalizers([]string{ownerFinalizer})
	bad.SetAnnotations(map[string]string{cu.AnnotationRefs: "oops"})
	if _, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Create(ctx, bad, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := env.Kube.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	deleteSecret(t, ns, "s")
	deleteWidget(t, ns, "w")
	deleteWidget(t, ns, "bad")
	r.c.enqueue(widgetNN(ns, "w"))
	gone(t, time.Minute, "w", widgetGet(ns, "w"))
	// Past w's own batches, and the grace's: what keeps it now is bad.
	time.Sleep(testGrace + 2*time.Second)
	if err := heldFor(ns, "s", ns)(); err != nil {
		t.Fatalf("the holder whose record can't be parsed keeps nothing: %v", err)
	}
	if _, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Patch(ctx, "bad", types.MergePatchType,
		[]byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	gone(t, 30*time.Second, "bad", widgetGet(ns, "bad"))
	gone(t, 30*time.Second, "s", secretGet(ns, "s"))
}
