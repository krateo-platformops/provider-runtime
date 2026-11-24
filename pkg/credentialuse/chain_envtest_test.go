package credentialuse_test

// Chains against envtest (plan, PR 4; §9): a Widget names a Target, which
// names a Secret, as core-provider's CompositionDefinition names a
// KubernetesTarget, which names a kubeconfig Secret. Production refuses a
// chain in this release (chain_refused_test.go): FollowChains is the tests'
// switch.

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/driver"
)

// targetRefs is a Widget's RefsFunc for a chain: spec.target names a Target,
// whose spec.secret names the Secret.
func targetRefs(obj metav1.Object) ([]cu.Ref, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("a %T, not an unstructured Widget", obj)
	}
	name, _, err := unstructured.NestedString(u.Object, "spec", "target")
	if err != nil || name == "" {
		return nil, err
	}
	return []cu.Ref{{
		GroupKind: cutest.TargetGVK.GroupKind(), Namespace: obj.GetNamespace(), Name: name,
		Next: func(kt *unstructured.Unstructured) ([]cu.Ref, error) {
			secret, _, err := unstructured.NestedString(kt.Object, "spec", "secret")
			if err != nil || secret == "" {
				return nil, fmt.Errorf("Target %s names no secret: %v", kt.GetName(), err)
			}
			return []cu.Ref{cu.SecretRef(kt.GetNamespace(), secret)}, nil
		},
	}}, nil
}

func chainOptions(o *cu.Options) {
	o.Refs = targetRefs
	o.Protected = []cu.ProtectedKind{{GVK: secretGVK}, {GVK: cutest.TargetGVK, Content: true}}
}

var secretGVK = corev1.SchemeGroupVersion.WithKind("Secret")

func createChainTarget(t *testing.T, ns, name, secret string) {
	t.Helper()
	u := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"secret": secret}}}
	u.SetGroupVersionKind(cutest.TargetGVK)
	u.SetNamespace(ns)
	u.SetName(name)
	if _, err := env.Dynamic.Resource(cutest.TargetGVR).Namespace(ns).Create(context.Background(), u, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating target %s/%s: %v", ns, name, err)
	}
}

func createChainWidget(t *testing.T, ns, name, target string) {
	t.Helper()
	u := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"target": target}}}
	u.SetGroupVersionKind(cutest.WidgetGVK)
	u.SetNamespace(ns)
	u.SetName(name)
	if _, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Create(context.Background(), u, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating widget %s/%s: %v", ns, name, err)
	}
}

func targetGet(ns, name string) func() error {
	return func() error {
		_, err := env.Dynamic.Resource(cutest.TargetGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
		return err
	}
}

// chainEstablished waits until every Widget is established and its record
// names the Target and want, and both carry the Widget kind's finalizer for
// ns.
func chainEstablished(t *testing.T, ns, target, want string, widgets ...string) {
	t.Helper()
	f, _ := cu.FinalizerName(widgetGK, ns)
	cutest.Eventually(t, time.Minute, func() error {
		for _, w := range widgets {
			obj, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(context.Background(), w, metav1.GetOptions{})
			if err != nil {
				return err
			}
			rec := cu.ReadRecord(obj)
			if !hasString(obj.GetFinalizers(), ownerFinalizer) ||
				!rec.Names(cu.ID{Group: cutest.Group, Kind: "Target", Namespace: ns, Name: target}) ||
				!rec.Names(cu.SecretRef(ns, want).ID()) || len(rec.IDs) != 2 {
				return fmt.Errorf("widget %s not established on %s (record %v): %w", w, want, rec.IDs, cutest.ErrNotYet)
			}
		}
		kt, err := env.Dynamic.Resource(cutest.TargetGVR).Namespace(ns).Get(context.Background(), target, metav1.GetOptions{})
		if err != nil {
			return err
		}
		s, err := secrets(env.Metadata, ns).Get(context.Background(), want, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !hasString(kt.GetFinalizers(), f) || !hasString(s.Finalizers, f) {
			return fmt.Errorf("target %v or secret %v not held: %w", kt.GetFinalizers(), s.Finalizers, cutest.ErrNotYet)
		}
		return nil
	})
}

// The krateo-057 teardown one level up (§9): the namespace controller
// deletes the Target first, then the Secret, then the Widgets. With the
// tracker both are kept until the last Widget is gone, and every deletion
// pass reads the Target and the Secret. Without it the Target goes first, and
// the Widgets can never find their credentials.
func TestChainTeardown(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	teardown := func(t *testing.T, c *controller, ns string) {
		if err := env.Kube.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := env.Dynamic.Resource(cutest.TargetGVR).Namespace(ns).Delete(ctx, "kt", metav1.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
		deleteSecret(t, ns, "kubeconfig")
		for _, w := range []string{"w1", "w2"} {
			deleteWidget(t, ns, w)
			c.enqueue(widgetNN(ns, w))
		}
	}

	t.Run("with the tracker", func(t *testing.T) {
		r := newWriteRig(t, env.Config, cu.SharedOptions{}, chainOptions, (*cu.Shared).FollowChains)
		ns := env.Namespace(t, "chain")
		createSecret(t, ns, "kubeconfig")
		createChainTarget(t, ns, "kt", "kubeconfig")
		for _, w := range []string{"w1", "w2"} {
			createChainWidget(t, ns, w, "kt")
			r.c.enqueue(widgetNN(ns, w))
		}
		chainEstablished(t, ns, "kt", "kubeconfig", "w1", "w2")

		teardown(t, r.c, ns)
		gone(t, time.Minute, "a Widget", widgetGet(ns, "w1"))
		gone(t, time.Minute, "a Widget", widgetGet(ns, "w2"))
		gone(t, time.Minute, "the Target", targetGet(ns, "kt"))
		gone(t, time.Minute, "the Secret", secretGet(ns, "kubeconfig"))
		events := r.c.d.Events()
		for _, w := range []string{"w1", "w2"} {
			if !slices.Contains(events, "delete "+ns+"/"+w) {
				t.Errorf("no external Delete for %s: %v", w, events)
			}
		}
		if v := r.c.violations(); len(v) != 0 {
			t.Errorf("a Widget failed to read its chain while being deleted: %v", v)
		}
		if err := env.FinalizeNamespace(ctx, ns); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("control: without the tracker it wedges", func(t *testing.T) {
		c := newController(t)
		chainRefs := func(obj metav1.Object) ([]cu.Ref, error) {
			refs, err := targetRefs(obj)
			if err != nil || len(refs) == 0 {
				return refs, err
			}
			// Without a tracker nothing follows the chain: Connect reads the
			// Target, which the test deletes first.
			refs[0].Next = nil
			return refs, nil
		}
		c.start(&driver.Driver{Dynamic: env.Dynamic, Holder: cutest.WidgetGVR, OwnerFinalizer: ownerFinalizer, Refs: chainRefs, Connect: c.connect})
		ns := env.Namespace(t, "chain-control")
		createSecret(t, ns, "kubeconfig")
		createChainTarget(t, ns, "kt", "kubeconfig")
		for _, w := range []string{"w1", "w2"} {
			createChainWidget(t, ns, w, "kt")
			c.enqueue(widgetNN(ns, w))
		}
		cutest.Eventually(t, time.Minute, func() error {
			for _, w := range []string{"w1", "w2"} {
				obj, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(ctx, w, metav1.GetOptions{})
				if err != nil || !hasString(obj.GetFinalizers(), ownerFinalizer) {
					return fmt.Errorf("widget %s not established (%v): %w", w, err, cutest.ErrNotYet)
				}
			}
			return nil
		})
		teardown(t, c, ns)
		gone(t, 10*time.Second, "the Target", targetGet(ns, "kt"))
		cutest.Eventually(t, 30*time.Second, func() error {
			for _, w := range []string{"w1", "w2"} {
				if !strings.Contains(c.errorOf(widgetNN(ns, w)), "not found") {
					return fmt.Errorf("widget %s's last pass: %q: %w", w, c.errorOf(widgetNN(ns, w)), cutest.ErrNotYet)
				}
			}
			return nil
		})
		for _, w := range []string{"w1", "w2"} {
			if err := widgetGet(ns, w)(); err != nil {
				t.Errorf("widget %s: %v; want it wedged", w, err)
			}
			removeFinalizers(t, ns, w)
		}
		if err := env.FinalizeNamespace(ctx, ns); err != nil {
			t.Fatal(err)
		}
	})
}

// A re-pointed Target enqueues its holders (§9.2, MED-5): someone else's
// write to a held Target with a Next reaches the Widgets through the
// tracker's enqueue alone, and their next passes hold the new Secret and
// release the old one. The tracker's own writes to the Target don't count:
// once settled, the Widgets aren't reconciled again (L2).
func TestARepointedTargetEnqueuesItsHolders(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	r := newWriteRig(t, env.Config, cu.SharedOptions{}, chainOptions, (*cu.Shared).FollowChains)
	ns := env.Namespace(t, "repoint")
	createSecret(t, ns, "old")
	createSecret(t, ns, "new")
	createChainTarget(t, ns, "kt", "old")
	createChainWidget(t, ns, "w", "kt")
	r.c.enqueue(widgetNN(ns, "w"))
	chainEstablished(t, ns, "kt", "old", "w")

	// Settled: the tracker's own A2s on the Target don't enqueue the Widget.
	settled := func() int64 {
		var n int64
		cutest.Eventually(t, 30*time.Second, func() error {
			before := r.c.passes.Load()
			time.Sleep(2 * time.Second)
			if n = r.c.passes.Load(); n != before {
				return fmt.Errorf("passes %d → %d: %w", before, n, cutest.ErrNotYet)
			}
			return nil
		})
		return n
	}
	settled()

	// Someone else re-points the Target; nothing enqueues the Widget but the
	// tracker.
	kt, err := env.Dynamic.Resource(cutest.TargetGVR).Namespace(ns).Get(ctx, "kt", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(kt.Object, "new", "spec", "secret"); err != nil {
		t.Fatal(err)
	}
	if _, err := env.Dynamic.Resource(cutest.TargetGVR).Namespace(ns).Update(ctx, kt, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	chainEstablished(t, ns, "kt", "new", "w")
	f, _ := cu.FinalizerName(widgetGK, ns)
	cutest.Eventually(t, time.Minute, func() error {
		s, err := secrets(env.Metadata, ns).Get(ctx, "old", metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if hasString(s.Finalizers, f) {
			return fmt.Errorf("the old Secret is still held: %w", cutest.ErrNotYet)
		}
		return nil
	})
	settled()
}

// configRefs is a Widget's RefsFunc through a ConfigMap: spec.config names a
// ConfigMap whose data.secret names the Secret, as a Secret that names a
// Secret would (§9.1, §10.4).
func configRefs(obj metav1.Object) ([]cu.Ref, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("a %T, not an unstructured Widget", obj)
	}
	name, _, err := unstructured.NestedString(u.Object, "spec", "config")
	if err != nil || name == "" {
		return nil, err
	}
	return []cu.Ref{{
		GroupKind: schema.GroupKind{Kind: "ConfigMap"}, Namespace: obj.GetNamespace(), Name: name,
		Next: func(cm *unstructured.Unstructured) ([]cu.Ref, error) {
			secret, _, err := unstructured.NestedString(cm.Object, "data", "secret")
			if err != nil || secret == "" {
				return nil, fmt.Errorf("ConfigMap %s names no secret: %v", cm.GetName(), err)
			}
			return []cu.Ref{cu.SecretRef(cm.GetNamespace(), secret)}, nil
		},
	}}, nil
}

// A ConfigMap has no generation: a change to its data is seen only by
// comparing content, so re-pointing it enqueues its holders only through the
// content cache and the self-write filter (§9.2 L2, §10.4).
func TestARepointedConfigMapEnqueuesItsHolders(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	r := newWriteRig(t, env.Config, cu.SharedOptions{}, func(o *cu.Options) {
		o.Refs = configRefs
		o.Protected = []cu.ProtectedKind{{GVK: secretGVK}, {GVK: corev1.SchemeGroupVersion.WithKind("ConfigMap"), Content: true}}
	}, (*cu.Shared).FollowChains)
	ns := env.Namespace(t, "repoint-cm")
	createSecret(t, ns, "old")
	createSecret(t, ns, "new")
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "cm"}, Data: map[string]string{"secret": "old"}}
	if _, err := env.Kube.CoreV1().ConfigMaps(ns).Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	u := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"config": "cm"}}}
	u.SetGroupVersionKind(cutest.WidgetGVK)
	u.SetNamespace(ns)
	u.SetName("w")
	if _, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Create(ctx, u, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	r.c.enqueue(widgetNN(ns, "w"))
	holds := func(secret string) func() error {
		return func() error {
			obj, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(ns).Get(ctx, "w", metav1.GetOptions{})
			if err != nil {
				return err
			}
			if rec := cu.ReadRecord(obj); !rec.Names(cu.SecretRef(ns, secret).ID()) || !hasString(obj.GetFinalizers(), ownerFinalizer) {
				return fmt.Errorf("widget record %v doesn't name %s: %w", rec.IDs, secret, cutest.ErrNotYet)
			}
			return nil
		}
	}
	cutest.Eventually(t, time.Minute, holds("old"))
	time.Sleep(2 * time.Second) // settle: the tracker's own writes to the ConfigMap

	got, err := env.Kube.CoreV1().ConfigMaps(ns).Get(ctx, "cm", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got.Data["secret"] = "new"
	if _, err := env.Kube.CoreV1().ConfigMaps(ns).Update(ctx, got, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	cutest.Eventually(t, time.Minute, holds("new"))
}
