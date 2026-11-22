package e2e

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
	"k8s.io/utils/ptr"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
	"github.com/krateo-platformops/provider-runtime/pkg/reconciler"
)

// §14's namespace teardown, with the real reconciler, in the order seen on
// krateo-057: a namespace is deleted, its Secret before the two Widgets that
// read it. The Secret stays, readable, until both Widgets have finished
// deleting, through Connect and Delete; no Widget's status ever shows a
// deletion that failed for want of its credential (I1); then the Secret
// goes, and the namespace can be finalized. The control, the same sequence
// with a connecter that doesn't name its refs, wedges.
func TestNamespaceTeardown(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	// teardown is steps 2 and 3: the namespace terminates and refuses new
	// objects; the Secret is deleted first, then both Widgets.
	teardown := func(t *testing.T, ns string) {
		if err := env.Kube.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
		n, err := env.Kube.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
		if err != nil || n.DeletionTimestamp == nil {
			t.Fatalf("namespace %s after its deletion: %v; want it terminating", ns, err)
		}
		late := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "late"}}
		if _, err := env.Kube.CoreV1().Secrets(ns).Create(ctx, late, metav1.CreateOptions{}); !apierrors.IsForbidden(err) {
			t.Fatalf("creating a Secret in the terminating namespace: %v; want Forbidden (is NamespaceLifecycle on?)", err)
		}
		deleteSecret(t, ns, "creds")
		deleteWidget(t, ns, "w1")
		deleteWidget(t, ns, "w2")
	}

	t.Run("with credential-use", func(t *testing.T) {
		w := newWorld(t)
		w.start(t, managerOptions{refs: true})
		ns := env.Namespace(t, "teardown")
		i1 := watchI1(t, ns)
		// 1. A namespace, a Secret, and two Widgets naming it.
		createSecret(t, ns, "creds")
		createWidget(t, w, ns, "w1", WidgetSpec{Secret: "creds"})
		createWidget(t, w, ns, "w2", WidgetSpec{Secret: "creds"})
		established(t, w, ns, nn(ns, "creds"), "w1", "w2")

		teardown(t, ns)
		s, err := env.Kube.CoreV1().Secrets(ns).Get(ctx, "creds", metav1.GetOptions{})
		if err != nil || s.DeletionTimestamp == nil {
			t.Fatalf("the Secret after its deletion: %v; want it kept, being deleted", err)
		}
		gone(t, time.Minute, "Widget w1", widgetGet(ns, "w1"))
		gone(t, time.Minute, "Widget w2", widgetGet(ns, "w2"))
		gone(t, time.Minute, "the Secret", secretGet(ns, "creds"))
		for _, name := range []string{"w1", "w2"} {
			if !slices.Contains(w.recorded(), "delete "+ns+"/"+name) {
				t.Errorf("no external Delete for %s: %v", name, w.recorded())
			}
		}
		if v := i1.seen(); len(v) != 0 {
			t.Errorf("I1: a Widget's deletion failed for want of its credential: %v", v)
		}
		// 4. The namespace controller's last step.
		if err := env.FinalizeNamespace(ctx, ns); err != nil {
			t.Fatal(err)
		}
		gone(t, time.Minute, "the namespace", func() error {
			_, err := env.Kube.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
			return err
		})
	})

	t.Run("control: without CredentialRefs it wedges", func(t *testing.T) {
		w := newWorld(t)
		w.start(t, managerOptions{})
		ns := env.Namespace(t, "control")
		createSecret(t, ns, "creds")
		createWidget(t, w, ns, "w1", WidgetSpec{Secret: "creds"})
		createWidget(t, w, ns, "w2", WidgetSpec{Secret: "creds"})
		cutest.Eventually(t, time.Minute, func() error {
			for _, name := range []string{"w1", "w2"} {
				if wd := getWidget(t, w, ns, name); !slices.Contains(wd.Finalizers, reconciler.FinalizerName) {
					return fmt.Errorf("Widget %s has no finalizer yet: %w", name, cutest.ErrNotYet)
				}
			}
			return nil
		})
		teardown(t, ns)
		gone(t, 10*time.Second, "the Secret", secretGet(ns, "creds"))
		// Each deletion pass fails to read the Secret, as on krateo-057.
		cutest.Eventually(t, 30*time.Second, func() error {
			for _, name := range []string{"w1", "w2"} {
				synced := getWidget(t, w, ns, name).GetCondition(prv1.TypeSynced)
				if synced.Reason != prv1.ReasonReconcileError || !strings.Contains(synced.Message, "not found") {
					return fmt.Errorf("Widget %s: Synced %+v: %w", name, synced, cutest.ErrNotYet)
				}
			}
			return nil
		})
		time.Sleep(2 * time.Second) // passes keep failing; nothing will change
		for _, name := range []string{"w1", "w2"} {
			if err := widgetGet(ns, name)(); err != nil {
				t.Errorf("Widget %s: %v; want it wedged", name, err)
			}
			removeFinalizers(t, cutest.WidgetGVR, ns, name)
		}
		if err := env.FinalizeNamespace(ctx, ns); err != nil {
			t.Fatal(err)
		}
	})
}

// Interleaving A (§5.1), with the real reconciler: in a live namespace, the
// Secret is deleted, then its holder 1s later, within the grace: the Secret
// is kept until the holder is gone. Then the residual: a holder deleted
// after the grace has released the Secret fails until the Secret is
// recreated, and then deletes.
func TestInterleavingA(t *testing.T) {
	cutest.Require(t, env)
	w := newWorld(t)
	w.start(t, managerOptions{refs: true})

	t.Run("within the grace", func(t *testing.T) {
		ns := env.Namespace(t, "interleaving-a")
		i1 := watchI1(t, ns)
		createSecret(t, ns, "creds")
		createWidget(t, w, ns, "w", WidgetSpec{Secret: "creds"})
		established(t, w, ns, nn(ns, "creds"), "w")
		deleteSecret(t, ns, "creds")
		time.Sleep(time.Second)
		deleteWidget(t, ns, "w")
		gone(t, time.Minute, "the Widget", widgetGet(ns, "w"))
		gone(t, time.Minute, "the Secret", secretGet(ns, "creds"))
		if v := i1.seen(); len(v) != 0 {
			t.Errorf("I1: %v", v)
		}
	})

	t.Run("after the grace: the residual", func(t *testing.T) {
		ns := env.Namespace(t, "interleaving-a-late")
		createSecret(t, ns, "creds")
		createWidget(t, w, ns, "w", WidgetSpec{Secret: "creds"})
		established(t, w, ns, nn(ns, "creds"), "w")
		deleteSecret(t, ns, "creds")
		// No user is deleting, the namespace is live: released after the grace.
		gone(t, time.Minute, "the Secret", secretGet(ns, "creds"))
		deleteWidget(t, ns, "w")
		cutest.Eventually(t, 30*time.Second, func() error {
			if synced := getWidget(t, w, ns, "w").GetCondition(prv1.TypeSynced); !strings.Contains(synced.Message, "not found") {
				return fmt.Errorf("Synced %+v: %w", synced, cutest.ErrNotYet)
			}
			return nil
		})
		createSecret(t, ns, "creds")
		gone(t, time.Minute, "the Widget", widgetGet(ns, "w"))
	})
}

// Interleaving B (§5.1), with the real reconciler: the Secret, then the
// namespace, then the holders, each right after the previous DELETE
// returns, as Helm's batchPerform does. The live re-read of the namespace's
// deletionTimestamp keeps the Secret until the holders are gone, and the
// namespace tears down.
func TestInterleavingB(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	w := newWorld(t)
	w.start(t, managerOptions{refs: true})
	ns := env.Namespace(t, "interleaving-b")
	i1 := watchI1(t, ns)
	createSecret(t, ns, "creds")
	createWidget(t, w, ns, "w1", WidgetSpec{Secret: "creds"})
	createWidget(t, w, ns, "w2", WidgetSpec{Secret: "creds"})
	established(t, w, ns, nn(ns, "creds"), "w1", "w2")
	deleteSecret(t, ns, "creds")
	if err := env.Kube.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	deleteWidget(t, ns, "w1")
	deleteWidget(t, ns, "w2")
	// Past the grace, the Secret is still kept for its deleting users.
	time.Sleep(testGrace + time.Second)
	gone(t, time.Minute, "Widget w1", widgetGet(ns, "w1"))
	gone(t, time.Minute, "Widget w2", widgetGet(ns, "w2"))
	gone(t, time.Minute, "the Secret", secretGet(ns, "creds"))
	if v := i1.seen(); len(v) != 0 {
		t.Errorf("I1: %v", v)
	}
	if err := env.FinalizeNamespace(ctx, ns); err != nil {
		t.Fatal(err)
	}
}

// §11.4, with the real reconciler: a new holder whose Secret is already being
// deleted without this kind's finalizer must not connect: Synced=False/
// ReconcileError with §12's message, Ready NotObserved, no owner finalizer,
// no Connect; so it deletes freely.
func TestNewHolderOnADeletingSecretDeletesFreely(t *testing.T) {
	cutest.Require(t, env)
	w := newWorld(t)
	w.start(t, managerOptions{refs: true})
	ns := env.Namespace(t, "deleting-secret")
	createSecret(t, ns, "creds", "test.krateo.io/keep")
	deleteSecret(t, ns, "creds")
	t.Cleanup(func() { removeFinalizers(t, secretGVR, ns, "creds") })
	createWidget(t, w, ns, "w", WidgetSpec{Secret: "creds"})
	want := "credential Secret " + ns + "/creds is being deleted and can't be held; not connecting until it is replaced or the reference changes"
	cutest.Eventually(t, 30*time.Second, func() error {
		wd := getWidget(t, w, ns, "w")
		synced, ready := wd.GetCondition(prv1.TypeSynced), wd.GetCondition(prv1.TypeReady)
		if synced.Reason != prv1.ReasonReconcileError || synced.Message != want || ready.Reason != prv1.ReasonNotObserved {
			return fmt.Errorf("Synced %+v, Ready %+v: %w", synced, ready, cutest.ErrNotYet)
		}
		if len(wd.Finalizers) != 0 {
			t.Fatalf("the new holder got finalizers %v", wd.Finalizers)
		}
		return nil
	})
	w.mu.Lock()
	reads := w.reads[nn(ns, "creds")]
	w.mu.Unlock()
	if reads != 0 {
		t.Errorf("Connect read the Secret %d times, want none", reads)
	}
	deleteWidget(t, ns, "w")
	gone(t, 10*time.Second, "the Widget", widgetGet(ns, "w"))
}

// §11.8, with the real reconciler: a holder that owns the Secret it reads,
// deleted in the foreground, completes. envtest has no garbage collector, so
// the test plays it: it deletes the dependent Secret, which is kept while the
// holder is a deleting user; the runtime deletes the holder, removing its
// owner finalizer and record in one write, so the holder stops counting and
// the Secret is released; then the collector removes foregroundDeletion.
func TestForegroundDeletionOfAnOwnerCompletes(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	w := newWorld(t)
	w.start(t, managerOptions{refs: true})
	ns := env.Namespace(t, "foreground")
	createWidget(t, w, ns, "w", WidgetSpec{Secret: "conn"})
	owner := getWidget(t, w, ns, "w")
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "conn", Namespace: ns, OwnerReferences: []metav1.OwnerReference{{
		APIVersion: widgetGVK.GroupVersion().String(), Kind: widgetGVK.Kind, Name: "w", UID: owner.UID,
		BlockOwnerDeletion: ptr.To(true), Controller: ptr.To(true),
	}}}}
	if _, err := env.Kube.CoreV1().Secrets(ns).Create(ctx, s, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	established(t, w, ns, nn(ns, "conn"), "w")

	deleteWidget(t, ns, "w", metav1.DeleteOptions{PropagationPolicy: ptr.To(metav1.DeletePropagationForeground)})
	if wd := getWidget(t, w, ns, "w"); !slices.Contains(wd.Finalizers, metav1.FinalizerDeleteDependents) {
		t.Fatalf("the Widget's finalizers %v lack %s", wd.Finalizers, metav1.FinalizerDeleteDependents)
	}
	// The collector deletes the blocking dependent first.
	deleteSecret(t, ns, "conn")
	// The runtime finishes the holder: its owner finalizer and record go in
	// one write, the Secret is released and goes, while the Widget still
	// waits for the collector.
	gone(t, time.Minute, "the Secret", secretGet(ns, "conn"))
	wd := getWidget(t, w, ns, "w")
	if slices.Contains(wd.Finalizers, reconciler.FinalizerName) || wd.Annotations[cu.AnnotationRefs] != "" {
		t.Errorf("Widget finalizers %v, annotations %v; want the owner finalizer and the record gone", wd.Finalizers, wd.Annotations)
	}
	if !slices.Contains(w.recorded(), "delete "+ns+"/w") {
		t.Errorf("no external Delete: %v", w.recorded())
	}
	// The collector's last step.
	removeFinalizers(t, cutest.WidgetGVR, ns, "w")
	gone(t, 10*time.Second, "the Widget", widgetGet(ns, "w"))
}
