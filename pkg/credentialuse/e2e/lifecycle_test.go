package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/reconciler"
)

// M4, with a real manager: a controller built without
// WatchesRawSource(r.CredentialUseSource()) makes the manager refuse to run:
// its first Reconcile does nothing and fails it, with the message that names
// the fix.
func TestStartFailsWithoutTheSource(t *testing.T) {
	cutest.Require(t, env)
	w := newWorld(t)
	ns := env.Namespace(t, "no-source")
	createSecret(t, ns, "creds")
	createWidget(t, w, ns, "w", WidgetSpec{Secret: "creds"})
	m := w.start(t, managerOptions{refs: true, noSource: true})
	select {
	case err := <-m.errc:
		want := "credential-use source for " + widgetGK.String() + " was not started: add .WatchesRawSource(r.CredentialUseSource()) to the controller"
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("the manager's Start = %v, want %q", err, want)
		}
		m.errc <- err // for the cleanup's stop
	case <-time.After(time.Minute):
		t.Fatal("the manager kept running without the source")
	}
	if wd := getWidget(t, w, ns, "w"); wd.Annotations[cu.AnnotationRefs] != "" || len(wd.Finalizers) != 0 {
		t.Errorf("the pass acted: annotations %v, finalizers %v", wd.Annotations, wd.Finalizers)
	}
	if len(w.recorded()) != 0 {
		t.Errorf("the pass called the external system: %v", w.recorded())
	}
}

// L8, with a real manager: the predicate passes a record change this process
// didn't write, so a hand edit of the record starts a pass, which writes the
// record back. Nothing else enqueues the Widget: its poll interval is 10m.
func TestARecordChangeThisProcessDidntWriteStartsAPass(t *testing.T) {
	cutest.Require(t, env)
	w := newWorld(t)
	m := w.start(t, managerOptions{refs: true})
	ns := env.Namespace(t, "foreign-record")
	createSecret(t, ns, "creds")
	createWidget(t, w, ns, "w", WidgetSpec{Secret: "creds"})
	established(t, w, ns, nn(ns, "creds"), "w")
	want := getWidget(t, w, ns, "w").Annotations[cu.AnnotationRefs]
	time.Sleep(time.Second)
	before := m.passes.Load()
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, cu.AnnotationRefs, `[{"group":"","kind":"Secret","namespace":"`+ns+`","name":"elsewhere"}]`)
	if err := w.kube.Patch(context.Background(), &Widget{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "w"}}, client.RawPatch(types.MergePatchType, []byte(patch))); err != nil {
		t.Fatal(err)
	}
	cutest.Eventually(t, 30*time.Second, func() error {
		if got := getWidget(t, w, ns, "w").Annotations[cu.AnnotationRefs]; got != want {
			return fmt.Errorf("record %s, want it written back to %s: %w", got, want, cutest.ErrNotYet)
		}
		return nil
	})
	if m.passes.Load() == before {
		t.Error("no pass ran")
	}
}

// §8 steps 4 and 7 (R12), with the real reconciler: a deleting Widget whose
// owner finalizer someone else removed, leaving its record, has the record
// removed with an Update of its own, on the orphan path and on the delete
// path.
func TestStandaloneForget(t *testing.T) {
	cutest.Require(t, env)
	w := newWorld(t)
	w.start(t, managerOptions{refs: true})
	for _, policy := range []string{meta.DeletionPolicyOrphan, ""} {
		name := "delete"
		if policy != "" {
			name = "orphan"
		}
		t.Run(name, func(t *testing.T) {
			ns := env.Namespace(t, "forget-"+name)
			createSecret(t, ns, "creds")
			createWidget(t, w, ns, "w", WidgetSpec{Secret: "creds"}, func(wd *Widget) {
				wd.Finalizers = []string{"test.krateo.io/keep"}
				if policy != "" {
					wd.Annotations = map[string]string{meta.AnnotationKeyDeletionPolicy: policy}
				}
			})
			t.Cleanup(func() { removeFinalizers(t, cutest.WidgetGVR, ns, "w") })
			established(t, w, ns, nn(ns, "creds"), "w")
			// A third party strips the owner finalizer, and the external
			// resource is gone, so the delete path needs no Delete.
			wd := getWidget(t, w, ns, "w")
			wd.Finalizers = []string{"test.krateo.io/keep"}
			if err := w.kube.Update(context.Background(), wd); err != nil {
				t.Fatal(err)
			}
			w.mu.Lock()
			delete(w.exists, nn(ns, "w"))
			w.mu.Unlock()
			deleteWidget(t, ns, "w")
			cutest.Eventually(t, 30*time.Second, func() error {
				if a := getWidget(t, w, ns, "w").Annotations; a[cu.AnnotationRefs] != "" {
					return fmt.Errorf("record still there: %v: %w", a, cutest.ErrNotYet)
				}
				return nil
			})
			// With no user left, the Secret's hold is released.
			f, _ := cu.FinalizerName(widgetGK, ns)
			cutest.Eventually(t, 30*time.Second, func() error {
				s := &corev1.Secret{}
				if err := w.kube.Get(context.Background(), nn(ns, "creds"), s); err != nil {
					return err
				}
				for _, x := range s.Finalizers {
					if x == f {
						return fmt.Errorf("the Secret is still held: %w", cutest.ErrNotYet)
					}
				}
				return nil
			})
		})
	}
}

// §12, with the real reconciler: a pass that fails in Connect, for a Widget
// with a ref its target didn't consent to, names the unheld refs in Synced's
// message.
func TestTheSyncedMessageNamesTheUnheldRefs(t *testing.T) {
	cutest.Require(t, env)
	w := newWorld(t)
	w.start(t, managerOptions{refs: true})
	ns, other := env.Namespace(t, "suffix"), env.Namespace(t, "suffix-target")
	createSecret(t, other, "creds")
	createWidget(t, w, ns, "w", WidgetSpec{Secret: "creds", SecretNamespace: other, FailConnect: true})
	cutest.Eventually(t, 30*time.Second, func() error {
		synced := getWidget(t, w, ns, "w").GetCondition(prv1.TypeSynced)
		prefix := "connect failed: the external system refused the credentials; credential-use: 1 ref not held (NotConsented since "
		suffix := "), see annotation " + cu.AnnotationUnheld
		if !strings.HasPrefix(synced.Message, prefix) || !strings.HasSuffix(synced.Message, suffix) {
			return fmt.Errorf("Synced message %q: %w", synced.Message, cutest.ErrNotYet)
		}
		return nil
	})
	if wd := getWidget(t, w, ns, "w"); !strings.Contains(wd.Annotations[cu.AnnotationUnheld], other+"/Secret/creds") {
		t.Errorf("unheld = %q, want the ref listed", wd.Annotations[cu.AnnotationUnheld])
	}
	_ = reconciler.FinalizerName
}
