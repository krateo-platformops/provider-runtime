package credentialuse_test

// What a conversion webhook does to the metadata that the package writes
// (§6.2, P21): the API server keeps all of it but the labels and the
// annotations, which are as the webhook answers.

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
)

// dropsAnnotations is a webhook's loss of the annotations; ownMetadata, of the
// labels too, as of one that builds its own ObjectMeta.
func dropsAnnotations(u *unstructured.Unstructured) error {
	u.SetAnnotations(nil)
	return nil
}

func ownMetadata(u *unstructured.Unstructured) error {
	u.SetLabels(nil)
	return dropsAnnotations(u)
}

func widgetFinalizer(t *testing.T, ns string) string {
	t.Helper()
	f, err := cu.FinalizerName(widgetGK, ns)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// mappedClients are clients whose mapper knows kind gk, at both versions, and
// answers v1 for the kind alone.
func mappedClients(t *testing.T, gk schema.GroupKind) cu.Clients {
	t.Helper()
	var clients cu.Clients
	cutest.Eventually(t, time.Minute, func() error {
		clients = clientsFor(t, env.Config) // discovery may lag behind the CRD
		for _, v := range pruneVersions {
			if _, err := clients.Mapper.RESTMapping(gk, v); err != nil {
				return err
			}
		}
		return nil
	})
	if m, err := clients.Mapper.RESTMapping(gk); err != nil || m.Resource.Version != "v1" {
		t.Fatalf("precondition: the mapper's version is %v, %v; want v1", m, err)
	}
	return clients
}

func TestPremiseConversionThatDropsMetadata(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	f := widgetFinalizer(t, "prune")
	for i, c := range []struct {
		name   string
		edit   func(*unstructured.Unstructured) error
		labels bool // kept
	}{
		{"drops the annotations", dropsAnnotations, true},
		{"builds its own metadata", ownMetadata, false},
	} {
		for j, storage := range pruneVersions {
			t.Run(fmt.Sprintf("%s, stored at %s", c.name, storage), func(t *testing.T) {
				p := newPruneRig(t, fmt.Sprintf("Meta%d%d", i, j), storage, roundTrips(t, c.edit))
				for _, through := range pruneVersions {
					name := "through-" + through
					p.create(name)
					r := env.Metadata.Resource(p.gvr(through)).Namespace(p.ns)
					cur, err := r.Get(ctx, name, metav1.GetOptions{})
					if err != nil {
						t.Fatal(err)
					}
					pt, err := cu.AcquireGuardedPatch(cur, widgetGK, "prune", "e")
					if err != nil {
						t.Fatal(err)
					}
					out, err := send(ctx, r, name, pt)
					if err != nil {
						t.Fatal(err)
					}
					read, err := r.Get(ctx, name, metav1.GetOptions{})
					if err != nil {
						t.Fatal(err)
					}
					converted := through != storage
					for what, m := range map[string]*metav1.PartialObjectMetadata{"the response": out, "a read": read} {
						if !hasString(m.Finalizers, f) {
							t.Errorf("A2 through %s: %s has no finalizer", through, what)
						}
						if kept := m.Annotations[f] != ""; kept == converted {
							t.Errorf("A2 through %s: %s has the epoch: %v, want %v", through, what, kept, !converted)
						}
						if kept := m.Labels[cu.LabelHeld] != ""; kept != (c.labels || !converted) {
							t.Errorf("A2 through %s: %s has the held label: %v, want %v", through, what, kept, c.labels || !converted)
						}
					}
					t.Logf("META %s, stored at %s, A2 through %s: finalizer kept, epoch kept: %v, label kept: %v",
						c.name, storage, through, read.Annotations[f] != "", read.Labels[cu.LabelHeld] != "")
				}
			})
		}
	}
}

// A hold on an object whose conversion drops annotations, written through a
// version that isn't the storage one, is never released: R1 restores its epoch
// at each look, and the write drops it. It is released once the webhook keeps
// them; a webhook that always has is the control, and Disown's strip is the
// other way out, for it tests no epoch.
func TestAnEpochTheConversionDropsIsNeverReleased(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	for i, c := range []struct {
		name  string
		drops bool
		strip bool
	}{
		{"a webhook that drops annotations, and is then mended", true, false},
		{"a webhook that drops annotations, and Disown's strip", true, true},
		{"control: a webhook that keeps them", false, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			resetPublished(t, cutest.WidgetGVR)
			t.Cleanup(func() { resetPublished(t, cutest.WidgetGVR) })
			var drops atomic.Bool
			drops.Store(c.drops)
			p := newPruneRig(t, fmt.Sprintf("Epoch%d", i), "v1beta1", roundTrips(t, func(u *unstructured.Unstructured) error {
				if drops.Load() {
					return dropsAnnotations(u)
				}
				return nil
			}))
			gk := schema.GroupKind{Group: pruneGroup, Kind: p.kind}
			s := cu.NewShared(mappedClients(t, gk), cu.SharedOptions{})
			var jsonPatches atomic.Int64 // the restores, and R3
			s.HookCalls(nil, func(call cu.Call, err error) {
				if call.Op == "patch" && call.GK == gk && call.PatchType == types.JSONPatchType && err == nil {
					jsonPatches.Add(1)
				}
			})
			var wants atomic.Bool
			wants.Store(true)
			o := widgetOptions(func(types.NamespacedName) {})
			o.Protected = []cu.ProtectedKind{{GVK: gk.WithVersion("v1")}}
			o.Refs = func(obj metav1.Object) ([]cu.Ref, error) {
				if !wants.Load() {
					return nil, nil
				}
				return []cu.Ref{{GroupKind: gk, Namespace: obj.GetNamespace(), Name: "p"}}, nil
			}
			tr, err := s.Track(ctx, o)
			if err != nil {
				t.Fatal(err)
			}
			running(t, s)
			p.create("p")
			createWidget(t, p.ns, "w", "unused")
			pass := func() cu.Acquired {
				t.Helper()
				w, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(p.ns).Get(ctx, "w", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				a, err := tr.Acquire(ctx, w)
				if err != nil {
					t.Fatal(err)
				}
				return a
			}
			f := widgetFinalizer(t, p.ns)
			object := func() *metav1.PartialObjectMetadata {
				t.Helper()
				m, err := env.Metadata.Resource(p.gvr("v1")).Namespace(p.ns).Get(ctx, "p", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				return m
			}
			if a := pass(); len(a.Held) != 1 {
				t.Fatalf("Acquire = %+v; want the object held", a)
			}
			if m := object(); !hasString(m.Finalizers, f) || (m.Annotations[f] == "") != c.drops {
				t.Fatalf("precondition: finalizers %v, epoch %q", m.Finalizers, m.Annotations[f])
			}
			wants.Store(false)
			pass() // the record goes
			if c.drops {
				cutest.Eventually(t, time.Minute, func() error {
					if n := jsonPatches.Load(); n < 5 {
						return fmt.Errorf("%d restores: %w", n, cutest.ErrNotYet)
					}
					return nil
				})
				if m := object(); !hasString(m.Finalizers, f) || m.Annotations[f] != "" {
					t.Fatalf("after 5 restores: finalizers %v, epoch %q; want the finalizer, and no epoch", m.Finalizers, m.Annotations[f])
				}
				t.Logf("META never released: %d JSON patches and no release", jsonPatches.Load())
				if c.strip {
					cutest.Eventually(t, time.Minute, func() error { // it loses to a restore that comes between
						pt, err := cu.DisownPatch(object(), widgetGK)
						if err != nil {
							return err
						}
						_, err = send(ctx, env.Metadata.Resource(p.gvr("v1")).Namespace(p.ns), "p", pt)
						return err
					})
				} else {
					drops.Store(false)
				}
			}
			cutest.Eventually(t, time.Minute, func() error {
				if m := object(); hasString(m.Finalizers, f) {
					return fmt.Errorf("finalizers %v: %w", m.Finalizers, cutest.ErrNotYet)
				}
				return nil
			})
		})
	}
}
