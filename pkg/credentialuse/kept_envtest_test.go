package credentialuse_test

// The consent gate on refs that a record keeps, though its holder's spec no
// longer names them, against envtest (§11.3, §9.1, §8).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
)

// keptRefs is a Widget's RefsFunc: spec.secrets are Secrets, "<name>" or
// "<namespace>/<name>"; spec.target is a Target whose spec.secret names one
// more; and with spec.refsError it fails.
func keptRefs(obj metav1.Object) ([]cu.Ref, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("a %T, not an unstructured Widget", obj)
	}
	if fail, _, _ := unstructured.NestedBool(u.Object, "spec", "refsError"); fail {
		return nil, errors.New("the spec names nothing")
	}
	var out []cu.Ref
	secrets, _, err := unstructured.NestedStringSlice(u.Object, "spec", "secrets")
	if err != nil {
		return nil, errors.New("spec.secrets isn't a list of names")
	}
	for _, s := range secrets {
		ns, name, other := strings.Cut(s, "/")
		if !other {
			ns, name = obj.GetNamespace(), s
		}
		out = append(out, cu.SecretRef(ns, name))
	}
	if name, _, _ := unstructured.NestedString(u.Object, "spec", "target"); name != "" {
		out = append(out, cu.Ref{
			GroupKind: cutest.TargetGVK.GroupKind(), Namespace: obj.GetNamespace(), Name: name,
			Next: func(kt *unstructured.Unstructured) ([]cu.Ref, error) {
				secret, _, _ := unstructured.NestedString(kt.Object, "spec", "secret")
				if secret == "" {
					return nil, fmt.Errorf("Target %s names no spec.secret", kt.GetName())
				}
				return []cu.Ref{cu.SecretRef(kt.GetNamespace(), secret)}, nil
			},
		})
	}
	return out, nil
}

// keptWorld is a Widget, home/w, that held the Secret other/x with consent.
type keptWorld struct {
	*writeRig
	home, other string
	// failA2 names the Target in home whose A2 fails, if any.
	failA2 atomic.Pointer[string]
	// invalid counts the record writes refused as invalid, and forgedGets
	// the GETs of other's Secrets named forged-….
	invalid, forgedGets atomic.Int64
}

func newKeptWorld(t *testing.T) *keptWorld {
	t.Helper()
	return newKeptWorldWith(t, cu.SharedOptions{}, nil)
}

// newKeptWorldWith is newKeptWorld with SharedOptions so, and the options
// change makes.
func newKeptWorldWith(t *testing.T, so cu.SharedOptions, change func(o *cu.Options)) *keptWorld {
	t.Helper()
	k := &keptWorld{home: env.Namespace(t, "kept-home"), other: env.Namespace(t, "kept-other")}
	k.writeRig = newWriteRig(t, env.Config, so, func(o *cu.Options) {
		o.Refs = keptRefs
		o.Protected = []cu.ProtectedKind{{GVK: secretGVK}, {GVK: cutest.TargetGVK, Content: true}}
		if change != nil {
			change(o)
		}
	}, func(s *cu.Shared) {
		s.HookCalls(func(_ context.Context, c cu.Call) error {
			if name := k.failA2.Load(); name != nil && c.Op == "patch" && c.GK.Kind == "Target" && c.Namespace == k.home && c.Name == *name {
				return apierrors.NewServiceUnavailable("injected")
			}
			if c.Op == "get" && c.GK.Kind == "Secret" && c.Namespace == k.other && strings.HasPrefix(c.Name, "forged-") {
				k.forgedGets.Add(1)
			}
			return nil
		}, func(c cu.Call, err error) {
			if c.Op == "patch" && c.GK.Kind == "Widget" && apierrors.IsInvalid(err) {
				k.invalid.Add(1)
			}
		})
	})
	createSecret(t, k.other, "x")
	setSecretConsent(t, k.other, "x", k.home)
	w := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"secrets": []any{k.other + "/x"}}}}
	w.SetGroupVersionKind(cutest.WidgetGVK)
	w.SetNamespace(k.home)
	w.SetName("w")
	if _, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(k.home).Create(context.Background(), w, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	k.c.enqueue(widgetNN(k.home, "w"))
	cutest.Eventually(t, time.Minute, heldFor(k.other, "x", k.home))
	cutest.Eventually(t, time.Minute, func() error {
		if o := k.widget(t); !hasString(o.GetFinalizers(), ownerFinalizer) || !k.recorded(t) {
			return fmt.Errorf("w isn't established with x recorded: %w", cutest.ErrNotYet)
		}
		return nil
	})
	return k
}

func (k *keptWorld) widget(t *testing.T) *unstructured.Unstructured {
	t.Helper()
	o, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(k.home).Get(context.Background(), "w", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func (k *keptWorld) recorded(t *testing.T) bool {
	t.Helper()
	return cu.ReadRecord(k.widget(t)).Names(cu.SecretRef(k.other, "x").ID())
}

// name makes w name what spec says, and not x.
func (k *keptWorld) name(t *testing.T, spec map[string]any) {
	t.Helper()
	if _, has := spec["secrets"]; !has {
		spec["secrets"] = nil
	}
	data, err := json.Marshal(map[string]any{"spec": spec})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(k.home).Patch(context.Background(), "w", types.MergePatchType, data, metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
}

// passes runs at least n more passes of w.
func (k *keptWorld) passes(t *testing.T, n int) {
	t.Helper()
	for range n {
		before := k.c.passes.Load()
		k.c.enqueue(widgetNN(k.home, "w"))
		cutest.Eventually(t, time.Minute, func() error {
			if k.c.passes.Load() == before {
				return fmt.Errorf("no pass: %w", cutest.ErrNotYet)
			}
			return nil
		})
	}
	time.Sleep(300 * time.Millisecond) // the last of them
}

// keptState says why x is still held, for a failure's message.
func (k *keptWorld) keptState(t *testing.T) string {
	t.Helper()
	w := k.widget(t)
	fs := []string{"(the Secret is gone)"}
	if s, err := env.Kube.CoreV1().Secrets(k.other).Get(context.Background(), "x", metav1.GetOptions{}); err == nil {
		fs = append([]string{fmt.Sprintf("deleting %v", s.DeletionTimestamp != nil)}, s.Finalizers...)
	}
	a := k.c.lastResult(widgetNN(k.home, "w")).Acquired
	return fmt.Sprintf("w's record %v, unheld %+v, last pass degraded %v (%v) unheld %v, RetryAfter %v; x's finalizers %v",
		cu.ReadRecord(w).IDs, cu.ReadUnheld(w.GetAnnotations()), a.Degraded, a.Err, a.Unheld, a.RetryAfter, fs)
}

// released fails t unless the withdrawn x is dropped from w's record, and
// its finalizer comes off.
func (k *keptWorld) released(t *testing.T) {
	t.Helper()
	from := k.c.passes.Load()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		k.c.enqueue(widgetNN(k.home, "w"))
		time.Sleep(500 * time.Millisecond)
		if !k.recorded(t) && notHeldFor(k.other, "x", k.home)() == nil {
			return
		}
	}
	t.Errorf("x, whose consent was withdrawn, is still held after %d passes in 20s: %s", k.c.passes.Load()-from, k.keptState(t))
}

// The ways a Widget's pass keeps x, though its spec no longer names it.
func keptEnvCases() map[string]func(t *testing.T, k *keptWorld) {
	return map[string]func(t *testing.T, k *keptWorld){
		"a Missing hop": func(t *testing.T, k *keptWorld) {
			k.name(t, map[string]any{"target": "missing"})
		},
		"a Deleting hop": func(t *testing.T, k *keptWorld) {
			createTarget(t, k.home, "going", "example.com/keep")
			if err := env.Dynamic.Resource(cutest.TargetGVR).Namespace(k.home).Delete(context.Background(), "going", metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			k.name(t, map[string]any{"target": "going"})
		},
		"a Next that fails": func(t *testing.T, k *keptWorld) {
			createTarget(t, k.home, "empty")
			k.name(t, map[string]any{"target": "empty"})
		},
		"a Refs that fails": func(t *testing.T, k *keptWorld) {
			k.name(t, map[string]any{"refsError": true})
		},
		"a hop's A2 that fails": func(t *testing.T, k *keptWorld) {
			createChainTarget(t, k.home, "kt", "home")
			createSecret(t, k.home, "home")
			name := "kt"
			k.failA2.Store(&name)
			k.name(t, map[string]any{"target": "kt"})
		},
	}
}

// A withdrawal releases a hold that the record keeps, whatever keeps it
// (§11.3: the gate runs on every pass for every held cross-namespace ref).
func TestAWithdrawalReleasesAKeptRef(t *testing.T) {
	cutest.Require(t, env)
	for name, keep := range keptEnvCases() {
		t.Run(name, func(t *testing.T) {
			k := newKeptWorld(t)
			keep(t, k)
			k.passes(t, 3)
			if !k.recorded(t) || heldFor(k.other, "x", k.home)() != nil {
				t.Fatalf("the case doesn't keep x while its consent stands: %s", k.keptState(t))
			}
			setSecretConsent(t, k.other, "x", "")
			k.released(t)
		})
	}
	t.Run("control: w still names it", func(t *testing.T) {
		k := newKeptWorld(t)
		setSecretConsent(t, k.other, "x", "")
		k.released(t)
	})
	t.Run("control: nothing keeps it, with consent", func(t *testing.T) {
		k := newKeptWorld(t)
		k.name(t, map[string]any{})
		k.released(t)
	})
}

// annotationCap is the API server's bound on an object's annotations, keys
// and values together.
const annotationCap = 256 << 10

// annotate sets one annotation of w.
func (k *keptWorld) annotate(t *testing.T, key, value string) {
	t.Helper()
	data, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": map[string]any{key: value}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.Dynamic.Resource(cutest.WidgetGVR).Namespace(k.home).Patch(context.Background(), "w", types.MergePatchType, data, metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
}

// A holder's other annotations may fill what the API server allows, so that
// the withdrawn ref's unheld entry doesn't fit where its record entry did: the
// hold is dropped all the same (§11.3).
func TestAWithdrawalReleasesAKeptRefOfAHolderWithFullAnnotations(t *testing.T) {
	cutest.Require(t, env)
	k := newKeptWorld(t)
	k.name(t, map[string]any{"target": "missing"})
	k.passes(t, 3)
	used := 0
	for key, v := range k.widget(t).GetAnnotations() {
		used += len(key) + len(v)
	}
	const pad = "example.com/pad"
	k.annotate(t, pad, strings.Repeat("x", annotationCap-used-len(pad)))
	k.passes(t, 2)
	if !k.recorded(t) {
		t.Fatalf("the case doesn't keep x while its consent stands: %s", k.keptState(t))
	}
	before := k.invalid.Load()
	setSecretConsent(t, k.other, "x", "")
	k.released(t)
	if n := k.invalid.Load() - before; n == 0 {
		t.Error("the API server refused no record write: the case doesn't fill the annotations")
	} else {
		t.Logf("%d record writes refused for their size", n)
	}
}

// Whoever can edit a holder writes its record. One that names many Secrets
// costs no GETs where the caches cover them, and at most 16 a pass where they
// don't; and a real ref among them is still released once its consent is
// withdrawn (§11.3).
func TestAForgedRecordsReads(t *testing.T) {
	cutest.Require(t, env)
	for name, tc := range map[string]struct {
		so      func(t *testing.T) cu.SharedOptions
		change  func(o *cu.Options)
		forged  int
		perPass int64
	}{
		"every namespace cached": {so: func(*testing.T) cu.SharedOptions { return cu.SharedOptions{} }, forged: 1500},
		"not cached": {so: func(t *testing.T) cu.SharedOptions {
			return cu.SharedOptions{Namespaces: []string{env.Namespace(t, "kept-cached")}}
		}, forged: 100, perPass: 16},
		"PerName": {so: func(t *testing.T) cu.SharedOptions {
			return cu.SharedOptions{IndexNamespace: indexNamespace(t)}
		}, change: perName, forged: 1500},
	} {
		t.Run(name, func(t *testing.T) {
			k := newKeptWorldWith(t, tc.so(t), tc.change)
			k.name(t, map[string]any{"refsError": true})
			k.passes(t, 2)
			ids := []cu.ID{cu.SecretRef(k.other, "x").ID()}
			for i := range tc.forged {
				ids = append(ids, cu.SecretRef(k.other, fmt.Sprintf("forged-%05d", i)).ID())
			}
			v, err := cu.EncodeRecord(cu.SortedIDs(ids))
			if err != nil {
				t.Fatal(err)
			}
			k.annotate(t, cu.AnnotationRefs, v)
			k.passes(t, 2)
			passes, gets := k.c.passes.Load(), k.forgedGets.Load()
			k.passes(t, 5)
			// A pass may be under way at either reading.
			passes, gets = k.c.passes.Load()-passes+1, k.forgedGets.Load()-gets
			if gets > tc.perPass*passes {
				t.Errorf("%d GETs of forged refs in %d passes, want at most %d a pass", gets, passes, tc.perPass)
			}
			if tc.perPass > 0 && gets == 0 {
				t.Error("no GETs where no cache covers them: the case reads nothing")
			}
			if !k.recorded(t) {
				t.Fatalf("x, with consent, was dropped: %s", k.keptState(t))
			}
			setSecretConsent(t, k.other, "x", "")
			k.released(t)
		})
	}
}

// x's namespace is deleted while a live Widget elsewhere is recorded as its
// user. §5 keeps x for it, and the documented recovery is the owner's
// withdrawal of consent: it must work for a ref that the record keeps too, or
// the namespace's teardown waits for a holder that no longer names x.
func TestAWithdrawalFreesAKeptRefsTerminatingNamespace(t *testing.T) {
	cutest.Require(t, env)
	ctx := context.Background()
	run := func(t *testing.T, keep func(t *testing.T, k *keptWorld)) {
		k := newKeptWorld(t)
		keep(t, k)
		k.passes(t, 3)
		if err := env.Kube.CoreV1().Namespaces().Delete(ctx, k.other, metav1.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
		deleteSecret(t, k.other, "x")
		time.Sleep(2 * testGrace)
		if heldFor(k.other, "x", k.home)() != nil {
			t.Fatalf("x isn't kept for its recorded user in the first place: %s", k.keptState(t))
		}
		// The owner's recovery. A terminating namespace takes updates.
		setSecretConsent(t, k.other, "x", "")
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			k.c.enqueue(widgetNN(k.home, "w"))
			time.Sleep(500 * time.Millisecond)
			if apierrors.IsNotFound(secretGet(k.other, "x")()) {
				if err := env.FinalizeNamespace(ctx, k.other); err != nil {
					t.Fatal(err)
				}
				return
			}
		}
		t.Errorf("x still blocks its namespace's teardown 20s after its consent was withdrawn: %s", k.keptState(t))
	}
	for name, keep := range keptEnvCases() {
		t.Run(name, func(t *testing.T) { run(t, keep) })
	}
	t.Run("control: w still names it", func(t *testing.T) { run(t, func(*testing.T, *keptWorld) {}) })
}
