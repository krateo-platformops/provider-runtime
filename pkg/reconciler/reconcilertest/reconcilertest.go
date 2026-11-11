// Package reconcilertest helps providers test their ExternalClient against
// the contracts pkg/reconciler relies on.
package reconcilertest

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/reconciler"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
)

// AssertObserveSetsReady checks the Ready contract (since v1.8.0) on one
// path of c's Observe: every successful Observe must set the Ready condition.
//
// It sets Ready on mg to prv1.NotObserved(), the marker the runtime leaves
// after a failed Connect or Observe, calls c.Observe(ctx, mg), and fails t if
// Ready's reason is still NotObserved afterwards, because the runtime would
// then keep the marker and report the provider as broken. Preparing mg is the
// helper's job, so the check cannot pass just because mg already carried a
// Ready from its fixture.
//
// The path must be one on which Observe owns Ready: it returns a nil error,
// or an error wrapped with reconciler.ReadyAssessed. A plain error fails t,
// because on that path the runtime sets Ready itself and there is nothing for
// this helper to check; a test that expected success has drifted.
//
// Call it once per success path of Observe, with mg and the external fakes
// arranged to reach that path. It returns what Observe returned, for further
// assertions.
func AssertObserveSetsReady(t testing.TB, ctx context.Context, c reconciler.ExternalClient, mg resource.Managed) (reconciler.ExternalObservation, error) {
	t.Helper()

	mg.SetConditions(prv1.NotObserved())
	obs, err := c.Observe(ctx, mg)

	if err != nil && !reconciler.IsReadyAssessed(err) {
		t.Errorf("Observe returned an error not wrapped with reconciler.ReadyAssessed, so this is not a path on which Observe owns Ready: %v", err)
		return obs, err
	}
	if ready := mg.GetCondition(prv1.TypeReady); ready.Reason == prv1.ReasonNotObserved {
		if err != nil {
			t.Errorf("Observe returned an error wrapped with reconciler.ReadyAssessed but did not set the Ready condition (still %s); "+
				"ReadyAssessed promises that Observe set Ready: %v", prv1.ReasonNotObserved, err)
			return obs, err
		}
		t.Errorf("Observe succeeded but did not set the Ready condition (still %s): "+
			"every successful Observe must set Ready", prv1.ReasonNotObserved)
	}
	return obs, err
}

// A Method is one of an ExternalClient's methods, or any call that gets mg as
// the runtime hands it over: c.Create, c.Update, c.Delete, or a closure around
// c.Observe.
type Method func(ctx context.Context, mg resource.Managed) error

// AssertLeavesManagedCurrent checks the contract on ExternalClient for a
// method that writes the managed object itself: it must leave mg describing
// what is stored.
//
// mg must be stored in kube at the resourceVersion it carries: create it, or
// read it, through kube first. kube is the client the method writes through,
// an envtest client or controller-runtime's fake client built with
// WithStatusSubresource. The helper calls method(ctx, mg), then does what the
// runtime does next: it sets Synced on mg (ReconcileSuccess, or ReconcileError
// with the method's error) and writes mg's status with kube.Status().Update.
// It fails t if that write conflicts, because the method wrote and left mg
// stale, or if it reverts any field the method wrote, which is what a method
// that copies back only the resourceVersion does.
//
// A method that writes nothing passes. It returns what the method returned.
func AssertLeavesManagedCurrent(t testing.TB, ctx context.Context, kube client.Client, mg resource.Managed, method Method) error {
	t.Helper()

	key := client.ObjectKeyFromObject(mg)
	before, ok := stored(t, ctx, kube, key, mg, "before the method")
	if !ok {
		return nil
	}
	if rv := mg.GetResourceVersion(); rv != before.GetResourceVersion() {
		t.Errorf("mg carries resourceVersion %q, but %q is stored: read mg through kube before calling AssertLeavesManagedCurrent",
			rv, before.GetResourceVersion())
		return nil
	}

	err := method(ctx, mg)

	written, ok := stored(t, ctx, kube, key, mg, "after the method")
	if !ok {
		return err
	}
	if err != nil {
		mg.SetConditions(prv1.ReconcileError(err))
	} else {
		mg.SetConditions(prv1.ReconcileSuccess())
	}
	if werr := kube.Status().Update(ctx, mg); werr != nil {
		if kerrors.IsConflict(werr) {
			t.Errorf("the runtime's status write after the method conflicted: the method wrote the object and left mg stale. "+
				"Write mg itself, or copy the written resourceVersion and every field you wrote onto mg (see reconciler.ExternalClient): %v", werr)
			return err
		}
		t.Errorf("the runtime's status write after the method failed: %v", werr)
		return err
	}
	after, ok := stored(t, ctx, kube, key, mg, "after the status write")
	if !ok {
		return err
	}

	reverted, cerr := revertedPaths(before, written, after)
	if cerr != nil {
		t.Errorf("cannot compare the stored object: %v", cerr)
		return err
	}
	if len(reverted) > 0 {
		t.Errorf("the runtime's status write after the method reverted what the method wrote: %s. "+
			"Copying back only the resourceVersion isn't enough: copy every field you wrote onto mg too (see reconciler.ExternalClient)",
			strings.Join(reverted, ", "))
	}
	return err
}

// stored reads key from kube into a new object of mg's type.
func stored(t testing.TB, ctx context.Context, kube client.Client, key client.ObjectKey, mg resource.Managed, when string) (client.Object, bool) {
	t.Helper()
	obj, ok := mg.DeepCopyObject().(client.Object)
	if !ok {
		t.Errorf("mg's DeepCopyObject is not a client.Object")
		return nil, false
	}
	if err := kube.Get(ctx, key, obj); err != nil {
		t.Errorf("cannot read the stored object %s: %v", when, err)
		return nil, false
	}
	return obj, true
}

// ignored are paths a status write may change, or that the helper changes
// itself: the version and managedFields, and the Synced condition the helper
// sets as the runtime does.
func ignored(path string) bool {
	return path == "metadata.resourceVersion" ||
		strings.HasPrefix(path, "metadata.managedFields") ||
		strings.HasPrefix(path, "status.conditions["+string(prv1.TypeSynced)+"]")
}

// revertedPaths are the leaf paths the method changed (before to written)
// whose value the status write then changed again (written to after), sorted.
func revertedPaths(before, written, after runtime.Object) ([]string, error) {
	leaves := make([]map[string]any, 3)
	for i, obj := range []runtime.Object{before, written, after} {
		u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
		if err != nil {
			return nil, err
		}
		leaves[i] = map[string]any{}
		flatten("", u, leaves[i])
	}
	b, w, a := leaves[0], leaves[1], leaves[2]

	var out []string
	for path := range union(b, w) {
		if ignored(path) || same(b, w, path) {
			continue // the method didn't write it
		}
		if !same(w, a, path) {
			out = append(out, path)
		}
	}
	slices.Sort(out)
	return out, nil
}

// flatten records every leaf of v under prefix. A list of objects that all
// carry a string type, such as conditions, is keyed by type; other lists are
// leaves.
func flatten(prefix string, v any, out map[string]any) {
	switch x := v.(type) {
	case map[string]any:
		if len(x) == 0 {
			out[prefix] = x
			return
		}
		for k, e := range x {
			flatten(join(prefix, k), e, out)
		}
	case []any:
		types := make([]string, 0, len(x))
		for _, e := range x {
			m, ok := e.(map[string]any)
			if !ok {
				break
			}
			ty, ok := m["type"].(string)
			if !ok {
				break
			}
			types = append(types, ty)
		}
		if len(x) == 0 || len(types) != len(x) {
			out[prefix] = x
			return
		}
		for i, e := range x {
			flatten(prefix+"["+types[i]+"]", e, out)
		}
	default:
		out[prefix] = x
	}
}

func join(prefix, k string) string {
	if prefix == "" {
		return k
	}
	return prefix + "." + k
}

func union(a, b map[string]any) map[string]struct{} {
	out := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		out[k] = struct{}{}
	}
	for k := range b {
		out[k] = struct{}{}
	}
	return out
}

// same reports whether path has the same value, or is absent, in both a and b.
func same(a, b map[string]any, path string) bool {
	va, oka := a[path]
	vb, okb := b[path]
	return oka == okb && reflect.DeepEqual(va, vb)
}
