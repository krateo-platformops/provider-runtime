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
// stale, or if it reverts a status field the method wrote, which is what a
// method that copies back only the resourceVersion does. It checks status
// fields only: a status write can't change anything else.
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

// annotationVersionMoved is the annotation AssertFailedWriteLeavesManaged
// writes to move the stored resourceVersion before it calls the method.
const annotationVersionMoved = "reconcilertest.krateo.io/version-moved"

// AssertFailedWriteLeavesManaged checks the ordering rule on ExternalClient: a
// method whose write of the managed object fails must leave mg's
// resourceVersion untouched. A version copied onto mg before the write
// succeeded, from a re-read, would let the runtime's next write succeed
// against an object the method never wrote, where a stale version fails
// loudly.
//
// mg must be stored in kube at the resourceVersion it carries: create it, or
// read it, through kube first. kube must fail the method's writes of the
// object; with controller-runtime's fake client, build it with
// WithInterceptorFuncs and, for a method that writes status, an
// interceptor.Funcs whose SubResourceUpdate (or SubResourcePatch) returns a
// conflict.
//
// Before it calls the method, the helper moves the stored resourceVersion with
// a write of its own: a kube.Update that adds one annotation. A method that
// re-reads then gets a version mg doesn't carry, so copying it onto mg too
// early shows. kube must let that Update through. Failing only status writes
// does. If the method also writes the object with Update, arm that failure
// inside method, for example with a flag the interceptor reads and method sets
// before it calls the provider's code.
//
// The helper then calls method(ctx, mg), and fails t:
//   - if the method returns nil: its write didn't fail, so this is not the path
//     the helper checks;
//   - if the method's write landed anyway (the stored version moved past the
//     helper's own write): kube doesn't fail the method's writes;
//   - if mg's resourceVersion changed.
//
// It checks only the resourceVersion. A method may change mg's status in
// memory on failure; the runtime then writes it, and that write fails on the
// stale version, as it should. A retry loop such as retry.RetryOnConflict that
// fails on every attempt is covered: each attempt's re-read version must stay
// off mg. It returns what the method returned.
func AssertFailedWriteLeavesManaged(t testing.TB, ctx context.Context, kube client.Client, mg resource.Managed, method Method) error {
	t.Helper()

	key := client.ObjectKeyFromObject(mg)
	cur, ok := stored(t, ctx, kube, key, mg, "before the method")
	if !ok {
		return nil
	}
	if rv := mg.GetResourceVersion(); rv != cur.GetResourceVersion() {
		t.Errorf("mg carries resourceVersion %q, but %q is stored: read mg through kube before calling AssertFailedWriteLeavesManaged",
			rv, cur.GetResourceVersion())
		return nil
	}

	// Move the stored version, so a re-read returns one mg doesn't carry.
	ann := cur.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	ann[annotationVersionMoved] = "true"
	cur.SetAnnotations(ann)
	if err := kube.Update(ctx, cur); err != nil {
		t.Errorf("the helper's own write, a kube.Update adding the %s annotation, failed: kube must let it through "+
			"and fail only the method's writes: %v", annotationVersionMoved, err)
		return nil
	}
	moved := cur.GetResourceVersion()
	want := mg.GetResourceVersion()

	err := method(ctx, mg)

	if err == nil {
		t.Errorf("the method returned nil, so its write didn't fail: arrange kube to fail the method's writes of the object")
		return nil
	}
	after, ok := stored(t, ctx, kube, key, mg, "after the method")
	if !ok {
		return err
	}
	if got := after.GetResourceVersion(); got != moved {
		t.Errorf("the method's write landed (the stored resourceVersion moved from %q to %q): kube must fail the method's writes of the object",
			moved, got)
		return err
	}
	if got := mg.GetResourceVersion(); got != want {
		t.Errorf("the method's write failed, but mg's resourceVersion changed from %q to %q (%q is stored): "+
			"copy a written resourceVersion onto mg only after the write succeeded (see reconciler.ExternalClient)",
			want, got, moved)
	}
	return err
}

// stored reads key from kube into a new, zero object of mg's type. Not a copy
// of mg: a decoder that doesn't zero its target, as the REST client's JSON
// decoding doesn't, would keep mg's value of a field the stored object omits,
// and a field the method cleared would look unchanged.
func stored(t testing.TB, ctx context.Context, kube client.Client, key client.ObjectKey, mg resource.Managed, when string) (client.Object, bool) {
	t.Helper()
	ty := reflect.TypeOf(mg)
	if ty.Kind() != reflect.Pointer || ty.Elem().Kind() != reflect.Struct {
		t.Errorf("mg is a %s, not a pointer to a struct", ty)
		return nil, false
	}
	obj, ok := reflect.New(ty.Elem()).Interface().(client.Object)
	if !ok {
		t.Errorf("a new %s is not a client.Object", ty)
		return nil, false
	}
	if err := kube.Get(ctx, key, obj); err != nil {
		t.Errorf("cannot read the stored object %s: %v", when, err)
		return nil, false
	}
	return obj, true
}

func isStatus(path string) bool {
	return path == "status" || strings.HasPrefix(path, "status.")
}

// ignored is the Synced condition, which the helper sets itself, as the
// runtime does.
func ignored(path string) bool {
	return strings.HasPrefix(path, "status.conditions["+string(prv1.TypeSynced)+"]")
}

// revertedPaths are the leaf status paths the method changed (before to
// written) whose value the status write then changed again (written to
// after), sorted.
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
		if !isStatus(path) || ignored(path) || same(b, w, path) {
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
// leaves. An empty object records nothing, so it reads as absent: a cleared
// field is reported by its own path, not also by its emptied parent's.
func flatten(prefix string, v any, out map[string]any) {
	switch x := v.(type) {
	case map[string]any:
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
