package credentialuse

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// A holder kind whose CRD was cluster-scoped, and was created again namespaced
// (§6.2). A process that mapped it before keeps that mapping; it runs beside one
// that maps it as it is, whose holders are in namespaces. What the old one can't
// list is never "no users".

// scopeRig is a rig whose mapper has Widgets at scope. Nothing is cached: its
// informers don't run, so R0 decides nothing, as with a cache that doesn't
// show the holder.
func scopeRig(t *testing.T, scope meta.RESTScope, objs ...runtime.Object) (*fakeRig, *Tracker, *warnings) {
	t.Helper()
	r := newFakeRig(t, append(objs, namespaceObject("ns"))...)
	widgets := widgetGK.WithVersion("v1")
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Version: "v1"}, widgets.GroupVersion()})
	mapper.Add(widgets, scope)
	mapper.Add(secretGVK, meta.RESTScopeNamespace)
	mapper.Add(schema.GroupVersionKind{Version: "v1", Kind: "Namespace"}, meta.RESTScopeRoot)
	r.s.c.Mapper = mapper
	w := &warnings{}
	o := r.options(widgetGK)
	o.Logger = w
	tr, err := r.s.Track(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	r.drain()
	return r, tr, w
}

func TestR2OfAHolderKindMappedClusterScoped(t *testing.T) {
	ctx := context.Background()
	id := secretID("ns", "s")
	for name, c := range map[string]struct {
		scope meta.RESTScope
		n     string // the batch's namespace
		users bool
		fails bool
		held  bool // after it
	}{
		"mapped as it was, with a user":    {meta.RESTScopeRoot, "ns", true, true, true},
		"mapped as it was, without one":    {meta.RESTScopeRoot, "ns", false, true, true},
		"mapped as it is, with a user":     {meta.RESTScopeNamespace, "ns", true, false, true},
		"mapped as it is, without one":     {meta.RESTScopeNamespace, "ns", false, false, false},
		"cluster-scoped, with a user":      {meta.RESTScopeRoot, "", true, false, true},
		"cluster-scoped, without one":      {meta.RESTScopeRoot, "", false, false, false},
		"namespaced, of no namespace":      {meta.RESTScopeNamespace, "", true, false, true},
		"namespaced, of none, without one": {meta.RESTScopeNamespace, "", false, false, false},
	} {
		objs := []runtime.Object{heldObject(secretGVK, "ns", "s", "u1", widgetGK, c.n)}
		if c.users {
			objs = append(objs, holderObject(c.n, "h", id))
		}
		r, tr, _ := scopeRig(t, c.scope, objs...)
		res, err := tr.proto.release(ctx, c.n, []ID{id})
		if (err != nil) != c.fails || (len(res.Released) == 0) != c.held || len(res.Bare) != len(res.Released) {
			t.Errorf("%s: %+v, %v; want failed %v, held %v", name, res, err, c.fails, c.held)
		}
		if got := hasFinalizer(r.secretMeta(t, "ns", "s"), mustFinalizer(widgetGK, c.n)); got != c.held {
			t.Errorf("%s: held %v, want %v", name, got, c.held)
		}
		// Users, for diagnostics, says no less.
		users, err := tr.Users(ctx, SecretRef("ns", "s"), c.n)
		if (err != nil) != c.fails || err == nil && (len(users) == 1) != c.users {
			t.Errorf("%s: Users = %v, %v", name, users, err)
		}
	}
}

// The release loop retries such a batch, and says why; the hold stays.
func TestABatchOfAHolderKindMappedClusterScopedIsRetried(t *testing.T) {
	id := secretID("ns", "s")
	r, _, w := scopeRig(t, meta.RESTScopeRoot, heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"), holderObject("ns", "h", id))
	k := batchKey{widgetGK, "ns"}
	for n, d := range []time.Duration{1, 2, 4} {
		r.s.releaseBatch(context.Background(), k, []ID{id})
		if !r.heldBy(t, widgetGK, "ns", "s") {
			t.Fatalf("released, at run %d", n+1)
		}
		w.mu.Lock()
		if len(w.got) != n+1 || !strings.Contains(w.got[n], "cluster-scoped") {
			t.Errorf("run %d: warnings %q; want one more, saying why", n+1, w.got)
		}
		w.mu.Unlock()
		r.clk.Step(d * time.Second)
		if got := r.queued(widgetGK, "ns"); !slices.Equal(got, []ID{id}) {
			t.Fatalf("run %d: the queued batch is %v, want it retried", n+1, got)
		}
		r.drain()
	}
}
