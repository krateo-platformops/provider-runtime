package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"
)

// A holder kind whose CRD was deleted, and created again with another scope
// (§6.2): spec.scope can't change on an established CRD. A process that mapped
// it before keeps that mapping. What the old one can't list is never "no
// users".

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
		scope  meta.RESTScope
		n      string   // the batch's namespace
		users  []string // the namespaces of holders that name the Secret
		held   bool     // after the batch
		listed string   // the namespace the list asked for
	}{
		// The process mapped the kind before it was namespaced.
		"mapped as it was, a user there":              {meta.RESTScopeRoot, "ns", []string{"ns"}, true, ""},
		"mapped as it was, users there and elsewhere": {meta.RESTScopeRoot, "ns", []string{"ns", "other"}, true, ""},
		"mapped as it was, a user elsewhere alone":    {meta.RESTScopeRoot, "ns", []string{"other"}, false, ""},
		"mapped as it was, no user":                   {meta.RESTScopeRoot, "ns", nil, false, ""},
		// The kind is cluster-scoped, and the finalizer from when it wasn't: the
		// cluster-scoped user has a finalizer of its own.
		"cluster-scoped, an old finalizer":                 {meta.RESTScopeRoot, "ns", []string{""}, false, ""},
		"cluster-scoped, a user":                           {meta.RESTScopeRoot, "", []string{""}, true, ""},
		"cluster-scoped, no user":                          {meta.RESTScopeRoot, "", nil, false, ""},
		"namespaced, a user there":                         {meta.RESTScopeNamespace, "ns", []string{"ns"}, true, "ns"},
		"namespaced, a user elsewhere alone":               {meta.RESTScopeNamespace, "ns", []string{"other"}, false, "ns"},
		"namespaced, no user":                              {meta.RESTScopeNamespace, "ns", nil, false, "ns"},
		"namespaced, a cluster-scoped finalizer, any user": {meta.RESTScopeNamespace, "", []string{"ns"}, true, ""},
	} {
		objs := []runtime.Object{heldObject(secretGVK, "ns", "s", "u1", widgetGK, c.n)}
		for _, ns := range c.users {
			objs = append(objs, holderObject(ns, "h", id))
		}
		r, tr, _ := scopeRig(t, c.scope, objs...)
		r.md.ClearActions()
		res, err := tr.proto.release(ctx, c.n, []ID{id})
		if err != nil || (len(res.Released) == 0) != c.held {
			t.Errorf("%s: %+v, %v; want held %v", name, res, err, c.held)
		}
		if got := hasFinalizer(r.secretMeta(t, "ns", "s"), mustFinalizer(widgetGK, c.n)); got != c.held {
			t.Errorf("%s: held %v, want %v", name, got, c.held)
		}
		var lists []string
		for _, a := range r.md.Actions() {
			if a.GetVerb() == "list" && a.GetResource().Resource == "widgets" {
				lists = append(lists, a.GetNamespace())
			}
		}
		if !slices.Equal(lists, []string{c.listed}) {
			t.Errorf("%s: lists of the holders in %q, want one, in %q", name, lists, c.listed)
		}
		// Users, for diagnostics, says the same.
		users, err := tr.Users(ctx, SecretRef("ns", "s"), c.n)
		if err != nil || (len(users) > 0) != c.held {
			t.Errorf("%s: Users = %v, %v; want some %v", name, users, err, c.held)
		}
	}
}

// A list that fails is never "no users", whatever the mapping: with the CRD gone,
// the API server's 404, of a kind that is cluster-scoped now for a process that
// mapped it namespaced, the same, or a refusal. The batch is retried, and the
// hold stays.
func TestABatchWhoseHolderListFails(t *testing.T) {
	ctx := context.Background()
	id := secretID("ns", "s")
	forbidden := apierrors.NewForbidden(schema.GroupResource{Group: widgetGK.Group, Resource: "widgets"}, "", errors.New(
		`User "system:serviceaccount:default:provider" cannot list resource "widgets" in API group "credentialuse.test.krateo.io" at the cluster scope`))
	for name, c := range map[string]struct {
		scope meta.RESTScope
		err   error
		is    func(error) bool
		in    string // the namespace the list asks for
	}{
		"mapped cluster-scoped, the CRD gone":           {meta.RESTScopeRoot, unservedError(), notServed, ""},
		"mapped cluster-scoped, forbidden":              {meta.RESTScopeRoot, forbidden, apierrors.IsForbidden, ""},
		"mapped namespaced, the kind is cluster-scoped": {meta.RESTScopeNamespace, unservedError(), notServed, "ns"},
		"mapped namespaced, forbidden":                  {meta.RESTScopeNamespace, forbidden, apierrors.IsForbidden, "ns"},
	} {
		t.Run(name, func(t *testing.T) {
			r, tr, w := scopeRig(t, c.scope, heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"))
			r.md.PrependReactor("list", "widgets", func(a clienttesting.Action) (bool, runtime.Object, error) {
				if a.GetNamespace() != c.in {
					t.Errorf("a list in %q, want in %q", a.GetNamespace(), c.in)
				}
				return true, nil, c.err
			})
			if _, err := tr.proto.release(ctx, "ns", []ID{id}); err == nil || !c.is(err) {
				t.Errorf("release = %v; want the API server's answer in its chain", err)
			}
			if _, err := tr.Users(ctx, SecretRef("ns", "s"), "ns"); err == nil || !c.is(err) {
				t.Errorf("Users = %v; want the API server's answer", err)
			}
			k := batchKey{widgetGK, "ns"}
			for n, d := range []time.Duration{1, 2, 4} {
				r.s.releaseBatch(ctx, k, []ID{id})
				if !r.heldBy(t, widgetGK, "ns", "s") {
					t.Fatalf("released, at run %d", n+1)
				}
				w.mu.Lock()
				if len(w.got) != n+1 || !strings.Contains(w.got[n], "release batch failed") {
					t.Errorf("run %d: warnings %q; want one more", n+1, w.got)
				}
				w.mu.Unlock()
				r.clk.Step(d * time.Second)
				if got := r.queued(widgetGK, "ns"); !slices.Equal(got, []ID{id}) {
					t.Fatalf("run %d: the queued batch is %v, want it retried", n+1, got)
				}
				r.drain()
			}
		})
	}
}

// The prober doesn't list the holder kind, cluster-scoped and protected too, for
// refs that name it with a namespace: each names no object, and backs off.
func TestProbeOfAClusterScopedHolderKindNamedWithANamespace(t *testing.T) {
	r, tr, _ := scopeRig(t, meta.RESTScopeRoot)
	tr.o.ProbeInterval = time.Second
	var ids []ID
	for i := range probeGetLimit + 2 {
		ids = append(ids, ID{Group: widgetGK.Group, Kind: widgetGK.Kind, Namespace: "ns", Name: fmt.Sprintf("h%d", i)})
	}
	tr.missing.set(types.NamespacedName{Name: "h"}, ids, r.clk.Now(), time.Second)
	r.md.ClearActions()
	for range 2 {
		r.clk.Step(5 * time.Second)
		tr.probe(context.Background())
	}
	if len(r.md.Actions()) != 0 {
		t.Errorf("calls were made: %v", r.md.Actions())
	}
	for _, id := range ids {
		if got := tr.missing.refs[id].backoff; got != 4*time.Second {
			t.Errorf("%s backs off to %s, want 4s", id, got)
		}
	}
}

// The list is filtered on every page, and only the holder kind's is made.
func TestAListOfAHolderKindMappedClusterScopedIsFiltered(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var asked []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		asked = append(asked, r.URL.Path+" "+r.URL.Query().Get("continue"))
		mu.Unlock()
		item := func(ns, name string) string {
			return fmt.Sprintf(`{"apiVersion":"meta.k8s.io/v1","kind":"PartialObjectMetadata","metadata":{"namespace":%q,"name":%q,"uid":%q}}`, ns, name, ns+name)
		}
		next, items := "2", item("a", "1")+","+item("b", "1")
		if r.URL.Query().Get("continue") == "2" {
			next, items = "", item("b", "2")+","+item("a", "2")+","+item("", "a")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"apiVersion":"meta.k8s.io/v1","kind":"PartialObjectMetadataList","metadata":{"continue":"`+next+`"},"items":[`+items+`]}`)
	}))
	defer srv.Close()
	md, err := metadata.NewForConfig(&rest.Config{Host: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	thing := schema.GroupVersionKind{Group: "x.io", Version: "v1", Kind: "Thing"}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Version: "v1"}, thing.GroupVersion()})
	mapper.Add(thing, meta.RESTScopeRoot)
	mapper.Add(secretGVK, meta.RESTScopeNamespace)
	for ns, want := range map[string][]string{"a": {"a/1", "a/2"}, "b": {"b/1", "b/2"}, "c": nil, "": {"a/1", "b/1", "b/2", "a/2", "/a"}} {
		l, err := clientServer{md: md, mapper: mapper, holder: thing}.List(ctx, thing.GroupKind(), ns)
		var got []string
		for i := range l {
			got = append(got, l[i].Namespace+"/"+l[i].Name)
		}
		if err != nil || !slices.Equal(got, want) {
			t.Errorf("in %q: %v, %v; want %v", ns, got, err, want)
		}
		if a := []string{"/apis/x.io/v1/things ", "/apis/x.io/v1/things 2"}; !slices.Equal(asked, a) {
			t.Errorf("in %q: the calls %q, want %q", ns, asked, a)
		}
		asked = nil
	}
	// Another kind's, and one of no tracker's, names no object: no call.
	for _, s := range []clientServer{{md: md, mapper: mapper}, {md: md, mapper: mapper, holder: secretGVK}} {
		if l, err := s.List(ctx, thing.GroupKind(), "a"); err != nil || len(l) != 0 || len(asked) != 0 {
			t.Errorf("a cluster-scoped kind that isn't the holder's: %v, %v, the calls %q; want none", l, err, asked)
		}
	}
}
