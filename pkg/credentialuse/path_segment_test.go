package credentialuse

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"
	metadatafake "k8s.io/client-go/metadata/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	testingclock "k8s.io/utils/clock/testing"
)

// A name, or a namespace, that isn't a single path segment is no object's
// (§6.2): it is NotFound, without a call.
func TestANameNoObjectCanHave(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var asked []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		asked = append(asked, r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"apiVersion":"meta.k8s.io/v1","kind":"PartialObjectMetadata","metadata":{"namespace":"ns","name":"s","uid":"u"}}`)
	}))
	defer srv.Close()
	md, err := metadata.NewForConfig(&rest.Config{Host: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	dyn, err := dynamic.NewForConfig(&rest.Config{Host: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	thing := schema.GroupVersionKind{Group: "x.io", Version: "v1", Kind: "Thing"}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Version: "v1"}, thing.GroupVersion()})
	mapper.Add(secretGVK, meta.RESTScopeNamespace)
	mapper.Add(thing, meta.RESTScopeRoot)
	s, contents := clientServer{md: md, mapper: mapper}, dynamicContent{dyn: dyn, mapper: mapper}
	// A holder kind is asked for at its declared version (§6.2).
	ofSecrets, ofThings := clientServer{md: md, mapper: mapper, holder: secretGVK}, clientServer{md: md, mapper: mapper, holder: thing}
	calls := map[string]func(gk schema.GroupKind, ns, name string) error{
		"Get": func(gk schema.GroupKind, ns, name string) error {
			_, err := s.Get(ctx, gk, ns, name)
			return err
		},
		"Patch": func(gk schema.GroupKind, ns, name string) error {
			_, err := s.Patch(ctx, gk, ns, name, types.JSONPatchType, []byte(`[]`))
			return err
		},
		"Get, of a holder kind": func(gk schema.GroupKind, ns, name string) error {
			srv := ofSecrets
			if gk == thing.GroupKind() {
				srv = ofThings
			}
			_, err := srv.Get(ctx, gk, ns, name)
			return err
		},
		"Patch, of a holder kind": func(gk schema.GroupKind, ns, name string) error {
			srv := ofSecrets
			if gk == thing.GroupKind() {
				srv = ofThings
			}
			_, err := srv.Patch(ctx, gk, ns, name, types.JSONPatchType, []byte(`[]`))
			return err
		},
		"Content": func(gk schema.GroupKind, ns, name string) error {
			_, err := contents.Content(ctx, gk, ns, name)
			return err
		},
	}
	took := func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := asked
		asked = nil
		return out
	}
	for op, call := range calls {
		for _, id := range []ID{
			{Kind: "Secret", Namespace: "ns", Name: "a/b"},
			{Kind: "Secret", Namespace: "ns", Name: ".."},
			{Kind: "Secret", Namespace: "ns", Name: "."},
			{Kind: "Secret", Namespace: "ns", Name: "a%2Fb"},
			{Kind: "Secret", Namespace: "a/b", Name: "s"},
			{Kind: "Secret", Namespace: "..", Name: "s"},
			{Group: "x.io", Kind: "Thing", Name: "a/b"},
			{Group: "x.io", Kind: "Thing", Namespace: "a", Name: "c"},
		} {
			err := call(id.GroupKind(), id.Namespace, id.Name)
			if st, _ := statusOf(err); Classify(err) != ClassNotFound || st.Details == nil || st.Details.Name != id.Name {
				t.Errorf("%s of %s: %v; want NotFound, naming it", op, id, err)
			}
			if a := took(); len(a) != 0 {
				t.Errorf("%s of %s made the calls %q", op, id, a)
			}
		}
		// A real one is asked for.
		_ = call(secretGroupKind, "ns", "s")
		_ = call(thing.GroupKind(), "", "s")
		if a := slices.Compact(took()); !slices.Equal(a, []string{"/api/v1/namespaces/ns/secrets/s", "/apis/x.io/v1/things/s"}) {
			t.Errorf("%s made the calls %q", op, a)
		}
	}
	// Nothing is in a namespace that can't exist.
	for _, ns := range []string{"a/b", ".."} {
		if l, err := s.List(ctx, secretGroupKind, ns); err != nil || len(l) != 0 {
			t.Errorf("a list in %q: %v, %v; want none", ns, l, err)
		}
		if l, err := listLive(ctx, Clients{Metadata: md, Mapper: mapper}, secretGroupKind, ns, ""); err != nil || len(l) != 0 {
			t.Errorf("a live list in %q: %v, %v; want none", ns, l, err)
		}
		if l, err := listResource(ctx, Clients{Metadata: md}, secretGroupKind, schema.GroupVersionResource{Version: "v1", Resource: "secrets"}, ns, ""); err != nil || len(l) != 0 {
			t.Errorf("a live list of a resource in %q: %v, %v; want none", ns, l, err)
		}
		if l, err := ofSecrets.List(ctx, secretGroupKind, ns); err != nil || len(l) != 0 {
			t.Errorf("a list of a holder kind in %q: %v, %v; want none", ns, l, err)
		}
		if a := took(); len(a) != 0 {
			t.Errorf("a list in %q made the calls %q", ns, a)
		}
	}
	// Nor is anything of a cluster-scoped kind in a namespace.
	for _, srv := range []clientServer{s, ofThings} {
		if l, err := srv.List(ctx, thing.GroupKind(), "a"); err != nil || len(l) != 0 {
			t.Errorf("a list of a cluster-scoped kind in a namespace: %v, %v; want none", l, err)
		}
	}
	if a := took(); len(a) != 0 {
		t.Errorf("a list of a cluster-scoped kind in a namespace made the calls %q", a)
	}
}

// Nor do the caches have an object by such a name.
func TestNoCachedObjectOfANameNoObjectCanHave(t *testing.T) {
	informer := func(obj runtime.Object) *protectedInformer {
		inf := cache.NewSharedIndexInformer(&cache.ListWatch{}, obj, 0, cache.Indexers{})
		must(inf.GetStore().Add(obj))
		return &protectedInformer{inf: inf, content: true}
	}
	u := &unstructured.Unstructured{Object: map[string]any{}}
	u.SetNamespace("ns")
	u.SetName("s")
	m, c := informer(&metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "s"}}), informer(u)
	for id, want := range map[types.NamespacedName]bool{{Namespace: "ns", Name: "s"}: true, {Name: "ns/s"}: false} {
		if _, ok := m.get(id.Namespace, id.Name); ok != want {
			t.Errorf("%v: cached %v, want %v", id, ok, want)
		}
		if _, ok := c.getContent(id.Namespace, id.Name); ok != want {
			t.Errorf("the content of %v: cached %v, want %v", id, ok, want)
		}
	}
}

// Nor has it a per-name watch (§13 shape 1). Any other name's selects that
// name.
func TestNoWatchOfANameNoObjectCanHave(t *testing.T) {
	ctx, stop := context.WithCancel(context.Background())
	stop() // a watch that is started ends at once
	md := metadatafake.NewSimpleMetadataClient(metadatafake.NewTestScheme())
	n := newPerNames(&Tracker{s: newShared(Clients{Metadata: md}, SharedOptions{}, testingclock.NewFakeClock(time.Now())), gk: widgetGK})
	thing := schema.GroupKind{Group: "x.io", Kind: "Thing"}
	n.infos[secretGroupKind] = perNameKind{gvr: schema.GroupVersionResource{Version: "v1", Resource: "secrets"}, namespaced: true}
	n.infos[thing] = perNameKind{gvr: schema.GroupVersionResource{Group: "x.io", Version: "v1", Resource: "things"}}
	n.parent = ctx
	for id, want := range map[ID]bool{
		SecretRef("ns", "s").ID():                                 true,
		SecretRef("ns", "a/b").ID():                               false,
		SecretRef("..", "s").ID():                                 false,
		SecretRef("", "s").ID():                                   false,
		{Group: "x.io", Kind: "Thing", Name: "c"}:                 true,
		{Group: "x.io", Kind: "Thing", Name: "a/b"}:               false,
		{Group: "x.io", Kind: "Thing", Namespace: "a", Name: "c"}: false,
	} {
		w := n.newWatch(id)
		if (w != nil) != want {
			t.Errorf("%s: a watch %v, want %v", id, w != nil, want)
		}
		if w != nil {
			w.stop()
		}
	}
	w := n.newWatch(SecretRef("ns", "a,b=c").ID())
	defer w.stop()
	if got := w.p.lw.field; got != `metadata.name=a\,b\=c` {
		t.Errorf("the watch selects %q", got)
	}
}
