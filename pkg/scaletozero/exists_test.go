package scaletozero

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func runLabel(run string) map[string]string { return map[string]string{"scaletozero-test": run} }

func runSelector(run string) labels.Selector { return labels.SelectorFromSet(runLabel(run)) }

// Exists lists metadata-only, one item per page, across all namespaces, with
// the selector, and finds an instance that is only in another namespace than
// the ones around it.
func TestExistsListsMetadataOnlyAcrossNamespaces(t *testing.T) {
	requireAPIServer(t)
	a, b := newNamespace(t), newNamespace(t)
	// Non-matching instances first, so a page that came back short would
	// show up as more than one request.
	for i := range 20 {
		newInstance(t, widgetGVK, a, fmt.Sprintf("other%d", i), runLabel("wire-other"))
	}
	newInstance(t, widgetGVK, b, "match", runLabel("wire"))

	c, requests := recordingClient(t, "widgets")
	ok, err := Exists(context.Background(), c, widgetGVK, runSelector("wire"))
	if err != nil || !ok {
		t.Fatalf("Exists = (%v, %v), want (true, nil)", ok, err)
	}
	reqs := requests()
	if len(reqs) == 0 {
		t.Fatal("Exists made no LIST request")
	}
	// The API server's etcd store fills a page before it returns it, so on
	// envtest this is one request. More would be the empty pages with a
	// continue token that TestExistsFollowsEmptyPagesWithAContinueToken
	// produces with an interceptor.
	t.Logf("%d LIST request(s)", len(reqs))
	for i, r := range reqs {
		if strings.Contains(r.path, "/namespaces/") {
			t.Errorf("request %d listed one namespace (%s), want all namespaces", i, r.path)
		}
		if r.limit != "1" {
			t.Errorf("request %d had limit %q, want 1", i, r.limit)
		}
		if r.labelSelector != "scaletozero-test=wire" {
			t.Errorf("request %d had labelSelector %q, want scaletozero-test=wire", i, r.labelSelector)
		}
		if !strings.Contains(r.accept, "as=PartialObjectMetadataList") {
			t.Errorf("request %d had Accept %q, want a metadata-only list (as=PartialObjectMetadataList)", i, r.accept)
		}
	}
}

// No instance matching the selector is false, with no error.
func TestExistsIsFalseWhenNoneMatch(t *testing.T) {
	requireAPIServer(t)
	ns := newNamespace(t)
	newInstance(t, widgetGVK, ns, "w0", runLabel("none-other"))
	newInstance(t, widgetGVK, ns, "w1", nil)
	ok, err := Exists(context.Background(), k8s, widgetGVK, runSelector("none"))
	if err != nil || ok {
		t.Errorf("Exists = (%v, %v), want (false, nil)", ok, err)
	}
}

// emptyPagesClient answers the first `empty` LIST calls itself with an empty
// page that carries a continue token, as the API server may do under a label
// selector, then passes the next call on to the API server without the token.
// It fails call failOn (when > 0) with failure, and records the continue token
// of every call.
func emptyPagesClient(t *testing.T, empty, failOn int, failure error) (client.Client, *[]string) {
	t.Helper()
	tokens := &[]string{}
	c := interceptor.NewClient(k8sWithWatch(t), interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			o := &client.ListOptions{}
			o.ApplyOptions(opts)
			*tokens = append(*tokens, o.Continue)
			call := len(*tokens)
			if call == failOn {
				return failure
			}
			if call <= empty {
				list.SetContinue(fmt.Sprintf("empty-%d", call))
				return nil
			}
			o.Continue = ""
			return c.List(ctx, list, o)
		},
	})
	return c, tokens
}

// An empty page that carries a continue token is not the end: Exists follows
// the token until it sees an item or the token runs out.
func TestExistsFollowsEmptyPagesWithAContinueToken(t *testing.T) {
	requireAPIServer(t)
	ns := newNamespace(t)
	newInstance(t, widgetGVK, ns, "w", runLabel("empty-pages"))
	for name, tc := range map[string]struct {
		sel  labels.Selector
		want bool
	}{
		"an instance behind two empty pages": {runSelector("empty-pages"), true},
		"no instance behind two empty pages": {runSelector("empty-pages-none"), false},
	} {
		t.Run(name, func(t *testing.T) {
			c, tokens := emptyPagesClient(t, 2, 0, nil)
			ok, err := Exists(context.Background(), c, widgetGVK, tc.sel)
			if err != nil || ok != tc.want {
				t.Errorf("Exists = (%v, %v), want (%v, nil)", ok, err, tc.want)
			}
			if want := []string{"", "empty-1", "empty-2"}; !slices.Equal(*tokens, want) {
				t.Errorf("continue tokens sent = %q, want %q", *tokens, want)
			}
		})
	}
}

// A failure on any page is an error, never false.
func TestExistsListErrorIsAnError(t *testing.T) {
	requireAPIServer(t)
	ns := newNamespace(t)
	newInstance(t, widgetGVK, ns, "w", runLabel("list-error"))
	injected := errors.New("injected LIST failure")
	for _, failOn := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("page %d of 3", failOn), func(t *testing.T) {
			c, _ := emptyPagesClient(t, 2, failOn, injected)
			ok, err := Exists(context.Background(), c, widgetGVK, runSelector("list-error"))
			if !errors.Is(err, injected) {
				t.Fatalf("Exists = (%v, %v), want the injected error", ok, err)
			}
			if ok {
				t.Error("Exists returned true with its error")
			}
		})
	}
}

// A server that hands back the token it was given would loop forever: an
// error, never false.
func TestExistsRepeatedContinueTokenIsAnError(t *testing.T) {
	calls := 0
	r := listFunc(func(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
		calls++
		if calls > 10 {
			// Without the guard Exists would loop: end it here, so the
			// call count below fails rather than the test hangs.
			return errors.New("still looping")
		}
		list.SetContinue("same")
		return nil
	})
	ok, err := Exists(context.Background(), r, widgetGVK, labels.Everything())
	if err == nil || ok {
		t.Errorf("Exists = (%v, %v), want an error", ok, err)
	}
	if calls != 2 {
		t.Errorf("Exists listed %d times, want 2", calls)
	}
}

type listFunc func(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error

func (f listFunc) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New("unexpected Get")
}

func (f listFunc) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return f(ctx, list, opts...)
}

// Terminating instances count: their finalizers still need the controller.
func TestExistsIncludesTerminatingInstances(t *testing.T) {
	requireAPIServer(t)
	ns := newNamespace(t)
	const finalizer = "scaletozero.test.krateo.io/hold"
	dying := newInstance(t, widgetGVK, ns, "dying", runLabel("terminating"), finalizer)
	if err := k8s.Delete(context.Background(), dying); err != nil {
		t.Fatal(err)
	}
	got := &metav1.PartialObjectMetadata{}
	got.SetGroupVersionKind(widgetGVK)
	if err := k8s.Get(context.Background(), client.ObjectKeyFromObject(dying), got); err != nil || got.DeletionTimestamp == nil {
		t.Fatalf("the deleted instance should be terminating, held by its finalizer: %v %v", err, got.DeletionTimestamp)
	}

	ok, err := Exists(context.Background(), k8s, widgetGVK, runSelector("terminating"))
	if err != nil || !ok {
		t.Errorf("Exists = (%v, %v), want (true, nil): a terminating instance counts", ok, err)
	}
}

// labels.Everything() matches an unlabelled instance.
func TestExistsEverything(t *testing.T) {
	requireAPIServer(t)
	ns := newNamespace(t)
	newInstance(t, sprocketGVK, ns, "s0", nil)
	ok, err := Exists(context.Background(), k8s, sprocketGVK, labels.Everything())
	if err != nil || !ok {
		t.Errorf("Exists(Everything) = (%v, %v), want (true, nil)", ok, err)
	}
}

// The selector is required: a nil one is an error, not "everything" (nor
// controller-runtime's "nothing").
func TestExistsNilSelectorIsAnError(t *testing.T) {
	requireAPIServer(t)
	ns := newNamespace(t)
	newInstance(t, widgetGVK, ns, "w", nil)
	ok, err := Exists(context.Background(), k8s, widgetGVK, nil)
	if err == nil {
		t.Fatalf("Exists with a nil selector = (%v, nil), want an error", ok)
	}
	if !strings.Contains(err.Error(), "labels.Everything()") {
		t.Errorf("the error should tell the caller to pass labels.Everything(): %v", err)
	}
}

func TestExistsRejectsBadArguments(t *testing.T) {
	if _, err := Exists(context.Background(), nil, widgetGVK, labels.Everything()); err == nil {
		t.Error("Exists with a nil reader: want an error")
	}
	r := listFunc(func(context.Context, client.ObjectList, ...client.ListOption) error {
		t.Error("Exists listed despite arguments it should have rejected")
		return errors.New("unexpected List")
	})
	for _, g := range []schema.GroupVersionKind{{Group: testGroup, Kind: "Widget"}, {Group: testGroup, Version: "v1"}} {
		if _, err := Exists(context.Background(), r, g, labels.Everything()); err == nil {
			t.Errorf("Exists(%v): want an error", g)
		}
	}
	if _, err := Exists(context.Background(), r, widgetGVK, nil); err == nil {
		t.Error("Exists with a nil selector: want an error")
	}
}

// A missing CRD and an unserved version are errors, never false.
func TestExistsNoMatchIsAnError(t *testing.T) {
	requireAPIServer(t)
	for name, g := range map[string]schema.GroupVersionKind{
		"a missing CRD":       missingGVK,
		"an unserved version": unservedWidgetGVK,
		"a missing group":     {Group: "nowhere.test.krateo.io", Version: "v1", Kind: "Widget"},
	} {
		t.Run(name, func(t *testing.T) {
			ok, err := Exists(context.Background(), k8s, g, labels.Everything())
			if err == nil {
				t.Fatalf("Exists(%v) = (%v, nil), want an error", g, ok)
			}
			if !meta.IsNoMatchError(err) {
				t.Errorf("Exists(%v): want the RESTMapper's no-match error wrapped, got %v", g, err)
			}
		})
	}
}

// A CRD deleted after the client mapped it fails the LIST: an error, never
// false.
func TestExistsDeletedCRDIsAnError(t *testing.T) {
	requireAPIServer(t)
	ns := newNamespace(t)
	newInstance(t, gadgetGVK, ns, "g", nil)
	c, err := client.New(testCfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := Exists(context.Background(), c, gadgetGVK, labels.Everything()); err != nil || !ok {
		t.Fatalf("Exists before the delete = (%v, %v), want (true, nil)", ok, err)
	}

	crd := &apiextensionsv1.CustomResourceDefinition{}
	crd.Name = "gadgets." + testGroup
	if err := k8s.Delete(context.Background(), crd); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the Gadget CRD to be gone", func() bool {
		return apierrors.IsNotFound(k8s.Get(context.Background(), client.ObjectKeyFromObject(crd), &apiextensionsv1.CustomResourceDefinition{}))
	})

	ok, err := Exists(context.Background(), c, gadgetGVK, labels.Everything())
	if err == nil {
		t.Fatalf("Exists after the CRD was deleted = (%v, nil), want an error", ok)
	}
}

// A cache-backed reader can't paginate: it truncates at the limit and hands
// back a continue token it then refuses. Exists must never say false on it.
// It fails when no instance matches, and finds one that does.
func TestExistsWithACachedReaderNeverSaysFalse(t *testing.T) {
	requireAPIServer(t)
	ns := newNamespace(t)
	newInstance(t, widgetGVK, ns, "w", runLabel("cached"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ca, err := cache.New(testCfg, cache.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = ca.Start(ctx) }()
	if !ca.WaitForCacheSync(ctx) {
		t.Fatal("cache did not sync")
	}
	if ok, err := Exists(ctx, ca, widgetGVK, runSelector("cached-none")); err == nil {
		t.Errorf("Exists with a cached reader and no match = (%v, nil), want an error", ok)
	}
	if ok, err := Exists(ctx, ca, widgetGVK, runSelector("cached")); err != nil || !ok {
		t.Errorf("Exists with a cached reader and a match = (%v, %v), want (true, nil)", ok, err)
	}
}

func k8sWithWatch(t *testing.T) client.WithWatch {
	t.Helper()
	c, err := client.NewWithWatch(testCfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	return c
}
