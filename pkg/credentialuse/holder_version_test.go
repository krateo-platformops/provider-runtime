package credentialuse

import (
	"context"
	"net/http"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clienttesting "k8s.io/client-go/testing"
)

// The version the holder kind is read and written at (§6.2). The API server of
// these tests serves Widgets at v1, the mapper's answer for the kind alone, and
// at v1beta1, the one Options.Holder declares. The fake client keeps versions
// apart, so a call at the other version finds nothing.

var widgetDeclared = widgetGK.WithVersion("v1beta1")

func holderVersionRig(t *testing.T, objs ...runtime.Object) *fakeRig {
	t.Helper()
	r := newFakeRig(t, objs...)
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Version: "v1"}, {Group: widgetGK.Group, Version: "v1"}, widgetDeclared.GroupVersion()})
	for _, gvk := range []schema.GroupVersionKind{widgetGK.WithVersion("v1"), widgetDeclared, secretGVK} {
		mapper.Add(gvk, meta.RESTScopeNamespace)
	}
	mapper.Add(schema.GroupVersionKind{Version: "v1", Kind: "Namespace"}, meta.RESTScopeRoot)
	r.s.c.Mapper = mapper
	return r
}

func trackDeclared(t *testing.T, r *fakeRig) *Tracker {
	t.Helper()
	o := r.options(widgetGK)
	o.Holder = widgetDeclared
	tr, err := r.s.Track(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

// declaredHolder is an established Widget at the declared version, whose
// record names ids.
func declaredHolder(ns, name string, ids ...ID) *metav1.PartialObjectMetadata {
	m := holderObject(ns, name, ids...)
	m.APIVersion = widgetDeclared.GroupVersion().String()
	m.ResourceVersion = "1"
	return m
}

func (r *fakeRig) declaredWidget(t *testing.T, ns, name string) *metav1.PartialObjectMetadata {
	t.Helper()
	obj, err := r.md.Tracker().Get(widgetDeclared.GroupVersion().WithResource("widgets"), ns, name)
	if err != nil {
		t.Fatalf("reading widget %s/%s: %v", ns, name, err)
	}
	return obj.(*metav1.PartialObjectMetadata)
}

// onlyDeclared fails unless the calls on Widgets include verb, and all of them
// are at the declared version.
func (r *fakeRig) onlyDeclared(t *testing.T, verb string) {
	t.Helper()
	seen := false
	for _, a := range r.md.Actions() {
		if a.GetResource().Resource != "widgets" {
			continue
		}
		seen = seen || a.GetVerb() == verb
		if v := a.GetResource().Version; v != widgetDeclared.Version {
			t.Errorf("a %s of Widgets at %s, want %s", a.GetVerb(), v, widgetDeclared.Version)
		}
	}
	if !seen {
		t.Errorf("no %s of Widgets", verb)
	}
}

// A1 patches the holder at the version its own controller declares.
func TestA1UsesTheHoldersDeclaredVersion(t *testing.T) {
	r := holderVersionRig(t, namespaceObject("ns"), declaredHolder("ns", "w"), heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"))
	tr := trackDeclared(t, r)
	r.setRefs("w", SecretRef("ns", "s"))
	h := r.declaredWidget(t, "ns", "w").DeepCopy()
	r.md.ClearActions()
	if _, err := tr.Acquire(context.Background(), h); err != nil {
		t.Fatal(err)
	}
	r.onlyDeclared(t, "patch")
	if rec := ReadRecord(r.declaredWidget(t, "ns", "w")); !rec.Names(secretID("ns", "s")) {
		t.Errorf("record = %+v, want the Secret named", rec)
	}
}

// R2 lists the holders, and so does Users, at the declared version.
func TestR2ListsTheHoldersDeclaredVersion(t *testing.T) {
	ctx := context.Background()
	r := holderVersionRig(t, namespaceObject("ns"), declaredHolder("ns", "w", secretID("ns", "s")), heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"))
	tr := trackDeclared(t, r)
	r.md.ClearActions()
	if _, err := tr.proto.release(ctx, "ns", []ID{secretID("ns", "s")}); err != nil {
		t.Fatal(err)
	}
	r.onlyDeclared(t, "list")
	if !hasFinalizer(r.secretMeta(t, "ns", "s"), fWidgetNS) {
		t.Error("R2 missed the user, and the hold was released")
	}
	users, err := tr.Users(ctx, SecretRef("ns", "s"), "ns")
	if err != nil || len(users) != 1 {
		t.Errorf("Users = %v, %v; want the holder", users, err)
	}
	r.onlyDeclared(t, "list")
}

// A holder kind whose declared version isn't served any more is never "no
// users": R2's list fails, and nothing is released.
func TestR2WithTheDeclaredVersionUnserved(t *testing.T) {
	r := holderVersionRig(t, namespaceObject("ns"), declaredHolder("ns", "w", secretID("ns", "s")), heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns"))
	tr := trackDeclared(t, r)
	r.md.PrependReactor("list", "widgets", func(a clienttesting.Action) (bool, runtime.Object, error) {
		if a.GetResource().Version != widgetDeclared.Version {
			return false, nil, nil
		}
		// As client-go reports a plain-text 404.
		return true, nil, apierrors.NewGenericServerResponse(http.StatusNotFound, "get", schema.GroupResource{}, "", "404 page not found", 0, true)
	})
	if _, err := tr.proto.release(context.Background(), "ns", []ID{secretID("ns", "s")}); err == nil {
		t.Error("the batch didn't fail")
	}
	if !hasFinalizer(r.secretMeta(t, "ns", "s"), fWidgetNS) {
		t.Error("released without a list of the holders")
	}
}

// Disown lists the holders, and strips their records, at the declared version.
func TestDisownUsesTheHoldersDeclaredVersion(t *testing.T) {
	r := holderVersionRig(t, namespaceObject("ns"), declaredHolder("ns", "w", secretID("ns", "s")))
	tr := trackDeclared(t, r)
	r.md.ClearActions()
	r.s.disownPass(context.Background(), tr, parseDisown("all"))
	r.onlyDeclared(t, "list")
	r.onlyDeclared(t, "patch")
	if rec := ReadRecord(r.declaredWidget(t, "ns", "w")); rec.Present {
		t.Errorf("record = %+v, want it stripped", rec)
	}
}

// The holder informer reads the declared version too.
func TestHolderInformerUsesTheDeclaredVersion(t *testing.T) {
	r := holderVersionRig(t, namespaceObject("ns"), declaredHolder("ns", "w"))
	tr := trackDeclared(t, r)
	r.run(t)
	r.onlyDeclared(t, "list")
	eventually(t, "the holder to be cached", func() bool { return len(tr.holders.GetStore().List()) == 1 })
}

// A Holder without a version declares the one Track maps its kind at.
func TestAnEmptyHolderVersionIsTheOneTrackMaps(t *testing.T) {
	r := holderVersionRig(t)
	o := r.options(widgetGK)
	o.Holder = widgetGK.WithVersion("")
	tr, err := r.s.Track(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if want := widgetGK.WithVersion("v1"); tr.o.Holder != want {
		t.Errorf("the tracker declares %v, want %v", tr.o.Holder, want)
	}
	if again, err := r.s.Track(context.Background(), r.options(widgetGK)); err != nil || again != tr {
		t.Errorf("Track with that version written out = %p, %v; want the same tracker", again, err)
	}
}

// One Shared tracks a holder kind at one version: a second Track at another is
// refused, as with any other Options. Processes may differ (§6.8).
func TestOneSharedTracksAHolderKindAtOneVersion(t *testing.T) {
	r := holderVersionRig(t)
	trackDeclared(t, r)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := r.s.Track(ctx, r.options(widgetGK)); err == nil {
		t.Error("a second version of the holder kind was tracked")
	}
}
