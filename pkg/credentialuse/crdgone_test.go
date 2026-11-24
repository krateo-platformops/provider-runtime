package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clienttesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// A holder kind whose CRD is gone (§6.4, "The holder kind itself gone").

// R2 without a list of holders asks noHolders, once, after every R1 and the
// list; its answer is an empty list, or the batch's error, with every hold
// kept. Without it, as in the model, the list's failure is the error.
func TestReleaseWithoutAHolderKind(t *testing.T) {
	ctx := context.Background()
	for name, c := range map[string]struct {
		deleting bool // P is being deleted, inside its grace
		answer   func(listErr error) error
		released bool
		want     string
	}{
		"no one to ask":                     {answer: nil, want: "could not find the requested resource"},
		"no holders":                        {answer: func(error) error { return nil }, released: true},
		"no holders, P being deleted":       {answer: func(error) error { return nil }, deleting: true, released: true},
		"the list's error":                  {answer: func(err error) error { return err }, want: "could not find the requested resource"},
		"the list's error, P being deleted": {answer: func(err error) error { return err }, deleting: true, want: "could not find the requested resource"},
		"the CRD can't be read":             {answer: func(err error) error { return fmt.Errorf("%w (and the CRD: no)", err) }, want: "and the CRD: no"},
	} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			heldSecret(r)
			if c.deleting {
				must(r.m.Delete(secretKey("ns", "s")))
			}
			r.srv.before = func(op, kind, _ string) error {
				if op == "list" && kind == widgetGK.Kind {
					return errTest404
				}
				return nil
			}
			if c.answer != nil {
				r.p.noHolders = func(_ context.Context, listErr error) error {
					r.srv.calls = append(r.srv.calls, "noHolders")
					return c.answer(listErr)
				}
			}
			res, err := r.p.release(ctx, "ns", []ID{SecretRef("ns", "s").ID()})
			if c.want == "" && err != nil || c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)) {
				t.Errorf("release = %v, want %q", err, c.want)
			}
			if c.answer != nil {
				i := slices.Index(r.srv.calls, "noHolders")
				if i < 0 || slices.Contains(r.srv.calls[i+1:], "noHolders") || r.srv.calls[i-1] != "list Widget/" ||
					slices.ContainsFunc(r.srv.calls[i+1:], func(s string) bool { return !strings.HasPrefix(s, "patch ") }) {
					t.Errorf("calls %q: want one noHolders, right after the list, and only R3 after it", r.srv.calls)
				}
			}
			if got := len(res.Released) == 1; got != c.released {
				t.Errorf("released %v, want %v", res.Released, c.released)
			}
			o, err := r.m.Get(secretKey("ns", "s"))
			if held := err == nil && hasFinalizer(o, fNS); held == c.released {
				t.Errorf("held %v after the batch", held)
			}
		})
	}
}

// noHolders is asked only when the list failed, and a release on a list isn't
// one for want of the kind.
func TestReleaseListsWhatThereIs(t *testing.T) {
	for _, user := range []bool{true, false} {
		r := newRig(t)
		heldSecret(r)
		if user {
			userOf(r, "h", true)
		}
		r.p.noHolders = func(context.Context, error) error {
			t.Error("asked, though the list succeeded")
			return nil
		}
		r.p.releasedGone = func(id ID) { t.Errorf("the release of %s is told as one for want of the kind", id) }
		if res, err := r.p.release(context.Background(), "ns", []ID{SecretRef("ns", "s").ID()}); err != nil || (len(res.Released) == 0) != user {
			t.Errorf("user %v: release = %v, %v", user, res.Released, err)
		}
	}
}

// What is released for want of the kind is logged after R3, not when R3 is
// refused, and once for each time the CRD goes, whatever the objects.
func TestAReleaseForWantOfTheKindIsLoggedOnce(t *testing.T) {
	const logged = "the holder kind's CRD is gone"
	r := newFakeRig(t, namespaceObject("ns"), heldObject(secretGVK, "ns", "a", "u-a", widgetGK, "ns"), heldObject(secretGVK, "ns", "b", "u-b", widgetGK, "ns"))
	out, sink := r.breakable("widgets"), &sinkLines{}
	r.trackLogged(t, sink)
	r.run(t)
	out.fail(errTest404)
	r.removeCRD(t, widgetGK)
	var refuse atomic.Bool
	refuse.Store(true)
	r.md.PrependReactor("patch", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) {
		if refuse.Load() {
			return true, nil, errTest403
		}
		return false, nil, nil
	})
	r.runWrites(t)
	eventually(t, "a refused R3", func() bool { return sink.warned("a release batch failed") >= 1 })
	if n := sink.informed(logged); n != 0 {
		t.Errorf("a refused R3 was logged as a release %d times", n)
	}
	refuse.Store(false)
	releasedStepping(t, r, "both holds to be released", func() bool {
		return !r.heldBy(t, widgetGK, "ns", "a") && !r.heldBy(t, widgetGK, "ns", "b")
	})
	eventually(t, "the log", func() bool { return sink.informed(logged) >= 1 })
	r.idle(t, batchKey{widgetGK, "ns"})
	if n := sink.informed(logged); n != 1 {
		t.Errorf("logged %d times, want once: %q", n, sink.infos)
	}
	// It returns, and goes again.
	r.restoreCRD(t, widgetGK)
	tr := r.s.trackers[widgetGK]
	eventually(t, "the CRD informer to see it back", func() bool { return !tr.crdAbsent() })
	eventually(t, "the log to be due again", func() bool { return !tr.goneLogged.Load() })
	must(r.md.Tracker().Create(secretsGVR, heldObject(secretGVK, "ns", "c", "u-c", widgetGK, "ns"), "ns"))
	r.removeCRD(t, widgetGK)
	releasedStepping(t, r, "the later hold to be released", func() bool { return !r.heldBy(t, widgetGK, "ns", "c") })
	eventually(t, "the second log", func() bool { return sink.informed(logged) == 2 })
}

// crdGets are the GETs of CRDs made through the metadata client, by name.
func (r *fakeRig) crdGets() []string {
	var out []string
	for _, a := range r.md.Actions() {
		if g, ok := a.(clienttesting.GetAction); ok && a.GetResource() == crdGVR && a.GetVerb() == "get" {
			out = append(out, g.GetName())
		}
	}
	return out
}

// removeCRD takes gk's CRD out of the fake API server, and its resource out of
// the discovery: the informer sees it go.
func (r *fakeRig) removeCRD(t *testing.T, gk schema.GroupKind) {
	t.Helper()
	r.disc.serve("v1", strings.ToLower(gk.Kind)+"s", "") // first: its deletion queues batches
	if err := r.md.Tracker().Delete(crdGVR, "", crdOf(gk).GetName()); err != nil {
		t.Fatal(err)
	}
}

// restoreCRD brings gk's CRD back.
func (r *fakeRig) restoreCRD(t *testing.T, gk schema.GroupKind) {
	t.Helper()
	if err := r.md.Tracker().Create(crdGVR, crdMeta(gk), ""); err != nil {
		t.Fatal(err)
	}
	r.disc.serve("v1", strings.ToLower(gk.Kind)+"s", gk.Kind)
}

// failCRDGet makes the GET of every CRD fail with err, or work when it
// returns nil.
func (r *fakeRig) failCRDGet(f func() error) {
	r.md.PrependReactor("get", "customresourcedefinitions", func(clienttesting.Action) (bool, runtime.Object, error) {
		if err := f(); err != nil {
			return true, nil, err
		}
		return false, nil, nil
	})
}

// movedMapper maps gk as to says, and the rest as the mapper it wraps.
type movedMapper struct {
	meta.RESTMapper
	gk schema.GroupKind
	to func() (*meta.RESTMapping, error)
}

func (m movedMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	if gk == m.gk {
		return m.to()
	}
	return m.RESTMapper.RESTMapping(gk, versions...)
}

// What may mean that the kind isn't served, and so is worth a GET of its CRD;
// and what the GET must answer.
func TestNoHolders(t *testing.T) {
	ctx := context.Background()
	gvr := schema.GroupVersionResource{Group: widgetGK.Group, Version: "v1", Resource: "widgets"}
	noMatch := &meta.NoKindMatchError{GroupKind: widgetGK, SearchedVersions: []string{"v1"}}
	crd := "widgets." + widgetGK.Group
	crds := crdGVR.GroupResource()

	lists := map[string]struct {
		err  error
		asks bool
	}{
		"a 404 in plain text, as for a CRD that is gone":    {errTest404, true},
		"a 404 Status that names nothing":                   {apierrors.NewGenericServerResponse(404, "list", gvr.GroupResource(), "", "the server could not find the requested resource", 0, false), true},
		"wrapped":                                           {fmt.Errorf("page 2: %w", errTest404), true},
		"the mapper knows no such kind":                     {fmt.Errorf("credential-use: mapping: %w", noMatch), true},
		"the mapper knows no such resource":                 {&meta.NoResourceMatchError{PartialResource: gvr}, true},
		"controller-runtime's mapper finds no such version": {&apiutil.ErrResourceDiscoveryFailed{gvr.GroupVersion(): errTest404}, true},
		"403":                               {apierrors.NewForbidden(gvr.GroupResource(), "", errors.New("no")), false},
		"410":                               {apierrors.NewResourceExpired("test"), false},
		"500":                               {apierrors.NewInternalError(errors.New("test")), false},
		"503, as an API server that starts": {apierrors.NewServiceUnavailable("test"), false},
		"unreachable":                       {&url.Error{Op: "Get", URL: "https://x", Err: errors.New("connection refused")}, false},
		"controller-runtime's mapper can't read the discovery": {&apiutil.ErrResourceDiscoveryFailed{gvr.GroupVersion(): apierrors.NewServiceUnavailable("test")}, false},
		"the context ended": {context.Canceled, false},
	}
	for name, c := range lists {
		t.Run("the list: "+name, func(t *testing.T) {
			r := newFakeRig(t)
			tr := r.track(t, widgetGK)
			r.removeCRD(t, widgetGK)
			r.md.ClearActions()
			err := tr.noHolders(ctx, c.err)
			if gets := r.crdGets(); c.asks && !slices.Equal(gets, []string{crd}) || !c.asks && len(gets) != 0 {
				t.Errorf("the CRDs read: %v", gets)
			}
			if c.asks && err != nil || !c.asks && !errors.Is(err, c.err) {
				t.Errorf("noHolders = %v", err)
			}
		})
	}

	gone := apierrors.NewNotFound(crds, crd)
	page := apierrors.NewGenericServerResponse(http.StatusNotFound, "get", crds, crd, "404 page not found", 0, true)
	nameless := apierrors.NewNotFound(crds, crd)
	nameless.ErrStatus.Details = nil
	gets := map[string]struct {
		err  error
		gone bool
	}{
		"it doesn't exist":                  {gone, true},
		"it exists":                         {nil, false},
		"403":                               {apierrors.NewForbidden(crds, crd, errors.New("no")), false},
		"500":                               {apierrors.NewInternalError(errors.New("test")), false},
		"503":                               {apierrors.NewServiceUnavailable("test"), false},
		"unreachable":                       {errors.New("connection refused"), false},
		"a 404 that isn't the API server's": {page, false},
		"a 404 that names nothing":          {nameless, false},
		"a 404 that names another CRD":      {apierrors.NewNotFound(crds, "gadgets."+widgetGK.Group), false},
		"a 404 of another resource":         {apierrors.NewNotFound(gvr.GroupResource(), crd), false},
		"a 404 a webhook made, with no name in it": {apierrors.NewGenericServerResponse(404, "get", crds, "", "no", 0, false), false},
	}
	for name, c := range gets {
		t.Run("the CRD: "+name, func(t *testing.T) {
			r := newFakeRig(t)
			tr := r.track(t, widgetGK)
			if c.err != nil {
				r.removeCRD(t, widgetGK)
			}
			r.disc.serve("v1", "widgets", "") // only the GET tells: discovery may lag
			r.failCRDGet(func() error { return c.err })
			r.md.ClearActions()
			err := tr.noHolders(ctx, errTest404)
			if (err == nil) != c.gone {
				t.Errorf("noHolders = %v, want gone %v", err, c.gone)
			}
			if err != nil && !errors.Is(err, errTest404) {
				t.Errorf("noHolders = %v: it lost the list's error", err)
			}
			if err != nil && c.err != nil && !strings.Contains(err.Error(), c.err.Error()) {
				t.Errorf("noHolders = %v: it lost the GET's error", err)
			}
			if got := err != nil && strings.Contains(err.Error(), "grant get on the CRD "+crd); got != (name == "403") {
				t.Errorf("noHolders = %v: a grant is the advice for a 403, and only for it", err)
			}
			if gets := r.crdGets(); len(gets) != 1 {
				t.Errorf("%d GETs of the CRD, want one", len(gets))
			}
		})
	}

	// The CRD that is gone must still be the kind's.
	mappings := map[string]struct {
		to   func() (*meta.RESTMapping, error)
		gone bool
		want string
	}{
		"no such kind":   {func() (*meta.RESTMapping, error) { return nil, noMatch }, true, ""},
		"another plural": {func() (*meta.RESTMapping, error) { return mappingOf("widgetries"), nil }, false, "restart"},
		"it can't tell":  {func() (*meta.RESTMapping, error) { return nil, errors.New("discovery failed") }, false, ""},
		"as tracked":     {func() (*meta.RESTMapping, error) { return mappingOf("widgets"), nil }, true, ""},
	}
	for name, c := range mappings {
		t.Run("the mapper: "+name, func(t *testing.T) {
			r := newFakeRig(t)
			tr := r.track(t, widgetGK)
			r.removeCRD(t, widgetGK)
			r.md.ClearActions()
			r.s.c.Mapper = movedMapper{r.s.c.Mapper, widgetGK, c.to}
			err := tr.noHolders(ctx, errTest404)
			if (err == nil) != c.gone || c.want != "" && !strings.Contains(err.Error(), c.want) {
				t.Errorf("noHolders = %v, want gone %v %q", err, c.gone, c.want)
			}
			if n := len(r.crdGets()); !c.gone && n != 0 {
				t.Errorf("%d GETs of a CRD that says nothing of the kind", n)
			}
		})
	}
}

func mappingOf(plural string) *meta.RESTMapping {
	return &meta.RESTMapping{Resource: schema.GroupVersionResource{Group: widgetGK.Group, Version: "v1", Resource: plural},
		GroupVersionKind: widgetGK.WithVersion("v1"), Scope: meta.RESTScopeNamespace}
}

// holderless is a rig with one Secret held for Widgets, in namespace ns, whose
// holder is gone; its Widgets can be made to fail.
func holderless(t *testing.T, o SharedOptions, ns string) (*fakeRig, *outage, *sinkLines) {
	t.Helper()
	r := newFakeRigWith(t, o, namespaceObject("ns"), namespaceObject("out"), heldObject(secretGVK, ns, "s", "u1", widgetGK, ns))
	return r, r.breakable("widgets"), &sinkLines{}
}

// A hold left by a holder that went with its kind, while this process was a
// standby, is released once its write loops run: as soon as the CRD is gone,
// and not while it exists or can't be read, or the list fails otherwise.
func TestAHoldOfAKindThatIsGoneIsReleased(t *testing.T) {
	crds := crdGVR.GroupResource()
	// stays runs the write loops until two batches failed, and checks the
	// hold is there.
	stays := func(t *testing.T, r *fakeRig, sink *sinkLines) {
		t.Helper()
		r.runWrites(t)
		for n := 1; n <= 2; n++ {
			eventually(t, "a batch to fail", func() bool { return sink.warned("a release batch failed") >= n })
			eventually(t, "its retry's timer", func() bool { return r.queueTimers() >= 1 })
			r.clk.Step(releaseBackoffMax)
		}
		if !r.heldBy(t, widgetGK, "ns", "s") {
			t.Fatal("released")
		}
	}
	released := func(t *testing.T, r *fakeRig) {
		t.Helper()
		releasedStepping(t, r, "the hold to be released", func() bool { return !r.heldBy(t, widgetGK, "ns", "s") })
	}
	t.Run("the CRD is gone", func(t *testing.T) {
		r, o, sink := holderless(t, SharedOptions{}, "ns")
		r.trackLogged(t, sink)
		r.run(t)
		o.fail(errTest404)
		r.removeCRD(t, widgetGK)
		r.runWrites(t)
		released(t, r)
		eventually(t, "it to say why it released", func() bool { return sink.informed("the holder kind's CRD is gone") == 1 })
	})
	t.Run("the CRD exists, then goes", func(t *testing.T) {
		r, o, sink := holderless(t, SharedOptions{}, "ns")
		r.trackLogged(t, sink)
		r.run(t)
		o.fail(errTest404)
		stays(t, r, sink)
		r.removeCRD(t, widgetGK)
		released(t, r)
	})
	t.Run("the CRD can't be read, then can", func(t *testing.T) {
		r, o, sink := holderless(t, SharedOptions{}, "ns")
		r.trackLogged(t, sink)
		r.run(t)
		o.fail(errTest404)
		r.removeCRD(t, widgetGK)
		var granted atomic.Bool
		r.failCRDGet(func() error {
			if granted.Load() {
				return nil
			}
			return apierrors.NewForbidden(crds, "widgets."+widgetGK.Group, errors.New("no"))
		})
		stays(t, r, sink)
		if sink.warned("grant get on the CRD") == 0 {
			t.Errorf("the failure doesn't say what to grant: %v", sink.warns)
		}
		granted.Store(true)
		released(t, r)
	})
	t.Run("the mapper no longer knows the kind", func(t *testing.T) {
		r, o, sink := holderless(t, SharedOptions{}, "ns")
		r.trackLogged(t, sink)
		r.run(t)
		o.fail(errTest404)
		r.removeCRD(t, widgetGK)
		r.s.c.Mapper = movedMapper{r.s.c.Mapper, widgetGK, func() (*meta.RESTMapping, error) {
			return nil, &meta.NoKindMatchError{GroupKind: widgetGK}
		}}
		// The tracker's server has the mapper it was built with.
		tr := r.s.trackers[widgetGK]
		tr.proto.srv = r.s.newServer(tr.o.Holder)
		r.runWrites(t)
		released(t, r)
	})
	t.Run("the list is refused, with the CRD gone", func(t *testing.T) {
		r, o, sink := holderless(t, SharedOptions{}, "ns")
		r.trackLogged(t, sink)
		r.run(t)
		o.fail(errTest403)
		r.removeCRD(t, widgetGK)
		stays(t, r, sink)
		o.fail(errTest404)
		released(t, r)
	})
	t.Run("the CRD came back, and a holder of it", func(t *testing.T) {
		r, o, sink := holderless(t, SharedOptions{}, "ns")
		r.trackLogged(t, sink)
		r.run(t)
		o.fail(errTest404)
		r.removeCRD(t, widgetGK)
		r.restoreCRD(t, widgetGK)
		stays(t, r, sink)
		must(r.md.Tracker().Create(widgetsGVR, holderObject("ns", "h2", secretID("ns", "s")), "ns"))
		o.heal()
		r.s.releaseBatch(context.Background(), batchKey{widgetGK, "ns"}, []ID{secretID("ns", "s")})
		if !r.heldBy(t, widgetGK, "ns", "s") {
			t.Error("the new holder's hold was released")
		}
	})
	t.Run("a hold outside the caches", func(t *testing.T) {
		r, o, sink := holderless(t, SharedOptions{Namespaces: []string{"ns"}}, "out")
		r.trackLogged(t, sink)
		r.run(t)
		o.fail(errTest404)
		r.removeCRD(t, widgetGK)
		r.runWrites(t)
		releasedStepping(t, r, "the hold to be released", func() bool { return !r.heldBy(t, widgetGK, "out", "s") })
	})
	t.Run("another kind's hold on the same Secret", func(t *testing.T) {
		r, o, sink := holderless(t, SharedOptions{}, "ns")
		s := r.secretMeta(t, "ns", "s").DeepCopy()
		fg := mustFinalizer(gadgetGK, "ns")
		s.Finalizers = append(s.Finalizers, fg)
		s.Annotations[fg] = "e1"
		must(r.md.Tracker().Update(secretsGVR, s, "ns"))
		g := holderObject("ns", "g", secretID("ns", "s"))
		g.Kind = gadgetGK.Kind
		must(r.md.Tracker().Create(schema.GroupVersionResource{Group: gadgetGK.Group, Version: "v1", Resource: "gadgets"}, g, "ns"))
		r.trackLogged(t, sink)
		r.track(t, gadgetGK)
		r.run(t)
		o.fail(errTest404)
		r.removeCRD(t, widgetGK)
		r.runWrites(t)
		released(t, r)
		if !r.heldBy(t, gadgetGK, "ns", "s") {
			t.Error("the kind that stays lost its hold")
		}
	})
}
