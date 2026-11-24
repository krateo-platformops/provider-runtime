package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	metadatafake "k8s.io/client-go/metadata/fake"
	clienttesting "k8s.io/client-go/testing"
)

// A 404 of a patch isn't the API server's word that the object doesn't exist:
// admission may answer one, for another object, or for this very one (§6.2).
// Only a read's says it is gone.

func TestPatchConfirmsA404WithARead(t *testing.T) {
	secrets := schema.GroupResource{Resource: "secrets"}
	gone := apierrors.NewNotFound(secrets, "s")
	elsewhere := apierrors.NewNotFound(schema.GroupResource{Resource: "namespaces"}, "ns")
	nameless := apierrors.NewGenericServerResponse(http.StatusNotFound, "GET", schema.GroupResource{}, "", "", 0, false)
	for name, c := range map[string]struct {
		patch error
		there bool  // the read finds the object
		read  error // or fails so
		want  Class
		calls int
	}{
		"gone":                               {patch: gone, read: gone, want: ClassNotFound, calls: 2},
		"another's, gone":                    {patch: elsewhere, read: gone, want: ClassNotFound, calls: 2},
		"there":                              {patch: gone, there: true, want: ClassOther, calls: 2},
		"another's, there":                   {patch: elsewhere, there: true, want: ClassOther, calls: 2},
		"its kind no longer served":          {patch: gone, read: unservedError(), want: ClassOther, calls: 2},
		"a read that names nothing":          {patch: gone, read: nameless, want: ClassOther, calls: 2},
		"a read that names another":          {patch: gone, read: elsewhere, want: ClassOther, calls: 2},
		"a read the authorizer refuses":      {patch: gone, read: rbacForbidden("ns", "s"), want: ClassOther, calls: 2},
		"a read that fails":                  {patch: gone, read: apierrors.NewServiceUnavailable("down"), want: ClassOther, calls: 2},
		"a read that times out":              {patch: gone, read: context.DeadlineExceeded, want: ClassOther, calls: 2},
		"a patch to a kind not served":       {patch: unservedError(), there: true, want: ClassOther, calls: 1},
		"a conflict":                         {patch: apierrors.NewConflict(secrets, "s", errors.New("modified")), there: true, want: ClassConflict, calls: 1},
		"a patch that lands":                 {there: true, want: ClassNone, calls: 1},
		"a patch the authorizer refuses":     {patch: rbacForbidden("ns", "s"), there: true, want: ClassRBAC, calls: 1},
		"a patch that fails":                 {patch: apierrors.NewInternalError(errors.New("boom")), there: true, want: ClassOther, calls: 1},
		"a webhook's 404 that names nothing": {patch: nameless, there: true, want: ClassOther, calls: 1},
	} {
		mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Version: "v1"}})
		mapper.Add(secretGVK, meta.RESTScopeNamespace)
		md := metadatafake.NewSimpleMetadataClient(metadatafake.NewTestScheme())
		obj := heldObject(secretGVK, "ns", "s", "u", widgetGK)
		answer := func(err error) (bool, runtime.Object, error) {
			if err != nil {
				return true, nil, err
			}
			return true, obj.DeepCopy(), nil
		}
		md.PrependReactor("patch", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) { return answer(c.patch) })
		md.PrependReactor("get", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) {
			if !c.there && c.read == nil {
				t.Errorf("%s: the test's read has no answer", name)
			}
			return answer(c.read)
		})
		out, err := clientServer{md: md, mapper: mapper}.Patch(context.Background(), secretGroupKind, "ns", "s", types.JSONPatchType, []byte(`[]`))
		if got, j := Classify(err), ClassifyJSONPatch(err); got != c.want || j != c.want {
			t.Errorf("%s: Classify %v, ClassifyJSONPatch %v, want %v (%v)", name, got, j, c.want, err)
		}
		if c.want != ClassNotFound && c.calls == 2 && apierrors.IsNotFound(err) {
			t.Errorf("%s: %v is a NotFound", name, err)
		}
		if errors.Is(c.read, context.DeadlineExceeded) && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("%s: %v lost its cause", name, err)
		}
		if (out != nil) != (c.want == ClassNone) {
			t.Errorf("%s: the patch returned %v", name, out)
		}
		if n := len(md.Actions()); n != c.calls {
			t.Errorf("%s: %d calls, want %d: %v", name, n, c.calls, md.Actions())
		}
		if c.want == ClassOther && c.calls == 2 && !strings.Contains(err.Error(), c.patch.Error()) {
			t.Errorf("%s: %q doesn't say what the patch was answered", name, err)
		}
	}
}

// Every caller of a patch, when its 404 isn't about a missing object: a hold
// isn't Missing, and nothing held is taken for gone.
func TestA404OfAPatchOfAnObjectThatIsThere(t *testing.T) {
	ctx := context.Background()
	for name, refusal := range map[string]error{
		"naming its namespace": apierrors.NewNotFound(schema.GroupResource{Resource: "namespaces"}, "ns"),
		"naming it":            apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, "s"),
	} {
		t.Run(name, func(t *testing.T) {
			rig := func(t *testing.T, objs ...runtime.Object) (*fakeRig, *Tracker, *warnings) {
				r := newFakeRig(t, append(objs, namespaceObject("ns"))...)
				w := &warnings{}
				o := r.options(widgetGK)
				o.Logger = w
				tr, err := r.s.Track(ctx, o)
				if err != nil {
					t.Fatal(err)
				}
				r.run(t)
				r.md.PrependReactor("patch", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) { return true, nil, refusal })
				return r, tr, w
			}
			id := secretID("ns", "s")
			t.Run("A2", func(t *testing.T) {
				for _, established := range []bool{false, true} {
					h := holderObject("ns", "h")
					if !established {
						h.Finalizers = nil
					}
					r, tr, _ := rig(t, heldObject(secretGVK, "ns", "s", "u", widgetGK), h)
					r.setRefs("h", SecretRef("ns", "s"))
					a, err := tr.Acquire(ctx, h.DeepCopy())
					if err != nil || a.Err == nil || a.MustNotConnect == established || a.Degraded != established {
						t.Errorf("established %v: %+v, %v; want a failed pass", established, a, err)
					}
					for _, u := range a.Unheld {
						if u.Reason == ReasonMissing {
							t.Errorf("established %v: the Secret is Missing", established)
						}
					}
				}
			})
			t.Run("R3", func(t *testing.T) {
				_, tr, _ := rig(t, heldObject(secretGVK, "ns", "s", "u", widgetGK, "ns"))
				if res, err := tr.proto.release(ctx, "ns", []ID{id}); err == nil || len(res.Bare)+len(res.Released) != 0 {
					t.Errorf("%+v, %v; want an error, and nothing bare", res, err)
				}
			})
			t.Run("the epoch's repair", func(t *testing.T) {
				o := heldObject(secretGVK, "ns", "s", "u", widgetGK, "ns")
				delete(o.Annotations, fWidgetNS)
				_, tr, _ := rig(t, o)
				if res, err := tr.proto.release(ctx, "ns", []ID{id}); err == nil {
					t.Errorf("%+v, no error: the repair was refused", res)
				}
			})
			t.Run("the janitor", func(t *testing.T) {
				o := heldObject(secretGVK, "ns", "s", "u", widgetGK)
				o.Labels[MarkerLabel(widgetGK)] = "true"
				r, _, _ := rig(t, o)
				if err := r.s.clean(ctx, id); err == nil {
					t.Error("the janitor took the refusal for an object that is gone")
				}
			})
			t.Run("Disown's strip", func(t *testing.T) {
				o := heldObject(secretGVK, "ns", "s", "u", widgetGK, "ns")
				o.Labels[MarkerLabel(widgetGK)] = "true"
				r, tr, w := rig(t, o)
				r.s.disownPass(ctx, tr, parseDisown("all"))
				w.mu.Lock()
				defer w.mu.Unlock()
				if !strings.Contains(fmt.Sprint(w.got), "couldn't strip a protected object") {
					t.Errorf("warnings %q; want the failed strip", w.got)
				}
			})
			t.Run("DisownPrefix", func(t *testing.T) {
				goneGK := schema.GroupKind{Group: "gone.example.com", Kind: "Gone"}
				r, _, _ := rig(t, heldObject(secretGVK, "ns", "s", "u", goneGK, "ns"))
				rep, err := DisownPrefix(ctx, r.s.c, DisownPrefixOptions{Holder: goneGK, Names: []Ref{SecretRef("ns", "s")}})
				if err == nil || len(rep.Stripped) != 0 || len(rep.Objects) != 1 {
					t.Errorf("%+v, %v; want the object found, not stripped, and an error", rep, err)
				}
			})
		})
	}
}

// Disown's record strip of a holder whose patch is refused so: the next pass
// retries, and says so. A holder that is gone needs none.
func TestDisownSaysARecordItCouldntStrip(t *testing.T) {
	ctx := context.Background()
	for _, there := range []bool{true, false} {
		h := holderObject("ns", "h", secretID("ns", "s"))
		r := newFakeRig(t, namespaceObject("ns"), h)
		w := &warnings{}
		o := r.options(widgetGK)
		o.Logger = w
		tr, err := r.s.Track(ctx, o)
		if err != nil {
			t.Fatal(err)
		}
		refusal := apierrors.NewNotFound(schema.GroupResource{Group: widgetGK.Group, Resource: "widgets"}, "h")
		r.md.PrependReactor("patch", "widgets", func(clienttesting.Action) (bool, runtime.Object, error) { return true, nil, refusal })
		if !there {
			r.md.PrependReactor("get", "widgets", func(clienttesting.Action) (bool, runtime.Object, error) { return true, nil, refusal })
		}
		r.s.disownPass(ctx, tr, parseDisown("all"))
		w.mu.Lock()
		if got := strings.Contains(fmt.Sprint(w.got), "couldn't strip a holder's record"); got != there {
			t.Errorf("the holder there %v: warnings %q", there, w.got)
		}
		w.mu.Unlock()
	}
}

// What Disown can't read is said with a remedy for its class; the error's
// text, which may vary at every pass, is in the log alone.
func TestDisownsRemedies(t *testing.T) {
	ctx := context.Background()
	text := "attempt 7f3a"
	down := apierrors.NewServiceUnavailable(text)
	forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "", errors.New(text))
	for name, c := range map[string]struct {
		err  error
		want []string
		not  []string
	}{
		"not served": {unservedError(), []string{"its kind isn't served", "restore it", "once its objects are gone", "if another version"}, []string{"grant", "the next pass"}},
		"forbidden":  {forbidden, []string{"(forbidden)", "grant list"}, []string{"protected kinds", "the next pass"}},
		"down":       {down, []string{"(serviceunavailable)", "the next pass retries"}, []string{"grant", "protected kinds"}},
		"gone":       {apierrors.NewResourceExpired(text), []string{"(expired)", "the next pass retries"}, []string{"grant", "protected kinds"}},
		"timeout":    {context.DeadlineExceeded, []string{"(timeout)", "the next pass retries"}, []string{"grant", "protected kinds"}},
		"other":      {errors.New(text), []string{"(error)", "the next pass retries"}, []string{"grant", "protected kinds"}},
	} {
		for _, resource := range []string{"secrets", "widgets"} {
			r := newFakeRig(t)
			w := &warnings{}
			o := r.options(widgetGK)
			o.Logger = w
			tr, err := r.s.Track(ctx, o)
			if err != nil {
				t.Fatal(err)
			}
			r.md.PrependReactor("list", resource, func(clienttesting.Action) (bool, runtime.Object, error) { return true, nil, c.err })
			r.s.disownPass(ctx, tr, parseDisown("all"))
			var e string
			select {
			case e = <-r.events.Events:
			default:
				t.Errorf("%s, %s: no event", name, resource)
				continue
			}
			kind := map[string]string{"secrets": "Secret", "widgets": "Widget"}[resource]
			if !strings.Contains(e, EventDisownUnreadable) || !strings.Contains(e, kind) || strings.Contains(e, text) {
				t.Errorf("%s, %s: the event is %q", name, resource, e)
			}
			want, not := c.want, c.not
			if resource == "widgets" && name == "not served" {
				want, not = []string{"its kind isn't served", "restore it", "restart"}, []string{"protected kinds", "grant"}
			}
			for _, s := range want {
				if !strings.Contains(e, s) {
					t.Errorf("%s, %s: the event %q doesn't say %q", name, resource, e, s)
				}
			}
			for _, s := range not {
				if strings.Contains(e, s) {
					t.Errorf("%s, %s: the event %q says %q", name, resource, e, s)
				}
			}
			w.mu.Lock()
			if got := fmt.Sprint(w.got); !strings.Contains(got, c.err.Error()) || !strings.Contains(got, "what list ") {
				t.Errorf("%s, %s: warnings %q don't have the error, or what it is about", name, resource, w.got)
			}
			w.mu.Unlock()
		}
	}
}

// unreadableGauge is what the gauge of unreadable objects was set to.
type unreadableGauge struct {
	NopMetrics
	sets []int
}

func (g *unreadableGauge) DisownUnreadable(_ string, n int) { g.sets = append(g.sets, n) }

// Disown of an indexed kind: an index, or an object, it can't read.
func TestDisownSaysItCantReadAnIndexedKind(t *testing.T) {
	ctx := context.Background()
	text := "attempt 7f3a"
	for name, c := range map[string]struct {
		getErr error
		fail   bool // the object's read, not the index's
		want   []string
	}{
		"the index missing":     {nil, false, []string{"the index idx/cu-", "(notfound)"}},
		"the index refused":     {apierrors.NewForbidden(configMapsGR, "cu-index", errors.New(text)), false, []string{"the index idx/cu-", "(forbidden)", "grant get"}},
		"the index unreachable": {apierrors.NewServiceUnavailable(text), false, []string{"the index idx/cu-", "(serviceunavailable)", "the next pass retries"}},
		"an object":             {nil, true, []string{"ns/Secret/s: serviceunavailable"}},
	} {
		r := newFakeRig(t)
		w := &warnings{}
		g := &unreadableGauge{}
		o := r.options(widgetGK, ProtectedKind{GVK: secretGVK})
		o.Logger, o.Metrics = w, g
		tr, err := r.s.Track(ctx, o)
		if err != nil {
			t.Fatal(err)
		}
		// As a PerName kind, with an index of the test's own.
		tr.o.Protected = []ProtectedKind{{GVK: secretGVK, Watch: PerName}}
		st := &memStore{exists: name != "the index missing", rebuilt: true, failGet: c.getErr}
		if c.fail {
			st.lines = linesOf([]ID{secretID("ns", "s")})
			r.md.PrependReactor("get", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewServiceUnavailable(text)
			})
		}
		tr.idx = testIndex(st, r.s.newServer(tr.o.Holder))
		if !tr.isPerName(secretGroupKind) || len(tr.disownPerName(parseDisown("all"))) != 1 {
			t.Fatal("the rig's kind isn't PerName")
		}
		r.s.disownPass(ctx, tr, parseDisown("all"))
		var e string
		select {
		case e = <-r.events.Events:
		default:
			t.Errorf("%s: no event", name)
			continue
		}
		for _, s := range append(c.want, EventDisownUnreadable) {
			if !strings.Contains(e, s) {
				t.Errorf("%s: the event %q doesn't say %q", name, e, s)
			}
		}
		if strings.Contains(e, text) {
			t.Errorf("%s: the event %q has the error's text", name, e)
		}
		w.mu.Lock()
		if c.getErr != nil || c.fail {
			if !strings.Contains(fmt.Sprint(w.got), text) {
				t.Errorf("%s: warnings %q don't have the error", name, w.got)
			}
		}
		w.mu.Unlock()
		if n := len(r.events.Events); n != 0 {
			t.Errorf("%s: %d events more, the first %q", name, n, <-r.events.Events)
		}
		// A pass that the process's stop cut short says nothing.
		stopped, cancel := context.WithCancel(ctx)
		cancel()
		r.clk.Step(2 * eventThrottle)
		sets := len(g.sets)
		r.s.disownPass(stopped, tr, parseDisown("all"))
		if len(r.events.Events) != 0 {
			t.Errorf("%s: a stopped pass recorded %q", name, <-r.events.Events)
		}
		if want := map[bool]int{true: 1}[c.fail]; sets != 1 || g.sets[0] != want || len(g.sets) != sets {
			t.Errorf("%s: the gauge was set to %v, want once, to %d", name, g.sets, want)
		}
	}
}
