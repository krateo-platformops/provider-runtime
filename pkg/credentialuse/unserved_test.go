package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	metadatafake "k8s.io/client-go/metadata/fake"
	clienttesting "k8s.io/client-go/testing"
	testingclock "k8s.io/utils/clock/testing"

	"github.com/krateo-platformops/provider-runtime/pkg/logging"
)

// A 404 for a resource the API server doesn't serve says nothing of the
// object (§6.2): it is an ordinary failure everywhere, never "gone". The
// premise, that the API server and client-go give it as here, is
// TestPremiseUnservedResource's.

// unservedError is that 404, as client-go builds it from the API server's
// plain-text answer.
func unservedError() error {
	return apierrors.NewGenericServerResponse(http.StatusNotFound, "GET", schema.GroupResource{}, "", "404 page not found", 0, true)
}

// unserve makes every call on kind with one of ops, or any, fail that way.
func unserve(kind string, ops ...string) func(op, k, name string) error {
	return func(op, k, _ string) error {
		if k == kind && (len(ops) == 0 || slices.Contains(ops, op)) {
			return unservedError()
		}
		return nil
	}
}

func TestAnUnservedResourceIsntNotFound(t *testing.T) {
	for name, err := range map[string]error{"bare": unservedError(), "wrapped": fmt.Errorf("R1: %w", unservedError())} {
		if !apierrors.IsNotFound(err) {
			t.Errorf("%s: the test's error isn't the 404 client-go gives", name)
		}
		if c := Classify(err); c != ClassOther {
			t.Errorf("%s: Classify = %v, want Other", name, c)
		}
		if c := ClassifyJSONPatch(err); c != ClassOther {
			t.Errorf("%s: ClassifyJSONPatch = %v, want Other", name, c)
		}
	}
	// Another cause doesn't make an object's 404 an unserved resource's.
	other := apierrors.NewNotFound(secrets, "s")
	other.ErrStatus.Details.Causes = []metav1.StatusCause{{Type: metav1.CauseTypeFieldValueInvalid}}
	if c := Classify(other); c != ClassNotFound {
		t.Errorf("an object's 404 with another cause: Classify = %v, want NotFound", c)
	}
	gone := apierrors.NewNotFound(secrets, "s")
	if Classify(gone) != ClassNotFound || ClassifyJSONPatch(gone) != ClassNotFound {
		t.Error("a missing object isn't NotFound")
	}
	if got := shortError(unservedError()); strings.Contains(got, "notfound") {
		t.Errorf("an event names it %q", got)
	}
	if got := shortError(gone); got != "notfound" {
		t.Errorf("an event names a missing object %q", got)
	}
}

// The acquirer: A2 fails, so a new holder doesn't connect and an established
// one is Degraded, where Missing would let both go on as if nothing were
// there to hold; and BeforeEstablish adds no owner finalizer.
func TestAcquireOfAnUnservedKind(t *testing.T) {
	ctx := context.Background()
	for _, established := range []bool{false, true} {
		r := newRig(t)
		heldSecret(r)
		h := r.holder(t, "h", established)
		r.srv.before = unserve("Secret")
		a, err := r.p.acquire(ctx, h, []Ref{SecretRef("ns", "s")})
		if err != nil || a.Err == nil || a.MustNotConnect == established || a.Degraded != established || len(a.Unheld) != 0 {
			t.Errorf("established %v: %+v, %v; want a failed acquire, and no Missing ref", established, a, err)
		}
	}
	r := newRig(t)
	h := r.holder(t, "h", false)
	a, err := r.p.acquire(ctx, h, []Ref{SecretRef("ns", "s")}) // s doesn't exist
	if err != nil || len(a.Unheld) != 1 || a.Unheld[0].Reason != ReasonMissing {
		t.Fatalf("%+v, %v; want s Missing", a, err)
	}
	r.srv.before = unserve("Secret")
	if err := r.p.beforeEstablish(ctx, h, a); err == nil {
		t.Error("BeforeEstablish lets a holder establish over a ref it couldn't read")
	}
}

// The consent gate: the read failed, so a hold is kept and the pass requeues,
// and a ref not held yet is unreadable, not Missing (§11.3).
func TestConsentOfAnUnservedKind(t *testing.T) {
	ctx := context.Background()
	r, _ := consentRig(t, "ns")
	must(r.m.Create(secretKey("other", "s"), metav1.ObjectMeta{}))
	h := r.holder(t, "h", true)
	if a, err := r.p.acquire(ctx, h, []Ref{otherSecret()}); err != nil || len(a.Held) != 1 {
		t.Fatalf("%+v, %v; want it held", a, err)
	}
	r.srv.before = unserve("Secret")
	a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{otherSecret()})
	if err != nil || len(a.Held) != 1 || a.RetryAfter == 0 || len(a.Unheld) != 0 {
		t.Errorf("a held ref: %+v, %v; want it kept, and the pass requeued", a, err)
	}
	h2 := r.holder(t, "h2", true)
	a, err = r.p.acquire(ctx, h2, []Ref{otherSecret()})
	if err != nil || len(a.Unheld) != 1 || a.Unheld[0].Reason != ReasonConsentUnreadable {
		t.Errorf("a new ref: %+v, %v; want its consent unreadable", a, err)
	}
}

// The releaser: an R1 or an R3 that fails so leaves the object undecided, for
// a retry, and never reports it bare, which would prune its line; nor is a
// holder list that fails so a list of no users.
func TestReleaseOfAnUnservedKind(t *testing.T) {
	ctx := context.Background()
	id := SecretRef("ns", "s").ID()
	for name, before := range map[string]func(op, kind, name string) error{
		"R1":              unserve("Secret", "get"),
		"R3":              unserve("Secret", "patch"),
		"the holder list": unserve("Widget", "list"),
	} {
		r := newRig(t)
		heldSecret(r)
		r.srv.before = before
		res, err := r.p.release(ctx, "ns", []ID{id})
		if err == nil || len(res.Bare) != 0 || len(res.Released) != 0 {
			t.Errorf("%s: %+v, %v; want an error, and nothing bare or released", name, res, err)
		}
		if !hasFinalizer(r.get(t, secretKey("ns", "s")), fNS) {
			t.Errorf("%s: s lost its finalizer", name)
		}
	}
	// The epoch's repair.
	r := newRig(t)
	heldSecret(r)
	if _, err := r.m.Patch(secretKey("ns", "s"), types.MergePatchType, []byte(`{"metadata":{"annotations":{"`+fNS+`":null}}}`)); err != nil {
		t.Fatal(err)
	}
	r.srv.before = unserve("Secret", "patch")
	if _, err := r.p.release(ctx, "ns", []ID{id}); err == nil {
		t.Error("a failed repair of the epoch isn't reported")
	}
}

// The index: an object that can't be read so may carry the finalizer, so a
// prune puts its line back, and the startup recheck keeps it and fails.
func TestIndexLinesOfAnUnservedKind(t *testing.T) {
	ctx := context.Background()
	id := SecretRef("ns", "s").ID()
	st := &memStore{exists: true, lines: []string{id.String()}, rebuilt: true}
	srv := &countingServer{server: &objServer{objs: map[ID]*metav1.PartialObjectMetadata{id: heldObj("s")}}}
	ix := testIndex(st, srv)
	srv.before = unserve("Secret")
	if err := ix.prune(ctx, []ID{id}); err != nil || !slices.Contains(st.lines, id.String()) {
		t.Errorf("after a prune: %q, %v; want the line back", st.lines, err)
	}
	if err := ix.recheck(ctx, func(ID, string) {}); err == nil || !slices.Contains(st.lines, id.String()) {
		t.Errorf("after the recheck: %q, %v; want the line kept, and an error to retry", st.lines, err)
	}
}

// The prober: a probe that couldn't tell is retried on its backoff as it is,
// not taken for still missing.
func TestProbeOfAnUnservedKind(t *testing.T) {
	clk := testingclock.NewFakeClock(time.Now())
	cs := &countingServer{server: &objServer{}, before: unserve("Secret")}
	tr := &Tracker{s: newShared(Clients{}, SharedOptions{}, clk), gk: widgetGK, o: Options{ProbeInterval: time.Second}, missing: newMissingRefs(),
		proto: &protocol{srv: cs}, sink: sinks{enqueue: func(types.NamespacedName) { t.Error("a holder was enqueued") }}}
	id := SecretRef("ns", "s").ID()
	tr.missing.set(types.NamespacedName{Namespace: "ns", Name: "h"}, []ID{id}, clk.Now(), time.Second)
	for range 3 {
		clk.Step(2 * time.Second)
		tr.probe(context.Background())
	}
	if got := tr.missing.refs[id].backoff; got != time.Second {
		t.Errorf("the probe's backoff is %s, want it unchanged", got)
	}
}

// The janitor, Disown's strip and its switch, and DisownPrefix: each reports
// the failure, rather than take the object, or the CRD, for gone.
func TestDisownAndTheJanitorOfAnUnservedKind(t *testing.T) {
	ctx := context.Background()
	id := SecretRef("ns", "s").ID()
	r := newFakeRig(t)
	tr := r.track(t, widgetGK)
	for _, op := range []string{"get", "patch"} {
		stripped := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "s", UID: "u1", ResourceVersion: "1",
			Labels: map[string]string{LabelHeld: "true", MarkerLabel(widgetGK): "true"}}}
		r.s.hooks.wrapServer = func(server) server {
			return &countingServer{server: &objServer{objs: map[ID]*metav1.PartialObjectMetadata{id: stripped}}, before: unserve("Secret", op)}
		}
		if err := r.s.clean(ctx, id); !apierrors.IsNotFound(err) {
			t.Errorf("the janitor, its %s failing: %v; want the failure, not an object that is gone", op, err)
		}
	}

	srv := &countingServer{server: &objServer{}, before: unserve("Secret")}
	heldS := heldObj("s")
	heldS.UID, heldS.ResourceVersion, heldS.Annotations = "u1", "1", map[string]string{fNS: "e0"}
	if stripped, _, err := stripDisowned(ctx, srv, secretGroupKind, widgetGK, heldS); !apierrors.IsNotFound(err) || stripped {
		t.Errorf("Disown's strip: %v, %v; want the failure", stripped, err)
	}
	// Its read after a patch whose test failed.
	first := true
	srv.before = func(op, _, _ string) error {
		if op == "patch" && first {
			first = false
			return testFailedError()
		}
		return unservedError()
	}
	if _, _, err := stripDisowned(ctx, srv, secretGroupKind, widgetGK, heldS); !apierrors.IsNotFound(err) {
		t.Errorf("Disown's second read: %v; want the failure", err)
	}

	r.md.PrependReactor("get", "customresourcedefinitions", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, unservedError()
	})
	if _, err := tr.disownLive(ctx); err == nil {
		t.Error("a switch that couldn't be read is taken for none")
	}

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{crdGVR: "CustomResourceDefinitionList"})
	md := metadatafake.NewSimpleMetadataClient(metadatafake.NewTestScheme())
	md.PrependReactor("get", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) { return true, nil, unservedError() })
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Version: "v1"}})
	mapper.Add(secretGVK, meta.RESTScopeNamespace)
	_, err := DisownPrefix(ctx, Clients{Metadata: md, Dynamic: dyn, Mapper: mapper}, DisownPrefixOptions{Holder: widgetGK, Names: []Ref{SecretRef("ns", "s")}})
	if err == nil || errors.Is(err, ErrHolderCRDExists) {
		t.Errorf("DisownPrefix = %v; want the read's failure", err)
	}
}

// DisownPrefix can't read or strip an unserved kind's objects: it strips the
// others, and reports those it couldn't, on every run while they are named or
// the kind is listed; without them it succeeds.
func TestDisownPrefixStripsAroundAnUnservedKind(t *testing.T) {
	ctx := context.Background()
	gone := schema.GroupVersionKind{Group: widgetGK.Group, Version: "v1", Kind: "Multi"}
	run := func(o DisownPrefixOptions, mapped bool) (DisownReport, error) {
		t.Helper()
		dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
			map[schema.GroupVersionResource]string{crdGVR: "CustomResourceDefinitionList"})
		scheme := metadatafake.NewTestScheme()
		if err := metav1.AddMetaToScheme(scheme); err != nil {
			t.Fatal(err)
		}
		held := heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns")
		held.ResourceVersion = "1"
		held.Labels[MarkerLabel(widgetGK)] = "true"
		md := metadatafake.NewSimpleMetadataClient(scheme, held)
		md.PrependReactor("*", "multis", func(clienttesting.Action) (bool, runtime.Object, error) { return true, nil, unservedError() })
		mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Version: "v1"}, {Group: gone.Group, Version: "v1"}})
		mapper.Add(secretGVK, meta.RESTScopeNamespace)
		if mapped { // as a mapper that hasn't noticed
			mapper.Add(gone, meta.RESTScopeNamespace)
		}
		o.Holder = widgetGK
		return DisownPrefix(ctx, Clients{Metadata: md, Dynamic: dyn, Mapper: mapper}, o)
	}
	multi := Ref{GroupKind: gone.GroupKind(), Namespace: "ns", Name: "m"}
	for name, o := range map[string]DisownPrefixOptions{
		"by name":       {Names: []Ref{SecretRef("ns", "s"), multi}},
		"by the marker": {Protected: []schema.GroupVersionKind{secretGVK, gone}},
	} {
		for _, mapped := range []bool{true, false} {
			rep, err := run(o, mapped)
			if err == nil || len(rep.Stripped) != 1 || rep.Stripped[0] != SecretRef("ns", "s").ID() {
				t.Errorf("%s, mapped %v: %+v, %v; want the Secret stripped, and the unserved kind reported", name, mapped, rep, err)
			}
		}
	}
	if rep, err := run(DisownPrefixOptions{Names: []Ref{SecretRef("ns", "s")}}, false); err != nil || len(rep.Stripped) != 1 {
		t.Errorf("without it: %+v, %v", rep, err)
	}
}

// warnings records a tracker's warnings.
type warnings struct {
	mu  sync.Mutex
	got []string
}

func (w *warnings) Info(string, ...any)         {}
func (w *warnings) Debug(string, ...any)        {}
func (w *warnings) Error(error, string, ...any) {}
func (w *warnings) Warn(msg string, kv ...any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.got = append(w.got, fmt.Sprint(msg, kv))
}
func (w *warnings) WithValues(...any) logging.Logger { return w }
func (w *warnings) WithName(string) logging.Logger   { return w }

// Disown of a LabelScoped kind that isn't served: the count is unknown, and
// each pass says which kind, and why.
func TestDisownSaysWhichKindItCantList(t *testing.T) {
	ctx := context.Background()
	r := newFakeRig(t)
	w := &warnings{}
	o := r.options(widgetGK)
	o.Logger = w
	tr, err := r.s.Track(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.dyn.Resource(crdGVR).Patch(ctx, "widgets."+widgetGK.Group, types.MergePatchType,
		[]byte(`{"metadata":{"annotations":{"`+AnnotationDisown+`":"all"}}}`), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	r.md.PrependReactor("list", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) { return true, nil, unservedError() })
	r.s.disownPass(ctx, tr, parseDisown("all"))
	crd, err := r.dyn.Resource(crdGVR).Get(ctx, "widgets."+widgetGK.Group, metav1.GetOptions{})
	if err != nil || crd.GetAnnotations()[AnnotationHeldCount] != "unknown" {
		t.Errorf("held-count %q, %v; want unknown", crd.GetAnnotations()[AnnotationHeldCount], err)
	}
	if !slices.ContainsFunc(w.got, func(s string) bool { return strings.Contains(s, "Secret") && strings.Contains(s, "isn't served") }) {
		t.Errorf("warnings %q; want one naming Secret, and that it isn't served", w.got)
	}
}

// A chain: what lies behind an unheld hop that couldn't be read so isn't
// taken for absent, and a shared read doesn't remember it as gone (§9.4).
func TestAnUnheldHopOfAnUnservedKind(t *testing.T) {
	ctx := context.Background()
	calls := 0
	c := contentFunc(func() error { calls++; return unservedError() })
	s := newSharedReads(testingclock.NewFakePassiveClock(time.Now()))
	for range 2 {
		if _, err := s.get(ctx, c, SecretRef("ns", "s").ID()); err == nil || Classify(err) != ClassOther {
			t.Errorf("the shared read = %v, want the failure", err)
		}
	}
	if calls != 2 {
		t.Errorf("%d reads, want 2: a failure isn't remembered", calls)
	}
}

// contentFunc is a contentReader whose every read fails with its error.
type contentFunc func() error

func (f contentFunc) Content(context.Context, schema.GroupKind, string, string) (*unstructured.Unstructured, error) {
	return nil, f()
}
