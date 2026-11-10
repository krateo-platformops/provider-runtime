package credentialuse

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

// Direct tests of the protocol's steps on the model, one call at a time, with
// no scheduler: what each step writes, and every outcome of §6.2 and §8.

var targetGK = schema.GroupKind{Group: "credentialuse.test.krateo.io", Kind: "Target"}

// countingServer counts calls, and lets a test change or fail one.
type countingServer struct {
	server
	calls  []string
	before func(op, kind, name string) error // nil, or an error to return instead
}

func (c *countingServer) record(op, kind, name string) error {
	c.calls = append(c.calls, op+" "+kind+"/"+name)
	if c.before != nil {
		return c.before(op, kind, name)
	}
	return nil
}

func (c *countingServer) Get(ctx context.Context, gk schema.GroupKind, ns, name string) (*metav1.PartialObjectMetadata, error) {
	if err := c.record("get", gk.Kind, name); err != nil {
		return nil, err
	}
	return c.server.Get(ctx, gk, ns, name)
}

func (c *countingServer) Patch(ctx context.Context, gk schema.GroupKind, ns, name string, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error) {
	if err := c.record("patch", gk.Kind, name); err != nil {
		return nil, err
	}
	return c.server.Patch(ctx, gk, ns, name, pt, data)
}

func (c *countingServer) List(ctx context.Context, gk schema.GroupKind, ns string) ([]metav1.PartialObjectMetadata, error) {
	if err := c.record("list", gk.Kind, ""); err != nil {
		return nil, err
	}
	return c.server.List(ctx, gk, ns)
}

// rig is a model, a clock that tests move, and a protocol on them.
type rig struct {
	m     *apimodel.Model
	now   time.Time
	srv   *countingServer
	views views
	p     *protocol
	rep   *recordingReports
}

type recordingReports struct {
	degraded []string
	rejected []ID
}

func (r *recordingReports) Degraded(_ schema.GroupKind, reason string) {
	r.degraded = append(r.degraded, reason)
}
func (r *recordingReports) ReleaseRejected(_ schema.GroupKind, p ID, _ string) {
	r.rejected = append(r.rejected, p)
}

// rigClock moves a microsecond on every reading, as a real clock's readings
// are each later than the last.
type rigClock struct{ r *rig }

func (c rigClock) Now() time.Time {
	c.r.now = c.r.now.Add(time.Microsecond)
	return c.r.now
}
func (c rigClock) Since(t time.Time) time.Duration { return c.r.now.Sub(t) }

func newRig(t *testing.T) *rig {
	t.Helper()
	r := &rig{now: t0, rep: &recordingReports{}}
	r.m = apimodel.New(func() time.Time { return r.now })
	r.srv = &countingServer{server: modelServer{m: r.m}}
	r.views = noViews{}
	r.p = newProtocol(r.srv, viewsOf{r}, newVerified(widgetGK), newFirstSight(), rigClock{r}, time.Minute)
	r.p.reports = r.rep
	must(r.m.Create(nsKey("ns"), metav1.ObjectMeta{}))
	return r
}

// viewsOf follows the rig's views, so a test can swap them.
type viewsOf struct{ r *rig }

func (v viewsOf) Protected(id ID) (*metav1.PartialObjectMetadata, bool) {
	return v.r.views.Protected(id)
}
func (v viewsOf) HoldersNaming(id ID, ns string) []*metav1.PartialObjectMetadata {
	return v.r.views.HoldersNaming(id, ns)
}
func (v viewsOf) NamespaceDeleting(ns string) (bool, bool) { return v.r.views.NamespaceDeleting(ns) }

func (r *rig) tick(d time.Duration) { r.now = r.now.Add(d) }

func (r *rig) get(t *testing.T, k apimodel.Key) *metav1.PartialObjectMetadata {
	t.Helper()
	o, err := r.m.Get(k)
	if err != nil {
		t.Fatalf("get %s: %v", k, err)
	}
	return o
}

func (r *rig) holder(t *testing.T, name string, established bool) *metav1.PartialObjectMetadata {
	t.Helper()
	m := metav1.ObjectMeta{}
	if established {
		m.Finalizers = []string{ownerFinalizer}
	}
	must(r.m.Create(widgetKey("ns", name), m))
	return r.get(t, widgetKey("ns", name))
}

var fNS = func() string { f, _ := FinalizerName(widgetGK, "ns"); return f }()

func TestAcquireANewHolder(t *testing.T) {
	r := newRig(t)
	must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{Finalizers: []string{"example.com/other"}}))
	h := r.holder(t, "h", false)
	a, err := r.p.acquire(context.Background(), h, []Ref{SecretRef("ns", "s"), SecretRef("ns", "s")})
	if err != nil || a.MustNotConnect || a.Degraded || len(a.Held) != 1 {
		t.Fatalf("acquire = %+v, %v; want one ref held", a, err)
	}
	// A1: the record, guarded, and the caller's object follows.
	stored := r.get(t, widgetKey("ns", "h"))
	if got := ReadRecord(stored); !got.Names(SecretRef("ns", "s").ID()) || got.Unparseable {
		t.Errorf("record = %+v", got)
	}
	if h.ResourceVersion != stored.ResourceVersion || h.Annotations[AnnotationRefs] == "" {
		t.Errorf("the caller's object didn't take A1's response: rv %s vs %s", h.ResourceVersion, stored.ResourceVersion)
	}
	// A2: the finalizer, an epoch and both labels, and the other finalizer kept.
	s := r.get(t, secretKey("ns", "s"))
	if !hasFinalizer(s, fNS) || !hasFinalizer(s, "example.com/other") || s.Annotations[fNS] == "" ||
		s.Labels[LabelHeld] != "true" || s.Labels[MarkerLabel(widgetGK)] != "true" {
		t.Errorf("secret after A2: %+v", s.ObjectMeta)
	}
	if want := []string{"patch Widget/h", "patch Secret/s"}; strings.Join(r.srv.calls, ",") != strings.Join(want, ",") {
		t.Errorf("calls = %v, want %v", r.srv.calls, want)
	}
}

// cacheOf answers Protected from the model directly: a cache with no lag.
type cacheOf struct {
	noViews
	m *apimodel.Model
}

func (c cacheOf) Protected(id ID) (*metav1.PartialObjectMetadata, bool) {
	o, err := c.m.Get(keyOf(id))
	return o, err == nil
}

func TestAcquireSkipsInTheSteadyState(t *testing.T) {
	r := newRig(t)
	must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
	h := r.holder(t, "h", true)
	refs := []Ref{SecretRef("ns", "s")}
	if _, err := r.p.acquire(context.Background(), h, refs); err != nil {
		t.Fatal(err)
	}
	r.views = cacheOf{m: r.m}
	// The informer delivers the version A1 wrote, after the pass's A2.
	current := r.get(t, widgetKey("ns", "h"))
	r.p.ver.holderEvent(current.UID, current, r.now)
	r.tick(time.Millisecond)

	// The run's first delivery came after the first write, so the next pass
	// writes once more (t_obs is the later of the delivery and A1's return,
	// §6.3), and only the pass after it skips: no call.
	r.srv.calls = nil
	if _, err := r.p.acquire(context.Background(), current, refs); err != nil {
		t.Fatal(err)
	}
	if want := []string{"patch Secret/s"}; strings.Join(r.srv.calls, ",") != strings.Join(want, ",") {
		t.Errorf("the pass after the delivery made %v, want %v", r.srv.calls, want)
	}
	r.srv.calls = nil
	a, err := r.p.acquire(context.Background(), r.get(t, widgetKey("ns", "h")), refs)
	if err != nil || len(a.Held) != 1 {
		t.Fatalf("acquire = %+v, %v", a, err)
	}
	if len(r.srv.calls) != 0 {
		t.Errorf("a steady-state pass made calls: %v", r.srv.calls)
	}

	// The same pass from a version the informer hasn't delivered writes
	// (H1 of #59's review).
	if _, err := r.m.Patch(widgetKey("ns", "h"), types.MergePatchType, []byte(`{"metadata":{"labels":{"x":"1"}}}`)); err != nil {
		t.Fatal(err)
	}
	r.srv.calls = nil
	if _, err := r.p.acquire(context.Background(), r.get(t, widgetKey("ns", "h")), refs); err != nil {
		t.Fatal(err)
	}
	if want := []string{"patch Secret/s"}; strings.Join(r.srv.calls, ",") != strings.Join(want, ",") {
		t.Errorf("a pass from an undelivered version made %v, want %v", r.srv.calls, want)
	}

	// A deletion pass always writes (§5, §6.3), even from a version the
	// informer has delivered, in the same run, after this process's write:
	// everything but the deletion would let it skip.
	labeled := r.get(t, widgetKey("ns", "h"))
	r.p.ver.holderEvent(labeled.UID, labeled, r.now)
	must(r.m.Delete(widgetKey("ns", "h")))
	deleting := r.get(t, widgetKey("ns", "h"))
	r.p.ver.holderEvent(deleting.UID, deleting, r.now)
	r.tick(time.Millisecond)
	if cached, ok := r.views.Protected(SecretRef("ns", "s").ID()); !ok || !r.p.ver.canSkip(deleting.UID, deleting.ResourceVersion, SecretRef("ns", "s").ID(), cached.UID, "ns") {
		t.Fatal("the deletion pass's version isn't otherwise skippable: the case below would test nothing")
	}
	before := r.get(t, secretKey("ns", "s")).Annotations[fNS]
	r.srv.calls = nil
	if _, err := r.p.acquire(context.Background(), deleting, refs); err != nil {
		t.Fatal(err)
	}
	if want := []string{"patch Secret/s"}; strings.Join(r.srv.calls, ",") != strings.Join(want, ",") {
		t.Errorf("a deletion pass made %v, want %v", r.srv.calls, want)
	}
	if after := r.get(t, secretKey("ns", "s")).Annotations[fNS]; after == before {
		t.Error("a deletion pass didn't move the epoch")
	}
}

func TestAcquireOutcomes(t *testing.T) {
	ctx := context.Background()
	t.Run("missing, then created before establishing", func(t *testing.T) {
		r := newRig(t)
		h := r.holder(t, "h", false)
		a, err := r.p.acquire(ctx, h, []Ref{SecretRef("ns", "s")})
		if err != nil || a.MustNotConnect || len(a.Unheld) != 1 || a.Unheld[0].Reason != ReasonMissing {
			t.Fatalf("acquire = %+v, %v; want Missing, connecting", a, err)
		}
		if !ReadRecord(r.get(t, widgetKey("ns", "h"))).Names(SecretRef("ns", "s").ID()) {
			t.Error("a Missing ref isn't recorded")
		}
		if err := r.p.beforeEstablish(ctx, h, a); err != nil {
			t.Errorf("beforeEstablish with the ref still missing: %v", err)
		}
		must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
		if err := r.p.beforeEstablish(ctx, h, a); err != nil {
			t.Fatal(err)
		}
		if !hasFinalizer(r.get(t, secretKey("ns", "s")), fNS) {
			t.Error("beforeEstablish didn't hold the ref that appeared (§6.7)")
		}
	})
	deletingSecret := func(r *rig) {
		must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{Finalizers: []string{"example.com/other"}}))
		must(r.m.Delete(secretKey("ns", "s")))
	}
	t.Run("missing, then appeared but can't be held: no establishing", func(t *testing.T) {
		r := newRig(t)
		h := r.holder(t, "h", false)
		a, err := r.p.acquire(ctx, h, []Ref{SecretRef("ns", "s")})
		if err != nil || len(a.Unheld) != 1 || a.Unheld[0].Reason != ReasonMissing {
			t.Fatalf("acquire = %+v, %v; want Missing", a, err)
		}
		deletingSecret(r) // it appears already being deleted: A2 can't add the finalizer
		if err := r.p.beforeEstablish(ctx, h, a); err == nil {
			t.Error("beforeEstablish let the holder establish on a ref it couldn't hold (§6.7)")
		}
	})
	t.Run("being deleted, a new holder must not connect", func(t *testing.T) {
		r := newRig(t)
		deletingSecret(r)
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{SecretRef("ns", "s")})
		if err != nil || !a.MustNotConnect || a.Degraded || a.Err == nil || !strings.Contains(a.Err.Error(), "is being deleted and can't be held") {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
	})
	t.Run("being deleted, an established holder is Degraded", func(t *testing.T) {
		r := newRig(t)
		deletingSecret(r)
		a, err := r.p.acquire(ctx, r.holder(t, "h", true), []Ref{SecretRef("ns", "s")})
		if err != nil || a.MustNotConnect || !a.Degraded || len(a.Unheld) != 1 || a.Unheld[0].Reason != ReasonDeleting {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
		if len(r.rep.degraded) != 1 {
			t.Errorf("degraded reports = %v", r.rep.degraded)
		}
	})
	t.Run("an A2 error fails closed for a new holder and open for an established one", func(t *testing.T) {
		for _, established := range []bool{false, true} {
			r := newRig(t)
			must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
			h := r.holder(t, "h", established)
			r.srv.before = func(op, kind, _ string) error {
				if op == "patch" && kind == "Secret" {
					return apierrors.NewServiceUnavailable("down")
				}
				return nil
			}
			a, err := r.p.acquire(ctx, h, []Ref{SecretRef("ns", "s")})
			if err != nil || a.MustNotConnect == established || a.Degraded != established || a.Err == nil {
				t.Errorf("established=%v: acquire = %+v, %v", established, a, err)
			}
		}
	})
	t.Run("a conflict on A1 is the caller's to requeue", func(t *testing.T) {
		r := newRig(t)
		h := r.holder(t, "h", true)
		h.ResourceVersion = "1" // stale
		_, err := r.p.acquire(ctx, h, []Ref{SecretRef("ns", "s")})
		if !errors.Is(err, ErrHolderConflict) {
			t.Fatalf("acquire with a stale holder: %v, want ErrHolderConflict", err)
		}
	})
	t.Run("a ref in another namespace is never held yet", func(t *testing.T) {
		r := newRig(t)
		must(r.m.Create(nsKey("other"), metav1.ObjectMeta{}))
		must(r.m.Create(secretKey("other", "s"), metav1.ObjectMeta{}))
		h := r.holder(t, "h", true)
		a, err := r.p.acquire(ctx, h, []Ref{SecretRef("other", "s")})
		if err != nil || len(a.Unheld) != 1 || a.Unheld[0].Reason != ReasonNotConsented || len(r.srv.calls) != 0 {
			t.Fatalf("acquire = %+v, %v, calls %v", a, err, r.srv.calls)
		}
		if hasFinalizer(r.get(t, secretKey("other", "s")), fNS) {
			t.Error("a Secret in another namespace was pinned")
		}
	})
	t.Run("chains aren't supported yet", func(t *testing.T) {
		r := newRig(t)
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{{GroupKind: targetGK, Namespace: "ns", Name: "t", Guaranteed: true}})
		if err != nil || !a.MustNotConnect || !errors.Is(a.Err, errChainsNotYet) {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
	})
	t.Run("an unparseable record is rewritten, and every ref writes", func(t *testing.T) {
		r := newRig(t)
		must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
		must(r.m.Create(widgetKey("ns", "h"), metav1.ObjectMeta{Annotations: map[string]string{AnnotationRefs: "oops"}}))
		a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{SecretRef("ns", "s")})
		if err != nil || len(a.Held) != 1 {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
		if rec := ReadRecord(r.get(t, widgetKey("ns", "h"))); rec.Unparseable || !rec.Names(SecretRef("ns", "s").ID()) {
			t.Errorf("record = %+v", rec)
		}
	})
	t.Run("an unparseable record and no refs: the record goes", func(t *testing.T) {
		// An unparseable record names nothing, so it "equals" no refs; it is
		// rewritten all the same, or it would stay unparseable for good.
		r := newRig(t)
		must(r.m.Create(widgetKey("ns", "h"), metav1.ObjectMeta{Annotations: map[string]string{AnnotationRefs: "oops"}}))
		if _, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), nil); err != nil {
			t.Fatal(err)
		}
		if ReadRecord(r.get(t, widgetKey("ns", "h"))).Present {
			t.Error("the unparseable record is still there")
		}
	})
	t.Run("no refs: the record goes, and observed with it", func(t *testing.T) {
		r := newRig(t)
		must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
		must(r.m.Create(widgetKey("ns", "h"), metav1.ObjectMeta{}))
		p := SecretRef("ns", "s").ID()
		// A1 adds the ref: observed gets A1's return.
		if _, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{SecretRef("ns", "s")}); err != nil {
			t.Fatal(err)
		}
		uid := r.get(t, widgetKey("ns", "h")).UID
		if _, ok := r.p.ver.observed[uid][p]; !ok {
			t.Fatal("A1 that added the ref didn't set observed")
		}
		// A1 removes it: observed goes (§6.3).
		if _, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), nil); err != nil {
			t.Fatal(err)
		}
		if ReadRecord(r.get(t, widgetKey("ns", "h"))).Present {
			t.Error("the record is still there")
		}
		if _, ok := r.p.ver.observed[uid][p]; ok {
			t.Error("A1 that removed the ref left observed")
		}
	})
	t.Run("forget", func(t *testing.T) {
		r := newRig(t)
		h := &metav1.ObjectMeta{UID: "h1", Annotations: map[string]string{AnnotationRefs: "[]", AnnotationUnheld: "[]", "keep": "x"}}
		r.p.ver.announced("h1", []ID{SecretRef("ns", "s").ID()}, nil, r.now)
		if !r.p.forget(h) || len(h.Annotations) != 1 || r.p.forget(h) {
			t.Errorf("forget left %v", h.Annotations)
		}
		if _, ok := r.p.ver.observed["h1"]; ok {
			t.Error("forget left observed")
		}
	})
}

// A webhook that changes the holder on A1 (L2 of the review of revision 16)
// makes the caller re-read before its next write.
func TestAcquireRereadsAfterAChangedHolder(t *testing.T) {
	r := newRig(t)
	must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
	h := r.holder(t, "h", true)
	r.srv.server = mutatingServer{server: modelServer{m: r.m}}
	a, err := r.p.acquire(context.Background(), h, []Ref{SecretRef("ns", "s")})
	if err != nil || !a.Reread {
		t.Fatalf("acquire = %+v, %v; want Reread", a, err)
	}
	if h.Labels["webhook"] != "added" {
		t.Error("the caller's object didn't take the whole returned metadata")
	}
}

// mutatingServer is a webhook that labels every holder it sees patched.
type mutatingServer struct{ server }

func (s mutatingServer) Patch(ctx context.Context, gk schema.GroupKind, ns, name string, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error) {
	out, err := s.server.Patch(ctx, gk, ns, name, pt, data)
	if err == nil && gk == widgetGK {
		return s.server.Patch(ctx, gk, ns, name, types.MergePatchType, []byte(`{"metadata":{"labels":{"webhook":"added"}}}`))
	}
	return out, err
}

func TestAcquireACustomResourceRetriesConflicts(t *testing.T) {
	for _, conflicts := range []int{2, 3} {
		r := newRig(t)
		must(r.m.Create(apimodel.Key{GK: targetGK, Namespace: "ns", Name: "t"}, metav1.ObjectMeta{Finalizers: []string{"example.com/other"}}))
		h := r.holder(t, "h", true)
		n := 0
		r.srv.before = func(op, kind, name string) error {
			if op == "patch" && kind == "Target" && n < conflicts {
				n++
				// Someone writes between A2's GET and its patch.
				_, err := r.m.Patch(apimodel.Key{GK: targetGK, Namespace: "ns", Name: "t"}, types.MergePatchType, []byte(`{"metadata":{"labels":{"bump":"`+string(rune('0'+n))+`"}}}`))
				return err
			}
			return nil
		}
		a, err := r.p.acquire(context.Background(), h, []Ref{{GroupKind: targetGK, Namespace: "ns", Name: "t"}})
		if err != nil {
			t.Fatal(err)
		}
		tgt := r.get(t, apimodel.Key{GK: targetGK, Namespace: "ns", Name: "t"})
		switch conflicts {
		case 2: // the third attempt lands
			if len(a.Held) != 1 || !hasFinalizer(tgt, fNS) || !hasFinalizer(tgt, "example.com/other") {
				t.Errorf("after 2 conflicts: %+v, finalizers %v", a, tgt.Finalizers)
			}
		case 3: // out of retries: Degraded (M5)
			if !a.Degraded || hasFinalizer(tgt, fNS) {
				t.Errorf("after 3 conflicts: %+v, finalizers %v", a, tgt.Finalizers)
			}
		}
	}
}

// heldSecret creates a Secret held for ns, as after an A2.
func heldSecret(r *rig, extra ...string) {
	must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{
		Finalizers:  append([]string{fNS}, extra...),
		Annotations: map[string]string{fNS: "e0"},
		Labels:      map[string]string{LabelHeld: "true", MarkerLabel(widgetGK): "true"},
	}))
}

// userOf creates a holder whose record names the Secret.
func userOf(r *rig, name string, established bool) {
	rec, _ := EncodeRecord([]ID{SecretRef("ns", "s").ID()})
	m := metav1.ObjectMeta{Annotations: map[string]string{AnnotationRefs: rec}}
	if established {
		m.Finalizers = []string{ownerFinalizer}
	}
	must(r.m.Create(widgetKey("ns", name), m))
}

func TestReleaseRows(t *testing.T) {
	ctx := context.Background()
	p := SecretRef("ns", "s").ID()
	type want struct {
		released bool
		requeue  bool
	}
	cases := map[string]struct {
		setup func(r *rig)
		want  want
	}{
		"live, a user: kept":      {func(r *rig) { heldSecret(r); userOf(r, "h", true) }, want{}},
		"live, no user: released": {func(r *rig) { heldSecret(r) }, want{released: true}},
		"a user without the owner finalizer, deleting, doesn't count": {func(r *rig) {
			heldSecret(r)
			userOf(r, "h", false)
			_, err := r.m.Patch(widgetKey("ns", "h"), types.MergePatchType, []byte(`{"metadata":{"finalizers":["example.com/keep"]}}`))
			must(err)
			must(r.m.Delete(widgetKey("ns", "h")))
		}, want{released: true}},
		"deleting, in the grace, a user: requeued": {func(r *rig) {
			heldSecret(r, "example.com/other")
			userOf(r, "h", true)
			must(r.m.Delete(secretKey("ns", "s")))
		}, want{requeue: true}},
		"deleting, after the grace, live users only: released": {func(r *rig) {
			heldSecret(r, "example.com/other")
			userOf(r, "h", true)
			must(r.m.Delete(secretKey("ns", "s")))
			sawDeletion(r)
			r.tick(2 * time.Minute)
		}, want{released: true}},
		"deleting, after the grace, a deleting user: kept": {func(r *rig) {
			heldSecret(r, "example.com/other")
			userOf(r, "h", true)
			must(r.m.Delete(widgetKey("ns", "h")))
			must(r.m.Delete(secretKey("ns", "s")))
			sawDeletion(r)
			r.tick(2 * time.Minute)
		}, want{}},
		"deleting, its namespace deleting, a live user: kept": {func(r *rig) {
			heldSecret(r, "example.com/other")
			userOf(r, "h", true)
			must(r.m.Delete(secretKey("ns", "s")))
			must(r.m.Delete(nsKey("ns")))
			r.tick(2 * time.Minute)
		}, want{}},
		"deleting, no user: released at once": {func(r *rig) {
			heldSecret(r, "example.com/other")
			must(r.m.Delete(secretKey("ns", "s")))
		}, want{released: true}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			tc.setup(r)
			res, err := r.p.release(ctx, "ns", []ID{p})
			if err != nil {
				t.Fatal(err)
			}
			s, gerr := r.m.Get(secretKey("ns", "s"))
			released := gerr != nil || !hasFinalizer(s, fNS)
			if released != tc.want.released {
				t.Errorf("released = %v, want %v; decisions %+v", released, tc.want.released, res.Decisions)
			}
			if (res.RequeueAfter > 0) != tc.want.requeue {
				t.Errorf("requeue after %s, want a requeue: %v", res.RequeueAfter, tc.want.requeue)
			}
			if released && gerr == nil {
				if _, ok := s.Labels[LabelHeld]; ok {
					t.Error("the held label stayed")
				}
			}
		})
	}
}

// sawDeletion runs a batch when the deletion happens, so that this process's
// first sight of it is then.
func sawDeletion(r *rig) {
	_, _ = r.p.release(context.Background(), "ns", []ID{SecretRef("ns", "s").ID()})
}

// The grace runs from first sight when this process sees a deletion late
// (§5, L4).
func TestReleaseGraceFromFirstSight(t *testing.T) {
	r := newRig(t)
	heldSecret(r, "example.com/other")
	userOf(r, "h", true)
	must(r.m.Delete(secretKey("ns", "s")))
	r.tick(50 * time.Second) // delivered late
	res, err := r.p.release(context.Background(), "ns", []ID{SecretRef("ns", "s").ID()})
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter < 59*time.Second || res.RequeueAfter > time.Minute {
		t.Errorf("requeue after %s, want the full grace from first sight", res.RequeueAfter)
	}
}

func TestReleaseRestoresAMissingEpoch(t *testing.T) {
	r := newRig(t)
	must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{Finalizers: []string{fNS}}))
	res, err := r.p.release(context.Background(), "ns", []ID{SecretRef("ns", "s").ID()})
	if err != nil || res.RequeueAfter == 0 {
		t.Fatalf("release = %+v, %v", res, err)
	}
	if s := r.get(t, secretKey("ns", "s")); s.Annotations[fNS] == "" || !hasFinalizer(s, fNS) {
		t.Errorf("secret = %+v", s.ObjectMeta)
	}
}

// A concurrent A2 between R1 and R3 breaks the release; the next round reads
// again, and keeps P for its new user.
func TestReleaseRetriesAfterAStateChange(t *testing.T) {
	r := newRig(t)
	heldSecret(r)
	done := false
	r.srv.before = func(op, kind, _ string) error {
		if op == "patch" && kind == "Secret" && !done {
			done = true
			userOf(r, "h2", false)
			pt, _ := AcquireStrategicPatch(widgetGK, "ns", "fresh")
			_, err := r.m.Patch(secretKey("ns", "s"), pt.Type, pt.Data)
			return err
		}
		return nil
	}
	res, err := r.p.release(context.Background(), "ns", []ID{SecretRef("ns", "s").ID()})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Released) != 0 || !hasFinalizer(r.get(t, secretKey("ns", "s")), fNS) {
		t.Errorf("the release won over a concurrent A2: %+v", res)
	}
	if len(res.Decisions) != 2 {
		t.Errorf("decisions = %+v, want two rounds", res.Decisions)
	}
}

// L1 of the review of revision 16: a release that fails against the
// resourceVersion it was built from is reported, not retried.
func TestReleaseRejectedByAnUnchangedObject(t *testing.T) {
	r := newRig(t)
	heldSecret(r)
	r.srv.before = func(op, kind, _ string) error {
		if op == "patch" && kind == "Secret" {
			return apierrors.NewGenericServerResponse(422, "", schema.GroupResource{}, "", "test failed", 0, false)
		}
		return nil
	}
	_, err := r.p.release(context.Background(), "ns", []ID{SecretRef("ns", "s").ID()})
	if !errors.Is(err, ErrReleaseRejected) {
		t.Fatalf("release = %v, want ErrReleaseRejected", err)
	}
	if len(r.rep.rejected) != 1 {
		t.Errorf("rejected reports = %v", r.rep.rejected)
	}
	patches := 0
	for _, c := range r.srv.calls {
		if strings.HasPrefix(c, "patch") {
			patches++
		}
	}
	if patches != 1 {
		t.Errorf("%d release patches, want 1: %v", patches, r.srv.calls)
	}
}

// R0: with the caches showing a user of a live P, a batch makes no call.
func TestReleaseFastPath(t *testing.T) {
	r := newRig(t)
	heldSecret(r)
	userOf(r, "h", true)
	r.views = modelViews{m: r.m}
	res, err := r.p.release(context.Background(), "ns", []ID{SecretRef("ns", "s").ID()})
	if err != nil || len(res.Released) != 0 || len(r.srv.calls) != 0 {
		t.Errorf("release = %+v, %v, calls %v", res, err, r.srv.calls)
	}
}

// R0 settles nothing for a P its cache doesn't have, whatever the cached
// holders say: the batch reads P live.
func TestReleaseFastPathNeedsP(t *testing.T) {
	r := newRig(t)
	heldSecret(r)
	userOf(r, "h", true)
	r.views = holdersOnly{modelViews{m: r.m}}
	res, err := r.p.release(context.Background(), "ns", []ID{SecretRef("ns", "s").ID()})
	if err != nil || len(res.Released) != 0 {
		t.Fatalf("release = %+v, %v", res, err)
	}
	if len(r.srv.calls) == 0 {
		t.Error("R0 settled a P its cache doesn't have")
	}
}

// holdersOnly are caches that have the holders but not P.
type holdersOnly struct{ modelViews }

func (holdersOnly) Protected(ID) (*metav1.PartialObjectMetadata, bool) { return nil, false }

// A Namespace that is gone counts as deleting (§5): P is kept for its live
// users, after the grace too.
func TestReleaseTreatsAMissingNamespaceAsDeleting(t *testing.T) {
	r := newRig(t)
	heldSecret(r, "example.com/other")
	userOf(r, "h", true)
	must(r.m.Delete(secretKey("ns", "s")))
	sawDeletion(r)
	r.tick(2 * time.Minute)
	r.srv.before = func(op, kind, name string) error {
		if op == "get" && kind == apimodel.NamespaceKind.Kind {
			return apierrors.NewNotFound(schema.GroupResource{Resource: "namespaces"}, name)
		}
		return nil
	}
	res, err := r.p.release(context.Background(), "ns", []ID{SecretRef("ns", "s").ID()})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Released) != 0 || len(res.Decisions) != 1 || !res.Decisions[0].Input.NamespaceDeleting {
		t.Errorf("release = %+v; want P kept, its namespace deleting", res)
	}
}

// A successful R3 drops this process's written entry for P and N at once,
// and leaves other namespaces' entries (§6.2 R3).
func TestReleaseDropsWritten(t *testing.T) {
	r := newRig(t)
	heldSecret(r)
	s := r.get(t, secretKey("ns", "s"))
	r.p.ver.wrote(s.UID, "ns", r.now)
	r.p.ver.wrote(s.UID, "other", r.now)
	res, err := r.p.release(context.Background(), "ns", []ID{SecretRef("ns", "s").ID()})
	if err != nil || len(res.Released) != 1 {
		t.Fatalf("release = %+v, %v", res, err)
	}
	if r.p.ver.hasWritten(s.UID, "ns") {
		t.Error("the released (P, N) kept its written entry")
	}
	if !r.p.ver.hasWritten(s.UID, "other") {
		t.Error("another namespace's written entry was dropped")
	}
}

// modelViews are caches with no lag.
type modelViews struct{ m *apimodel.Model }

func (v modelViews) Protected(id ID) (*metav1.PartialObjectMetadata, bool) {
	o, err := v.m.Get(keyOf(id))
	return o, err == nil
}

func (v modelViews) HoldersNaming(id ID, ns string) []*metav1.PartialObjectMetadata {
	var out []*metav1.PartialObjectMetadata
	for _, h := range v.m.List(widgetGK, ns) {
		if ReadRecord(&h).Names(id) {
			out = append(out, &h)
		}
	}
	return out
}

func (v modelViews) NamespaceDeleting(ns string) (bool, bool) {
	o, err := v.m.Get(nsKey(ns))
	if err != nil {
		return false, false
	}
	return o.DeletionTimestamp != nil, true
}
