package credentialuse

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	testingclock "k8s.io/utils/clock/testing"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
	"github.com/krateo-platformops/provider-runtime/pkg/logging"
)

// A holder's record keeps refs that its pass doesn't name: behind a hop that
// can't be followed (§9.1), and when a pass fails (§8). §11.3's gate must run
// for those in another namespace too, or a withdrawal of consent doesn't
// release them, for as long as the holder stays so.

// keptCase is a way a pass keeps refs it doesn't name. refs are what the
// holder names after it dropped otherSecret.
type keptCase struct {
	refs func(r *rig, fc *fakeContents) []Ref
	// fails: the pass is an Acquire error, rather than one that goes on.
	fails bool
}

func keptCases() map[string]keptCase {
	hop := func(next func(*unstructured.Unstructured) ([]Ref, error)) []Ref {
		r := targetRef("kt")
		r.Next = next
		return []Ref{r}
	}
	lead := func(refs ...Ref) func(*unstructured.Unstructured) ([]Ref, error) {
		return func(*unstructured.Unstructured) ([]Ref, error) { return refs, nil }
	}
	return map[string]keptCase{
		"a Missing hop": {refs: func(*rig, *fakeContents) []Ref { return hop(nextOf) }},
		"a Deleting hop": {fails: true, refs: func(r *rig, _ *fakeContents) []Ref {
			must(r.m.Create(targetKey("kt"), metav1.ObjectMeta{Finalizers: []string{"example.com/keep"}}))
			must(r.m.Delete(targetKey("kt")))
			return hop(nextOf)
		}},
		"a Next that fails": {fails: true, refs: func(r *rig, fc *fakeContents) []Ref {
			r.target(fc, "kt", "Secret", "home")
			return hop(func(*unstructured.Unstructured) ([]Ref, error) { return nil, errors.New("no secret") })
		}},
		"a Guaranteed ref that can't be read": {fails: true, refs: func(*rig, *fakeContents) []Ref {
			refs := hop(nextOf)
			refs[0].Guaranteed = true
			return refs
		}},
		"a chain that is too deep": {fails: true, refs: func(r *rig, fc *fakeContents) []Ref {
			for _, name := range []string{"kt", "kt2", "kt3", "kt4"} {
				r.target(fc, name, "Secret", "home")
			}
			deeper := func(name string, next ...Ref) Ref {
				out := targetRef(name)
				out.Next = lead(next...)
				return out
			}
			return hop(lead(deeper("kt2", deeper("kt3", deeper("kt4", SecretRef("ns", "home"))))))
		}},
		"a Next without Content": {fails: true, refs: func(*rig, *fakeContents) []Ref {
			s := SecretRef("ns", "home")
			s.Next = lead()
			return []Ref{s}
		}},
		"a ref of no name": {fails: true, refs: func(*rig, *fakeContents) []Ref { return []Ref{SecretRef("ns", "")} }},
		"a hop's A2 that fails": {fails: true, refs: func(r *rig, fc *fakeContents) []Ref {
			r.target(fc, "kt", "Secret", "home")
			failA2(r, "Target", "kt")
			return hop(nextOf)
		}},
		"a hop that can't be read": {fails: true, refs: func(r *rig, fc *fakeContents) []Ref {
			r.target(fc, "kt", "Secret", "home")
			r.p.contents = unreadable{}
			return hop(nextOf)
		}},
	}
}

// unreadable is a contentReader that reads nothing.
type unreadable struct{}

func (unreadable) Content(context.Context, schema.GroupKind, string, string) (*unstructured.Unstructured, error) {
	return nil, apierrors.NewServiceUnavailable("no")
}

// failA2 makes the A2 of one object fail.
func failA2(r *rig, kind, name string) {
	r.srv.before = func(op, k, n string) error {
		if op == "patch" && k == kind && n == name {
			return apierrors.NewServiceUnavailable("no")
		}
		return nil
	}
}

// TestKeptRefsInAnotherNamespaceAreGated: whatever keeps a cross-namespace ref
// in the record, its target's withdrawal of consent drops it, and the release
// takes its finalizer off (§11.3).
func TestKeptRefsInAnotherNamespaceAreGated(t *testing.T) {
	ctx := context.Background()
	// start is a rig with a holder that held otherSecret with consent.
	start := func(t *testing.T, established bool) (*rig, *fakeContents) {
		t.Helper()
		r, _ := consentRig(t, "")
		fc := &fakeContents{r: r, specs: map[ID]map[string]any{}}
		r.p.contentKinds = map[schema.GroupKind]bool{targetGK: true}
		r.p.contents = fc
		must(r.m.Create(secretKey("ns", "home"), metav1.ObjectMeta{}))
		must(r.m.Create(secretKey("other", "s"), metav1.ObjectMeta{Annotations: map[string]string{AnnotationAllowHoldersFrom: "ns"}}))
		if a, err := r.p.acquire(ctx, r.holder(t, "h", established), []Ref{otherSecret()}); err != nil || len(a.Held) != 1 || !held(r, t) {
			t.Fatalf("pass 1: acquire = %+v, %v", a, err)
		}
		return r, fc
	}
	pass := func(t *testing.T, r *rig, refs []Ref) Acquired {
		t.Helper()
		a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), refs)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	recorded := func(t *testing.T, r *rig) bool {
		t.Helper()
		return ReadRecord(r.get(t, widgetKey("ns", "h"))).Names(otherSecretID())
	}
	for name, tc := range keptCases() {
		for _, established := range []bool{true, false} {
			t.Run(name+map[bool]string{true: ", established", false: ", not established"}[established], func(t *testing.T) {
				r, fc := start(t, established)
				refs := tc.refs(r, fc)

				// The case itself: the pass keeps it, while consent stands.
				a := pass(t, r, refs)
				if stopped := a.Degraded || a.MustNotConnect; stopped != tc.fails {
					t.Fatalf("acquire = %+v; want a pass that fails: %v", a, tc.fails)
				}
				if !recorded(t, r) || !held(r, t) {
					t.Fatalf("record %v, held %v: the case doesn't keep the ref", ReadRecord(r.get(t, widgetKey("ns", "h"))).IDs, held(r, t))
				}

				setConsent(t, r, secretKey("other", "s"), "")
				for range 3 {
					pass(t, r, refs)
				}
				if recorded(t, r) {
					t.Errorf("record %v after 3 passes: the withdrawn ref is still recorded", ReadRecord(r.get(t, widgetKey("ns", "h"))).IDs)
				}
				if _, err := r.p.release(ctx, "ns", []ID{otherSecretID()}); err != nil {
					t.Fatal(err)
				}
				if held(r, t) {
					t.Error("the withdrawn ref is still held after a release")
				}
			})
		}
	}
	// Controls: without a case, the record drops the ref whatever its consent;
	// and a ref that the pass names is gated, as ever.
	t.Run("control: nothing keeps it", func(t *testing.T) {
		r, _ := start(t, true)
		if pass(t, r, nil); recorded(t, r) {
			t.Error("the record kept a ref that nothing names")
		}
	})
	t.Run("control: an A2 that fails at the last level keeps nothing, since its A1 was exact", func(t *testing.T) {
		r, _ := start(t, true)
		failA2(r, "Secret", "home")
		if a := pass(t, r, []Ref{SecretRef("ns", "home")}); !a.Degraded || recorded(t, r) {
			t.Errorf("acquire = %+v, record %v; want Degraded, and the ref dropped", a, ReadRecord(r.get(t, widgetKey("ns", "h"))).IDs)
		}
	})
	t.Run("control: named, and withdrawn", func(t *testing.T) {
		r, _ := start(t, true)
		setConsent(t, r, secretKey("other", "s"), "")
		if a := pass(t, r, append([]Ref{targetRef("kt")}, otherSecret())); recorded(t, r) || len(a.Unheld) != 2 {
			t.Errorf("acquire = %+v; want the Missing hop and the withdrawn ref unheld, and it dropped", a)
		}
		if _, err := r.p.release(ctx, "ns", []ID{otherSecretID()}); err != nil || held(r, t) {
			t.Errorf("release = %v, held %v; want it released", err, held(r, t))
		}
	})
}

// keptRig is a rig whose holder h, established or not, held otherSecret with
// consent, and no longer names it; its refs, Target kt, is a Missing hop, so
// the record keeps it.
func keptRig(t *testing.T, established bool) (*rig, *fakeContents, []Ref) {
	t.Helper()
	r, _ := consentRig(t, "")
	fc := &fakeContents{r: r, specs: map[ID]map[string]any{}}
	r.p.contentKinds = map[schema.GroupKind]bool{targetGK: true}
	r.p.contents = fc
	must(r.m.Create(secretKey("ns", "home"), metav1.ObjectMeta{}))
	must(r.m.Create(secretKey("other", "s"), metav1.ObjectMeta{Annotations: map[string]string{AnnotationAllowHoldersFrom: "ns"}}))
	if a, err := r.p.acquire(context.Background(), r.holder(t, "h", established), []Ref{otherSecret()}); err != nil || len(a.Held) != 1 {
		t.Fatalf("pass 1: acquire = %+v, %v", a, err)
	}
	return r, fc, []Ref{targetRef("kt")}
}

func keptPass(t *testing.T, r *rig, refs []Ref) Acquired {
	t.Helper()
	a, err := r.p.acquire(context.Background(), r.get(t, widgetKey("ns", "h")), refs)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func keptRecorded(t *testing.T, r *rig) bool {
	t.Helper()
	return ReadRecord(r.get(t, widgetKey("ns", "h"))).Names(otherSecretID())
}

func failingNext() []Ref {
	r := targetRef("kt")
	r.Next = func(*unstructured.Unstructured) ([]Ref, error) { return nil, errors.New("no secret") }
	return []Ref{r}
}

// TestTheGateOfKeptRefs: what the gate of a recorded ref reads, keeps and
// drops (§11.3).
func TestTheGateOfKeptRefs(t *testing.T) {
	ctx := context.Background()
	reads := func(r *rig) int {
		return countCalls(r.srv.calls, "get Namespace/other") + countCalls(r.srv.calls, "get Secret/s")
	}
	unreadable := func(r *rig) {
		r.srv.before = func(op, kind, name string) error {
			if op == "get" && kind == "Namespace" {
				return apierrors.NewServiceUnavailable("no")
			}
			return nil
		}
	}
	t.Run("consent that can't be read keeps it, and backs off once per pass", func(t *testing.T) {
		for _, named := range []bool{false, true} {
			for _, fails := range []bool{false, true} {
				r, fc, refs := keptRig(t, true)
				if fails {
					r.target(fc, "kt", "Secret", "home")
					refs = failingNext()
				}
				if named {
					refs = append(refs, otherSecret())
				}
				setConsent(t, r, secretKey("other", "s"), "")
				unreadable(r)
				var got []time.Duration
				for range 3 {
					a := keptPass(t, r, refs)
					if !keptRecorded(t, r) || !held(r, t) || a.Degraded != fails {
						t.Fatalf("named %v, fails %v: acquire = %+v, record kept %v", named, fails, a, keptRecorded(t, r))
					}
					got = append(got, a.RetryAfter)
				}
				// A read that answers starts it over; the consent it finds gone
				// is dropped.
				setConsent(t, r, secretKey("other", "s"), "ns")
				r.srv.before = nil
				got = append(got, keptPass(t, r, refs).RetryAfter)
				unreadable(r)
				got = append(got, keptPass(t, r, refs).RetryAfter)
				want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 0, time.Second}
				if !slices.Equal(got, want) {
					t.Errorf("named %v, fails %v: RetryAfter = %v, want %v", named, fails, got, want)
				}
			}
		}
	})
	t.Run("one that is gone is kept, as Missing", func(t *testing.T) {
		r, _, refs := keptRig(t, true)
		if _, err := r.m.Patch(secretKey("other", "s"), types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`)); err != nil {
			t.Fatal(err)
		}
		must(r.m.Delete(secretKey("other", "s")))
		keptPass(t, r, refs)
		r.srv.calls = nil
		if a := keptPass(t, r, refs); !keptRecorded(t, r) || a.RetryAfter != 0 || countCalls(r.srv.calls, "patch Widget/h") != 0 {
			t.Errorf("acquire = %+v, calls %v; want it kept, with nothing written", a, r.srv.calls)
		}
	})
	t.Run("a cached yes serves, on a deleting pass too", func(t *testing.T) {
		for _, deleting := range []bool{false, true} {
			r, _, refs := keptRig(t, true)
			r.views = cacheOf{m: r.m}
			r.p.namespaceViews = modelNamespaces{m: r.m, known: true}
			if deleting {
				must(r.m.Delete(widgetKey("ns", "h")))
			}
			r.srv.calls = nil
			keptPass(t, r, refs)
			if n := reads(r); n != 0 || !keptRecorded(t, r) {
				t.Errorf("deleting %v: %d live reads, recorded %v; want none, and it kept", deleting, n, keptRecorded(t, r))
			}
		}
	})
	t.Run("on a deleting pass, one that the pass names is read live before its A2", func(t *testing.T) {
		r, _, refs := keptRig(t, true)
		r.views = cacheOf{m: r.m}
		r.p.namespaceViews = modelNamespaces{m: r.m, known: true}
		must(r.m.Delete(widgetKey("ns", "h")))
		r.srv.calls = nil
		keptPass(t, r, append(refs, otherSecret()))
		if n := countCalls(r.srv.calls, "get Namespace/other"); n != 1 {
			t.Errorf("%d live reads of the Namespace, want 1: %v", n, r.srv.calls)
		}
	})
	t.Run("a cached no is confirmed live, and read once", func(t *testing.T) {
		for _, named := range []bool{false, true} {
			r, _, refs := keptRig(t, true)
			if named {
				refs = append(refs, otherSecret())
			}
			r.views = fixedProtected{id: otherSecretID()} // without the consent
			r.p.namespaceViews = modelNamespaces{m: r.m, known: true}
			r.srv.calls = nil
			keptPass(t, r, refs)
			if n := countCalls(r.srv.calls, "get Namespace/other"); n != 1 || !keptRecorded(t, r) {
				t.Errorf("named %v: %d live reads, recorded %v; want one, and it kept", named, n, keptRecorded(t, r))
			}
		}
	})
	t.Run("the refs of the holder's namespace, and of a Disowned kind, aren't asked about", func(t *testing.T) {
		r, fc, refs := keptRig(t, true)
		r.target(fc, "kt2", "Secret", "home")
		must(r.m.Create(apimodel.Key{GK: targetGK, Namespace: "other", Name: "kt"}, metav1.ObjectMeta{Annotations: map[string]string{AnnotationAllowHoldersFrom: "ns"}}))
		other := Ref{GroupKind: targetGK, Namespace: "other", Name: "kt"}
		if a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{targetRef("kt2"), other, otherSecret()}); err != nil || len(a.Held) != 4 {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
		// The switch covers Secrets, not Targets; consent is withdrawn of both.
		set := parseDisown("Secret")
		r.p.disowned = func(gk schema.GroupKind) bool { return set.covers(gk) }
		live := 0
		r.p.disownLive = func(context.Context) (disownSet, error) { live++; return set, nil }
		setConsent(t, r, secretKey("other", "s"), "")
		setConsent(t, r, apimodel.Key{GK: targetGK, Namespace: "other", Name: "kt"}, "")
		r.srv.calls = nil
		// The gate alone: its A1 is what a pass that then fails leaves.
		var a Acquired
		if _, err := r.p.gateRecorded(ctx, r.get(t, widgetKey("ns", "h")), &a); err != nil {
			t.Fatal(err)
		}
		if n := countCalls(r.srv.calls, "get Secret/s"); n != 0 {
			t.Errorf("%d reads of a Disowned kind's ref", n)
		}
		if len(r.rep.withdrawn) != 1 || r.rep.withdrawn[0] != other.ID() {
			t.Errorf("withdrawn = %v, want only the Target", r.rep.withdrawn)
		}
		if rec := ReadRecord(r.get(t, widgetKey("ns", "h"))); rec.Names(other.ID()) || rec.Names(otherSecretID()) || !rec.Names(targetID("kt2")) {
			t.Errorf("record %v: want the withdrawn Target gone, and the Disowned Secret out of every A1", rec.IDs)
		}
		if live != 0 {
			t.Errorf("%d CRD reads for the gate, want none", live)
		}
		// All kinds: nothing is asked, and the CRD isn't read for it.
		set, live = parseDisown("all"), 0
		r.srv.calls = nil
		keptPass(t, r, refs)
		if reads(r) != 0 || live != 0 {
			t.Errorf("%d reads, %d CRD reads under a switch for all", reads(r), live)
		}
	})
	t.Run("an unparseable record names nothing to ask about, and isn't written for it", func(t *testing.T) {
		r, fc, _ := keptRig(t, true)
		if _, err := r.m.Patch(widgetKey("ns", "h"), types.MergePatchType, []byte(`{"metadata":{"annotations":{"`+AnnotationRefs+`":"oops"}}}`)); err != nil {
			t.Fatal(err)
		}
		setConsent(t, r, secretKey("other", "s"), "")
		r.target(fc, "kt", "Secret", "home")
		r.srv.calls = nil
		var a Acquired
		if _, err := r.p.gateRecorded(ctx, r.get(t, widgetKey("ns", "h")), &a); err != nil || len(r.srv.calls) != 0 {
			t.Errorf("gateRecorded = %v, calls %v; want nothing done", err, r.srv.calls)
		}
	})
}

// TestTheGateOfKeptRefsWritesOneA1: what it drops goes in one A1, guarded as
// any, with its unheld entry.
func TestTheGateOfKeptRefsWritesOneA1(t *testing.T) {
	ctx := context.Background()
	t.Run("one A1 for all it drops, and the event once for each", func(t *testing.T) {
		for _, named := range []bool{false, true} {
			r, _, refs := keptRig(t, true)
			s2 := SecretRef("other", "s2")
			must(r.m.Create(secretKey("other", "s2"), metav1.ObjectMeta{Annotations: map[string]string{AnnotationAllowHoldersFrom: "ns"}}))
			if a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{otherSecret(), s2}); err != nil || len(a.Held) != 2 {
				t.Fatalf("acquire = %+v, %v", a, err)
			}
			keptPass(t, r, refs) // the hop, recorded
			setConsent(t, r, secretKey("other", "s"), "")
			setConsent(t, r, secretKey("other", "s2"), "")
			if named {
				refs = append(refs, otherSecret(), s2)
			}
			r.srv.calls = nil
			a := keptPass(t, r, refs)
			if n := countCalls(r.srv.calls, "patch Widget/h"); n != 1 || len(r.rep.withdrawn) != 2 || len(unheldOf(t, r, "h")) != 2 {
				t.Errorf("named %v: %d holder writes, withdrawn %v, unheld %+v; want one write, and two of each", named, n, r.rep.withdrawn, unheldOf(t, r, "h"))
			}
			unheld := 1 // the hop
			if named {
				unheld = 3
			}
			if len(a.Unheld) != unheld {
				t.Errorf("named %v: Unheld = %+v", named, a.Unheld)
			}
		}
	})
	t.Run("a conflict ends the pass, and drops nothing", func(t *testing.T) {
		r, _, refs := keptRig(t, true)
		setConsent(t, r, secretKey("other", "s"), "")
		stale := r.get(t, widgetKey("ns", "h"))
		// A Forget in flight on another pass: its write lands first.
		if _, err := r.m.Patch(widgetKey("ns", "h"), types.MergePatchType, []byte(`{"metadata":{"labels":{"changed":"yes"}}}`)); err != nil {
			t.Fatal(err)
		}
		r.srv.calls = nil
		if _, err := r.p.acquire(ctx, stale, refs); !errors.Is(err, ErrHolderConflict) {
			t.Errorf("acquire = %v, want ErrHolderConflict", err)
		}
		if n := countCalls(r.srv.calls, "patch Widget/h"); n != 1 || countCalls(r.srv.calls, "patch Target/kt") != 0 {
			t.Errorf("calls = %v; want the pass to end at the A1", r.srv.calls)
		}
		if len(r.rep.withdrawn) != 0 {
			t.Errorf("reported %v, which wasn't dropped", r.rep.withdrawn)
		}
	})
	t.Run("another failure of it is an Acquire error", func(t *testing.T) {
		for _, established := range []bool{true, false} {
			r, _, refs := keptRig(t, established)
			setConsent(t, r, secretKey("other", "s"), "")
			r.srv.before = func(op, kind, _ string) error {
				if op == "patch" && kind == "Widget" {
					return apierrors.NewServiceUnavailable("no")
				}
				return nil
			}
			if a := keptPass(t, r, refs); a.Degraded != established || a.MustNotConnect == established || a.Err == nil {
				t.Errorf("established %v: acquire = %+v", established, a)
			}
		}
	})
	t.Run("no skip rests on what it dropped", func(t *testing.T) {
		r, _, _ := keptRig(t, true)
		r.views = cacheOf{m: r.m}
		r.p.namespaceViews = modelNamespaces{m: r.m, known: true}
		home := SecretRef("ns", "home")
		steady := func() int {
			t.Helper()
			h := r.get(t, widgetKey("ns", "h"))
			r.p.ver.holderEvent(h.UID, h, r.now)
			r.srv.calls = nil
			keptPass(t, r, []Ref{home, otherSecret()})
			return countCalls(r.srv.calls, "patch Secret/s")
		}
		steady()
		r.tick(time.Second)
		if n := steady(); n != 0 {
			t.Fatalf("%d A2s of s in a steady pass: the rig doesn't skip", n)
		}
		setConsent(t, r, secretKey("other", "s"), "")
		keptPass(t, r, nil) // named by nothing now: dropped
		if keptRecorded(t, r) {
			t.Fatal("not dropped")
		}
		setConsent(t, r, secretKey("other", "s"), "ns")
		if n := steady(); n != 1 {
			t.Errorf("%d A2s of s once it is held again, want 1: a skip rested on the dropped ref", n)
		}
	})
}

// TestTheUnheldListOfKeptRefs: a dropped ref stays listed as ConsentWithdrawn
// while the pass can't follow the whole chain, or fails: the holder may still
// read through to it. It goes once a pass follows the whole chain and doesn't
// name it, or holds it again.
func TestTheUnheldListOfKeptRefs(t *testing.T) {
	want := UnheldEntry{Ref: "other/Secret/s", Reason: UnheldConsentWithdrawn, Since: t0.Add(time.Second).UTC().Truncate(time.Second)}
	for _, fails := range []bool{false, true} {
		r, fc, refs := keptRig(t, true)
		// Another entry, of a ref the pass names after the one that fails.
		must(r.m.Create(secretKey("other", "n"), metav1.ObjectMeta{}))
		n := SecretRef("other", "n")
		if a := keptPass(t, r, []Ref{targetRef("kt"), n}); len(a.Unheld) != 2 || !keptRecorded(t, r) {
			t.Fatalf("acquire = %+v", a)
		}
		if fails {
			r.target(fc, "kt", "Secret", "home")
			refs = failingNext()
		}
		refs = append(refs, n)
		setConsent(t, r, secretKey("other", "s"), "")
		r.tick(time.Second)
		for pass := range 3 {
			r.srv.calls = nil
			keptPass(t, r, refs)
			r.tick(time.Minute)
			u := unheldOf(t, r, "h")
			if len(u) != 2 || !slices.Contains(u, want) || !slices.ContainsFunc(u, func(e UnheldEntry) bool { return e.Reason == UnheldNotConsented }) {
				t.Fatalf("fails %v, pass %d: unheld = %+v; want s withdrawn since the first pass, and n's entry kept", fails, pass, u)
			}
			if w := countCalls(r.srv.calls, "patch Widget/h"); pass > 0 && w != 0 {
				t.Errorf("fails %v, pass %d: %d holder writes, want none", fails, pass, w)
			}
		}
		if len(r.rep.withdrawn) != 1 {
			t.Errorf("fails %v: ConsentWithdrawn reported %d times", fails, len(r.rep.withdrawn))
		}
		// Held again, when the holder names it and the owner consents.
		setConsent(t, r, secretKey("other", "s"), "ns")
		keptPass(t, r, append(slices.Clone(refs), otherSecret()))
		if fails {
			refs = []Ref{targetRef("kt"), n, otherSecret()} // a pass that doesn't fail
			keptPass(t, r, refs)
		}
		if u := unheldOf(t, r, "h"); len(u) != 1 || u[0].Reason != UnheldNotConsented {
			t.Errorf("fails %v: unheld = %+v once s is held again", fails, u)
		}
	}
	t.Run("it goes once a pass follows the whole chain", func(t *testing.T) {
		r, fc, refs := keptRig(t, true)
		setConsent(t, r, secretKey("other", "s"), "")
		keptPass(t, r, refs)
		if len(unheldOf(t, r, "h")) != 1 {
			t.Fatalf("unheld = %+v", unheldOf(t, r, "h"))
		}
		r.target(fc, "kt", "Secret", "home")
		if a := keptPass(t, r, refs); len(a.Held) != 2 || len(unheldOf(t, r, "h")) != 0 {
			t.Errorf("acquire = %+v, unheld %+v; want none", a, unheldOf(t, r, "h"))
		}
	})
	t.Run("a pass that keeps nothing lists nothing", func(t *testing.T) {
		r, _, _ := keptRig(t, true)
		setConsent(t, r, secretKey("other", "s"), "")
		keptPass(t, r, nil)
		if u := unheldOf(t, r, "h"); keptRecorded(t, r) || len(u) != 0 {
			t.Errorf("recorded %v, unheld %+v; want neither", keptRecorded(t, r), u)
		}
	})
	t.Run("an entry of the dropped ref is replaced", func(t *testing.T) {
		r, _, _ := keptRig(t, true)
		found := []UnheldEntry{{Ref: unheldRef(otherSecretID()), Reason: UnheldNotConsented, Since: t0.Add(-time.Hour)}}
		if u := r.p.listWithdrawn(found, []ID{otherSecretID()}); len(u) != 1 || u[0].Reason != UnheldConsentWithdrawn || u[0].Since.Before(t0.Truncate(time.Second)) {
			t.Errorf("listed %+v; want it once, withdrawn now", u)
		}
	})
	t.Run("the count of a list past its cap survives a failing pass", func(t *testing.T) {
		var found []UnheldEntry
		for i := range maxUnheld - 1 {
			found = append(found, UnheldEntry{Ref: string(rune('a' + i)), Reason: UnheldNotConsented, Since: t0.UTC()})
		}
		found = append(found, UnheldEntry{Ref: "…", Reason: unheldMore, Count: 7})
		r, _, _ := keptRig(t, true)
		v, err := encodeUnheld(r.p.listWithdrawn(found, []ID{otherSecretID()}))
		if err != nil {
			t.Fatal(err)
		}
		u := ReadUnheld(map[string]string{AnnotationUnheld: *v})
		if len(u) != maxUnheld || u[0].Reason != UnheldConsentWithdrawn || u[len(u)-1].Count != 8 {
			t.Errorf("unheld = %+v; want %d entries, s's first, and 8 more", u, maxUnheld)
		}
	})
}

// TestAPassThatNamesNothingGatesKeptRefs: a Refs that fails, and a holder in
// the index namespace, end a pass before acquire; the gate runs in those too
// (§11.3).
func TestAPassThatNamesNothingGatesKeptRefs(t *testing.T) {
	ctx := context.Background()
	tracker := func(r *rig) *Tracker {
		return &Tracker{
			s: newShared(Clients{}, SharedOptions{}, testingclock.NewFakeClock(t0)), gk: widgetGK, proto: r.p, missing: newMissingRefs(),
			o:         Options{OwnerFinalizers: ownerFinalizers, Refs: func(metav1.Object) ([]Ref, error) { return nil, errors.New("no refs") }},
			misplaced: newReportedSet(), degradedLogged: newReportedSet(), sink: sinks{logger: logging.NewNopLogger(), metrics: NopMetrics{}},
		}
	}
	for name, misplaced := range map[string]bool{"a Refs that fails": false, "a holder in the index namespace": true} {
		for _, established := range []bool{true, false} {
			r, _, _ := keptRig(t, established)
			tr := tracker(r)
			if misplaced {
				tr.idx = testIndex(&memStore{}, nil)
				tr.idx.namespace = "ns"
				tr.o.Refs = func(metav1.Object) ([]Ref, error) { return nil, nil }
			}
			pass := func() (Acquired, error) { return tr.Acquire(ctx, r.get(t, widgetKey("ns", "h"))) }
			if a, err := pass(); err != nil || a.Err == nil || a.Degraded != established || !keptRecorded(t, r) {
				t.Fatalf("%s, established %v: Acquire = %+v, %v", name, established, a, err)
			}
			setConsent(t, r, secretKey("other", "s"), "")
			stale := r.get(t, widgetKey("ns", "h"))
			a, err := pass()
			if err != nil || a.Err == nil || a.Degraded != established || keptRecorded(t, r) || len(unheldOf(t, r, "h")) != 1 {
				t.Errorf("%s, established %v: Acquire = %+v, %v, unheld %+v; want it dropped, and listed", name, established, a, err, unheldOf(t, r, "h"))
			}
			if _, err := tr.Acquire(ctx, stale); !errors.Is(err, ErrHolderConflict) {
				t.Errorf("%s, established %v: Acquire of a stale holder = %v, want ErrHolderConflict", name, established, err)
			}
		}
	}
}

// TestAKeptRefInTheHoldersNamespaceIsKept: a kept ref in the holder's own
// namespace needs no consent, and stays kept.
func TestAKeptRefInTheHoldersNamespaceIsKept(t *testing.T) {
	ctx := context.Background()
	r, _ := consentRig(t, "")
	fc := &fakeContents{r: r, specs: map[ID]map[string]any{}}
	r.p.contentKinds = map[schema.GroupKind]bool{targetGK: true}
	r.p.contents = fc
	r.target(fc, "kt", "Secret", "home")
	must(r.m.Create(secretKey("ns", "home"), metav1.ObjectMeta{}))
	if a, err := r.p.acquire(ctx, r.holder(t, "h", true), []Ref{targetRef("kt")}); err != nil || len(a.Held) != 2 {
		t.Fatalf("pass 1: acquire = %+v, %v", a, err)
	}
	if _, err := r.m.Patch(targetKey("kt"), types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`)); err != nil {
		t.Fatal(err)
	}
	must(r.m.Delete(targetKey("kt")))
	for range 3 {
		if _, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{targetRef("kt")}); err != nil {
			t.Fatal(err)
		}
	}
	if rec := ReadRecord(r.get(t, widgetKey("ns", "h"))); !slices.Contains(rec.IDs, SecretRef("ns", "home").ID()) {
		t.Errorf("record %v: the Secret behind the Missing hop was dropped", rec.IDs)
	}
}
