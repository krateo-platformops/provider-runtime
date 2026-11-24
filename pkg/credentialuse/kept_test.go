package credentialuse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
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
			// The hop, and both dropped refs, once each, whether or not the
			// pass names them.
			var got []ID
			for _, u := range a.Unheld {
				if u.Reason == ReasonConsentWithdrawn {
					got = append(got, u.Ref.ID())
				}
			}
			if !slices.Equal(SortedIDs(got), []ID{otherSecretID(), s2.ID()}) || len(a.Unheld) != 3 {
				t.Errorf("named %v: Unheld = %+v", named, a.Unheld)
			}
		}
	})
	t.Run("Unheld has the pass's own Ref for one it names", func(t *testing.T) {
		r, fc, _ := keptRig(t, true)
		r.target(fc, "kt", "Secret", "home")
		setConsent(t, r, secretKey("other", "s"), "")
		r.p.contentKinds[secretGroupKind] = true // a Next needs it
		ref := otherSecret()
		ref.Next = func(*unstructured.Unstructured) ([]Ref, error) { return nil, nil }
		a := keptPass(t, r, []Ref{ref})
		if len(a.Unheld) != 1 || a.Unheld[0].Ref.ID() != otherSecretID() || a.Unheld[0].Ref.Next == nil {
			t.Errorf("Unheld = %+v", a.Unheld)
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
			r.srv.calls = nil
			if a := keptPass(t, r, refs); a.Degraded != established || a.MustNotConnect == established || a.Err == nil || len(a.Unheld) != 0 || len(r.rep.withdrawn) != 0 {
				t.Errorf("established %v: acquire = %+v, reported %v", established, a, r.rep.withdrawn)
			}
			if n := countCalls(r.srv.calls, "patch Widget/h"); n != 1 {
				t.Errorf("established %v: %d holder writes, want one: only a refusal for its size is tried again", established, n)
			}
		}
	})
	t.Run("refused for its size, it is tried again without the unheld list", func(t *testing.T) {
		for name, refusal := range map[string]error{
			"a 422": apierrors.NewInvalid(widgetGK, "h", nil),
			"a 413": apierrors.NewRequestEntityTooLargeError("too large"),
		} {
			r, _, refs := keptRig(t, true)
			keptPass(t, r, refs) // the hop, recorded
			setConsent(t, r, secretKey("other", "s"), "")
			refused := 0
			r.srv.before = func(op, kind, _ string) error {
				if op == "patch" && kind == "Widget" && refused == 0 {
					refused++
					return refusal
				}
				return nil
			}
			a := keptPass(t, r, refs)
			if refused != 1 || a.Degraded || keptRecorded(t, r) || len(r.rep.withdrawn) != 1 {
				t.Errorf("%s: acquire = %+v, recorded %v, reported %v; want it dropped", name, a, keptRecorded(t, r), r.rep.withdrawn)
			}
			if u := unheldOf(t, r, "h"); len(u) != 0 {
				t.Errorf("%s: unheld = %+v, which couldn't be written", name, u)
			}
			if len(a.Unheld) != 2 {
				t.Errorf("%s: Unheld = %+v; want it reported all the same", name, a.Unheld)
			}
		}
	})
	t.Run("nothing is dropped once ctx has ended", func(t *testing.T) {
		r, _, refs := keptRig(t, true)
		r.p.backoff = newConsentBackoff(time.Minute)
		s2 := SecretRef("other", "t")
		must(r.m.Create(secretKey("other", "t"), metav1.ObjectMeta{Annotations: map[string]string{AnnotationAllowHoldersFrom: "ns"}}))
		if a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{otherSecret(), s2}); err != nil || len(a.Held) != 2 {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
		setConsent(t, r, secretKey("other", "s"), "")
		setConsent(t, r, secretKey("other", "t"), "")
		ended, end := context.WithCancel(ctx)
		r.srv.before = func(op, kind, name string) error {
			if op == "get" && kind == "Secret" && name == "t" {
				end() // after s was found withdrawn
				return context.Canceled
			}
			return nil
		}
		r.srv.calls = nil
		a, err := r.p.acquire(ended, r.get(t, widgetKey("ns", "h")), refs)
		if err != nil || !a.Degraded || !errors.Is(a.Err, context.Canceled) || a.RetryAfter != 0 {
			t.Errorf("acquire = %+v, %v; want it to fail for its context", a, err)
		}
		if n := countCalls(r.srv.calls, "patch Widget/h"); n != 0 || !keptRecorded(t, r) || len(r.rep.withdrawn) != 0 {
			t.Errorf("%d holder writes, recorded %v, reported %v; want nothing dropped", n, keptRecorded(t, r), r.rep.withdrawn)
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

// forgeRecord writes a record naming ids on holder h, as whoever can edit the
// holder can.
func forgeRecord(t *testing.T, r *rig, ids []ID) {
	t.Helper()
	v, err := EncodeRecord(SortedIDs(ids))
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": map[string]any{AnnotationRefs: v}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.m.Patch(widgetKey("ns", "h"), types.MergePatchType, data); err != nil {
		t.Fatal(err)
	}
}

func manySecrets(ns string, n int) []ID {
	ids := make([]ID, n)
	for i := range ids {
		ids[i] = SecretRef(ns, fmt.Sprintf("s%05d", i)).ID()
	}
	return ids
}

// TestAForgedRecordCostsABoundedNumberOfReads: whoever can edit a holder
// writes its record, so what it names mustn't cost reads without bound, or
// tell what exists elsewhere (§11.3).
func TestAForgedRecordCostsABoundedNumberOfReads(t *testing.T) {
	ctx := context.Background()
	gets := func(r *rig) (objects, namespaces int) {
		for _, c := range r.srv.calls {
			switch {
			case strings.HasPrefix(c, "get Secret/"):
				objects++
			case strings.HasPrefix(c, "get Namespace/"):
				namespaces++
			}
		}
		return objects, namespaces
	}
	failing := failingNext // a pass that keeps its record
	t.Run("at most maxGateReads a pass, and its Namespace once", func(t *testing.T) {
		r, fc, _ := keptRig(t, true)
		r.target(fc, "kt", "Secret", "home")
		r.p.gateTurns = newGateTurns()
		r.p.backoff = newConsentBackoff(time.Minute)
		ids := manySecrets("other", 4000)
		for _, id := range ids[:200] {
			must(r.m.Create(secretKey("other", id.Name), metav1.ObjectMeta{Annotations: map[string]string{AnnotationAllowHoldersFrom: "ns"}}))
		}
		forgeRecord(t, r, ids)
		var retries []time.Duration
		for pass := range 4 {
			r.srv.calls = nil
			a := keptPass(t, r, failing())
			o, n := gets(r)
			if o != maxGateReads || n != 1 {
				t.Errorf("pass %d: %d reads of Secrets and %d of Namespaces, want %d and 1", pass, o, n, maxGateReads)
			}
			retries = append(retries, a.RetryAfter)
		}
		if want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}; !slices.Equal(retries, want) {
			t.Errorf("RetryAfter = %v, want %v", retries, want)
		}
		if rec := ReadRecord(r.get(t, widgetKey("ns", "h"))); len(rec.IDs) < len(ids) {
			t.Errorf("%d refs recorded, of %d: a ref it didn't read, or that is gone or consents, was dropped", len(rec.IDs), len(ids))
		}
	})
	t.Run("each gets its turn, the longest unread first", func(t *testing.T) {
		r, fc, _ := keptRig(t, true)
		r.target(fc, "kt", "Secret", "home")
		r.p.gateTurns = newGateTurns()
		ids := manySecrets("other", 2*maxGateReads+3)
		for _, id := range ids {
			must(r.m.Create(secretKey("other", id.Name), metav1.ObjectMeta{Annotations: map[string]string{AnnotationAllowHoldersFrom: "ns"}}))
		}
		forgeRecord(t, r, ids)
		read := map[string]int{}
		for range 3 {
			r.srv.calls = nil
			keptPass(t, r, failing())
			for _, c := range r.srv.calls {
				if name, ok := strings.CutPrefix(c, "get Secret/"); ok {
					read[name]++
				}
			}
		}
		for _, id := range ids {
			if read[id.Name] == 0 {
				t.Errorf("%s wasn't read in 3 passes", id.Name)
			}
		}
		// A withdrawal is found within as many passes.
		last := ids[len(ids)-1]
		setConsent(t, r, secretKey("other", last.Name), "")
		for range 3 {
			keptPass(t, r, failing())
		}
		if rec := ReadRecord(r.get(t, widgetKey("ns", "h"))); rec.Names(last) {
			t.Errorf("%s is still recorded 3 passes after its withdrawal", last)
		}
		// Down to the bound, all are read, and no retry is asked for.
		forgeRecord(t, r, ids[:maxGateReads])
		if a := keptPass(t, r, failing()); a.RetryAfter != 0 {
			t.Errorf("RetryAfter = %v with no more than one pass reads", a.RetryAfter)
		}
		// With nothing to ask about, nothing is remembered.
		forgeRecord(t, r, nil)
		if keptPass(t, r, failing()); len(r.p.gateTurns.at) != 0 {
			t.Errorf("turns = %v with nothing to ask about", r.p.gateTurns.at)
		}
	})
	t.Run("the retry for the rest starts over once none is left", func(t *testing.T) {
		r, fc, _ := keptRig(t, true)
		r.target(fc, "kt", "Secret", "home")
		r.p.backoff = newConsentBackoff(time.Minute)
		ids := manySecrets("other", maxGateReads+1)
		var got []time.Duration
		for _, n := range []int{maxGateReads + 1, maxGateReads + 1, maxGateReads, maxGateReads + 1} {
			forgeRecord(t, r, ids[:n])
			got = append(got, keptPass(t, r, failing()).RetryAfter)
		}
		if want := []time.Duration{time.Second, 2 * time.Second, 0, time.Second}; !slices.Equal(got, want) {
			t.Errorf("RetryAfter = %v, want %v", got, want)
		}
	})
	t.Run("kinds that aren't protected aren't read", func(t *testing.T) {
		r, fc, _ := keptRig(t, true)
		r.target(fc, "kt", "Secret", "home")
		r.p.protects = func(gk schema.GroupKind) bool { return gk == secretGroupKind }
		var ids []ID
		for i := range 100 {
			ids = append(ids, ID{Kind: "ConfigMap", Namespace: "other", Name: fmt.Sprint(i)}, ID{Group: targetGK.Group, Kind: targetGK.Kind, Namespace: "other", Name: fmt.Sprint(i)})
		}
		forgeRecord(t, r, append(ids, otherSecretID()))
		r.srv.calls = nil
		keptPass(t, r, failing())
		for _, c := range r.srv.calls {
			if strings.HasPrefix(c, "get ConfigMap") || strings.HasPrefix(c, "get Target") && c != "get Target/kt" {
				t.Errorf("read %q", c)
			}
		}
		if n := countCalls(r.srv.calls, "get Secret/s"); n != 1 || !ReadRecord(r.get(t, widgetKey("ns", "h"))).Names(ids[0]) {
			t.Errorf("%d reads of the protected kind's ref, want 1, and the others kept", n)
		}
	})
	t.Run("a cluster-scoped object isn't asked about", func(t *testing.T) {
		r, fc, _ := keptRig(t, true)
		r.target(fc, "kt", "Secret", "home")
		id := ID{Group: targetGK.Group, Kind: targetGK.Kind, Name: "cluster"}
		forgeRecord(t, r, []ID{id})
		r.srv.calls = nil
		a := keptPass(t, r, failing())
		for _, c := range r.srv.calls {
			if strings.HasPrefix(c, "get Namespace") || c == "get Target/cluster" {
				t.Errorf("read %q", c)
			}
		}
		if a.RetryAfter != 0 || !ReadRecord(r.get(t, widgetKey("ns", "h"))).Names(id) {
			t.Errorf("acquire = %+v; want it kept", a)
		}
	})
	t.Run("what its cache has synced and lacks is kept, and not read", func(t *testing.T) {
		r, fc, _ := keptRig(t, true)
		r.target(fc, "kt", "Secret", "home")
		r.p.cacheLacks = func(id ID) bool { return id.Name != "s" }
		ids := manySecrets("other", 4000)
		must(r.m.Create(secretKey("other", ids[0].Name), metav1.ObjectMeta{})) // exists, unheld, without consent
		forgeRecord(t, r, append(ids, otherSecretID()))
		r.srv.calls = nil
		a := keptPass(t, r, failing())
		o, n := gets(r)
		if o != 1 || n != 1 || a.RetryAfter != 0 {
			t.Errorf("%d reads of Secrets, %d of Namespaces, RetryAfter %v; want only s read", o, n, a.RetryAfter)
		}
		rec := ReadRecord(r.get(t, widgetKey("ns", "h")))
		if !rec.Names(ids[0]) || !rec.Names(ids[1]) {
			t.Error("one that exists was told apart from one that doesn't")
		}
		// One that the pass names is read, as ever.
		r.srv.calls = nil
		keptPass(t, r, []Ref{{GroupKind: secretGroupKind, Namespace: "other", Name: ids[0].Name}})
		if n := countCalls(r.srv.calls, "get Secret/"+ids[0].Name); n != 1 {
			t.Errorf("%d reads of a ref the pass names, want 1", n)
		}
	})
	t.Run("another holder's pass costs what it did", func(t *testing.T) {
		r, fc, _ := keptRig(t, true)
		r.target(fc, "kt", "Secret", "home")
		r.p.gateTurns = newGateTurns()
		other := func() []string {
			r.srv.calls = nil
			if a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h2")), []Ref{otherSecret()}); err != nil || len(a.Held) != 1 || a.RetryAfter != 0 {
				t.Fatalf("acquire = %+v, %v", a, err)
			}
			return slices.Clone(r.srv.calls)
		}
		r.holder(t, "h2", true)
		other()
		before := other()
		forgeRecord(t, r, manySecrets("other", 4000))
		keptPass(t, r, failing())
		if after := other(); !slices.Equal(after, before) {
			t.Errorf("calls = %v, were %v", after, before)
		}
	})
	t.Run("a holder that is gone leaves no turns", func(t *testing.T) {
		turns := newGateTurns()
		ids := manySecrets("other", 40)
		turns.next("u1", ids, maxGateReads)
		turns.next("u1", ids[20:], maxGateReads) // the first 20 aren't recorded any more
		if n := len(turns.at["u1"]); n != 20 {
			t.Errorf("%d refs remembered, want those still asked about: 20", n)
		}
		turns.forget("u1")
		if len(turns.at) != 0 {
			t.Errorf("turns = %v after forget", turns.at)
		}
	})
}

// TestAForgedRecordCantStarveARef: whoever edits the record can't keep the
// gate from a ref in it by what else they put there (§11.3).
func TestAForgedRecordCantStarveARef(t *testing.T) {
	ctx := context.Background()
	// world is a holder whose record keeps s, whose consent is withdrawn.
	world := func(t *testing.T) *rig {
		t.Helper()
		r, fc, _ := keptRig(t, true)
		r.target(fc, "kt", "Secret", "home")
		r.p.gateTurns = newGateTurns()
		setConsent(t, r, secretKey("other", "s"), "")
		return r
	}
	fresh := func(prefix string, n int) []ID { // all sort before s
		ids := make([]ID, n)
		for i := range ids {
			ids[i] = SecretRef("other", fmt.Sprintf("a%s-%05d", prefix, i)).ID()
		}
		return ids
	}
	passes := func(t *testing.T, r *rig, record func(pass int) []ID) int {
		t.Helper()
		for pass := 1; pass <= 10; pass++ {
			forgeRecord(t, r, append(record(pass), otherSecretID()))
			keptPass(t, r, failingNext())
			if !keptRecorded(t, r) {
				return pass
			}
		}
		return 0
	}
	t.Run("16 fresh refs before every pass", func(t *testing.T) {
		if got := passes(t, world(t), func(pass int) []ID { return fresh(fmt.Sprint(pass), maxGateReads) }); got != 2 {
			t.Errorf("s was read on pass %d, want the second", got)
		}
	})
	t.Run("16 more before every pass", func(t *testing.T) {
		var all []ID
		if got := passes(t, world(t), func(pass int) []ID {
			all = append(all, fresh(fmt.Sprint(pass), maxGateReads)...)
			return slices.Clone(all)
		}); got != 2 {
			t.Errorf("s was read on pass %d, want the second", got)
		}
	})
	t.Run("refs dropped from the record, and returning", func(t *testing.T) {
		if got := passes(t, world(t), func(pass int) []ID { return fresh(fmt.Sprint(pass%2), maxGateReads) }); got != 2 {
			t.Errorf("s was read on pass %d, want the second", got)
		}
	})
	t.Run("as many as came before it, whatever comes after", func(t *testing.T) {
		before := fresh("", 40)
		if got := passes(t, world(t), func(pass int) []ID {
			return append(slices.Clone(before), fresh(fmt.Sprint("x", pass), maxGateReads)...)
		}); got != 3 { // 40 and s: ⌈41/16⌉, the first pass's fresh ones being gone
			t.Errorf("s was read on pass %d, want the third", got)
		}
	})
	// cut runs n passes, in each of which the gate's deadline, or with
	// wholePass the pass's, comes on the read of a Secret that at says. It
	// returns how often each was answered.
	cut := func(t *testing.T, r *rig, wholePass bool, record func(pass int) []ID, at func(nth int, name string) bool, n int) map[string]int {
		t.Helper()
		read := map[string]int{}
		for pass := 1; pass <= n; pass++ {
			forgeRecord(t, r, record(pass))
			c, end := context.WithCancel(ctx)
			endReads := end
			reads := c
			if !wholePass {
				r.p.gateContext = func(ctx context.Context) (context.Context, context.CancelFunc) {
					reads, endReads = context.WithCancel(ctx)
					return reads, endReads
				}
			}
			nth := 0
			r.srv.before = func(op, kind, name string) error {
				if op != "get" || kind != "Secret" || name == "home" {
					return nil
				}
				if nth++; at(nth, name) {
					endReads()
				}
				if reads.Err() != nil {
					return reads.Err()
				}
				read[name]++
				return nil
			}
			if _, err := r.p.acquire(c, r.get(t, widgetKey("ns", "h")), failingNext()); err != nil {
				t.Fatal(err)
			}
			endReads()
			end()
		}
		r.srv.before, r.p.gateContext = nil, nil
		return read
	}
	t.Run("a deadline that comes before its reads end", func(t *testing.T) {
		// On the fourth read of each pass: three answers a pass, however many
		// refs there are. Were a cut read always moved back, then with 3 more
		// than a multiple of 4 the same refs would be cut on every pass.
		for _, wholePass := range []bool{false, true} {
			for _, total := range []int{7, 10, maxGateReads, 19, 2*maxGateReads + 3} {
				r := world(t)
				ids := fresh("", total)
				n := (total + 2) / 3
				read := cut(t, r, wholePass, func(int) []ID { return ids }, func(nth int, _ string) bool { return nth == 4 }, n)
				for _, id := range ids {
					if read[id.Name] == 0 {
						t.Errorf("of %d: %s wasn't read in %d passes", total, id.Name, n)
					}
				}
			}
		}
	})
	t.Run("a ref whose read hangs", func(t *testing.T) {
		for _, total := range []int{5, maxGateReads + 4} {
			r := world(t)
			ids := fresh("", total)
			slow := ids[0].Name
			read := cut(t, r, false, func(int) []ID { return ids }, func(_ int, name string) bool { return name == slow }, 4)
			for _, id := range ids[1:] {
				if read[id.Name] == 0 {
					t.Errorf("of %d: %s wasn't read in 4 passes, of which the hung one may take all of two", total, id.Name)
				}
			}
		}
	})
	t.Run("a withdrawn ref, with a hung one before it or behind it", func(t *testing.T) {
		for _, before := range []bool{false, true} {
			r := world(t)
			r.p.srv = ctxServer{r.p.srv}
			hung := SecretRef("other", map[bool]string{true: "a", false: "t"}[before]).ID()
			passes := cut(t, r, false, func(int) []ID { return []ID{hung, otherSecretID()} }, func(_ int, name string) bool { return name == hung.Name }, 1)
			if recorded := keptRecorded(t, r); recorded != before {
				t.Errorf("before %v: s recorded %v after one pass (%v)", before, recorded, passes)
			}
			cut(t, r, false, func(int) []ID { return []ID{hung, otherSecretID()} }, func(_ int, name string) bool { return name == hung.Name }, 1)
			if before && keptRecorded(t, r) {
				t.Error("s wasn't dropped by the second pass")
			}
		}
	})
	t.Run("a pass goes on after the gate's deadline", func(t *testing.T) {
		r := world(t)
		r.p.gateContext = func(ctx context.Context) (context.Context, context.CancelFunc) {
			ended, end := context.WithCancel(ctx)
			end()
			return ended, end
		}
		a := keptPass(t, r, []Ref{SecretRef("ns", "home")})
		if a.Degraded || a.Err != nil || len(a.Held) != 1 || a.RetryAfter == 0 {
			t.Errorf("acquire = %+v; want its own ref held, and a retry of the gate's", a)
		}
	})
	t.Run("one whose A1 failed is first again", func(t *testing.T) {
		r := world(t)
		ids := fresh("", 3*maxGateReads)
		forgeRecord(t, r, append(slices.Clone(ids), otherSecretID()))
		keptPass(t, r, failingNext()) // the hop's A1; s's turn is the fourth
		refused := 0
		r.srv.before = func(op, kind, _ string) error {
			if op == "patch" && kind == "Widget" && refused == 0 {
				refused++
				return apierrors.NewServiceUnavailable("no")
			}
			return nil
		}
		for pass := 0; pass < 10 && refused == 0; pass++ {
			keptPass(t, r, failingNext())
		}
		if refused == 0 || !keptRecorded(t, r) {
			t.Fatalf("%d A1s failed, s recorded %v; want one, and it kept", refused, keptRecorded(t, r))
		}
		keptPass(t, r, failingNext())
		if keptRecorded(t, r) {
			t.Error("s wasn't dropped by the next pass")
		}
	})
}

// ctxServer refuses a write whose context has ended, as a client does.
type ctxServer struct{ server }

func (s ctxServer) Patch(ctx context.Context, gk schema.GroupKind, namespace, name string, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return s.server.Patch(ctx, gk, namespace, name, pt, data)
}

// TestTheBackoffOfForgedRefs: the gate has one backoff a holder, of its own,
// whatever the record names (§11.3).
func TestTheBackoffOfForgedRefs(t *testing.T) {
	ctx := context.Background()
	world := func(t *testing.T) *rig {
		t.Helper()
		r, fc, _ := keptRig(t, true)
		r.target(fc, "kt", "Secret", "home")
		r.p.gateTurns = newGateTurns()
		r.srv.before = func(op, kind, _ string) error {
			if op == "get" && kind == "Namespace" {
				return apierrors.NewServiceUnavailable("no")
			}
			return nil
		}
		return r
	}
	fresh := func(pass, n int) []ID {
		ids := make([]ID, n)
		for i := range ids {
			ids[i] = SecretRef("other", fmt.Sprintf("a%d-%05d", pass, i)).ID()
		}
		return ids
	}
	kept := func(r *rig) int {
		r.p.backoff.mu.Lock()
		defer r.p.backoff.mu.Unlock()
		return len(r.p.backoff.at)
	}
	gates := func(t *testing.T, r *rig) consentKey {
		return consentKey{holder: r.get(t, widgetKey("ns", "h")).GetUID()}
	}
	own := func(t *testing.T, r *rig) consentKey {
		k := gates(t, r)
		k.ref = otherSecretID()
		return k
	}
	var doubling []time.Duration
	for d := time.Second; d < time.Minute; d *= 2 {
		doubling = append(doubling, d)
	}
	t.Run("others on every pass, fewer and more than it reads", func(t *testing.T) {
		r := world(t)
		var got []time.Duration
		for pass := range len(doubling) {
			forgeRecord(t, r, append(fresh(pass, []int{10, 40}[pass%2]), otherSecretID()))
			got = append(got, keptPass(t, r, failingNext()).RetryAfter)
			if n := kept(r); n != 1 {
				t.Fatalf("pass %d: %d backoffs kept, want the gate's one", pass, n)
			}
		}
		if !slices.Equal(got, doubling) {
			t.Errorf("RetryAfter = %v, want %v", got, doubling)
		}
		forgeRecord(t, r, nil)
		keptPass(t, r, failingNext())
		if n := kept(r); n != 0 {
			t.Errorf("%d backoffs kept with nothing to ask about", n)
		}
	})
	t.Run("a named ref keeps its own, whatever the gate asks about", func(t *testing.T) {
		r := world(t)
		for pass := range len(doubling) {
			forgeRecord(t, r, append(fresh(pass, []int{0, 10, 40}[pass%3]), otherSecretID()))
			if d := keptPass(t, r, append(failingNext(), otherSecret())).RetryAfter; d != doubling[pass] {
				t.Errorf("pass %d: RetryAfter = %v, want %v", pass, d, doubling[pass])
			}
			if n := kept(r); n != 2 {
				t.Fatalf("pass %d: %d backoffs kept, want the gate's and the named ref's", pass, n)
			}
		}
		// Each is reset by what it is of: the named ref's by its own answer.
		r.srv.before = func(op, kind, name string) error {
			if op == "get" && kind == "Secret" && name != "s" {
				return apierrors.NewServiceUnavailable("overloaded")
			}
			return nil
		}
		if a := keptPass(t, r, []Ref{otherSecret()}); a.Degraded {
			t.Fatalf("acquire = %+v", a)
		}
		if _, ok := r.p.backoff.at[own(t, r)]; ok || r.p.backoff.at[gates(t, r)] == 0 {
			t.Errorf("backoffs = %v; want the named ref's gone, and the gate's kept", r.p.backoff.at)
		}
	})
	t.Run("a pass whose ctx has ended changes none", func(t *testing.T) {
		r := world(t)
		for range 3 {
			keptPass(t, r, failingNext())
		}
		ended, end := context.WithCancel(ctx)
		end()
		if _, err := r.p.acquire(ended, r.get(t, widgetKey("ns", "h")), failingNext()); err != nil {
			t.Fatal(err)
		}
		if after := r.p.backoff.at[gates(t, r)]; after != 8*time.Second {
			t.Errorf("next backoff = %v; want 8s still", after)
		}
	})
}

// TestAPerNameHoldWithoutALine: the gate keeps a ref whose hold has no index
// line, without a read, until the line is back (§11.3, §10.6.6).
func TestAPerNameHoldWithoutALine(t *testing.T) {
	r, fc, _ := keptRig(t, true)
	r.target(fc, "kt", "Secret", "home")
	ix := testIndex(&memStore{}, nil)
	ix.inf = syncedInformer(t, nil)
	tr := &Tracker{gk: widgetGK, idx: ix}
	tr.names = newPerNames(tr)
	r.p.cacheLacks = tr.cacheLacks
	waitSynced := func(inf cache.SharedIndexInformer) {
		t.Helper()
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if !cache.WaitForCacheSync(c.Done(), inf.HasSynced) {
			t.Fatal("the informer didn't sync")
		}
	}
	waitSynced(ix.inf)
	setConsent(t, r, secretKey("other", "s"), "")
	r.srv.calls = nil
	a := keptPass(t, r, failingNext())
	if n := countCalls(r.srv.calls, "get Secret/s"); n != 0 || !keptRecorded(t, r) || !held(r, t) || a.RetryAfter != 0 {
		t.Errorf("%d reads, recorded %v, RetryAfter %v; want it kept, without a read", n, keptRecorded(t, r), a.RetryAfter)
	}
	// The line is back, and its watch delivers the object, labelled or not.
	p := &protectedInformer{gk: secretGroupKind, ns: "other", name: "s", inf: syncedInformer(t, nil)}
	waitSynced(p.inf)
	obj := r.get(t, secretKey("other", "s"))
	obj.Labels = nil
	if err := p.inf.GetStore().Add(obj); err != nil {
		t.Fatal(err)
	}
	tr.names.by[otherSecretID()] = &nameWatch{p: p, stop: func() {}}
	keptPass(t, r, failingNext())
	if keptRecorded(t, r) {
		t.Error("the withdrawn ref is still recorded once its line is back")
	}
}

// TestKeptRefsOfAClusterScopedHolder: every namespaced ref of one is in
// another namespace, and only "*" consents to it.
func TestKeptRefsOfAClusterScopedHolder(t *testing.T) {
	ctx := context.Background()
	r, _ := consentRig(t, "")
	must(r.m.Create(widgetKey("", "c"), metav1.ObjectMeta{Finalizers: ownerFinalizers}))
	must(r.m.Create(secretKey("other", "s"), metav1.ObjectMeta{Annotations: map[string]string{AnnotationAllowHoldersFrom: "*"}}))
	pass := func() Record {
		t.Helper()
		if _, err := r.p.acquire(ctx, r.get(t, widgetKey("", "c")), failingNext()); err != nil {
			t.Fatal(err)
		}
		return ReadRecord(r.get(t, widgetKey("", "c")))
	}
	cluster := ID{Group: targetGK.Group, Kind: targetGK.Kind, Name: "cluster"}
	v, err := EncodeRecord(SortedIDs([]ID{otherSecretID(), cluster}))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": map[string]any{AnnotationRefs: v}}})
	if _, err := r.m.Patch(widgetKey("", "c"), types.MergePatchType, data); err != nil {
		t.Fatal(err)
	}
	r.p.contents = unreadable{}
	r.p.contentKinds = map[schema.GroupKind]bool{targetGK: true}
	if rec := pass(); !rec.Names(otherSecretID()) || !rec.Names(cluster) {
		t.Errorf("record %v; want both kept while \"*\" stands", rec.IDs)
	}
	setConsent(t, r, secretKey("other", "s"), "team-a")
	if rec := pass(); rec.Names(otherSecretID()) || !rec.Names(cluster) {
		t.Errorf("record %v; want the namespaced one dropped, and the cluster-scoped one not asked about", rec.IDs)
	}
}

// TestTheCountOfKeptReleases: past the unheld list's cap, the count of the
// released holds that no pass meets stays right, through passes that can't
// follow the whole chain, and goes when one does (§11.3).
func TestTheCountOfKeptReleases(t *testing.T) {
	ctx := context.Background()
	const n = maxUnheld + 4
	// world is a holder that held n Secrets of other with consent, which it
	// then withdraws, and that names refs since.
	world := func(t *testing.T) (*rig, *fakeContents, []ID) {
		t.Helper()
		r, fc, _ := keptRig(t, true)
		r.p.gateTurns = newGateTurns()
		ids := manySecrets("other", n)
		refs := []Ref{}
		for _, id := range ids {
			must(r.m.Create(secretKey("other", id.Name), metav1.ObjectMeta{Annotations: map[string]string{AnnotationAllowHoldersFrom: "ns"}}))
			refs = append(refs, Ref{GroupKind: secretGroupKind, Namespace: "other", Name: id.Name})
		}
		if a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), refs); err != nil || len(a.Held) != n {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
		return r, fc, ids
	}
	count := func(t *testing.T, r *rig) int {
		t.Helper()
		sum, _ := SummarizeUnheld(r.get(t, widgetKey("ns", "h")).Annotations)
		return sum.Count
	}
	withdraw := func(t *testing.T, r *rig, ids []ID) {
		t.Helper()
		for _, id := range ids {
			setConsent(t, r, secretKey("other", id.Name), "")
		}
	}
	ref := func(id ID) Ref { return Ref{GroupKind: secretGroupKind, Namespace: id.Namespace, Name: id.Name} }
	t.Run("none named", func(t *testing.T) {
		r, fc, ids := world(t)
		withdraw(t, r, ids)
		for pass := range 5 {
			keptPass(t, r, []Ref{targetRef("kt")}) // Missing: unfollowed
			r.tick(time.Minute)
			if got, want := count(t, r), min((pass+1)*maxGateReads, n); got != want {
				t.Errorf("pass %d: %d listed, want %d", pass, got, want)
			}
		}
		if w := len(unheldOf(t, r, "h")); w != maxUnheld {
			t.Errorf("%d entries, want %d", w, maxUnheld)
		}
		r.target(fc, "kt", "Secret", "home")
		keptPass(t, r, []Ref{targetRef("kt")})
		if got := count(t, r); got != 0 {
			t.Errorf("%d listed once the whole chain was followed, want none", got)
		}
	})
	t.Run("some named, some not", func(t *testing.T) {
		r, _, ids := world(t)
		withdraw(t, r, ids)
		// Some of them the holder still names: the first and the last, and
		// two that a first A1 folds, the one after the cap in the first pass
		// and one that the second pass's, more recent, push out.
		refs := []Ref{targetRef("kt"), ref(ids[0]), ref(ids[maxUnheld-3]), ref(ids[maxUnheld-1]), ref(ids[n-1])}
		for pass := range 5 {
			keptPass(t, r, refs)
			r.tick(time.Minute)
			if pass >= 1 {
				if got := count(t, r); got != n {
					t.Errorf("pass %d: %d listed, want %d", pass, got, n)
				}
			}
		}
	})
	t.Run("a named one pushes another out in the last A1", func(t *testing.T) {
		r, _, ids := world(t)
		r.p.views, r.p.namespaceViews = cacheOf{m: r.m}, modelNamespaces{m: r.m, known: true}
		withdraw(t, r, ids[:1])
		keptPass(t, r, []Ref{targetRef("kt")})
		r.tick(time.Minute)
		// The gate's A1 folds the first, the oldest, and the last of the 16
		// that it drops now; the pass names that one, a second later.
		withdraw(t, r, ids[1:maxGateReads+1])
		refs := []Ref{targetRef("kt"), ref(ids[maxGateReads])}
		r.srv.before = func(op, kind, _ string) error {
			if op == "get" && kind == "Target" {
				r.srv.before = nil
				r.tick(2 * time.Second)
			}
			return nil
		}
		for pass := range 3 {
			keptPass(t, r, refs)
			r.tick(time.Minute)
			if got := count(t, r); got != maxGateReads+1 {
				t.Errorf("pass %d: %d listed, want %d", pass, got, maxGateReads+1)
			}
			if u := unheldOf(t, r, "h"); u[0].Ref != unheldRef(ids[maxGateReads]) {
				t.Errorf("pass %d: first is %s; the case doesn't push another out", pass, u[0].Ref)
			}
		}
	})
	t.Run("a failing pass keeps the count", func(t *testing.T) {
		r, fc, ids := world(t)
		r.target(fc, "kt", "Secret", "home")
		withdraw(t, r, ids)
		for pass := range 4 {
			keptPass(t, r, failingNext())
			if got, want := count(t, r), min((pass+1)*maxGateReads, n); got != want {
				t.Errorf("pass %d: %d listed, want %d", pass, got, want)
			}
		}
	})
}

// TestCacheLacks: an ID whose cache covers it, has synced, and lacks it isn't
// read by the gate; one that no cache covers, or whose cache hasn't synced, is
// (§11.3, §10.5). Through Acquire, a forged record costs no GETs where the
// caches cover it.
func TestCacheLacks(t *testing.T) {
	ctx := context.Background()
	in, out := manySecrets("in", 500), manySecrets("out", 500)
	var forged []ID
	forged = append(append(forged, in...), out...)
	for i := range 100 {
		forged = append(forged, ID{Kind: "ConfigMap", Namespace: "in", Name: fmt.Sprint(i)}) // not a protected kind
	}
	held := heldObject(secretGVK, "in", "held", "u-in", widgetGK, "team")
	held.Annotations[AnnotationAllowHoldersFrom] = "team"
	r := newFakeRigWith(t, SharedOptions{Namespaces: []string{"in"}},
		namespaceObject("in"), namespaceObject("out"), namespaceObject("team"), held,
		holderObject("team", "h", append(forged, secretID("in", "held"))...))
	r.setRefs("h")
	tr := r.track(t, widgetGK)
	if tr.cacheLacks(secretID("in", "absent")) {
		t.Error("a cache that hasn't synced lacks nothing yet")
	}
	// The gate's reads have a deadline of their own.
	reads, cancel := tr.proto.gateContext(ctx)
	if d, ok := reads.Deadline(); !ok || time.Until(d) > gateReadTimeout {
		t.Errorf("the gate's deadline is %v, %v; want %v from now", d, ok, gateReadTimeout)
	}
	cancel()
	r.run(t)
	for id, want := range map[ID]bool{
		secretID("in", "absent"):                        true,
		secretID("in", "held"):                          false,
		secretID("out", "absent"):                       false,
		{Kind: "ConfigMap", Namespace: "in", Name: "c"}: false,
	} {
		if got := tr.cacheLacks(id); got != want {
			t.Errorf("cacheLacks(%s) = %v, want %v", id, got, want)
		}
	}
	holder, err := r.md.Tracker().Get(widgetsGVR, "team", "h")
	if err != nil {
		t.Fatal(err)
	}
	r.md.ClearActions()
	a, err := tr.Acquire(ctx, holder.(*metav1.PartialObjectMetadata).DeepCopy())
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, act := range r.md.Actions() {
		if act.GetVerb() == "get" {
			got[act.GetResource().Resource+" in "+act.GetNamespace()]++
		}
	}
	want := map[string]int{"secrets in out": maxGateReads, "namespaces in ": 1}
	if !maps.Equal(got, want) || a.RetryAfter == 0 {
		t.Errorf("GETs %v, want %v; RetryAfter %v, want one: the rest of out's are left", got, want, a.RetryAfter)
	}
	// The holder goes without a Forget: nothing is kept for it.
	left := func() (turns, retries int) { // the holder has no failing reads
		tr.proto.gateTurns.mu.Lock()
		defer tr.proto.gateTurns.mu.Unlock()
		tr.proto.backoff.mu.Lock()
		defer tr.proto.backoff.mu.Unlock()
		return len(tr.proto.gateTurns.at), len(tr.proto.backoff.at)
	}
	if turns, retries := left(); turns != 1 || retries == 0 {
		t.Fatalf("%d holders' turns and %d retries kept before it goes", turns, retries)
	}
	if err := r.md.Tracker().Delete(widgetsGVR, "team", "h"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if turns, retries := left(); turns == 0 && retries == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	turns, retries := left()
	t.Errorf("%d holders' turns and %d retries kept after it went", turns, retries)
}

// TestAHolderGoneWithoutAForgetLeavesNothing: one that was never established is
// deleted with no pass that forgets it.
func TestAHolderGoneWithoutAForgetLeavesNothing(t *testing.T) {
	h := holderObject("team", "h", secretID("out", "kept"), secretID("unreadable", "s"))
	h.Finalizers = nil
	r := newFakeRigWith(t, SharedOptions{Namespaces: []string{"in"}},
		namespaceObject("in"), namespaceObject("out"), namespaceObject("team"), namespaceObject("unreadable"),
		plainSecret("out", "unconsented"), h)
	r.md.PrependReactor("get", "secrets", func(a clienttesting.Action) (bool, runtime.Object, error) {
		if a.GetNamespace() == "unreadable" {
			return true, nil, apierrors.NewServiceUnavailable("overloaded")
		}
		return false, nil, nil
	})
	// Two named refs: one is reported not held, one has a backoff.
	r.setRefs("h", SecretRef("out", "unconsented"), SecretRef("unreadable", "s"))
	tr := r.track(t, widgetGK)
	r.run(t)
	obj, err := r.md.Tracker().Get(widgetsGVR, "team", "h")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Acquire(context.Background(), obj.(*metav1.PartialObjectMetadata).DeepCopy()); err != nil {
		t.Fatal(err)
	}
	left := func() (turns, backoffs, reported int) {
		p := tr.proto
		p.gateTurns.mu.Lock()
		defer p.gateTurns.mu.Unlock()
		p.backoff.mu.Lock()
		defer p.backoff.mu.Unlock()
		p.reported.mu.Lock()
		defer p.reported.mu.Unlock()
		return len(p.gateTurns.at), len(p.backoff.at), len(p.reported.m)
	}
	if turns, backoffs, reported := left(); turns != 1 || backoffs != 2 || reported != 1 {
		t.Fatalf("%d holders' turns, %d backoffs and %d reported refs kept before it goes; want 1, the gate's and a named ref's, and 1", turns, backoffs, reported)
	}
	if err := r.md.Tracker().Delete(widgetsGVR, "team", "h"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "what is kept for it to go", func() bool {
		turns, backoffs, reported := left()
		return turns == 0 && backoffs == 0 && reported == 0
	})
}

// TestCacheLacksPerName: a PerName kind's caches are the watches of the index
// lines. An ID with none lacks, once the index has synced; one with a watch,
// once that has (§10.6).
func TestCacheLacksPerName(t *testing.T) {
	indexList, watchList := make(chan struct{}), make(chan struct{})
	ix := testIndex(&memStore{}, nil)
	ix.inf = syncedInformer(t, indexList)
	tr := &Tracker{gk: widgetGK, idx: ix}
	tr.names = newPerNames(tr)
	lined, none := SecretRef("ns", "lined").ID(), SecretRef("ns", "none").ID()
	p := &protectedInformer{gk: secretGroupKind, ns: "ns", name: "lined", inf: syncedInformer(t, watchList)}
	tr.names.by[lined] = &nameWatch{p: p, stop: func() {}}
	expect := func(what string, id ID, want bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for tr.cacheLacks(id) != want {
			if time.Now().After(deadline) {
				t.Fatalf("%s: cacheLacks(%s) = %v, want %v", what, id, !want, want)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	time.Sleep(100 * time.Millisecond)
	expect("nothing synced", none, false)
	expect("nothing synced", lined, false)
	close(indexList)
	expect("the index synced", none, true)
	expect("the index synced, not its watch", lined, false)
	close(watchList)
	expect("its watch synced, without it", lined, true)
	if err := p.inf.GetStore().Add(heldObj("lined")); err != nil {
		t.Fatal(err)
	}
	expect("its watch has it", lined, false)
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
			if len(a.Unheld) != 1 || a.Unheld[0].Ref.ID() != otherSecretID() || a.Unheld[0].Reason != ReasonConsentWithdrawn {
				t.Errorf("%s, established %v: Unheld = %+v; want the dropped ref", name, established, a.Unheld)
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
