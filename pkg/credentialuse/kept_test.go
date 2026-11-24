package credentialuse

import (
	"context"
	"errors"
	"slices"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
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
