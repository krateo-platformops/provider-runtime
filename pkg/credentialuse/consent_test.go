package credentialuse

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

// The cross-namespace consent gate (§11.3) and the unheld annotation, on the
// protocol rig. The holder is in namespace "ns"; the target is in "other".

// modelNamespaces is the gate's cached Namespace view, read from the model,
// or unknown when known is false (an unsynced cache).
type modelNamespaces struct {
	m     *apimodel.Model
	known bool
}

func (v modelNamespaces) NamespaceConsent(ns string) (string, bool) {
	if !v.known {
		return "", false
	}
	o, err := v.m.Get(nsKey(ns))
	if err != nil {
		return "", false
	}
	return o.Annotations[AnnotationAllowHoldersFrom], true
}

type batchCall struct {
	id ID
	ns string
	at int // the index in the rig's calls when it was enqueued
}

// consentRig is a rig with the gate's parts wired, and namespace "other".
func consentRig(t *testing.T, nsConsent string) (*rig, *[]batchCall) {
	r := newRig(t)
	ann := map[string]string{}
	if nsConsent != "" {
		ann[AnnotationAllowHoldersFrom] = nsConsent
	}
	must(r.m.Create(nsKey("other"), metav1.ObjectMeta{Annotations: ann}))
	r.p.namespaceViews = modelNamespaces{m: r.m}
	r.p.backoff = newConsentBackoff(time.Minute)
	r.p.reported = newReportedSet()
	r.p.unheldReads = newSharedReads(rigClock{r})
	var batches []batchCall
	r.p.enqueueBatch = func(id ID, ns string) { batches = append(batches, batchCall{id, ns, len(r.srv.calls)}) }
	return r, &batches
}

func setConsent(t *testing.T, r *rig, k apimodel.Key, v string) {
	t.Helper()
	var value any = v
	if v == "" {
		value = nil
	}
	data, _ := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": map[string]any{AnnotationAllowHoldersFrom: value}}})
	if _, err := r.m.Patch(k, types.MergePatchType, data); err != nil {
		t.Fatal(err)
	}
}

func otherSecret() Ref               { return SecretRef("other", "s") }
func otherSecretID() ID              { return otherSecret().ID() }
func held(r *rig, t *testing.T) bool { return hasFinalizer(r.get(t, secretKey("other", "s")), fNS) }

func unheldOf(t *testing.T, r *rig, holder string) []UnheldEntry {
	t.Helper()
	return ReadUnheld(r.get(t, widgetKey("ns", holder)).Annotations)
}

func TestConsentSources(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name       string
		ns, object string
		wantHeld   bool
	}{
		{"none", "", "", false},
		{"by the object", "", "ns", true},
		{"by the Namespace", "ns", "", true},
		{"* on the object", "", "*", true},
		{"* on the Namespace is ignored", "*", "", false},
		{"another namespace named", "team-x", "team-y", false},
		{"the union: the object adds to its Namespace", "team-x", "ns", true},
		{"the union: the object can't narrow its Namespace", "ns", "team-y", true},
		{"a list with blanks", " team-x , ns ", "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, _ := consentRig(t, c.ns)
			meta := metav1.ObjectMeta{}
			if c.object != "" {
				meta.Annotations = map[string]string{AnnotationAllowHoldersFrom: c.object}
			}
			must(r.m.Create(secretKey("other", "s"), meta))
			a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{otherSecret()})
			if err != nil || a.MustNotConnect || a.Degraded {
				t.Fatalf("acquire = %+v, %v", a, err)
			}
			if held(r, t) != c.wantHeld || (len(a.Held) == 1) != c.wantHeld {
				t.Fatalf("held = %v (acquire %+v), want %v", held(r, t), a, c.wantHeld)
			}
			rec := ReadRecord(r.get(t, widgetKey("ns", "h")))
			if rec.Names(otherSecretID()) != c.wantHeld {
				t.Errorf("record = %v: a ref is recorded exactly when it is held", rec.IDs)
			}
			unheld := unheldOf(t, r, "h")
			if c.wantHeld != (len(unheld) == 0) {
				t.Errorf("unheld = %+v", unheld)
			}
			if !c.wantHeld && (len(unheld) != 1 || unheld[0].Reason != UnheldNotConsented || unheld[0].Ref != "other/Secret/s" || unheld[0].Since.IsZero()) {
				t.Errorf("unheld = %+v, want one NotConsented entry with its since", unheld)
			}
		})
	}
}

func TestConsentNotHeldIsReportedOnceAndConnectsAsToday(t *testing.T) {
	r, _ := consentRig(t, "")
	must(r.m.Create(secretKey("other", "s"), metav1.ObjectMeta{}))
	h := r.holder(t, "h", false)
	for pass := range 3 {
		a, err := r.p.acquire(context.Background(), r.get(t, widgetKey("ns", "h")), []Ref{otherSecret()})
		if err != nil || a.MustNotConnect || a.Degraded || a.Err != nil {
			t.Fatalf("pass %d: acquire = %+v, %v; want Connect as today", pass, a, err)
		}
		r.tick(time.Minute)
	}
	_ = h
	if len(r.rep.notHeld) != 1 {
		t.Errorf("RefNotHeld reported %d times, want once", len(r.rep.notHeld))
	}
	since := unheldOf(t, r, "h")[0].Since
	if !since.Equal(t0.UTC().Truncate(time.Second)) {
		t.Errorf("since = %v, want the first pass's %v", since, t0)
	}
}

func TestConsentWithdrawalDropsTheHold(t *testing.T) {
	ctx := context.Background()
	r, batches := consentRig(t, "")
	must(r.m.Create(secretKey("other", "s"), metav1.ObjectMeta{Annotations: map[string]string{AnnotationAllowHoldersFrom: "ns"}}))
	if a, err := r.p.acquire(ctx, r.holder(t, "h", true), []Ref{otherSecret()}); err != nil || len(a.Held) != 1 || !held(r, t) {
		t.Fatalf("pass 1: acquire = %+v, %v", a, err)
	}
	if len(*batches) != 1 || (*batches)[0].id != otherSecretID() || (*batches)[0].ns != "ns" {
		t.Fatalf("batches = %+v: a cross-namespace A2 enqueues its own (K, N) batch", *batches)
	}
	if at := (*batches)[0].at; at == 0 || !strings.HasPrefix(r.srv.calls[at-1], "patch Secret/s") {
		t.Errorf("the batch was enqueued at call %d of %v: want right after the A2", at, r.srv.calls)
	}

	setConsent(t, r, secretKey("other", "s"), "")
	r.srv.calls = nil
	r.tick(time.Second)
	a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{otherSecret()})
	if err != nil || a.MustNotConnect || a.Degraded || len(a.Held) != 0 || len(a.Unheld) != 1 || a.Unheld[0].Reason != ReasonConsentWithdrawn {
		t.Fatalf("pass 2: acquire = %+v, %v", a, err)
	}
	if rec := ReadRecord(r.get(t, widgetKey("ns", "h"))); rec.Names(otherSecretID()) {
		t.Errorf("record = %v: the withdrawn ref is still recorded", rec.IDs)
	}
	if slices.Contains(r.srv.calls, "patch Secret/s") {
		t.Errorf("calls = %v: an A2 after the withdrawal", r.srv.calls)
	}
	if u := unheldOf(t, r, "h"); len(u) != 1 || u[0].Reason != UnheldConsentWithdrawn {
		t.Errorf("unheld = %+v", u)
	}
	if len(r.rep.withdrawn) != 1 {
		t.Errorf("ConsentWithdrawn reported %d times", len(r.rep.withdrawn))
	}
	// One write: the record and the unheld list in the same patch.
	if n := countCalls(r.srv.calls, "patch Widget/h"); n != 1 {
		t.Errorf("%d holder writes, want 1: %v", n, r.srv.calls)
	}
	// Still withdrawn: the entry and its since stay, and nothing is reported again.
	r.tick(time.Minute)
	if _, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{otherSecret()}); err != nil {
		t.Fatal(err)
	}
	if u := unheldOf(t, r, "h"); len(u) != 1 || u[0].Reason != UnheldConsentWithdrawn || !u[0].Since.Equal(t0.Add(time.Second).UTC().Truncate(time.Second)) || len(r.rep.withdrawn) != 1 {
		t.Errorf("after another pass: unheld = %+v, withdrawn reported %d times", u, len(r.rep.withdrawn))
	}
	// Consent returns: held again, and the entry goes.
	setConsent(t, r, nsKey("other"), "ns")
	if a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{otherSecret()}); err != nil || len(a.Held) != 1 {
		t.Fatalf("consent back: acquire = %+v, %v", a, err)
	}
	if u := unheldOf(t, r, "h"); len(u) != 0 {
		t.Errorf("unheld = %+v after the ref was held again", u)
	}
}

func countCalls(calls []string, c string) int {
	n := 0
	for _, x := range calls {
		if x == c {
			n++
		}
	}
	return n
}

// The gate on every pass (§11.3, M1): a skipping pass reads consent from the
// cache; a cached no, or a cache that can't answer, is confirmed live before
// any drop; a deleting holder always reads live.
func TestConsentGateOnEveryPass(t *testing.T) {
	ctx := context.Background()
	// established drives a holder to the steady state, where its next pass
	// skips A2 (§6.3): the informer has delivered A1's version, and one more
	// pass has written after that delivery.
	established := func(t *testing.T, cachedNS bool) *rig {
		r, _ := consentRig(t, "ns")
		r.p.namespaceViews = modelNamespaces{m: r.m, known: cachedNS}
		must(r.m.Create(secretKey("other", "s"), metav1.ObjectMeta{}))
		if a, err := r.p.acquire(ctx, r.holder(t, "h", true), []Ref{otherSecret()}); err != nil || len(a.Held) != 1 {
			t.Fatalf("pass 1: acquire = %+v, %v", a, err)
		}
		r.views = cacheOf{m: r.m}
		current := r.get(t, widgetKey("ns", "h"))
		r.p.ver.holderEvent(current.UID, current, r.now)
		r.tick(time.Millisecond)
		if a, err := r.p.acquire(ctx, current, []Ref{otherSecret()}); err != nil || len(a.Held) != 1 {
			t.Fatalf("pass 2: acquire = %+v, %v", a, err)
		}
		r.srv.calls = nil
		return r
	}
	t.Run("a skipping pass reads the cache, not the server", func(t *testing.T) {
		r := established(t, true)
		a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{otherSecret()})
		if err != nil || len(a.Held) != 1 {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
		if slices.Contains(r.srv.calls, "get Namespace/other") {
			t.Errorf("calls = %v: a live consent read on a skipping pass", r.srv.calls)
		}
	})
	t.Run("a cache that can't answer is confirmed live, and the hold kept", func(t *testing.T) {
		r := established(t, false)
		a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{otherSecret()})
		if err != nil || len(a.Held) != 1 || !slices.Contains(r.srv.calls, "get Namespace/other") {
			t.Fatalf("acquire = %+v, %v, calls %v", a, err, r.srv.calls)
		}
	})
	t.Run("a cached withdrawal the live read contradicts keeps the hold", func(t *testing.T) {
		r := established(t, true)
		// The cache shows no consent; the server still grants it.
		r.p.namespaceViews = staleNamespaces{}
		a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{otherSecret()})
		if err != nil || len(a.Held) != 1 || !ReadRecord(r.get(t, widgetKey("ns", "h"))).Names(otherSecretID()) {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
	})
	t.Run("a deleting holder always reads live", func(t *testing.T) {
		r := established(t, true)
		h := r.get(t, widgetKey("ns", "h"))
		now := metav1.NewTime(t0)
		h.DeletionTimestamp = &now
		if _, err := r.p.acquire(ctx, h, []Ref{otherSecret()}); err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(r.srv.calls, "get Namespace/other") {
			t.Errorf("calls = %v: a deleting holder's pass read consent from the cache", r.srv.calls)
		}
	})
	t.Run("a cached yes whose A2 runs is read live first", func(t *testing.T) {
		r := established(t, true)
		// Recreate the Secret: the skip can't hold, so A2 runs, after a
		// live read.
		r.views = noViews{}
		r.p.views = viewsOf{r}
		// A cached yes, while the server has withdrawn: the A2 must not run.
		r.p.namespaceViews = modelNamespaces{m: r.m, known: true}
		cached := modelViews{r.m}
		r.views = cached
		setConsent(t, r, nsKey("other"), "")
		setConsent(t, r, secretKey("other", "s"), "")
		// The cached object still shows consent: put it in the views by
		// hand.
		r.views = fixedProtected{id: otherSecretID(), consent: "ns"}
		r.srv.calls = nil
		a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{otherSecret()})
		if err != nil || len(a.Held) != 0 || slices.Contains(r.srv.calls, "patch Secret/s") {
			t.Fatalf("acquire = %+v, %v, calls %v: an A2 rested on a cached consent", a, err, r.srv.calls)
		}
	})
}

// staleNamespaces is a cache whose Namespace shows no consent.
type staleNamespaces struct{}

func (staleNamespaces) NamespaceConsent(string) (string, bool) { return "", true }

// fixedProtected caches one object with a consent, unknown to the skip.
type fixedProtected struct {
	id      ID
	consent string
}

func (f fixedProtected) Protected(id ID) (*metav1.PartialObjectMetadata, bool) {
	if id != f.id {
		return nil, false
	}
	return &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Name: id.Name, Namespace: id.Namespace, UID: "stale",
		Annotations: map[string]string{AnnotationAllowHoldersFrom: f.consent}}}, true
}
func (fixedProtected) HoldersNaming(ID, string) []*metav1.PartialObjectMetadata { return nil }
func (fixedProtected) NamespaceDeleting(string) (bool, bool)                    { return false, false }

func TestConsentReadFailures(t *testing.T) {
	ctx := context.Background()
	t.Run("a failed read keeps the hold, and backs off per ref", func(t *testing.T) {
		r, _ := consentRig(t, "ns")
		must(r.m.Create(secretKey("other", "s"), metav1.ObjectMeta{}))
		if a, err := r.p.acquire(ctx, r.holder(t, "h", true), []Ref{otherSecret()}); err != nil || len(a.Held) != 1 {
			t.Fatalf("pass 1: acquire = %+v, %v", a, err)
		}
		failing := true
		r.srv.before = func(op, kind, name string) error {
			if failing && op == "get" && kind == "Namespace" {
				return apierrors.NewServiceUnavailable("overloaded")
			}
			return nil
		}
		var got []time.Duration
		for range 8 {
			a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{otherSecret()})
			if err != nil || len(a.Held) != 1 || a.MustNotConnect || a.Degraded {
				t.Fatalf("acquire = %+v, %v; want the hold kept", a, err)
			}
			got = append(got, a.RetryAfter)
		}
		want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, time.Minute, time.Minute}
		if !slices.Equal(got, want) {
			t.Errorf("RetryAfter = %v, want %v", got, want)
		}
		if !ReadRecord(r.get(t, widgetKey("ns", "h"))).Names(otherSecretID()) || !held(r, t) {
			t.Error("a failed read dropped the hold")
		}
		// One read succeeds: back to 1s.
		failing = false
		if a, _ := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{otherSecret()}); a.RetryAfter != 0 {
			t.Errorf("RetryAfter = %v after a read that succeeded", a.RetryAfter)
		}
		failing = true
		if a, _ := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{otherSecret()}); a.RetryAfter != time.Second {
			t.Errorf("RetryAfter = %v, want 1s again", a.RetryAfter)
		}
	})
	t.Run("a failed read of a new ref holds nothing and retries", func(t *testing.T) {
		r, _ := consentRig(t, "ns")
		must(r.m.Create(secretKey("other", "s"), metav1.ObjectMeta{}))
		r.srv.before = func(op, kind, name string) error {
			if op == "get" && kind == "Namespace" {
				return apierrors.NewServiceUnavailable("overloaded")
			}
			return nil
		}
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{otherSecret()})
		if err != nil || len(a.Held) != 0 || a.RetryAfter != time.Second || len(a.Unheld) != 1 || a.Unheld[0].Reason != ReasonConsentUnreadable || held(r, t) {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
		if u := unheldOf(t, r, "h"); len(u) != 0 {
			t.Errorf("unheld = %+v: an unread consent isn't a state to record", u)
		}
	})
	t.Run("a Namespace that's gone is Missing, not a retry", func(t *testing.T) {
		r, _ := consentRig(t, "ns")
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{SecretRef("gone", "s")})
		if err != nil || a.RetryAfter != 0 || len(a.Unheld) != 1 || a.Unheld[0].Reason != ReasonMissing {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
	})
	t.Run("NotFound is Missing, not a retry", func(t *testing.T) {
		r, _ := consentRig(t, "ns")
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{otherSecret()})
		if err != nil || a.RetryAfter != 0 || len(a.Unheld) != 1 || a.Unheld[0].Reason != ReasonMissing {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
		if !ReadRecord(r.get(t, widgetKey("ns", "h"))).Names(otherSecretID()) {
			t.Error("a Missing ref isn't recorded, as any Missing ref is")
		}
		if slices.ContainsFunc(r.srv.calls, func(c string) bool { return strings.HasPrefix(c, "patch Secret") }) {
			t.Errorf("calls = %v: an A2 on a Missing cross-namespace ref", r.srv.calls)
		}
	})
}

func TestUnheldAnnotation(t *testing.T) {
	t.Run("capped at 16, withdrawals first, then the most recent", func(t *testing.T) {
		var es []UnheldEntry
		base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
		for i := range 20 {
			es = append(es, UnheldEntry{Ref: fmt.Sprintf("o/Secret/n%02d", i), Reason: UnheldNotConsented, Since: base.Add(time.Duration(i) * time.Hour)})
		}
		es = append(es, UnheldEntry{Ref: "o/Secret/w", Reason: UnheldConsentWithdrawn, Since: base})
		v, err := encodeUnheld(es)
		if err != nil {
			t.Fatal(err)
		}
		got := ReadUnheld(map[string]string{AnnotationUnheld: *v})
		if len(got) != maxUnheld {
			t.Fatalf("%d entries, want %d", len(got), maxUnheld)
		}
		if got[0].Ref != "o/Secret/w" || got[1].Ref != "o/Secret/n19" {
			t.Errorf("order: %s, %s; want the withdrawal, then the most recent", got[0].Ref, got[1].Ref)
		}
		last := got[maxUnheld-1]
		if last.Reason != unheldMore || last.Count != 21-(maxUnheld-1) {
			t.Errorf("last = %+v, want More with %d", last, 21-(maxUnheld-1))
		}
		if len(*v) > 6*1024 {
			t.Errorf("%d bytes, past the 6 KB §11.3 bounds it to", len(*v))
		}
	})
	t.Run("none removes the annotation", func(t *testing.T) {
		if v, err := encodeUnheld(nil); err != nil || v != nil {
			t.Errorf("encodeUnheld(nil) = %v, %v", v, err)
		}
	})
	t.Run("an edit doesn't survive the next A1", func(t *testing.T) {
		r, _ := consentRig(t, "")
		must(r.m.Create(secretKey("other", "s"), metav1.ObjectMeta{}))
		r.holder(t, "h", false)
		data := []byte(`{"metadata":{"annotations":{"` + AnnotationUnheld + `":"edited"}}}`)
		if _, err := r.m.Patch(widgetKey("ns", "h"), types.MergePatchType, data); err != nil {
			t.Fatal(err)
		}
		if _, err := r.p.acquire(context.Background(), r.get(t, widgetKey("ns", "h")), []Ref{otherSecret()}); err != nil {
			t.Fatal(err)
		}
		if u := unheldOf(t, r, "h"); len(u) != 1 || u[0].Reason != UnheldNotConsented {
			t.Errorf("unheld = %+v", u)
		}
	})
	t.Run("an entry goes when the holder stops naming the ref", func(t *testing.T) {
		r, _ := consentRig(t, "")
		must(r.m.Create(secretKey("other", "s"), metav1.ObjectMeta{}))
		r.holder(t, "h", false)
		if _, err := r.p.acquire(context.Background(), r.get(t, widgetKey("ns", "h")), []Ref{otherSecret()}); err != nil {
			t.Fatal(err)
		}
		if _, err := r.p.acquire(context.Background(), r.get(t, widgetKey("ns", "h")), nil); err != nil {
			t.Fatal(err)
		}
		if _, has := r.get(t, widgetKey("ns", "h")).Annotations[AnnotationUnheld]; has {
			t.Error("the unheld annotation outlived the ref")
		}
	})
}

// §9.4's two shapes of a partly consenting chain.
func TestConsentChains(t *testing.T) {
	ctx := context.Background()
	contentRig := func(t *testing.T) (*rig, *fakeContents) {
		r, _ := consentRig(t, "")
		fc := &fakeContents{r: r, specs: map[ID]map[string]any{}}
		r.p.contentKinds = map[schema.GroupKind]bool{targetGK: true}
		r.p.contents = fc
		return r, fc
	}
	t.Run("a held intermediate, an unheld Secret elsewhere", func(t *testing.T) {
		r, fc := contentRig(t)
		must(r.m.Create(targetKey("c"), metav1.ObjectMeta{}))
		fc.specs[targetID("c")] = map[string]any{"ref": map[string]any{"kind": "Secret", "name": "s"}}
		must(r.m.Create(secretKey("other", "s"), metav1.ObjectMeta{}))
		c := targetRef("c")
		c.Next = func(*unstructured.Unstructured) ([]Ref, error) { return []Ref{otherSecret()}, nil }
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{c})
		if err != nil || a.MustNotConnect || len(a.Held) != 1 || len(a.Unheld) != 1 || a.Unheld[0].Reason != ReasonNotConsented {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
		if !hasFinalizer(r.get(t, targetKey("c")), fNS) || held(r, t) {
			t.Error("want the intermediate held and the Secret elsewhere not")
		}
	})
	t.Run("an intermediate elsewhere, the Secret at home", func(t *testing.T) {
		r, fc := contentRig(t)
		ckey := apimodel.Key{GK: targetGK, Namespace: "other", Name: "c"}
		must(r.m.Create(ckey, metav1.ObjectMeta{}))
		fc.specs[ID{Group: targetGK.Group, Kind: targetGK.Kind, Namespace: "other", Name: "c"}] = map[string]any{"ref": map[string]any{"kind": "Secret", "name": "home"}}
		must(r.m.Create(secretKey("ns", "home"), metav1.ObjectMeta{}))
		c := Ref{GroupKind: targetGK, Namespace: "other", Name: "c", Next: func(*unstructured.Unstructured) ([]Ref, error) {
			return []Ref{SecretRef("ns", "home")}, nil
		}}
		a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{c})
		if err != nil || a.MustNotConnect || len(a.Held) != 1 || a.Held[0].ID() != SecretRef("ns", "home").ID() {
			t.Fatalf("acquire = %+v, %v; want the home Secret held behind the unheld hop", a, err)
		}
		if hasFinalizer(r.get(t, ckey), fNS) {
			t.Error("the unheld intermediate was pinned")
		}
		if got, ok := Resolved(WithResolved(ctx, a), c); !ok || got.GetName() != "c" {
			t.Errorf("Resolved = %v, %v; want the unheld hop's read", got, ok)
		}
	})
	t.Run("an unheld hop's read is shared for 10s", func(t *testing.T) {
		r, fc := contentRig(t)
		ckey := apimodel.Key{GK: targetGK, Namespace: "other", Name: "c"}
		must(r.m.Create(ckey, metav1.ObjectMeta{}))
		fc.specs[ID{Group: targetGK.Group, Kind: targetGK.Kind, Namespace: "other", Name: "c"}] = map[string]any{"ref": map[string]any{"kind": "Secret", "name": "home"}}
		must(r.m.Create(secretKey("ns", "home"), metav1.ObjectMeta{}))
		c := Ref{GroupKind: targetGK, Namespace: "other", Name: "c", Next: func(*unstructured.Unstructured) ([]Ref, error) {
			return []Ref{SecretRef("ns", "home")}, nil
		}}
		reads := func() int { return countCalls(r.srv.calls, "content Target/c") }
		for _, h := range []string{"h1", "h2", "h3"} {
			if _, err := r.p.acquire(ctx, r.holder(t, h, false), []Ref{c}); err != nil {
				t.Fatal(err)
			}
		}
		if reads() != 1 {
			t.Errorf("%d reads for three holders within 10s, want 1", reads())
		}
		r.tick(11 * time.Second)
		if _, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h1")), []Ref{c}); err != nil {
			t.Fatal(err)
		}
		if reads() != 2 {
			t.Errorf("%d reads after 11s, want 2", reads())
		}
	})
}

// TestAWithdrawnRefLeavesTheRecordEvenWithAHopUnfollowed: a hop that can't be
// held keeps the record's refs (§9.1), but never one whose target withdrew
// consent this pass (§11.3).
func TestAWithdrawnRefLeavesTheRecordEvenWithAHopUnfollowed(t *testing.T) {
	ctx := context.Background()
	r, _ := consentRig(t, "")
	fc := &fakeContents{r: r, specs: map[ID]map[string]any{}}
	r.p.contentKinds = map[schema.GroupKind]bool{targetGK: true}
	r.p.contents = fc
	r.target(fc, "kt", "Secret", "home")
	must(r.m.Create(secretKey("ns", "home"), metav1.ObjectMeta{}))
	must(r.m.Create(secretKey("other", "s"), metav1.ObjectMeta{Annotations: map[string]string{AnnotationAllowHoldersFrom: "ns"}}))
	refs := []Ref{targetRef("kt"), otherSecret()}
	if a, err := r.p.acquire(ctx, r.holder(t, "h", true), refs); err != nil || len(a.Held) != 3 {
		t.Fatalf("pass 1: acquire = %+v, %v", a, err)
	}
	// The hop goes, and consent is withdrawn, in one pass.
	data := []byte(`{"metadata":{"finalizers":null}}`)
	if _, err := r.m.Patch(targetKey("kt"), types.MergePatchType, data); err != nil {
		t.Fatal(err)
	}
	must(r.m.Delete(targetKey("kt")))
	setConsent(t, r, secretKey("other", "s"), "")
	if _, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), refs); err != nil {
		t.Fatal(err)
	}
	rec := ReadRecord(r.get(t, widgetKey("ns", "h")))
	if rec.Names(otherSecretID()) {
		t.Errorf("record = %v: the withdrawn ref was kept with the unfollowed hop's", rec.IDs)
	}
	if !rec.Names(SecretRef("ns", "home").ID()) {
		t.Errorf("record = %v: the Secret behind the unfollowed hop was dropped", rec.IDs)
	}
}

// TestOneWriteForANewHoldAndANewUnheldEntry: a pass that adds a held ref and
// lists another as unheld writes the holder once (§11.3: unheld goes in the
// A1 patch, with the record).
func TestOneWriteForANewHoldAndANewUnheldEntry(t *testing.T) {
	r, _ := consentRig(t, "")
	// A clock that moves on every read, as a real one does between a pass's
	// two A1s: both must still compute one list, from one read.
	r.p.clock = tickingClock{r}
	must(r.m.Create(secretKey("ns", "home"), metav1.ObjectMeta{}))
	must(r.m.Create(secretKey("other", "s"), metav1.ObjectMeta{}))
	r.holder(t, "h", false)
	r.srv.calls = nil
	a, err := r.p.acquire(context.Background(), r.get(t, widgetKey("ns", "h")), []Ref{SecretRef("ns", "home"), otherSecret()})
	if err != nil || len(a.Held) != 1 || len(a.Unheld) != 1 {
		t.Fatalf("acquire = %+v, %v", a, err)
	}
	if n := countCalls(r.srv.calls, "patch Widget/h"); n != 1 {
		t.Errorf("%d holder writes, want 1: %v", n, r.srv.calls)
	}
	h := r.get(t, widgetKey("ns", "h"))
	if !ReadRecord(h).Names(SecretRef("ns", "home").ID()) || len(ReadUnheld(h.Annotations)) != 1 {
		t.Errorf("record %v, unheld %v", ReadRecord(h).IDs, ReadUnheld(h.Annotations))
	}
}

// BeforeEstablish never pins a ref in another namespace without a live yes
// (§11.3; the review of #123): a Missing one that appears may still not
// consent.
func TestBeforeEstablishGatesACrossNamespaceRef(t *testing.T) {
	ctx := context.Background()
	for _, consent := range []bool{false, true} {
		t.Run(fmt.Sprintf("consent %v", consent), func(t *testing.T) {
			r, batches := consentRig(t, "")
			a, err := r.p.acquire(ctx, r.holder(t, "h", false), []Ref{otherSecret()})
			if err != nil || len(a.Unheld) != 1 || a.Unheld[0].Reason != ReasonMissing {
				t.Fatalf("acquire = %+v, %v", a, err)
			}
			meta := metav1.ObjectMeta{}
			if consent {
				meta.Annotations = map[string]string{AnnotationAllowHoldersFrom: "ns"}
			}
			must(r.m.Create(secretKey("other", "s"), meta))
			if err := r.p.beforeEstablish(ctx, r.get(t, widgetKey("ns", "h")), a); err != nil {
				t.Fatal(err)
			}
			if held(r, t) != consent {
				t.Errorf("held = %v, want %v: BeforeEstablish holds a cross-namespace ref only on a live yes", held(r, t), consent)
			}
			if consent && (len(*batches) != 1 || (*batches)[0].id != otherSecretID()) {
				t.Errorf("batches = %+v: BeforeEstablish's A2 enqueues its own batch too", *batches)
			}
		})
	}
}

// Past maxUnheld entries the list is stable (the review of #123): a ref
// trimmed into More comes back each pass as if new, and mustn't displace a
// listed one, rewrite the holder, move a since, or be reported again.
func TestUnheldPastTheCapIsStable(t *testing.T) {
	ctx := context.Background()
	r, _ := consentRig(t, "")
	var refs []Ref
	for i := range maxUnheld + 1 {
		name := fmt.Sprintf("s%02d", i)
		must(r.m.Create(secretKey("other", name), metav1.ObjectMeta{}))
		refs = append(refs, SecretRef("other", name))
	}
	r.holder(t, "h", false)
	if _, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), refs); err != nil {
		t.Fatal(err)
	}
	first := r.get(t, widgetKey("ns", "h")).Annotations[AnnotationUnheld]
	for pass := 2; pass <= 4; pass++ {
		r.tick(time.Minute)
		r.srv.calls = nil
		if _, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), refs); err != nil {
			t.Fatal(err)
		}
		if n := countCalls(r.srv.calls, "patch Widget/h"); n != 0 {
			t.Errorf("pass %d: %d holder writes, want none when nothing changed", pass, n)
		}
		if got := r.get(t, widgetKey("ns", "h")).Annotations[AnnotationUnheld]; got != first {
			t.Errorf("pass %d: the list changed:\n%s\n%s", pass, first, got)
		}
	}
	if len(r.rep.notHeld) != maxUnheld+1 {
		t.Errorf("RefNotHeld reported %d times, want %d: once per ref", len(r.rep.notHeld), maxUnheld+1)
	}
}

// Any live read that succeeds resets the backoff (§11.3; the review of
// #123), whatever it answered.
func TestTheBackoffResetsOnAnySuccessfulRead(t *testing.T) {
	ctx := context.Background()
	r, _ := consentRig(t, "ns")
	must(r.m.Create(secretKey("other", "s"), metav1.ObjectMeta{}))
	if _, err := r.p.acquire(ctx, r.holder(t, "h", true), []Ref{otherSecret()}); err != nil {
		t.Fatal(err)
	}
	failing := true
	r.srv.before = func(op, kind, name string) error {
		if failing && op == "get" && kind == "Namespace" {
			return apierrors.NewServiceUnavailable("overloaded")
		}
		return nil
	}
	for range 3 {
		_, _ = r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{otherSecret()})
	}
	// A read that succeeds and finds no consent: the hold goes, and the
	// backoff resets.
	failing = false
	setConsent(t, r, nsKey("other"), "")
	if a, _ := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{otherSecret()}); a.RetryAfter != 0 || len(a.Held) != 0 {
		t.Fatalf("acquire = %+v", a)
	}
	failing = true
	if a, _ := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{otherSecret()}); a.RetryAfter != time.Second {
		t.Errorf("RetryAfter = %v after a successful read that found no consent, want 1s", a.RetryAfter)
	}
}

// tickingClock is the rig's clock, a second later on every read.
type tickingClock struct{ r *rig }

func (c tickingClock) Now() time.Time {
	c.r.now = c.r.now.Add(time.Second)
	return c.r.now
}
func (c tickingClock) Since(t time.Time) time.Duration { return c.Now().Sub(t) }

// TestANewEntryBesideListedOnesIsOneWrite: a pass that adds an unheld entry
// next to listed ones writes the holder once (the re-check of #123): both of
// its A1s compute from the list as the pass found it.
func TestANewEntryBesideListedOnesIsOneWrite(t *testing.T) {
	ctx := context.Background()
	r, _ := consentRig(t, "")
	r.p.clock = tickingClock{r}
	var refs []Ref
	for i := range 4 {
		name := fmt.Sprintf("s%d", i)
		must(r.m.Create(secretKey("other", name), metav1.ObjectMeta{}))
		refs = append(refs, SecretRef("other", name))
	}
	r.holder(t, "h", false)
	if _, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), refs[:3]); err != nil {
		t.Fatal(err)
	}
	r.srv.calls = nil
	if _, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), refs); err != nil {
		t.Fatal(err)
	}
	if n := countCalls(r.srv.calls, "patch Widget/h"); n != 1 {
		t.Errorf("%d holder writes for one new entry, want 1: %v", n, r.srv.calls)
	}
	if got := unheldOf(t, r, "h"); len(got) != 4 || got[0].Ref != "other/Secret/s3" {
		t.Errorf("unheld = %+v; want 4, the newest first", got)
	}
}
