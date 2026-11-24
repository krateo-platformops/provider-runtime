package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

// Chains are refused in this release (§9, "Not in this release"; the user's
// decision of 2026-11-24): the tests of the refusal. The chain code's own
// tests set followChains.

// noContents fails the test on any read of content: nothing but following a
// chain reads it.
type noContents struct{ t *testing.T }

func (n noContents) Content(_ context.Context, gk schema.GroupKind, ns, name string) (*unstructured.Unstructured, error) {
	n.t.Errorf("content of %s %s/%s was read", gk, ns, name)
	return nil, errors.New("no content here")
}

type noContentCache struct{ t *testing.T }

func (n noContentCache) Content(id ID) (*unstructured.Unstructured, bool) {
	n.t.Errorf("cached content of %s was read", id)
	return nil, false
}

// refusingRig is a rig as production builds a protocol: it doesn't follow
// chains, though every part that would is there.
func refusingRig(t *testing.T) (*rig, *disownSet) {
	r, set := disownRig(t)
	r.p.contentKinds = map[schema.GroupKind]bool{targetGK: true}
	r.p.contents = noContents{t}
	r.p.contentCache = noContentCache{t}
	r.p.listers = map[schema.GroupKind]Lister{targetGK: func(ns, name string) (*unstructured.Unstructured, error) {
		t.Errorf("the lister read Target %s/%s", ns, name)
		return nil, errors.New("no lister here")
	}}
	return r, set
}

// TestAcquireRefusesAChainBeforeAnyWrite: a ref with a Next anywhere in the
// list is ErrChainsNotSupported, before any call: no A1, no A2, not even a
// read, and no report but the degraded one. A new holder must not connect;
// an established one, deleting or not, is Degraded and proceeds (§8).
func TestAcquireRefusesAChainBeforeAnyWrite(t *testing.T) {
	ctx := context.Background()
	next := func(*unstructured.Unstructured) ([]Ref, error) {
		t.Error("a Next was called")
		return nil, nil
	}
	chain := func(r Ref) Ref { r.Next = next; return r }
	guaranteed := func(r Ref) Ref { r.Guaranteed = true; return r }
	direct := SecretRef("ns", "direct")
	refs := map[string]struct {
		refs   []Ref
		named  ID // the ref the error names
		disown string
	}{
		"the only ref":                     {refs: []Ref{chain(targetRef("kt"))}, named: targetID("kt")},
		"after a direct ref":               {refs: []Ref{direct, chain(targetRef("kt"))}, named: targetID("kt")},
		"before a direct ref":              {refs: []Ref{chain(targetRef("kt")), direct}, named: targetID("kt")},
		"on a Secret":                      {refs: []Ref{direct, chain(SecretRef("ns", "outer"))}, named: SecretRef("ns", "outer").ID()},
		"on a Guaranteed ref":              {refs: []Ref{direct, chain(guaranteed(targetRef("cd")))}, named: targetID("cd")},
		"in another namespace":             {refs: []Ref{direct, chain(otherSecret())}, named: otherSecretID()},
		"on the second ref to one object":  {refs: []Ref{direct, chain(direct)}, named: direct.ID()},
		"on a Guaranteed second ref to it": {refs: []Ref{direct, chain(guaranteed(direct))}, named: direct.ID()},
		"on a ref with no name":            {refs: []Ref{chain(Ref{GroupKind: targetGK, Namespace: "ns"})}, named: ID{Group: targetGK.Group, Kind: targetGK.Kind, Namespace: "ns"}},
		"on a kind the switch covers":      {refs: []Ref{direct, chain(targetRef("kt"))}, named: targetID("kt"), disown: "all"},
	}
	holders := map[string]struct {
		established, deleting bool
	}{
		"new":                      {},
		"established":              {established: true},
		"established and deleting": {established: true, deleting: true},
	}
	for rn, rc := range refs {
		for hn, hc := range holders {
			t.Run(rn+", "+hn, func(t *testing.T) {
				r, set := refusingRig(t)
				*set = parseDisown(rc.disown)
				for _, k := range []apimodel.Key{targetKey("kt"), targetKey("cd"), secretKey("ns", "direct"), secretKey("ns", "outer"), secretKey("other", "s")} {
					must(r.m.Create(k, metav1.ObjectMeta{}))
				}
				h := r.holder(t, "h", hc.established)
				if hc.deleting {
					must(r.m.Delete(widgetKey("ns", "h")))
					h = r.get(t, widgetKey("ns", "h"))
				}
				before := h.DeepCopy()
				r.srv.calls = nil
				a, err := r.p.acquire(ctx, h, rc.refs)
				if err != nil {
					t.Fatalf("acquire's error = %v; the refusal goes in Acquired.Err", err)
				}
				if !errors.Is(a.Err, ErrChainsNotSupported) {
					t.Fatalf("Err = %v, want ErrChainsNotSupported", a.Err)
				}
				for _, want := range []string{rc.named.String(), "without a Next"} {
					if !strings.Contains(a.Err.Error(), want) {
						t.Errorf("Err = %q, want it to say %q", a.Err, want)
					}
				}
				if a.MustNotConnect == hc.established || a.Degraded != hc.established {
					t.Errorf("MustNotConnect %v, Degraded %v for a holder with established %v", a.MustNotConnect, a.Degraded, hc.established)
				}
				if len(a.Held) != 0 || len(a.Unheld) != 0 || a.RetryAfter != 0 || a.Reread || len(a.resolved) != 0 {
					t.Errorf("acquire = %+v, want nothing but the refusal", a)
				}
				if len(r.srv.calls) != 0 {
					t.Errorf("calls = %v, want none: the refusal comes before any write, and any read", r.srv.calls)
				}
				if len(r.rep.notHeld) != 0 || len(r.rep.withdrawn) != 0 {
					t.Errorf("reports %+v, want only the degraded one", r.rep)
				}
				want := []string(nil)
				if hc.established {
					// Its own reason, which no other failure has (§12).
					want = []string{"ChainsNotSupported"}
				}
				if !slices.Equal(r.rep.degraded, want) {
					t.Errorf("degraded reports = %v, want %v", r.rep.degraded, want)
				}
				if !equality.Semantic.DeepEqual(h, before) || !equality.Semantic.DeepEqual(r.get(t, widgetKey("ns", "h")), before) {
					t.Error("the holder changed")
				}
			})
		}
	}
}

// TestDirectRefsAreNotRefused is the control: the same rig takes a list with
// no Next, Guaranteed refs included, which stay neither recorded, held nor
// read.
func TestDirectRefsAreNotRefused(t *testing.T) {
	r, _ := refusingRig(t)
	must(r.m.Create(secretKey("ns", "direct"), metav1.ObjectMeta{}))
	must(r.m.Create(targetKey("cd"), metav1.ObjectMeta{}))
	g := Ref{GroupKind: targetGK, Namespace: "ns", Name: "cd", Guaranteed: true}
	a, err := r.p.acquire(context.Background(), r.holder(t, "h", false), []Ref{SecretRef("ns", "direct"), g})
	if err != nil || a.Err != nil || a.MustNotConnect || len(a.Held) != 1 || len(a.Unheld) != 0 {
		t.Fatalf("acquire = %+v, %v; want the Secret held", a, err)
	}
	if got := chainRecord(t, r, "h"); !slices.Equal(got, []ID{SecretRef("ns", "direct").ID()}) {
		t.Errorf("record = %v, want the Secret only", got)
	}
	if hasFinalizer(r.get(t, targetKey("cd")), fNS) {
		t.Error("the Guaranteed ref was held")
	}
}

// TestNoRefsPastTheFirstLevelWithoutANext: a ref is found inside another only
// by calling a Next, so a list that passes the refusal is the whole chain:
// over random lists of every kind of direct ref, nothing is resolved, no
// content is read, and the record, the held and the unheld refs are all from
// the list.
func TestNoRefsPastTheFirstLevelWithoutANext(t *testing.T) {
	rng := rand.New(rand.NewSource(1)) // #nosec G404 -- a test's reproducible choices
	pool := []Ref{
		SecretRef("ns", "here"), SecretRef("ns", "missing"), SecretRef("ns", "deleting"),
		SecretRef("other", "s"), SecretRef("other", "consents"), SecretRef("other", "missing"),
		{GroupKind: targetGK, Namespace: "ns", Name: "kt"},
		{GroupKind: targetGK, Namespace: "ns", Name: "cd", Guaranteed: true},
		{GroupKind: targetGK, Namespace: "other", Name: "away"},
	}
	for i := range 200 {
		r, set := refusingRig(t)
		if rng.Intn(4) == 0 {
			*set = parseDisown(targetGK.Kind + "." + targetGK.Group)
		}
		for _, k := range []apimodel.Key{secretKey("ns", "here"), secretKey("other", "s"), targetKey("kt"), targetKey("cd")} {
			must(r.m.Create(k, metav1.ObjectMeta{}))
		}
		must(r.m.Create(secretKey("other", "consents"), metav1.ObjectMeta{Annotations: map[string]string{AnnotationAllowHoldersFrom: "ns"}}))
		must(r.m.Create(secretKey("ns", "deleting"), metav1.ObjectMeta{Finalizers: []string{"example.com/keep"}}))
		must(r.m.Delete(secretKey("ns", "deleting")))
		var refs []Ref
		named := map[ID]bool{}
		for range 1 + rng.Intn(6) {
			ref := pool[rng.Intn(len(pool))]
			refs = append(refs, ref)
			named[ref.ID()] = true
		}
		h := r.holder(t, "h", rng.Intn(2) == 0)
		a, err := r.p.acquire(context.Background(), h, refs)
		if err != nil || errors.Is(a.Err, ErrChainsNotSupported) {
			t.Fatalf("list %d %v: acquire = %+v, %v", i, refs, a, err)
		}
		got := slices.Clone(chainRecord(t, r, "h"))
		for _, ref := range a.Held {
			got = append(got, ref.ID())
		}
		for _, u := range a.Unheld {
			got = append(got, u.Ref.ID())
		}
		for _, id := range got {
			if !named[id] {
				t.Errorf("list %d %v: %s isn't in it", i, refs, id)
			}
		}
		if len(a.resolved) != 0 {
			t.Errorf("list %d %v: %d objects resolved", i, refs, len(a.resolved))
		}
	}
}

// TestARefusedChainKeepsWhatItHeld: a holder that holds refs, through a chain
// under an older build or directly before its refs gained one, keeps its
// record, so a release keeps both objects; its deletion still ends in Forget
// and releases both; and once its refs have no Next, a pass writes the exact
// record again.
func TestARefusedChainKeepsWhatItHeld(t *testing.T) {
	ctx := context.Background()
	ids := SortedIDs([]ID{targetID("kt"), SecretRef("ns", "kubeconfig").ID()})
	setup := func(t *testing.T, established bool) *rig {
		r, fc := chainRig(t)
		r.target(fc, "kt", "Secret", "kubeconfig")
		must(r.m.Create(secretKey("ns", "kubeconfig"), metav1.ObjectMeta{}))
		if a, err := r.p.acquire(ctx, r.holder(t, "h", established), []Ref{targetRef("kt")}); err != nil || len(a.Held) != 2 {
			t.Fatalf("the older build's pass: acquire = %+v, %v", a, err)
		}
		r.p.followChains = false
		r.p.contents = noContents{t}
		return r
	}
	release := func(t *testing.T, r *rig) (target, secret bool) {
		t.Helper()
		if _, err := r.p.release(ctx, "ns", ids); err != nil {
			t.Fatal(err)
		}
		return hasFinalizer(r.get(t, targetKey("kt")), fNS), hasFinalizer(r.get(t, secretKey("ns", "kubeconfig")), fNS)
	}
	held := func(t *testing.T, r *rig, when string) {
		t.Helper()
		if got := chainRecord(t, r, "h"); !slices.Equal(got, ids) {
			t.Errorf("%s: record = %v, want %v as it was", when, got, ids)
		}
		if target, secret := release(t, r); !target || !secret {
			t.Errorf("%s: the Target held %v, the Secret %v; a release took what the record names", when, target, secret)
		}
	}
	t.Run("established, then deleted", func(t *testing.T) {
		r := setup(t, true)
		for range 2 {
			a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{targetRef("kt")})
			if err != nil || !a.Degraded || a.MustNotConnect || !errors.Is(a.Err, ErrChainsNotSupported) {
				t.Fatalf("acquire = %+v, %v", a, err)
			}
			held(t, r, "after a refused pass")
		}
		must(r.m.Delete(widgetKey("ns", "h")))
		h := r.get(t, widgetKey("ns", "h"))
		a, err := r.p.acquire(ctx, h, []Ref{targetRef("kt")})
		if err != nil || !a.Degraded || a.MustNotConnect {
			t.Fatalf("the deletion pass: acquire = %+v, %v; want it to proceed", a, err)
		}
		held(t, r, "while deleting")
		if !r.p.forget(h) {
			t.Fatal("Forget changed nothing")
		}
		if _, err := r.m.Patch(widgetKey("ns", "h"), types.MergePatchType, finalizerPatch(h, false)); err != nil {
			t.Fatal(err)
		}
		if target, secret := release(t, r); target || secret {
			t.Errorf("after the deletion: the Target held %v, the Secret %v, want both released", target, secret)
		}
	})
	t.Run("not established yet", func(t *testing.T) {
		r := setup(t, false)
		a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{targetRef("kt")})
		if err != nil || !a.MustNotConnect || a.Degraded {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
		held(t, r, "after a refused pass")
		// It never got its finalizer, so its deletion is immediate.
		must(r.m.Delete(widgetKey("ns", "h")))
		if target, secret := release(t, r); target || secret {
			t.Errorf("after the deletion: the Target held %v, the Secret %v, want both released", target, secret)
		}
	})
	t.Run("the refs name the Secret directly", func(t *testing.T) {
		r := setup(t, true)
		a, err := r.p.acquire(ctx, r.get(t, widgetKey("ns", "h")), []Ref{SecretRef("ns", "kubeconfig")})
		if err != nil || a.Err != nil || len(a.Held) != 1 {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
		if got := chainRecord(t, r, "h"); !slices.Equal(got, []ID{SecretRef("ns", "kubeconfig").ID()}) {
			t.Errorf("record = %v, want the Secret only", got)
		}
		if target, secret := release(t, r); target || !secret {
			t.Errorf("the Target held %v, the Secret %v; want the Target released and the Secret kept", target, secret)
		}
	})
}

// TestBeforeEstablishRefusesAChain: BeforeEstablish never establishes a
// holder whose pass was refused, nor holds a Missing ref with a Next an
// Acquired built by hand names.
func TestBeforeEstablishRefusesAChain(t *testing.T) {
	ctx := context.Background()
	t.Run("after a refused pass", func(t *testing.T) {
		r, _ := refusingRig(t)
		h := r.holder(t, "h", false)
		a, err := r.p.acquire(ctx, h, []Ref{targetRef("kt")})
		if err != nil {
			t.Fatal(err)
		}
		if err := r.p.beforeEstablish(ctx, h, a); !errors.Is(err, ErrChainsNotSupported) {
			t.Errorf("beforeEstablish = %v, want ErrChainsNotSupported", err)
		}
		if len(r.srv.calls) != 0 {
			t.Errorf("calls = %v, want none", r.srv.calls)
		}
	})
	t.Run("an Acquired built by hand", func(t *testing.T) {
		r, _ := refusingRig(t)
		must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
		must(r.m.Create(targetKey("kt"), metav1.ObjectMeta{}))
		h := r.holder(t, "h", false)
		r.srv.calls = nil
		a := Acquired{Unheld: []Unheld{{Ref: SecretRef("ns", "s"), Reason: ReasonMissing}, {Ref: targetRef("kt"), Reason: ReasonMissing}}}
		if err := r.p.beforeEstablish(ctx, h, a); !errors.Is(err, ErrChainsNotSupported) {
			t.Errorf("beforeEstablish = %v, want ErrChainsNotSupported", err)
		}
		if len(r.srv.calls) != 0 {
			t.Errorf("calls = %v, want none", r.srv.calls)
		}
	})
	t.Run("control: a direct Missing ref that appeared is held", func(t *testing.T) {
		r, _ := refusingRig(t)
		h := r.holder(t, "h", false)
		a, err := r.p.acquire(ctx, h, []Ref{SecretRef("ns", "s")})
		if err != nil || len(a.Unheld) != 1 {
			t.Fatalf("acquire = %+v, %v", a, err)
		}
		must(r.m.Create(secretKey("ns", "s"), metav1.ObjectMeta{}))
		if err := r.p.beforeEstablish(ctx, h, a); err != nil {
			t.Fatal(err)
		}
		if !hasFinalizer(r.get(t, secretKey("ns", "s")), fNS) {
			t.Error("the Secret that appeared isn't held")
		}
	})
}

// TestTheTrackerRefusesChains: a Tracker as Track builds it refuses a chain
// with no API call, records CredentialUseDegraded on an established holder,
// and takes the holder out of the missing-ref prober, which would otherwise
// enqueue it every tick for a ref no pass of it can hold.
func TestTheTrackerRefusesChains(t *testing.T) {
	ctx := context.Background()
	holder := holderObject("ns", "h")
	r := newFakeRig(t, holder, namespaceObject("ns"))
	tr := r.track(t, widgetGK)
	r.setRefs("h", SecretRef("ns", "gone"))
	if a, err := tr.Acquire(ctx, r.currentHolder(t)); err != nil || len(a.Unheld) != 1 {
		t.Fatalf("a direct pass: Acquire = %+v, %v", a, err)
	}
	later := r.clk.Now().Add(time.Hour)
	if len(tr.missing.due(later)) != 1 {
		t.Fatal("the prober doesn't wait for the Missing ref")
	}
	r.takeEvents()

	chained := SecretRef("ns", "outer")
	chained.Next = func(*unstructured.Unstructured) ([]Ref, error) {
		t.Error("the Next was called")
		return nil, nil
	}
	r.setRefs("h", SecretRef("ns", "gone"), chained)
	r.md.ClearActions()
	r.dyn.ClearActions()
	before := r.currentHolder(t)
	a, err := tr.Acquire(ctx, before.DeepCopy())
	if err != nil || !a.Degraded || a.MustNotConnect || !errors.Is(a.Err, ErrChainsNotSupported) {
		t.Fatalf("Acquire = %+v, %v; want Degraded with ErrChainsNotSupported", a, err)
	}
	if got := append(r.md.Actions(), r.dyn.Actions()...); len(got) != 0 {
		t.Errorf("API calls %v, want none", got)
	}
	if !ReadRecord(before).Names(SecretRef("ns", "gone").ID()) || !equality.Semantic.DeepEqual(r.currentHolder(t), before) {
		t.Errorf("the holder is now %v, want it as it was, its record with it", r.currentHolder(t).Annotations)
	}
	if got := r.takeEvents(); len(got) != 1 || !strings.Contains(got[0], EventDegraded) || !strings.Contains(got[0], chained.ID().String()) {
		t.Errorf("events = %q, want one %s naming the ref", got, EventDegraded)
	}
	if due := tr.missing.due(later); len(due) != 0 {
		t.Errorf("the prober still waits for %v", due)
	}
}

// TestFollowChainsReachesTheTracker: the switch the chain tests set is the
// only difference between a tracker that refuses a chain and one that follows
// it, here through a Guaranteed ref's lister.
func TestFollowChainsReachesTheTracker(t *testing.T) {
	ctx := context.Background()
	for _, follow := range []bool{false, true} {
		t.Run(fmt.Sprintf("follow %v", follow), func(t *testing.T) {
			reads := 0
			cm := schema.GroupKind{Kind: "ConfigMap"}
			r := newFakeRigWith(t, SharedOptions{Listers: map[schema.GroupKind]Lister{cm: func(ns, name string) (*unstructured.Unstructured, error) {
				reads++
				u := &unstructured.Unstructured{Object: map[string]any{}}
				u.SetNamespace(ns)
				u.SetName(name)
				return u, nil
			}}}, holderObject("ns", "h"), namespaceObject("ns"))
			if follow {
				r.s.FollowChains()
			}
			tr := r.track(t, widgetGK)
			ref := Ref{GroupKind: cm, Namespace: "ns", Name: "config", Guaranteed: true, Next: func(*unstructured.Unstructured) ([]Ref, error) {
				return []Ref{SecretRef("ns", "behind")}, nil
			}}
			r.setRefs("h", ref)
			a, err := tr.Acquire(ctx, r.currentHolder(t))
			if err != nil {
				t.Fatal(err)
			}
			_, ok := Resolved(WithResolved(ctx, a), ref)
			names := ReadRecord(r.currentHolder(t)).Names(SecretRef("ns", "behind").ID())
			if refused := errors.Is(a.Err, ErrChainsNotSupported); refused == follow || ok != follow || names != follow || (reads > 0) != follow {
				t.Errorf("refused %v, resolved %v, recorded %v, reads %d, with the switch %v", refused, ok, names, reads, follow)
			}
		})
	}
}

// TestNothingInProductionFollowsChains: no non-test file of this package, or
// of reconcilertest, turns its switch on. The switches are unexported, so
// nothing outside the two packages can; inside, the one place that sets one
// copies the tests' hook, whose zero value refuses.
func TestNothingInProductionFollowsChains(t *testing.T) {
	var names []string
	for _, dir := range []string{".", "../reconciler/reconcilertest"} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".go") && !strings.HasSuffix(e.Name(), "_test.go") {
				names = append(names, filepath.Join(dir, e.Name()))
			}
		}
	}
	fset := token.NewFileSet()
	src := func(n ast.Node) string {
		var b strings.Builder
		_ = printer.Fprint(&b, fset, n)
		return b.String()
	}
	isSwitch := func(e ast.Expr) bool {
		switch e := e.(type) {
		case *ast.Ident:
			return e.Name == "followChains"
		case *ast.SelectorExpr:
			return e.Sel.Name == "followChains"
		}
		return false
	}
	copies, fields, vars := 0, 0, 0
	for _, name := range names {
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.Field:
				for _, name := range n.Names {
					if name.Name == "followChains" {
						fields++
					}
				}
			case *ast.ValueSpec:
				for _, name := range n.Names {
					if name.Name == "followChains" {
						vars++
						if len(n.Values) != 0 {
							t.Errorf("%s: %s", fset.Position(n.Pos()), src(n))
						}
					}
				}
			case *ast.AssignStmt:
				for _, l := range n.Lhs {
					if isSwitch(l) {
						t.Errorf("%s: %s", fset.Position(n.Pos()), src(n))
					}
				}
			case *ast.UnaryExpr:
				if n.Op == token.AND && isSwitch(n.X) {
					t.Errorf("%s: %s", fset.Position(n.Pos()), src(n))
				}
			case *ast.KeyValueExpr:
				if !isSwitch(n.Key) {
					break
				}
				copies++
				if v := src(n.Value); v != "s.hooks.followChains" {
					t.Errorf("%s: %s, want the tests' hook", fset.Position(n.Pos()), src(n))
				}
			}
			return true
		})
	}
	if copies != 1 || fields != 2 || vars != 1 {
		t.Errorf("%d copies of the hook, %d fields and %d variables, want 1, 2 and 1: the check no longer sees the code it guards", copies, fields, vars)
	}
}
