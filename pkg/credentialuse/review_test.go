package credentialuse

// Deterministic reproductions of the independent review of #59: three ways
// the A2 skip could rest on an observation of an earlier run of the record
// (H1), and the grace measured after R2's reads (M4). Each failed on
// 04b389d. The skip now needs the exact holder version the pass works from
// to be the one the informer delivered (verified.go), and R2 reads the clock
// before its first read (release.go). The last test is a Low: first sights
// are pruned.

import (
	"context"
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

type stepClock struct{ t *time.Time }

func (c stepClock) Now() time.Time                  { *c.t = c.t.Add(time.Millisecond); return *c.t }
func (c stepClock) Since(t time.Time) time.Duration { return c.t.Sub(t) }

// lossy runs the call, then (when lose says so) drops the response.
type lossy struct {
	server
	lose func(gk schema.GroupKind, name string) bool
	fail func(gk schema.GroupKind, name string) bool
}

func (l lossy) Patch(ctx context.Context, gk schema.GroupKind, ns, name string, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error) {
	if l.fail != nil && l.fail(gk, name) {
		return nil, apimodel.ErrInjected
	}
	out, err := l.server.Patch(ctx, gk, ns, name, pt, data)
	if l.lose != nil && l.lose(gk, name) {
		return nil, errors.New("lost")
	}
	return out, err
}

func drain(p *process, now time.Time) {
	for p.cache.Pending() {
		if ch, ok := p.cache.Next(); ok {
			p.deliver(ch, now)
		}
	}
}

func setupSkipHole(t *testing.T) (*apimodel.Model, *time.Time, *process, ID) {
	now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	clk := &now
	m := apimodel.New(func() time.Time { return *clk })
	ns := "n"
	must(m.Create(nsKey(ns), metav1.ObjectMeta{}))
	must(m.Create(secretKey(ns, "s"), metav1.ObjectMeta{}))
	must(m.Create(widgetKey(ns, "h"), metav1.ObjectMeta{Finalizers: []string{ownerFinalizer}}))
	pa := newProcess("A", m, now)
	return m, clk, pa, SecretRef(ns, "s").ID()
}

func passOn(t *testing.T, m *apimodel.Model, srv server, v views, ver *verified, clk *time.Time, refs []Ref) (Acquired, error) {
	t.Helper()
	obj, err := m.Get(widgetKey("n", "h"))
	if err != nil {
		t.Fatal(err)
	}
	pr := newProtocol(srv, v, ver, newFirstSight(), stepClock{clk}, testGrace)
	return pr.acquire(context.Background(), obj, refs)
}

func liveHeld(m *apimodel.Model, p ID) bool {
	o, err := m.Get(keyOf(p))
	if err != nil {
		return false
	}
	f, _ := FinalizerName(widgetGK, "n")
	return hasFinalizer(o, f)
}

// Single process: the re-adding A1's response is lost; the holder
// informer hasn't delivered the away version; the next pass skips on the
// first run's recordSince.
func TestSkipAfterALostReAdd(t *testing.T) {
	m, clk, pa, p := setupSkipHole(t)
	srv := modelServer{m: m}
	vA := cacheViews{&pa}
	ref := []Ref{SecretRef("n", "s")}
	// Steady state in A: two passes, informer delivered in between.
	if _, err := passOn(t, m, srv, vA, pa.ver, clk, ref); err != nil {
		t.Fatal(err)
	}
	drain(pa, clk.Add(time.Millisecond))
	*clk = clk.Add(2 * time.Millisecond)
	if _, err := passOn(t, m, srv, vA, pa.ver, clk, ref); err != nil {
		t.Fatal(err)
	}
	drain(pa, clk.Add(time.Millisecond))
	*clk = clk.Add(2 * time.Millisecond)
	if a, _ := passOn(t, m, countNoWrites{srv, t}, vA, pa.ver, clk, ref); len(a.Held) != 1 {
		t.Fatalf("steady state didn't skip: %+v", a)
	}
	// Away: A1 removes P. A's informer stalls from here.
	if _, err := passOn(t, m, srv, vA, pa.ver, clk, nil); err != nil {
		t.Fatal(err)
	}
	// A releaser (anywhere) sees no user and releases.
	rel := newProtocol(srv, noViews{}, newVerified(widgetGK), newFirstSight(), stepClock{clk}, testGrace)
	res, err := rel.release(context.Background(), "n", []ID{p})
	if err != nil || len(res.Released) != 1 {
		t.Fatalf("release: %+v %v", res, err)
	}
	// Back: A1 lands, response lost.
	l := lossy{server: srv, lose: func(gk schema.GroupKind, _ string) bool { return gk == widgetGK }}
	a, err := passOn(t, m, l, vA, pa.ver, clk, ref)
	t.Logf("lost re-add pass: %+v %v", a, err)
	// Next pass, reading H live (or from a manager cache ahead of the core informer).
	a, err = passOn(t, m, srv, vA, pa.ver, clk, ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Held) == 1 && !liveHeld(m, p) {
		t.Errorf("pass reported %v held, and P has no finalizer", a.Held)
	}
}

// Two processes: B removes and re-adds P, B's A2 fails; A's informer has
// delivered the away version but not the back one; A's stale observed lets
// it skip.
func TestSkipAcrossProcesses(t *testing.T) {
	m, clk, pa, p := setupSkipHole(t)
	srv := modelServer{m: m}
	vA := cacheViews{&pa}
	ref := []Ref{SecretRef("n", "s")}
	if _, err := passOn(t, m, srv, vA, pa.ver, clk, ref); err != nil {
		t.Fatal(err)
	}
	drain(pa, clk.Add(time.Millisecond))
	*clk = clk.Add(2 * time.Millisecond)
	if _, err := passOn(t, m, srv, vA, pa.ver, clk, ref); err != nil {
		t.Fatal(err)
	}
	drain(pa, clk.Add(time.Millisecond))
	*clk = clk.Add(2 * time.Millisecond)
	// B: away.
	vB := newVerified(widgetGK)
	if _, err := passOn(t, m, srv, noViews{}, vB, clk, nil); err != nil {
		t.Fatal(err)
	}
	// A's informer delivers the away version only.
	if ch, ok := pa.cache.Next(); ok {
		pa.deliver(ch, clk.Add(time.Millisecond))
	}
	*clk = clk.Add(2 * time.Millisecond)
	rel := newProtocol(srv, noViews{}, newVerified(widgetGK), newFirstSight(), stepClock{clk}, testGrace)
	res, err := rel.release(context.Background(), "n", []ID{p})
	if err != nil || len(res.Released) != 1 {
		t.Fatalf("release: %+v %v", res, err)
	}
	// B: back, A2 fails (B is Degraded).
	f := lossy{server: srv, fail: func(gk schema.GroupKind, _ string) bool { return gk == secretGroupKind }}
	a, err := passOn(t, m, f, noViews{}, vB, clk, ref)
	t.Logf("B's re-add pass: %+v %v", a, err)
	// A's pass on the current holder.
	a, err = passOn(t, m, srv, vA, pa.ver, clk, ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Held) == 1 && !liveHeld(m, p) {
		t.Errorf("A's pass reported %v held (Degraded=%v), and P has no finalizer", a.Held, a.Degraded)
	}
}

type countNoWrites struct {
	server
	t *testing.T
}

func (c countNoWrites) Patch(ctx context.Context, gk schema.GroupKind, ns, name string, pt types.PatchType, data []byte) (*metav1.PartialObjectMetadata, error) {
	c.t.Logf("steady-state pass wrote %s/%s", gk.Kind, name)
	return c.server.Patch(ctx, gk, ns, name, pt, data)
}

// Two processes, no lost response: B removes and re-adds P (A2 fails); A's
// informer has delivered nothing since the steady state.
func TestSkipAcrossProcessesStaleInformer(t *testing.T) {
	m, clk, pa, p := setupSkipHole(t)
	srv := modelServer{m: m}
	vA := cacheViews{&pa}
	ref := []Ref{SecretRef("n", "s")}
	for range 2 {
		if _, err := passOn(t, m, srv, vA, pa.ver, clk, ref); err != nil {
			t.Fatal(err)
		}
		drain(pa, clk.Add(time.Millisecond))
		*clk = clk.Add(2 * time.Millisecond)
	}
	vB := newVerified(widgetGK)
	if _, err := passOn(t, m, srv, noViews{}, vB, clk, nil); err != nil {
		t.Fatal(err)
	}
	rel := newProtocol(srv, noViews{}, newVerified(widgetGK), newFirstSight(), stepClock{clk}, testGrace)
	if res, err := rel.release(context.Background(), "n", []ID{p}); err != nil || len(res.Released) != 1 {
		t.Fatalf("release: %+v %v", res, err)
	}
	f := lossy{server: srv, fail: func(gk schema.GroupKind, _ string) bool { return gk == secretGroupKind }}
	_, _ = passOn(t, m, f, noViews{}, vB, clk, ref)
	a, err := passOn(t, m, srv, vA, pa.ver, clk, ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Held) == 1 && !liveHeld(m, p) {
		t.Errorf("A's pass reported %v held (Degraded=%v), and P has no finalizer", a.Held, a.Degraded)
	}
}

type listHook struct {
	server
	before func()
}

func (l listHook) List(ctx context.Context, gk schema.GroupKind, ns string) ([]metav1.PartialObjectMetadata, error) {
	l.before()
	return l.server.List(ctx, gk, ns)
}

// R2's reads start inside the grace; the decision's clock is read after the
// list. A namespace deletion that starts inside the grace, after the
// Namespace GET, is then released over.
func TestReleaseMeasuresTheGraceBeforeTheReads(t *testing.T) {
	now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	clk := &now
	m := apimodel.New(func() time.Time { return *clk })
	seed(m, "n", "s", []string{"h"}, true)
	p := SecretRef("n", "s").ID()
	must(m.Delete(keyOf(p)))
	seen := newFirstSight()
	srv := modelServer{m: m}
	pr := newProtocol(srv, noViews{}, newVerified(widgetGK), seen, stepClock{clk}, 60*time.Second)
	res, _ := pr.release(context.Background(), "n", []ID{p})
	t.Logf("first batch: %+v", res.Decisions)
	o, err := m.Get(keyOf(p))
	if err != nil {
		t.Fatalf("the first batch, inside the grace, released P: %+v", res)
	}
	graceEnd := GraceEnd(o.DeletionTimestamp.Time, seen.see(o.UID, time.Time{}), 60*time.Second)
	*clk = graceEnd.Add(-3 * time.Millisecond)
	var nsDel time.Time
	hook := listHook{server: srv, before: func() { nsDel = *clk; must(m.Delete(nsKey("n"))) }}
	pr = newProtocol(hook, noViews{}, newVerified(widgetGK), seen, stepClock{clk}, 60*time.Second)
	res, err = pr.release(context.Background(), "n", []ID{p})
	t.Logf("second batch: released=%v decisions=%+v err=%v", res.Released, res.Decisions, err)
	if len(res.Released) == 1 && nsDel.Before(graceEnd) {
		t.Errorf("RELEASED with the namespace deleted at %s, before the grace ended at %s (namespace GET at %s)",
			nsDel.Format("05.000"), graceEnd.Format("05.000"), res.Decisions[0].NamespaceReadAt.Format("05.000"))
	}
}

// First sights are kept while P waits out its grace, and dropped once it is
// released, so the map holds only the objects being deleted that this
// process still holds (a Low of #59's review).
func TestReleasePrunesFirstSight(t *testing.T) {
	now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	clk := &now
	m := apimodel.New(func() time.Time { return *clk })
	seed(m, "n", "s", []string{"h"}, true)
	p := SecretRef("n", "s").ID()
	must(m.Delete(keyOf(p)))
	seen := newFirstSight()
	pr := newProtocol(modelServer{m: m}, noViews{}, newVerified(widgetGK), seen, stepClock{clk}, testGrace)
	if res, err := pr.release(context.Background(), "n", []ID{p}); err != nil || len(res.Released) != 0 {
		t.Fatalf("inside the grace: %+v, %v", res, err)
	}
	if len(seen.at) != 1 {
		t.Fatalf("inside the grace, first sights = %v, want P's", seen.at)
	}
	*clk = clk.Add(2 * testGrace)
	if res, err := pr.release(context.Background(), "n", []ID{p}); err != nil || len(res.Released) != 1 {
		t.Fatalf("after the grace: %+v, %v", res, err)
	}
	if len(seen.at) != 0 {
		t.Errorf("after the release, first sights = %v, want none", seen.at)
	}
}

// The equal-lag stale pass, the window §6.4 accepts (the re-review of #59).
// A's typed cache and core informer are equally behind, at the steady-state
// version V. B removes P, a releaser releases it, and B puts it back with a
// failed A2. A's pass works from V, which A's informer delivered, so the
// exact-version rule lets it skip: it reports P held while P has no
// finalizer. That window lasts until A's informer delivers B's versions;
// then A's next pass works from the current version and writes.
func TestStalePassEqualLagWindow(t *testing.T) {
	m, clk, pa, p := setupSkipHole(t)
	srv := modelServer{m: m}
	vA := cacheViews{&pa}
	ref := []Ref{SecretRef("n", "s")}
	for range 2 {
		if _, err := passOn(t, m, srv, vA, pa.ver, clk, ref); err != nil {
			t.Fatal(err)
		}
		drain(pa, clk.Add(time.Millisecond))
		*clk = clk.Add(2 * time.Millisecond)
	}
	stale, _ := m.Get(widgetKey("n", "h")) // V: what A's caches hold
	vB := newVerified(widgetGK)
	if _, err := passOn(t, m, srv, noViews{}, vB, clk, nil); err != nil {
		t.Fatal(err)
	}
	rel := newProtocol(srv, noViews{}, newVerified(widgetGK), newFirstSight(), stepClock{clk}, testGrace)
	if res, err := rel.release(context.Background(), "n", []ID{p}); err != nil || len(res.Released) != 1 {
		t.Fatalf("release: %+v %v", res, err)
	}
	f := lossy{server: srv, fail: func(gk schema.GroupKind, _ string) bool { return gk == secretGroupKind }}
	_, _ = passOn(t, m, f, noViews{}, vB, clk, ref)

	// Inside the window: the stale pass skips.
	pr := newProtocol(srv, vA, pa.ver, newFirstSight(), stepClock{clk}, testGrace)
	a, err := pr.acquire(context.Background(), stale, ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Held) != 1 || liveHeld(m, p) {
		t.Fatalf("expected the accepted window (held reported, P unheld); got held=%v, P held live: %v", a.Held, liveHeld(m, p))
	}

	// The window closes on delivery: a pass from the current version writes.
	drain(pa, clk.Add(time.Millisecond))
	*clk = clk.Add(2 * time.Millisecond)
	a, err = passOn(t, m, srv, vA, pa.ver, clk, ref)
	if err != nil || len(a.Held) != 1 || !liveHeld(m, p) {
		t.Errorf("after delivery: held=%v err=%v, P held live: %v; want it held", a.Held, err, liveHeld(m, p))
	}
}
