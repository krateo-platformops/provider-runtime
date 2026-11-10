package reconciler

import (
	"context"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
	"github.com/krateo-platformops/provider-runtime/pkg/resource/fake"
	"github.com/krateo-platformops/provider-runtime/pkg/tracing"
)

// D8 (docs/design-tracing.md §13): every reconcile is its own trace. The root
// links to the object's krateo.io/traceparent and is never its child; only the
// first act step for a new value joins the upstream trace.

// passSpans are the spans of the pass whose root is root: those in its trace,
// and those that link to it.
func passSpans(rec *tracetest.SpanRecorder, root sdktrace.ReadOnlySpan) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, sp := range rec.Ended() {
		if sp.SpanContext().TraceID() == root.SpanContext().TraceID() {
			out = append(out, sp)
			continue
		}
		for _, l := range sp.Links() {
			if l.SpanContext.SpanID() == root.SpanContext().SpanID() {
				out = append(out, sp)
				break
			}
		}
	}
	return out
}

// childrenOf counts the ended spans whose parent is tp's span. (tpA and tpB
// share a trace ID, so spansIn can't tell their children apart.)
func childrenOf(t *testing.T, rec *tracetest.SpanRecorder, tp string) int {
	t.Helper()
	r := remote(t, tp)
	n := 0
	for _, sp := range rec.Ended() {
		if sp.Parent().TraceID() == r.TraceID() && sp.Parent().SpanID() == r.SpanID() {
			n++
		}
	}
	return n
}

// D8 test 1: one reconcile is one trace in every mode in which the pass doesn't
// act. The root is a new trace, linked to the upstream when there is one; every
// span of the pass is in the root's trace; nothing is in the upstream's.
func TestD8OneReconcileOneTrace(t *testing.T) {
	type built struct {
		s   *traceServer
		r   *Reconciler
		rec *tracetest.SpanRecorder
	}
	plain := func(obj *Widget, o ...ReconcilerOption) func(t *testing.T) built {
		return func(t *testing.T) built {
			s := newTraceServer(t, obj)
			tp, rec := tracer(t)
			return built{s, newTraceReconciler(s, &widgetProvider{ops: s.ops}, append([]ReconcilerOption{WithTracerProvider(tp)}, o...)...), rec}
		}
	}
	cases := []struct {
		name  string
		build func(t *testing.T) built
		mode  tracing.Mode
		tp    string
	}{
		{name: "NoAnnotation", build: plain(initialized(newWidget("w", nil))), mode: tracing.NoParent},
		{name: "NewValueObserveOnly", build: plain(initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))), mode: tracing.Parent, tp: tpA},
		{name: "Continued", build: plain(initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA, meta.AnnotationKeyTraceparentContinued, tpA)))), mode: tracing.Link, tp: tpA},
		{name: "WindowZeroObserveOnly", build: plain(initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA))), WithTraceContinueWindow(0)), mode: tracing.Parent, tp: tpA},
		{name: "Paused", build: plain(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA, meta.AnnotationKeyReconciliationPaused, "true"))), mode: tracing.Parent, tp: tpA},
		{name: "Deferred", build: func(t *testing.T) built {
			s := newTraceServer(t, newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))
			tp, rec := tracer(t)
			g := newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: 1})
			p := &widgetProvider{ops: s.ops, key: "api.example.com"}
			p.observeHook = func(ctx context.Context, _ *Widget) {
				ratelimit.Report(ctx, ratelimit.Signal{Key: "api.example.com", Throttled: true, RetryAfter: time.Minute})
			}
			return built{s, newTraceReconciler(s, p, WithTracerProvider(tp), WithRateLimitGovernor(g)), rec}
		}, mode: tracing.Parent, tp: tpA},
		{name: "MarkerConflict", build: func(t *testing.T) built {
			s := newTraceServer(t, newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))
			s.faults.patch = func(client.Object) error { return conflictErr() }
			tp, rec := tracer(t)
			return built{s, newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp)), rec}
		}, mode: tracing.Parent, tp: tpA},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := tc.build(t)
			_, _ = b.r.Reconcile(context.Background(), reqFor("w"))
			if n := b.s.ops.count("act-update"); n != 0 {
				t.Fatalf("ops = %v: this case must not act", b.s.ops.list())
			}
			root := lastRoot(t, b.rec)
			wantMode(t, root, tc.mode, tc.tp)
			ps := passSpans(b.rec, root)
			if len(ps) != len(b.rec.Ended()) {
				t.Errorf("%d of %d spans belong to the pass's trace", len(ps), len(b.rec.Ended()))
			}
			for _, sp := range ps {
				if sp.SpanContext().TraceID() != root.SpanContext().TraceID() {
					t.Errorf("%s left the root's trace on a pass that didn't act", sp.Name())
				}
			}
			if tc.tp != "" {
				if n := spansIn(t, b.rec, tc.tp); n != 0 {
					t.Errorf("%d spans joined the upstream trace on a pass that didn't act", n)
				}
			}
		})
	}
}

// D8 test 2: the first act for a new value is parented on the upstream, with a
// link back to its root, on the update, create and delete paths. The marker is
// written before it, and the rest of the pass stays in the root's trace.
func TestD8FirstActJoinsTheUpstream(t *testing.T) {
	deleting := initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))
	deleting.Finalizers = []string{FinalizerName}
	now := metav1.Now()
	deleting.DeletionTimestamp = &now

	for name, tc := range map[string]struct {
		obj    *Widget
		exists func(*Widget) bool
		span   string
		op     string
	}{
		"Update": {obj: newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)), span: tracing.SpanUpdate, op: "act-update"},
		"Create": {obj: newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)), exists: func(*Widget) bool { return false }, span: tracing.SpanCreate, op: "create"},
		"Delete": {obj: deleting, span: tracing.SpanDelete, op: "act-delete"},
	} {
		t.Run(name, func(t *testing.T) {
			s := newTraceServer(t, tc.obj)
			tp, rec := tracer(t)
			_, _ = newTraceReconciler(s, &widgetProvider{ops: s.ops, exists: tc.exists}, WithTracerProvider(tp)).Reconcile(context.Background(), reqFor("w"))
			root := lastRoot(t, rec)
			wantMode(t, root, tracing.Parent, tpA)
			acts := actSpans(rec, tc.span)
			if len(acts) != 1 {
				t.Fatalf("%d %s spans, want 1", len(acts), tc.span)
			}
			wantAct(t, acts[0], root, tpA)
			if n := spansIn(t, rec, tpA); n != 1 {
				t.Errorf("%d spans in the upstream trace, want only the act", n)
			}
			for _, sp := range rec.Ended() {
				if sp.Name() == tracing.SpanConnect || sp.Name() == tracing.SpanObserve {
					if sp.SpanContext().TraceID() != root.SpanContext().TraceID() {
						t.Errorf("%s left the root's trace", sp.Name())
					}
				}
			}
			if i, j := s.ops.index("patch"), s.ops.index(tc.op); i < 0 || j < 0 || i > j {
				t.Errorf("ops = %v: the marker must be written before the act", s.ops.list())
			}
		})
	}
}

// D8 test 3: after the marker, a later act isn't parented: the value is
// continued, so the act is an ordinary child of its root.
func TestD8SecondActIsNotParented(t *testing.T) {
	s := newTraceServer(t, newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))
	tp, rec := tracer(t)
	r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))
	mustReconcile(t, r, "w") // acts and marks

	w := s.widget("w")
	w.Spec.Wanted = "more"
	if err := s.raw.Update(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, r, "w")
	root := lastRoot(t, rec)
	wantMode(t, root, tracing.Link, tpA)
	acts := actSpans(rec, tracing.SpanUpdate)
	if len(acts) != 2 {
		t.Fatalf("%d update spans, want 2", len(acts))
	}
	wantAct(t, acts[1], root, "")
	if n := spansIn(t, rec, tpA); n != 1 {
		t.Errorf("%d spans in the upstream trace, want only the first act", n)
	}
}

// D8 test 4: observe-only passes inside the window aren't parented: none of
// them, nor any of their children, joins the upstream trace. This is the
// krateo-057 failure D8 fixes: objects stamped by their creating composition,
// and re-stamped on every composition reconcile, joined its trace for ten
// minutes of polls after each stamp.
func TestD8ObserveOnlyPassesInTheWindowStayOut(t *testing.T) {
	s := newTraceServer(t, initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA))))
	tp, rec := tracer(t)
	r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))
	const polls = 5
	for range polls {
		mustReconcile(t, r, "w")
	}
	rs := roots(rec)
	if len(rs) != polls {
		t.Fatalf("%d roots, want %d", len(rs), polls)
	}
	traces := map[string]bool{}
	for _, root := range rs {
		wantMode(t, root, tracing.Parent, tpA)
		traces[root.SpanContext().TraceID().String()] = true
	}
	if len(traces) != polls {
		t.Errorf("%d polls made %d traces, want one each", polls, len(traces))
	}
	if n := spansIn(t, rec, tpA); n != 0 {
		t.Errorf("%d spans of observe-only passes joined the upstream trace, want 0", n)
	}
	if s.ops.count("patch") != 0 {
		t.Error("an observe-only pass inside the window wrote the marker")
	}
}

// D8 test 5: the window is only the fallback that writes the marker when no
// pass acts. It parents nothing, and a later act isn't parented.
func TestD8WindowFallbackWritesTheMarker(t *testing.T) {
	s := newTraceServer(t, initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA))))
	tp, rec := tracer(t)
	r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))
	mustReconcile(t, r, "w")
	backdate(r, "w", defaultTraceContinueWindow)
	mustReconcile(t, r, "w")
	wantMode(t, lastRoot(t, rec), tracing.Link, tpA)
	if got := s.widget("w").GetAnnotations()[meta.AnnotationKeyTraceparentContinued]; got != tpA {
		t.Fatalf("marker = %q after the window closed, want %s", got, tpA)
	}

	w := s.widget("w")
	w.Spec.Wanted = "later"
	if err := s.raw.Update(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, r, "w")
	acts := actSpans(rec, tracing.SpanUpdate)
	if len(acts) != 1 {
		t.Fatalf("%d update spans, want 1", len(acts))
	}
	wantAct(t, acts[0], lastRoot(t, rec), "")
	if n := spansIn(t, rec, tpA); n != 0 {
		t.Errorf("%d spans in the upstream trace, want 0", n)
	}
}

// D8 test 6: window 0 writes the marker before Connect on the first pass; if
// that pass acts, its act is still the first act for the value and joins.
func TestD8WindowZeroFirstActJoins(t *testing.T) {
	s := newTraceServer(t, newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))
	tp, rec := tracer(t)
	mustReconcile(t, newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp), WithTraceContinueWindow(0)), "w")
	root := lastRoot(t, rec)
	wantMode(t, root, tracing.Parent, tpA)
	acts := actSpans(rec, tracing.SpanUpdate)
	if len(acts) != 1 {
		t.Fatalf("%d update spans, want 1", len(acts))
	}
	wantAct(t, acts[0], root, tpA)
	if i, j := s.ops.index("patch"), s.ops.index("connect"); i < 0 || i > j {
		t.Errorf("ops = %v, want the marker written before Connect", s.ops.list())
	}
}

// D8 test 7: a failed marker write (Forbidden) lets the pass go on; its act is
// still the first act for the value and joins. The memo then holds the value
// as continued, so the next act doesn't.
func TestD8FailedMarkerWriteStillJoinsTheFirstAct(t *testing.T) {
	s := newTraceServer(t, newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))
	s.faults.patch = func(client.Object) error { return forbiddenErr() }
	tp, rec := tracer(t)
	r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))
	mustReconcile(t, r, "w")
	acts := actSpans(rec, tracing.SpanUpdate)
	if len(acts) != 1 {
		t.Fatalf("%d update spans, want 1", len(acts))
	}
	wantAct(t, acts[0], lastRoot(t, rec), tpA)

	w := s.widget("w")
	w.Spec.Wanted = "more"
	if err := s.raw.Update(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, r, "w")
	acts = actSpans(rec, tracing.SpanUpdate)
	if len(acts) != 2 {
		t.Fatalf("%d update spans, want 2", len(acts))
	}
	wantAct(t, acts[1], lastRoot(t, rec), "")
}

// D8 test 8: D6's Analyzing → Open remediation still joins. The Open pass's
// Update, which starts the check Pod, is parented on B, the traceparent stamped
// from incident.analyze before Open. The Analyzing pass that only observes B
// puts nothing in B's trace, and the poll after Open only links.
func TestD8AnalyzingToOpenRemediationJoins(t *testing.T) {
	w := newWidget("inc", ann(meta.AnnotationKeyTraceparent, tpA))
	w.Status.State = "Analyzing"
	s := newTraceServer(t, w)
	tp, rec := tracer(t)
	r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))

	mustReconcile(t, r, "inc") // created with A: the first pass acts, and its act joins A
	wantAct(t, actSpans(rec, tracing.SpanUpdate)[0], lastRoot(t, rec), tpA)

	s.annotate("inc", ann(meta.AnnotationKeyTraceparent, tpB))
	mustReconcile(t, r, "inc") // Analyzing: observe-only
	wantMode(t, lastRoot(t, rec), tracing.Parent, tpB)
	if n := childrenOf(t, rec, tpB); n != 0 {
		t.Errorf("the observe-only Analyzing pass parented %d spans on B", n)
	}

	s.setStatus("inc", func(st *WidgetStatus) { st.State, st.Script = "Open", "fix.sh" })
	mustReconcile(t, r, "inc") // Open: acts
	root := lastRoot(t, rec)
	wantMode(t, root, tracing.Parent, tpB)
	acts := actSpans(rec, tracing.SpanUpdate)
	if len(acts) != 2 {
		t.Fatalf("%d update spans, want 2", len(acts))
	}
	wantAct(t, acts[1], root, tpB)

	mustReconcile(t, r, "inc")
	wantMode(t, lastRoot(t, rec), tracing.Link, tpB)
	if n := childrenOf(t, rec, tpB); n != 1 {
		t.Errorf("%d spans parented on B, want only the Open pass's Update", n)
	}
}

// D8 test 9, regression coverage for the krateo-057 report (which was not a
// marker bug: 28/28 markers were written). The marker reaches the API server on
// the real path: envtest's kube-apiserver, a client that reads through an
// informer cache as a manager's does, and the reconciler's default finalizer
// and critical annotation updater. Once by the first act, once by the window
// fallback.
func TestD8EnvtestMarkerIsWrittenOnTheRealPath(t *testing.T) {
	requireTraceAPIServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sch := widgetScheme()
	informers, err := cache.New(traceEnv.Config, cache.Options{Scheme: sch})
	if err != nil {
		t.Fatal(err)
	}
	// Start the Widget informer before the cache, so WaitForCacheSync covers it.
	if _, err := informers.GetInformer(ctx, &Widget{}); err != nil {
		t.Fatal(err)
	}
	go func() { _ = informers.Start(ctx) }()
	if !informers.WaitForCacheSync(ctx) {
		t.Fatal("the cache didn't sync")
	}
	cached, err := client.New(traceEnv.Config, client.Options{Scheme: sch, Cache: &client.CacheOptions{Reader: informers}})
	if err != nil {
		t.Fatal(err)
	}
	// No WithFinalizer and no WithCriticalAnnotationUpdater: the defaults.
	reconcilerFor := func(p *widgetProvider, o ...ReconcilerOption) *Reconciler {
		return NewReconciler(&fake.Manager{Client: cached, Scheme: sch}, resource.ManagedKind(widgetGVK), append([]ReconcilerOption{WithExternalConnecter(p)}, o...)...)
	}
	create := func(t *testing.T, name string) types.NamespacedName {
		key := createLiveWidget(t, name, ann(meta.AnnotationKeyTraceparent, tpA))
		// Runs before createLiveWidget's delete: drop the default finalizer.
		t.Cleanup(func() {
			_ = traceClient.Patch(context.Background(), &Widget{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "demo"}},
				client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`)))
		})
		return key
	}
	marker := func(t *testing.T, key types.NamespacedName) string {
		return liveWidget(t, key).GetAnnotations()[meta.AnnotationKeyTraceparentContinued]
	}
	// untilMarked reconciles until the live object carries the marker, as the
	// workqueue retries a pass that read a lagging cache or met a conflict.
	untilMarked := func(t *testing.T, r *Reconciler, key types.NamespacedName) {
		t.Helper()
		for range 100 {
			_, _ = r.Reconcile(ctx, reqFor(key.Name))
			if marker(t, key) == tpA {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("the marker never reached the API server: annotations %v", liveWidget(t, key).GetAnnotations())
	}
	// cachedSees waits until the cache serves the object in the state ok wants.
	cachedSees := func(t *testing.T, key types.NamespacedName, ok func(*Widget) bool) {
		t.Helper()
		for range 250 {
			w := &Widget{}
			if cached.Get(ctx, key, w) == nil && ok(w) {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("the cache never served %s as wanted", key)
	}

	t.Run("ByTheFirstAct", func(t *testing.T) {
		key := create(t, "real-act")
		cachedSees(t, key, func(*Widget) bool { return true })
		tp, rec := tracer(t)
		p := &widgetProvider{ops: &apiOps{}}
		untilMarked(t, reconcilerFor(p, WithTracerProvider(tp)), key)
		acts := actSpans(rec, tracing.SpanUpdate)
		if len(acts) == 0 {
			t.Fatalf("no update span; provider ops %v", p.ops.list())
		}
		if r := remote(t, tpA); acts[0].Parent().SpanID() != r.SpanID() || acts[0].SpanContext().TraceID() != r.TraceID() {
			t.Errorf("the first act's parent is %s, want the upstream", acts[0].Parent().SpanID())
		}
		if got := liveWidget(t, key).GetFinalizers(); len(got) != 1 || got[0] != FinalizerName {
			t.Errorf("finalizers = %v: the default finalizer didn't run", got)
		}
	})

	t.Run("ByTheWindowFallback", func(t *testing.T) {
		key := create(t, "real-window")
		w := liveWidget(t, key)
		w.Status.Initialized = true
		if err := traceClient.Status().Update(ctx, w); err != nil {
			t.Fatal(err)
		}
		cachedSees(t, key, func(w *Widget) bool { return w.Status.Initialized })
		tp, rec := tracer(t)
		p := &widgetProvider{ops: &apiOps{}}
		r := reconcilerFor(p, WithTracerProvider(tp), WithTraceContinueWindow(50*time.Millisecond))
		// The first pass opens the window: observe-only, so no marker.
		_, _ = r.Reconcile(ctx, reqFor(key.Name))
		if got := marker(t, key); got != "" {
			t.Fatalf("the first observe-only pass wrote the marker %q", got)
		}
		time.Sleep(60 * time.Millisecond)
		untilMarked(t, r, key)
		if n := p.ops.count("act-update"); n != 0 {
			t.Errorf("provider ops %v: the fallback case must not act", p.ops.list())
		}
		if n := spansIn(t, rec, tpA); n != 0 {
			t.Errorf("%d spans in the upstream trace, want 0", n)
		}
	})
}

// D8 test 10: within one pass only the first act step joins the upstream. No
// reconcile path acts twice today; this keeps the rule if one ever does.
func TestD8OnlyThePassesFirstActStepJoins(t *testing.T) {
	tp, rec := tracer(t)
	p := &tracePass{tp: tp, d: tracing.Decision{Mode: tracing.Parent, Remote: remote(t, tpA)}}
	ctx, root := tp.Tracer("test").Start(context.Background(), tracing.SpanReconcile)
	_, first := p.actStep(ctx, tracing.SpanCreate)
	first.End()
	_, second := p.actStep(ctx, tracing.SpanUpdate)
	second.End()
	root.End()
	r := lastRoot(t, rec)
	wantAct(t, actSpans(rec, tracing.SpanCreate)[0], r, tpA)
	wantAct(t, actSpans(rec, tracing.SpanUpdate)[0], r, "")
}
