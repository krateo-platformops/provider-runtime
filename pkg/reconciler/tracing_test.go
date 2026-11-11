package reconciler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit/httpsignal"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
	"github.com/krateo-platformops/provider-runtime/pkg/tracing"
)

var widgetGR = schema.GroupResource{Group: widgetGVK.Group, Resource: "widgets"}

func conflictErr() error {
	return kerrors.NewConflict(widgetGR, "w", errors.New("the object has been modified"))
}

func forbiddenErr() error {
	return kerrors.NewForbidden(widgetGR, "w", errors.New("denied by a webhook"))
}

func ann(kv ...string) map[string]string {
	m := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

// initialized marks a widget as already initialized, so a pass has no work and
// only observes.
func initialized(w *Widget) *Widget {
	w.Status.Initialized = true
	return w
}

func mustReconcile(t *testing.T, r *Reconciler, name string) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), reqFor(name)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

// design-tracing §10, test 1. A pass that observes and updates makes exactly
// reconcile (Server) with connect, observe and update (Internal) under it. The
// root has exactly the three object attributes, with the Kind alone, plus
// provider_runtime.acted=true because the pass updates (#74), and starts no
// later than the Get.
func TestTracingShape(t *testing.T) {
	s := newTraceServer(t, newWidget("w1", nil))
	tp, rec := tracer(t)
	r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))

	before := time.Now()
	mustReconcile(t, r, "w1")

	root := lastRoot(t, rec)
	if root.SpanKind() != trace.SpanKindServer {
		t.Errorf("root kind = %s, want Server", root.SpanKind())
	}
	if root.StartTime().Before(before) {
		t.Error("root starts before Reconcile was called")
	}
	want := map[attribute.Key]string{"k8s.object.kind": "Widget", "k8s.object.name": "w1", "k8s.object.namespace": "demo"}
	if len(root.Attributes()) != 4 {
		t.Errorf("root attributes = %v, want exactly the three object attributes and acted", root.Attributes())
	}
	for _, kv := range root.Attributes() {
		if kv.Key == tracing.AttrActed {
			if !kv.Value.AsBool() {
				t.Errorf("%s = %v, want true", kv.Key, kv.Value.AsBool())
			}
			continue
		}
		if want[kv.Key] != kv.Value.AsString() {
			t.Errorf("%s = %q, want %q", kv.Key, kv.Value.AsString(), want[kv.Key])
		}
	}

	var children []string
	for _, sp := range rec.Ended() {
		if sp.Name() == tracing.SpanReconcile {
			continue
		}
		children = append(children, sp.Name())
		if sp.SpanKind() != trace.SpanKindInternal || sp.Parent().SpanID() != root.SpanContext().SpanID() {
			t.Errorf("%s: kind %s, parent %s; want Internal under the root", sp.Name(), sp.SpanKind(), sp.Parent().SpanID())
		}
		if sp.StartTime().Before(root.StartTime()) {
			t.Errorf("%s starts before the root", sp.Name())
		}
	}
	if len(children) != 3 || children[0] != "connect" || children[1] != "observe" || children[2] != "update" {
		t.Errorf("children = %v, want [connect observe update]", children)
	}
}

// design-tracing §10, test 2: each rule of §6.2 through Reconcile. Each case
// asserts the root's mode and the writes the API server saw.
func TestTracingModes(t *testing.T) {
	t.Run("Rule1NoAnnotation", func(t *testing.T) {
		s := newTraceServer(t, initialized(newWidget("w", nil)))
		tp, rec := tracer(t)
		mustReconcile(t, newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp)), "w")
		wantMode(t, lastRoot(t, rec), tracing.NoParent, "")
		if s.ops.count("patch") != 0 {
			t.Error("NoParent wrote a marker")
		}
	})

	t.Run("Rule4ObserveOnlyWritesNothing", func(t *testing.T) {
		s := newTraceServer(t, initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA))))
		tp, rec := tracer(t)
		r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))
		mustReconcile(t, r, "w")
		wantMode(t, lastRoot(t, rec), tracing.Parent, tpA)
		mustReconcile(t, r, "w")
		wantMode(t, lastRoot(t, rec), tracing.Parent, tpA)
		if s.ops.count("patch") != 0 {
			t.Errorf("observe-only passes wrote %d markers, want none", s.ops.count("patch"))
		}
	})

	t.Run("FirstActionWritesTheMarkerBeforeTheCall", func(t *testing.T) {
		s := newTraceServer(t, newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))
		tp, rec := tracer(t)
		var patchedAt time.Time
		s.faults.patch = func(client.Object) error { patchedAt = time.Now(); return nil }
		p := &widgetProvider{ops: s.ops}
		p.observeHook = func(_ context.Context, w *Widget) { w.Status.State = "observed-in-memory" }
		var sawRV, storedRV, sawState, sawMarker string
		p.updateHook = func(_ context.Context, w *Widget) {
			sawRV, sawState = w.GetResourceVersion(), w.Status.State
			sawMarker = w.GetAnnotations()[meta.AnnotationKeyTraceparentContinued]
			storedRV = s.widget("w").GetResourceVersion()
		}
		mustReconcile(t, newTraceReconciler(s, p, WithTracerProvider(tp)), "w")

		wantMode(t, lastRoot(t, rec), tracing.Parent, tpA)
		if i, j := s.ops.index("patch"), s.ops.index("act-update"); i < 0 || j < 0 || i > j {
			t.Fatalf("ops = %v, want the marker patch before the Update call", s.ops.list())
		}
		if got := s.widget("w").GetAnnotations()[meta.AnnotationKeyTraceparentContinued]; got != tpA {
			t.Errorf("stored marker = %q, want %s", got, tpA)
		}
		if sawState != "observed-in-memory" {
			t.Errorf("Update saw status.state %q: the marker patch threw away Observe's in-memory change", sawState)
		}
		if sawRV != storedRV || sawMarker != tpA {
			t.Errorf("Update saw resourceVersion %q (stored %q) and marker %q; want the patch's version and the marker", sawRV, storedRV, sawMarker)
		}
		for _, sp := range rec.Ended() {
			if sp.Name() == tracing.SpanUpdate && sp.StartTime().Before(patchedAt) {
				t.Error("the update span started before the marker write")
			}
		}
	})

	t.Run("CreatePathPatchesBeforeCreatePending", func(t *testing.T) {
		s := newTraceServer(t, newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))
		tp, _ := tracer(t)
		p := &widgetProvider{ops: s.ops, exists: func(*Widget) bool { return false }}
		mustReconcile(t, newTraceReconciler(s, p, WithTracerProvider(tp)), "w")
		ops := s.ops.list()
		ip, iu, ic := s.ops.index("patch"), s.ops.index("update"), s.ops.index("create")
		if s.ops.count("patch") != 1 || ip > iu || iu > ic {
			t.Errorf("ops = %v, want one marker patch, then the create-pending update, then create", ops)
		}
	})

	t.Run("DeferredActionWritesNothing", func(t *testing.T) {
		s := newTraceServer(t, newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))
		tp, rec := tracer(t)
		g := newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: 1})
		p := &widgetProvider{ops: s.ops, key: "api.example.com"}
		// Observe hears a throttle, so the Update's gate defers.
		p.observeHook = func(ctx context.Context, _ *Widget) {
			ratelimit.Report(ctx, ratelimit.Signal{Key: "api.example.com", Throttled: true, RetryAfter: time.Minute})
		}
		mustReconcile(t, newTraceReconciler(s, p, WithTracerProvider(tp), WithRateLimitGovernor(g)), "w")
		if s.ops.count("patch") != 0 || s.ops.count("act-update") != 0 {
			t.Errorf("ops = %v, want no marker and no act on a deferred pass", s.ops.list())
		}
		root := lastRoot(t, rec)
		wantMode(t, root, tracing.Parent, tpA)
		if cause, ok := eventAttr(root, tracing.EventRateLimitDeferred, string(tracing.AttrRateLimitCause)); !ok || cause != "throttled" {
			t.Errorf("deferral event cause = %q (%v), want throttled", cause, ok)
		}
	})

	t.Run("Rule2Link", func(t *testing.T) {
		s := newTraceServer(t, newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA, meta.AnnotationKeyTraceparentContinued, tpA)))
		tp, rec := tracer(t)
		mustReconcile(t, newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp)), "w")
		wantMode(t, lastRoot(t, rec), tracing.Link, tpA)
		if s.ops.count("patch") != 0 {
			t.Error("a Link pass wrote a marker")
		}
	})

	t.Run("NewValueParents", func(t *testing.T) {
		s := newTraceServer(t, initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpB, meta.AnnotationKeyTraceparentContinued, tpA))))
		tp, rec := tracer(t)
		mustReconcile(t, newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp)), "w")
		wantMode(t, lastRoot(t, rec), tracing.Parent, tpB)
	})

	t.Run("GenerationOnlyLinks", func(t *testing.T) {
		w := newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA, meta.AnnotationKeyTraceparentContinued, tpA))
		w.Generation = 7
		s := newTraceServer(t, w)
		tp, rec := tracer(t)
		mustReconcile(t, newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp)), "w")
		wantMode(t, lastRoot(t, rec), tracing.Link, tpA)
		if s.ops.count("patch") != 0 {
			t.Error("a generation-only change wrote a marker")
		}
	})

	t.Run("DeletionWithTheSameValueLinks", func(t *testing.T) {
		w := initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA, meta.AnnotationKeyTraceparentContinued, tpA)))
		w.Finalizers = []string{FinalizerName}
		now := metav1.Now()
		w.DeletionTimestamp = &now
		s := newTraceServer(t, w)
		tp, rec := tracer(t)
		mustReconcile(t, newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp)), "w")
		wantMode(t, lastRoot(t, rec), tracing.Link, tpA)
		if s.ops.count("act-delete") != 1 || s.ops.count("patch") != 0 {
			t.Errorf("ops = %v, want the delete and no marker", s.ops.list())
		}
	})

	t.Run("DeletePathWritesTheMarkerBeforeDelete", func(t *testing.T) {
		w := initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))
		w.Finalizers = []string{FinalizerName}
		now := metav1.Now()
		w.DeletionTimestamp = &now
		s := newTraceServer(t, w)
		tp, rec := tracer(t)
		p := &widgetProvider{ops: s.ops, deleteErr: errors.New("boom")}
		r := newTraceReconciler(s, p, WithTracerProvider(tp))
		if _, err := r.Reconcile(context.Background(), reqFor("w")); err == nil {
			t.Fatal("want the Delete error")
		}
		wantMode(t, lastRoot(t, rec), tracing.Parent, tpA)
		if i, j := s.ops.index("patch"), s.ops.index("act-delete"); i < 0 || j < 0 || i > j {
			t.Fatalf("ops = %v, want the marker patch before the Delete call", s.ops.list())
		}
		_, _ = r.Reconcile(context.Background(), reqFor("w"))
		wantMode(t, lastRoot(t, rec), tracing.Link, tpA)
	})

	t.Run("InvalidTraceparentAndInvalidMarker", func(t *testing.T) {
		s := newTraceServer(t,
			initialized(newWidget("bad-tp", ann(meta.AnnotationKeyTraceparent, "garbage"))),
			initialized(newWidget("bad-marker", ann(meta.AnnotationKeyTraceparent, tpA, meta.AnnotationKeyTraceparentContinued, "garbage"))))
		tp, rec := tracer(t)
		r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))
		mustReconcile(t, r, "bad-tp")
		wantMode(t, lastRoot(t, rec), tracing.NoParent, "")
		mustReconcile(t, r, "bad-marker")
		wantMode(t, lastRoot(t, rec), tracing.Parent, tpA)
	})

	t.Run("FlagsOrTracestateAloneLink", func(t *testing.T) {
		unsampled := tpAUnsampled
		s := newTraceServer(t, initialized(newWidget("w", ann(
			meta.AnnotationKeyTraceparent, unsampled, meta.AnnotationKeyTracestate, "vendor=new",
			meta.AnnotationKeyTraceparentContinued, tpA))))
		tp, rec := tracer(t)
		mustReconcile(t, newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp)), "w")
		wantMode(t, lastRoot(t, rec), tracing.Link, tpA)
	})

	t.Run("AnInProcessSpanIsNeverTheParent", func(t *testing.T) {
		s := newTraceServer(t, initialized(newWidget("w", nil)))
		tp, rec := tracer(t)
		ctx, wrapper := tp.Tracer("wrapper").Start(context.Background(), "alert.reconcile")
		if _, err := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp)).Reconcile(ctx, reqFor("w")); err != nil {
			t.Fatal(err)
		}
		wrapper.End()
		root := lastRoot(t, rec)
		wantMode(t, root, tracing.NoParent, "")
		if root.SpanContext().TraceID() == wrapper.SpanContext().TraceID() {
			t.Error("the reconcile span joined the wrapper's trace")
		}
	})
}

// design-tracing §10, test 3: window 0 is the first-sight rule. The first pass
// is Parent and writes the marker right away, after the pause check.
func TestTracingWindowZero(t *testing.T) {
	s := newTraceServer(t,
		initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA))),
		initialized(newWidget("paused", ann(meta.AnnotationKeyTraceparent, tpA, meta.AnnotationKeyReconciliationPaused, "true"))))
	tp, rec := tracer(t)
	r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp), WithTraceContinueWindow(0))

	mustReconcile(t, r, "w")
	wantMode(t, lastRoot(t, rec), tracing.Parent, tpA)
	if i, j := s.ops.index("patch"), s.ops.index("connect"); i < 0 || i > j {
		t.Errorf("ops = %v, want the marker written before Connect", s.ops.list())
	}

	s.ops.reset()
	mustReconcile(t, r, "paused")
	if s.ops.count("patch") != 0 {
		t.Error("window 0 wrote a marker on a paused object")
	}
}

// design-tracing §10, test 4: the window closes. A value first seen a window
// ago gives Link, and the marker is written after the pause check and before
// the gates.
func TestTracingWindowCloses(t *testing.T) {
	s := newTraceServer(t, initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA))))
	tp, rec := tracer(t)
	r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))

	mustReconcile(t, r, "w")
	wantMode(t, lastRoot(t, rec), tracing.Parent, tpA)
	backdate(r, "w", defaultTraceContinueWindow)

	s.ops.reset()
	mustReconcile(t, r, "w")
	wantMode(t, lastRoot(t, rec), tracing.Link, tpA)
	if i, j := s.ops.index("patch"), s.ops.index("connect"); i < 0 || i > j {
		t.Errorf("ops = %v, want the marker written before Connect", s.ops.list())
	}
	if st, ok := memoStateOf(r, "w"); !ok || st != memoMarked {
		t.Errorf("memo state = %v (%v), want marked", st, ok)
	}

	s.ops.reset()
	mustReconcile(t, r, "w")
	wantMode(t, lastRoot(t, rec), tracing.Link, tpA)
	if s.ops.count("patch") != 0 {
		t.Error("a pass after the marker was written wrote it again")
	}
	if _, ok := memoStateOf(r, "w"); ok {
		t.Error("the memo kept the entry after a read showed the marker")
	}
}

// design-tracing §10, test 5: restarts.
func TestTracingRestart(t *testing.T) {
	t.Run("MarkedObjectLinksInANewProcess", func(t *testing.T) {
		s := newTraceServer(t, newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))
		tp, rec := tracer(t)
		mustReconcile(t, newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp)), "w")
		mustReconcile(t, newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp)), "w")
		wantMode(t, lastRoot(t, rec), tracing.Link, tpA)
	})
	t.Run("SeenButNotActedStartsItsOwnWindow", func(t *testing.T) {
		s := newTraceServer(t, initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA))))
		tp, rec := tracer(t)
		r1 := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))
		mustReconcile(t, r1, "w")
		backdate(r1, "w", defaultTraceContinueWindow-time.Second)
		r2 := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))
		mustReconcile(t, r2, "w")
		wantMode(t, lastRoot(t, rec), tracing.Parent, tpA)
		if st, ok := memoStateOf(r2, "w"); !ok || st != memoSeen {
			t.Errorf("the new process's memo = %v (%v), want a seen entry of its own", st, ok)
		}
	})
}

// design-tracing §10, test 6: acmp's Analyzing → Open sequence.
func TestTracingAnalyzingToOpen(t *testing.T) {
	t.Run("AnnotationFirst", func(t *testing.T) {
		w := newWidget("inc", ann(meta.AnnotationKeyTraceparent, tpA))
		w.Status.State = "Analyzing"
		s := newTraceServer(t, w)
		tp, rec := tracer(t)
		r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))

		// 1. Created with A, in Analyzing: the first pass initializes it, so it acts.
		mustReconcile(t, r, "inc")
		wantMode(t, lastRoot(t, rec), tracing.Parent, tpA)
		if got := s.widget("inc").GetAnnotations()[meta.AnnotationKeyTraceparentContinued]; got != tpA {
			t.Fatalf("marker after the first pass = %q, want %s", got, tpA)
		}

		// 2. A fresh B, in a metadata patch: the pass is Parent mode (its root links to B) and doesn't act.
		s.annotate("inc", ann(meta.AnnotationKeyTraceparent, tpB))
		s.ops.reset()
		mustReconcile(t, r, "inc")
		wantMode(t, lastRoot(t, rec), tracing.Parent, tpB)
		if s.ops.count("patch") != 0 || s.ops.count("act-update") != 0 {
			t.Errorf("ops = %v, want no marker and no act while Analyzing", s.ops.list())
		}

		// 3. Open with a script, a status write: the pass is Parent mode, acts (its Update is a child of B), and
		// writes marker B before Update.
		s.setStatus("inc", func(st *WidgetStatus) { st.State, st.Script = "Open", "fix.sh" })
		s.ops.reset()
		mustReconcile(t, r, "inc")
		wantMode(t, lastRoot(t, rec), tracing.Parent, tpB)
		if i, j := s.ops.index("patch"), s.ops.index("act-update"); i < 0 || j < 0 || i > j {
			t.Fatalf("ops = %v, want marker B before the act", s.ops.list())
		}

		// 4. The next poll links to B.
		mustReconcile(t, r, "inc")
		wantMode(t, lastRoot(t, rec), tracing.Link, tpB)
	})

	t.Run("StatusFirstLinks", func(t *testing.T) {
		w := newWidget("inc", ann(meta.AnnotationKeyTraceparent, tpA))
		w.Status.State = "Analyzing"
		s := newTraceServer(t, w)
		tp, rec := tracer(t)
		r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))
		mustReconcile(t, r, "inc") // marker A

		s.setStatus("inc", func(st *WidgetStatus) { st.State, st.Script = "Open", "fix.sh" })
		s.ops.reset()
		mustReconcile(t, r, "inc")
		wantMode(t, lastRoot(t, rec), tracing.Link, tpA)
		if s.ops.count("act-update") != 1 {
			t.Errorf("ops = %v, want the Open pass to act", s.ops.list())
		}
	})
}

// design-tracing §10, test 7: a forbidden marker write. The pass completes and
// records trace.marker_failed; later passes in this process link; a new
// process parents again.
func TestTracingMarkerWriteFails(t *testing.T) {
	s := newTraceServer(t, newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))
	s.faults.patch = func(client.Object) error { return forbiddenErr() }
	tp, rec := tracer(t)
	r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))

	mustReconcile(t, r, "w")
	root := lastRoot(t, rec)
	wantMode(t, root, tracing.Parent, tpA)
	if _, ok := eventAttr(root, tracing.EventMarkerFailed, "exception.message"); !ok {
		t.Errorf("events = %v, want trace.marker_failed", events(root))
	}
	if s.ops.count("act-update") != 1 {
		t.Errorf("ops = %v, want the pass to go on and act", s.ops.list())
	}
	if st, ok := memoStateOf(r, "w"); !ok || st != memoFailed {
		t.Errorf("memo = %v (%v), want failed", st, ok)
	}

	// More work, same process: Link, and no new attempt.
	s.ops.reset()
	w := s.widget("w")
	w.Spec.Wanted = "more"
	if err := s.raw.Update(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, r, "w")
	wantMode(t, lastRoot(t, rec), tracing.Link, tpA)
	if s.ops.count("patch-rejected") != 0 {
		t.Error("a later pass in the same process tried the marker again")
	}

	// A new process parents again until it acts.
	w = s.widget("w")
	w.Spec.Wanted = "again"
	if err := s.raw.Update(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp)), "w")
	wantMode(t, lastRoot(t, rec), tracing.Parent, tpA)
}

// design-tracing §10, test 8.
func TestTracingFailedPasses(t *testing.T) {
	t.Run("AfterActingTheRetryLinks", func(t *testing.T) {
		s := newTraceServer(t, newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))
		tp, rec := tracer(t)
		p := &widgetProvider{ops: s.ops, updateErr: errors.New("boom")}
		r := newTraceReconciler(s, p, WithTracerProvider(tp))
		if _, err := r.Reconcile(context.Background(), reqFor("w")); err == nil {
			t.Fatal("want the Update error")
		}
		_, _ = r.Reconcile(context.Background(), reqFor("w"))
		wantMode(t, lastRoot(t, rec), tracing.Link, tpA)
	})
	t.Run("BeforeActingTheRetryParents", func(t *testing.T) {
		s := newTraceServer(t, newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))
		tp, rec := tracer(t)
		p := &widgetProvider{ops: s.ops, observeErr: errors.New("boom")}
		r := newTraceReconciler(s, p, WithTracerProvider(tp))
		_, _ = r.Reconcile(context.Background(), reqFor("w"))
		_, _ = r.Reconcile(context.Background(), reqFor("w"))
		wantMode(t, lastRoot(t, rec), tracing.Parent, tpA)
		if s.ops.count("patch") != 0 {
			t.Error("a pass that failed before acting wrote the marker")
		}
	})
}

// design-tracing §10, test 9: one test per row of §7.
func TestTracingStatusMapping(t *testing.T) {
	t.Run("ReturnedError", func(t *testing.T) {
		s := newTraceServer(t, newWidget("w", nil))
		tp, rec := tracer(t)
		_, _ = newTraceReconciler(s, &widgetProvider{ops: s.ops, observeErr: errors.New("boom")}, WithTracerProvider(tp)).Reconcile(context.Background(), reqFor("w"))
		root := lastRoot(t, rec)
		if root.Status().Code != codes.Error {
			t.Errorf("status = %v, want Error", root.Status())
		}
		if _, ok := eventAttr(root, "exception", "exception.message"); !ok {
			t.Error("no exception event")
		}
		for _, sp := range rec.Ended() {
			if sp.Name() == tracing.SpanObserve && sp.Status().Code != codes.Error {
				t.Error("the observe child isn't Error")
			}
		}
	})

	t.Run("HandshakeInsideItsWindowIsError", func(t *testing.T) {
		w := newWidget("w", nil)
		meta.SetExternalCreatePending(w, time.Now())
		s := newTraceServer(t, w)
		tp, rec := tracer(t)
		p := &widgetProvider{ops: s.ops, exists: func(*Widget) bool { return false }}
		res, err := newTraceReconciler(s, p, WithTracerProvider(tp)).Reconcile(context.Background(), reqFor("w"))
		if err != nil || res.RequeueAfter == 0 {
			t.Fatalf("Reconcile = %v, %v; want a requeue and no error", res, err)
		}
		root := lastRoot(t, rec)
		if root.Status().Code != codes.Error || root.Status().Description != errCreateIncomplete {
			t.Errorf("status = %v, want Error with the handshake message", root.Status())
		}
	})

	t.Run("DeferralIsUnsetWithAnEvent", func(t *testing.T) {
		s := newTraceServer(t, newWidget("w", nil))
		tp, rec := tracer(t)
		g := newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: 1})
		p := &widgetProvider{ops: s.ops, key: "api.example.com"}
		p.observeHook = func(ctx context.Context, _ *Widget) {
			ratelimit.Report(ctx, ratelimit.Signal{Key: "api.example.com", Throttled: true, RetryAfter: time.Minute})
		}
		mustReconcile(t, newTraceReconciler(s, p, WithTracerProvider(tp), WithRateLimitGovernor(g)), "w")
		root := lastRoot(t, rec)
		if root.Status().Code != codes.Unset {
			t.Errorf("status = %v, want Unset", root.Status())
		}
		for key, want := range map[attribute.Key]string{
			tracing.AttrRateLimitKey:   "api.example.com",
			tracing.AttrRateLimitCause: "throttled",
		} {
			if got, ok := eventAttr(root, tracing.EventRateLimitDeferred, string(key)); !ok || got != want {
				t.Errorf("deferral %s = %q (%v), want %q", key, got, ok, want)
			}
		}
		for _, key := range []attribute.Key{tracing.AttrRateLimitRequeueAfter, tracing.AttrRateLimitUntil} {
			if _, ok := eventAttr(root, tracing.EventRateLimitDeferred, string(key)); !ok {
				t.Errorf("deferral event has no %s", key)
			}
		}
	})

	conflictCases := map[tracing.Step]func(t *testing.T) (*traceServer, *widgetProvider, []ReconcilerOption){
		tracing.StepConnect: func(t *testing.T) (*traceServer, *widgetProvider, []ReconcilerOption) {
			s := newTraceServer(t, newWidget("w", nil))
			return s, &widgetProvider{ops: s.ops, connectErr: conflictErr()}, nil
		},
		tracing.StepObserve: func(t *testing.T) (*traceServer, *widgetProvider, []ReconcilerOption) {
			s := newTraceServer(t, newWidget("w", nil))
			return s, &widgetProvider{ops: s.ops, observeErr: conflictErr()}, nil
		},
		tracing.StepHandshake: func(t *testing.T) (*traceServer, *widgetProvider, []ReconcilerOption) {
			w := newWidget("w", nil)
			meta.SetExternalCreatePending(w, time.Now().Add(-time.Hour))
			s := newTraceServer(t, w)
			s.faults.update = func(client.Object) error { return conflictErr() }
			return s, &widgetProvider{ops: s.ops, exists: func(*Widget) bool { return false }}, nil
		},
		tracing.StepFinalizer: func(t *testing.T) (*traceServer, *widgetProvider, []ReconcilerOption) {
			s := newTraceServer(t, newWidget("w", nil))
			return s, &widgetProvider{ops: s.ops}, []ReconcilerOption{WithFinalizer(resource.FinalizerFns{
				AddFinalizerFn:    func(context.Context, resource.Object) error { return conflictErr() },
				RemoveFinalizerFn: func(context.Context, resource.Object) error { return nil },
			})}
		},
		tracing.StepCreatePending: func(t *testing.T) (*traceServer, *widgetProvider, []ReconcilerOption) {
			s := newTraceServer(t, newWidget("w", nil))
			s.faults.update = func(client.Object) error { return conflictErr() }
			return s, &widgetProvider{ops: s.ops, exists: func(*Widget) bool { return false }}, nil
		},
		tracing.StepCreate: func(t *testing.T) (*traceServer, *widgetProvider, []ReconcilerOption) {
			s := newTraceServer(t, newWidget("w", nil))
			return s, &widgetProvider{ops: s.ops, exists: func(*Widget) bool { return false }, createErr: conflictErr()}, nil
		},
		tracing.StepLateInit: func(t *testing.T) (*traceServer, *widgetProvider, []ReconcilerOption) {
			s := newTraceServer(t, initialized(newWidget("w", nil)))
			s.faults.update = func(client.Object) error { return conflictErr() }
			return s, &widgetProvider{ops: s.ops, lateInit: true}, nil
		},
		tracing.StepMarker: func(t *testing.T) (*traceServer, *widgetProvider, []ReconcilerOption) {
			s := newTraceServer(t, newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))
			s.faults.patch = func(client.Object) error { return conflictErr() }
			return s, &widgetProvider{ops: s.ops}, nil
		},
		// The one step that doesn't requeue: a status conflict is the pass's error.
		tracing.StepStatus: func(t *testing.T) (*traceServer, *widgetProvider, []ReconcilerOption) {
			s := newTraceServer(t, initialized(newWidget("w", nil)))
			s.faults.status = func(client.Object) error { return conflictErr() }
			return s, &widgetProvider{ops: s.ops}, nil
		},
	}
	if len(conflictCases) != len(tracing.Steps()) {
		t.Fatalf("conflict cases cover %d steps, tracing.Steps has %d", len(conflictCases), len(tracing.Steps()))
	}
	for step, build := range conflictCases {
		t.Run("Conflict/"+string(step), func(t *testing.T) {
			s, p, o := build(t)
			tp, rec := tracer(t)
			res, err := newTraceReconciler(s, p, append([]ReconcilerOption{WithTracerProvider(tp)}, o...)...).Reconcile(context.Background(), reqFor("w"))
			root := lastRoot(t, rec)
			if step == tracing.StepStatus {
				if err == nil || !kerrors.IsConflict(err) {
					t.Fatalf("Reconcile = %v, %v; want the status conflict returned as the error", res, err)
				}
				if root.Status().Code != codes.Error {
					t.Errorf("status = %v, want Error", root.Status())
				}
			} else {
				if err != nil || !isRateLimitedRequeue(res) {
					t.Fatalf("Reconcile = %v, %v; want a rate-limited requeue", res, err)
				}
				if root.Status().Code != codes.Unset {
					t.Errorf("status = %v, want Unset", root.Status())
				}
			}
			if got, ok := eventAttr(root, tracing.EventConflict, string(tracing.AttrStep)); !ok || got != string(step) {
				t.Errorf("k8s.conflict step = %q (%v), want %s; events %v", got, ok, step, events(root))
			}
		})
	}

	t.Run("PausedIsUnsetWithAnEvent", func(t *testing.T) {
		s := newTraceServer(t, newWidget("w", ann(meta.AnnotationKeyReconciliationPaused, "true")))
		tp, rec := tracer(t)
		mustReconcile(t, newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp)), "w")
		root := lastRoot(t, rec)
		if root.Status().Code != codes.Unset || len(events(root)) != 1 || events(root)[0] != tracing.EventReconcilePaused {
			t.Errorf("status %v, events %v; want Unset and reconcile.paused", root.Status(), events(root))
		}
	})

	t.Run("SuccessIsUnsetNeverOk", func(t *testing.T) {
		s := newTraceServer(t, initialized(newWidget("w", nil)))
		tp, rec := tracer(t)
		mustReconcile(t, newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp)), "w")
		root := lastRoot(t, rec)
		if root.Status().Code != codes.Unset || len(root.Events()) != 0 {
			t.Errorf("status %v, events %v; want Unset and none", root.Status(), events(root))
		}
	})

	t.Run("PanicIsErrorAndStillPanics", func(t *testing.T) {
		s := newTraceServer(t, newWidget("w", nil))
		tp, rec := tracer(t)
		p := &widgetProvider{ops: s.ops}
		p.observeHook = func(context.Context, *Widget) { panic("provider bug") }
		r := newTraceReconciler(s, p, WithTracerProvider(tp))
		func() {
			defer func() {
				if v := recover(); v != "provider bug" {
					t.Errorf("recovered %v, want the provider's panic", v)
				}
			}()
			_, _ = r.Reconcile(context.Background(), reqFor("w"))
		}()
		root := lastRoot(t, rec)
		if root.Status().Code != codes.Error || root.Status().Description != "panic" {
			t.Errorf("status = %v, want Error panic", root.Status())
		}
	})

	t.Run("NotFoundHasNoSpan", func(t *testing.T) {
		s := newTraceServer(t)
		tp, rec := tracer(t)
		mustReconcile(t, newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp)), "gone")
		if len(rec.Ended()) != 0 {
			t.Errorf("a NotFound pass made %d spans, want none", len(rec.Ended()))
		}
	})

	t.Run("OtherGetErrorIsError", func(t *testing.T) {
		s := newTraceServer(t, newWidget("w", nil))
		s.faults.get = func(client.ObjectKey, client.Object) (bool, error) { return true, errors.New("etcd down") }
		tp, rec := tracer(t)
		if _, err := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp)).Reconcile(context.Background(), reqFor("w")); err == nil {
			t.Fatal("want the Get error")
		}
		root := lastRoot(t, rec)
		if root.Status().Code != codes.Error {
			t.Errorf("status = %v, want Error", root.Status())
		}
		wantMode(t, root, tracing.NoParent, "")
	})
}

// design-tracing §10, test 15: tracing off, past the window. No patch, no memo
// entry, no header.
func TestTracingOffPastTheWindow(t *testing.T) {
	tracing.SetTracerProvider(nil)
	var gotHeader string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotHeader += r.Header.Get("traceparent")
		mu.Unlock()
	}))
	defer srv.Close()
	hc := &http.Client{Transport: httpsignal.NewTransport(nil)}

	s := newTraceServer(t, newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))
	p := &widgetProvider{ops: s.ops}
	p.observeHook = func(ctx context.Context, _ *Widget) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
		if resp, err := hc.Do(req); err == nil {
			_ = resp.Body.Close()
		}
	}
	r := newTraceReconciler(s, p, WithTraceContinueWindow(time.Nanosecond))
	for range 3 {
		mustReconcile(t, r, "w")
		time.Sleep(time.Millisecond)
	}
	if s.ops.count("patch") != 0 {
		t.Error("tracing off wrote a marker")
	}
	if r.traces.len() != 0 {
		t.Errorf("tracing off kept %d memo entries", r.traces.len())
	}
	mu.Lock()
	defer mu.Unlock()
	if gotHeader != "" {
		t.Errorf("tracing off sent traceparent %q", gotHeader)
	}
}

// design-tracing §10, test 16: NotFound, finalizer removal, and delete and
// recreate clear the entry.
func TestTracingMemoLifecycle(t *testing.T) {
	t.Run("NotFoundDropsTheEntry", func(t *testing.T) {
		s := newTraceServer(t, initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA))))
		tp, _ := tracer(t)
		r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))
		mustReconcile(t, r, "w")
		if r.traces.len() != 1 {
			t.Fatal("no entry after a Parent pass")
		}
		if err := s.raw.Delete(context.Background(), s.widget("w")); err != nil {
			t.Fatal(err)
		}
		mustReconcile(t, r, "w")
		if r.traces.len() != 0 {
			t.Error("NotFound kept the entry")
		}
	})
	t.Run("FinalizerRemovalDropsTheEntry", func(t *testing.T) {
		w := initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA, meta.AnnotationKeyDeletionPolicy, meta.DeletionPolicyOrphan)))
		w.Finalizers = []string{FinalizerName}
		s := newTraceServer(t, w)
		tp, _ := tracer(t)
		r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))
		mustReconcile(t, r, "w")
		if r.traces.len() != 1 {
			t.Fatal("no entry after a Parent pass")
		}
		got := s.widget("w")
		now := metav1.Now()
		got.DeletionTimestamp = &now
		// The fake client sets deletionTimestamp on Delete when finalizers remain.
		if err := s.raw.Delete(context.Background(), got); err != nil {
			t.Fatal(err)
		}
		mustReconcile(t, r, "w")
		if r.traces.len() != 0 {
			t.Error("removing the finalizer kept the entry")
		}
	})
	t.Run("NonOrphanFinalizerRemovalDropsTheEntry", func(t *testing.T) {
		w := initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))
		w.Finalizers = []string{FinalizerName}
		s := newTraceServer(t, w)
		tp, _ := tracer(t)
		deleting := false
		p := &widgetProvider{ops: s.ops, exists: func(*Widget) bool { return !deleting }}
		r := newTraceReconciler(s, p, WithTracerProvider(tp))
		mustReconcile(t, r, "w")
		if r.traces.len() != 1 {
			t.Fatal("no entry after a Parent pass")
		}
		// Deleted under the default (Delete) policy, with the external resource
		// already gone: the pass skips Delete and removes the finalizer.
		if err := s.raw.Delete(context.Background(), s.widget("w")); err != nil {
			t.Fatal(err)
		}
		deleting = true
		mustReconcile(t, r, "w")
		if s.ops.count("act-delete") != 0 {
			t.Fatalf("ops = %v, want no Delete for a resource that's gone", s.ops.list())
		}
		if r.traces.len() != 0 {
			t.Error("removing the finalizer on the Delete path kept the entry")
		}
	})

	t.Run("RecreatedObjectIsDecidedAfresh", func(t *testing.T) {
		s := newTraceServer(t, initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA))))
		tp, rec := tracer(t)
		r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))
		mustReconcile(t, r, "w")
		backdate(r, "w", defaultTraceContinueWindow) // the old object's window has closed
		if err := s.raw.Delete(context.Background(), s.widget("w")); err != nil {
			t.Fatal(err)
		}
		fresh := initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))
		fresh.UID = "uid-recreated"
		if err := s.raw.Create(context.Background(), fresh); err != nil {
			t.Fatal(err)
		}
		s.ops.reset()
		mustReconcile(t, r, "w")
		wantMode(t, lastRoot(t, rec), tracing.Parent, tpA)
		if s.ops.count("patch") != 0 {
			t.Error("the recreated object inherited the old object's closed window")
		}
	})
}

// design-tracing §10, test 17: a deferral storm. With the key at capacity, the
// passes inside the window are Parent-mode roots (linked, never children) with ratelimit.deferred and no
// marker; the first pass after the window links and writes the marker.
func TestTracingDeferralStorm(t *testing.T) {
	s := newTraceServer(t, initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA))))
	tp, rec := tracer(t)
	g := newGovernor(t, ratelimit.Options{MaxConcurrentReconciles: 1})
	r := newTraceReconciler(s, &widgetProvider{ops: s.ops, key: "api.example.com"}, WithTracerProvider(tp), WithRateLimitGovernor(g))
	// Another worker holds the key's only slot.
	release, d := g.AcquireAll(context.Background(), []string{"api.example.com"})
	if d != nil {
		t.Fatalf("could not take the slot: %v", d)
	}
	defer release()

	const storm = 5
	for range storm {
		mustReconcile(t, r, "w")
	}
	rs := roots(rec)
	if len(rs) != storm {
		t.Fatalf("%d roots, want %d", len(rs), storm)
	}
	for _, root := range rs {
		wantMode(t, root, tracing.Parent, tpA)
		if cause, ok := eventAttr(root, tracing.EventRateLimitDeferred, string(tracing.AttrRateLimitCause)); !ok || cause != "capacity" {
			t.Errorf("deferral cause = %q (%v), want capacity", cause, ok)
		}
	}
	if s.ops.count("patch") != 0 {
		t.Error("a deferred pass wrote the marker")
	}

	backdate(r, "w", defaultTraceContinueWindow)
	mustReconcile(t, r, "w")
	wantMode(t, lastRoot(t, rec), tracing.Link, tpA)
	if s.ops.count("patch") != 1 {
		t.Errorf("ops = %v, want the marker written once the window closed", s.ops.list())
	}
}

// design-tracing §10, test 18: a paused object past the window gets no marker;
// the pass after it's unpaused writes it.
func TestTracingPausedPastTheWindow(t *testing.T) {
	s := newTraceServer(t, initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA, meta.AnnotationKeyReconciliationPaused, "true"))))
	tp, _ := tracer(t)
	r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))
	mustReconcile(t, r, "w")
	backdate(r, "w", defaultTraceContinueWindow)
	mustReconcile(t, r, "w")
	if s.ops.count("patch") != 0 {
		t.Fatal("a paused object got a marker")
	}
	w := s.widget("w")
	meta.RemoveAnnotations(w, meta.AnnotationKeyReconciliationPaused)
	if err := s.raw.Update(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, r, "w")
	if s.ops.count("patch") != 1 {
		t.Errorf("ops = %v, want the marker written after unpausing", s.ops.list())
	}
}

// design-tracing §10, test 19: a marker conflict, on each kind of write,
// requeues before acting with step marker and leaves the entry seen.
func TestTracingMarkerConflict(t *testing.T) {
	cases := map[string]struct {
		obj    *Widget
		window time.Duration
	}{
		"Now":               {obj: initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA))), window: 0},
		"BeforeFirstAction": {obj: newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)), window: defaultTraceContinueWindow},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := newTraceServer(t, tc.obj)
			s.faults.patch = func(client.Object) error { return conflictErr() }
			tp, rec := tracer(t)
			r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp), WithTraceContinueWindow(tc.window))
			res, err := r.Reconcile(context.Background(), reqFor("w"))
			if err != nil || !isRateLimitedRequeue(res) {
				t.Fatalf("Reconcile = %v, %v; want a rate-limited requeue", res, err)
			}
			if s.ops.count("act-update") != 0 {
				t.Errorf("ops = %v, want no act after a marker conflict", s.ops.list())
			}
			if got, _ := eventAttr(lastRoot(t, rec), tracing.EventConflict, string(tracing.AttrStep)); got != string(tracing.StepMarker) {
				t.Errorf("conflict step = %q, want marker", got)
			}
			if st, ok := memoStateOf(r, "w"); !ok || st != memoSeen {
				t.Errorf("memo = %v (%v), want seen", st, ok)
			}
		})
	}
}

// design-tracing §10, test 20: admission rejects the marker patch. The pass
// records trace.marker_failed, the entry becomes failed, create-pending is
// written without the marker, and the create proceeds.
func TestTracingAdmissionRejectsTheMarker(t *testing.T) {
	for name, reject := range map[string]func() error{
		"Forbidden": forbiddenErr,
		"Invalid": func() error {
			return kerrors.NewInvalid(widgetGVK.GroupKind(), "w", nil)
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := newTraceServer(t, newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))
			s.faults.patch = func(client.Object) error { return reject() }
			tp, rec := tracer(t)
			created := false
			p := &widgetProvider{ops: s.ops, exists: func(*Widget) bool { return created }}
			r := newTraceReconciler(s, p, WithTracerProvider(tp))
			mustReconcile(t, r, "w")
			created = true

			if _, ok := eventAttr(lastRoot(t, rec), tracing.EventMarkerFailed, "exception.message"); !ok {
				t.Error("no trace.marker_failed")
			}
			if st, ok := memoStateOf(r, "w"); !ok || st != memoFailed {
				t.Errorf("memo = %v (%v), want failed", st, ok)
			}
			if s.ops.count("create") != 1 {
				t.Errorf("ops = %v, want the create to proceed", s.ops.list())
			}
			got := s.widget("w")
			if got.GetAnnotations()[meta.AnnotationKeyTraceparentContinued] != "" {
				t.Error("the marker was stored despite the rejection")
			}
			if meta.ExternalCreateIncomplete(got) {
				t.Error("an unresolved create handshake was left behind")
			}

			s.ops.reset()
			mustReconcile(t, r, "w")
			wantMode(t, lastRoot(t, rec), tracing.Link, tpA)
			if s.ops.count("patch-rejected") != 0 {
				t.Error("the next pass tried the marker again")
			}
		})
	}
}

// design-tracing §10, test 25: N workers reconcile distinct objects at once,
// under -race. Every root's parent belongs to its own object, and the memo stays
// consistent while objects are deleted, recreated and restamped.
func TestTracingConcurrency(t *testing.T) {
	const n = 16
	tps := []string{tpA, tpB, tpC}
	var objs []client.Object
	for i := range n {
		objs = append(objs, newWidget(name(i), ann(meta.AnnotationKeyTraceparent, tps[i%3])))
	}
	s := newTraceServer(t, objs...)
	tp, rec := tracer(t)
	r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))

	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			for round := range 4 {
				switch round {
				case 1:
					s.annotate(name(i), ann(meta.AnnotationKeyTraceparent, tps[(i+1)%3]))
				case 2:
					if i%4 == 0 {
						_ = s.raw.Delete(context.Background(), s.widget(name(i)))
					}
				case 3:
					if i%4 == 0 {
						fresh := newWidget(name(i), ann(meta.AnnotationKeyTraceparent, tps[i%3]))
						fresh.UID = "recreated"
						_ = s.raw.Create(context.Background(), fresh)
					}
				}
				_, _ = r.Reconcile(context.Background(), reqFor(name(i)))
			}
		})
	}
	wg.Wait()

	valid := map[string]bool{}
	for _, v := range tps {
		sc := remote(t, v)
		valid[sc.TraceID().String()+"/"+sc.SpanID().String()] = true
	}
	for _, root := range roots(rec) {
		m, sc := mode(root)
		if m == tracing.NoParent {
			continue
		}
		if !valid[sc.TraceID().String()+"/"+sc.SpanID().String()] {
			t.Errorf("a root's %s points at %s/%s, which no object carries", m, sc.TraceID(), sc.SpanID())
		}
	}
	if r.traces.len() > n {
		t.Errorf("memo has %d entries for %d objects", r.traces.len(), n)
	}
}

func name(i int) string {
	return "w" + string(rune('a'+i))
}

// A marker patch that returns NotFound counts as success: the object is gone,
// so there is nothing to mark and nothing failed. The pass goes on, records no
// trace.marker_failed, and the memo says marked.
func TestTracingMarkerPatchNotFoundCountsAsSuccess(t *testing.T) {
	s := newTraceServer(t, newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))
	s.faults.patch = func(client.Object) error { return kerrors.NewNotFound(widgetGR, "w") }
	tp, rec := tracer(t)
	r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))
	mustReconcile(t, r, "w")
	root := lastRoot(t, rec)
	if _, failed := eventAttr(root, tracing.EventMarkerFailed, "exception.message"); failed {
		t.Error("a NotFound marker patch was recorded as trace.marker_failed")
	}
	if st, ok := memoStateOf(r, "w"); !ok || st != memoMarked {
		t.Errorf("memo = %v (%v), want marked", st, ok)
	}
	if s.ops.count("act-update") != 1 {
		t.Errorf("ops = %v, want the pass to go on and act", s.ops.list())
	}
}

// With tracing off, a pass's tracing work allocates nothing when the object has
// no krateo.io/traceparent: beginTrace returns the shared offPass
// (design-tracing §3.4).
func TestTracingOffBeginTraceAllocatesNothing(t *testing.T) {
	s := newTraceServer(t, newWidget("w", nil))
	r := newTraceReconciler(s, &widgetProvider{ops: s.ops})
	w := s.widget("w")
	ctx := context.Background()
	req := reqFor("w")
	start := time.Now()
	n := testing.AllocsPerRun(100, func() { _, _ = r.beginTrace(ctx, nil, req, w, true, start) })
	if n != 0 {
		t.Errorf("beginTrace with tracing off allocates %v times per pass, want 0", n)
	}
}

// With tracing off, the shared offPass serves every pass at once; N workers on
// distinct objects under -race catch a future write to it before a tp check
// (design-tracing §10, test 25, off mode).
func TestTracingConcurrencyOff(t *testing.T) {
	tracing.SetTracerProvider(nil)
	const n = 16
	tps := []string{tpA, tpB, tpC}
	var objs []client.Object
	for i := range n {
		objs = append(objs, newWidget(name(i), ann(meta.AnnotationKeyTraceparent, tps[i%3])))
	}
	s := newTraceServer(t, objs...)
	p := &widgetProvider{ops: s.ops, updateErr: nil}
	p.observeHook = func(context.Context, *Widget) {}
	r := newTraceReconciler(s, p)

	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			for round := range 4 {
				if round == 2 {
					s.annotate(name(i), ann(meta.AnnotationKeyTraceparent, tps[(i+1)%3]))
				}
				_, _ = r.Reconcile(context.Background(), reqFor(name(i)))
			}
		})
	}
	wg.Wait()
	if s.ops.count("patch") != 0 {
		t.Error("tracing off wrote a marker")
	}
	if r.traces.len() != 0 {
		t.Errorf("tracing off kept %d memo entries", r.traces.len())
	}
	if o := offPass; o.tp != nil || o.span != nil || o.memo != nil || o.marked || o.key.Name != "" || o.uid != "" || o.d.Mode != tracing.NoParent || o.d.Marker != "" {
		t.Errorf("offPass was written: %+v", *o)
	}
}

// An entry kept from when tracing was on is dropped when its object is
// deleted, even with tracing off by then: the NotFound pass forgets it either
// way.
func TestTracingEntryDroppedOnDeleteAfterTracingTurnsOff(t *testing.T) {
	s := newTraceServer(t, initialized(newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA))))
	tp, _ := tracer(t)
	p := &widgetProvider{ops: s.ops}
	on := newTraceReconciler(s, p, WithTracerProvider(tp))
	mustReconcile(t, on, "w")
	if on.traces.len() != 1 {
		t.Fatal("no entry after a Parent pass with tracing on")
	}
	// Tracing turns off: the same reconciler now resolves no provider.
	tracing.SetTracerProvider(nil)
	on.tracerProv = nil
	if err := s.raw.Delete(context.Background(), s.widget("w")); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, on, "w")
	if on.traces.len() != 0 {
		t.Error("the entry outlived its object with tracing off")
	}
}
