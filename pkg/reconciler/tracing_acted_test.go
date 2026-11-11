package reconciler

import (
	"context"
	"errors"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/tracing"
)

// acted reports whether root carries provider_runtime.acted, and its value.
func acted(root sdktrace.ReadOnlySpan) (present, value bool) {
	for _, kv := range root.Attributes() {
		if kv.Key == tracing.AttrActed {
			return true, kv.Value.AsBool()
		}
	}
	return false, false
}

// #74 and design-tracing §6.7: every pass that starts an act step marks its
// root with provider_runtime.acted=true, whether the act joined the upstream
// trace or not. Only the joined act goes through StartActStep, which marks the
// root itself; the others are StartStep children, so the reconciler must mark
// those. The collector keeps acting roots, so a missed mark loses the record
// of a change.
func TestTracingEveryActMarksTheRoot(t *testing.T) {
	deleting := func(kv map[string]string) *Widget {
		w := initialized(newWidget("w", kv))
		w.Finalizers = []string{FinalizerName}
		now := metav1.Now()
		w.DeletionTimestamp = &now
		return w
	}
	gone := func(*Widget) bool { return false }
	upstream := ann(meta.AnnotationKeyTraceparent, tpA)

	for name, tc := range map[string]struct {
		obj    *Widget
		p      widgetProvider
		mode   tracing.Mode
		joined bool
	}{
		"joined update": {obj: newWidget("w", upstream), mode: tracing.Parent, joined: true},
		"joined create": {obj: newWidget("w", upstream), p: widgetProvider{exists: gone}, mode: tracing.Parent, joined: true},
		"joined delete": {obj: deleting(upstream), mode: tracing.Parent, joined: true},
		"plain update":  {obj: newWidget("w", nil), mode: tracing.NoParent},
		"plain create":  {obj: newWidget("w", nil), p: widgetProvider{exists: gone}, mode: tracing.NoParent},
		"plain delete":  {obj: deleting(nil), mode: tracing.NoParent},
		// An act that fails still started: the pass tried to change something.
		"failed plain update": {obj: newWidget("w", nil), p: widgetProvider{updateErr: errors.New("boom")}, mode: tracing.NoParent},
	} {
		t.Run(name, func(t *testing.T) {
			s := newTraceServer(t, tc.obj)
			tp, rec := tracer(t)
			p := tc.p
			p.ops = s.ops
			_, _ = newTraceReconciler(s, &p, WithTracerProvider(tp)).Reconcile(context.Background(), reqFor("w"))
			root := lastRoot(t, rec)
			if tc.mode == tracing.Parent {
				wantMode(t, root, tracing.Parent, tpA)
			} else {
				wantMode(t, root, tracing.NoParent, "")
			}
			if joined := spansIn(t, rec, tpA) == 1; joined != tc.joined {
				t.Fatalf("premise: act joined the upstream = %v, want %v", joined, tc.joined)
			}
			if present, v := acted(root); !present || !v {
				t.Errorf("root acted = (present %v, %v), want (true, true)", present, v)
			}
		})
	}
}

// The second act for a value is an ordinary child in a Link pass. It marks
// its root too.
func TestTracingSecondActMarksItsRoot(t *testing.T) {
	s := newTraceServer(t, newWidget("w", ann(meta.AnnotationKeyTraceparent, tpA)))
	tp, rec := tracer(t)
	r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))
	mustReconcile(t, r, "w") // the joined act, which writes the marker

	w := s.widget("w")
	w.Spec.Wanted = "more"
	if err := s.raw.Update(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, r, "w")
	rs := roots(rec)
	if len(rs) != 2 {
		t.Fatalf("%d roots, want 2", len(rs))
	}
	wantMode(t, rs[1], tracing.Link, tpA)
	for i, root := range rs {
		if present, v := acted(root); !present || !v {
			t.Errorf("root %d acted = (present %v, %v), want (true, true)", i, present, v)
		}
	}
}

// A pass that only observes carries no provider_runtime.acted at all: absent,
// never false, so the tail sampler's rule can't match it.
func TestTracingObserveOnlyPassIsNotMarked(t *testing.T) {
	for name, kv := range map[string]map[string]string{
		"with an upstream": ann(meta.AnnotationKeyTraceparent, tpA),
		"without":          nil,
	} {
		t.Run(name, func(t *testing.T) {
			s := newTraceServer(t, initialized(newWidget("w", kv)))
			tp, rec := tracer(t)
			r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tp))
			for range 3 {
				mustReconcile(t, r, "w")
			}
			for i, root := range roots(rec) {
				if present, _ := acted(root); present {
					t.Errorf("observe-only root %d carries provider_runtime.acted", i)
				}
			}
			if n := len(actSpans(rec, tracing.SpanUpdate)); n != 0 {
				t.Fatalf("premise: %d update spans, want 0", n)
			}
		})
	}
}
