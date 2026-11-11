package tracing

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/provider-runtime/pkg/meta"
)

// The rule lives in plumbing's tracecontext, which takes a Tracer. A nil tp
// must reach it as a nil Tracer, never as a non-nil Tracer that would turn
// tracing on, and must still allocate nothing (design §10, test 11): with no
// annotation for StartReconcile, and always for StartStep and StartActStep,
// whatever the upstream.
func TestNilProviderAllocatesNothing(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	upstream := Decide(ann(meta.AnnotationKeyTraceparent, tpA), "", time.Time{}, start, time.Minute).Remote
	if !upstream.IsValid() {
		t.Fatal("tpA must give a valid upstream")
	}
	for name, f := range map[string]func(){
		"StartReconcile":             func() { _, _ = StartReconcile(ctx, nil, obj, Decision{}, start) },
		"StartStep":                  func() { _, _ = StartStep(ctx, nil, SpanObserve) },
		"StartActStepNoUpstream":     func() { _, _ = StartActStep(ctx, nil, obj, SpanCreate, Decision{}.Remote) },
		"StartActStepValidUpstream":  func() { _, _ = StartActStep(ctx, nil, obj, SpanCreate, upstream) },
		"DecideWithoutTheAnnotation": func() { _ = Decide(nil, "", time.Time{}, start, time.Minute) },
	} {
		if n := testing.AllocsPerRun(100, f); n != 0 {
			t.Errorf("%s with tp nil allocates %v times per call, want 0", name, n)
		}
	}
}

// With tp nil the spans are no-ops, and StartActStep never joins the upstream:
// the context it returns has no active span.
func TestNilProviderStartsNothing(t *testing.T) {
	d := Decide(ann(meta.AnnotationKeyTraceparent, tpA), "", time.Time{}, time.Now(), time.Minute)
	ctx, root := StartReconcile(context.Background(), nil, obj, d, time.Now())
	actCtx, act := StartActStep(ctx, nil, obj, SpanCreate, d.Remote)
	if root.IsRecording() || act.IsRecording() || act.SpanContext().IsValid() {
		t.Error("with tp nil every span must be the no-op span")
	}
	if HasActiveSpan(actCtx) {
		t.Error("with tp nil StartActStep must not make the upstream the active span")
	}
}

// Every span carries provider-runtime's instrumentation scope, not
// tracecontext's: the Start functions pass tp.Tracer(ScopeName).
func TestSpansCarryProviderRuntimeScope(t *testing.T) {
	tp, rec := recorder(t)
	d := Decide(ann(meta.AnnotationKeyTraceparent, tpA), "", time.Time{}, time.Now(), time.Minute)
	ctx, root := StartReconcile(context.Background(), tp, obj, d, time.Now())
	_, step := StartStep(ctx, tp, SpanObserve)
	step.End()
	_, act := StartActStep(ctx, tp, obj, SpanCreate, d.Remote)
	act.End()
	root.End()

	ended := rec.Ended()
	if len(ended) != 3 {
		t.Fatalf("want 3 ended spans, got %d", len(ended))
	}
	for _, s := range ended {
		if got := s.InstrumentationScope().Name; got != ScopeName {
			t.Errorf("span %q scope = %q, want %q", s.Name(), got, ScopeName)
		}
	}
}
