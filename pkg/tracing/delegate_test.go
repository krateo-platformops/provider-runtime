package tracing

import (
	"context"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/krateo-platformops/provider-runtime/pkg/meta"
)

// The aliased names keep their bytes. Every other test reads them through the
// same alias, so only literals catch a swap.
func TestAliasedNamesAreTheirStrings(t *testing.T) {
	for _, tc := range []struct{ name, got, want string }{
		{"AttrObjectKind", string(AttrObjectKind), "k8s.object.kind"},
		{"AttrObjectName", string(AttrObjectName), "k8s.object.name"},
		{"AttrObjectNamespace", string(AttrObjectNamespace), "k8s.object.namespace"},
		{"SpanReconcile", SpanReconcile, "reconcile"},
		{"EventReconcilePaused", EventReconcilePaused, "reconcile.paused"},
		{"EventConflict", EventConflict, "k8s.conflict"},
		{"EventMarkerFailed", EventMarkerFailed, "trace.marker_failed"},
		{"AttrStep", string(AttrStep), "provider_runtime.step"},
		{"AttrActed", string(AttrActed), "provider_runtime.acted"},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %q, want %q", tc.name, tc.got, tc.want)
		}
	}
}

// recordOnlyActs samples every span except the update act, which it records
// without sampling: IsRecording is true and IsSampled is false.
type recordOnlyActs struct{}

func (recordOnlyActs) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	if p.Name == SpanUpdate {
		return sdktrace.SamplingResult{Decision: sdktrace.RecordOnly}
	}
	return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample}
}

func (recordOnlyActs) Description() string { return "recordOnlyActs" }

// provider-runtime#72, kept here as well as in plumbing's tracecontext, so a
// plumbing bump that broke it fails this repo's CI too. The root links to the
// act only when the act is sampled: a recorded but unsampled act is never
// exported. The guard is IsSampled, not IsRecording, and this is the case that
// tells them apart.
func TestStartActStepRecordOnlyActGetsNoRootLink(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec), sdktrace.WithSampler(recordOnlyActs{}))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	d := Decide(ann(meta.AnnotationKeyTraceparent, tpA), "", time.Time{}, time.Now(), time.Minute)
	ctx, root := StartReconcile(context.Background(), tp, obj, d, time.Now())
	_, act := StartActStep(ctx, tp, obj, SpanUpdate, d.Remote)
	if !act.IsRecording() || act.SpanContext().IsSampled() {
		t.Fatalf("premise: act recording=%v sampled=%v, want true/false", act.IsRecording(), act.SpanContext().IsSampled())
	}
	act.End()
	root.End()

	var rootSpan sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() == SpanReconcile {
			rootSpan = s
		}
	}
	if rootSpan == nil {
		t.Fatal("the root was not recorded")
	}
	links := rootSpan.Links()
	if len(links) != 1 || links[0].SpanContext.SpanID() != d.Remote.SpanID() {
		t.Errorf("root links = %v, want exactly the upstream link, none to the unsampled act", links)
	}
}

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
