package tracing

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/krateo-platformops/provider-runtime/pkg/meta"
)

func recorder(t *testing.T) (*sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return tp, rec
}

func onlySpan(t *testing.T, rec *tracetest.SpanRecorder) sdktrace.ReadOnlySpan {
	t.Helper()
	ended := rec.Ended()
	if len(ended) != 1 {
		t.Fatalf("want 1 ended span, got %d", len(ended))
	}
	return ended[0]
}

var obj = Object{Kind: "Repo", Name: "cool", Namespace: "demo"}

// The root span's shape (design §10, test 1): reconcile, Server, exactly the
// three attributes, and the start time it's given.
func TestStartReconcileShape(t *testing.T) {
	tp, rec := recorder(t)
	start := time.Now().Add(-time.Second)
	_, span := StartReconcile(context.Background(), tp, obj, Decision{}, start)
	span.End()

	s := onlySpan(t, rec)
	if s.Name() != "reconcile" || s.SpanKind() != trace.SpanKindServer {
		t.Errorf("span = %q %s, want reconcile Server", s.Name(), s.SpanKind())
	}
	if !s.StartTime().Equal(start) {
		t.Errorf("start = %v, want %v", s.StartTime(), start)
	}
	want := map[attribute.Key]string{
		"k8s.object.kind":      "Repo",
		"k8s.object.name":      "cool",
		"k8s.object.namespace": "demo",
	}
	if len(s.Attributes()) != len(want) {
		t.Errorf("attributes = %v, want exactly %v", s.Attributes(), want)
	}
	for _, kv := range s.Attributes() {
		if want[kv.Key] != kv.Value.AsString() {
			t.Errorf("attribute %s = %q, want %q", kv.Key, kv.Value.AsString(), want[kv.Key])
		}
	}
}

// A cluster-scoped object has an empty namespace attribute, not a missing one.
func TestStartReconcileClusterScoped(t *testing.T) {
	tp, rec := recorder(t)
	_, span := StartReconcile(context.Background(), tp, Object{Kind: "Repo", Name: "cool"}, Decision{}, time.Now())
	span.End()
	found := false
	for _, kv := range onlySpan(t, rec).Attributes() {
		if kv.Key == AttrObjectNamespace {
			found = true
			if kv.Value.AsString() != "" {
				t.Errorf("namespace = %q, want empty", kv.Value.AsString())
			}
		}
	}
	if !found {
		t.Error("k8s.object.namespace is missing on a cluster-scoped object")
	}
}

// D8: every mode gives a new root. Parent and Link carry one link to Remote,
// NoParent none, and an in-process span in ctx is never the root's parent.
func TestStartReconcileModes(t *testing.T) {
	now := time.Now()
	parent := Decide(ann(meta.AnnotationKeyTraceparent, tpA), "", time.Time{}, now, time.Minute)
	link := Decide(ann(meta.AnnotationKeyTraceparent, tpA, meta.AnnotationKeyTraceparentContinued, tpA), "", time.Time{}, now, time.Minute)

	for name, d := range map[string]Decision{"Parent": parent, "Link": link, "NoParent": {}} {
		t.Run(name, func(t *testing.T) {
			tp, rec := recorder(t)
			ctx, wrapper := tp.Tracer("wrapper").Start(context.Background(), "wrapper")
			_, span := StartReconcile(ctx, tp, obj, d, now)
			span.End()
			wrapper.End()

			var s sdktrace.ReadOnlySpan
			for _, e := range rec.Ended() {
				if e.Name() == "reconcile" {
					s = e
				}
			}
			if s == nil {
				t.Fatal("no reconcile span")
			}
			if s.Parent().IsValid() {
				t.Errorf("root has a parent %s: every reconcile is its own trace", s.Parent().SpanID())
			}
			links := s.Links()
			if d.Mode == NoParent {
				if len(links) != 0 {
					t.Errorf("links = %v, want none", links)
				}
			} else {
				if len(links) != 1 || links[0].SpanContext.SpanID() != d.Remote.SpanID() || links[0].SpanContext.TraceID() != d.Remote.TraceID() {
					t.Errorf("links = %v, want one to the remote", links)
				}
				if s.SpanContext().TraceID() == d.Remote.TraceID() {
					t.Error("the root joined the upstream trace")
				}
			}
			if s.SpanContext().TraceID() == wrapper.SpanContext().TraceID() {
				t.Error("the root took the in-process wrapper span as its parent")
			}
		})
	}
}

// StartActStep with a valid upstream is parented on it, in its trace, with one
// link back to the root in ctx. With an invalid upstream it is an ordinary
// child of the root; with tp nil it starts nothing.
func TestStartActStep(t *testing.T) {
	tp, rec := recorder(t)
	d := Decide(ann(meta.AnnotationKeyTraceparent, tpA), "", time.Time{}, time.Now(), time.Minute)
	ctx, root := StartReconcile(context.Background(), tp, obj, d, time.Now())

	_, joined := StartActStep(ctx, tp, SpanUpdate, d.Remote)
	joined.End()
	_, plain := StartActStep(ctx, tp, SpanUpdate, trace.SpanContext{})
	plain.End()
	root.End()

	var acts []sdktrace.ReadOnlySpan
	for _, e := range rec.Ended() {
		if e.Name() == SpanUpdate {
			acts = append(acts, e)
		}
	}
	if len(acts) != 2 {
		t.Fatalf("want 2 act spans, got %d", len(acts))
	}
	j, p := acts[0], acts[1]
	if j.Parent().SpanID() != d.Remote.SpanID() || j.SpanContext().TraceID() != d.Remote.TraceID() {
		t.Errorf("joined act: parent %s in trace %s, want the upstream", j.Parent().SpanID(), j.SpanContext().TraceID())
	}
	if l := j.Links(); len(l) != 1 || l[0].SpanContext.SpanID() != root.SpanContext().SpanID() {
		t.Errorf("joined act links = %v, want one to the root", l)
	}
	if j.SpanKind() != trace.SpanKindInternal {
		t.Errorf("joined act kind = %s, want Internal", j.SpanKind())
	}
	if p.Parent().SpanID() != root.SpanContext().SpanID() || len(p.Links()) != 0 {
		t.Errorf("plain act: parent %s, links %v; want a plain child of the root", p.Parent().SpanID(), p.Links())
	}

	offCtx, off := StartActStep(context.Background(), nil, SpanUpdate, d.Remote)
	if off.SpanContext().IsValid() || HasActiveSpan(offCtx) {
		t.Error("StartActStep with tp nil must start nothing")
	}
}

// With tracing off, StartReconcile starts nothing, and carries Remote for Inject
// only, never as the active span (design §6.4).
func TestStartReconcileOff(t *testing.T) {
	d := Decide(ann(meta.AnnotationKeyTraceparent, tpA), "", time.Time{}, time.Now(), time.Minute)
	ctx, span := StartReconcile(context.Background(), nil, obj, d, time.Now())
	if span.IsRecording() || span.SpanContext().IsValid() {
		t.Error("with tracing off the span must be the no-op span")
	}
	if HasActiveSpan(ctx) {
		t.Error("with tracing off the inbound context must not be the active span")
	}
	a := map[string]string{}
	if !InjectAnnotations(ctx, a) || a[meta.AnnotationKeyTraceparent] != tpA {
		t.Errorf("InjectAnnotations with tracing off = %v, want the inbound %s", a, tpA)
	}
}

// With tracing off and no annotation, StartReconcile allocates nothing
// (design §10, test 11).
func TestStartReconcileOffAllocatesNothing(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	n := testing.AllocsPerRun(100, func() { _, _ = StartReconcile(ctx, nil, obj, Decision{}, start) })
	if n != 0 {
		t.Errorf("StartReconcile with tracing off allocates %v times per call, want 0", n)
	}
}

// StartStep's span is an Internal child of the root; with tp nil it starts
// nothing.
func TestStartStep(t *testing.T) {
	tp, rec := recorder(t)
	ctx, root := StartReconcile(context.Background(), tp, obj, Decision{}, time.Now())
	_, child := StartStep(ctx, tp, SpanObserve)
	child.End()
	root.End()

	ended := rec.Ended()
	if len(ended) != 2 {
		t.Fatalf("want 2 spans, got %d", len(ended))
	}
	c := ended[0]
	if c.Name() != "observe" || c.SpanKind() != trace.SpanKindInternal {
		t.Errorf("child = %q %s, want observe Internal", c.Name(), c.SpanKind())
	}
	if c.Parent().SpanID() != root.SpanContext().SpanID() {
		t.Error("the child's parent must be the root")
	}
	if len(c.Attributes()) != 0 {
		t.Errorf("child attributes = %v, want none", c.Attributes())
	}

	offCtx, off := StartStep(context.Background(), nil, SpanObserve)
	if off.SpanContext().IsValid() || HasActiveSpan(offCtx) {
		t.Error("StartStep with tp nil must start nothing")
	}
}
