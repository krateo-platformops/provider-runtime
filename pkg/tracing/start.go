package tracing

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// The root span's attribute keys: exactly these three, as unstructured-runtime
// sets them.
const (
	AttrObjectKind      = attribute.Key("k8s.object.kind")
	AttrObjectName      = attribute.Key("k8s.object.name")
	AttrObjectNamespace = attribute.Key("k8s.object.namespace")
)

// Object names the object a reconcile is about.
type Object struct {
	// Kind is the Kind alone, such as Repo, without its group.
	Kind string
	// Name is the object's name.
	Name string
	// Namespace is the object's namespace, "" for a cluster-scoped object.
	Namespace string
}

// inboundKey holds, with tracing off, the annotation's span context for Inject
// and InjectAnnotations. It is never the active span.
type inboundKey struct{}

// StartReconcile opens the reconcile root span for obj, starting at start.
// Every reconcile is its own trace (docs/design-tracing.md, D8): the root is
// always a new root, never a child of d.Remote and never of a span already in
// ctx. When d.Remote is valid (modes Parent and Link) it carries one link to
// it; with NoParent it has none. Only the pass's first act step for a new value
// joins the upstream trace (StartActStep). The span is SpanKindServer, named
// reconcile, with exactly the attributes k8s.object.kind, k8s.object.name and
// k8s.object.namespace.
//
// With tp nil it starts nothing. It returns ctx carrying d.Remote, when that's
// valid, for Inject and InjectAnnotations only, and the no-op span.
func StartReconcile(ctx context.Context, tp trace.TracerProvider, obj Object, d Decision, start time.Time) (context.Context, trace.Span) {
	if tp == nil {
		if d.Remote.IsValid() {
			ctx = context.WithValue(ctx, inboundKey{}, d.Remote)
		}
		return ctx, trace.SpanFromContext(context.Background())
	}
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithTimestamp(start),
		trace.WithAttributes(
			AttrObjectKind.String(obj.Kind),
			AttrObjectName.String(obj.Name),
			AttrObjectNamespace.String(obj.Namespace),
		),
		trace.WithNewRoot(),
	}
	if d.Mode != NoParent && d.Remote.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: d.Remote}))
	}
	return tp.Tracer(ScopeName).Start(ctx, SpanReconcile, opts...)
}

// StartActStep opens an act step, the create, update or delete span around
// the provider's call. With upstream valid, it is the first act for a new
// krateo.io/traceparent value: the span's parent is upstream, so the act and
// the calls it makes join the upstream trace, and it carries one link back to
// the reconcile root in ctx (D8). With upstream invalid it is an ordinary
// StartStep child of the root. With tp nil it starts nothing.
func StartActStep(ctx context.Context, tp trace.TracerProvider, step string, upstream trace.SpanContext) (context.Context, trace.Span) {
	if tp == nil || !upstream.IsValid() {
		return StartStep(ctx, tp, step)
	}
	root := trace.SpanContextFromContext(ctx)
	opts := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindInternal)}
	if root.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: root}))
	}
	return tp.Tracer(ScopeName).Start(trace.ContextWithRemoteSpanContext(ctx, upstream), step, opts...)
}

// StartStep opens the child span named step under the reconcile root in ctx,
// with tp's tracer. It's SpanKindInternal and has no attributes of its own. With
// tp nil it starts nothing and returns ctx and the no-op span.
func StartStep(ctx context.Context, tp trace.TracerProvider, step string) (context.Context, trace.Span) {
	if tp == nil {
		return ctx, trace.SpanFromContext(context.Background())
	}
	return tp.Tracer(ScopeName).Start(ctx, step, trace.WithSpanKind(trace.SpanKindInternal))
}
