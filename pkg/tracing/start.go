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

// StartReconcile opens the reconcile root span for obj as d says, starting at
// start. The reconciler computes d, from its memo and Decide, before it calls
// this, so the span's mode is the decision's.
//
//   - Parent: the span is a child of d.Remote.
//   - Link: the span is a new root with one link to d.Remote.
//   - NoParent: the span is a new root.
//
// In every mode the root ignores any span already in ctx: it never takes an
// in-process parent. The span is SpanKindServer, named reconcile, with exactly
// the attributes k8s.object.kind, k8s.object.name and k8s.object.namespace.
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
	}
	switch {
	case d.Mode == Parent && d.Remote.IsValid():
		ctx = trace.ContextWithRemoteSpanContext(ctx, d.Remote)
	case d.Mode == Link && d.Remote.IsValid():
		opts = append(opts, trace.WithNewRoot(), trace.WithLinks(trace.Link{SpanContext: d.Remote}))
	default:
		opts = append(opts, trace.WithNewRoot())
	}
	return tp.Tracer(ScopeName).Start(ctx, SpanReconcile, opts...)
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
