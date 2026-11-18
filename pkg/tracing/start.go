package tracing

import (
	"context"
	"time"

	"github.com/krateo-platformops/plumbing/tracecontext"
	"go.opentelemetry.io/otel/trace"
)

// The root span's attribute keys: exactly these three, as unstructured-runtime
// sets them.
const (
	AttrObjectKind      = tracecontext.AttrObjectKind
	AttrObjectName      = tracecontext.AttrObjectName
	AttrObjectNamespace = tracecontext.AttrObjectNamespace
)

// Object names the object a reconcile is about.
//
//   - Kind is the Kind alone, such as Repo, without its group.
//   - Name is the object's name.
//   - Namespace is the object's namespace, "" for a cluster-scoped object.
type Object = tracecontext.Object

// tracer is tp's tracer for provider-runtime's instrumentation scope, or nil,
// which tracecontext reads as tracing off, when tp is nil.
func tracer(tp trace.TracerProvider) trace.Tracer {
	if tp == nil {
		return nil
	}
	return tp.Tracer(ScopeName)
}

// StartReconcile opens the reconcile root span for obj, starting at start.
// Every reconcile is its own trace (docs/design-tracing.md, D8): the root is
// always a new root, never a child of d.Remote and never of a span already in
// ctx. When d.Remote is valid (modes Parent and Link) it carries one link to
// it; with NoParent it has none. Only the pass's first act step for a new value
// joins the upstream trace (StartActStep), which adds a second link, from the
// root to that act. The span is SpanKindServer, named reconcile, with exactly
// the attributes k8s.object.kind, k8s.object.name and k8s.object.namespace.
//
// With tp nil it starts nothing. It returns ctx carrying d.Remote, when that's
// valid, for Inject and InjectAnnotations only, and the no-op span.
func StartReconcile(ctx context.Context, tp trace.TracerProvider, obj Object, d Decision, start time.Time) (context.Context, trace.Span) {
	return tracecontext.StartReconcile(ctx, tracer(tp), obj, d, start)
}

// StartActStep opens an act step, the create, update or delete span around
// the provider's call, for obj. With upstream valid, it is the first act for a
// new krateo.io/traceparent value (D8):
//
//   - its parent is upstream, so the act and the calls it makes join the
//     upstream trace;
//   - it carries one link back to the reconcile root in ctx;
//   - it carries the root's three k8s.object attributes, since it isn't the
//     root's child and its root is often not stored;
//   - the root gains a link to it (Span.AddLink), when the act is sampled. An
//     unsampled act, which a parent-based sampler makes under an unsampled
//     upstream, is never exported, so the root doesn't point at it.
//
// With upstream invalid it is an ordinary StartStep child of the root. With tp
// nil it starts nothing.
func StartActStep(ctx context.Context, tp trace.TracerProvider, obj Object, step string, upstream trace.SpanContext) (context.Context, trace.Span) {
	return tracecontext.StartActStep(ctx, tracer(tp), obj, step, upstream)
}

// StartStep opens the child span named step under the reconcile root in ctx,
// with tp's tracer. It's SpanKindInternal and has no attributes of its own. With
// tp nil it starts nothing and returns ctx and the no-op span.
func StartStep(ctx context.Context, tp trace.TracerProvider, step string) (context.Context, trace.Span) {
	return tracecontext.StartStep(ctx, tracer(tp), step)
}

// MarkActed sets AttrActed on the span active in ctx, which must be the pass's
// reconcile root. StartActStep marks the root itself, so a caller needs
// MarkActed only for an act it opens with StartStep. Without a recording span
// in ctx it does nothing.
func MarkActed(ctx context.Context) {
	tracecontext.MarkActed(ctx)
}
