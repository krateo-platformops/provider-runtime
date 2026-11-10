package tracing

import (
	"context"
	"net/http"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/krateo-platformops/provider-runtime/pkg/meta"
)

// objectContext is the trace context to write onto an object: the active
// span's, or with tracing off the inbound context StartReconcile carried.
func objectContext(ctx context.Context) trace.SpanContext {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		return sc
	}
	if sc, ok := ctx.Value(inboundKey{}).(trace.SpanContext); ok && sc.IsValid() {
		return sc
	}
	return trace.SpanContext{}
}

// Inject writes ctx's trace context onto obj, an object another Krateo
// controller reconciles: krateo.io/traceparent, and krateo.io/tracestate when
// the context has one. The context is the active span's, or with tracing off
// the inbound context the reconciler carried.
//
// It overwrites: an existing krateo.io/traceparent is replaced, and an existing
// krateo.io/tracestate is replaced when the context has a tracestate and removed
// when it has none, so a stale tracestate never travels with a new traceparent.
// Without a valid context it returns false and changes nothing. It never writes
// krateo.io/traceparent-continued.
//
// See the package documentation for where to call it: only onto an object
// another Krateo controller reconciles, only on a write the caller makes anyway,
// never in a status write, and before the change that triggers the work.
func Inject(ctx context.Context, obj metav1.Object) bool {
	sc := objectContext(ctx)
	if !sc.IsValid() {
		return false
	}
	ann := obj.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	write(sc, ann)
	obj.SetAnnotations(ann)
	return true
}

// InjectAnnotations does what Inject does, on an annotations map. It returns
// false and changes nothing when ctx has no valid trace context, or when
// annotations is nil.
func InjectAnnotations(ctx context.Context, annotations map[string]string) bool {
	if annotations == nil {
		return false
	}
	sc := objectContext(ctx)
	if !sc.IsValid() {
		return false
	}
	write(sc, annotations)
	return true
}

// write sets sc as krateo.io/traceparent and krateo.io/tracestate on ann.
func write(sc trace.SpanContext, ann map[string]string) {
	ann[meta.AnnotationKeyTraceparent] = Canonical(sc)
	if ts := sc.TraceState().String(); ts != "" {
		ann[meta.AnnotationKeyTracestate] = ts
	} else {
		delete(ann, meta.AnnotationKeyTracestate)
	}
}

// InjectHeader sets the W3C traceparent header on h, and tracestate when there
// is one, from ctx's active span only. It never uses the inbound context, so
// with tracing off it sends nothing. It never overwrites a traceparent h
// already has, which comes from a more specific span in the same process, such
// as an otelhttp client span. It never sets baggage. It reports whether it set
// the header.
func InjectHeader(ctx context.Context, h http.Header) bool {
	if !HasActiveSpan(ctx) || h.Get(traceparentHeader) != "" {
		return false
	}
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(h))
	return true
}

// HasActiveSpan reports whether ctx holds a span with a valid span context,
// which is when InjectHeader would set a header. A transport calls it first, so
// it clones a request only when there's something to set.
func HasActiveSpan(ctx context.Context) bool {
	return trace.SpanContextFromContext(ctx).IsValid()
}
