package tracing

import (
	"time"

	"github.com/krateo-platformops/plumbing/tracecontext"
	"go.opentelemetry.io/otel/trace"
)

// parse reads a traceparent and an optional tracestate into a span context. An
// invalid traceparent gives an invalid span context. It is the parse Decide
// applies, which lives in tracecontext: Decide's Remote is the annotation's span
// context when that's valid, and the invalid span context otherwise.
func parse(traceparent, tracestate string) trace.SpanContext {
	a := map[string]string{tracecontext.AnnotationKeyTraceparent: traceparent}
	if tracestate != "" {
		a[tracecontext.AnnotationKeyTracestate] = tracestate
	}
	return tracecontext.Decide(a, "", time.Time{}, time.Time{}, 0).Remote
}
