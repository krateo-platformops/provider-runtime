package tracing

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/krateo-platformops/provider-runtime/pkg/meta"
)

// Mode is how a reconcile relates to the object's krateo.io/traceparent
// (docs/design-tracing.md §6.2, D8). In every mode the reconcile root is a new
// root; the modes differ in its link and in whether an act step joins the
// upstream trace.
type Mode int

const (
	// NoParent: the object carries no valid traceparent. The root has no link.
	NoParent Mode = iota
	// Parent: the traceparent's value is new. The root links to it, and the
	// pass's first act step (create, update or delete) is its child.
	Parent
	// Link: the value was already continued, or its window has closed. The root
	// links to it, and nothing is its child.
	Link
)

func (m Mode) String() string {
	switch m {
	case NoParent:
		return "NoParent"
	case Parent:
		return "Parent"
	case Link:
		return "Link"
	}
	return "Mode(?)"
}

// When says when the reconciler writes Decision.Marker.
type When int

const (
	// Never: there's no marker to write.
	Never When = iota
	// BeforeFirstAction: just before the pass's first Create, Update or Delete
	// call. A pass that doesn't act writes nothing.
	BeforeFirstAction
	// Now: right after the span starts, past the pause and Orphan checks.
	Now
)

func (w When) String() string {
	switch w {
	case Never:
		return "Never"
	case BeforeFirstAction:
		return "BeforeFirstAction"
	case Now:
		return "Now"
	}
	return "When(?)"
}

// A Decision is what Decide concluded for one reconcile.
type Decision struct {
	// Mode is how the reconcile relates to Remote.
	Mode Mode
	// Remote is the annotation's span context. It is valid in Parent and Link.
	Remote trace.SpanContext
	// Marker is the krateo.io/traceparent-continued value to write, or "" when
	// WriteMarker is Never.
	Marker string
	// WriteMarker is when the reconciler writes Marker.
	WriteMarker When
}

// Decide applies the parent-or-link rule of docs/design-tracing.md §6.2. It is
// pure, and it allocates nothing when the object carries no
// krateo.io/traceparent.
//
//   - annotations are the object's annotations.
//   - remembered is the value this process already marked, or failed to mark,
//     on this object, or "".
//   - firstSeen is when this process first saw the annotation's current value,
//     by the monotonic clock. The zero time means first sight, with the window
//     open.
//   - window is how long a new value keeps making Parent passes that don't act.
//     0 means Parent with WriteMarker Now on the first pass: the first-sight rule.
//
// The rules, in order. R is the annotation's span context. R is continued when
// the object's marker, or remembered, has R's trace ID and span ID.
//
//  1. R isn't valid: NoParent, Never.
//  2. R is continued: Link, Never.
//  3. window is 0: Parent, Now.
//  4. firstSeen is zero, or now - firstSeen < window: Parent, BeforeFirstAction.
//  5. Otherwise: Link, Now.
func Decide(annotations map[string]string, remembered string, firstSeen, now time.Time, window time.Duration) Decision {
	tp := annotations[meta.AnnotationKeyTraceparent]
	if tp == "" {
		return Decision{}
	}
	r := parse(tp, annotations[meta.AnnotationKeyTracestate])
	if !r.IsValid() {
		return Decision{}
	}
	if continues(annotations[meta.AnnotationKeyTraceparentContinued], r) || continues(remembered, r) {
		return Decision{Mode: Link, Remote: r}
	}
	d := Decision{Remote: r, Marker: Canonical(r)}
	switch {
	case window <= 0:
		d.Mode, d.WriteMarker = Parent, Now
	case firstSeen.IsZero() || now.Sub(firstSeen) < window:
		d.Mode, d.WriteMarker = Parent, BeforeFirstAction
	default:
		d.Mode, d.WriteMarker = Link, Now
	}
	return d
}

// Canonical is sc's traceparent as W3C TraceContext writes it: version 00,
// lower-case hex, and only the sampled and random flags. It's the form the
// marker records, and "" for an invalid sc.
func Canonical(sc trace.SpanContext) string {
	if !sc.IsValid() {
		return ""
	}
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(trace.ContextWithSpanContext(context.Background(), sc), carrier)
	return carrier[traceparentHeader]
}

// The W3C carrier keys.
const (
	traceparentHeader = "traceparent"
	tracestateHeader  = "tracestate"
)

// parse reads a traceparent and an optional tracestate into a span context. An
// invalid traceparent gives an invalid span context.
func parse(traceparent, tracestate string) trace.SpanContext {
	carrier := propagation.MapCarrier{traceparentHeader: traceparent}
	if tracestate != "" {
		carrier[tracestateHeader] = tracestate
	}
	ctx := propagation.TraceContext{}.Extract(context.Background(), carrier)
	return trace.SpanContextFromContext(ctx)
}

// continues reports whether the traceparent value v names r: the same trace ID
// and span ID. Flags and tracestate don't count.
func continues(v string, r trace.SpanContext) bool {
	if v == "" {
		return false
	}
	m := parse(v, "")
	return m.IsValid() && m.TraceID() == r.TraceID() && m.SpanID() == r.SpanID()
}
