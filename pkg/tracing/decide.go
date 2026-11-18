package tracing

import (
	"time"

	"github.com/krateo-platformops/plumbing/tracecontext"
	"go.opentelemetry.io/otel/trace"
)

// The parent-or-link rule lives in plumbing's tracecontext, so that
// provider-runtime and unstructured-runtime apply one identical rule
// (docs/design-tracing.md §14). The names below are aliases and thin wrappers:
// every exported name, signature and doc is provider-runtime's, unchanged.

// Mode is how a reconcile relates to the object's krateo.io/traceparent
// (docs/design-tracing.md §6.2, D8). In every mode the reconcile root is a new
// root; the modes differ in its link and in whether an act step joins the
// upstream trace.
type Mode = tracecontext.Mode

const (
	// NoParent: the object carries no valid traceparent. The root has no link.
	NoParent = tracecontext.NoParent
	// Parent: the traceparent's value is new. The root links to it, and the
	// pass's first act step (create, update or delete) is its child.
	Parent = tracecontext.Parent
	// Link: the value was already continued, or its window has closed. The root
	// links to it, and nothing is its child.
	Link = tracecontext.Link
)

// When says when the reconciler writes Decision.Marker.
type When = tracecontext.When

const (
	// Never: there's no marker to write.
	Never = tracecontext.Never
	// BeforeFirstAction: just before the pass's first Create, Update or Delete
	// call. A pass that doesn't act writes nothing.
	BeforeFirstAction = tracecontext.BeforeFirstAction
	// Now: right after the span starts, past the pause and Orphan checks.
	Now = tracecontext.Now
)

// A Decision is what Decide concluded for one reconcile.
//
//   - Mode is how the reconcile relates to Remote.
//   - Remote is the annotation's span context. It is valid in Parent and Link.
//   - Marker is the krateo.io/traceparent-continued value to write, or "" when
//     WriteMarker is Never.
//   - WriteMarker is when the reconciler writes Marker.
type Decision = tracecontext.Decision

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
	return tracecontext.Decide(annotations, remembered, firstSeen, now, window)
}

// Canonical is sc's traceparent as W3C TraceContext writes it: version 00,
// lower-case hex, and only the sampled and random flags. It's the form the
// marker records, and "" for an invalid sc.
func Canonical(sc trace.SpanContext) string {
	return tracecontext.Canonical(sc)
}
