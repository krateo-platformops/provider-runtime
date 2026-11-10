package tracing

import "go.opentelemetry.io/otel/attribute"

// Span names: a fixed set, never one per object (docs/design-tracing.md §4).
const (
	// SpanReconcile is the root span of one reconcile.
	SpanReconcile = "reconcile"
	// SpanConnect, SpanObserve, SpanCreate, SpanUpdate and SpanDelete are the
	// children around the provider's calls.
	SpanConnect = "connect"
	SpanObserve = "observe"
	SpanCreate  = "create"
	SpanUpdate  = "update"
	SpanDelete  = "delete"
)

// Events on the root span (docs/design-tracing.md §4.5).
const (
	// EventRateLimitDeferred: the governor deferred the pass's next call.
	EventRateLimitDeferred = "ratelimit.deferred"
	// EventReconcilePaused: the pause annotation short-circuited the pass.
	EventReconcilePaused = "reconcile.paused"
	// EventConflict: the pass requeued because a write returned a conflict.
	EventConflict = "k8s.conflict"
	// EventMarkerFailed: the marker write failed with anything but a conflict.
	EventMarkerFailed = "trace.marker_failed"
)

// Event attribute keys.
const (
	AttrRateLimitKey          = attribute.Key("provider_runtime.ratelimit.key")
	AttrRateLimitCause        = attribute.Key("provider_runtime.ratelimit.cause")
	AttrRateLimitRequeueAfter = attribute.Key("provider_runtime.ratelimit.requeue_after_s")
	AttrRateLimitUntil        = attribute.Key("provider_runtime.ratelimit.until")
	AttrStep                  = attribute.Key("provider_runtime.step")
)

// A Step names the write that returned a conflict, as the k8s.conflict event's
// provider_runtime.step attribute. The set is every kerrors.IsConflict site in
// the reconciler, and the marker write.
type Step string

const (
	StepConnect       Step = "connect"
	StepObserve       Step = "observe"
	StepHandshake     Step = "handshake"
	StepFinalizer     Step = "finalizer"
	StepCreatePending Step = "create_pending"
	StepCreate        Step = "create"
	StepLateInit      Step = "late_init"
	StepMarker        Step = "marker"
)

// Steps returns every Step, in the order above, as a new slice each call.
func Steps() []Step {
	return []Step{StepConnect, StepObserve, StepHandshake, StepFinalizer, StepCreatePending, StepCreate, StepLateInit, StepMarker}
}
