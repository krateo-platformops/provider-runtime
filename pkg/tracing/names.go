package tracing

import (
	"github.com/krateo-platformops/plumbing/tracecontext"
	"go.opentelemetry.io/otel/attribute"
)

// Span names: a fixed set, never one per object (docs/design-tracing.md §4).
const (
	// SpanReconcile is the root span of one reconcile.
	SpanReconcile = tracecontext.SpanReconcile
	// SpanConnect, SpanObserve, SpanCreate, SpanUpdate and SpanDelete are the
	// children around the provider's calls.
	SpanConnect = "connect"
	SpanObserve = "observe"
	SpanCreate  = "create"
	SpanUpdate  = "update"
	SpanDelete  = "delete"
)

// Events on the root span (docs/design-tracing.md §4.5). EventReconcilePaused,
// EventConflict and EventMarkerFailed, and the AttrStep key below, are
// plumbing's tracecontext constants, which unstructured-runtime emits too. The
// rate-limit events and keys are provider-runtime's own.
const (
	// EventRateLimitDeferred: the governor deferred the pass's next call.
	EventRateLimitDeferred = "ratelimit.deferred"
	// EventReconcilePaused: the pause annotation short-circuited the pass.
	EventReconcilePaused = tracecontext.EventReconcilePaused
	// EventConflict: a write returned a conflict. At every Step but
	// StepStatus the pass requeues, rate-limited, with a nil error; a status
	// conflict is returned as the pass's error.
	EventConflict = tracecontext.EventConflict
	// EventMarkerFailed: the marker write failed with anything but a conflict.
	EventMarkerFailed = tracecontext.EventMarkerFailed
	// EventCredentialUseAcquire: one per credential-use Acquire in the pass
	// (docs/design-credential-use-finalizer.md §8 step 5), with what it held
	// and its outcome; never a ref's name or a Secret's data.
	EventCredentialUseAcquire = "credentialuse.acquire"
	// EventCredentialUseRelease is reserved: release runs in the
	// credential-use write loops, outside any reconcile span.
	EventCredentialUseRelease = "credentialuse.release"
)

// The credentialuse.acquire event's attributes: the refs the pass named,
// chains followed, how many it held, how many it didn't by reason, as
// AttrCredentialUseUnheldPrefix plus the reason (only the reasons present),
// and the outcome.
const (
	AttrCredentialUseRefs    = attribute.Key("provider_runtime.credentialuse.refs")
	AttrCredentialUseHeld    = attribute.Key("provider_runtime.credentialuse.held")
	AttrCredentialUseOutcome = attribute.Key("provider_runtime.credentialuse.outcome")
	// AttrCredentialUseUnheldPrefix prefixes a count per reason, as
	// provider_runtime.credentialuse.unheld.Missing.
	AttrCredentialUseUnheldPrefix = "provider_runtime.credentialuse.unheld."
)

// The credentialuse.acquire event's outcomes.
const (
	// CredentialUseOK: the pass proceeds with its refs acquired.
	CredentialUseOK = "ok"
	// CredentialUseDegraded: the acquire failed for an established object,
	// which proceeds (fail open).
	CredentialUseDegraded = "degraded"
	// CredentialUseMustNotConnect: the object is new and its acquire failed,
	// or a ref is being deleted: the pass stops before Connect.
	CredentialUseMustNotConnect = "must_not_connect"
	// CredentialUseConflict: the record write conflicted; the pass requeues.
	CredentialUseConflict = "conflict"
)

// Event attribute keys.
const (
	AttrRateLimitKey          = attribute.Key("provider_runtime.ratelimit.key")
	AttrRateLimitCause        = attribute.Key("provider_runtime.ratelimit.cause")
	AttrRateLimitRequeueAfter = attribute.Key("provider_runtime.ratelimit.requeue_after_s")
	AttrRateLimitUntil        = attribute.Key("provider_runtime.ratelimit.until")
	AttrStep                  = tracecontext.AttrStep
)

// AttrActed is a bool on the reconcile root, true when the pass started an act
// step (create, update or delete), joined upstream or not, and never set
// false. The collector's tail sampler keeps the traces that carry it, so every
// change a provider makes keeps its reconcile (provider-runtime#74,
// docs/design-tracing.md §6.7). It's plumbing's tracecontext key, which
// unstructured-runtime sets too.
const AttrActed = tracecontext.AttrActed

// A Step names the write that returned a conflict, as the k8s.conflict event's
// provider_runtime.step attribute. The set is every kerrors.IsConflict site in
// the reconciler, and the marker write. StepStatus is every status write: one
// site, which all the reconciler's status writes go through.
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
	StepStatus        Step = "status"
	// StepCredentialUse is the credential-use record write (A1), a merge
	// patch guarded by the object's resourceVersion
	// (docs/design-credential-use-finalizer.md §6.2, §8 step 5).
	StepCredentialUse Step = "credential_use"
)

// Steps returns every Step, in the order above, as a new slice each call.
func Steps() []Step {
	return []Step{StepConnect, StepObserve, StepHandshake, StepFinalizer, StepCreatePending, StepCreate, StepLateInit, StepMarker, StepStatus, StepCredentialUse}
}
