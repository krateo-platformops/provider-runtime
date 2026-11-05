// Package ratelimit slows reconciles down automatically when an external API
// signals a rate limit, per endpoint key, or when the controller's own container
// is CPU- or memory-saturated. It replaces hand-tuning --max-reconcile-rate and
// --poll per installation.
//
// Design: docs/design-rate-limit-governor.md. The Signal shape and its meaning
// are shared with unstructured-runtime's pkg/ratelimit, which declares the same
// type. The two are deliberately duplicated rather than imported from a shared
// module (design §10, open question 1), so a change here must be made there too.
//
// The runtime makes no outbound calls of its own: the provider does. So a rate
// limit is visible only to the provider, which reports it with Report; the
// runtime acts on it through a Governor. Without a Governor, Report is a no-op
// and nothing changes.
package ratelimit

import (
	"context"
	"time"
)

// Signal is one observation of an endpoint's rate-limit state, as the provider
// saw it. Report it on a successful response as well as on a failed one: Azure
// DevOps announces a delay on a successful response before it starts refusing.
//
// Field names and meaning are identical to unstructured-runtime's Signal.
type Signal struct {
	// Key identifies the rate-limited scope: normally the endpoint host, or
	// host|credential-reference when the quota is per identity. It must be the
	// same string the provider's RateLimitKeyer returns for the object, or the
	// signal throttles a key no call is gated on. A Signal with an empty Key is
	// dropped.
	Key string

	// RetryAfter is how long the server asked the client to wait, when it said
	// so as a delay. Zero when it gave an instant instead.
	RetryAfter time.Duration

	// ResetAt is when the server's window resets, when it gave an instant.
	// When both RetryAfter and ResetAt are set, the later of the two wins.
	ResetAt time.Time

	// Remaining and Limit are the quota counters, when the server publishes
	// them. HasQuota tells "Remaining is 0" apart from "no counters were sent".
	Remaining, Limit int
	HasQuota         bool

	// Throttled means the server refused this request. A Signal that is not
	// Throttled is advisory at most: the call happened, and the server says the
	// client is close to, or already being slowed by, a limit.
	Throttled bool
}

// Deadline returns the instant s says to wait until: the later of now+RetryAfter
// and ResetAt. It is the zero time when s names neither.
func (s Signal) Deadline(now time.Time) time.Time {
	var d time.Time
	if s.RetryAfter > 0 {
		d = now.Add(s.RetryAfter)
	}
	if s.ResetAt.After(d) {
		d = s.ResetAt
	}
	return d
}

// SignalKind is what a Signal means to the governor.
type SignalKind string

const (
	// SignalThrottled closes the gate for the key until the signal's deadline,
	// and drops the key's capacity to 1.
	SignalThrottled SignalKind = "throttled"
	// SignalAdvisory halves the key's capacity, with a minimum of 1.
	SignalAdvisory SignalKind = "advisory"
	// SignalNone changes nothing. A healthy quota report, such as GitHub's
	// X-RateLimit-* headers on every response, is SignalNone.
	SignalNone SignalKind = "none"
)

// advisoryQuotaDivisor sets the share of the quota below which a non-throttled
// signal is advisory: Remaining*advisoryQuotaDivisor <= Limit, that is a
// quarter or less left. The same threshold as unstructured-runtime's governor.
const advisoryQuotaDivisor = 4

// Classify says what s means at now. The governor acts on it, and the
// conformance helpers in pkg/reconciler/reconcilertest use it to tell an
// advisory signal from a healthy one, so both read a Signal the same way.
//
//   - Throttled is SignalThrottled.
//   - Otherwise, quota counters under a quarter of the limit are
//     SignalAdvisory, as is a RetryAfter (a server asking for a delay on a call
//     it served, as Azure DevOps does), or a future ResetAt sent without quota
//     counters.
//   - A future ResetAt sent next to a healthy quota is SignalNone: it is only
//     the end of the current window, and GitHub sends it on every response.
func Classify(s Signal, now time.Time) SignalKind {
	if s.Throttled {
		return SignalThrottled
	}
	if s.HasQuota && s.Limit > 0 && s.Remaining*advisoryQuotaDivisor <= s.Limit {
		return SignalAdvisory
	}
	if s.RetryAfter > 0 {
		return SignalAdvisory
	}
	if !s.HasQuota && s.ResetAt.After(now) {
		return SignalAdvisory
	}
	return SignalNone
}

// Sink receives signals. The reconciler installs the Governor as the sink of
// the context it passes to Connect and to every ExternalClient call.
type Sink interface {
	Observe(Signal)
}

type sinkKey struct{}

// WithSink returns a context carrying s. A nil s returns ctx unchanged.
func WithSink(ctx context.Context, s Sink) context.Context {
	if s == nil {
		return ctx
	}
	return context.WithValue(ctx, sinkKey{}, s)
}

// Report records s on the sink ctx carries. It is a no-op when ctx carries no
// sink, which is the case whenever no Governor is configured, so a provider can
// report unconditionally. A Signal with an empty Key is dropped: the empty key
// is not a scope.
//
// Report from the provider's own code (or let pkg/ratelimit/httpsignal do it)
// with the context the runtime passed in, on success and on error.
func Report(ctx context.Context, s Signal) {
	if s.Key == "" {
		return
	}
	if sink, ok := ctx.Value(sinkKey{}).(Sink); ok && sink != nil {
		sink.Observe(s)
	}
}
