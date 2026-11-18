// Package httpsignal reports rate-limit signals from HTTP responses, so a
// provider that talks to GitHub, Azure DevOps or any API using the same
// headers doesn't rewrite the parsing. Wrap the provider's HTTP client
// transport with NewTransport, pass the context the runtime handed to Connect
// or the ExternalClient method into each request (http.NewRequestWithContext),
// and every response that carries rate-limit information is reported with
// ratelimit.Report. The response is passed through unchanged.
//
// The runtime core never parses HTTP; this package is optional (design §6).
//
// # Trace propagation
//
// When the request's context holds an active span, which is the reconcile's
// connect, observe, create, update or delete span with tracing on, RoundTrip
// sets the W3C traceparent header, and tracestate when there is one, on a clone
// of the request (docs/design-tracing.md §6.6). It never sets baggage, never
// overwrites a traceparent the request already carries (an otelhttp transport
// wrapped outside it, for example), and never uses an inbound context the
// reconciler carries with tracing off: with tracing off it sends no trace
// header. Set Transport.NoTracePropagation for a host that must not receive one.
//
// # Headers
//
// Verified against the vendors' documentation on 2026-11-05:
//
//   - GitHub, "Rate limits for the REST API"
//     (https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api):
//     x-ratelimit-limit, -remaining and -used are request counts in the
//     current window; x-ratelimit-reset is the window's end in UTC epoch
//     seconds; x-ratelimit-resource names the resource counted against. Over
//     the primary limit the response is a 403 or 429 with
//     x-ratelimit-remaining 0: wait until x-ratelimit-reset. Over a secondary
//     limit it is a 403 or 429, and when retry-after is present, wait that many
//     seconds; it may come with remaining above 0.
//   - Azure DevOps, "Rate and usage limits"
//     (https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rate-limits):
//     Retry-After is in seconds and may be sent on an HTTP 200 (the request was
//     served and delayed); X-RateLimit-Delay is how long the request was
//     delayed, in seconds with up to three decimals; X-RateLimit-Limit and
//     -Remaining are TSTUs, Remaining is 0 once requests are delayed or
//     blocked; X-RateLimit-Reset is Unix epoch time; X-RateLimit-Resource names
//     the threshold, for display only. A blocked request is a 429.
//   - Retry-After is either delay-seconds or an HTTP-date (RFC 9110 §10.2.3).
//
// Vendor headers change without notice. When they do, update this package and
// its recorded responses in pkg/reconciler/reconcilertest/recorded.
//
// # When a response is throttled
//
//   - a 429;
//   - a 403 with X-RateLimit-Remaining 0, or with Retry-After. Retry-After is
//     authoritative on its own: GitHub's secondary limits arrive that way,
//     sometimes with quota left;
//   - a 503 with Retry-After: a saturated server, not a rate-limited one, but
//     it named a time to come back (design §7).
//
// Any other response carrying these headers is reported as not throttled; the
// governor reads it as advisory or healthy (ratelimit.Classify). A secondary
// limit signalled only in a 403's body, with neither Retry-After nor Remaining
// 0, is not detected: body parsing is not in v1, so classifying it stays the
// provider's job.
package httpsignal

import (
	"context"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
	"github.com/krateo-platformops/provider-runtime/pkg/tracing"
)

// Header names, canonicalised.
const (
	headerRetryAfter = "Retry-After"
	headerRemaining  = "X-Ratelimit-Remaining"
	headerLimit      = "X-Ratelimit-Limit"
	headerUsed       = "X-Ratelimit-Used"
	headerReset      = "X-Ratelimit-Reset"
	headerDelay      = "X-Ratelimit-Delay"
	headerResource   = "X-Ratelimit-Resource"
)

type keyContext struct{}

// WithKey returns a context whose requests report signals under key instead of
// the request's host. Use it where the quota is per identity: key on
// host|credential-reference, matching what the provider's RateLimitKeyer
// returns.
func WithKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, keyContext{}, key)
}

// HostKey is the default key of a request to u: its host, with the port when u
// names one, in lower case. A RateLimitKeyer keying by host should return
// HostKey of the same base URL, so the gate and the signals agree.
func HostKey(u *url.URL) string {
	return strings.ToLower(u.Host)
}

func keyFor(req *http.Request) string {
	if k, ok := req.Context().Value(keyContext{}).(string); ok && k != "" {
		return k
	}
	return HostKey(req.URL)
}

// Transport is an http.RoundTripper that reports a ratelimit.Signal for each
// response carrying rate-limit information, to the sink on the request's
// context.
type Transport struct {
	// Base makes the request. Nil means http.DefaultTransport.
	Base http.RoundTripper

	// NoTracePropagation turns off the traceparent header, for a provider whose
	// external host must not receive one.
	NoTracePropagation bool

	// now is replaced in tests.
	now func() time.Time
}

// NewTransport wraps base. A nil base means http.DefaultTransport.
func NewTransport(base http.RoundTripper) *Transport {
	return &Transport{Base: base}
}

// RoundTrip makes the request with Base, reports the response's signal, and
// returns the response and error unchanged. With an active span in the
// request's context it first sets traceparent on a clone of the request (see
// the package documentation); the caller's request is never changed.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	if !t.NoTracePropagation && tracing.HasActiveSpan(req.Context()) && req.Header.Get("traceparent") == "" {
		req = req.Clone(req.Context())
		tracing.InjectHeader(req.Context(), req.Header)
	}
	resp, err := base.RoundTrip(req)
	if err != nil || resp == nil {
		return resp, err
	}
	now := time.Now
	if t.now != nil {
		now = t.now
	}
	if s, ok := Parse(resp, keyFor(req), now()); ok {
		ratelimit.Report(req.Context(), s)
	}
	return resp, nil
}

// Parse reads resp's rate-limit headers into a Signal for key. ok is false
// when resp carries no rate-limit information and is not throttled, so there
// is nothing to report.
//
// X-RateLimit-Delay on a response that is not throttled and has no
// Retry-After becomes the Signal's RetryAfter: the server is slowing each
// request down by that much, and asking for the same spacing is the
// conservative reading. On a throttled response it is not used, because only
// Retry-After and X-RateLimit-Reset name a deadline.
func Parse(resp *http.Response, key string, now time.Time) (s ratelimit.Signal, ok bool) {
	h := resp.Header
	s.Key = key

	retryAfter, hasRetryAfter := parseRetryAfter(h.Get(headerRetryAfter), now)
	s.RetryAfter = retryAfter

	remaining, hasRemaining := parseCount(h.Get(headerRemaining))
	limit, hasLimit := parseCount(h.Get(headerLimit))
	used, hasUsed := parseCount(h.Get(headerUsed))
	if !hasRemaining && hasLimit && hasUsed {
		remaining, hasRemaining = max(0, limit-used), true
	}
	if hasRemaining {
		s.HasQuota = true
		s.Remaining = remaining
		s.Limit = limit
	}

	if reset, hasReset := parseEpoch(h.Get(headerReset)); hasReset {
		s.ResetAt = reset
	}

	switch resp.StatusCode {
	case http.StatusTooManyRequests:
		s.Throttled = true
	case http.StatusForbidden:
		s.Throttled = hasRetryAfter || (hasRemaining && remaining == 0)
	case http.StatusServiceUnavailable:
		s.Throttled = hasRetryAfter
	}

	delay, hasDelay := parseSeconds(h.Get(headerDelay))
	if hasDelay && !s.Throttled && s.RetryAfter == 0 {
		s.RetryAfter = delay
	}

	found := s.Throttled || hasRetryAfter || hasRemaining || hasLimit || hasUsed || !s.ResetAt.IsZero() || hasDelay ||
		h.Get(headerResource) != ""
	return s, found
}

// Resource returns X-RateLimit-Resource: which limit a response counted
// against (GitHub) or which threshold was reached (Azure DevOps). It is for
// showing to a human, for example in an error message; Azure DevOps documents
// that its values change without warning, so don't compute on it.
func Resource(h http.Header) string {
	return h.Get(headerResource)
}

// parseRetryAfter reads delay-seconds or an HTTP-date. A date in the past is a
// zero delay; ok reports that the header was present and valid.
func parseRetryAfter(v string, now time.Time) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	if d, ok := parseSeconds(v); ok {
		return d, true
	}
	t, err := http.ParseTime(v)
	if err != nil {
		return 0, false
	}
	if d := t.Sub(now); d > 0 {
		return d, true
	}
	return 0, true
}

// maxSeconds bounds a parsed delay, so a hostile header can't overflow a
// time.Duration (about 292 years).
const maxSeconds = float64(100 * 365 * 24 * 60 * 60)

// parseSeconds reads a non-negative number of seconds, possibly fractional.
func parseSeconds(v string) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || math.IsNaN(f) || f < 0 {
		return 0, false
	}
	f = min(f, maxSeconds)
	return time.Duration(math.Ceil(f * float64(time.Second))), true
}

// parseCount reads a non-negative count. Azure DevOps's TSTU counts may be
// fractional; they are rounded down, the conservative direction for what is
// left.
func parseCount(v string) (int, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || math.IsNaN(f) || f < 0 {
		return 0, false
	}
	return int(math.Floor(min(f, math.MaxInt32))), true
}

// parseEpoch reads a Unix time in seconds.
func parseEpoch(v string) (time.Time, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}, false
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || math.IsNaN(f) || f <= 0 || f > 1e11 {
		return time.Time{}, false
	}
	sec, frac := math.Modf(f)
	return time.Unix(int64(sec), int64(frac*1e9)), true
}
