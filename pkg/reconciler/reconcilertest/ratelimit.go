package reconcilertest

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
)

// RateLimitedCall runs one of the provider's external calls (an Observe, a
// Create, whatever reaches the vendor) against the server at baseURL, with ctx.
// It must pass ctx to the HTTP request (http.NewRequestWithContext, or a client
// that does), because ctx carries the sink the signals are captured on.
type RateLimitedCall func(ctx context.Context, baseURL string) error

// AssertReportsRateLimits checks that a provider reports rate limits: it feeds
// call a recorded GitHub 429, a recorded Azure DevOps advisory 200 and a
// recorded healthy GitHub 200, and fails t unless the 429 is reported as
// throttled with a deadline, the 200 as advisory, and the healthy response as
// neither. See AssertReportsThrottle, AssertReportsAdvisory and
// AssertSilentWhenHealthy.
//
// The runtime can't check this from outside: it never sees the provider's
// responses. A provider that never reports looks exactly like an endpoint that
// is never limited, and the governor then does nothing while looking
// configured. So the check belongs in the provider's own tests:
//
//	func TestReportsRateLimits(t *testing.T) {
//		reconcilertest.AssertReportsRateLimits(t, func(ctx context.Context, baseURL string) error {
//			_, err := newExternal(baseURL).Observe(ctx, fixture())
//			return err
//		})
//	}
func AssertReportsRateLimits(t *testing.T, call RateLimitedCall) {
	t.Helper()
	t.Run("throttled 429", func(t *testing.T) { AssertReportsThrottle(t, GitHub429RetryAfter, call) })
	t.Run("advisory 200", func(t *testing.T) { AssertReportsAdvisory(t, AzureDevOps200Advisory, call) })
	t.Run("healthy 200", func(t *testing.T) { AssertSilentWhenHealthy(t, GitHub200Healthy, call) })
}

// AssertReportsThrottle serves the recorded response named recordedName to
// call and fails t unless call reported a throttled Signal with a Key and a
// deadline in the future. call's error is ignored: a throttled call should
// normally fail, and how the provider maps that error is its own business.
//
// The deadline is required because the governor can't close a gate for an
// unknown time: a provider that drops the server's Retry-After or
// X-RateLimit-Reset has reported something the governor can only half use.
func AssertReportsThrottle(t testing.TB, recordedName string, call RateLimitedCall) []ratelimit.Signal {
	t.Helper()
	c := capture(t, recordedName, call)
	signals, now := c.signals, c.now
	var throttled []ratelimit.Signal
	for _, s := range signals {
		if ratelimit.Classify(s, now) == ratelimit.SignalThrottled {
			throttled = append(throttled, s)
		}
	}
	if len(throttled) == 0 {
		t.Errorf("for the recorded %s, the provider reported %d signal(s) and none throttled: %+v. "+
			"The runtime can't see the response, so an unreported throttle leaves every worker calling a server that refuses them",
			recordedName, len(signals), signals)
		return signals
	}
	for _, s := range throttled {
		if s.Key == "" {
			t.Errorf("a throttled signal carries no Key, so it is dropped: %+v", s)
		}
		if !s.Deadline(now).After(now) {
			t.Errorf("the throttled signal for key %q carries no future deadline (RetryAfter %v, ResetAt %v): "+
				"the server said how long to wait and the provider dropped it", s.Key, s.RetryAfter, s.ResetAt)
		}
	}
	return signals
}

// AssertReportsAdvisory serves the recorded response named recordedName, a
// successful response carrying rate-limit information, and fails t unless
// call succeeded and reported a Signal the governor reads as advisory.
//
// This is the check a provider that reports only on errors fails: Azure DevOps
// announces its delay on a successful response, before it starts refusing.
func AssertReportsAdvisory(t testing.TB, recordedName string, call RateLimitedCall) []ratelimit.Signal {
	t.Helper()
	c := capture(t, recordedName, call)
	signals, err, now := c.signals, c.err, c.now
	if err != nil {
		t.Errorf("the call against the recorded %s failed (%v); this check is about a successful response", recordedName, err)
		return signals
	}
	for _, s := range signals {
		if ratelimit.Classify(s, now) == ratelimit.SignalAdvisory {
			if s.Key == "" {
				t.Errorf("the advisory signal carries no Key, so it is dropped: %+v", s)
			}
			return signals
		}
	}
	t.Errorf("for the recorded %s, the provider reported %d signal(s) and none advisory: %+v. "+
		"An error-only reporter reacts only after throttling has begun",
		recordedName, len(signals), signals)
	return signals
}

// AssertSilentWhenHealthy serves the recorded response named recordedName, a
// healthy response, and fails t if call reported a Signal the governor reads
// as throttled or advisory. A healthy quota report (GitHub sends
// X-RateLimit-* on every response) is fine.
//
// It is the control for the other two: a provider reporting a throttle on every
// response would pass them, and hold every key at capacity 1 forever.
func AssertSilentWhenHealthy(t testing.TB, recordedName string, call RateLimitedCall) []ratelimit.Signal {
	t.Helper()
	c := capture(t, recordedName, call)
	signals, err, now := c.signals, c.err, c.now
	if err != nil {
		t.Errorf("the call against the recorded %s failed (%v); the fixture or the call is wrong", recordedName, err)
	}
	for _, s := range signals {
		if kind := ratelimit.Classify(s, now); kind != ratelimit.SignalNone {
			t.Errorf("for the healthy recorded %s, the provider reported a %s signal: %+v", recordedName, kind, s)
		}
	}
	return signals
}

// signalRecorder is a ratelimit.Sink that keeps what it is given.
type signalRecorder struct {
	mu      sync.Mutex
	signals []ratelimit.Signal
}

func (r *signalRecorder) Observe(s ratelimit.Signal) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.signals = append(r.signals, s)
}

// captured is what one call against a recorded response did.
type captured struct {
	signals []ratelimit.Signal
	// err is what the call returned.
	err error
	// now is the time the recorded response was moved to.
	now time.Time
}

// capture serves the recorded response to call and returns what it reported.
func capture(t testing.TB, recordedName string, call RateLimitedCall) captured {
	t.Helper()
	now := time.Now()
	resp, err := RecordedResponse(recordedName, now)
	if err != nil {
		t.Fatalf("%v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for k, vs := range resp.Header {
			if k == "Content-Length" {
				continue
			}
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	rec := &signalRecorder{}
	callErr := call(ratelimit.WithSink(context.Background(), rec), srv.URL)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return captured{signals: append([]ratelimit.Signal(nil), rec.signals...), err: callErr, now: now}
}
