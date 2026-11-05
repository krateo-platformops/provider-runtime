package httpsignal_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit/httpsignal"
	"github.com/krateo-platformops/provider-runtime/pkg/reconciler/reconcilertest"
)

type sink struct{ got []ratelimit.Signal }

func (s *sink) Observe(sig ratelimit.Signal) { s.got = append(s.got, sig) }

// serve answers every request with the recorded response, moved to now.
func serve(t *testing.T, name string, now time.Time) *httptest.Server {
	t.Helper()
	resp, err := reconcilertest.RecordedResponse(name, now)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for k, vs := range resp.Header {
			if k != "Content-Length" {
				w.Header()[k] = vs
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// get makes one request through the Transport and returns what was reported
// and the response body.
func get(t *testing.T, ctx context.Context, rawURL string) ([]ratelimit.Signal, string) {
	t.Helper()
	s := &sink{}
	req, err := http.NewRequestWithContext(ratelimit.WithSink(ctx, s), http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := (&http.Client{Transport: httpsignal.NewTransport(nil)}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return s.got, string(body)
}

// Each recorded vendor response is read into the Signal the vendor's
// documentation says it means.
func TestRecordedResponses(t *testing.T) {
	cases := map[string]struct {
		throttled  bool
		kind       ratelimit.SignalKind
		retryAfter time.Duration
		resetIn    time.Duration // 0: no ResetAt
		hasQuota   bool
		remaining  int
		limit      int
	}{
		// 59 of 60 left: a quota report, not a slow-down, even though Reset is in the future.
		reconcilertest.GitHub200Healthy: {kind: ratelimit.SignalNone, resetIn: time.Hour + 8*time.Second, hasQuota: true, remaining: 59, limit: 60},
		// Primary limit: 403 with Remaining 0, wait until Reset.
		reconcilertest.GitHub403PrimaryExhausted: {throttled: true, kind: ratelimit.SignalThrottled, resetIn: time.Hour + 8*time.Second, hasQuota: true, limit: 5000},
		// Secondary limit: 403 with Retry-After and quota left. Retry-After alone makes it throttled.
		reconcilertest.GitHub403SecondaryRetryAfter: {throttled: true, kind: ratelimit.SignalThrottled, retryAfter: time.Minute, resetIn: time.Hour + 8*time.Second, hasQuota: true, remaining: 4890, limit: 5000},
		reconcilertest.GitHub429RetryAfter:          {throttled: true, kind: ratelimit.SignalThrottled, retryAfter: time.Minute, resetIn: time.Hour + 8*time.Second, hasQuota: true, limit: 5000},
		// Azure DevOps serving and delaying: advisory, Retry-After kept.
		reconcilertest.AzureDevOps200Advisory: {kind: ratelimit.SignalAdvisory, retryAfter: 10 * time.Second, resetIn: 5 * time.Minute, hasQuota: true, limit: 200},
		reconcilertest.AzureDevOps429Blocked:  {throttled: true, kind: ratelimit.SignalThrottled, retryAfter: 30 * time.Second, resetIn: 5 * time.Minute, hasQuota: true, limit: 200},
		// A saturated server's 503 with a Retry-After date two minutes out.
		reconcilertest.Service503RetryAfterDate: {throttled: true, kind: ratelimit.SignalThrottled, retryAfter: 2 * time.Minute},
	}
	if len(cases) != len(reconcilertest.RecordedResponseNames()) {
		t.Fatalf("%d cases for %d recorded responses: every recording needs an expectation", len(cases), len(reconcilertest.RecordedResponseNames()))
	}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			now := time.Now().Truncate(time.Second)
			resp, err := reconcilertest.RecordedResponse(name, now)
			if err != nil {
				t.Fatal(err)
			}
			s, ok := httpsignal.Parse(resp, "api.example.com", now)
			if !ok {
				t.Fatal("Parse reported nothing for a response with rate-limit headers")
			}
			if s.Key != "api.example.com" || s.Throttled != want.throttled || s.HasQuota != want.hasQuota ||
				s.Remaining != want.remaining || s.Limit != want.limit {
				t.Errorf("Signal = %+v, want Throttled=%v HasQuota=%v Remaining=%d Limit=%d", s, want.throttled, want.hasQuota, want.remaining, want.limit)
			}
			// Retry-After as a date is relative to now; allow the second it was rounded to.
			if d := s.RetryAfter - want.retryAfter; d < -time.Second || d > time.Second {
				t.Errorf("RetryAfter = %v, want %v", s.RetryAfter, want.retryAfter)
			}
			if want.resetIn == 0 && !s.ResetAt.IsZero() {
				t.Errorf("ResetAt = %v, want none", s.ResetAt)
			}
			if want.resetIn != 0 {
				if d := s.ResetAt.Sub(now) - want.resetIn; d < -time.Second || d > time.Second {
					t.Errorf("ResetAt is %v from now, want %v", s.ResetAt.Sub(now), want.resetIn)
				}
			}
			if got := ratelimit.Classify(s, now); got != want.kind {
				t.Errorf("Classify = %q, want %q", got, want.kind)
			}
		})
	}
}

func TestStatusRules(t *testing.T) {
	now := time.Now()
	resp := func(status int, h map[string]string) *http.Response {
		r := &http.Response{StatusCode: status, Header: http.Header{}}
		for k, v := range h {
			r.Header.Set(k, v)
		}
		return r
	}
	cases := map[string]struct {
		resp          *http.Response
		wantReport    bool
		wantThrottled bool
	}{
		"429 with no headers":              {resp(429, nil), true, true},
		"403 with no rate-limit headers":   {resp(403, nil), false, false},
		"403 permission error, quota left": {resp(403, map[string]string{"X-RateLimit-Remaining": "4999", "X-RateLimit-Limit": "5000"}), true, false},
		"403 with Retry-After alone":       {resp(403, map[string]string{"Retry-After": "60"}), true, true},
		"403 with Remaining 0":             {resp(403, map[string]string{"X-RateLimit-Remaining": "0"}), true, true},
		"503 with Retry-After":             {resp(503, map[string]string{"Retry-After": "120"}), true, true},
		"503 without Retry-After":          {resp(503, nil), false, false},
		"500 with Retry-After":             {resp(500, map[string]string{"Retry-After": "5"}), true, false},
		"200 with nothing":                 {resp(200, nil), false, false},
		"200 with Used and Limit only":     {resp(200, map[string]string{"X-RateLimit-Used": "4000", "X-RateLimit-Limit": "5000"}), true, false},
	}
	for name, tc := range cases {
		s, ok := httpsignal.Parse(tc.resp, "k", now)
		if ok != tc.wantReport || s.Throttled != tc.wantThrottled {
			t.Errorf("%s: Parse = %+v, %v; want report=%v throttled=%v", name, s, ok, tc.wantReport, tc.wantThrottled)
		}
	}

	// Remaining derived from Limit - Used is a quarter or less: advisory.
	s, _ := httpsignal.Parse(resp(200, map[string]string{"X-RateLimit-Used": "4000", "X-RateLimit-Limit": "5000"}), "k", now)
	if !s.HasQuota || s.Remaining != 1000 || ratelimit.Classify(s, now) != ratelimit.SignalAdvisory {
		t.Errorf("Limit 5000, Used 4000: Signal = %+v, want Remaining 1000 and advisory", s)
	}

	// X-RateLimit-Delay alone, on a served call, asks for that spacing.
	s, ok := httpsignal.Parse(resp(200, map[string]string{"X-RateLimit-Delay": "0.250"}), "k", now)
	if !ok || s.RetryAfter != 250*time.Millisecond || ratelimit.Classify(s, now) != ratelimit.SignalAdvisory {
		t.Errorf("X-RateLimit-Delay 0.250: Signal = %+v, %v; want RetryAfter 250ms, advisory", s, ok)
	}

	// Garbage is ignored rather than read as zero.
	s, ok = httpsignal.Parse(resp(200, map[string]string{"Retry-After": "soon", "X-RateLimit-Remaining": "-3", "X-RateLimit-Reset": "NaN"}), "k", now)
	if ok || s.HasQuota || s.RetryAfter != 0 || !s.ResetAt.IsZero() {
		t.Errorf("garbage headers: Signal = %+v, %v; want nothing", s, ok)
	}
}

// The Transport reports through the request's context, keys by host by
// default or by the context's key, and passes the response through unchanged.
func TestTransportReportsAndPassesThrough(t *testing.T) {
	srv := serve(t, reconcilertest.GitHub429RetryAfter, time.Now())
	u, _ := url.Parse(srv.URL)

	got, body := get(t, context.Background(), srv.URL+"/repos/o/r")
	if len(got) != 1 || !got[0].Throttled || got[0].Key != httpsignal.HostKey(u) {
		t.Fatalf("reported %+v, want one throttled signal keyed %q", got, httpsignal.HostKey(u))
	}
	if body == "" {
		t.Error("the response body did not reach the caller")
	}

	got, _ = get(t, httpsignal.WithKey(context.Background(), "api.github.com|ns/github-token"), srv.URL)
	if len(got) != 1 || got[0].Key != "api.github.com|ns/github-token" {
		t.Errorf("reported %+v, want the key from the context", got)
	}

	// Without a sink, nothing breaks: Report is a no-op.
	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	resp, err := httpsignal.NewTransport(nil).RoundTrip(req)
	if err != nil || resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("RoundTrip = %v, %v", resp, err)
	}
	_ = resp.Body.Close()
}

func TestHealthyResponseWithoutHeadersReportsNothing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer srv.Close()
	if got, _ := get(t, context.Background(), srv.URL); len(got) != 0 {
		t.Errorf("reported %+v for a response without rate-limit headers", got)
	}
}

func TestHostKey(t *testing.T) {
	for raw, want := range map[string]string{
		"https://API.GitHub.com/repos":         "api.github.com",
		"https://ghe.example.com:8443/api/v3/": "ghe.example.com:8443",
	} {
		u, _ := url.Parse(raw)
		if got := httpsignal.HostKey(u); got != want {
			t.Errorf("HostKey(%s) = %q, want %q", raw, got, want)
		}
	}
}
