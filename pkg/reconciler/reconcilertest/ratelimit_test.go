package reconcilertest

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit/httpsignal"
)

// fatalTB extends recordingTB with Fatalf, which capture calls on a broken
// fixture.
type fatalTB struct{ recordingTB }

func (f *fatalTB) Fatalf(format string, args ...any) {
	f.failures = append(f.failures, fmt.Sprintf(format, args...))
}

// providerCall is a provider's external call: a GET against baseURL with the
// given client, that fails on a non-2xx status.
func providerCall(c *http.Client) RateLimitedCall {
	return func(ctx context.Context, baseURL string) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/repos/o/r", nil)
		if err != nil {
			return err
		}
		resp, err := c.Do(req)
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.Copy(io.Discard, resp.Body)
		if resp.StatusCode >= 300 {
			return fmt.Errorf("unexpected status %d", resp.StatusCode)
		}
		return nil
	}
}

// A provider using httpsignal conforms.
func TestAssertReportsRateLimitsPassesAProviderUsingHTTPSignal(t *testing.T) {
	AssertReportsRateLimits(t, providerCall(&http.Client{Transport: httpsignal.NewTransport(nil)}))
}

// Every recorded throttle shape is reported as throttled with a deadline.
func TestEveryRecordedThrottleConforms(t *testing.T) {
	call := providerCall(&http.Client{Transport: httpsignal.NewTransport(nil)})
	for _, name := range []string{GitHub403PrimaryExhausted, GitHub403SecondaryRetryAfter, GitHub429RetryAfter, AzureDevOps429Blocked, Service503RetryAfterDate} {
		t.Run(name, func(t *testing.T) { AssertReportsThrottle(t, name, call) })
	}
}

// reportingTransport reports what report returns for each response.
type reportingTransport struct {
	report func(resp *http.Response) (ratelimit.Signal, bool)
}

func (rt reportingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err == nil {
		if s, ok := rt.report(resp); ok {
			ratelimit.Report(req.Context(), s)
		}
	}
	return resp, err
}

func TestRateLimitAssertsFailBrokenReporters(t *testing.T) {
	never := providerCall(http.DefaultClient)
	errorOnly := providerCall(&http.Client{Transport: reportingTransport{report: func(resp *http.Response) (ratelimit.Signal, bool) {
		if resp.StatusCode < 400 {
			return ratelimit.Signal{}, false
		}
		return httpsignal.Parse(resp, resp.Request.URL.Host, time.Now())
	}}})
	alwaysThrottled := providerCall(&http.Client{Transport: reportingTransport{report: func(resp *http.Response) (ratelimit.Signal, bool) {
		return ratelimit.Signal{Key: resp.Request.URL.Host, Throttled: true, RetryAfter: time.Minute}, true
	}}})
	noDeadline := providerCall(&http.Client{Transport: reportingTransport{report: func(resp *http.Response) (ratelimit.Signal, bool) {
		return ratelimit.Signal{Key: resp.Request.URL.Host, Throttled: resp.StatusCode == http.StatusTooManyRequests}, resp.StatusCode == http.StatusTooManyRequests
	}}})

	cases := map[string]struct {
		assert      func(testing.TB, string, RateLimitedCall) []ratelimit.Signal
		recorded    string
		call        RateLimitedCall
		wantMessage string
	}{
		"never reports a 429":         {AssertReportsThrottle, GitHub429RetryAfter, never, "none throttled"},
		"drops the server's deadline": {AssertReportsThrottle, GitHub429RetryAfter, noDeadline, "no future deadline"},
		"never reports an advisory":   {AssertReportsAdvisory, AzureDevOps200Advisory, never, "none advisory"},
		"reports only on errors":      {AssertReportsAdvisory, AzureDevOps200Advisory, errorOnly, "none advisory"},
		"throttles every response":    {AssertSilentWhenHealthy, GitHub200Healthy, alwaysThrottled, "reported a throttled signal"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tb := &fatalTB{recordingTB{TB: t}}
			tc.assert(tb, tc.recorded, tc.call)
			if len(tb.failures) == 0 {
				t.Fatal("the assert passed a broken reporter")
			}
			if !strings.Contains(strings.Join(tb.failures, "\n"), tc.wantMessage) {
				t.Errorf("failures %q do not say %q", tb.failures, tc.wantMessage)
			}
		})
	}
}

// RecordedResponse moves the recording's deadlines to now.
func TestRecordedResponseMovesDeadlinesToNow(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	resp, err := RecordedResponse(GitHub429RetryAfter, now)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := httpsignal.Parse(resp, "k", now)
	// Recorded: Date 23:49:16, reset 00:49:24 the next day.
	if want := now.Add(time.Hour + 8*time.Second); !s.ResetAt.Equal(want) {
		t.Errorf("ResetAt = %v, want %v", s.ResetAt, want)
	}
	if _, err := RecordedResponse("no-such-recording", now); err == nil {
		t.Error("an unknown recording returned no error")
	}
}
