package reconcilertest

import (
	"bufio"
	"bytes"
	"embed"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/krateo-platformops/provider-runtime/pkg/errors"
)

// Recorded vendor responses, as raw HTTP/1.1 in recorded/<name>.http.
//
// Provenance, so nobody mistakes a constructed fixture for a capture:
//
//   - GitHub200Healthy is recorded verbatim from GET https://api.github.com/zen
//     (unauthenticated) on 2026-11-04, with the request id and edge region
//     removed.
//   - The GitHub 403 and 429 responses keep that recorded envelope and set the
//     status, headers and body GitHub documents for its primary and secondary
//     limits (https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api).
//     No live throttled GitHub response was captured.
//   - The Azure DevOps responses keep the header envelope recorded from
//     dev.azure.com on 2026-11-04 (session and auth headers removed) and set
//     the headers documented in "Rate and usage limits"
//     (https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rate-limits).
//     No live delayed or blocked Azure DevOps response was captured.
//   - Service503RetryAfterDate is a generic 503 with Retry-After as an
//     HTTP-date.
const (
	GitHub200Healthy             = "github-200-healthy"
	GitHub403PrimaryExhausted    = "github-403-primary-exhausted"
	GitHub403SecondaryRetryAfter = "github-403-secondary-retry-after"
	GitHub429RetryAfter          = "github-429-retry-after"
	AzureDevOps200Advisory       = "azure-devops-200-advisory"
	AzureDevOps429Blocked        = "azure-devops-429-blocked"
	Service503RetryAfterDate     = "service-503-retry-after-date"
)

const (
	recordedDir          = "recorded"
	headerDate           = "Date"
	headerRetryAfter     = "Retry-After"
	headerRateLimitReset = "X-Ratelimit-Reset"
)

//go:embed recorded/*.http
var recorded embed.FS

// RecordedResponseNames lists the recorded responses.
func RecordedResponseNames() []string {
	entries, _ := recorded.ReadDir(recordedDir)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, strings.TrimSuffix(e.Name(), ".http"))
	}
	sort.Strings(names)
	return names
}

// RecordedResponse returns the named recorded response, with its times moved
// to now: the Date header becomes now, and an X-RateLimit-Reset epoch or a
// Retry-After HTTP-date moves by the same amount. A recording's own deadlines
// are long past, and a past deadline is no deadline.
func RecordedResponse(name string, now time.Time) (*http.Response, error) {
	raw, err := recorded.ReadFile(recordedDir + "/" + name + ".http")
	if err != nil {
		return nil, errors.Wrapf(err, "no recorded response %q", name)
	}
	resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(raw)), nil)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot parse recorded response %q", name)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot read recorded response %q", name)
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))

	recordedAt, err := http.ParseTime(resp.Header.Get(headerDate))
	if err != nil {
		return nil, errors.Wrapf(err, "recorded response %q has no valid Date header", name)
	}
	shift := now.Sub(recordedAt)
	resp.Header.Set(headerDate, now.UTC().Format(http.TimeFormat))
	if v := resp.Header.Get(headerRateLimitReset); v != "" {
		if epoch, err := strconv.ParseInt(v, 10, 64); err == nil {
			resp.Header.Set(headerRateLimitReset, strconv.FormatInt(time.Unix(epoch, 0).Add(shift).Unix(), 10))
		}
	}
	if v := resp.Header.Get(headerRetryAfter); v != "" {
		if at, err := http.ParseTime(v); err == nil {
			resp.Header.Set(headerRetryAfter, at.Add(shift).UTC().Format(http.TimeFormat))
		}
	}
	return resp, nil
}
