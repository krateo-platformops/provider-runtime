package meta

import (
	"testing"

	"github.com/krateo-platformops/plumbing/tracecontext"
)

// The three trace keys are the bytes plumbing's tracecontext reads and writes,
// so a composition (unstructured-runtime) and its provider's managed resource
// (provider-runtime) share one contract. pkg/meta keeps its own constants, so
// it doesn't import OpenTelemetry; this test keeps them equal.
func TestTraceKeysAreTracecontexts(t *testing.T) {
	for name, tc := range map[string]struct{ got, want string }{
		"Traceparent":          {AnnotationKeyTraceparent, tracecontext.AnnotationKeyTraceparent},
		"Tracestate":           {AnnotationKeyTracestate, tracecontext.AnnotationKeyTracestate},
		"TraceparentContinued": {AnnotationKeyTraceparentContinued, tracecontext.AnnotationKeyTraceparentContinued},
	} {
		if tc.got != tc.want {
			t.Errorf("AnnotationKey%s = %q, tracecontext's is %q", name, tc.got, tc.want)
		}
	}
}
