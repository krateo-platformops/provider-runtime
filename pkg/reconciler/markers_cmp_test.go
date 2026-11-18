package reconciler

import (
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/krateo-platformops/provider-runtime/pkg/meta"
)

// equateMarkerTimes compares the external-create markers as times rather than
// strings. The markers are written with nanosecond precision (#88), so an
// expected object built with its own time.Now() differs from the reconciler's
// in the string, though not in any way the reconciler reads. Combined with
// cmpopts.EquateApproxTime, the markers match within its tolerance; every other
// annotation still compares as a string, and a marker that fails to parse is
// compared as its raw string, so a malformed value still shows as a diff.
func equateMarkerTimes() cmp.Option {
	markers := map[string]bool{
		meta.AnnotationKeyExternalCreatePending:   true,
		meta.AnnotationKeyExternalCreateSucceeded: true,
		meta.AnnotationKeyExternalCreateFailed:    true,
	}
	return cmp.Transformer("markerTimes", func(in map[string]string) map[string]any {
		if in == nil {
			return nil
		}
		out := make(map[string]any, len(in))
		for k, v := range in {
			if markers[k] {
				if t, err := time.Parse(meta.MarkerTimeLayout, v); err == nil {
					out[k] = t
					continue
				}
			}
			out[k] = v
		}
		return out
	})
}
