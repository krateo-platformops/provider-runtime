package tracing

import (
	"testing"
	"time"

	"github.com/krateo-platformops/provider-runtime/pkg/meta"
)

// W3C traceparents for the tests. B shares A's trace ID with another span ID;
// AUnsampled is A unsampled, and C is another trace. The rest are invalid or
// non-canonical forms of A.
const (
	tpA          = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	tpAUnsampled = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00"
	tpB          = "00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01"
	tpC          = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	tpZeroTraceID     = "00-00000000000000000000000000000000-b7ad6b7169203331-01"
	tpLaterVersion    = "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-05-extra"
	tpReservedFlags   = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-05"
	tpUpperCaseHexOfA = "00-0AF7651916CD43DD8448EB211C80319C-B7AD6B7169203331-01"
)

func ann(kv ...string) map[string]string {
	m := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

// Each rule of docs/design-tracing.md §6.2, and the edges between them
// (design §10, tests 2, 3 and 4, the part that's pure).
func TestDecide(t *testing.T) {
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	const window = 10 * time.Minute

	cases := map[string]struct {
		annotations map[string]string
		remembered  string
		firstSeen   time.Time
		window      time.Duration
		mode        Mode
		write       When
		marker      string
	}{
		"Rule1NoAnnotation":       {annotations: nil, window: window, mode: NoParent, write: Never},
		"Rule1EmptyAnnotation":    {annotations: ann(meta.AnnotationKeyTraceparent, ""), window: window, mode: NoParent, write: Never},
		"Rule1InvalidTraceparent": {annotations: ann(meta.AnnotationKeyTraceparent, "not-a-traceparent"), window: window, mode: NoParent, write: Never},
		"Rule1AllZeroTraceID": {
			annotations: ann(meta.AnnotationKeyTraceparent, tpZeroTraceID),
			window:      window, mode: NoParent, write: Never,
		},
		"Rule2MarkerMatches": {
			annotations: ann(meta.AnnotationKeyTraceparent, tpA, meta.AnnotationKeyTraceparentContinued, tpA),
			window:      window, mode: Link, write: Never,
		},
		"Rule2RememberedMatches": {
			annotations: ann(meta.AnnotationKeyTraceparent, tpA),
			remembered:  tpA, window: window, mode: Link, write: Never,
		},
		"Rule2OnlyFlagsChanged": {
			annotations: ann(meta.AnnotationKeyTraceparent, tpAUnsampled, meta.AnnotationKeyTraceparentContinued, tpA),
			window:      window, mode: Link, write: Never,
		},
		"Rule2OnlyTracestateChanged": {
			annotations: ann(meta.AnnotationKeyTraceparent, tpA, meta.AnnotationKeyTracestate, "vendor=new", meta.AnnotationKeyTraceparentContinued, tpA),
			window:      window, mode: Link, write: Never,
		},
		"Rule2WindowZeroStillLinksWhenContinued": {
			annotations: ann(meta.AnnotationKeyTraceparent, tpA, meta.AnnotationKeyTraceparentContinued, tpA),
			window:      0, mode: Link, write: Never,
		},
		"Rule3WindowZero": {
			annotations: ann(meta.AnnotationKeyTraceparent, tpA),
			window:      0, mode: Parent, write: Now, marker: tpA,
		},
		"Rule4ZeroFirstSeenIsFirstSight": {
			annotations: ann(meta.AnnotationKeyTraceparent, tpA),
			window:      window, mode: Parent, write: BeforeFirstAction, marker: tpA,
		},
		"Rule4InsideTheWindow": {
			annotations: ann(meta.AnnotationKeyTraceparent, tpA),
			firstSeen:   now.Add(-window + time.Nanosecond), window: window,
			mode: Parent, write: BeforeFirstAction, marker: tpA,
		},
		"Rule4NewSpanIDIsNew": {
			annotations: ann(meta.AnnotationKeyTraceparent, tpB, meta.AnnotationKeyTraceparentContinued, tpA),
			window:      window, mode: Parent, write: BeforeFirstAction, marker: tpB,
		},
		"Rule4NewTraceIsNew": {
			annotations: ann(meta.AnnotationKeyTraceparent, tpC, meta.AnnotationKeyTraceparentContinued, tpA),
			remembered:  tpA, window: window, mode: Parent, write: BeforeFirstAction, marker: tpC,
		},
		"Rule4InvalidMarkerCountsAsAbsent": {
			annotations: ann(meta.AnnotationKeyTraceparent, tpA, meta.AnnotationKeyTraceparentContinued, "garbage"),
			window:      window, mode: Parent, write: BeforeFirstAction, marker: tpA,
		},
		// A later version's traceparent is canonicalised to version 00, with only
		// the sampled and random flags, as TraceContext.Inject writes it.
		"Rule4MarkerIsCanonicalised": {
			annotations: ann(meta.AnnotationKeyTraceparent, tpLaterVersion),
			window:      window, mode: Parent, write: BeforeFirstAction, marker: tpA,
		},
		// Version 00 forbids the reserved flag bits, so such a value is invalid.
		"Rule1ReservedFlagsInVersion00": {
			annotations: ann(meta.AnnotationKeyTraceparent, tpReservedFlags),
			window:      window, mode: NoParent, write: Never,
		},
		// W3C TraceContext allows lower-case hex only, and Go's propagator
		// enforces it, so an upper-case value is invalid.
		"Rule1UpperCaseIsInvalid": {
			annotations: ann(meta.AnnotationKeyTraceparent, tpUpperCaseHexOfA),
			window:      window, mode: NoParent, write: Never,
		},
		"Rule5TheWindowHasClosed": {
			annotations: ann(meta.AnnotationKeyTraceparent, tpA),
			firstSeen:   now.Add(-window), window: window,
			mode: Link, write: Now, marker: tpA,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			d := Decide(tc.annotations, tc.remembered, tc.firstSeen, now, tc.window)
			if d.Mode != tc.mode || d.WriteMarker != tc.write || d.Marker != tc.marker {
				t.Errorf("Decide = {Mode: %s, WriteMarker: %s, Marker: %q}, want {Mode: %s, WriteMarker: %s, Marker: %q}",
					d.Mode, d.WriteMarker, d.Marker, tc.mode, tc.write, tc.marker)
			}
			if (tc.mode == NoParent) == d.Remote.IsValid() {
				t.Errorf("Remote valid = %v in mode %s", d.Remote.IsValid(), d.Mode)
			}
		})
	}
}

// Decide keeps the annotation's tracestate in Remote, so the root's parent or
// link carries it.
func TestDecideKeepsTracestate(t *testing.T) {
	d := Decide(ann(meta.AnnotationKeyTraceparent, tpA, meta.AnnotationKeyTracestate, "vendor=a"), "", time.Time{}, time.Now(), time.Minute)
	if got := d.Remote.TraceState().String(); got != "vendor=a" {
		t.Errorf("Remote tracestate = %q, want vendor=a", got)
	}
}

// With no krateo.io/traceparent, Decide allocates nothing (design §3.4).
func TestDecideWithoutAnnotationAllocatesNothing(t *testing.T) {
	a := ann("other", "x")
	now := time.Now()
	if n := testing.AllocsPerRun(100, func() { _ = Decide(a, "", time.Time{}, now, time.Minute) }); n != 0 {
		t.Errorf("Decide without the annotation allocates %v times per call, want 0", n)
	}
}
