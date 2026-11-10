package tracing

import (
	"context"
	"net/http"
	"testing"
	"time"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
	corev1 "k8s.io/api/core/v1"

	"github.com/krateo-platformops/provider-runtime/pkg/meta"
)

// spanContext returns a context whose active span is the context in tp.
func spanContext(t *testing.T, traceparent, tracestate string) context.Context {
	t.Helper()
	sc := parse(traceparent, tracestate)
	if !sc.IsValid() {
		t.Fatalf("invalid traceparent %q", traceparent)
	}
	return trace.ContextWithSpanContext(context.Background(), sc)
}

// Inject overwrites both keys, removes a stale tracestate, and never writes the
// marker key (design §10, test 22).
func TestInject(t *testing.T) {
	cases := map[string]struct {
		ctx  context.Context
		in   map[string]string
		want map[string]string
		ok   bool
	}{
		"OverwritesBoth": {
			ctx:  spanContext(t, tpB, "vendor=b"),
			in:   ann(meta.AnnotationKeyTraceparent, tpA, meta.AnnotationKeyTracestate, "vendor=a", "other", "kept"),
			want: ann(meta.AnnotationKeyTraceparent, tpB, meta.AnnotationKeyTracestate, "vendor=b", "other", "kept"),
			ok:   true,
		},
		"RemovesAStaleTracestate": {
			ctx:  spanContext(t, tpB, ""),
			in:   ann(meta.AnnotationKeyTraceparent, tpA, meta.AnnotationKeyTracestate, "vendor=a"),
			want: ann(meta.AnnotationKeyTraceparent, tpB),
			ok:   true,
		},
		"NoAnnotationsYet": {
			ctx:  spanContext(t, tpA, ""),
			in:   nil,
			want: ann(meta.AnnotationKeyTraceparent, tpA),
			ok:   true,
		},
		"NoContextChangesNothing": {
			ctx:  context.Background(),
			in:   ann(meta.AnnotationKeyTraceparent, tpA, meta.AnnotationKeyTracestate, "vendor=a"),
			want: ann(meta.AnnotationKeyTraceparent, tpA, meta.AnnotationKeyTracestate, "vendor=a"),
			ok:   false,
		},
		"NeverWritesTheMarker": {
			ctx:  spanContext(t, tpB, ""),
			in:   ann(markerKey, tpA),
			want: ann(markerKey, tpA, meta.AnnotationKeyTraceparent, tpB),
			ok:   true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			o := &corev1.ConfigMap{}
			if tc.in != nil {
				o.SetAnnotations(copyMap(tc.in))
			}
			if got := Inject(tc.ctx, o); got != tc.ok {
				t.Errorf("Inject = %v, want %v", got, tc.ok)
			}
			if !equal(o.GetAnnotations(), tc.want) {
				t.Errorf("annotations = %v, want %v", o.GetAnnotations(), tc.want)
			}

			m := copyMap(tc.in)
			if m == nil {
				m = map[string]string{}
			}
			if got := InjectAnnotations(tc.ctx, m); got != tc.ok {
				t.Errorf("InjectAnnotations = %v, want %v", got, tc.ok)
			}
			if !equal(m, tc.want) {
				t.Errorf("InjectAnnotations map = %v, want %v", m, tc.want)
			}
		})
	}
}

// InjectAnnotations can't write into a nil map, and says so.
func TestInjectAnnotationsNilMap(t *testing.T) {
	if InjectAnnotations(spanContext(t, tpA, ""), nil) {
		t.Error("InjectAnnotations(nil) = true, want false")
	}
}

// InjectHeader uses only the active span: an inbound context alone sends
// nothing. It keeps an existing traceparent and never sends baggage
// (design §10, tests 11 and 23).
func TestInjectHeader(t *testing.T) {
	m, err := baggage.NewMember("session", "s1")
	if err != nil {
		t.Fatal(err)
	}
	b, err := baggage.New(m)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("ActiveSpan", func(t *testing.T) {
		h := http.Header{}
		ctx := baggage.ContextWithBaggage(spanContext(t, tpA, "vendor=a"), b)
		if !InjectHeader(ctx, h) {
			t.Fatal("InjectHeader = false with an active span")
		}
		if h.Get("traceparent") != tpA || h.Get("tracestate") != "vendor=a" {
			t.Errorf("headers = %v, want traceparent %s and tracestate vendor=a", h, tpA)
		}
		if h.Get("baggage") != "" {
			t.Errorf("baggage header = %q, want none", h.Get("baggage"))
		}
	})
	t.Run("InboundOnly", func(t *testing.T) {
		d := Decide(ann(meta.AnnotationKeyTraceparent, tpA), "", time.Time{}, time.Now(), time.Minute)
		ctx, _ := StartReconcile(context.Background(), nil, obj, d, time.Now())
		h := http.Header{}
		if InjectHeader(ctx, h) || len(h) != 0 {
			t.Errorf("InjectHeader with only an inbound context set %v, want nothing", h)
		}
	})
	t.Run("KeepsAnExistingTraceparent", func(t *testing.T) {
		h := http.Header{}
		h.Set("traceparent", tpC)
		if InjectHeader(spanContext(t, tpA, ""), h) || h.Get("traceparent") != tpC {
			t.Errorf("traceparent = %q, want the existing %s kept", h.Get("traceparent"), tpC)
		}
	})
	t.Run("NoSpan", func(t *testing.T) {
		h := http.Header{}
		if InjectHeader(context.Background(), h) || len(h) != 0 {
			t.Errorf("InjectHeader without a span set %v", h)
		}
	})
}

func copyMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func equal(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}
