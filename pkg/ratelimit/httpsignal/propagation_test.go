package httpsignal

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/tracing"
)

const (
	tpA = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	tpC = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
)

// headerServer records the trace headers of every request it gets.
type headerServer struct {
	*httptest.Server
	mu  sync.Mutex
	got []http.Header
}

func newHeaderServer(t *testing.T) *headerServer {
	t.Helper()
	s := &headerServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.got = append(s.got, r.Header.Clone())
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *headerServer) last(t *testing.T) http.Header {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.got) == 0 {
		t.Fatal("the server got no request")
	}
	return s.got[len(s.got)-1]
}

// activeSpan returns a context whose active span is tp, with tracestate ts.
func activeSpan(t *testing.T, tp, ts string) context.Context {
	t.Helper()
	carrier := propagation.MapCarrier{"traceparent": tp}
	if ts != "" {
		carrier["tracestate"] = ts
	}
	ctx := propagation.TraceContext{}.Extract(context.Background(), carrier)
	if !trace.SpanContextFromContext(ctx).IsValid() {
		t.Fatalf("invalid traceparent %s", tp)
	}
	return ctx
}

func roundTrip(t *testing.T, rt http.RoundTripper, ctx context.Context, url string, hdr http.Header) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header[k] = v
	}
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	_ = resp.Body.Close()
	return req
}

// docs/design-tracing.md §10, test 23.
func TestTransportPropagatesTheActiveSpanOnly(t *testing.T) {
	m, err := baggage.NewMember("session", "s1")
	if err != nil {
		t.Fatal(err)
	}
	bag, err := baggage.New(m)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("ActiveSpan", func(t *testing.T) {
		srv := newHeaderServer(t)
		ctx := baggage.ContextWithBaggage(activeSpan(t, tpA, "vendor=a"), bag)
		req := roundTrip(t, NewTransport(nil), ctx, srv.URL, nil)
		got := srv.last(t)
		if got.Get("traceparent") != tpA || got.Get("tracestate") != "vendor=a" {
			t.Errorf("headers = traceparent %q tracestate %q, want %s and vendor=a", got.Get("traceparent"), got.Get("tracestate"), tpA)
		}
		if got.Get("baggage") != "" {
			t.Errorf("baggage header = %q, want none", got.Get("baggage"))
		}
		if req.Header.Get("traceparent") != "" {
			t.Error("the caller's request was changed: RoundTrip must set the header on a clone")
		}
	})
	t.Run("NoSpan", func(t *testing.T) {
		srv := newHeaderServer(t)
		roundTrip(t, NewTransport(nil), context.Background(), srv.URL, nil)
		if got := srv.last(t).Get("traceparent"); got != "" {
			t.Errorf("traceparent = %q without a span, want none", got)
		}
	})
	t.Run("InboundContextOnly", func(t *testing.T) {
		// With tracing off the reconciler carries the annotation's context for
		// Inject only; it must not reach the wire.
		srv := newHeaderServer(t)
		d := tracing.Decide(map[string]string{meta.AnnotationKeyTraceparent: tpA}, "", time.Time{}, time.Now(), time.Minute)
		ctx, _ := tracing.StartReconcile(context.Background(), nil, tracing.Object{Kind: "Repo", Name: "r"}, d, time.Now())
		roundTrip(t, NewTransport(nil), ctx, srv.URL, nil)
		if got := srv.last(t).Get("traceparent"); got != "" {
			t.Errorf("traceparent = %q with only an inbound context, want none", got)
		}
	})
	t.Run("KeepsAnExistingTraceparent", func(t *testing.T) {
		srv := newHeaderServer(t)
		roundTrip(t, NewTransport(nil), activeSpan(t, tpA, ""), srv.URL, http.Header{"Traceparent": {tpC}})
		if got := srv.last(t).Get("traceparent"); got != tpC {
			t.Errorf("traceparent = %q, want the existing %s kept", got, tpC)
		}
	})
	t.Run("NoTracePropagation", func(t *testing.T) {
		srv := newHeaderServer(t)
		roundTrip(t, &Transport{NoTracePropagation: true}, activeSpan(t, tpA, ""), srv.URL, nil)
		if got := srv.last(t).Get("traceparent"); got != "" {
			t.Errorf("traceparent = %q with NoTracePropagation, want none", got)
		}
	})
	t.Run("LiteralTransport", func(t *testing.T) {
		// Propagation is in RoundTrip, so a struct literal gets it too.
		srv := newHeaderServer(t)
		roundTrip(t, &Transport{}, activeSpan(t, tpA, ""), srv.URL, nil)
		if got := srv.last(t).Get("traceparent"); got != tpA {
			t.Errorf("traceparent = %q through a Transport literal, want %s", got, tpA)
		}
	})
}
