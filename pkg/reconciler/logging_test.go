package reconciler

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/krateo-platformops/provider-runtime/pkg/logging"
	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
	"github.com/krateo-platformops/provider-runtime/pkg/resource/fake"
	"github.com/krateo-platformops/provider-runtime/pkg/test"
)

// The per-reconcile Logger is bound to the ctx Reconcile is given, so every record it logs, before
// and after the Logger gains the managed resource's values, carries the trace and span ids of the
// span in that ctx. Without a span it carries none.
func TestReconcileLoggerIsContextBound(t *testing.T) {
	tid, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	sid, _ := trace.SpanIDFromHex("0102030405060708")
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled})

	cases := map[string]struct {
		ctx    context.Context
		traced bool
	}{
		"SpanInContext": {ctx: trace.ContextWithSpanContext(context.Background(), sc), traced: true},
		"NoSpan":        {ctx: context.Background()},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			log := logging.NewSlogLogger(*slog.New(logging.NewOTelJSONHandler(slog.LevelDebug, &buf)))

			// A paused resource logs once with the request-scoped Logger ("Reconciling") and once
			// after it gains the resource's uid, version and external name.
			mc := &test.MockClient{
				MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
					obj.(*fake.Managed).SetAnnotations(map[string]string{meta.AnnotationKeyReconciliationPaused: "true"})
					return nil
				}),
				MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
			}
			r := NewReconciler(&fake.Manager{Client: mc, Scheme: fake.SchemeWith(&fake.Managed{})},
				resource.ManagedKind(fake.GVK(&fake.Managed{})),
				WithLogger(log),
			)

			if _, err := r.Reconcile(tc.ctx, reconcile.Request{}); err != nil {
				t.Fatalf("r.Reconcile(...): %v", err)
			}

			var msgs []string
			for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
				var rec map[string]any
				if err := json.Unmarshal([]byte(line), &rec); err != nil {
					t.Fatalf("not a JSON line: %v\n%s", err, line)
				}
				msgs = append(msgs, rec["msg"].(string))
				_, hasTrace := rec["trace_id"]
				_, hasSpan := rec["span_id"]
				switch {
				case tc.traced && (rec["trace_id"] != tid.String() || rec["span_id"] != sid.String()):
					t.Errorf("%q: want trace_id %s and span_id %s, got %v and %v",
						rec["msg"], tid, sid, rec["trace_id"], rec["span_id"])
				case !tc.traced && (hasTrace || hasSpan):
					t.Errorf("%q: want no trace_id or span_id without a span, got %v and %v",
						rec["msg"], rec["trace_id"], rec["span_id"])
				}
			}
			if len(msgs) != 2 || msgs[0] != "Reconciling" || !strings.HasPrefix(msgs[1], "Reconciliation is paused") {
				t.Errorf("want the Reconciling and the paused records, got %q", msgs)
			}
		})
	}
}
