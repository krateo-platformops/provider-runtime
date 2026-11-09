package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel/trace"
)

// Every Logger this package returns is a ContextLogger.
var (
	_ ContextLogger = nopLogger{}
	_ ContextLogger = logrLogger{}
	_ ContextLogger = slogLogger{}
)

const (
	testTraceID = "0102030405060708090a0b0c0d0e0f10"
	testSpanID  = "0102030405060708"
)

// spanCtx returns a context holding a valid, sampled span context.
func spanCtx(t *testing.T) context.Context {
	t.Helper()
	tid, err := trace.TraceIDFromHex(testTraceID)
	if err != nil {
		t.Fatal(err)
	}
	sid, err := trace.SpanIDFromHex(testSpanID)
	if err != nil {
		t.Fatal(err)
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled})
	if !sc.IsValid() {
		t.Fatal("span context is not valid")
	}
	return trace.ContextWithSpanContext(context.Background(), sc)
}

// otelSlog returns a slog-backed Logger over NewOTelJSONHandler, writing to buf.
func otelSlog(buf *bytes.Buffer) Logger {
	return NewSlogLogger(*slog.New(NewOTelJSONHandler(slog.LevelDebug, buf)))
}

// records decodes each JSON line in buf.
func records(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("not a JSON line: %v\n%s", err, line)
		}
		out = append(out, m)
	}
	return out
}

// logAll logs one record through each of l's methods.
func logAll(l Logger) {
	l.Debug("debug")
	l.Info("info")
	l.Warn("warn")
	l.Error(errors.New("boom"), "error")
}

func wantTraced(t *testing.T, rec map[string]any) {
	t.Helper()
	if rec["trace_id"] != testTraceID || rec["span_id"] != testSpanID {
		t.Errorf("%v: want trace_id %s and span_id %s, got %v and %v",
			rec["msg"], testTraceID, testSpanID, rec["trace_id"], rec["span_id"])
	}
}

func wantUntraced(t *testing.T, rec map[string]any) {
	t.Helper()
	if _, ok := rec["trace_id"]; ok {
		t.Errorf("%v: want no trace_id, got %v", rec["msg"], rec["trace_id"])
	}
	if _, ok := rec["span_id"]; ok {
		t.Errorf("%v: want no span_id, got %v", rec["msg"], rec["span_id"])
	}
}

// A record logged through WithContext, with a span in the context, carries its trace and span
// ids through NewOTelJSONHandler on every level. One logged without a span does not.
func TestWithContextSlogStampsTraceIDs(t *testing.T) {
	cases := map[string]struct {
		logger func(Logger) Logger
		traced bool
	}{
		"BoundToSpan":       {logger: func(l Logger) Logger { return WithContext(l, spanCtx(t)) }, traced: true},
		"Unbound":           {logger: func(l Logger) Logger { return l }},
		"BoundWithoutSpan":  {logger: func(l Logger) Logger { return WithContext(l, context.Background()) }},
		"RebindDropsSpan":   {logger: func(l Logger) Logger { return WithContext(WithContext(l, spanCtx(t)), context.Background()) }},
		"RebindAddsSpan":    {logger: func(l Logger) Logger { return WithContext(WithContext(l, context.Background()), spanCtx(t)) }, traced: true},
		"BoundAfterValues":  {logger: func(l Logger) Logger { return WithContext(l.WithValues("k", "v"), spanCtx(t)) }, traced: true},
		"BoundBeforeValues": {logger: func(l Logger) Logger { return WithContext(l, spanCtx(t)).WithValues("k", "v") }, traced: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			logAll(tc.logger(otelSlog(&buf)))

			recs := records(t, &buf)
			if len(recs) != 4 {
				t.Fatalf("want 4 records (debug, info, warn, error), got %d:\n%s", len(recs), buf.String())
			}
			for _, rec := range recs {
				if tc.traced {
					wantTraced(t, rec)
				} else {
					wantUntraced(t, rec)
				}
			}
		})
	}
}

// WithName and WithValues on a context-bound Logger keep the context, and still add their name
// and values.
func TestWithContextSlogSurvivesWithNameAndWithValues(t *testing.T) {
	var buf bytes.Buffer
	l := WithContext(otelSlog(&buf), spanCtx(t)).WithName("component").WithValues("k", "v")
	l.Info("hello")
	l.WithValues("k2", "v2").WithName("sub").Error(errors.New("boom"), "failed")

	recs := records(t, &buf)
	if len(recs) != 2 {
		t.Fatalf("want 2 records, got %d:\n%s", len(recs), buf.String())
	}
	for _, rec := range recs {
		wantTraced(t, rec)
		if rec["k"] != "v" {
			t.Errorf("%v: want k=v, got %v", rec["msg"], rec["k"])
		}
	}
	if recs[0]["logger"] != "component" {
		t.Errorf("want logger=component, got %v", recs[0]["logger"])
	}
	if recs[1]["k2"] != "v2" || recs[1]["err"] != "boom" {
		t.Errorf("want k2=v2 and err=boom, got %v", recs[1])
	}
}

// plainLogger is a Logger written without ContextLogger, as a provider's own Logger may be.
type plainLogger struct{ infos *int }

func (l *plainLogger) Info(string, ...any)         { *l.infos++ }
func (l *plainLogger) Debug(string, ...any)        {}
func (l *plainLogger) Error(error, string, ...any) {}
func (l *plainLogger) Warn(string, ...any)         {}
func (l *plainLogger) WithValues(...any) Logger    { return l }
func (l *plainLogger) WithName(string) Logger      { return l }

// The helper returns a Logger that is not a ContextLogger unchanged, so it keeps working.
func TestWithContextReturnsNonContextLoggerUnchanged(t *testing.T) {
	infos := 0
	var l Logger = &plainLogger{infos: &infos}
	if _, ok := l.(ContextLogger); ok {
		t.Fatal("plainLogger must not be a ContextLogger for this test")
	}

	got := WithContext(l, spanCtx(t))
	if got != l {
		t.Errorf("WithContext(non-ContextLogger, ctx): want the same Logger back, got %#v", got)
	}
	got.Info("still logs")
	if infos != 1 {
		t.Errorf("want the Logger to keep logging, got %d Info calls", infos)
	}
}

// The nop Logger is a ContextLogger whose bound Logger is still a nop, through WithName and
// WithValues too.
func TestWithContextNop(t *testing.T) {
	l := WithContext(NewNopLogger(), spanCtx(t))
	if _, ok := l.(nopLogger); !ok {
		t.Fatalf("want a nopLogger, got %T", l)
	}
	logAll(l)
	if _, ok := l.WithName("n").WithValues("k", "v").(nopLogger); !ok {
		t.Errorf("want WithName and WithValues of a bound nop Logger to stay a nopLogger")
	}
}

// A logr Logger has no path that carries a context to its sink, so WithContext returns it
// unchanged. Even over a slog handler that stamps trace ids, its records carry none: logr's
// slog adapter calls the handler with context.Background().
func TestWithContextLogrCannotCarryContext(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogrLogger(logr.FromSlogHandler(NewOTelJSONHandler(slog.LevelDebug, &buf)))

	got := WithContext(l, spanCtx(t))
	if got != l {
		t.Errorf("WithContext(logr Logger, ctx): want the same Logger back, got %#v", got)
	}
	got.Info("info")
	got.Error(errors.New("boom"), "error")

	recs := records(t, &buf)
	if len(recs) != 2 {
		t.Fatalf("want 2 records, got %d:\n%s", len(recs), buf.String())
	}
	for _, rec := range recs {
		wantUntraced(t, rec)
	}
}
