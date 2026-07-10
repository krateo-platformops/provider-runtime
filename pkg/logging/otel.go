// OpenTelemetry-aligned logging for provider-runtime consumers.
//
// This is the single shared home for the OTel log model that core-provider (and every other
// provider-runtime consumer) previously duplicated: one JSON object per line in the OTel log model
// — `timestamp` RFC3339Nano UTC, the slog level (SeverityText) plus a sibling SeverityNumber, the
// `service`/`service.name` resource attributes, and trace_id/span_id whenever a span is in the
// record's context. NewOTelHandler additionally tees each record to the global OTel LoggerProvider
// (via the otelslog bridge), so when the binary installs an OTLP LoggerProvider (the SDK + exporter
// setup belongs in main, next to the trace/metric setup), logs become a first-class OTLP signal
// alongside traces and metrics — while the stderr JSON stream keeps working for log scrapers.
//
// This package deliberately depends only on the OTel log API + the otelslog bridge (not the log
// SDK), so consumers that never export via OTLP pull no SDK.
package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	logglobal "go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/trace"
)

// otelSeverityNumber maps an slog.Level to the OpenTelemetry SeverityNumber
// (TRACE=1, DEBUG=5, INFO=9, WARN=13, ERROR=17).
func otelSeverityNumber(l slog.Level) int {
	switch {
	case l < slog.LevelDebug:
		return 1
	case l < slog.LevelInfo:
		return 5
	case l < slog.LevelWarn:
		return 9
	case l < slog.LevelError:
		return 13
	default:
		return 17
	}
}

// NewOTelJSONHandler returns a slog.Handler that writes one JSON object per line to w (defaulting to
// os.Stderr when nil) in the OTel log model: `timestamp` RFC3339Nano UTC, the level (SeverityText)
// plus a sibling SeverityNumber, the supplied persistent attrs (e.g. service.name), and
// trace_id/span_id whenever a span is present in the record's context.
func NewOTelJSONHandler(level slog.Leveler, w io.Writer, attrs ...slog.Attr) slog.Handler {
	if w == nil {
		w = os.Stderr
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level:     level,
		AddSource: false,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Only rewrite the top-level time field, not nested attributes sharing the key.
			if len(groups) == 0 && a.Key == slog.TimeKey {
				return slog.String("timestamp", a.Value.Time().UTC().Format(time.RFC3339Nano))
			}
			return a
		},
	})
	var base slog.Handler = h
	if len(attrs) > 0 {
		base = h.WithAttrs(attrs)
	}
	return &otelHandler{Handler: base}
}

// otelHandler enriches each record with SeverityNumber and, when a span is in context,
// trace_id/span_id — without polluting the Body.
type otelHandler struct {
	slog.Handler
}

func (o *otelHandler) Handle(ctx context.Context, rec slog.Record) error {
	rec.AddAttrs(slog.Int("SeverityNumber", otelSeverityNumber(rec.Level)))
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		rec.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return o.Handler.Handle(ctx, rec)
}

func (o *otelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &otelHandler{Handler: o.Handler.WithAttrs(attrs)}
}

func (o *otelHandler) WithGroup(name string) slog.Handler {
	return &otelHandler{Handler: o.Handler.WithGroup(name)}
}

// ServiceNameAttr is the persistent OTel resource attribute pair to pass to NewOTelHandler /
// NewOTelJSONHandler for a given service name.
func ServiceNameAttr(serviceName string) []slog.Attr {
	return []slog.Attr{
		slog.String("service.name", serviceName),
		// `service` kept alongside the OTel `service.name` during the log-scraper transition.
		slog.String("service", serviceName),
	}
}

// NewOTelHandler returns the recommended slog.Handler for a service: it tees each record to (a) the
// OTel-model JSON stream on w (stderr by default, for log scrapers) and (b) the OTel LoggerProvider
// via the otelslog bridge, so records are exported over OTLP once SetupOTLPLogs has installed an
// exporter. With no LoggerProvider installed the bridge is a no-op and only the JSON stream is
// written, so this is always safe to use.
func NewOTelHandler(level slog.Leveler, w io.Writer, serviceName string) slog.Handler {
	jsonH := NewOTelJSONHandler(level, w, ServiceNameAttr(serviceName)...)
	bridge := otelslog.NewHandler(serviceName, otelslog.WithLoggerProvider(logglobal.GetLoggerProvider()))
	return &teeHandler{handlers: []slog.Handler{jsonH, bridge}}
}

// teeHandler fans a record out to every sub-handler (JSON stream + OTLP bridge).
type teeHandler struct {
	handlers []slog.Handler
}

func (t *teeHandler) Enabled(ctx context.Context, l slog.Level) bool {
	for _, h := range t.handlers {
		if h.Enabled(ctx, l) {
			return true
		}
	}
	return false
}

func (t *teeHandler) Handle(ctx context.Context, rec slog.Record) error {
	var firstErr error
	for _, h := range t.handlers {
		if !h.Enabled(ctx, rec.Level) {
			continue
		}
		if err := h.Handle(ctx, rec.Clone()); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (t *teeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := make([]slog.Handler, len(t.handlers))
	for i, h := range t.handlers {
		next[i] = h.WithAttrs(attrs)
	}
	return &teeHandler{handlers: next}
}

func (t *teeHandler) WithGroup(name string) slog.Handler {
	next := make([]slog.Handler, len(t.handlers))
	for i, h := range t.handlers {
		next[i] = h.WithGroup(name)
	}
	return &teeHandler{handlers: next}
}

// The OTLP LoggerProvider (SDK + exporter) is installed by the BINARY next to its trace/metric
// setup: build the LoggerProvider with go.opentelemetry.io/otel/sdk/log + an otlplog exporter and
// register it with go.opentelemetry.io/otel/log/global.SetLoggerProvider, BEFORE constructing the
// log handler via NewOTelHandler (so the otelslog bridge captures the installed provider). This
// package intentionally does not import the log SDK, keeping non-exporting consumers SDK-free.
