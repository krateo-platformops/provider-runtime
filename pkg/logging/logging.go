package logging

import (
	"context"
	"log/slog"

	"github.com/go-logr/logr"
)

// A Logger logs messages. Messages may be supplemented by structured data.
type Logger interface {
	// Info logs a message with optional structured data. Structured data must
	// be supplied as an array that alternates between string keys and values of
	// an arbitrary type. Use Info for messages that operators are
	// very likely to be concerned with when running.
	Info(msg string, keysAndValues ...any)

	// Debug logs a message with optional structured data. Structured data must
	// be supplied as an array that alternates between string keys and values of
	// an arbitrary type. Use Debug for messages that operators or
	// developers may be concerned with when debugging.
	Debug(msg string, keysAndValues ...any)

	// Error logs an error with a message and optional structured data.
	// Structured data must be supplied as an array that alternates between
	// string keys and values of an arbitrary type. Use Error for messages
	Error(err error, msg string, keysAndValues ...any)

	// Warn logs a message with optional structured data. Structured data must
	// be supplied as an array that alternates between string keys and values of
	// an arbitrary type. Use Warn for messages that operators should be aware of.
	Warn(msg string, keysAndValues ...any)

	// WithValues returns a Logger that will include the supplied structured
	// data with any subsequent messages it logs. Structured data must
	// be supplied as an array that alternates between string keys and values of
	// an arbitrary type.
	WithValues(keysAndValues ...any) Logger

	// WithName returns a Logger that will include the supplied name with any
	// subsequent messages it logs. The name is typically a component name or
	// similar, and should be used to distinguish between different components
	// or subsystems.
	WithName(name string) Logger
}

// A ContextLogger is a Logger that can bind a context.Context, so that every
// record it logs carries that context to the backend. A backend that reads the
// record's context can act on what it holds: NewOTelJSONHandler, and the
// otelslog bridge behind NewOTelHandler, stamp the trace and span ids of the
// span in it, which correlates the log with the trace.
//
// It is optional, so a Logger written before it existed keeps compiling and
// working. Call it through WithContext, which handles a Logger that does not
// implement it.
type ContextLogger interface {
	// WithContext returns a Logger that passes ctx with every record it logs.
	// The Loggers that its WithName and WithValues return keep ctx.
	WithContext(ctx context.Context) Logger
}

// WithContext returns l bound to ctx when l is a ContextLogger, and l unchanged
// when it is not. A ctx that holds no span stamps nothing.
func WithContext(l Logger, ctx context.Context) Logger {
	if c, ok := l.(ContextLogger); ok {
		return c.WithContext(ctx)
	}
	return l
}

// NewNopLogger returns a Logger that does nothing.
func NewNopLogger() Logger { return nopLogger{} }

type nopLogger struct{}

func (l nopLogger) Info(msg string, keysAndValues ...any)             {}
func (l nopLogger) Debug(msg string, keysAndValues ...any)            {}
func (l nopLogger) Error(err error, msg string, keysAndValues ...any) {}
func (l nopLogger) Warn(msg string, keysAndValues ...any)             {}
func (l nopLogger) WithName(name string) Logger                       { return nopLogger{} }
func (l nopLogger) WithValues(keysAndValues ...any) Logger            { return nopLogger{} }
func (l nopLogger) WithContext(ctx context.Context) Logger            { return nopLogger{} }

// NewLogrLogger returns a Logger that is satisfied by the supplied logr.Logger,
// which may be satisfied in turn by various logging implementations (Zap, klog,
// etc). Debug messages are logged at V(1).
//
// Its records carry no context, so they never carry trace or span ids: see
// logrLogger.WithContext.
func NewLogrLogger(l logr.Logger) Logger {
	return logrLogger{log: l}
}

type logrLogger struct {
	log logr.Logger
}

func (l logrLogger) Info(msg string, keysAndValues ...any) {
	l.log.Info(msg, keysAndValues...) //nolint:logrlint // False positive - logrlint thinks there's an odd number of args.
}

func (l logrLogger) Debug(msg string, keysAndValues ...any) {
	l.log.V(1).Info(msg, keysAndValues...) //nolint:logrlint // False positive - logrlint thinks there's an odd number of args.
}

func (l logrLogger) Error(err error, msg string, keysAndValues ...any) {
	l.log.Error(err, msg, keysAndValues...) //nolint:logrlint // False positive - logrlint thinks there's an odd number of args.
}

func (l logrLogger) Warn(msg string, keysAndValues ...any) {
	l.log.V(0).Info(msg, keysAndValues...) //nolint:logrlint // False positive - logrlint thinks there's an odd number of args.
}

func (l logrLogger) WithName(name string) Logger {
	return logrLogger{log: l.log.WithName(name)}
}

func (l logrLogger) WithValues(keysAndValues ...any) Logger {
	return logrLogger{log: l.log.WithValues(keysAndValues...)} //nolint:logrlint // False positive - logrlint thinks there's an odd number of args.
}

// WithContext returns l unchanged, because logr has no path that carries a
// context to its sink. logr.LogSink's Info and Error take no context, and
// logr's own slog adapter (logr.FromSlogHandler) calls the slog.Handler with
// context.Background(). logr.NewContext goes the other way: it stores a Logger
// in a context and gives the sink nothing. So a record logged through a logr
// Logger never carries the trace or span id of a span in a context. For
// log-to-trace correlation, build the Logger with NewSlogLogger.
func (l logrLogger) WithContext(_ context.Context) Logger { return l }

// slogLogger logs through a *slog.Logger. ctx is the context bound by
// WithContext and passed with every record. It is nil when none is bound, and
// slog treats a nil context as context.Background().
type slogLogger struct {
	log *slog.Logger
	ctx context.Context
}

// NewSlogLogger returns a Logger that logs through l. It is a ContextLogger:
// after WithContext, every record carries the bound context to l's handler.
func NewSlogLogger(l slog.Logger) Logger {
	return slogLogger{log: &l}
}

func (l slogLogger) Info(msg string, keysAndValues ...any) {
	l.log.InfoContext(l.ctx, msg, keysAndValues...)
}
func (l slogLogger) Debug(msg string, keysAndValues ...any) {
	l.log.DebugContext(l.ctx, msg, keysAndValues...)
}
func (l slogLogger) Warn(msg string, keysAndValues ...any) {
	l.log.WarnContext(l.ctx, msg, keysAndValues...)
}
func (l slogLogger) Error(err error, msg string, keysAndValues ...any) {
	l.log.ErrorContext(l.ctx, msg, append(keysAndValues, slog.Any("err", err))...)
}
func (l slogLogger) WithName(name string) Logger {
	return slogLogger{log: l.log.With(slog.String("logger", name)), ctx: l.ctx}
}
func (l slogLogger) WithValues(keysAndValues ...any) Logger {
	return slogLogger{log: l.log.With(keysAndValues...), ctx: l.ctx}
}
func (l slogLogger) WithContext(ctx context.Context) Logger {
	return slogLogger{log: l.log, ctx: ctx}
}
