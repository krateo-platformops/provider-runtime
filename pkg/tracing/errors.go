package tracing

import (
	"reflect"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// MaxErrorText bounds the status description and the exception.message
// attribute. The SDK bounds neither: SetStatus keeps the whole description, and
// event attributes are cut by count, not length. Error text often embeds an
// HTTP body (docs/design-tracing.md §7).
const MaxErrorText = 4096

// RecordError records err on span as an exception event and sets the span's
// status to Error, both with the error text cut to MaxErrorText bytes on a rune
// boundary. A nil err records nothing.
func RecordError(span trace.Span, err error) {
	if err == nil {
		return
	}
	msg := Truncate(err.Error())
	span.AddEvent("exception", trace.WithAttributes(
		attribute.String("exception.type", errorType(err)),
		attribute.String("exception.message", msg),
	))
	span.SetStatus(codes.Error, msg)
}

// Truncate cuts s to at most MaxErrorText bytes, without splitting a rune.
func Truncate(s string) string {
	if len(s) <= MaxErrorText {
		return s
	}
	cut := MaxErrorText
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// errorType names err's type the way the SDK's RecordError does.
func errorType(err error) string {
	t := reflect.TypeOf(err)
	if t.PkgPath() == "" && t.Name() == "" {
		return t.String()
	}
	return t.PkgPath() + "." + t.Name()
}
