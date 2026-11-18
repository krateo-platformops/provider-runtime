package tracing

import (
	"context"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/codes"
)

// Truncate never exceeds MaxErrorText and never splits a rune.
func TestTruncate(t *testing.T) {
	short := "fine"
	if Truncate(short) != short {
		t.Errorf("Truncate(%q) changed it", short)
	}
	ascii := strings.Repeat("a", MaxErrorText+10)
	if got := Truncate(ascii); len(got) != MaxErrorText {
		t.Errorf("len = %d, want %d", len(got), MaxErrorText)
	}
	// "é" is two bytes: MaxErrorText-1 ASCII bytes then "é" puts a rune across
	// the boundary.
	across := strings.Repeat("a", MaxErrorText-1) + "é" + "tail"
	got := Truncate(across)
	if len(got) > MaxErrorText || !utf8.ValidString(got) {
		t.Errorf("Truncate across a rune: len %d, valid %v", len(got), utf8.ValidString(got))
	}
	if got != strings.Repeat("a", MaxErrorText-1) {
		t.Error("Truncate should cut before the split rune")
	}
}

type typedError struct{ msg string }

func (e *typedError) Error() string { return e.msg }

// RecordError adds the exception event and sets Error, both cut to
// MaxErrorText, with the error's type as the SDK names it.
func TestRecordError(t *testing.T) {
	tp, rec := recorder(t)
	_, span := StartReconcile(context.Background(), tp, obj, Decision{}, time.Now())
	long := strings.Repeat("x", MaxErrorText*2)
	RecordError(span, &typedError{msg: long})
	RecordError(span, nil)
	span.End()

	s := onlySpan(t, rec)
	if s.Status().Code != codes.Error || len(s.Status().Description) != MaxErrorText {
		t.Errorf("status = %v with a %d-byte description, want Error with %d", s.Status().Code, len(s.Status().Description), MaxErrorText)
	}
	events := s.Events()
	if len(events) != 1 || events[0].Name != "exception" {
		t.Fatalf("events = %v, want one exception", events)
	}
	attrs := map[string]string{}
	for _, kv := range events[0].Attributes {
		attrs[string(kv.Key)] = kv.Value.AsString()
	}
	if len(attrs["exception.message"]) != MaxErrorText {
		t.Errorf("exception.message is %d bytes, want %d", len(attrs["exception.message"]), MaxErrorText)
	}
	// The SDK's RecordError names a pointer type by its String form.
	if attrs["exception.type"] != "*tracing.typedError" {
		t.Errorf("exception.type = %q, want *tracing.typedError", attrs["exception.type"])
	}
}

// SetTracerProvider and TracerProvider round-trip, and nil turns tracing off.
func TestTracerProviderRegistry(t *testing.T) {
	t.Cleanup(func() { SetTracerProvider(nil) })
	if TracerProvider() != nil {
		t.Fatal("tracing must start off")
	}
	tp, _ := recorder(t)
	SetTracerProvider(tp)
	if TracerProvider() != tp {
		t.Error("TracerProvider() didn't return the provider set")
	}
	SetTracerProvider(nil)
	if TracerProvider() != nil {
		t.Error("SetTracerProvider(nil) didn't turn tracing off")
	}
}
