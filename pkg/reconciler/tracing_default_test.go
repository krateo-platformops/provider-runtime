package reconciler

import (
	"testing"

	"github.com/krateo-platformops/provider-runtime/pkg/tracing"
)

// A Reconciler built without WithTracerProvider reads tracing.TracerProvider()
// on every pass, not once at construction (docs/design-tracing.md §3.3: "a
// Setup called after the reconcilers were built still takes effect").
//
// Only a default that is already set at construction and later replaced tells
// the two apart. Starting from a nil default can't: a reconciler that reads
// the default once would get nil, fall back to the per-pass read, and pass.
func TestTracingDefaultProviderIsReadEveryPass(t *testing.T) {
	t.Cleanup(func() { tracing.SetTracerProvider(nil) })
	tpA, recA := tracer(t)
	tpB, recB := tracer(t)

	s := newTraceServer(t, initialized(newWidget("w", nil)))
	tracing.SetTracerProvider(tpA)
	r := newTraceReconciler(s, &widgetProvider{ops: s.ops})

	mustReconcile(t, r, "w")
	if got := len(roots(recA)); got != 1 {
		t.Fatalf("with A as the default at construction: A has %d reconcile spans, want 1", got)
	}

	tracing.SetTracerProvider(tpB)
	mustReconcile(t, r, "w")
	if got := len(roots(recA)); got != 1 {
		t.Errorf("after the default was replaced by B: A has %d reconcile spans, want still 1", got)
	}
	if got := len(roots(recB)); got != 1 {
		t.Errorf("after the default was replaced by B: B has %d reconcile spans, want 1", got)
	}

	tracing.SetTracerProvider(nil)
	mustReconcile(t, r, "w")
	if a, b := len(roots(recA)), len(roots(recB)); a != 1 || b != 1 {
		t.Errorf("after tracing was turned off: A has %d and B has %d reconcile spans, want 1 and 1", a, b)
	}
}

// WithTracerProvider wins over the default, including a default set or
// replaced after construction.
func TestTracingOptionOverridesTheDefault(t *testing.T) {
	t.Cleanup(func() { tracing.SetTracerProvider(nil) })
	tpOpt, recOpt := tracer(t)
	tpDef, recDef := tracer(t)

	s := newTraceServer(t, initialized(newWidget("w", nil)))
	tracing.SetTracerProvider(tpDef)
	r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithTracerProvider(tpOpt))
	mustReconcile(t, r, "w")

	tpDef2, recDef2 := tracer(t)
	tracing.SetTracerProvider(tpDef2)
	mustReconcile(t, r, "w")

	if got := len(roots(recOpt)); got != 2 {
		t.Errorf("the option's provider has %d reconcile spans, want 2", got)
	}
	if a, b := len(roots(recDef)), len(roots(recDef2)); a != 0 || b != 0 {
		t.Errorf("the defaults have %d and %d reconcile spans, want 0 and 0", a, b)
	}
}
