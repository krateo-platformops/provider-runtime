package tracing

import (
	"sync/atomic"

	"go.opentelemetry.io/otel/trace"
)

// ScopeName is the instrumentation scope of every span provider-runtime starts.
// The meter in pkg/telemetry uses the same name.
const ScopeName = "github.com/krateo-platformops/provider-runtime"

// providerBox wraps the provider so an atomic.Pointer can hold an interface.
type providerBox struct{ tp trace.TracerProvider }

var current atomic.Pointer[providerBox]

// SetTracerProvider sets the provider the reconcilers use when they're given
// none of their own. telemetry.Setup calls it when tracing is on. A provider
// that builds its own SDK TracerProvider calls it too, so the reconciler's
// spans go through that pipeline. nil turns tracing off.
func SetTracerProvider(tp trace.TracerProvider) {
	if tp == nil {
		current.Store(nil)
		return
	}
	current.Store(&providerBox{tp: tp})
}

// TracerProvider returns the provider SetTracerProvider set, or nil when
// tracing is off. A reconciler reads it on every pass, with one atomic load.
func TracerProvider() trace.TracerProvider {
	if b := current.Load(); b != nil {
		return b.tp
	}
	return nil
}
