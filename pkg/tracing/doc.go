// Package tracing is the API-only half of provider-runtime's OpenTelemetry
// tracing (docs/design-tracing.md). It holds the provider the reconcilers use,
// the krateo.io/traceparent parent-or-link rule (Decide), the reconcile root
// span (StartReconcile), and the functions that carry a trace context onto a
// Kubernetes object (Inject, InjectAnnotations) or an HTTP request
// (InjectHeader).
//
// It depends on the OpenTelemetry API only, never on the SDK or an exporter,
// so the reconciler imports it without linking an OTLP exporter into every
// provider. telemetry.Setup builds the SDK pipeline and hands it here with
// SetTracerProvider.
//
// # Propagation rules
//
// Inject and InjectAnnotations write the context onto an object another Krateo
// controller reconciles, and only there. Callers must follow these rules, which
// docs/usage.md repeats:
//
//   - Only onto an object another Krateo controller reconciles: a composition,
//     or another provider's managed resource. Never onto a Pod, ConfigMap or
//     Secret, and never onto the managed resource being reconciled, whose
//     annotation is input.
//   - Only on a write the caller makes anyway: a create, or an update that
//     changes the object. A write made only to refresh the annotation makes
//     every downstream pass see a new value.
//   - Never in a status write: the status subresource drops metadata. When the
//     change that matters is in status, write the annotation in a separate
//     metadata write.
//   - Stamp before the change that triggers the work. When the context and the
//     change that makes the downstream controller act are two writes, write the
//     annotation first, so the downstream pass that acts is the child.
//   - Never copy krateo.io/traceparent-continued: it belongs to the object it is
//     written on.
//
// InjectHeader is for outbound HTTP calls. It reads only the active span, so
// with tracing off it sends nothing, and it never overwrites a traceparent the
// request already carries.
package tracing
