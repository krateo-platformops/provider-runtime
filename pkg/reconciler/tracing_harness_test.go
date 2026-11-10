package reconciler

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
	"github.com/krateo-platformops/provider-runtime/pkg/resource/fake"
	"github.com/krateo-platformops/provider-runtime/pkg/tracing"
)

// The tracing tests reconcile a Widget, a managed resource with real
// TypeMeta/ObjectMeta JSON, a status subresource, and an Incident-like state
// machine: a check runs only once the widget is Open with a script.
var widgetGVK = schema.GroupVersionKind{Group: "trace.test.krateo.io", Version: "v1", Kind: "Widget"}

type Widget struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              WidgetSpec   `json:"spec,omitempty"`
	Status            WidgetStatus `json:"status,omitempty"`
}

type WidgetSpec struct {
	Wanted string `json:"wanted,omitempty"`
}

type WidgetStatus struct {
	prv1.ConditionedStatus `json:",inline"`
	State                  string `json:"state,omitempty"`
	Script                 string `json:"script,omitempty"`
	Initialized            bool   `json:"initialized,omitempty"`
	CheckStarted           bool   `json:"checkStarted,omitempty"`
	Observed               string `json:"observed,omitempty"`
}

func (w *Widget) DeepCopyObject() runtime.Object {
	out := &Widget{}
	b, err := json.Marshal(w)
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(b, out); err != nil {
		panic(err)
	}
	return out
}

func (w *Widget) GetCondition(ct prv1.ConditionType) prv1.Condition {
	return w.Status.GetCondition(ct)
}

func (w *Widget) SetConditions(c ...prv1.Condition) { w.Status.SetConditions(c...) }

type WidgetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Widget `json:"items"`
}

func (l *WidgetList) DeepCopyObject() runtime.Object {
	out := &WidgetList{}
	b, _ := json.Marshal(l)
	_ = json.Unmarshal(b, out)
	return out
}

func widgetScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	s.AddKnownTypes(widgetGVK.GroupVersion(), &Widget{}, &WidgetList{})
	metav1.AddToGroupVersion(s, widgetGVK.GroupVersion())
	return s
}

// The W3C traceparents the tracing tests use. B shares A's trace with another
// span; C is another trace.
const (
	tpA = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	tpB = "00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01"
	tpC = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
)

// apiOps is an ordered log of the writes the API server got and the provider
// calls the reconciler made, shared by the fake API server and the provider.
type apiOps struct {
	mu  sync.Mutex
	ops []string
}

func (o *apiOps) add(op string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ops = append(o.ops, op)
}

func (o *apiOps) list() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.ops...)
}

func (o *apiOps) reset() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ops = nil
}

func (o *apiOps) count(op string) int {
	n := 0
	for _, x := range o.list() {
		if x == op {
			n++
		}
	}
	return n
}

// index is the position of op in the log, or -1.
func (o *apiOps) index(op string) int {
	for i, x := range o.list() {
		if x == op {
			return i
		}
	}
	return -1
}

// Fault hooks: each, when set, can fail a write before it reaches the store.
type faults struct {
	mu     sync.Mutex
	patch  func(obj client.Object) error
	update func(obj client.Object) error
	get    func(key client.ObjectKey, obj client.Object) (handled bool, err error)
}

// traceServer is a fake API server: controller-runtime's fake client, which
// enforces the resourceVersion precondition and the status subresource, with
// an interceptor that logs writes and injects faults.
type traceServer struct {
	t      *testing.T
	scheme *runtime.Scheme
	raw    client.WithWatch
	c      client.Client
	ops    *apiOps
	faults faults
}

func newTraceServer(t *testing.T, objs ...client.Object) *traceServer {
	t.Helper()
	s := &traceServer{t: t, scheme: widgetScheme(), ops: &apiOps{}}
	s.raw = clientfake.NewClientBuilder().WithScheme(s.scheme).WithStatusSubresource(&Widget{}).WithObjects(objs...).Build()
	s.c = interceptor.NewClient(s.raw, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			s.faults.mu.Lock()
			fn := s.faults.get
			s.faults.mu.Unlock()
			if fn != nil {
				if handled, err := fn(key, obj); handled {
					return err
				}
			}
			return c.Get(ctx, key, obj, opts...)
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			s.faults.mu.Lock()
			fn := s.faults.patch
			s.faults.mu.Unlock()
			if fn != nil {
				if err := fn(obj); err != nil {
					s.ops.add("patch-rejected")
					return err
				}
			}
			if err := c.Patch(ctx, obj, patch, opts...); err != nil {
				s.ops.add("patch-failed")
				return err
			}
			s.ops.add("patch")
			return nil
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			s.faults.mu.Lock()
			fn := s.faults.update
			s.faults.mu.Unlock()
			if fn != nil {
				if err := fn(obj); err != nil {
					s.ops.add("update-rejected")
					return err
				}
			}
			if err := c.Update(ctx, obj, opts...); err != nil {
				s.ops.add("update-failed")
				return err
			}
			s.ops.add("update")
			return nil
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if err := c.SubResource(sub).Update(ctx, obj, opts...); err != nil {
				s.ops.add("status-failed")
				return err
			}
			s.ops.add("status")
			return nil
		},
	})
	return s
}

// widget reads the stored widget.
func (s *traceServer) widget(name string) *Widget {
	s.t.Helper()
	w := &Widget{}
	if err := s.raw.Get(context.Background(), types.NamespacedName{Namespace: "demo", Name: name}, w); err != nil {
		s.t.Fatalf("get widget %s: %v", name, err)
	}
	return w
}

// annotate writes annotations onto the stored widget, as another writer would.
func (s *traceServer) annotate(name string, kv map[string]string) {
	s.t.Helper()
	w := s.widget(name)
	meta.AddAnnotations(w, kv)
	if err := s.raw.Update(context.Background(), w); err != nil {
		s.t.Fatalf("annotate: %v", err)
	}
}

// setStatus changes the stored widget's status, as another writer would.
func (s *traceServer) setStatus(name string, fn func(*WidgetStatus)) {
	s.t.Helper()
	w := s.widget(name)
	fn(&w.Status)
	if err := s.raw.Status().Update(context.Background(), w); err != nil {
		s.t.Fatalf("set status: %v", err)
	}
}

// newWidget is a widget named name in namespace demo with the annotations kv.
func newWidget(name string, kv map[string]string) *Widget {
	w := &Widget{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "demo", UID: types.UID("uid-" + name), Annotations: kv}}
	w.SetGroupVersionKind(widgetGVK)
	return w
}

// widgetProvider is a scripted ExternalClient over a Widget. Observe sets
// Ready and reports the widget out of date when it has work: a first pass
// that initializes it, an Open widget with a script whose check hasn't
// started, or a spec Wanted different from what was observed. Update does
// that work in memory; the reconciler's status update persists it.
type widgetProvider struct {
	ops *apiOps

	// exists says whether the external resource exists (default true).
	exists func(w *Widget) bool
	// observeErr, updateErr, createErr, deleteErr, connectErr fail the call.
	observeErr, updateErr, createErr, deleteErr, connectErr error
	// observeHook runs inside Observe, with its ctx, after the work is computed.
	observeHook func(ctx context.Context, w *Widget)
	// updateHook runs inside Update, with its ctx.
	updateHook func(ctx context.Context, w *Widget)
	// lateInit makes Observe report ResourceLateInitialized.
	lateInit bool
	// key, when set, is the rate-limit key of every call.
	key string
}

func (p *widgetProvider) Connect(ctx context.Context, mg resource.Managed) (ExternalClient, error) {
	p.ops.add("connect")
	if p.connectErr != nil {
		return nil, p.connectErr
	}
	ec := &ExternalClientFns{
		ObserveFn: p.observe,
		CreateFn: func(ctx context.Context, mg resource.Managed) error {
			p.ops.add("create")
			if p.createErr != nil {
				return p.createErr
			}
			meta.SetExternalName(mg, "ext-"+mg.GetName())
			return nil
		},
		UpdateFn: func(ctx context.Context, mg resource.Managed) error {
			p.ops.add("act-update")
			if p.updateHook != nil {
				p.updateHook(ctx, mg.(*Widget))
			}
			if p.updateErr != nil {
				return p.updateErr
			}
			w := mg.(*Widget)
			if !w.Status.Initialized {
				w.Status.Initialized = true
			}
			if w.Status.State == "Open" && w.Status.Script != "" {
				w.Status.CheckStarted = true
			}
			w.Status.Observed = w.Spec.Wanted
			return nil
		},
		DeleteFn: func(ctx context.Context, mg resource.Managed) error {
			p.ops.add("act-delete")
			return p.deleteErr
		},
	}
	if p.key != "" {
		return &keyedClient{ExternalClientFns: *ec, key: func(resource.Managed) (string, error) { return p.key, nil }}, nil
	}
	return ec, nil
}

func (p *widgetProvider) observe(ctx context.Context, mg resource.Managed) (ExternalObservation, error) {
	p.ops.add("observe")
	if p.observeErr != nil {
		return ExternalObservation{}, p.observeErr
	}
	w := mg.(*Widget)
	w.SetConditions(prv1.Available())
	if p.observeHook != nil {
		p.observeHook(ctx, w)
	}
	exists := true
	if p.exists != nil {
		exists = p.exists(w)
	}
	work := !w.Status.Initialized ||
		(w.Status.State == "Open" && w.Status.Script != "" && !w.Status.CheckStarted) ||
		w.Spec.Wanted != w.Status.Observed
	return ExternalObservation{ResourceExists: exists, ResourceUpToDate: !work, ResourceLateInitialized: p.lateInit}, nil
}

// tracer is an in-memory TracerProvider with a span recorder.
func tracer(t *testing.T) (*sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return tp, rec
}

// newTraceReconciler builds a Reconciler over s for widgets, with no-op
// finalizers unless the options set some.
func newTraceReconciler(s *traceServer, p *widgetProvider, o ...ReconcilerOption) *Reconciler {
	return NewReconciler(&fake.Manager{Client: s.c, Scheme: s.scheme}, resource.ManagedKind(widgetGVK), append([]ReconcilerOption{
		WithExternalConnecter(p),
		WithFinalizer(resource.FinalizerFns{
			AddFinalizerFn:    func(context.Context, resource.Object) error { return nil },
			RemoveFinalizerFn: func(context.Context, resource.Object) error { return nil },
		}),
	}, o...)...)
}

func reqFor(name string) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "demo", Name: name}}
}

// roots are the ended reconcile spans, oldest first.
func roots(rec *tracetest.SpanRecorder) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() == tracing.SpanReconcile {
			out = append(out, s)
		}
	}
	return out
}

func lastRoot(t *testing.T, rec *tracetest.SpanRecorder) sdktrace.ReadOnlySpan {
	t.Helper()
	r := roots(rec)
	if len(r) == 0 {
		t.Fatal("no reconcile span")
	}
	return r[len(r)-1]
}

// remote is the span context of a traceparent.
func remote(t *testing.T, tp string) trace.SpanContext {
	t.Helper()
	d := tracing.Decide(map[string]string{meta.AnnotationKeyTraceparent: tp}, "", time.Time{}, time.Now(), time.Minute)
	if !d.Remote.IsValid() {
		t.Fatalf("invalid traceparent %s", tp)
	}
	return d.Remote
}

// mode reads a root span's relation to the annotation: Parent of want, Link to
// want, or NoParent.
func mode(s sdktrace.ReadOnlySpan) (tracing.Mode, trace.SpanContext) {
	if s.Parent().IsValid() {
		return tracing.Parent, s.Parent()
	}
	if len(s.Links()) > 0 {
		return tracing.Link, s.Links()[0].SpanContext
	}
	return tracing.NoParent, trace.SpanContext{}
}

// wantMode checks a root span against D8: every reconcile is its own trace, so
// the root never has a parent. With want Parent or Link it carries exactly one
// link, to tp; with NoParent it carries none. (Parent and Link differ only in
// whether the pass's first act step joins tp's trace: see wantAct.)
func wantMode(t *testing.T, s sdktrace.ReadOnlySpan, want tracing.Mode, tp string) {
	t.Helper()
	if s.Parent().IsValid() {
		t.Fatalf("root has parent %s/%s: under D8 a reconcile root is never a child", s.Parent().TraceID(), s.Parent().SpanID())
	}
	links := s.Links()
	if want == tracing.NoParent {
		if len(links) != 0 {
			t.Fatalf("root links = %v, want none", links)
		}
		return
	}
	r := remote(t, tp)
	if len(links) != 1 || links[0].SpanContext.TraceID() != r.TraceID() || links[0].SpanContext.SpanID() != r.SpanID() {
		t.Fatalf("root links = %v, want one to %s", links, tp)
	}
	if s.SpanContext().TraceID() == r.TraceID() {
		t.Errorf("root is in the upstream trace %s: it must be its own trace", r.TraceID())
	}
}

// actSpans are the ended spans named name (create, update or delete), oldest
// first.
func actSpans(rec interface {
	Ended() []sdktrace.ReadOnlySpan
}, name string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, sp := range rec.Ended() {
		if sp.Name() == name {
			out = append(out, sp)
		}
	}
	return out
}

// wantAct checks an act span against D8. With tp set, it is the first act for
// a new value: its parent is tp's span, in tp's trace, and it links back to
// root. With tp empty, it is an ordinary child of root, in root's trace.
func wantAct(t *testing.T, act, root sdktrace.ReadOnlySpan, tp string) {
	t.Helper()
	if tp == "" {
		if act.Parent().SpanID() != root.SpanContext().SpanID() || act.SpanContext().TraceID() != root.SpanContext().TraceID() {
			t.Errorf("%s: parent %s in trace %s, want a child of its root %s", act.Name(), act.Parent().SpanID(), act.SpanContext().TraceID(), root.SpanContext().SpanID())
		}
		return
	}
	r := remote(t, tp)
	if act.Parent().TraceID() != r.TraceID() || act.Parent().SpanID() != r.SpanID() || act.SpanContext().TraceID() != r.TraceID() {
		t.Errorf("%s: parent %s/%s, want the upstream %s", act.Name(), act.Parent().TraceID(), act.Parent().SpanID(), tp)
	}
	links := act.Links()
	if len(links) != 1 || links[0].SpanContext.SpanID() != root.SpanContext().SpanID() {
		t.Errorf("%s: links %v, want one to its reconcile root %s", act.Name(), links, root.SpanContext().SpanID())
	}
}

// spansIn counts the ended spans in trace tp's trace.
func spansIn(t *testing.T, rec interface {
	Ended() []sdktrace.ReadOnlySpan
}, tp string) int {
	t.Helper()
	r := remote(t, tp)
	n := 0
	for _, sp := range rec.Ended() {
		if sp.SpanContext().TraceID() == r.TraceID() {
			n++
		}
	}
	return n
}

// events are a span's event names.
func events(s sdktrace.ReadOnlySpan) []string {
	var out []string
	for _, e := range s.Events() {
		out = append(out, e.Name)
	}
	return out
}

func eventAttr(s sdktrace.ReadOnlySpan, name, key string) (string, bool) {
	for _, e := range s.Events() {
		if e.Name != name {
			continue
		}
		for _, kv := range e.Attributes {
			if string(kv.Key) == key {
				return kv.Value.String(), true
			}
		}
	}
	return "", false
}

// backdate moves the memo's first sight of name's value back by d, as if the
// value had been seen d ago.
func backdate(r *Reconciler, name string, d time.Duration) {
	r.traces.mu.Lock()
	defer r.traces.mu.Unlock()
	key := types.NamespacedName{Namespace: "demo", Name: name}
	if e, ok := r.traces.entries[key]; ok {
		e.firstSeen = e.firstSeen.Add(-d)
		r.traces.entries[key] = e
	}
}

func memoStateOf(r *Reconciler, name string) (memoState, bool) {
	r.traces.mu.Lock()
	defer r.traces.mu.Unlock()
	e, ok := r.traces.entries[types.NamespacedName{Namespace: "demo", Name: name}]
	return e.state, ok
}
