// Command greeting-controller is a minimal, compilable managed-resource
// controller built on provider-runtime. It reconciles a Greeting custom
// resource against a fake "external system" (an in-process store of external
// names), demonstrating the full wiring a real Krateo provider uses:
//
//   - a Managed type (Greeting) embedding provider-runtime conditions,
//   - an ExternalConnecter / ExternalClient pair (Observe/Create/Update/Delete),
//   - reconciler.NewReconciler with functional options,
//   - controller.DefaultOptions() -> ForControllerRuntime() queue wiring,
//   - the OTel-model slog logging handler from pkg/logging.
//
// Build it with `go build .` (no cluster needed). Running it requires a
// kubeconfig and the Greeting CRD from ./crd.yaml — see README.md.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/controller"
	"github.com/krateo-platformops/provider-runtime/pkg/logging"
	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/reconciler"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
)

// GroupVersion identifies the example API group.
var GroupVersion = schema.GroupVersion{Group: "examples.krateo.io", Version: "v1"}

// GreetingSpec is the desired state: just a message.
type GreetingSpec struct {
	Message string `json:"message,omitempty"`
}

// GreetingStatus embeds provider-runtime's conditioned status.
type GreetingStatus struct {
	prv1.ConditionedStatus `json:",inline"`
}

// Greeting is a managed resource: it satisfies resource.Managed by combining
// Kubernetes object metadata with provider-runtime conditions.
type Greeting struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   GreetingSpec   `json:"spec,omitempty"`
	Status GreetingStatus `json:"status,omitempty"`
}

// GetCondition satisfies resource.Conditioned.
func (g *Greeting) GetCondition(ct prv1.ConditionType) prv1.Condition {
	return g.Status.GetCondition(ct)
}

// SetConditions satisfies resource.Conditioned.
func (g *Greeting) SetConditions(c ...prv1.Condition) {
	g.Status.SetConditions(c...)
}

// DeepCopyObject satisfies runtime.Object.
func (g *Greeting) DeepCopyObject() runtime.Object {
	out := &Greeting{TypeMeta: g.TypeMeta, Spec: g.Spec}
	g.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	if cs := g.Status.ConditionedStatus.DeepCopy(); cs != nil {
		out.Status.ConditionedStatus = *cs
	}
	return out
}

// GreetingList is the list kind, required for scheme registration and caching.
type GreetingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Greeting `json:"items"`
}

// DeepCopyObject satisfies runtime.Object.
func (gl *GreetingList) DeepCopyObject() runtime.Object {
	out := &GreetingList{TypeMeta: gl.TypeMeta}
	gl.ListMeta.DeepCopyInto(&out.ListMeta)
	out.Items = make([]Greeting, len(gl.Items))
	for i := range gl.Items {
		out.Items[i] = *gl.Items[i].DeepCopyObject().(*Greeting)
	}
	return out
}

// externalStore stands in for a real external system (a cloud API, a Git
// host, ...). Keyed by external name.
type externalStore map[string]string

// connector satisfies reconciler.ExternalConnecter. A real provider would read
// credentials here (e.g. via resource.GetSecret) and build an API client.
type connector struct {
	log   logging.Logger
	store externalStore
}

func (c *connector) Connect(_ context.Context, _ resource.Managed) (reconciler.ExternalClient, error) {
	return &external{log: c.log, store: c.store}, nil
}

// external satisfies reconciler.ExternalClient against the fake store.
type external struct {
	log   logging.Logger
	store externalStore
}

func (e *external) Observe(_ context.Context, mg resource.Managed) (reconciler.ExternalObservation, error) {
	g, ok := mg.(*Greeting)
	if !ok {
		return reconciler.ExternalObservation{}, fmt.Errorf("managed resource is not a Greeting")
	}

	name := meta.GetExternalName(g)
	if name == "" {
		return reconciler.ExternalObservation{ResourceExists: false}, nil
	}

	got, exists := e.store[name]
	if !exists {
		return reconciler.ExternalObservation{ResourceExists: false}, nil
	}

	if got != g.Spec.Message {
		return reconciler.ExternalObservation{
			ResourceExists:   true,
			ResourceUpToDate: false,
			Diff:             fmt.Sprintf("message: %q != %q", got, g.Spec.Message),
		}, nil
	}

	g.SetConditions(prv1.Available())
	return reconciler.ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
}

func (e *external) Create(_ context.Context, mg resource.Managed) error {
	g := mg.(*Greeting)
	meta.SetExternalName(g, g.GetName())
	e.store[g.GetName()] = g.Spec.Message
	e.log.Info("created external resource", "externalName", g.GetName())
	return nil
}

func (e *external) Update(_ context.Context, mg resource.Managed) error {
	g := mg.(*Greeting)
	e.store[meta.GetExternalName(g)] = g.Spec.Message
	e.log.Info("updated external resource", "externalName", meta.GetExternalName(g))
	return nil
}

func (e *external) Delete(_ context.Context, mg resource.Managed) error {
	g := mg.(*Greeting)
	delete(e.store, meta.GetExternalName(g))
	e.log.Info("deleted external resource", "externalName", meta.GetExternalName(g))
	return nil
}

func main() {
	// OTel-model JSON logs on stderr (timestamp, SeverityText/Number,
	// service.name, trace/span ids when a span is in context).
	log := logging.NewSlogLogger(*slog.New(
		logging.NewOTelJSONHandler(slog.LevelDebug, os.Stderr,
			logging.ServiceNameAttr("greeting-controller")...)))

	scheme := runtime.NewScheme()
	scheme.AddKnownTypes(GroupVersion, &Greeting{}, &GreetingList{})
	metav1.AddToGroupVersion(scheme, GroupVersion)

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: scheme})
	if err != nil {
		log.Error(err, "unable to create manager")
		os.Exit(1)
	}

	o := controller.DefaultOptions()
	o.Logger = log

	r := reconciler.NewReconciler(mgr,
		resource.ManagedKind(GroupVersion.WithKind("Greeting")),
		reconciler.WithExternalConnecter(&connector{log: log, store: externalStore{}}),
		reconciler.WithLogger(log.WithValues("controller", "greeting")),
	)

	if err := ctrl.NewControllerManagedBy(mgr).
		Named("greeting").
		For(&Greeting{}).
		WithOptions(o.ForControllerRuntime()).
		Complete(r); err != nil {
		log.Error(err, "unable to create controller")
		os.Exit(1)
	}

	log.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "manager exited with error")
		os.Exit(1)
	}
}
