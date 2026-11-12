package reconciler

import (
	"context"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/krateo-platformops/plumbing/kubeutil/event"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/errors"
	"github.com/krateo-platformops/provider-runtime/pkg/logging"
	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
	"github.com/krateo-platformops/provider-runtime/pkg/tracing"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
)

const (
	// FinalizerName is the string that is used as finalizer on managed resource
	// objects.
	FinalizerName = "finalizer.managedresource.krateo.io"

	reconcileGracePeriod = 30 * time.Second
	reconcileTimeout     = 1 * time.Minute

	defaultpollInterval = 1 * time.Minute
	defaultGracePeriod  = 30 * time.Second

	// defaultCreateRecoveryGracePeriod bounds the confirm-the-negative window for an unresolved
	// create handshake. Long enough for an eventually-consistent API to surface a create that
	// landed; short enough that a create which never landed is retried promptly.
	defaultCreateRecoveryGracePeriod = 2 * time.Minute
	// defaultCreateRecoveryReobserveInterval is how often to re-observe inside that window.
	defaultCreateRecoveryReobserveInterval = 15 * time.Second
)

// Error strings.
const (
	errGetManaged               = "cannot get managed resource"
	errUpdateManagedAnnotations = "cannot update managed resource annotations"
	errCreateIncomplete         = "cannot determine creation result - remove the " + meta.AnnotationKeyExternalCreatePending + " annotation if it is safe to proceed"
	errReconcileConnect         = "connect failed"
	errReconcileObserve         = "observe failed"
	errFmtCreateWaitsOnObserve  = "external create pending since %s cannot be resolved until observe succeeds"
	errReconcileCreate          = "create failed"
	errReconcileUpdate          = "update failed"
	errReconcileDelete          = "delete failed"
	errExternalResourceNotExist = "external resource does not exist"
	errReadyNotSetAfterObserve  = "Observe succeeded but did not set the Ready condition, which still reads " +
		string(prv1.ReasonNotObserved) + " from an earlier failure: the provider breaks the provider-runtime contract " +
		"\"a successful Observe must set Ready\". Ready stays " + string(prv1.ReasonNotObserved) + " until the provider sets it"
)

// Event reasons.
// ReasonReadyNotSetAfterObserve is the reason of the Warning event the runtime
// records when a provider breaks the Ready contract: Observe succeeded but left
// Ready at NotObserved. Exported so providers' tests can assert on the event.
// See the Ready contract on ExternalClient.Observe.
const ReasonReadyNotSetAfterObserve event.Reason = "ReadyNotSetAfterObserve"

const (
	reasonCannotConnect       event.Reason = "CannotConnectToProvider"
	reasonCannotDisconnect    event.Reason = "CannotDisconnectFromProvider"
	reasonCannotInitialize    event.Reason = "CannotInitializeManagedResource"
	reasonCannotResolveRefs   event.Reason = "CannotResolveResourceReferences"
	reasonCannotObserve       event.Reason = "CannotObserveExternalResource"
	reasonCannotCreate        event.Reason = "CannotCreateExternalResource"
	reasonCannotDelete        event.Reason = "CannotDeleteExternalResource"
	reasonCannotPublish       event.Reason = "CannotPublishConnectionDetails"
	reasonCannotUnpublish     event.Reason = "CannotUnpublishConnectionDetails"
	reasonCannotUpdate        event.Reason = "CannotUpdateExternalResource"
	reasonCannotUpdateManaged event.Reason = "CannotUpdateManagedResource"

	reasonDeleted event.Reason = "DeletedExternalResource"
	reasonCreated event.Reason = "CreatedExternalResource"
	reasonUpdated event.Reason = "UpdatedExternalResource"
	reasonPending event.Reason = "PendingExternalResource"

	reasonReconciliationPaused event.Reason = "ReconciliationPaused"
)

// Event actions.
const (
	actionProcessEvent            event.Action = "ProcessEvent"
	actionCreateEvent             event.Action = "CreateEvent"
	actionUpdateEvent             event.Action = "UpdateEvent"
	actionDeleteEvent             event.Action = "DeleteEvent"
	actionObserveEvent            event.Action = "ObserveEvent"
	actionCreateExternalResource  event.Action = "CreateExternalResource"
	actionUpdateExternalResource  event.Action = "UpdateExternalResource"
	actionDeleteExternalResource  event.Action = "DeleteExternalResource"
	actionObserveExternalResource event.Action = "ObserveExternalResource"
)

// ControllerName returns the recommended name for controllers that use this
// package to reconcile a particular kind of managed resource.
func ControllerName(kind string) string {
	return "managed/" + strings.ToLower(kind)
}

// MetricsRecorder records high-level reconcile metrics.
type MetricsRecorder interface {
	RecordGetDuration(ctx context.Context, d time.Duration)
	IncGetFailure(ctx context.Context)

	RecordConnectDuration(ctx context.Context, d time.Duration)
	IncConnectFailure(ctx context.Context)
	RecordObserveDuration(ctx context.Context, d time.Duration)
	IncObserveFailure(ctx context.Context)
	RecordCreateDuration(ctx context.Context, d time.Duration)
	IncCreateFailure(ctx context.Context)
	RecordUpdateDuration(ctx context.Context, d time.Duration)
	IncUpdateFailure(ctx context.Context)
	RecordDeleteDuration(ctx context.Context, d time.Duration)
	IncDeleteFailure(ctx context.Context)
	RecordFinalizerAddDuration(ctx context.Context, d time.Duration)
	IncFinalizerAddFailure(ctx context.Context)
	RecordFinalizerRemoveDuration(ctx context.Context, d time.Duration)
	IncFinalizerRemoveFailure(ctx context.Context)
	RecordStatusUpdateDuration(ctx context.Context, d time.Duration)
	IncStatusUpdateFailure(ctx context.Context)

	RecordReconcileDuration(ctx context.Context, d time.Duration)
	IncReconcileSuccess(ctx context.Context)
	IncReconcileFailure(ctx context.Context)
	IncReconcileRequeueAfter(ctx context.Context)
	IncReconcileRequeueImmediate(ctx context.Context)
	IncReconcileErrorRequeue(ctx context.Context)
	AddReconcileInFlight(delta int64)
}

// A CriticalAnnotationUpdater is used when it is critical that annotations must
// be updated before returning from the Reconcile loop.
type CriticalAnnotationUpdater interface {
	UpdateCriticalAnnotations(ctx context.Context, o client.Object) error
}

// A CriticalAnnotationUpdateFn may be used when it is critical that annotations
// must be updated before returning from the Reconcile loop.
type CriticalAnnotationUpdateFn func(ctx context.Context, o client.Object) error

// UpdateCriticalAnnotations of the supplied object.
func (fn CriticalAnnotationUpdateFn) UpdateCriticalAnnotations(ctx context.Context, o client.Object) error {
	return fn(ctx, o)
}

// An ExternalConnecter produces a new ExternalClient given the supplied
// Managed resource.
//
// With tracing on, Connect's ctx holds the reconcile's connect span, which
// ends when Connect returns. An ExternalClient built here must not keep that
// ctx for later calls: each ExternalClient method gets its own ctx, with its own
// span, and must use that one (docs/design-tracing.md §4.4).
type ExternalConnecter interface {
	// Connect to the provider specified by the supplied managed resource and
	// produce an ExternalClient.
	Connect(ctx context.Context, mg resource.Managed) (ExternalClient, error)
}

// An ExternalDisconnecter disconnects from a provider.
type ExternalDisconnecter interface {
	// Disconnect from the provider and close the ExternalClient.
	Disconnect(ctx context.Context) error
}

// A NopDisconnecter converts an ExternalConnecter into an
// ExternalConnectDisconnecter with a no-op Disconnect method.
type NopDisconnecter struct {
	c ExternalConnecter
}

// Connect calls the underlying ExternalConnecter's Connect method.
func (c *NopDisconnecter) Connect(ctx context.Context, mg resource.Managed) (ExternalClient, error) {
	return c.c.Connect(ctx, mg)
}

// Disconnect does nothing. It never returns an error.
func (c *NopDisconnecter) Disconnect(_ context.Context) error {
	return nil
}

// NewNopDisconnecter converts an ExternalConnecter into an
// ExternalConnectDisconnecter with a no-op Disconnect method.
func NewNopDisconnecter(c ExternalConnecter) ExternalConnectDisconnecter {
	return &NopDisconnecter{c}
}

// An ExternalConnectDisconnecter produces a new ExternalClient given the supplied
// Managed resource.
type ExternalConnectDisconnecter interface {
	ExternalConnecter
	ExternalDisconnecter
}

// An ExternalConnectorFn is a function that satisfies the ExternalConnecter
// interface.
type ExternalConnectorFn func(ctx context.Context, mg resource.Managed) (ExternalClient, error)

// Connect to the provider specified by the supplied managed resource and
// produce an ExternalClient.
func (ec ExternalConnectorFn) Connect(ctx context.Context, mg resource.Managed) (ExternalClient, error) {
	return ec(ctx, mg)
}

// An ExternalDisconnectorFn is a function that satisfies the ExternalConnecter
// interface.
type ExternalDisconnectorFn func(ctx context.Context) error

// Disconnect from provider and close the ExternalClient.
func (ed ExternalDisconnectorFn) Disconnect(ctx context.Context) error {
	return ed(ctx)
}

// ExternalConnectDisconnecterFns are functions that satisfy the
// ExternalConnectDisconnecter interface.
type ExternalConnectDisconnecterFns struct {
	ConnectFn    func(ctx context.Context, mg resource.Managed) (ExternalClient, error)
	DisconnectFn func(ctx context.Context) error
}

// Connect to the provider specified by the supplied managed resource and
// produce an ExternalClient.
func (fns ExternalConnectDisconnecterFns) Connect(ctx context.Context, mg resource.Managed) (ExternalClient, error) {
	return fns.ConnectFn(ctx, mg)
}

// Disconnect from the provider and close the ExternalClient.
func (fns ExternalConnectDisconnecterFns) Disconnect(ctx context.Context) error {
	return fns.DisconnectFn(ctx)
}

// An ExternalClient manages the lifecycle of an external resource.
// None of the calls here should be blocking. All of the calls should be
// idempotent. For example, Create call should not return AlreadyExists error
// if it's called again with the same parameters or Delete call should not
// return error if there is an ongoing deletion or resource does not exist.
//
// Each method's ctx holds its own child span of the reconcile (observe,
// create, update or delete) with tracing on: use it for the calls the method
// makes, and bind a Logger to it with logging.WithContext, so they join the
// trace. Never use a ctx kept from Connect.
//
// A method that writes the managed object to the API server must leave mg
// describing what is stored. Write mg itself, so the client decodes the
// response into it. Or write a copy that carries mg's resourceVersion, then
// copy the new resourceVersion and every field you wrote onto mg. Never copy
// only the resourceVersion: the runtime's next write would succeed and revert
// your write. A method that re-reads before writing must copy the whole
// written object onto mg, not only its version, or it can revert another
// writer's change. A write that leaves mg stale makes the runtime's next write
// of the object fail with a conflict. After Observe, that includes the
// finalizer, create-pending and marker writes as well as status.
//
// Copy the new resourceVersion onto mg only after a write succeeded: an
// attempt that read and then failed to write must leave mg untouched. A wrong
// version succeeds against the wrong object state; a stale one fails loudly.
//
// A re-read-then-write still reverts a change someone else made between the
// runtime's read and yours, including a human's status patch; write mg itself
// if that matters.
//
// The runtime does not retry or merge a status write that conflicts. It
// returns the error, which names this rule and keeps the pass's own error.
// Two helpers check the rule in a provider's tests:
// reconcilertest.AssertLeavesManagedCurrent, that a method's successful write
// leaves mg current, and reconcilertest.AssertFailedWriteLeavesManaged, that a
// failed write leaves mg's resourceVersion untouched.
type ExternalClient interface {
	// Observe the external resource the supplied Managed resource
	// represents, if any. Observe implementations must not modify the
	// external resource, but may update the supplied Managed resource to
	// reflect the state of the external resource. Status modifications are
	// automatically persisted unless ResourceLateInitialized is true - see
	// ResourceLateInitialized for more detail.
	//
	// When the external resource does not exist, return ResourceExists: false
	// with a nil error. An error ends the reconcile before Create and before
	// an unresolved create handshake can be resolved, which needs Observe's
	// answer. Before returning an error, ask whether answering
	// ResourceExists: false could make Create duplicate the resource. If not,
	// answer. If so, return an error only for a transient failure that a
	// retry can resolve; a permanent one (for example a local template that
	// never renders) keeps the resource stuck until the provider changes.
	// See docs/overview.md.
	//
	// Every Observe that returns a nil error must set the Ready condition on
	// mg, on every path, including those that report ResourceExists: false.
	// When Connect or Observe returns an error, the runtime sets Ready to
	// NotObserved (Status Unknown) next to the error on Synced, unless Ready
	// is Deleting or the error is wrapped with ReadyAssessed, which an Observe
	// returns when it set Ready itself and the error concerns something else.
	// The runtime never sets Ready on recovery: a successful Observe that
	// leaves Ready at NotObserved is reported as a provider defect.
	// reconcilertest.AssertObserveSetsReady checks this in a provider's tests.
	Observe(ctx context.Context, mg resource.Managed) (ExternalObservation, error)

	// Create an external resource per the specifications of the supplied
	// Managed resource. Called when Observe reports that the associated
	// external resource does not exist. Create implementations may update
	// managed resource annotations, and those updates will be persisted.
	// All other updates will be discarded.
	Create(ctx context.Context, mg resource.Managed) error

	// Update the external resource represented by the supplied Managed
	// resource, if necessary. Called unless Observe reports that the
	// associated external resource is up to date.
	Update(ctx context.Context, mg resource.Managed) error

	// Delete the external resource upon deletion of its associated Managed
	// resource. Called when the managed resource has been deleted.
	Delete(ctx context.Context, mg resource.Managed) error
}

// ExternalClientFns are a series of functions that satisfy the ExternalClient
// interface.
type ExternalClientFns struct {
	ObserveFn func(ctx context.Context, mg resource.Managed) (ExternalObservation, error)
	CreateFn  func(ctx context.Context, mg resource.Managed) error
	UpdateFn  func(ctx context.Context, mg resource.Managed) error
	DeleteFn  func(ctx context.Context, mg resource.Managed) error
}

// Observe the external resource the supplied Managed resource represents, if
// any.
func (e ExternalClientFns) Observe(ctx context.Context, mg resource.Managed) (ExternalObservation, error) {
	return e.ObserveFn(ctx, mg)
}

// Create an external resource per the specifications of the supplied Managed
// resource.
func (e ExternalClientFns) Create(ctx context.Context, mg resource.Managed) error {
	return e.CreateFn(ctx, mg)
}

// Update the external resource represented by the supplied Managed resource, if
// necessary.
func (e ExternalClientFns) Update(ctx context.Context, mg resource.Managed) error {
	return e.UpdateFn(ctx, mg)
}

// Delete the external resource upon deletion of its associated Managed
// resource.
func (e ExternalClientFns) Delete(ctx context.Context, mg resource.Managed) error {
	return e.DeleteFn(ctx, mg)
}

// A NopConnecter does nothing.
type NopConnecter struct{}

// Connect returns a NopClient. It never returns an error.
func (c *NopConnecter) Connect(_ context.Context, _ resource.Managed) (ExternalClient, error) {
	return &NopClient{}, nil
}

// A NopClient does nothing.
type NopClient struct{}

// Observe does nothing. It returns an empty ExternalObservation and no error.
func (c *NopClient) Observe(ctx context.Context, mg resource.Managed) (ExternalObservation, error) {
	return ExternalObservation{}, nil
}

// Create does nothing. It returns an empty ExternalCreation and no error.
func (c *NopClient) Create(ctx context.Context, mg resource.Managed) error {
	return nil
}

// Update does nothing. It returns an empty ExternalUpdate and no error.
func (c *NopClient) Update(ctx context.Context, mg resource.Managed) error {
	return nil
}

// Delete does nothing. It never returns an error.
func (c *NopClient) Delete(ctx context.Context, mg resource.Managed) error { return nil }

// An ExternalObservation is the result of an observation of an external
// resource.
type ExternalObservation struct {
	// ResourceExists must be true if a corresponding external resource exists
	// for the managed resource. Typically this is proven by the presence of an
	// external resource of the expected kind whose unique identifier matches
	// the managed resource's external name. The Provider Runtime uses this information to
	// determine whether it needs to create or delete the external resource.
	ResourceExists bool

	// ResourceUpToDate should be true if the corresponding external resource
	// appears to be up-to-date - i.e. updating the external resource to match
	// the desired state of the managed resource would be a no-op. Keep in mind
	// that often only a subset of external resource fields can be updated.
	// The Provider Runtime uses this information to determine whether it needs to update
	// the external resource.
	ResourceUpToDate bool

	// ResourceLateInitialized should be true if the managed resource's spec was
	// updated during its observation. A provider may update a
	// managed resource's spec fields after it is created or updated, as long as
	// the updates are limited to setting previously unset fields, and adding
	// keys to maps. Provider Runtime uses this information to determine whether
	// changes to the spec were made during observation that must be persisted.
	// Note that changes to the spec will be persisted before changes to the
	// status, and that pending changes to the status may be lost when the spec
	// is persisted. Status changes will be persisted by the first subsequent
	// observation that _does not_ late initialize the managed resource, so it
	// is important that Observe implementations do not late initialize the
	// resource every time they are called.
	ResourceLateInitialized bool

	// Diff is a Debug level message that is sent to the reconciler when
	// there is a change in the observed Managed Resource. It is useful for
	// finding where the observed diverges from the desired state.
	// The string should be a cmp.Diff that details the difference.
	Diff string
}

// A Reconciler reconciles managed resources by creating and managing the
// lifecycle of an external resource, i.e. a resource in an external system such
// as a cloud provider API. Each controller must watch the managed resource kind
// for which it is responsible.
type Reconciler struct {
	client     client.Client
	newManaged func() resource.Managed

	// apiReader reads the API server live, for a status conflict's message
	// only (WithAPIReader). nil, the default, gives the plain message.
	apiReader client.Reader

	// kind names the managed kind (Kind.group) in reports about it, because a typed object
	// read through the client does not reliably carry its own TypeMeta.
	kind string
	// objectKind is the Kind alone, the reconcile span's k8s.object.kind.
	objectKind string

	pollInterval        time.Duration
	pollIntervalHook    PollIntervalHook
	timeout             time.Duration
	creationGracePeriod time.Duration

	// createRecoveryGracePeriod bounds how long an UNRESOLVED create handshake keeps refusing
	// before concluding the create never landed and retrying it. Distinct from
	// creationGracePeriod, which covers a create we know SUCCEEDED but whose resource is not yet
	// visible; this one covers a create whose outcome was never recorded at all.
	createRecoveryGracePeriod time.Duration
	// createRecoveryReobserveInterval is how soon to re-observe while still inside that window.
	createRecoveryReobserveInterval time.Duration

	// The below structs embed the set of interfaces used to implement the
	// managed resource reconciler. We do this primarily for readability, so
	// that the reconciler logic reads r.external.Connect(),
	// r.managed.Delete(), etc.
	external mrExternal
	managed  mrManaged

	log               logging.Logger
	record            event.Recorder
	throttledRecorder event.Recorder
	metrics           MetricsRecorder

	// governor gates external calls; nil (the default) gates nothing.
	governor  *ratelimit.Governor
	keyErrors keyErrorLog

	// tracerProv is the TracerProvider WithTracerProvider set, or nil to use
	// tracing.TracerProvider() (docs/design-tracing.md §3.3).
	tracerProv trace.TracerProvider
	// traceWindow is how long a new traceparent value keeps making Parent
	// passes that don't act (§6.2).
	traceWindow time.Duration
	// traces is the in-memory record of traceparent values seen and marked.
	traces traceMemo
}

type mrManaged struct {
	CriticalAnnotationUpdater
	resource.Finalizer
}

func defaultMRManaged(m manager.Manager) mrManaged {
	return mrManaged{
		CriticalAnnotationUpdater: NewRetryingCriticalAnnotationUpdater(m.GetClient()),
		Finalizer:                 resource.NewAPIFinalizer(m.GetClient(), FinalizerName),
	}
}

type mrExternal struct {
	ExternalConnectDisconnecter

	// keyer names the connecter's rate-limit keys, or is nil. It is taken from the
	// connecter the provider supplied, because NewNopDisconnecter hides it.
	keyer rateLimitKeys
}

func defaultMRExternal() mrExternal {
	return mrExternal{
		ExternalConnectDisconnecter: NewNopDisconnecter(&NopConnecter{}),
	}
}

// PollIntervalHook represents the function type passed to the
// WithPollIntervalHook option to support dynamic computation of the poll
// interval.
type PollIntervalHook func(managed resource.Managed, pollInterval time.Duration) time.Duration

func defaultPollIntervalHook(_ resource.Managed, pollInterval time.Duration) time.Duration {
	return pollInterval
}

func (r *Reconciler) recordGet(ctx context.Context, started time.Time, err error) {
	if r.metrics == nil {
		return
	}
	r.metrics.RecordGetDuration(ctx, time.Since(started))
	if err != nil {
		r.metrics.IncGetFailure(ctx)
	}
}

func (r *Reconciler) recordConnect(ctx context.Context, started time.Time, err error) {
	if r.metrics == nil {
		return
	}
	r.metrics.RecordConnectDuration(ctx, time.Since(started))
	if err != nil {
		r.metrics.IncConnectFailure(ctx)
	}
}

func (r *Reconciler) recordObserve(ctx context.Context, started time.Time, err error) {
	if r.metrics == nil {
		return
	}
	r.metrics.RecordObserveDuration(ctx, time.Since(started))
	if err != nil {
		r.metrics.IncObserveFailure(ctx)
	}
}

func (r *Reconciler) recordCreate(ctx context.Context, started time.Time, err error) {
	if r.metrics == nil {
		return
	}
	r.metrics.RecordCreateDuration(ctx, time.Since(started))
	if err != nil {
		r.metrics.IncCreateFailure(ctx)
	}
}

func (r *Reconciler) recordUpdate(ctx context.Context, started time.Time, err error) {
	if r.metrics == nil {
		return
	}
	r.metrics.RecordUpdateDuration(ctx, time.Since(started))
	if err != nil {
		r.metrics.IncUpdateFailure(ctx)
	}
}

func (r *Reconciler) recordDelete(ctx context.Context, started time.Time, err error) {
	if r.metrics == nil {
		return
	}
	r.metrics.RecordDeleteDuration(ctx, time.Since(started))
	if err != nil {
		r.metrics.IncDeleteFailure(ctx)
	}
}

func (r *Reconciler) recordFinalizerAdd(ctx context.Context, started time.Time, err error) {
	if r.metrics == nil {
		return
	}
	r.metrics.RecordFinalizerAddDuration(ctx, time.Since(started))
	if err != nil {
		r.metrics.IncFinalizerAddFailure(ctx)
	}
}

func (r *Reconciler) recordFinalizerRemove(ctx context.Context, started time.Time, err error) {
	if r.metrics == nil {
		return
	}
	r.metrics.RecordFinalizerRemoveDuration(ctx, time.Since(started))
	if err != nil {
		r.metrics.IncFinalizerRemoveFailure(ctx)
	}
}

func (r *Reconciler) recordStatusUpdate(ctx context.Context, started time.Time, err error) {
	if r.metrics == nil {
		return
	}
	r.metrics.RecordStatusUpdateDuration(ctx, time.Since(started))
	if err != nil {
		r.metrics.IncStatusUpdateFailure(ctx)
	}
}

// A ReconcilerOption configures a Reconciler.
type ReconcilerOption func(*Reconciler)

// WithTimeout specifies the timeout duration cumulatively for all the calls happen
// in the reconciliation function. In case the deadline exceeds, reconciler will
// still have some time to make the necessary calls to report the error such as
// status update.
func WithTimeout(duration time.Duration) ReconcilerOption {
	return func(r *Reconciler) {
		r.timeout = duration
	}
}

// WithPollInterval specifies how long the Reconciler should wait before queueing
// a new reconciliation after a successful reconcile. The Reconciler requeues
// after a specified duration when it is not actively waiting for an external
// operation, but wishes to check whether an existing external resource needs to
// be synced to its Managed resource.
func WithPollInterval(after time.Duration) ReconcilerOption {
	return func(r *Reconciler) {
		r.pollInterval = after
	}
}

// WithPollIntervalHook adds a hook that can be used to configure the
// delay before an up-to-date resource is reconciled again after a successful
// reconcile. If this option is passed multiple times, only the latest hook
// will be used.
func WithPollIntervalHook(hook PollIntervalHook) ReconcilerOption {
	return func(r *Reconciler) {
		r.pollIntervalHook = hook
	}
}

// WithPollJitterHook adds a simple PollIntervalHook to add jitter to the poll
// interval used when queuing a new reconciliation after a successful
// reconcile. The added jitter will be a random duration between -jitter and
// +jitter. This option wraps WithPollIntervalHook, and is subject to the same
// constraint that only the latest hook will be used.
func WithPollJitterHook(jitter time.Duration) ReconcilerOption {
	return WithPollIntervalHook(func(_ resource.Managed, pollInterval time.Duration) time.Duration {
		return pollInterval + time.Duration((rand.Float64()-0.5)*2*float64(jitter)) //nolint:gosec // No need for secure randomness.
	})
}

// WithCreationGracePeriod configures an optional period during which we will
// wait for the external API to report that a newly created external resource
// exists. This allows us to tolerate eventually consistent APIs that do not
// immediately report that newly created resources exist when queried. All
// resources have a 30 second grace period by default.
func WithCreationGracePeriod(d time.Duration) ReconcilerOption {
	return func(r *Reconciler) {
		r.creationGracePeriod = d
	}
}

// WithCreateRecoveryGracePeriod specifies how long an UNRESOLVED create handshake keeps refusing
// before the reconciler concludes the create never reached the external system and retries it.
//
// This is the confirm-the-negative window. Inside it nothing is recreated, so a create that DID
// land but is not yet visible in an eventually-consistent API is adopted rather than duplicated.
// Once it elapses with the resource still absent, refusing further would strand the resource
// forever — see the recovery branch in Reconcile. Defaults to 2 minutes.
//
// Providers whose external API is slower to converge should raise this. Setting it to zero makes
// recovery retry immediately, which is only safe when the external API is strongly consistent.
func WithCreateRecoveryGracePeriod(d time.Duration) ReconcilerOption {
	return func(r *Reconciler) {
		r.createRecoveryGracePeriod = d
	}
}

// WithCreateRecoveryReobserveInterval specifies how soon to re-observe an unresolved create while
// still inside the recovery grace period. Defaults to 15 seconds.
func WithCreateRecoveryReobserveInterval(d time.Duration) ReconcilerOption {
	return func(r *Reconciler) {
		r.createRecoveryReobserveInterval = d
	}
}

// WithExternalConnecter specifies how the Reconciler should connect to the API
// used to sync and delete external resources.
func WithExternalConnecter(c ExternalConnecter) ReconcilerOption {
	return func(r *Reconciler) {
		r.external.ExternalConnectDisconnecter = NewNopDisconnecter(c)
		r.external.keyer = keyerOf(c)
	}
}

// WithExternalConnectDisconnecter specifies how the Reconciler should connect and disconnect to the API
// used to sync and delete external resources.
func WithExternalConnectDisconnecter(c ExternalConnectDisconnecter) ReconcilerOption {
	return func(r *Reconciler) {
		r.external.ExternalConnectDisconnecter = c
		r.external.keyer = keyerOf(c)
	}
}

// WithCriticalAnnotationUpdater specifies how the Reconciler should update a
// managed resource's critical annotations. Implementations typically contain
// some kind of retry logic to increase the likelihood that critical annotations
// (like non-deterministic external names) will be persisted.
func WithCriticalAnnotationUpdater(u CriticalAnnotationUpdater) ReconcilerOption {
	return func(r *Reconciler) {
		r.managed.CriticalAnnotationUpdater = u
	}
}

// WithFinalizer specifies how the Reconciler should add and remove
// finalizers to and from the managed resource.
func WithFinalizer(f resource.Finalizer) ReconcilerOption {
	return func(r *Reconciler) {
		r.managed.Finalizer = f
	}
}

// WithLogger specifies how the Reconciler should log messages.
func WithLogger(l logging.Logger) ReconcilerOption {
	return func(r *Reconciler) {
		r.log = l
	}
}

// WithMetrics specifies how the Reconciler should record telemetry.
func WithMetrics(m MetricsRecorder) ReconcilerOption {
	return func(r *Reconciler) {
		r.metrics = m
	}
}

// WithRecorder specifies how the Reconciler should record events.
func WithRecorder(er event.Recorder) ReconcilerOption {
	return func(r *Reconciler) {
		r.record = er
	}
}

// WithThrottledRecorder specifies a separate event recorder for events that are emitted when an error is encountered during reconciliation. This allows users of the Reconciler to choose to emit these events at a different level (e.g. Warning instead of Normal) without having to use the same level for all events. Defaults to the same recorder as the one set by WithRecorder if not specified.
func WithThrottledRecorder(er event.Recorder) ReconcilerOption {
	return func(r *Reconciler) {
		r.throttledRecorder = er
	}
}

// WithAPIReader gives the Reconciler a reader that reads the API server live,
// such as mgr.GetAPIReader(). It's used for one thing: when a status write
// conflicts, the Reconciler reads the object once through it and adds to the
// error the live resourceVersion and generation, the top-level status keys
// that differ from the pass's read, and the newest managedFields entry, as a
// hint. Nothing is decided from that read. A failed read gives the plain
// message.
//
// Setting it costs one in-memory deep copy of the managed object per pass (the
// pass's read, kept for the comparison), and one uncached Get, only when a
// status write conflicts. With a rate-limit governor too, the governor reuses
// that copy, so a pass takes one copy, or two on a pass where the trace marker
// write moved the version, which happens at most once per trace window.
//
// Without it, or with a nil reader, a status conflict's error is the plain
// message. NewReconciler can't take the reader from its manager itself: a
// test manager such as fake.Manager embeds a nil manager.Manager, whose
// GetAPIReader would panic.
func WithAPIReader(rd client.Reader) ReconcilerOption {
	return func(r *Reconciler) {
		r.apiReader = rd
	}
}

// NewReconciler returns a Reconciler that reconciles managed resources of the
// supplied ManagedKind with resources in an external system such as a cloud
// provider API. It panics if asked to reconcile a managed resource kind that is
// not registered with the supplied manager's runtime.Scheme. The returned
// Reconciler reconciles with a dummy, no-op 'external system' by default;
// callers should supply an ExternalConnector that returns an ExternalClient
// capable of managing resources in a real system.
func NewReconciler(m manager.Manager, of resource.ManagedKind, o ...ReconcilerOption) *Reconciler {
	nm := func() resource.Managed {
		return resource.MustCreateObject(schema.GroupVersionKind(of), m.GetScheme()).(resource.Managed)
	}

	// Panic early if we've been asked to reconcile a resource kind that has not
	// been registered with our controller manager's scheme.
	_ = nm()

	r := &Reconciler{
		client:              m.GetClient(),
		newManaged:          nm,
		kind:                schema.GroupVersionKind(of).GroupKind().String(),
		objectKind:          schema.GroupVersionKind(of).Kind,
		traceWindow:         defaultTraceContinueWindow,
		pollInterval:        defaultpollInterval,
		pollIntervalHook:    defaultPollIntervalHook,
		creationGracePeriod: defaultGracePeriod,

		createRecoveryGracePeriod:       defaultCreateRecoveryGracePeriod,
		createRecoveryReobserveInterval: defaultCreateRecoveryReobserveInterval,
		timeout:                         reconcileTimeout,
		managed:                         defaultMRManaged(m),
		external:                        defaultMRExternal(),
		log:                             logging.NewNopLogger(),
		record:                          event.NewNopRecorder(),
	}

	for _, ro := range o {
		ro(r)
	}

	// Default the throttled recorder to the same recorder as the main recorder if it wasn't explicitly set by the caller.
	if r.throttledRecorder == nil {
		r.throttledRecorder = r.record
	}

	return r
}

// Reconcile a managed resource with an external resource.
//
// It reads the object, then opens the reconcile root span, start-stamped at the
// top of Reconcile so it covers the read, as the parent-or-link decision says
// (docs/design-tracing.md §4.1, §6.2). It binds the per-reconcile Logger to the
// span's context, and runs the pass with the span in every context it hands
// on. With tracing off no span starts.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (result reconcile.Result, err error) {
	started := time.Now()
	inCtx := ctx

	if r.metrics != nil {
		r.metrics.AddReconcileInFlight(1)
		defer r.metrics.AddReconcileInFlight(-1)
		defer func() {
			r.metrics.RecordReconcileDuration(inCtx, time.Since(started))
			if err != nil {
				r.metrics.IncReconcileFailure(inCtx)
				r.metrics.IncReconcileErrorRequeue(inCtx)
				return
			}

			r.metrics.IncReconcileSuccess(inCtx)
			switch {
			case result.RequeueAfter > 0:
				r.metrics.IncReconcileRequeueAfter(inCtx)
			case isRateLimitedRequeue(result):
				r.metrics.IncReconcileRequeueImmediate(inCtx)
			}
		}()
	}

	deadline := started.Add(r.timeout + reconcileGracePeriod)
	getCtx, cancelGet := context.WithDeadline(ctx, deadline)
	managed := r.newManaged()
	err = r.client.Get(getCtx, req.NamespacedName, managed)
	cancelGet()
	r.recordGet(inCtx, started, err)

	tp := r.tracerProvider()
	if kerrors.IsNotFound(err) {
		// The object is gone: no span, as in unstructured-runtime. The memo entry goes
		// whether tracing is on or off: an entry kept from when tracing was on would
		// otherwise outlive its object, and NotFound passes are rare enough that the
		// lock costs nothing.
		r.traces.forget(req.NamespacedName)
		// A NotFound is the normal end of a deletion: the finalizer was
		// released and the object went away. It isn't an error, so it's logged
		// at Debug and the pass returns no error and no requeue. Other Get
		// failures are still logged as errors below.
		logging.WithContext(r.log.WithValues("request", req), ctx).Debug("Managed resource is gone")
		return reconcile.Result{}, nil
	}

	spanCtx, pass := r.beginTrace(ctx, tp, req, managed, err == nil, started)
	panicked := true
	defer func() { pass.end(err, panicked) }()

	// Bound to the span's context, which is the incoming ctx plus the span: not the timeout one
	// below, so a record logged after the deadline still carries a live context to the exporter.
	log := logging.WithContext(r.log.WithValues("request", req), spanCtx)
	log.Debug("Reconciling")
	if err != nil {
		// We'll be requeued implicitly because we return an error.
		log.Error(err, "Cannot get managed resource")
		panicked = false
		return reconcile.Result{}, errors.Wrap(err, errGetManaged)
	}

	ctx, cancel := context.WithDeadline(spanCtx, deadline)
	defer cancel()
	externalCtx, externalCancel := context.WithDeadline(ctx, started.Add(r.timeout))
	defer externalCancel()
	if r.governor != nil {
		// Signals the provider reports during its calls reach the governor.
		externalCtx = ratelimit.WithSink(externalCtx, r.governor)
	}

	result, err = r.reconcile(ctx, externalCtx, log, managed, pass)
	panicked = false
	return result, err
}

// reconcile is one pass over managed, which Reconcile has read. ctx holds the
// reconcile span; externalCtx is the provider calls' context, with the external
// timeout. The pass's own spans, events and the marker go through pass.
func (r *Reconciler) reconcile(ctx, externalCtx context.Context, log logging.Logger, managed resource.Managed, pass *tracePass) (reconcile.Result, error) { //nolint:gocyclo // See note below.
	// read is the object as the pass read it, taken only with a live reader. A
	// status conflict's message compares the live object's status with it, and
	// the governor's deferral reuses it as pristine (below), so a pass with both
	// still takes one deep copy, unless the marker moved the version.
	var read resource.Managed
	if r.apiReader != nil {
		read, _ = managed.DeepCopyObject().(resource.Managed)
	}
	// writeStatus is every status write of the pass. Each is the last write of
	// its path. passErr is the error the path goes on to return, or nil. A
	// conflict isn't retried: it stays an error, which names the likely cause
	// and keeps passErr (see ExternalClient, and statusConflict).
	writeStatus := func(passErr error) error {
		started := time.Now()
		err := r.client.Status().Update(ctx, managed)
		r.recordStatusUpdate(ctx, started, err)
		if err == nil {
			return nil
		}
		if kerrors.IsConflict(err) {
			pass.conflict(tracing.StepStatus)
			return r.statusConflict(ctx, managed, read, err, passErr)
		}
		return errors.Wrap(err, errUpdateManagedStatus)
	}
	var started time.Time
	var err error

	record := r.record
	throttledRecorder := r.throttledRecorder
	log = log.WithValues(
		"uid", managed.GetUID(),
		"version", managed.GetResourceVersion(),
		"external-name", meta.GetExternalName(managed),
	)

	// Check the pause annotation and return if it has the value "true"
	// after logging, publishing an event and updating the SYNC status condition
	if meta.IsPaused(managed) {
		pass.event(tracing.EventReconcilePaused)
		log.Debug("Reconciliation is paused via the pause annotation", "annotation", meta.AnnotationKeyReconciliationPaused, "value", "true")
		record.Event(managed, event.Normal(reasonReconciliationPaused, actionProcessEvent, "Reconciliation is paused via the pause annotation"))
		managed.SetConditions(prv1.ReconcilePaused())
		// if the pause annotation is removed, we will have a chance to reconcile again and resume
		// and if status update fails, we will reconcile again to retry to update the status
		if err := writeStatus(nil); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	// If managed resource has a deletion timestamp and and a deletion policy of
	// Orphan, we do not need to observe the external resource before attempting
	// to remove finalizer.
	if meta.WasDeleted(managed) && !meta.ShouldDelete(managed) {
		log = log.WithValues("deletion-timestamp", managed.GetDeletionTimestamp())

		started = time.Now()
		err = r.managed.RemoveFinalizer(ctx, managed)
		r.recordFinalizerRemove(ctx, started, err)
		if err != nil {
			// If this is the first time we encounter this issue we'll be
			// requeued implicitly when we update our status with the new error
			// condition. If not, we requeue explicitly, which will trigger
			// backoff.
			log.Error(err, "Cannot remove managed resource finalizer")
			if kerrors.IsConflict(err) {
				pass.conflict(tracing.StepFinalizer)
				return rateLimitedRequeue(), nil
			}
			managed.SetConditions(prv1.Deleting(), prv1.ReconcileError(err))
			passErr := errors.Wrap(err, errUpdateManagedStatus)
			if err := writeStatus(passErr); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{}, passErr
		}
		pass.forget()

		// We've successfully unpublished our managed resource's connection
		// details and removed our finalizer. If we assume we were the only
		// controller that added a finalizer to this resource then it should no
		// longer exist and thus there is no point trying to update its status.
		log.Info("Successfully deleted managed resource")
		return reconcile.Result{}, nil
	}

	// We started creating an external resource but never recorded the outcome, so we may have lost
	// critical information — for example, if we didn't persist an updated external name we've leaked
	// a resource. We used to refuse outright here, with Requeue:false, which is terminal: nothing
	// re-triggers it and nothing changes, so the object could only ever be freed by a human deleting
	// the annotation (krateo-core-provider#110). That refusal also fired BEFORE Connect/Observe —
	// it was guessing at a question it never asked.
	//
	// Instead, carry the unresolved handshake forward and let Observe answer it below. The window
	// between SetExternalCreatePending and the terminal annotation is pure bookkeeping, measured at
	// well under a second on live resources (#88), so the overwhelmingly common interruption is one where the external work actually completed.
	// The marker the decision says to write now (its window closed, or the window is 0): past the
	// pause and Orphan checks, before anything acts and before pristine is taken (§6.2).
	if res, stop := r.writeMarkerIf(ctx, log, managed, pass, tracing.Now); stop {
		return res, nil
	}

	unresolvedCreate := meta.ExternalCreateIncomplete(managed)
	if unresolvedCreate {
		log.Debug("Create handshake is unresolved; asking Observe whether the external resource exists")
	}

	// pristine is the object as read, so a deferral that would write it back
	// unchanged can skip the status update. Taken only with a governor.
	// It's read when there is one, unless the marker was written above: that
	// write moved the version, and pristine must be the object after it.
	var pristine resource.Managed
	if r.governor != nil {
		pristine = read
		if pristine == nil || pristine.GetResourceVersion() != managed.GetResourceVersion() {
			pristine, _ = managed.DeepCopyObject().(resource.Managed)
		}
	}
	// deferred ends a pass whose next external call the governor deferred.
	deferred := func(d *ratelimit.Deferral) (reconcile.Result, error) {
		pass.deferred(d)
		return r.deferReconcile(log, managed, pristine, d, func() error {
			return writeStatus(nil)
		})
	}

	release, deferral := r.gate(ctx, log, r.external.keyer, managed)
	if deferral != nil {
		return deferred(deferral)
	}
	defer release() // a backstop if the call panics; release is idempotent
	started = time.Now()
	connectCtx, connectSpan := pass.step(externalCtx, tracing.SpanConnect)
	external, err := r.external.Connect(connectCtx, managed)
	pass.endStep(connectSpan, err)
	release()
	r.recordConnect(ctx, started, err)
	if err != nil {
		// We'll usually hit this case if our Provider or its secret are missing
		// or invalid. If this is first time we encounter this issue we'll be
		// requeued implicitly when we update our status with the new error
		// condition. If not, we requeue explicitly, which will trigger
		// backoff.
		log.Error(err, "Cannot connect to provider")
		if kerrors.IsConflict(err) {
			pass.conflict(tracing.StepConnect)
			return rateLimitedRequeue(), nil
		}
		throttledRecorder.Event(managed, event.Warning(reasonCannotConnect, actionProcessEvent, err))
		// Without a client nothing was observed, so the last Ready can no longer be vouched for.
		markNotObserved(managed, err)
		passErr := errors.Wrap(err, errReconcileConnect)
		managed.SetConditions(prv1.ReconcileError(passErr))
		if err := writeStatus(passErr); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, passErr
	}
	defer func() {
		if err := r.external.Disconnect(ctx); err != nil {
			log.Error(err, "Cannot disconnect from provider")
			throttledRecorder.Event(managed, event.Warning(reasonCannotDisconnect, actionProcessEvent, err))
		}
	}()

	// The client is gated on its own key, or on the connecter's.
	clientKeyer := keyerOf(external)
	if clientKeyer == nil {
		clientKeyer = r.external.keyer
	}

	release, deferral = r.gate(ctx, log, clientKeyer, managed)
	if deferral != nil {
		return deferred(deferral)
	}
	defer release() // a backstop if the call panics; release is idempotent
	started = time.Now()
	observeCtx, observeSpan := pass.step(externalCtx, tracing.SpanObserve)
	observation, err := external.Observe(observeCtx, managed)
	pass.endStep(observeSpan, err)
	release()
	r.recordObserve(ctx, started, err)
	if err != nil {
		// We'll usually hit this case if our Provider credentials are invalid
		// or insufficient for observing the external resource type we're
		// concerned with. If this is the first time we encounter this issue
		// we'll be requeued implicitly when we update our status with the new
		// error condition. If not, we requeue explicitly, which will
		// trigger backoff.
		log.Error(err, "Cannot observe external resource")
		if kerrors.IsConflict(err) {
			pass.conflict(tracing.StepObserve)
			return rateLimitedRequeue(), nil
		}
		throttledRecorder.Event(managed, event.Warning(reasonCannotObserve, actionObserveExternalResource, err))
		werr := errors.Wrap(err, errReconcileObserve)
		if unresolvedCreate {
			// An unresolved create handshake is resolved from Observe's answer, so it waits on
			// exactly this error. Name both on the object: what failed, and that a create is
			// pending behind it. Recovery resumes by itself once Observe answers; a permanent
			// Observe error is a provider defect, not something this reconciler can bound
			// without creating blind (oasgen-provider#266, core-provider#236).
			werr = errors.Wrapf(werr, errFmtCreateWaitsOnObserve, meta.GetExternalCreatePending(managed).Format(time.RFC3339))
		}
		// Observe could not answer, so the Ready it last reported is no longer known to hold.
		// Leaving it would show a stale True next to the error.
		markNotObserved(managed, err)
		managed.SetConditions(prv1.ReconcileError(werr))
		if err := writeStatus(werr); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, werr
	}

	// A successful Observe must set Ready. If it is still the NotObserved marker an earlier
	// failure left, this path of the provider's Observe never sets it. The runtime cannot
	// know the true Ready, so it does not invent one: it keeps the marker, which is honest,
	// and makes the breach loud so that it is fixed in the provider.
	//
	// A managed resource that is being deleted is exempt: on every deletion path below, the
	// runtime sets Ready=Deleting itself (or removes the finalizer and the object goes away), so
	// Ready is the runtime's to write there and a provider that leaves it unset breaks nothing.
	if !meta.WasDeleted(managed) && readyNotSetAfterObserve(managed) {
		log.Error(errors.New(errReadyNotSetAfterObserve), "Provider broke the Ready contract: a successful Observe must set Ready",
			"kind", r.kind, "namespace", managed.GetNamespace(), "name", managed.GetName())
		throttledRecorder.Event(managed, event.Warning(ReasonReadyNotSetAfterObserve, actionObserveExternalResource, errors.New(errReadyNotSetAfterObserve)))
	}

	// Resolve an unresolved create handshake now that Observe has answered the question the old
	// guard was only guessing at (krateo-core-provider#110).
	if unresolvedCreate {
		switch {
		case observation.ResourceExists:
			// Observe RESOLVED the external resource, so no identity was lost and nothing leaked —
			// the create did complete, only the bookkeeping was interrupted. Close the handshake
			// ourselves instead of waiting for a human to delete the annotation. Writing the
			// succeeded marker makes ExternalCreateIncomplete false from here on, because it
			// compares timestamps and succeeded is now the newest.
			log.Debug("Unresolved create handshake resolved: the external resource exists; adopting it")
			meta.SetExternalCreateSucceeded(managed, time.Now())
			if err := r.managed.UpdateCriticalAnnotations(ctx, managed); err != nil {
				log.Error(err, errUpdateManagedAnnotations)
				throttledRecorder.Event(managed, event.Warning(reasonCannotUpdateManaged, actionUpdateExternalResource, errors.Wrap(err, errUpdateManagedAnnotations)))
				return reconcile.Result{}, errors.Wrap(err, errUpdateManagedAnnotations)
			}

		case meta.WasDeleted(managed):
			// Being deleted and the external resource is gone: there is nothing left to leak, so
			// refusing here would only strand the object with its finalizer held — the failure mode
			// of krateo-core-provider#108. Fall through and let the delete path release it.
			log.Debug("Unresolved create handshake on a deleted resource with no external counterpart; proceeding with deletion")

		case meta.ExternalCreatePendingDuring(managed, r.createRecoveryGracePeriod):
			// Observe could NOT find it, but the create attempt is still RECENT. We cannot yet
			// distinguish "the create never reached the external system" (safe to retry) from "it
			// was created but we lost its external name" (retrying duplicates it), and an external
			// API that is eventually consistent may simply not be showing it yet.
			//
			// So confirm the negative before acting on it: keep the pending marker, keep refusing,
			// and re-observe. This is the conservative window — nothing is recreated inside it.
			log.Debug("Unresolved create handshake: external resource not observed yet, waiting out the recovery grace period",
				"pendingAge", time.Since(meta.GetExternalCreatePending(managed)).String(),
				"gracePeriod", r.createRecoveryGracePeriod.String())
			managed.SetConditions(prv1.Creating(), prv1.ReconcileError(errors.New(errCreateIncomplete)))
			// The pass returns nil here, but errCreateIncomplete is the error it reports, so a
			// conflict keeps it.
			if err := writeStatus(errors.New(errCreateIncomplete)); err != nil {
				return reconcile.Result{}, err
			}
			// The one path that writes ReconcileError and returns nil: it marks the root itself (§7).
			pass.markError(errors.New(errCreateIncomplete))
			return reconcile.Result{RequeueAfter: r.createRecoveryReobserveInterval}, nil

		default:
			// The grace period has FULLY elapsed and the external resource is still not observable.
			// An eventually-consistent API has had its window; a create that had landed would be
			// visible by now. So the create genuinely did not reach the external system, and
			// retrying is safe.
			//
			// The old behaviour refused here permanently, with `Requeue: false`. That was safe
			// against duplication and unsafe against everything else: the resource was terminal,
			// the ambiguity was never resolved, and the only documented remedy was for a human to
			// delete the annotation by hand — which papers over the defect and destroys the evidence
			// that any fix works. Six Repo CRs sat in exactly that state for over a day
			// (krateo-platformops/git-provider#22).
			//
			// Clearing the pending marker lets the normal create path run again on the next pass.
			// Note this does NOT relax the duplication guard — it relocates it into the window
			// above, where it is bounded by evidence instead of holding forever on an ambiguity
			// that nothing will ever resolve.
			log.Debug("Unresolved create handshake: recovery grace period elapsed with the external resource still absent; treating the create as never landed and retrying",
				"pendingAge", time.Since(meta.GetExternalCreatePending(managed)).String(),
				"gracePeriod", r.createRecoveryGracePeriod.String())
			meta.RemoveAnnotations(managed, meta.AnnotationKeyExternalCreatePending)

			// A plain Update, deliberately NOT UpdateCriticalAnnotations. That helper captures the
			// runtime's own keys (external-name, the create-* markers and the trace marker),
			// re-Gets the object from the API server, and then ADDS them back. So an annotation
			// removed locally is restored by the re-Get and written straight back. It can add and
			// overwrite; it cannot delete. Using it here would leave the marker in place and the
			// resource wedged exactly as before, with nothing failing to indicate it.
			if err := r.client.Update(ctx, managed); err != nil {
				if kerrors.IsConflict(err) {
					// Someone else wrote first; re-read and retry rather than fight over it.
					pass.conflict(tracing.StepHandshake)
					return rateLimitedRequeue(), nil
				}
				log.Error(err, errUpdateManagedAnnotations)
				throttledRecorder.Event(managed, event.Warning(reasonCannotUpdateManaged, actionUpdateExternalResource, errors.Wrap(err, errUpdateManagedAnnotations)))
				return reconcile.Result{}, errors.Wrap(err, errUpdateManagedAnnotations)
			}
		}
	}

	// In the observe-only mode, !observation.ResourceExists will be an error
	// case, and we will explicitly return this information to the user.
	if !observation.ResourceExists && meta.ShouldOnlyObserve(managed) {
		throttledRecorder.Event(managed, event.Warning(reasonCannotObserve, actionObserveExternalResource, errors.New(errExternalResourceNotExist)))
		passErr := errors.Wrap(errors.New(errExternalResourceNotExist), errReconcileObserve)
		managed.SetConditions(prv1.ReconcileError(passErr))
		if err := writeStatus(passErr); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, passErr
	}

	// If this resource has a non-zero creation grace period we want to wait
	// for that period to expire before we trust that the resource really
	// doesn't exist. This is because some external APIs are eventually
	// consistent and may report that a recently created resource does not
	// exist.
	if !observation.ResourceExists && meta.ExternalCreateSucceededDuring(managed, r.creationGracePeriod) {
		log.Debug("Waiting for external resource existence to be confirmed")
		record.Event(managed, event.Normal(reasonPending, actionProcessEvent, "Waiting for external resource existence to be confirmed"))
		return rateLimitedRequeue(), nil
	}

	if meta.WasDeleted(managed) {
		log = log.WithValues("deletion-timestamp", managed.GetDeletionTimestamp())

		// We'll only reach this point if deletion policy is not orphan, so we
		// are safe to call external deletion if external resource exists.
		if observation.ResourceExists && meta.ShouldDelete(managed) {
			release, deferral = r.gate(ctx, log, clientKeyer, managed)
			if deferral != nil {
				return deferred(deferral)
			}
			defer release() // a backstop if the call panics; release is idempotent
			if res, stop := r.writeMarkerIf(ctx, log, managed, pass, tracing.BeforeFirstAction); stop {
				return res, nil
			}
			started = time.Now()
			deleteCtx, deleteSpan := pass.actStep(externalCtx, tracing.SpanDelete)
			err = external.Delete(deleteCtx, managed)
			pass.endStep(deleteSpan, err)
			release()
			r.recordDelete(ctx, started, err)
			if err != nil {
				// We'll hit this condition if we can't delete our external
				// resource, for example if our provider credentials don't have
				// access to delete it. If this is the first time we encounter
				// this issue we'll be requeued implicitly when we update our
				// status with the new error condition. If not, we want requeue
				// explicitly, which will trigger backoff.
				log.Error(err, "Cannot delete external resource")
				throttledRecorder.Event(managed, event.Warning(reasonCannotDelete, actionDeleteExternalResource, err))
				passErr := errors.Wrap(err, errReconcileDelete)
				managed.SetConditions(prv1.Deleting(), prv1.ReconcileError(passErr))
				if err := writeStatus(passErr); err != nil {
					return reconcile.Result{}, err
				}
				return reconcile.Result{}, passErr
			}

			// We've successfully requested deletion of our external resource.
			// We queue another reconcile after a short wait rather than
			// immediately finalizing our delete in order to verify that the
			// external resource was actually deleted. If it no longer exists
			// we'll skip this block on the next reconcile and proceed to
			// unpublish and finalize. If it still exists we'll re-enter this
			// block and try again.
			log.Info("Successfully requested deletion of external resource")
			record.Event(managed, event.Normal(reasonDeleted, actionDeleteEvent, "Successfully requested deletion of external resource"))
			managed.SetConditions(prv1.Deleting(), prv1.ReconcileSuccess())
			if err := writeStatus(nil); err != nil {
				return reconcile.Result{}, err
			}
			return rateLimitedRequeue(), nil
		}

		started = time.Now()
		err = r.managed.RemoveFinalizer(ctx, managed)
		r.recordFinalizerRemove(ctx, started, err)
		if err != nil {
			// If this is the first time we encounter this issue we'll be
			// requeued implicitly when we update our status with the new error
			// condition. If not, we requeue explicitly, which will trigger
			// backoff.
			log.Error(err, "Cannot remove managed resource finalizer")
			if kerrors.IsConflict(err) {
				pass.conflict(tracing.StepFinalizer)
				return rateLimitedRequeue(), nil
			}
			managed.SetConditions(prv1.Deleting(), prv1.ReconcileError(err))
			passErr := errors.Wrap(err, errUpdateManagedStatus)
			if err := writeStatus(passErr); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{}, passErr
		}
		pass.forget()

		// We've successfully deleted our external resource (if necessary) and
		// removed our finalizer. If we assume we were the only controller that
		// added a finalizer to this resource then it should no longer exist and
		// thus there is no point trying to update its status.
		log.Info("Successfully deleted managed resource")
		return reconcile.Result{}, nil
	}

	started = time.Now()
	err = r.managed.AddFinalizer(ctx, managed)
	r.recordFinalizerAdd(ctx, started, err)
	if err != nil {
		// If this is the first time we encounter this issue we'll be requeued
		// implicitly when we update our status with the new error condition. If
		// not, we requeue explicitly, which will trigger backoff.
		log.Error(err, "Cannot add managed resource finalizer")
		if kerrors.IsConflict(err) {
			pass.conflict(tracing.StepFinalizer)
			return rateLimitedRequeue(), nil
		}
		managed.SetConditions(prv1.ReconcileError(err))
		passErr := errors.Wrap(err, errCreateIncomplete)
		if err := writeStatus(passErr); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, passErr
	}

	if !observation.ResourceExists && meta.ShouldCreate(managed) {
		// Ask the gate before the create-pending annotation is written: a
		// deferral after it would leave an unresolved create handshake behind
		// for a create that was never attempted.
		releaseCreate, deferral := r.gate(ctx, log, clientKeyer, managed)
		if deferral != nil {
			return deferred(deferral)
		}
		defer releaseCreate() // also a backstop if Create panics; release is idempotent

		// The marker goes in its own locked patch, before create-pending: a conflict there ends the
		// pass before any write that starts a create handshake (§6.2).
		if res, stop := r.writeMarkerIf(ctx, log, managed, pass, tracing.BeforeFirstAction); stop {
			return res, nil
		}

		// We write this annotation for two reasons. Firstly, it helps
		// us to detect the case in which we fail to persist critical
		// information (like the external name) that may be set by the
		// subsequent external.Create call. Secondly, it guarantees that
		// we're operating on the latest version of our resource. We
		// don't use the CriticalAnnotationUpdater because we _want_ the
		// update to fail if we get a 409 due to a stale version.
		meta.SetExternalCreatePending(managed, time.Now())
		if err := r.client.Update(ctx, managed); err != nil {
			log.Error(err, "Cannot update managed resource")
			if kerrors.IsConflict(err) {
				pass.conflict(tracing.StepCreatePending)
				return rateLimitedRequeue(), nil
			}
			throttledRecorder.Event(managed, event.Warning(reasonCannotUpdateManaged, actionUpdateExternalResource, errors.Wrap(err, errUpdateManaged)))
			passErr := errors.Wrap(err, errUpdateManaged)
			managed.SetConditions(prv1.Creating(), prv1.ReconcileError(passErr))
			if err := writeStatus(passErr); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{}, passErr
		}

		started = time.Now()
		createCtx, createSpan := pass.actStep(externalCtx, tracing.SpanCreate)
		err = external.Create(createCtx, managed)
		pass.endStep(createSpan, err)
		releaseCreate()
		r.recordCreate(ctx, started, err)
		if err != nil {
			// We'll hit this condition if we can't create our external
			// resource, for example if our provider credentials don't have
			// access to create it. If this is the first time we encounter this
			// issue we'll be requeued implicitly when we update our status with
			// the new error condition. If not, we requeue explicitly, which will trigger backoff.
			log.Error(err, "Cannot create external resource")

			// We handle annotations specially here because it's
			// critical that they are persisted to the API server.
			// If we don't add the external-create-failed annotation
			// the reconciler will refuse to proceed, because it
			// won't know whether or not it created an external
			// resource.
			//
			// This must happen for EVERY create error, conflicts included. We used to requeue on a
			// conflict before stamping, which left external-create-pending as the newest marker, so
			// every later reconcile refused with errCreateIncomplete until a human removed it
			// (krateo-platformops/oasgen-provider#254). The critical annotation updater re-reads
			// the object before writing, so a stale resourceVersion does not stop the stamp.
			meta.SetExternalCreateFailed(managed, time.Now())
			if err := r.managed.UpdateCriticalAnnotations(ctx, managed); err != nil {
				log.Error(err, errUpdateManagedAnnotations)
				throttledRecorder.Event(managed, event.Warning(reasonCannotUpdateManaged, actionUpdateExternalResource, errors.Wrap(err, errUpdateManagedAnnotations)))

				// We only log and emit an event here rather
				// than setting a status condition and returning
				// early because presumably it's more useful to
				// set our status condition to the reason the
				// create failed.
			}

			// A conflict is transient: someone else wrote the object first. Retry promptly rather
			// than report it as a create failure; the next pass observes before it creates again.
			if kerrors.IsConflict(err) {
				pass.conflict(tracing.StepCreate)
				return rateLimitedRequeue(), nil
			}
			throttledRecorder.Event(managed, event.Warning(reasonCannotCreate, actionCreateExternalResource, err))

			passErr := errors.Wrap(err, errReconcileCreate)
			managed.SetConditions(prv1.Creating(), prv1.ReconcileError(passErr))
			if err := writeStatus(passErr); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{}, passErr
		}

		// In some cases our external-name may be set by Create above.
		log = log.WithValues("external-name", meta.GetExternalName(managed))
		record = r.record

		// We handle annotations specially here because it's critical
		// that they are persisted to the API server. If we don't remove
		// add the external-create-succeeded annotation the reconciler
		// will refuse to proceed, because it won't know whether or not
		// it created an external resource. This is also important in
		// cases where we must record an external-name annotation set by
		// the Create call. The updater writes back only the runtime's own
		// keys (external-name, the create-* markers and the trace marker),
		// onto a fresh read. Any other change made during Create, other
		// annotations included, is reset to the API server's copy; at the
		// time of writing Create implementations are advised not to alter
		// status, but we may revisit this in future.
		meta.SetExternalCreateSucceeded(managed, time.Now())
		if err := r.managed.UpdateCriticalAnnotations(ctx, managed); err != nil {
			log.Error(err, errUpdateManagedAnnotations)
			throttledRecorder.Event(managed, event.Warning(reasonCannotUpdateManaged, actionUpdateExternalResource, errors.Wrap(err, errUpdateManagedAnnotations)))
			passErr := errors.Wrap(err, errUpdateManagedAnnotations)
			managed.SetConditions(prv1.Creating(), prv1.ReconcileError(passErr))
			if err := writeStatus(passErr); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{}, passErr
		}

		// We've successfully created our external resource. In many cases the
		// creation process takes a little time to finish. We requeue explicitly
		// order to observe the external resource to determine whether it's
		// ready for use.
		log.Info("Successfully requested creation of external resource")
		record.Event(managed, event.Normal(reasonCreated, actionCreateEvent, "Successfully requested creation of external resource"))
		managed.SetConditions(prv1.Creating(), prv1.ReconcileSuccess())
		if err := writeStatus(nil); err != nil {
			return reconcile.Result{}, err
		}
		return rateLimitedRequeue(), nil
	}

	if observation.ResourceLateInitialized {
		// Note that this update may reset any pending updates to the status of
		// the managed resource from when it was observed above. This is because
		// the API server replies to the update with its unchanged view of the
		// resource's status, which is subsequently deserialized into managed.
		// This is usually tolerable because the update will implicitly requeue
		// an immediate reconcile which should re-observe the external resource
		// and persist its status.
		if err := r.client.Update(ctx, managed); err != nil {
			// A conflict is transient: someone else wrote the object first. Requeue promptly and
			// late-initialize again from a fresh read, as every other write to the managed resource
			// does, rather than report a ReconcileError and wait out error backoff.
			if kerrors.IsConflict(err) {
				pass.conflict(tracing.StepLateInit)
				return rateLimitedRequeue(), nil
			}
			log.Error(err, "Cannot update managed resource")
			throttledRecorder.Event(managed, event.Warning(reasonCannotUpdateManaged, actionUpdateExternalResource, err))
			passErr := errors.Wrap(err, errUpdateManaged)
			managed.SetConditions(prv1.ReconcileError(passErr))
			if err := writeStatus(passErr); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{}, passErr
		}
	}

	if observation.ResourceUpToDate {
		// We did not need to create, update, or delete our external resource.
		// Per the below issue nothing will notify us if and when the external
		// resource we manage changes, so we requeue a speculative reconcile
		// after the specified poll interval in order to observe it and react
		// accordingly.
		// https://github.com/crossplane/crossplane/issues/289
		reconcileAfter := r.pollIntervalHook(managed, r.pollInterval)
		log.Info("External resource is up to date", "requeue-after", time.Now().Add(reconcileAfter))
		managed.SetConditions(prv1.ReconcileSuccess())
		if err := writeStatus(nil); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{RequeueAfter: reconcileAfter}, nil
	}

	if observation.Diff != "" {
		log.Debug("External resource differs from desired state", "diff", observation.Diff)
	}

	// skip the update if the management policy is set to ignore updates
	if !meta.ShouldUpdate(managed) {
		reconcileAfter := r.pollIntervalHook(managed, r.pollInterval)
		log.Debug("Skipping update due to managementPolicies. Reconciliation succeeded", "requeue-after", time.Now().Add(reconcileAfter))
		managed.SetConditions(prv1.ReconcileSuccess())
		if err := writeStatus(nil); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{RequeueAfter: reconcileAfter}, nil
	}

	release, deferral = r.gate(ctx, log, clientKeyer, managed)
	if deferral != nil {
		return deferred(deferral)
	}
	defer release() // a backstop if the call panics; release is idempotent
	if res, stop := r.writeMarkerIf(ctx, log, managed, pass, tracing.BeforeFirstAction); stop {
		return res, nil
	}
	started = time.Now()
	updateCtx, updateSpan := pass.actStep(externalCtx, tracing.SpanUpdate)
	err = external.Update(updateCtx, managed)
	pass.endStep(updateSpan, err)
	release()
	r.recordUpdate(ctx, started, err)
	if err != nil {
		// We'll hit this condition if we can't update our external resource,
		// for example if our provider credentials don't have access to update
		// it. If this is the first time we encounter this issue we'll be
		// requeued implicitly when we update our status with the new error
		// condition. If not, we requeue explicitly, which will trigger backoff.
		log.Error(err, "Cannot update external resource")
		throttledRecorder.Event(managed, event.Warning(reasonCannotUpdate, actionUpdateExternalResource, err))
		passErr := errors.Wrap(err, errReconcileUpdate)
		managed.SetConditions(prv1.ReconcileError(passErr))
		if err := writeStatus(passErr); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, passErr
	}

	// We've successfully updated our external resource. Per the below issue
	// nothing will notify us if and when the external resource we manage
	// changes, so we requeue a speculative reconcile after the specified poll
	// interval in order to observe it and react accordingly.
	// https://github.com/crossplane/crossplane/issues/289
	reconcileAfter := r.pollIntervalHook(managed, r.pollInterval)
	log.Info("Successfully requested update of external resource", "requeue-after", time.Now().Add(reconcileAfter))
	record.Event(managed, event.Normal(reasonUpdated, actionUpdateEvent, "Successfully requested update of external resource"))
	managed.SetConditions(prv1.ReconcileSuccess())
	if err := writeStatus(nil); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{RequeueAfter: reconcileAfter}, nil
}
