package reconciler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/kubeutil/event"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/errors"
	"github.com/krateo-platformops/provider-runtime/pkg/logging"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
	"github.com/krateo-platformops/provider-runtime/pkg/resource/fake"
	"github.com/krateo-platformops/provider-runtime/pkg/test"
)

// eventCapture is an event.Recorder that keeps what it is given.
type eventCapture struct{ events []event.Event }

func (c *eventCapture) Event(_ runtime.Object, e event.Event) { c.events = append(c.events, e) }

func (c *eventCapture) withReason(r event.Reason) []event.Event {
	var out []event.Event
	for _, e := range c.events {
		if e.Reason == r {
			out = append(out, e)
		}
	}
	return out
}

// errorLogCapture is a logging.Logger that keeps its Error calls, with the values bound by
// WithValues, and drops everything else.
type errorLogCapture struct {
	values []any
	errors *[]loggedError
}

type loggedError struct {
	err    error
	msg    string
	values map[string]any
}

func newErrorLogCapture() errorLogCapture { return errorLogCapture{errors: &[]loggedError{}} }

func (l errorLogCapture) Info(string, ...any)  {}
func (l errorLogCapture) Debug(string, ...any) {}
func (l errorLogCapture) Warn(string, ...any)  {}
func (l errorLogCapture) Error(err error, msg string, kv ...any) {
	all := append(append([]any{}, l.values...), kv...)
	values := map[string]any{}
	for i := 0; i+1 < len(all); i += 2 {
		values[fmt.Sprint(all[i])] = all[i+1]
	}
	*l.errors = append(*l.errors, loggedError{err: err, msg: msg, values: values})
}
func (l errorLogCapture) WithValues(kv ...any) logging.Logger {
	return errorLogCapture{values: append(append([]any{}, l.values...), kv...), errors: l.errors}
}
func (l errorLogCapture) WithName(string) logging.Logger { return l }

// readyRun is what one Reconcile did, for the Ready contract tests.
type readyRun struct {
	result reconcile.Result
	err    error

	// statusUpdates counts status writes; persisted is the object as last written. A test that
	// asserts on persisted must first assert statusUpdates, or it cannot tell an unchanged
	// condition from a write that never happened.
	statusUpdates int
	persisted     *fake.Managed
	// inMemory is the object the reconciler worked on, for paths that write nothing.
	inMemory *fake.Managed
	// creates counts calls to the external client's Create.
	creates int

	events *eventCapture
	log    errorLogCapture
}

type readySetup struct {
	// start is what the API server holds before the reconcile.
	start   []prv1.Condition
	deleted bool

	connectErr error
	observe    func(mg resource.Managed) (ExternalObservation, error)
}

func reconcileReady(t *testing.T, s readySetup) readyRun {
	t.Helper()
	run := readyRun{events: &eventCapture{}, log: newErrorLogCapture()}
	deletedAt := metav1.Now()

	mc := &test.MockClient{
		MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
			mg := obj.(*fake.Managed)
			mg.SetName("thing")
			mg.SetNamespace("ns")
			mg.SetConditions(s.start...)
			if s.deleted {
				mg.SetDeletionTimestamp(&deletedAt)
				mg.SetFinalizers([]string{FinalizerName})
			}
			run.inMemory = mg
			return nil
		}),
		MockUpdate: test.NewMockUpdateFn(nil),
		MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
			run.statusUpdates++
			run.persisted = obj.(*fake.Managed).DeepCopyObject().(*fake.Managed)
			return nil
		}),
	}

	r := NewReconciler(&fake.Manager{Client: mc, Scheme: fake.SchemeWith(&fake.Managed{})},
		resource.ManagedKind(fake.GVK(&fake.Managed{})),
		WithLogger(run.log),
		WithRecorder(run.events),
		WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, _ resource.Managed) (ExternalClient, error) {
			if s.connectErr != nil {
				return nil, s.connectErr
			}
			return &ExternalClientFns{
				ObserveFn: func(_ context.Context, mg resource.Managed) (ExternalObservation, error) {
					return s.observe(mg)
				},
				CreateFn: func(_ context.Context, _ resource.Managed) error {
					run.creates++
					return nil
				},
				DeleteFn: func(_ context.Context, _ resource.Managed) error { return nil },
			}, nil
		})),
		WithFinalizer(resource.FinalizerFns{
			AddFinalizerFn:    func(_ context.Context, _ resource.Object) error { return nil },
			RemoveFinalizerFn: func(_ context.Context, _ resource.Object) error { return nil },
		}),
	)

	run.result, run.err = r.Reconcile(context.Background(), reconcile.Request{})
	return run
}

// wantCondition fails t unless got has want's type, status and reason (and message, when want
// has one).
func wantCondition(t *testing.T, got prv1.Condition, want prv1.Condition) {
	t.Helper()
	if got.Type != want.Type || got.Status != want.Status || got.Reason != want.Reason ||
		(want.Message != "" && got.Message != want.Message) {
		t.Errorf("%s: want %s/%s %q, got %s/%s %q",
			want.Type, want.Status, want.Reason, want.Message, got.Status, got.Reason, got.Message)
	}
}

// mustHaveWrittenStatus is the denominator for every assertion on the persisted object.
func mustHaveWrittenStatus(t *testing.T, run readyRun) {
	t.Helper()
	if run.statusUpdates != 1 {
		t.Fatalf("want exactly 1 status update, got %d", run.statusUpdates)
	}
}

// A Connect or Observe error leaves the runtime unable to vouch for the last Ready. Before
// v1.8.0 only Synced changed, so Ready could keep showing a stale True next to the error.
func TestReadyNotObservedOnError(t *testing.T) {
	errBoom := errors.New("boom")
	available := []prv1.Condition{prv1.Available(), prv1.ReconcileSuccess()}

	cases := map[string]struct {
		setup      readySetup
		wantSynced string
	}{
		"ObserveError": {
			setup: readySetup{
				start:   available,
				observe: func(resource.Managed) (ExternalObservation, error) { return ExternalObservation{}, errBoom },
			},
			wantSynced: errors.Wrap(errBoom, errReconcileObserve).Error(),
		},
		"ConnectError": {
			setup:      readySetup{start: available, connectErr: errBoom},
			wantSynced: errors.Wrap(errBoom, errReconcileConnect).Error(),
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			run := reconcileReady(t, tc.setup)

			if run.err == nil {
				t.Errorf("r.Reconcile(...): want the error returned for backoff, got nil")
			}
			mustHaveWrittenStatus(t, run)
			wantCondition(t, run.persisted.GetCondition(prv1.TypeReady), prv1.NotObserved())
			wantCondition(t, run.persisted.GetCondition(prv1.TypeSynced),
				prv1.ReconcileError(errors.New(tc.wantSynced)))
		})
	}
}

// The marker's message is constant, so a repeated failure with a different error text does not
// rewrite Ready or move its LastTransitionTime; the changing text is on Synced.
func TestReadyNotObservedIsStableAcrossErrors(t *testing.T) {
	earlier := prv1.NotObserved()
	// Whole seconds, the precision a status write keeps.
	earlier.LastTransitionTime = metav1.NewTime(earlier.LastTransitionTime.Add(-time.Hour)).Rfc3339Copy()

	run := reconcileReady(t, readySetup{
		start: []prv1.Condition{earlier, prv1.ReconcileError(errors.New("first error"))},
		observe: func(resource.Managed) (ExternalObservation, error) {
			return ExternalObservation{}, errors.New("a different error")
		},
	})

	mustHaveWrittenStatus(t, run)
	got := run.persisted.GetCondition(prv1.TypeReady)
	wantCondition(t, got, prv1.NotObserved())
	if !got.LastTransitionTime.Equal(&earlier.LastTransitionTime) {
		t.Errorf("Ready.lastTransitionTime moved on a repeated failure: want %v, got %v",
			earlier.LastTransitionTime, got.LastTransitionTime)
	}
}

// Deleting stays true whatever the external system answers, so an Observe error during a
// delete must not replace it.
func TestReadyDeletingSurvivesObserveError(t *testing.T) {
	errBoom := errors.New("boom")
	run := reconcileReady(t, readySetup{
		start:   []prv1.Condition{prv1.Deleting(), prv1.ReconcileSuccess()},
		deleted: true,
		observe: func(resource.Managed) (ExternalObservation, error) { return ExternalObservation{}, errBoom },
	})

	mustHaveWrittenStatus(t, run)
	wantCondition(t, run.persisted.GetCondition(prv1.TypeReady), prv1.Deleting())
	wantCondition(t, run.persisted.GetCondition(prv1.TypeSynced),
		prv1.ReconcileError(errors.Wrap(errBoom, errReconcileObserve)))
}

// A resource being deleted whose Observe fails before writing Ready: its fate is not in doubt,
// so the runtime answers Ready=Deleting itself rather than NotObserved (v1.8.2). Before, a
// provider had to remember to set Deleting before every error on its deletion path.
func TestReadyDeletingOnObserveErrorWhileDeleted(t *testing.T) {
	errBoom := errors.New("boom")
	run := reconcileReady(t, readySetup{
		start:   []prv1.Condition{prv1.Available(), prv1.ReconcileSuccess()},
		deleted: true,
		observe: func(resource.Managed) (ExternalObservation, error) { return ExternalObservation{}, errBoom },
	})

	mustHaveWrittenStatus(t, run)
	wantCondition(t, run.persisted.GetCondition(prv1.TypeReady), prv1.Deleting())
	wantCondition(t, run.persisted.GetCondition(prv1.TypeSynced),
		prv1.ReconcileError(errors.Wrap(errBoom, errReconcileObserve)))
}

// The same for a Connect failure on a resource being deleted.
func TestReadyDeletingOnConnectErrorWhileDeleted(t *testing.T) {
	run := reconcileReady(t, readySetup{
		start:      []prv1.Condition{prv1.Available(), prv1.ReconcileSuccess()},
		deleted:    true,
		connectErr: errors.New("no credentials"),
	})

	mustHaveWrittenStatus(t, run)
	wantCondition(t, run.persisted.GetCondition(prv1.TypeReady), prv1.Deleting())
	if got := run.persisted.GetCondition(prv1.TypeSynced); got.Status != metav1.ConditionFalse || got.Reason != prv1.ReasonReconcileError {
		t.Errorf("Synced: want False/ReconcileError, got %s/%s", got.Status, got.Reason)
	}
}

// ReadyAssessed still wins on a resource being deleted: the provider said it set Ready.
func TestReadyAssessedWinsWhileDeleted(t *testing.T) {
	run := reconcileReady(t, readySetup{
		start:   []prv1.Condition{prv1.Available(), prv1.ReconcileSuccess()},
		deleted: true,
		observe: func(mg resource.Managed) (ExternalObservation, error) {
			mg.SetConditions(prv1.Unavailable())
			return ExternalObservation{}, ReadyAssessed(errors.New("cannot update status"))
		},
	})

	mustHaveWrittenStatus(t, run)
	wantCondition(t, run.persisted.GetCondition(prv1.TypeReady), prv1.Unavailable())
}

// The alert-provider Alert and incident-provider IncidentApply shape: Observe reads the external
// resource, sets Ready (here to the value it already had, so no before/after comparison could
// tell that it did), and then fails on something else. ReadyAssessed says so, and Ready stays.
func TestReadyAssessedErrorKeepsReady(t *testing.T) {
	errWrite := errors.New("cannot update status")
	run := reconcileReady(t, readySetup{
		start: []prv1.Condition{prv1.Available(), prv1.ReconcileSuccess()},
		observe: func(mg resource.Managed) (ExternalObservation, error) {
			mg.SetConditions(prv1.Available())
			return ExternalObservation{}, ReadyAssessed(errWrite)
		},
	})

	if run.err == nil {
		t.Errorf("r.Reconcile(...): want the error returned for backoff, got nil")
	}
	mustHaveWrittenStatus(t, run)
	wantCondition(t, run.persisted.GetCondition(prv1.TypeReady), prv1.Available())
	// ReadyAssessed changes nothing about how the error is reported.
	wantCondition(t, run.persisted.GetCondition(prv1.TypeSynced),
		prv1.ReconcileError(errors.Wrap(errWrite, errReconcileObserve)))
}

// After a NotObserved, a successful Observe that sets Ready is the contract working: Ready is
// the provider's answer and nothing is reported.
func TestReadyRecoveredByObserve(t *testing.T) {
	run := reconcileReady(t, readySetup{
		start: []prv1.Condition{prv1.NotObserved(), prv1.ReconcileError(errors.New("boom"))},
		observe: func(mg resource.Managed) (ExternalObservation, error) {
			mg.SetConditions(prv1.Available())
			return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
		},
	})

	if run.err != nil {
		t.Errorf("r.Reconcile(...): %v", run.err)
	}
	mustHaveWrittenStatus(t, run)
	wantCondition(t, run.persisted.GetCondition(prv1.TypeReady), prv1.Available())
	wantCondition(t, run.persisted.GetCondition(prv1.TypeSynced), prv1.ReconcileSuccess())
	for _, e := range run.events.events {
		if e.Type == event.TypeWarning {
			t.Errorf("want no Warning event, got %s: %s", e.Reason, e.Message)
		}
	}
	if len(*run.log.errors) != 0 {
		t.Errorf("want no Error log, got %v", *run.log.errors)
	}
}

// A successful Observe that leaves Ready untouched while it is NotObserved breaks the contract.
// The runtime cannot know the true Ready, so it keeps the marker and makes the breach loud.
func TestReadyNotSetAfterObserveIsReported(t *testing.T) {
	run := reconcileReady(t, readySetup{
		start: []prv1.Condition{prv1.NotObserved(), prv1.ReconcileError(errors.New("boom"))},
		observe: func(resource.Managed) (ExternalObservation, error) {
			return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
		},
	})

	if run.err != nil {
		t.Errorf("r.Reconcile(...): %v", run.err)
	}
	mustHaveWrittenStatus(t, run)
	wantCondition(t, run.persisted.GetCondition(prv1.TypeReady), prv1.NotObserved())
	// The breach is the provider's; the reconcile itself succeeded.
	wantCondition(t, run.persisted.GetCondition(prv1.TypeSynced), prv1.ReconcileSuccess())

	got := run.events.withReason(ReasonReadyNotSetAfterObserve)
	if len(got) != 1 || got[0].Type != event.TypeWarning || got[0].Message != errReadyNotSetAfterObserve {
		t.Errorf("want one Warning event %s with the contract message, got %+v (all events: %+v)",
			ReasonReadyNotSetAfterObserve, got, run.events.events)
	}

	wantKind := schema.GroupVersionKind(fake.GVK(&fake.Managed{})).GroupKind().String()
	var found bool
	for _, l := range *run.log.errors {
		if l.err != nil && l.err.Error() == errReadyNotSetAfterObserve {
			found = true
			if l.values["kind"] != wantKind || l.values["namespace"] != "ns" || l.values["name"] != "thing" {
				t.Errorf("the breach log must name the kind and object: want kind=%s ns/thing, got %v", wantKind, l.values)
			}
		}
	}
	if !found {
		t.Errorf("want an Error log for the breach, got %v", *run.log.errors)
	}
}

// A managed resource being deleted is exempt from the breach report: the runtime's deletion
// branch sets Ready=Deleting itself, so a provider whose Observe leaves Ready unset on that
// path breaks nothing, and reporting it would be a false alarm on every delete after an outage.
func TestReadyNotSetAfterObserveExemptsDeletion(t *testing.T) {
	run := reconcileReady(t, readySetup{
		start:   []prv1.Condition{prv1.NotObserved(), prv1.ReconcileError(errors.New("boom"))},
		deleted: true,
		observe: func(resource.Managed) (ExternalObservation, error) {
			return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
		},
	})

	if run.err != nil {
		t.Errorf("r.Reconcile(...): %v", run.err)
	}
	mustHaveWrittenStatus(t, run)
	// The runtime, not the provider, answers Ready while it deletes.
	wantCondition(t, run.persisted.GetCondition(prv1.TypeReady), prv1.Deleting())
	if got := run.events.withReason(ReasonReadyNotSetAfterObserve); len(got) != 0 {
		t.Errorf("a deleted resource must not be reported as a Ready-contract breach, got %+v", got)
	}
	for _, l := range *run.log.errors {
		if l.err != nil && l.err.Error() == errReadyNotSetAfterObserve {
			t.Errorf("a deleted resource must not log a Ready-contract breach, got %v", l.values)
		}
	}
}

// An Observe that fails must never lead to Create, even when its observation says the resource
// does not exist: "I could not look" is not "it is not there". Creating on that answer could
// duplicate an external resource that exists but could not be read. The reconciler returns on
// the error before the create branch; this pins it explicitly rather than relying on a nil
// CreateFn panicking.
func TestObserveErrorNeverCreates(t *testing.T) {
	run := reconcileReady(t, readySetup{
		start: []prv1.Condition{prv1.Available(), prv1.ReconcileSuccess()},
		observe: func(resource.Managed) (ExternalObservation, error) {
			return ExternalObservation{ResourceExists: false}, errors.New("cannot reach the external API")
		},
	})

	mustHaveWrittenStatus(t, run)
	if run.creates != 0 {
		t.Errorf("Create must not run after a failed Observe; it ran %d time(s)", run.creates)
	}
	wantCondition(t, run.persisted.GetCondition(prv1.TypeReady), prv1.NotObserved())
	if run.err == nil {
		t.Errorf("r.Reconcile(...): want the Observe error, got nil")
	}
}

// Control for TestObserveErrorNeverCreates: the same observation without the error does create,
// so a zero there is the reconciler's choice and not a harness that can never call Create.
func TestObserveNotFoundCreates(t *testing.T) {
	run := reconcileReady(t, readySetup{
		start: []prv1.Condition{prv1.ReconcileSuccess()},
		observe: func(mg resource.Managed) (ExternalObservation, error) {
			mg.SetConditions(prv1.Unavailable())
			return ExternalObservation{ResourceExists: false}, nil
		},
	})

	if run.creates != 1 {
		t.Errorf("a successful Observe reporting ResourceExists=false must lead to exactly one Create; got %d", run.creates)
	}
}

// A resource-version conflict from Observe is transient: requeue at once, write nothing, and
// leave the conditions as they were.
func TestReadyUntouchedOnObserveConflict(t *testing.T) {
	conflict := kerrors.NewConflict(schema.GroupResource{Group: "example.krateo.io", Resource: "things"}, "x",
		errors.New("the object has been modified"))
	run := reconcileReady(t, readySetup{
		start:   []prv1.Condition{prv1.Available(), prv1.ReconcileSuccess()},
		observe: func(resource.Managed) (ExternalObservation, error) { return ExternalObservation{}, conflict },
	})

	if run.err != nil {
		t.Errorf("r.Reconcile(...): a conflict must not return an error (that means error backoff); got %v", run.err)
	}
	if run.result != rateLimitedRequeue() {
		t.Errorf("r.Reconcile(...): want Requeue, got %+v", run.result)
	}
	if run.statusUpdates != 0 {
		t.Errorf("a conflict must requeue without writing status; got %d status updates", run.statusUpdates)
	}
	if run.inMemory == nil {
		t.Fatalf("the managed resource was never read")
	}
	wantCondition(t, run.inMemory.GetCondition(prv1.TypeReady), prv1.Available())
	wantCondition(t, run.inMemory.GetCondition(prv1.TypeSynced), prv1.ReconcileSuccess())
}

func TestReadyAssessed(t *testing.T) {
	errBoom := errors.New("boom")

	if got := ReadyAssessed(nil); got != nil {
		t.Errorf("ReadyAssessed(nil): want nil, got %v", got)
	}
	if IsReadyAssessed(nil) {
		t.Errorf("IsReadyAssessed(nil): want false")
	}
	if IsReadyAssessed(errBoom) {
		t.Errorf("IsReadyAssessed(unmarked): want false")
	}

	marked := ReadyAssessed(errBoom)
	if marked.Error() != errBoom.Error() {
		t.Errorf("ReadyAssessed must not change the message: want %q, got %q", errBoom.Error(), marked.Error())
	}
	if !IsReadyAssessed(marked) {
		t.Errorf("IsReadyAssessed(ReadyAssessed(err)): want true")
	}
	if got := errors.Unwrap(marked); got != errBoom {
		t.Errorf("errors.Unwrap(ReadyAssessed(err)): want err, got %v", got)
	}

	for name, wrapped := range map[string]error{
		"fmt.Errorf":  fmt.Errorf("observe: %w", marked),
		"errors.Wrap": errors.Wrap(marked, "observe"),
		"Twice":       fmt.Errorf("outer: %w", errors.Wrap(marked, "inner")),
	} {
		if !IsReadyAssessed(wrapped) {
			t.Errorf("%s: IsReadyAssessed must see through wrapping", name)
		}
		if !errors.Is(wrapped, errBoom) {
			t.Errorf("%s: errors.Is must still find the original error", name)
		}
	}

	// errors.As still reaches a typed error under the marker.
	var status kerrors.APIStatus
	if !errors.As(ReadyAssessed(fmt.Errorf("get: %w", kerrors.NewNotFound(schema.GroupResource{}, "x"))), &status) {
		t.Errorf("errors.As must see through ReadyAssessed")
	}
}
