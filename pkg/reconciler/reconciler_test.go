package reconciler

import (
	"context"
	"testing"
	"time"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/errors"
	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
	"github.com/krateo-platformops/provider-runtime/pkg/resource/fake"
	"github.com/krateo-platformops/provider-runtime/pkg/test"
)

var _ reconcile.Reconciler = &Reconciler{}

func TestReconciler(t *testing.T) {
	type args struct {
		m  manager.Manager
		mg resource.ManagedKind
		o  []ReconcilerOption
	}

	// Set by ExternalCreatePendingPastGracePeriod's MockUpdate when it observes the pending
	// annotation gone. Asserted after the table runs — MockUpdate serves several purposes in a
	// single reconcile, so "every call has it cleared" is the wrong assertion; "some call did" is
	// the one that distinguishes retrying from refusing forever.
	pendingCleared := false

	type want struct {
		result        reconcile.Result
		resultCmpOpts []cmp.Option
		err           error
	}

	errBoom := errors.New("boom")
	now := metav1.Now()

	cases := map[string]struct {
		reason string
		args   args
		want   want
	}{
		"GetManagedError": {
			reason: "Any error (except not found) encountered while getting the resource under reconciliation should be returned.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{MockGet: test.NewMockGetFn(errBoom)},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
			},
			want: want{err: errors.Wrap(errBoom, errGetManaged)},
		},
		"ManagedNotFound": {
			reason: "Not found errors encountered while getting the resource under reconciliation should be ignored.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{MockGet: test.NewMockGetFn(kerrors.NewNotFound(schema.GroupResource{}, ""))},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
			},
			want: want{result: reconcile.Result{}},
		},
		"RemoveFinalizerErrorDeletionPolicyOrphan": {
			reason: "Errors removing the managed resource finalizer should trigger a requeue after a short wait.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
							mg := obj.(*fake.Managed)
							mg.SetDeletionTimestamp(&now)
							mg.SetAnnotations(map[string]string{
								meta.AnnotationKeyDeletionPolicy: meta.DeletionPolicyOrphan,
							})
							return nil
						}),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							want := &fake.Managed{}
							want.SetDeletionTimestamp(&now)
							want.SetAnnotations(map[string]string{
								meta.AnnotationKeyDeletionPolicy: meta.DeletionPolicyOrphan,
							})
							want.SetConditions(prv1.Deleting())
							want.SetConditions(prv1.ReconcileError(errBoom))
							if diff := cmp.Diff(want, obj, test.EquateConditions()); diff != "" {
								reason := "Errors removing the managed resource finalizer should be reported as a conditioned status."
								t.Errorf("\nReason: %s\n-want, +got:\n%s", reason, diff)
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithFinalizer(resource.FinalizerFns{RemoveFinalizerFn: func(_ context.Context, _ resource.Object) error { return errBoom }}),
				},
			},
			want: want{err: errors.Wrap(errBoom, errUpdateManagedStatus)},
		},
		"DeleteSuccessfulDeletionPolicyOrphan": {
			reason: "Successful managed resource deletion with deletion policy Orphan should not trigger a requeue or status update.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
							mg := obj.(*fake.Managed)
							mg.SetDeletionTimestamp(&now)
							mg.SetAnnotations(map[string]string{
								meta.AnnotationKeyDeletionPolicy: meta.DeletionPolicyOrphan,
							})
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithFinalizer(resource.FinalizerFns{RemoveFinalizerFn: func(_ context.Context, _ resource.Object) error { return nil }}),
				},
			},
			want: want{result: reconcile.Result{Requeue: false}},
		},
		"ExternalCreatePending": {
			reason: "A RECENT unresolved create should keep refusing and re-observe, not recreate: an external API that is eventually consistent may not be showing a create that did land, and recreating inside that window would duplicate it.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
							meta.SetExternalCreatePending(obj, now.Time)
							return nil
						}),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							want := &fake.Managed{}
							meta.SetExternalCreatePending(want, now.Time)
							want.SetConditions(prv1.Creating(), prv1.ReconcileError(errors.New(errCreateIncomplete)))
							if diff := cmp.Diff(want, obj, test.EquateConditions()); diff != "" {
								reason := "We should update our status when we're asked to reconcile a managed resource that is pending creation."
								t.Errorf("\nReason: %s\n-want, +got:\n%s", reason, diff)
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
			},
			want: want{result: reconcile.Result{RequeueAfter: defaultCreateRecoveryReobserveInterval}},
		},
		// krateo-platformops/git-provider#22: once the confirm-the-negative window has FULLY
		// elapsed and the resource is still not observable, refusing further strands the object
		// forever — six Repo CRs sat terminal for over a day, with the error message instructing
		// the operator to delete the annotation by hand. The pending marker is cleared instead so
		// the normal create path can run again.
		"ExternalCreatePendingPastGracePeriod": {
			reason: "An unresolved create whose grace period has elapsed with the resource still absent should clear the pending marker and retry, not refuse forever.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
							// Older than the default 2-minute recovery window.
							meta.SetExternalCreatePending(obj, now.Add(-1*time.Hour))
							return nil
						}),
						MockUpdate: test.NewMockUpdateFn(nil, func(obj client.Object) error {
							if _, ok := obj.GetAnnotations()[meta.AnnotationKeyExternalCreatePending]; !ok {
								pendingCleared = true
							}
							return nil
						}),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, _ client.Object, _ ...client.SubResourceUpdateOption) error {
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnecter(&NopConnecter{}),
					WithFinalizer(resource.FinalizerFns{AddFinalizerFn: func(_ context.Context, _ resource.Object) error { return nil }}),
				},
			},
			// Requeue rather than the old terminal false: having cleared the marker we fall through
			// to the normal create path, which requeues to observe what it just created. The
			// difference from the old behaviour is the whole point — the resource moves again.
			want: want{result: reconcile.Result{Requeue: true}},
		},
		// krateo-core-provider#110: the guard used to refuse BEFORE Connect/Observe, with
		// Requeue:false — terminal, so the object could only ever be freed by a human deleting the
		// annotation. When Observe can RESOLVE the external resource, nothing was leaked and the
		// create plainly completed, so the handshake is closed automatically instead.
		"ExternalCreatePendingButResourceExists": {
			reason: "An unresolved create handshake whose external resource Observe can see should be adopted, not refused.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
							meta.SetExternalCreatePending(obj, now.Time)
							return nil
						}),
						MockUpdate: test.NewMockUpdateFn(nil),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							if cond := obj.(*fake.Managed).GetCondition(prv1.TypeReady); cond.Reason == prv1.ReasonCreating &&
								cond.Message == errCreateIncomplete {
								t.Errorf("must not refuse when Observe resolved the external resource")
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithFinalizer(resource.FinalizerFns{AddFinalizerFn: func(_ context.Context, _ resource.Object) error { return nil }}),
					WithCriticalAnnotationUpdater(CriticalAnnotationUpdateFn(func(_ context.Context, o client.Object) error {
						// The handshake is closed by stamping the terminal marker, which makes
						// ExternalCreateIncomplete false from here on (it compares timestamps).
						if meta.ExternalCreateIncomplete(o) {
							t.Errorf("adopting must resolve the handshake, but ExternalCreateIncomplete is still true")
						}
						return nil
					})),
					WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, _ resource.Managed) (ExternalClient, error) {
						return &ExternalClientFns{
							ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
								return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
							},
						}, nil
					})),
				},
			},
			want: want{result: reconcile.Result{RequeueAfter: defaultpollInterval}},
		},
		// Being deleted with no external counterpart: there is nothing left to leak, so refusing would
		// only strand the object with its finalizer held (the failure mode of #108).
		"ExternalCreatePendingWhileDeletedDoesNotRefuse": {
			reason: "An unresolved create handshake must not block deletion when the external resource does not exist.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
							meta.SetExternalCreatePending(obj, now.Time)
							obj.SetDeletionTimestamp(&now)
							return nil
						}),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							if cond := obj.(*fake.Managed).GetCondition(prv1.TypeReady); cond.Message == errCreateIncomplete {
								t.Errorf("must not refuse a deleted resource with no external counterpart")
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithFinalizer(resource.FinalizerFns{RemoveFinalizerFn: func(_ context.Context, _ resource.Object) error { return nil }}),
					WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, _ resource.Managed) (ExternalClient, error) {
						return &ExternalClientFns{
							ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
								return ExternalObservation{ResourceExists: false}, nil
							},
						}, nil
					})),
				},
			},
			want: want{result: reconcile.Result{Requeue: false}},
		},
		"ExternalConnectError": {
			reason: "Errors connecting to the provider should trigger a requeue after a short wait.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, got client.Object, _ ...client.SubResourceUpdateOption) error {
							want := &fake.Managed{}
							want.SetConditions(prv1.ReconcileError(errors.Wrap(errBoom, errReconcileConnect)))
							if diff := cmp.Diff(want, got, test.EquateConditions()); diff != "" {
								reason := "Errors connecting to the provider should be reported as a conditioned status."
								t.Errorf("\nReason: %s\n-want, +got:\n%s", reason, diff)
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, mg resource.Managed) (ExternalClient, error) {
						return nil, errBoom
					})),
				},
			},
			want: want{err: errors.Wrap(errBoom, errReconcileConnect)},
		},
		"ExternalDisconnectError": {
			reason: "Error disconnecting from the provider should not trigger requeue.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							want := &fake.Managed{}
							want.SetConditions(prv1.ReconcileSuccess())
							if diff := cmp.Diff(want, obj, test.EquateConditions()); diff != "" {
								reason := "A successful no-op reconcile should be reported as a conditioned status."
								t.Errorf("\nReason: %s\n-want, +got:\n%s", reason, diff)
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnectDisconnecter(ExternalConnectDisconnecterFns{
						ConnectFn: func(_ context.Context, mg resource.Managed) (ExternalClient, error) {
							c := &ExternalClientFns{
								ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
									return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
								},
							}
							return c, nil
						},
						DisconnectFn: func(_ context.Context) error { return errBoom },
					}),

					WithFinalizer(resource.FinalizerFns{AddFinalizerFn: func(_ context.Context, _ resource.Object) error { return nil }}),
				},
			},
			want: want{result: reconcile.Result{RequeueAfter: defaultpollInterval}},
		},
		"ExternalObserveError": {
			reason: "Errors observing the external resource should trigger a requeue after a short wait.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							want := &fake.Managed{}
							want.SetConditions(prv1.ReconcileError(errors.Wrap(errBoom, errReconcileObserve)))
							if diff := cmp.Diff(want, obj, test.EquateConditions()); diff != "" {
								reason := "Errors observing the managed resource should be reported as a conditioned status."
								t.Errorf("\nReason: %s\n-want, +got:\n%s", reason, diff)
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, mg resource.Managed) (ExternalClient, error) {
						c := &ExternalClientFns{
							ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
								return ExternalObservation{}, errBoom
							},
						}
						return c, nil
					})),
				},
			},
			want: want{err: errors.Wrap(errBoom, errReconcileObserve)},
		},
		// oasgen-provider#266, core-provider#236: an unresolved create waits on Observe's answer, so
		// an erroring Observe holds it. The object must say both what failed and that a create is
		// pending behind it; ExternalObserveError above is the control without a pending create.
		"ExternalObserveErrorWithUnresolvedCreate": {
			reason: "An Observe error on an unresolved create handshake should name the pending create alongside the Observe error.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
							meta.SetExternalCreatePending(obj, now.Time)
							return nil
						}),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							want := &fake.Managed{}
							meta.SetExternalCreatePending(want, now.Time)
							want.SetConditions(prv1.ReconcileError(errors.Wrapf(errors.Wrap(errBoom, errReconcileObserve),
								errFmtCreateWaitsOnObserve, now.Time.Format(time.RFC3339))))
							if diff := cmp.Diff(want, obj, test.EquateConditions()); diff != "" {
								reason := "An Observe error on an unresolved create should name the pending create in the conditioned status."
								t.Errorf("\nReason: %s\n-want, +got:\n%s", reason, diff)
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, _ resource.Managed) (ExternalClient, error) {
						return &ExternalClientFns{
							ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
								return ExternalObservation{}, errBoom
							},
						}, nil
					})),
				},
			},
			want: want{err: errors.Wrapf(errors.Wrap(errBoom, errReconcileObserve), errFmtCreateWaitsOnObserve, now.Time.Format(time.RFC3339))},
		},
		"CreationGracePeriod": {
			reason: "If our resource appears not to exist during the creation grace period we should return early.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
							meta.SetExternalCreateSucceeded(obj, time.Now())
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithCreationGracePeriod(1 * time.Minute),
					WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, mg resource.Managed) (ExternalClient, error) {
						c := &ExternalClientFns{
							ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
								return ExternalObservation{ResourceExists: false}, nil
							},
						}
						return c, nil
					})),
				},
			},
			want: want{result: reconcile.Result{Requeue: true}},
		},
		"ExternalDeleteError": {
			reason: "Errors deleting the external resource should trigger a requeue after a short wait.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
							mg := obj.(*fake.Managed)
							mg.SetDeletionTimestamp(&now)
							mg.SetAnnotations(map[string]string{
								meta.AnnotationKeyDeletionPolicy: meta.DeletionPolicyDelete,
							})
							return nil
						}),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							want := &fake.Managed{}
							want.SetDeletionTimestamp(&now)
							want.SetAnnotations(map[string]string{
								meta.AnnotationKeyDeletionPolicy: meta.DeletionPolicyDelete,
							})
							want.SetConditions(prv1.ReconcileError(errors.Wrap(errBoom, errReconcileDelete)))
							want.SetConditions(prv1.Deleting())
							if diff := cmp.Diff(want, obj, test.EquateConditions()); diff != "" {
								reason := "An error deleting an external resource should be reported as a conditioned status."
								t.Errorf("\nReason: %s\n-want, +got:\n%s", reason, diff)
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, mg resource.Managed) (ExternalClient, error) {
						c := &ExternalClientFns{
							ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
								return ExternalObservation{ResourceExists: true}, nil
							},
							DeleteFn: func(_ context.Context, _ resource.Managed) error {
								return errBoom
							},
						}
						return c, nil
					})),
				},
			},
			want: want{err: errors.Wrap(errBoom, errReconcileDelete)},
		},
		"ExternalDeleteSuccessful": {
			reason: "A deleted managed resource with the 'delete' reclaim policy should delete its external resource then requeue after a short wait.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
							mg := obj.(*fake.Managed)
							mg.SetDeletionTimestamp(&now)
							mg.SetAnnotations(map[string]string{
								meta.AnnotationKeyDeletionPolicy: meta.DeletionPolicyDelete,
							})
							return nil
						}),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							want := &fake.Managed{}
							want.SetDeletionTimestamp(&now)
							want.SetAnnotations(map[string]string{
								meta.AnnotationKeyDeletionPolicy: meta.DeletionPolicyDelete,
							})
							want.SetConditions(prv1.ReconcileSuccess())
							want.SetConditions(prv1.Deleting())
							if diff := cmp.Diff(want, obj, test.EquateConditions()); diff != "" {
								reason := "A deleted external resource should be reported as a conditioned status."
								t.Errorf("\nReason: %s\n-want, +got:\n%s", reason, diff)
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, mg resource.Managed) (ExternalClient, error) {
						c := &ExternalClientFns{
							ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
								return ExternalObservation{ResourceExists: true}, nil
							},
							DeleteFn: func(_ context.Context, _ resource.Managed) error {
								return nil
							},
						}
						return c, nil
					})),
				},
			},
			want: want{result: reconcile.Result{Requeue: true}},
		},
		"RemoveFinalizerErrorDeletionPolicyDelete": {
			reason: "Errors removing the managed resource finalizer should trigger a requeue after a short wait.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
							mg := obj.(*fake.Managed)
							mg.SetDeletionTimestamp(&now)
							mg.SetAnnotations(map[string]string{
								meta.AnnotationKeyDeletionPolicy: meta.DeletionPolicyDelete,
							})
							return nil
						}),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							want := &fake.Managed{}
							want.SetDeletionTimestamp(&now)
							want.SetAnnotations(map[string]string{
								meta.AnnotationKeyDeletionPolicy: meta.DeletionPolicyDelete,
							})
							want.SetConditions(prv1.Deleting())
							want.SetConditions(prv1.ReconcileError(errBoom))
							if diff := cmp.Diff(want, obj, test.EquateConditions()); diff != "" {
								reason := "Errors removing the managed resource finalizer should be reported as a conditioned status."
								t.Errorf("\nReason: %s\n-want, +got:\n%s", reason, diff)
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, mg resource.Managed) (ExternalClient, error) {
						c := &ExternalClientFns{
							ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
								return ExternalObservation{ResourceExists: false}, nil
							},
						}
						return c, nil
					})),
					WithFinalizer(resource.FinalizerFns{RemoveFinalizerFn: func(_ context.Context, _ resource.Object) error { return errBoom }}),
				},
			},
			want: want{err: errors.Wrap(errBoom, errUpdateManagedStatus)},
		},
		"DeleteSuccessfulDeletionPolicyDelete": {
			reason: "Successful managed resource deletion with deletion policy Delete should not trigger a requeue or status update.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
							mg := obj.(*fake.Managed)
							mg.SetDeletionTimestamp(&now)
							mg.SetAnnotations(map[string]string{
								meta.AnnotationKeyDeletionPolicy: meta.DeletionPolicyDelete,
							})
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, mg resource.Managed) (ExternalClient, error) {
						c := &ExternalClientFns{
							ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
								return ExternalObservation{ResourceExists: false}, nil
							},
						}
						return c, nil
					})),
					WithFinalizer(resource.FinalizerFns{RemoveFinalizerFn: func(_ context.Context, _ resource.Object) error { return nil }}),
				},
			},
			want: want{result: reconcile.Result{Requeue: false}},
		},
		"AddFinalizerError": {
			reason: "Errors adding a finalizer should trigger a requeue after a short wait.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							want := &fake.Managed{}
							want.SetConditions(prv1.ReconcileError(errBoom))
							if diff := cmp.Diff(want, obj, test.EquateConditions()); diff != "" {
								reason := "Errors adding a finalizer should be reported as a conditioned status."
								t.Errorf("\nReason: %s\n-want, +got:\n%s", reason, diff)
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnecter(&NopConnecter{}),
					WithFinalizer(resource.FinalizerFns{AddFinalizerFn: func(_ context.Context, _ resource.Object) error { return errBoom }}),
				},
			},
			want: want{err: errors.Wrap(errBoom, errCreateIncomplete)},
		},
		"UpdateCreatePendingError": {
			reason: "Errors while updating our external-create-pending annotation should trigger a requeue after a short wait.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet:    test.NewMockGetFn(nil),
						MockUpdate: test.NewMockUpdateFn(errBoom),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							want := &fake.Managed{}
							meta.SetExternalCreatePending(want, time.Now())
							want.SetConditions(prv1.Creating(), prv1.ReconcileError(errors.Wrap(errBoom, errUpdateManaged)))
							if diff := cmp.Diff(want, obj, test.EquateConditions(), cmpopts.EquateApproxTime(1*time.Second)); diff != "" {
								reason := "Errors while creating an external resource should be reported as a conditioned status."
								t.Errorf("\nReason: %s\n-want, +got:\n%s", reason, diff)
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, mg resource.Managed) (ExternalClient, error) {
						c := &ExternalClientFns{
							ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
								return ExternalObservation{ResourceExists: false}, nil
							},
							CreateFn: func(_ context.Context, _ resource.Managed) error {
								return errBoom
							},
						}
						return c, nil
					})),
					WithFinalizer(resource.FinalizerFns{AddFinalizerFn: func(_ context.Context, _ resource.Object) error { return nil }}),
				},
			},
			want: want{err: errors.Wrap(errBoom, errUpdateManaged)},
		},
		"CreateExternalError": {
			reason: "Errors while creating an external resource should trigger a requeue after a short wait.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet:    test.NewMockGetFn(nil),
						MockUpdate: test.NewMockUpdateFn(nil),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							want := &fake.Managed{}
							meta.SetExternalCreatePending(want, time.Now())
							meta.SetExternalCreateFailed(want, time.Now())
							want.SetConditions(prv1.ReconcileError(errors.Wrap(errBoom, errReconcileCreate)))
							want.SetConditions(prv1.Creating())
							if diff := cmp.Diff(want, obj, test.EquateConditions(), cmpopts.EquateApproxTime(1*time.Second)); diff != "" {
								reason := "Errors while creating an external resource should be reported as a conditioned status."
								t.Errorf("\nReason: %s\n-want, +got:\n%s", reason, diff)
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, mg resource.Managed) (ExternalClient, error) {
						c := &ExternalClientFns{
							ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
								return ExternalObservation{ResourceExists: false}, nil
							},
							CreateFn: func(_ context.Context, _ resource.Managed) error {
								return errBoom
							},
						}
						return c, nil
					})),
					// We simulate our critical annotation update failing too here.
					// This is mostly just to exercise the code, which just creates a log and an event.
					WithCriticalAnnotationUpdater(CriticalAnnotationUpdateFn(func(ctx context.Context, o client.Object) error { return errBoom })),
					WithFinalizer(resource.FinalizerFns{AddFinalizerFn: func(_ context.Context, _ resource.Object) error { return nil }}),
				},
			},
			want: want{err: errors.Wrap(errBoom, errReconcileCreate)},
		},
		"UpdateCriticalAnnotationsError": {
			reason: "Errors updating critical annotations after creation should trigger a requeue after a short wait.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet:    test.NewMockGetFn(nil),
						MockUpdate: test.NewMockUpdateFn(nil),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							want := &fake.Managed{}
							meta.SetExternalCreatePending(want, time.Now())
							meta.SetExternalCreateSucceeded(want, time.Now())
							want.SetConditions(prv1.ReconcileError(errors.Wrap(errBoom, errUpdateManagedAnnotations)))
							want.SetConditions(prv1.Creating())
							if diff := cmp.Diff(want, obj, test.EquateConditions(), cmpopts.EquateApproxTime(1*time.Second)); diff != "" {
								reason := "Errors updating critical annotations after creation should be reported as a conditioned status."
								t.Errorf("\nReason: %s\n-want, +got:\n%s", reason, diff)
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, mg resource.Managed) (ExternalClient, error) {
						c := &ExternalClientFns{
							ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
								return ExternalObservation{ResourceExists: false}, nil
							},
							CreateFn: func(_ context.Context, _ resource.Managed) error {
								return nil
							},
						}
						return c, nil
					})),
					WithFinalizer(resource.FinalizerFns{AddFinalizerFn: func(_ context.Context, _ resource.Object) error { return nil }}),
					WithCriticalAnnotationUpdater(CriticalAnnotationUpdateFn(func(ctx context.Context, o client.Object) error { return errBoom })),
				},
			},
			want: want{err: errors.Wrap(errBoom, errUpdateManagedAnnotations)},
		},
		"CreateSuccessful": {
			reason: "Successful managed resource creation should trigger a requeue after a short wait.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet:    test.NewMockGetFn(nil),
						MockUpdate: test.NewMockUpdateFn(nil),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							want := &fake.Managed{}
							meta.SetExternalCreatePending(want, time.Now())
							meta.SetExternalCreateSucceeded(want, time.Now())
							want.SetConditions(prv1.ReconcileSuccess())
							want.SetConditions(prv1.Creating())
							if diff := cmp.Diff(want, obj, test.EquateConditions(), cmpopts.EquateApproxTime(1*time.Second)); diff != "" {
								reason := "Successful managed resource creation should be reported as a conditioned status."
								t.Errorf("\nReason: %s\n-want, +got:\n%s", reason, diff)
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnecter(&NopConnecter{}),
					WithCriticalAnnotationUpdater(CriticalAnnotationUpdateFn(func(ctx context.Context, o client.Object) error { return nil })),
					WithFinalizer(resource.FinalizerFns{AddFinalizerFn: func(_ context.Context, _ resource.Object) error { return nil }}),
				},
			},
			want: want{result: reconcile.Result{Requeue: true}},
		},
		"LateInitializeUpdateError": {
			reason: "Errors updating a managed resource to persist late initialized fields should trigger a requeue after a short wait.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet:    test.NewMockGetFn(nil),
						MockUpdate: test.NewMockUpdateFn(errBoom),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							want := &fake.Managed{}
							want.SetConditions(prv1.ReconcileError(errors.Wrap(errBoom, errUpdateManaged)))
							if diff := cmp.Diff(want, obj, test.EquateConditions()); diff != "" {
								reason := "Errors updating a managed resource should be reported as a conditioned status."
								t.Errorf("\nReason: %s\n-want, +got:\n%s", reason, diff)
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, mg resource.Managed) (ExternalClient, error) {
						c := &ExternalClientFns{
							ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
								return ExternalObservation{ResourceExists: true, ResourceUpToDate: true, ResourceLateInitialized: true}, nil
							},
						}
						return c, nil
					})),
					WithFinalizer(resource.FinalizerFns{AddFinalizerFn: func(_ context.Context, _ resource.Object) error { return nil }}),
				},
			},
			want: want{err: errors.Wrap(errBoom, errUpdateManaged)},
		},
		"ExternalResourceUpToDate": {
			reason: "When the external resource exists and is up to date a requeue should be triggered after a long wait.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							want := &fake.Managed{}
							want.SetConditions(prv1.ReconcileSuccess())
							if diff := cmp.Diff(want, obj, test.EquateConditions()); diff != "" {
								reason := "A successful no-op reconcile should be reported as a conditioned status."
								t.Errorf("\nReason: %s\n-want, +got:\n%s", reason, diff)
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, mg resource.Managed) (ExternalClient, error) {
						c := &ExternalClientFns{
							ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
								return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
							},
						}
						return c, nil
					})),
					WithFinalizer(resource.FinalizerFns{AddFinalizerFn: func(_ context.Context, _ resource.Object) error { return nil }}),
				},
			},
			want: want{result: reconcile.Result{RequeueAfter: defaultpollInterval}},
		},
		"UpdateExternalError": {
			reason: "Errors while updating an external resource should trigger a requeue after a short wait.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							want := &fake.Managed{}
							want.SetConditions(prv1.ReconcileError(errors.Wrap(errBoom, errReconcileUpdate)))
							if diff := cmp.Diff(want, obj, test.EquateConditions()); diff != "" {
								reason := "Errors while updating an external resource should be reported as a conditioned status."
								t.Errorf("\nReason: %s\n-want, +got:\n%s", reason, diff)
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, mg resource.Managed) (ExternalClient, error) {
						c := &ExternalClientFns{
							ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
								return ExternalObservation{ResourceExists: true, ResourceUpToDate: false}, nil
							},
							UpdateFn: func(_ context.Context, _ resource.Managed) error {
								return errBoom
							},
						}
						return c, nil
					})),
					WithFinalizer(resource.FinalizerFns{AddFinalizerFn: func(_ context.Context, _ resource.Object) error { return nil }}),
				},
			},
			want: want{err: errors.Wrap(errBoom, errReconcileUpdate)},
		},
		"UpdateSuccessful": {
			reason: "A successful managed resource update should trigger a requeue after a long wait.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							want := &fake.Managed{}
							want.SetConditions(prv1.ReconcileSuccess())
							if diff := cmp.Diff(want, obj, test.EquateConditions()); diff != "" {
								reason := "A successful managed resource update should be reported as a conditioned status."
								t.Errorf("\nReason: %s\n-want, +got:\n%s", reason, diff)
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, mg resource.Managed) (ExternalClient, error) {
						c := &ExternalClientFns{
							ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
								return ExternalObservation{ResourceExists: true, ResourceUpToDate: false}, nil
							},
							UpdateFn: func(_ context.Context, _ resource.Managed) error {
								return nil
							},
						}
						return c, nil
					})),
					WithFinalizer(resource.FinalizerFns{AddFinalizerFn: func(_ context.Context, _ resource.Object) error { return nil }}),
				},
			},
			want: want{result: reconcile.Result{RequeueAfter: defaultpollInterval}},
		},
		"ReconciliationPausedSuccessful": {
			reason: `If a managed resource has the pause annotation with value "true", there should be no further requeue requests.`,
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
							mg := obj.(*fake.Managed)
							mg.SetAnnotations(map[string]string{meta.AnnotationKeyReconciliationPaused: "true"})
							return nil
						}),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							want := &fake.Managed{}
							want.SetAnnotations(map[string]string{meta.AnnotationKeyReconciliationPaused: "true"})
							want.SetConditions(prv1.ReconcilePaused())
							if diff := cmp.Diff(want, obj, test.EquateConditions()); diff != "" {
								reason := `If managed resource has the pause annotation with value "true", it should acquire "Synced" status condition with the status "False" and the reason "ReconcilePaused".`
								t.Errorf("\nReason: %s\n-want, +got:\n%s", reason, diff)
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
			},
			want: want{result: reconcile.Result{}},
		},
		"ReconciliationResumes": {
			reason: `If a managed resource has the pause annotation with some value other than "true" and the Synced=False/ReconcilePaused status condition, reconciliation should resume with requeueing.`,
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
							mg := obj.(*fake.Managed)
							mg.SetAnnotations(map[string]string{meta.AnnotationKeyReconciliationPaused: "false"})
							mg.SetConditions(prv1.ReconcilePaused())
							return nil
						}),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							want := &fake.Managed{}
							want.SetAnnotations(map[string]string{meta.AnnotationKeyReconciliationPaused: "false"})
							want.SetConditions(prv1.ReconcileSuccess())
							if diff := cmp.Diff(want, obj, test.EquateConditions()); diff != "" {
								reason := `Managed resource should acquire Synced=False/ReconcileSuccess status condition after a resume.`
								t.Errorf("\nReason: %s\n-want, +got:\n%s", reason, diff)
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnectDisconnecter(ExternalConnectDisconnecterFns{
						ConnectFn: func(_ context.Context, mg resource.Managed) (ExternalClient, error) {
							c := &ExternalClientFns{
								ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
									return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
								},
							}
							return c, nil
						},
						DisconnectFn: func(_ context.Context) error { return errBoom },
					}),
					WithFinalizer(resource.FinalizerFns{AddFinalizerFn: func(_ context.Context, _ resource.Object) error { return nil }}),
				},
			},
			want: want{result: reconcile.Result{RequeueAfter: defaultpollInterval}},
		},
		"ReconciliationPausedError": {
			reason: `If a managed resource has the pause annotation with value "true" and the status update due to reconciliation being paused fails, error should be reported causing an exponentially backed-off requeue.`,
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
							mg := obj.(*fake.Managed)
							mg.SetAnnotations(map[string]string{meta.AnnotationKeyReconciliationPaused: "true"})
							return nil
						}),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							return errBoom
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
			},
			want: want{err: errors.Wrap(errBoom, errUpdateManagedStatus)},
		},
		"ExternalResourceUpToDateWithJitter": {
			reason: "When the external resource exists and is up to date a requeue should be triggered after a long wait with jitter added.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, _ client.Object, _ ...client.SubResourceUpdateOption) error {
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, _ resource.Managed) (ExternalClient, error) {
						c := &ExternalClientFns{
							ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
								return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
							},
						}
						return c, nil
					})),
					WithFinalizer(resource.FinalizerFns{AddFinalizerFn: func(_ context.Context, _ resource.Object) error { return nil }}),
					WithPollJitterHook(time.Second),
				},
			},
			want: want{
				result: reconcile.Result{RequeueAfter: defaultpollInterval},
				resultCmpOpts: []cmp.Option{cmp.Comparer(func(l, r time.Duration) bool {
					diff := l - r
					if diff < 0 {
						diff = -diff
					}
					return diff < time.Second
				})},
			},
		},
		"ExternalResourceUpToDateWithPollIntervalHook": {
			reason: "When the external resource exists and is up to date a requeue should be triggered after a long wait processed by the interval hook.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, _ client.Object, _ ...client.SubResourceUpdateOption) error {
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, _ resource.Managed) (ExternalClient, error) {
						c := &ExternalClientFns{
							ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
								return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
							},
						}
						return c, nil
					})),
					WithFinalizer(resource.FinalizerFns{AddFinalizerFn: func(_ context.Context, _ resource.Object) error { return nil }}),
					WithPollIntervalHook(func(_ resource.Managed, pollInterval time.Duration) time.Duration {
						return 2 * pollInterval
					}),
				},
			},
			want: want{
				result: reconcile.Result{RequeueAfter: 2 * defaultpollInterval},
			},
		},
		"ObserveOnlyResourceDoesNotExist": {
			reason: "With only Observe management action, observing a resource that does not exist should be reported as a conditioned status error.",
			args: args{
				m: &fake.Manager{
					Client: &test.MockClient{
						MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
							mg := obj.(*fake.Managed)
							mg.SetAnnotations(map[string]string{
								meta.AnnotationKeyManagementPolicy: meta.ManagementPolicyObserve,
							})
							return nil
						}),
						MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
							want := &fake.Managed{}
							want.SetAnnotations(map[string]string{
								meta.AnnotationKeyManagementPolicy: meta.ManagementPolicyObserve,
							})
							want.SetConditions(prv1.ReconcileError(errors.Wrap(errors.New(errExternalResourceNotExist), errReconcileObserve)))
							if diff := cmp.Diff(want, obj, test.EquateConditions()); diff != "" {
								reason := "Resource does not exist should be reported as a conditioned status when ObserveOnly."
								t.Errorf("\nReason: %s\n-want, +got:\n%s", reason, diff)
							}
							return nil
						}),
					},
					Scheme: fake.SchemeWith(&fake.Managed{}),
				},
				mg: resource.ManagedKind(fake.GVK(&fake.Managed{})),
				o: []ReconcilerOption{
					WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, _ resource.Managed) (ExternalClient, error) {
						c := &ExternalClientFns{
							ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
								return ExternalObservation{ResourceExists: false}, nil
							},
						}
						return c, nil
					})),
				},
			},
			want: want{err: errors.Wrap(errors.New(errExternalResourceNotExist), errReconcileObserve)},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r := NewReconciler(tc.args.m, tc.args.mg, tc.args.o...)
			got, err := r.Reconcile(context.Background(), reconcile.Request{})

			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("\nReason: %s\nr.Reconcile(...): -want error, +got error:\n%s", tc.reason, diff)
			}

			if diff := cmp.Diff(tc.want.result, got, tc.want.resultCmpOpts...); diff != "" {
				t.Errorf("\nReason: %s\nr.Reconcile(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}

	if !pendingCleared {
		t.Errorf("ExternalCreatePendingPastGracePeriod: the pending annotation was never cleared. " +
			"An unresolved create whose grace period has elapsed must be retried; refusing forever " +
			"is what left six Repo CRs terminal for over a day (git-provider#22).")
	}
}

// Persisting late-initialized fields is a write to the managed resource like the finalizer and
// create-pending writes, and a resource-version conflict there is just as transient: someone
// else wrote first. It must requeue promptly like those, not be reported as a ReconcileError
// (with a Warning event and error backoff) for a race that the next pass resolves by re-reading.
func TestLateInitializeConflictRequeues(t *testing.T) {
	conflict := kerrors.NewConflict(schema.GroupResource{Group: "example.krateo.io", Resource: "things"}, "x",
		errors.New("the object has been modified"))

	updates, statusUpdates := 0, 0
	mc := &test.MockClient{
		MockGet: test.NewMockGetFn(nil),
		MockUpdate: test.NewMockUpdateFn(nil, func(_ client.Object) error {
			updates++
			return conflict
		}),
		MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
			statusUpdates++
			if cond := obj.(*fake.Managed).GetCondition(prv1.TypeSynced); cond.Reason == prv1.ReasonReconcileError {
				t.Errorf("a late-init conflict must not be recorded as %s: %q", prv1.ReasonReconcileError, cond.Message)
			}
			return nil
		}),
	}
	r := NewReconciler(&fake.Manager{Client: mc, Scheme: fake.SchemeWith(&fake.Managed{})},
		resource.ManagedKind(fake.GVK(&fake.Managed{})),
		WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, _ resource.Managed) (ExternalClient, error) {
			return &ExternalClientFns{
				ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
					return ExternalObservation{ResourceExists: true, ResourceUpToDate: true, ResourceLateInitialized: true}, nil
				},
			}, nil
		})),
		WithFinalizer(resource.FinalizerFns{AddFinalizerFn: func(_ context.Context, _ resource.Object) error { return nil }}),
	)

	got, err := r.Reconcile(context.Background(), reconcile.Request{})

	// The denominator: without a conflicting Update, this test could not tell the fix from its absence.
	if updates == 0 {
		t.Fatalf("the late-init Update was never called, so no conflict was produced")
	}
	if err != nil {
		t.Errorf("r.Reconcile(...): a late-init conflict must not return an error (that means error backoff); got %v", err)
	}
	if diff := cmp.Diff(reconcile.Result{Requeue: true}, got); diff != "" {
		t.Errorf("r.Reconcile(...): -want, +got:\n%s", diff)
	}
	if statusUpdates != 0 {
		t.Errorf("a late-init conflict must requeue without writing status; got %d status updates", statusUpdates)
	}
}

// krateo-platformops/oasgen-provider#254: a Create that returned a resource-version conflict,
// plain or wrapped, used to requeue BEFORE stamping external-create-failed. external-create-pending
// stayed the newest marker, and every later reconcile refused with errCreateIncomplete. Every create
// error must close the handshake, so that the next pass retries Create instead of refusing.
func TestCreateErrorClosesHandshake(t *testing.T) {
	conflict := kerrors.NewConflict(schema.GroupResource{Group: "example.krateo.io", Resource: "things"}, "x",
		errors.New("the object has been modified"))

	cases := map[string]struct {
		createErr  error
		wantResult reconcile.Result
		wantErr    bool
	}{
		"Conflict":        {createErr: conflict, wantResult: reconcile.Result{Requeue: true}},
		"WrappedConflict": {createErr: errors.Wrap(conflict, "cannot update status"), wantResult: reconcile.Result{Requeue: true}},
		"PlainError":      {createErr: errors.New("boom"), wantErr: true},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			// What the API server holds; every write lands here and every read comes from here.
			persisted := &fake.Managed{}
			creates := 0

			mc := &test.MockClient{
				MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
					obj.SetAnnotations(persisted.GetAnnotations())
					return nil
				}),
				MockUpdate: test.NewMockUpdateFn(nil, func(obj client.Object) error {
					persisted.SetAnnotations(obj.GetAnnotations())
					return nil
				}),
				MockStatusUpdate: test.MockSubResourceUpdateFn(func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
					if cond := obj.(*fake.Managed).GetCondition(prv1.TypeReady); cond.Message == errCreateIncomplete {
						t.Errorf("must not refuse with %q after a create error", errCreateIncomplete)
					}
					return nil
				}),
			}
			r := NewReconciler(&fake.Manager{Client: mc, Scheme: fake.SchemeWith(&fake.Managed{})},
				resource.ManagedKind(fake.GVK(&fake.Managed{})),
				WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, _ resource.Managed) (ExternalClient, error) {
					return &ExternalClientFns{
						ObserveFn: func(_ context.Context, _ resource.Managed) (ExternalObservation, error) {
							return ExternalObservation{ResourceExists: false}, nil
						},
						CreateFn: func(_ context.Context, _ resource.Managed) error {
							creates++
							return tc.createErr
						},
					}, nil
				})),
				WithCriticalAnnotationUpdater(CriticalAnnotationUpdateFn(func(_ context.Context, o client.Object) error {
					persisted.SetAnnotations(o.GetAnnotations())
					return nil
				})),
				WithFinalizer(resource.FinalizerFns{AddFinalizerFn: func(_ context.Context, _ resource.Object) error { return nil }}),
			)

			got, err := r.Reconcile(context.Background(), reconcile.Request{})
			if (err != nil) != tc.wantErr {
				t.Errorf("r.Reconcile(...): want error %t, got %v", tc.wantErr, err)
			}
			if diff := cmp.Diff(tc.wantResult, got); diff != "" {
				t.Errorf("r.Reconcile(...): -want, +got:\n%s", diff)
			}
			if meta.GetExternalCreateFailed(persisted).IsZero() {
				t.Errorf("a create error must persist %s; got annotations %v", meta.AnnotationKeyExternalCreateFailed, persisted.GetAnnotations())
			}
			if meta.ExternalCreateIncomplete(persisted) {
				t.Errorf("a create error must close the handshake, but %s is still the newest marker: %v",
					meta.AnnotationKeyExternalCreatePending, persisted.GetAnnotations())
			}

			// The next reconcile must retry Create, not refuse.
			_, _ = r.Reconcile(context.Background(), reconcile.Request{})
			if creates != 2 {
				t.Errorf("the reconcile after a create error must retry Create: want 2 Create calls, got %d", creates)
			}
		})
	}
}
