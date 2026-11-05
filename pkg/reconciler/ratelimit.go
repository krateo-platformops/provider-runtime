package reconciler

import (
	"context"
	"sync"

	"github.com/krateo-platformops/plumbing/kubeutil/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/errors"
	"github.com/krateo-platformops/provider-runtime/pkg/logging"
	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
)

// reasonReconcileDeferred is the reason of the Normal event recorded on the
// first deferral of an object per throttle window.
const reasonReconcileDeferred = event.Reason(prv1.ReasonReconcileDeferred)

// RateLimitKeyer names the rate-limited scope a managed resource's external
// calls belong to, typically the endpoint host, or host|credential when the
// quota is per identity. It must not do I/O: it is called before every gated
// call, including when the endpoint is throttled.
//
// Implement it on the ExternalConnecter, the ExternalClient, or both. With a
// Governor configured (WithRateLimitGovernor):
//
//   - Connect is gated on the connecter's key, when the connecter implements
//     it.
//   - Observe, Create, Update and Delete are gated on the client's key, or on
//     the connecter's when only the connecter implements it.
//   - A connecter and client that implement neither are never gated.
//
// Key on the credential's reference (for example the Secret's namespace and
// name), never its value: credentials rotate, and a key that changes on
// rotation splits one quota into several, each believing it has all of it.
// Return the same string the provider reports in ratelimit.Signal.Key (for an
// HTTP API, httpsignal.HostKey of its base URL), or signals throttle a key no
// call is gated on.
//
// An empty key means the object's calls are not gated. An error is a provider
// defect: the call proceeds ungated, and the runtime logs it once per object.
type RateLimitKeyer interface {
	RateLimitKey(mg resource.Managed) (string, error)
}

// WithRateLimitGovernor gates Connect and every ExternalClient call through g,
// for connecters and clients that implement RateLimitKeyer, and installs g as
// the ratelimit sink of the context those calls receive, so ratelimit.Report
// reaches it. A deferred call is not made: the pass writes
// Synced=False/ReconcileDeferred, leaves Ready unchanged, and requeues after
// the deferral's delay with a nil error, so no failure backoff is consumed.
//
// Share one Governor across the process's reconcilers; each one registers
// with it, which raises its ceiling by MaxConcurrentReconciles. Build it with
// controller.Options.SetupRateLimitGovernor, which also adds it to the manager
// so its saturation sampler runs. A nil g, the default, gates nothing and
// leaves Report a no-op, so
//
//	reconciler.WithRateLimitGovernor(o.RateLimitGovernor)
//
// is safe whether or not the provider's chart enabled the governor.
func WithRateLimitGovernor(g *ratelimit.Governor) ReconcilerOption {
	return func(r *Reconciler) {
		if g == nil {
			return
		}
		r.governor = g
		g.RegisterReconciler()
	}
}

// keyerOf returns v's RateLimitKeyer, or nil.
func keyerOf(v any) RateLimitKeyer {
	k, _ := v.(RateLimitKeyer)
	return k
}

// noRelease is the release of an ungated call.
func noRelease() {}

// keyErrorLog remembers which objects already had a RateLimitKey error
// logged, so a broken keyer logs once per object instead of once per pass.
type keyErrorLog struct {
	logged sync.Map // types.UID -> struct{}
}

// gate asks the governor to let one call for mg through, keyed by k. It returns
// the release to call when the call returns, or a Deferral. A nil governor or
// keyer, an empty key, or a keyer error lets the call through ungated.
func (r *Reconciler) gate(ctx context.Context, log logging.Logger, k RateLimitKeyer, mg resource.Managed) (func(), *ratelimit.Deferral) {
	if r.governor == nil || k == nil {
		return noRelease, nil
	}
	key, err := k.RateLimitKey(mg)
	if err != nil {
		if _, seen := r.keyErrors.logged.LoadOrStore(mg.GetUID(), struct{}{}); !seen {
			log.Error(err, "Cannot determine the rate-limit key; proceeding ungated. This is a provider defect: RateLimitKey must not fail",
				"kind", r.kind, "namespace", mg.GetNamespace(), "name", mg.GetName())
		}
		return noRelease, nil
	}
	r.keyErrors.logged.Delete(mg.GetUID())
	if key == "" {
		return noRelease, nil
	}
	return r.governor.Acquire(ctx, key)
}

// deferReconcile ends a pass whose next external call was deferred. It writes
// Synced=False/ReconcileDeferred with the deferral's message and nothing else
// (Ready is left as it is), in one resourceVersion-guarded status update, and
// requeues after the deferral's delay with a nil error: controller-runtime then
// forgets the item's failure backoff, because a deferral is not a failure.
//
// A Normal event is recorded, through the throttled recorder, when the object
// enters a deferral or a throttled deferral names a new deadline, so a long
// window does not flood events.
func (r *Reconciler) deferReconcile(log logging.Logger, mg resource.Managed, d *ratelimit.Deferral, statusUpdate func() error) (reconcile.Result, error) {
	prev := mg.GetCondition(prv1.TypeSynced)
	cond := prv1.ReconcileDeferred(d.Message())
	newWindow := prev.Reason != prv1.ReasonReconcileDeferred ||
		(d.Reason == ratelimit.DeferThrottled && prev.Message != cond.Message)

	log.Debug("Reconcile deferred", "key", d.Key, "reason", string(d.Reason), "requeue-after", d.RequeueAfter.String(), "message", cond.Message)
	if newWindow {
		r.throttledRecorder.Event(mg, event.Normal(reasonReconcileDeferred, actionProcessEvent, cond.Message))
	}
	mg.SetConditions(cond)
	if err := statusUpdate(); err != nil {
		return reconcile.Result{}, errors.Wrap(err, errUpdateManagedStatus)
	}
	return reconcile.Result{RequeueAfter: d.RequeueAfter}, nil
}
