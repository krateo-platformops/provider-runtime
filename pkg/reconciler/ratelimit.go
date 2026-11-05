package reconciler

import (
	"context"
	"sync"

	"github.com/krateo-platformops/plumbing/kubeutil/event"
	"k8s.io/apimachinery/pkg/api/equality"
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
// It is needed only for per-endpoint API rate limits. Implement it on the
// ExternalConnecter, the ExternalClient, or both. With a Governor configured
// (WithRateLimitGovernor):
//
//   - Connect is gated on the connecter's key, when the connecter implements
//     it.
//   - Observe, Create, Update and Delete are gated on the client's key, or on
//     the connecter's when only the connecter implements it.
//   - A connecter and client that implement neither are not gated per
//     endpoint. With saturation enabled, their calls still need a slot in the
//     process-wide local capacity: saturation bounds every gated call.
//
// Key on the credential's reference (for example the Secret's namespace and
// name), never its value: credentials rotate, and a key that changes on
// rotation splits one quota into several, each believing it has all of it.
// Return the same string the provider reports in ratelimit.Signal.Key (for an
// HTTP API, httpsignal.HostKey of its base URL), or signals throttle a key no
// call is gated on.
//
// An empty key means the object's calls are not gated per endpoint. An error
// is a provider defect: the call proceeds without per-endpoint gating (still
// bounded by saturation when that is on), and the runtime logs it once per
// object.
type RateLimitKeyer interface {
	RateLimitKey(mg resource.Managed) (string, error)
}

// WithRateLimitGovernor gates Connect and every ExternalClient call through g,
// for connecters and clients that implement RateLimitKeyer, and installs g as
// the ratelimit sink of the context those calls receive, so ratelimit.Report
// reaches it. A deferred call is not made: the pass writes
// Synced=False/ReconcileDeferred, leaves Ready unchanged, and requeues after
// the deferral's delay with a nil error, so no failure backoff is consumed. The
// one exception is a deferral on the startup ramp, which requeues quietly; see
// deferReconcile.
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

// gate asks the governor to let one call for mg through. It returns the
// release to call when the call returns, or a Deferral.
//
// The key comes from k. With no keyer, an empty key, or a keyer error, the
// call has no key: it is not gated per endpoint, but with saturation enabled it
// still needs a local slot, because saturation bounds every gated call. With
// no governor, or no key and saturation off, the call goes through ungated.
func (r *Reconciler) gate(ctx context.Context, log logging.Logger, k RateLimitKeyer, mg resource.Managed) (func(), *ratelimit.Deferral) {
	if r.governor == nil {
		return noRelease, nil
	}
	var key string
	if k != nil {
		var err error
		key, err = k.RateLimitKey(mg)
		if err != nil {
			if _, seen := r.keyErrors.logged.LoadOrStore(mg.GetUID(), struct{}{}); !seen {
				log.Error(err, "Cannot determine the rate-limit key; proceeding without per-endpoint gating. This is a provider defect: RateLimitKey must not fail",
					"kind", r.kind, "namespace", mg.GetNamespace(), "name", mg.GetName())
			}
			key = ""
		} else {
			r.keyErrors.logged.Delete(mg.GetUID())
		}
	}
	if key == "" && !r.governor.SaturationEnabled() {
		return noRelease, nil
	}
	return r.governor.Acquire(ctx, key)
}

// deferReconcile ends a pass whose next external call was deferred. It sets
// Synced=False/ReconcileDeferred with the deferral's message and nothing else
// (Ready is left as it is), writes it in one resourceVersion-guarded status
// update, and requeues after the deferral's delay with a nil error:
// controller-runtime then forgets the item's failure backoff, because a
// deferral is not a failure.
//
// When the object would be written exactly as it was read (pristine, a copy
// taken after the Get), the update is skipped and the pass only requeues. An
// at-capacity deferral repeats every second or so with the same message, and
// each identical write would be load on the API server for nothing. A
// throttled deferral with a new deadline has a new message, so it writes.
//
// A Normal event is recorded, through the throttled recorder, when the object
// enters a deferral or a throttled deferral names a new deadline, so a long
// window does not flood events.
//
// A DeferRamping is the exception: the controller has just started and is
// easing into its queue, for about 30s, which is not a state of the object.
// Reporting it would turn every healthy object Synced=False on every restart
// (200 objects, 200 status updates, 200 at ReconcileDeferred, 0 provider calls,
// before this was quiet). It sets no condition and records no event. What an
// earlier call of this pass learned is still written: quiet means no write for
// the deferral, not dropping an assessment.
func (r *Reconciler) deferReconcile(log logging.Logger, mg, pristine resource.Managed, d *ratelimit.Deferral, statusUpdate func() error) (reconcile.Result, error) {
	if d.Reason == ratelimit.DeferRamping {
		log.Debug("Reconcile deferred quietly: the local capacity is on its startup ramp", "requeue-after", d.RequeueAfter.String())
		if pristine == nil || !equality.Semantic.DeepEqual(pristine, mg) {
			if err := statusUpdate(); err != nil {
				return reconcile.Result{}, errors.Wrap(err, errUpdateManagedStatus)
			}
		}
		return reconcile.Result{RequeueAfter: d.RequeueAfter}, nil
	}

	prev := mg.GetCondition(prv1.TypeSynced)
	cond := prv1.ReconcileDeferred(d.Message())
	newWindow := prev.Reason != prv1.ReasonReconcileDeferred ||
		(d.Reason == ratelimit.DeferThrottled && prev.Message != cond.Message)

	log.Debug("Reconcile deferred", "key", d.Key, "reason", string(d.Reason), "requeue-after", d.RequeueAfter.String(), "message", cond.Message)
	if newWindow {
		r.throttledRecorder.Event(mg, event.Normal(reasonReconcileDeferred, actionProcessEvent, cond.Message))
	}
	mg.SetConditions(cond)
	if pristine != nil && equality.Semantic.DeepEqual(pristine, mg) {
		log.Debug("Deferral changes nothing on the object; skipping the status update")
		return reconcile.Result{RequeueAfter: d.RequeueAfter}, nil
	}
	if err := statusUpdate(); err != nil {
		return reconcile.Result{}, errors.Wrap(err, errUpdateManagedStatus)
	}
	return reconcile.Result{RequeueAfter: d.RequeueAfter}, nil
}
