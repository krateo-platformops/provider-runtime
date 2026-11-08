package reconciler

import (
	"context"
	"slices"
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

// RateLimitKeysProvider is RateLimitKeyer for an object whose calls reach
// several rate-limited scopes, such as a sync from one endpoint to another. One
// key can't describe that honestly: a throttled scope the call isn't gated on
// keeps being called, and a single shared key slows the healthy scope too.
//
// A gated call then needs a slot in every key plus the local one, all or
// nothing, and is deferred when any of them is held or full. Its deferral names
// the key that stopped it. Return only the scopes the object's calls really
// reach, so return one key for an object whose endpoints are the same.
//
// Everything RateLimitKeyer says about keys holds for each one: it must not do
// I/O, key on the credential's reference, and return the strings the provider
// reports in ratelimit.Signal.Key. Implement it on the ExternalConnecter, the
// ExternalClient, or both; it follows the same rules as RateLimitKeyer, and
// wins on a type that implements both. Empty keys are ignored. A nil or empty
// result means the object's calls are not gated per endpoint, and an error is
// treated as for RateLimitKeyer.
type RateLimitKeysProvider interface {
	RateLimitKeys(mg resource.Managed) ([]string, error)
}

// WithRateLimitGovernor gates Connect and every ExternalClient call through g,
// for connecters and clients that implement RateLimitKeyer, and installs g as
// the ratelimit sink of the context those calls receive, so ratelimit.Report
// reaches it. A deferred call is not made: the pass writes
// Synced=False/ReconcileDeferred, leaves Ready unchanged, and requeues after
// the deferral's delay with a nil error, so no failure backoff is consumed.
//
// The controller's own saturation is not gated here. With saturation on, build
// the controller with controller.Options.ForControllerRuntime: its queue takes
// a local slot before each reconcile and gives it back after, so the slot
// covers the whole reconcile and nothing is written on the object for it.
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

// rateLimitKeys names the keys of a managed resource's gated calls.
type rateLimitKeys func(mg resource.Managed) ([]string, error)

// keyerOf returns how v names its keys, or nil when it doesn't implement
// RateLimitKeysProvider or RateLimitKeyer.
func keyerOf(v any) rateLimitKeys {
	switch k := v.(type) {
	case RateLimitKeysProvider:
		return k.RateLimitKeys
	case RateLimitKeyer:
		return func(mg resource.Managed) ([]string, error) {
			key, err := k.RateLimitKey(mg)
			return []string{key}, err
		}
	}
	return nil
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
// The keys come from k. With no governor, no keyer, no non-empty key, or a
// keyer error, the call goes through ungated: the local capacity is taken by
// the queue, before the reconcile, not here.
func (r *Reconciler) gate(ctx context.Context, log logging.Logger, k rateLimitKeys, mg resource.Managed) (func(), *ratelimit.Deferral) {
	if r.governor == nil {
		return noRelease, nil
	}
	var keys []string
	if k != nil {
		var err error
		keys, err = k(mg)
		if err != nil {
			if _, seen := r.keyErrors.logged.LoadOrStore(mg.GetUID(), struct{}{}); !seen {
				log.Error(err, "Cannot determine the rate-limit key; proceeding without per-endpoint gating. This is a provider defect: RateLimitKey must not fail",
					"kind", r.kind, "namespace", mg.GetNamespace(), "name", mg.GetName())
			}
			keys = nil
		} else {
			r.keyErrors.logged.Delete(mg.GetUID())
		}
	}
	if !slices.ContainsFunc(keys, func(key string) bool { return key != "" }) {
		return noRelease, nil
	}
	return r.governor.AcquireAll(ctx, keys)
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
func (r *Reconciler) deferReconcile(log logging.Logger, mg, pristine resource.Managed, d *ratelimit.Deferral, statusUpdate func() error) (reconcile.Result, error) {
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
