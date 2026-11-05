package reconciler

import "sigs.k8s.io/controller-runtime/pkg/reconcile"

// rateLimitedRequeue is the result of a pass that should run again through the
// workqueue's rate limiter. controller-runtime calls AddRateLimited and not
// Forget, so repeated passes over one object back off.
//
// Result.Requeue is the only way to ask for that. It is deprecated in favour of
// RequeueAfter, which is not equivalent: controller-runtime calls Forget and
// then waits a fixed time, so the backoff is lost. Moving off it is a behaviour
// change to make deliberately, and this is the one place to make it.
func rateLimitedRequeue() reconcile.Result {
	return reconcile.Result{Requeue: true} //nolint:staticcheck // SA1019: nothing else requests a rate-limited requeue, see above.
}

// isRateLimitedRequeue reports whether r asks for what rateLimitedRequeue asks
// for.
func isRateLimitedRequeue(r reconcile.Result) bool {
	return r.Requeue //nolint:staticcheck // SA1019: the read side of rateLimitedRequeue, see its comment.
}
