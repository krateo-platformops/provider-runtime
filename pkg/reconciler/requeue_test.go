package reconciler

import (
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// The reconciler's tests compare against rateLimitedRequeue, so they can't notice
// it changing. This one pins what it asks controller-runtime for.
func TestRateLimitedRequeue(t *testing.T) {
	got := rateLimitedRequeue()
	if !isRateLimitedRequeue(got) {
		t.Errorf("rateLimitedRequeue() = %+v: want a rate-limited requeue", got)
	}
	if got.RequeueAfter != 0 {
		t.Errorf("rateLimitedRequeue() = %+v: RequeueAfter takes precedence and would lose the backoff", got)
	}
	if isRateLimitedRequeue(reconcile.Result{}) {
		t.Error("isRateLimitedRequeue(reconcile.Result{}) = true: want false")
	}
}
