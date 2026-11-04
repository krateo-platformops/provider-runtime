// Package reconcilertest helps providers test their ExternalClient against
// the contracts pkg/reconciler relies on.
package reconcilertest

import (
	"context"
	"testing"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/reconciler"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
)

// AssertObserveSetsReady checks the Ready contract (since v1.8.0) on one
// path of c's Observe: every successful Observe must set the Ready condition.
//
// It sets Ready on mg to prv1.NotObserved(), the marker the runtime leaves
// after a failed Connect or Observe, calls c.Observe(ctx, mg), and fails t if
// Ready's reason is still NotObserved afterwards, because the runtime would
// then keep the marker and report the provider as broken. Preparing mg is the
// helper's job, so the check cannot pass just because mg already carried a
// Ready from its fixture.
//
// The path must be one on which Observe owns Ready: it returns a nil error,
// or an error wrapped with reconciler.ReadyAssessed. A plain error fails t,
// because on that path the runtime sets Ready itself and there is nothing for
// this helper to check; a test that expected success has drifted.
//
// Call it once per success path of Observe, with mg and the external fakes
// arranged to reach that path. It returns what Observe returned, for further
// assertions.
func AssertObserveSetsReady(t testing.TB, ctx context.Context, c reconciler.ExternalClient, mg resource.Managed) (reconciler.ExternalObservation, error) {
	t.Helper()

	mg.SetConditions(prv1.NotObserved())
	obs, err := c.Observe(ctx, mg)

	if err != nil && !reconciler.IsReadyAssessed(err) {
		t.Errorf("Observe returned an error not wrapped with reconciler.ReadyAssessed, so this is not a path on which Observe owns Ready: %v", err)
		return obs, err
	}
	if ready := mg.GetCondition(prv1.TypeReady); ready.Reason == prv1.ReasonNotObserved {
		if err != nil {
			t.Errorf("Observe returned an error wrapped with reconciler.ReadyAssessed but did not set the Ready condition (still %s); "+
				"ReadyAssessed promises that Observe set Ready: %v", prv1.ReasonNotObserved, err)
			return obs, err
		}
		t.Errorf("Observe succeeded but did not set the Ready condition (still %s): "+
			"every successful Observe must set Ready", prv1.ReasonNotObserved)
	}
	return obs, err
}
