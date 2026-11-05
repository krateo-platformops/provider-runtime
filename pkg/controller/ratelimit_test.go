package controller

import (
	"context"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
)

type runnables struct{ added []manager.Runnable }

func (r *runnables) Add(x manager.Runnable) error {
	r.added = append(r.added, x)
	return nil
}

// Every rate-limit option is safe to leave unset: the zero value builds no
// governor, and Enabled alone builds a working one.
func TestSetupRateLimitGovernor(t *testing.T) {
	t.Run("unset is off", func(t *testing.T) {
		o, mgr := DefaultOptions(), &runnables{}
		if err := o.SetupRateLimitGovernor(mgr); err != nil {
			t.Fatal(err)
		}
		if o.RateLimitGovernor != nil || len(mgr.added) != 0 {
			t.Errorf("with RateLimit unset: governor %v, %d runnables added; want none", o.RateLimitGovernor, len(mgr.added))
		}
	})

	t.Run("Enabled alone", func(t *testing.T) {
		// Even MaxConcurrentReconciles is unset here.
		o, mgr := Options{RateLimit: RateLimitOptions{Enabled: true}}, &runnables{}
		if err := o.SetupRateLimitGovernor(mgr); err != nil {
			t.Fatalf("Enabled alone failed: %v", err)
		}
		g := o.RateLimitGovernor
		if g == nil || len(mgr.added) != 1 || mgr.added[0] != manager.Runnable(g) {
			t.Fatalf("governor %v, runnables %v; want the governor built and added to the manager", g, mgr.added)
		}
		if g.Ceiling() != 1 {
			t.Errorf("ceiling = %d with MaxConcurrentReconciles unset, want 1", g.Ceiling())
		}
		g.Observe(ratelimit.Signal{Key: "h", Throttled: true, RetryAfter: time.Minute})
		if _, d := g.Acquire(context.Background(), "h"); d == nil {
			t.Error("the rate-limit input is on but a throttled key let a call through")
		}
	})

	t.Run("ceiling from MaxConcurrentReconciles", func(t *testing.T) {
		o := DefaultOptions()
		o.MaxConcurrentReconciles = 5
		o.RateLimit.Enabled = true
		if err := o.SetupRateLimitGovernor(&runnables{}); err != nil {
			t.Fatal(err)
		}
		if got := o.RateLimitGovernor.Ceiling(); got != 5 {
			t.Errorf("ceiling = %d, want 5", got)
		}
	})

	t.Run("saturation alone ignores signals", func(t *testing.T) {
		o := Options{MaxConcurrentReconciles: 2, RateLimit: RateLimitOptions{Saturation: ratelimit.SaturationOptions{Enabled: true}}}
		if err := o.SetupRateLimitGovernor(&runnables{}); err != nil {
			t.Fatal(err)
		}
		g := o.RateLimitGovernor
		if g == nil {
			t.Fatal("saturation enabled built no governor")
		}
		g.Observe(ratelimit.Signal{Key: "h", Throttled: true, RetryAfter: time.Minute})
		if _, d := g.Acquire(context.Background(), "h"); d != nil {
			t.Errorf("a signal gated a call with only saturation enabled: %+v", d)
		}
	})
}
