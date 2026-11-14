package controller

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
)

// The capacity model's options reach the governor through
// RateLimit.Saturation, like the other saturation options: with CapacityModel
// set the governor's sampler runs the law, and without it it doesn't.
func TestSetupRateLimitGovernorCapacityModel(t *testing.T) {
	for _, model := range []bool{false, true} {
		o := Options{MaxConcurrentReconciles: 3, RateLimit: RateLimitOptions{Saturation: ratelimit.SaturationOptions{
			Enabled:       true,
			CapacityModel: model,
			CgroupDir:     filepath.Join("..", "ratelimit", "testdata", "cpu-limited"),
			SamplePeriod:  time.Hour,
		}}}
		if err := o.SetupRateLimitGovernor(&runnables{}); err != nil {
			t.Fatal(err)
		}
		g := o.RateLimitGovernor
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = g.Start(ctx); close(done) }()
		for deadline := time.Now().Add(5 * time.Second); g.Snapshot().CPU.Limit == nil; time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatal("the sampler never took its first sample")
			}
		}
		m := g.Snapshot().CPU.ModelCapacity
		cancel()
		<-done
		switch {
		case model && (m == nil || *m != 3):
			t.Errorf("with CapacityModel set the model's capacity = %v, want the ceiling 3", m)
		case !model && m != nil:
			t.Errorf("with CapacityModel unset the model's capacity is reported as %v", *m)
		}
	}
}

// A capacity model setting out of range is an error, like the other saturation
// settings; left unset, each has a working default.
func TestSetupRateLimitGovernorRejectsBadCapacityModelSettings(t *testing.T) {
	for name, sat := range map[string]ratelimit.SaturationOptions{
		"a CPU target over 1":               {CPUTarget: 1.5},
		"a negative CPU target":             {CPUTarget: -0.5},
		"a negative minimum limit":          {MinCPULimit: -1},
		"a negative model interval":         {ModelInterval: -time.Second},
		"a CPU target over 1, the model on": {CPUTarget: 1.5, CapacityModel: true},
	} {
		sat.Enabled = true
		o := Options{RateLimit: RateLimitOptions{Saturation: sat}}
		if err := o.SetupRateLimitGovernor(&runnables{}); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	o := Options{RateLimit: RateLimitOptions{Saturation: ratelimit.SaturationOptions{Enabled: true, CapacityModel: true}}}
	if err := o.SetupRateLimitGovernor(&runnables{}); err != nil {
		t.Errorf("CapacityModel alone, every setting unset: %v", err)
	}
}
