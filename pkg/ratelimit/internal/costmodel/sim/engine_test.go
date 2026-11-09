package sim

import (
	"math"
	"testing"
	"time"
)

// TestEngineMatchesTheMeasuredCurve checks the plant, not the model: at fixed
// capacities, the simulated CPU and throughput must land within 10% of the
// GKE measurement's per-capacity means (design §1's table).
func TestEngineMatchesTheMeasuredCurve(t *testing.T) {
	measuredCurve := []struct {
		slots int
		used  float64 // cores
		tput  float64 // per second
	}{
		{1, 0.054, 2.33}, {2, 0.105, 4.50}, {3, 0.153, 6.65}, {4, 0.203, 8.77},
		{5, 0.242, 10.34}, {6, 0.248, 10.90}, {7, 0.249, 11.10}, {16, 0.250, 11.35},
	}
	for _, m := range measuredCurve {
		cfg := measured(1)
		cfg.Duration = 120 * time.Second
		cfg.Fixed = m.slots
		w := summarize(Run(cfg), 20*time.Second)
		t.Logf("S=%2d  CPU %.3f (measured %.3f)  X %.2f (measured %.2f)  stall %.1f%%", m.slots, w.used, m.used, w.tput, m.tput, 100*w.stall)
		if math.Abs(w.used-m.used)/m.used > 0.10 {
			t.Errorf("S=%d: CPU %.3f, measured %.3f: more than 10%% apart", m.slots, w.used, m.used)
		}
		if math.Abs(w.tput-m.tput)/m.tput > 0.10 {
			t.Errorf("S=%d: throughput %.2f, measured %.2f: more than 10%% apart", m.slots, w.tput, m.tput)
		}
	}
}

// TestEngineStall checks PSI's "some": no stall with one runnable task per CPU
// and room in the quota; stall when throttled.
func TestEngineStall(t *testing.T) {
	cfg := measured(1)
	cfg.Duration = 60 * time.Second
	cfg.Fixed = 1
	tr := Run(cfg)
	var stall, dt float64
	for i := range tr.At[5:] {
		stall += tr.Samples[i+5].Stall.Seconds()
		dt += tr.Samples[i+5].Dt.Seconds()
	}
	if s := stall / dt; s > 0.02 {
		t.Errorf("one slot: stall share %.3f, want about 0", s)
	}
	// At the cap, calibrated to the steady GKE arms: 56% and 59%.
	cfg.Fixed = 16
	w := summarize(Run(cfg), 20*time.Second)
	if w.stall < 0.50 || w.stall > 0.65 {
		t.Errorf("sixteen slots at the cap: stall share %.3f, want the measured 0.56 to 0.59 (±0.06)", w.stall)
	}
	// Growing with the slots.
	prev := -1.0
	for _, s := range []int{1, 3, 5, 7, 16} {
		cfg.Fixed = s
		w := summarize(Run(cfg), 20*time.Second)
		if w.stall < prev {
			t.Errorf("stall at %d slots %.3f, below %.3f at fewer", s, w.stall, prev)
		}
		prev = w.stall
	}
}

// TestEngineThrottlingIsTheQuotas checks nr_throttled: a node share under the
// quota throttles no period; demand over the quota with no share throttles
// every period with demand.
func TestEngineThrottlingIsTheQuotas(t *testing.T) {
	periods := func(cfg Config) (throttled, total uint64) {
		tr := Run(cfg)
		for i, s := range tr.Samples {
			if tr.At[i] >= 10*time.Second {
				throttled += s.Throttled
				total += s.Periods
			}
		}
		return throttled, total
	}
	cfg := measured(1)
	cfg.Duration, cfg.Fixed = 40*time.Second, 8
	cfg.Share = func(time.Duration) float64 { return 0.125 }
	if thr, n := periods(cfg); thr != 0 || n == 0 {
		t.Errorf("a 0.125 share under a 250m quota: %d of %d periods throttled, want 0", thr, n)
	}
	cfg.Share = nil
	cfg.Fixed = 16
	if thr, n := periods(cfg); thr != n || n == 0 {
		t.Errorf("demand over the quota, no share: %d of %d periods throttled, want all", thr, n)
	}
}
