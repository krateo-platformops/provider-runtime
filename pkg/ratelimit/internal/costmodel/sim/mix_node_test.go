package sim

import (
	"math"
	"testing"
	"time"

	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit/internal/costmodel"
)

// M1: a mix shift that caps the CPU.

func during(from, to time.Duration) func(time.Duration) bool {
	return func(t time.Duration) bool { return t >= from && t < to }
}

func mixShift(seed uint64, seen bool) (Config, time.Duration) {
	cheap := Class{CPU: time.Millisecond, Wait: 2 * time.Second, Jitter: 0.1}
	exp := Class{CPU: 23 * time.Millisecond, Wait: 400 * time.Millisecond, Jitter: 0.1}
	cfg := Config{Seed: seed, Procs: 2, NodeCPUs: 4, Limit: 0.25, Ceiling: 32, Model: defaultModel()}
	var shift time.Duration
	if seen {
		shift = 120 * time.Second
		cfg.Duration = 240 * time.Second
		cfg.Queues = []Queue{
			{Name: "exp", Class: fixedClass(exp), Active: func(t time.Duration) bool { return t < 60*time.Second || t >= shift }},
			{Name: "cheap", Class: fixedClass(cheap), Active: during(60*time.Second, shift)},
		}
	} else {
		shift = 60 * time.Second
		cfg.Duration = 180 * time.Second
		cfg.Queues = []Queue{
			{Name: "cheap", Class: fixedClass(cheap), Active: during(0, shift)},
			{Name: "exp", Class: fixedClass(exp), Active: after(shift)},
		}
	}
	return cfg, shift
}

func TestMixShiftSeenBefore(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed uint64) {
		cfg, shift := mixShift(seed, true)
		tr := Run(cfg)
		target := settled(tr, shift+60*time.Second, cfg.Duration)
		i0 := index(tr, shift)
		capped, out := 0, -1
		for i := i0; i < len(tr.Results); i++ {
			if tr.Results[i].CPU.Kind == costmodel.Capped {
				capped++
				continue
			}
			if capped > 0 {
				out = i
				break
			}
		}
		if capped > 0 && (out < 0 || out-i0 > 6) {
			t.Errorf("not out of the cap within 6 samples of the shift: %v", slotsFrom(tr, i0, 12))
		}
		// At most one crossing of the target (with a 5% band against noise).
		crossings := 0
		side := 1
		if tr.Results[i0].Slots < target {
			side = -1
		}
		for i := i0 + 1; i < len(tr.Results); i++ {
			s := tr.Results[i].Slots
			switch {
			case side > 0 && s < 0.95*target:
				crossings, side = crossings+1, -1
			case side < 0 && s > 1.05*target:
				crossings, side = crossings+1, 1
			}
		}
		if crossings > 1 {
			t.Errorf("crossed the target %.2f %d times after the shift: %v", target, crossings, slotsFrom(tr, i0, 30))
		}
	})
}

func TestMixShiftNeverSeen(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed uint64) {
		cfg, shift := mixShift(seed, false)
		tr := Run(cfg)
		for i := index(tr, shift); i < len(tr.Results); i++ {
			r := tr.Results[i]
			if r.CPU.Kind != costmodel.Capped {
				continue
			}
			prev := tr.Results[i-1].Slots
			if r.Slots > prev+1e-9 {
				t.Errorf("rose while capped at %v: %.2f to %.2f", tr.At[i], prev, r.Slots)
			}
			theta := (0.9*cfg.Limit - r.CPU.Background) / (cfg.Limit - r.CPU.Background)
			if r.Slots > math.Max(1, theta*r.CPU.InFlight)+1e-6 {
				t.Errorf("capped at %v: S %.2f above θN̄ = %.2f", tr.At[i], r.Slots, theta*r.CPU.InFlight)
			}
		}
	})
}

// M2: no cpu.max, a node that gives the container a share of its CPUs.

func shareSteps(t time.Duration) float64 {
	if t >= 300*time.Second && t < 600*time.Second {
		return 0.8
	}
	return 0.4
}

func nodeShare(seed uint64) Config {
	return Config{
		Seed: seed, Duration: 900 * time.Second, Procs: 4, NodeCPUs: 4, Ceiling: 32, Share: shareSteps,
		Queues: []Queue{{Name: "q", Class: fixedClass(Class{CPU: 23 * time.Millisecond, Wait: 400 * time.Millisecond, Jitter: 0.1}),
			Active: always}},
		Model: defaultModel(),
	}
}

func TestNodeShare(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed uint64) {
		cfg := nodeShare(seed)
		tr := Run(cfg)
		learned := -1
		for i := 1; i < len(tr.Results); i++ {
			if tr.Results[i].CPU.Phase == costmodel.Held && tr.Results[i-1].CPU.Phase != costmodel.Held {
				learned = i
				break
			}
		}
		if learned < 0 || learned+1 >= len(tr.Results) {
			t.Fatal("no limit learned")
		}
		r := tr.Results[learned]
		want := (0.9*r.CPU.Leff - r.CPU.Background) / r.CPU.PerSlot
		got := tr.Results[learned+1].Slots
		if math.Abs(got-want)/want > 0.10 {
			t.Errorf("after learning %.3f cores at %v: S %.2f, the law's value %.2f", r.CPU.Leff, tr.At[learned], got, want)
		}
		for _, w := range [][2]time.Duration{{150 * time.Second, 300 * time.Second}, {450 * time.Second, 600 * time.Second}, {750 * time.Second, 900 * time.Second}} {
			over, n := 0, 0
			for i, a := range tr.At {
				if a >= w[0] && a < w[1] {
					n++
					if tr.Results[i].CPU.Stall > 50 {
						over++
					}
				}
			}
			if float64(over) > 0.10*float64(n) {
				t.Errorf("stall over the threshold in %d of %d samples in [%v, %v)", over, n, w[0], w[1])
			}
		}
		found := false
		for from := 300 * time.Second; from+10*time.Second <= 400*time.Second; from += time.Second {
			if meanCPU(tr, from, from+10*time.Second) >= 0.9*0.9*0.8 {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("the doubled share not found within 10 windows: CPU over [300s, 400s) %.3f", meanCPU(tr, 300*time.Second, 400*time.Second))
		}
	})
}

// M2-a: a node that gives 4 cores, with a blip and a dip to 0.5 core.

func blipNode(seed uint64, from, to time.Duration) Config {
	return Config{
		Seed: seed, Duration: 150 * time.Second, Procs: 4, NodeCPUs: 4, Ceiling: 64,
		Share: func(t time.Duration) float64 {
			if t >= from && t < to {
				return 0.5
			}
			return 4
		},
		Queues: []Queue{{Name: "q", Class: fixedClass(Class{CPU: 23 * time.Millisecond, Wait: 400 * time.Millisecond, Jitter: 0.1}),
			Active: always}},
		Model: defaultModel(),
	}
}

func TestBlip(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed uint64) {
		cfg := blipNode(seed, 60*time.Second, 61*time.Second)
		tr := Run(cfg)
		blocked := 0
		for i, r := range tr.Results {
			if r.CPU.Phase == costmodel.Held {
				t.Fatalf("a limit learned at %v from a 1s blip", tr.At[i])
			}
			if r.Held {
				blocked++
			}
		}
		if blocked > 2 {
			t.Errorf("rises held in %d samples, want only the blip's (at most 2)", blocked)
		}
	})
}

func TestDip(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed uint64) {
		const from, to = 60 * time.Second, 75 * time.Second
		cfg := blipNode(seed, from, to)
		tr := Run(cfg)
		learnedAt := time.Duration(-1)
		for i, r := range tr.Results {
			if r.CPU.Phase == costmodel.Held {
				learnedAt = tr.At[i]
				break
			}
		}
		if learnedAt < 0 {
			t.Fatal("no limit learned from a 15s dip")
		}
		if learnedAt < from+10*time.Second {
			t.Errorf("learned at %v, before the dip spanned τ (10s)", learnedAt)
		}
		pre := meanCPU(tr, from-20*time.Second, from)
		back := false
		for i := index(tr, to); i < len(tr.Results) && tr.At[i] <= to+11*time.Second; i++ {
			if tr.Used(i) >= 0.9*pre {
				back = true
				break
			}
		}
		if !back {
			t.Errorf("CPU not back to 90%% of the pre-onset %.2f cores within one clear window of the dip's end", pre)
		}
	})
}
