package sim

import (
	"math"
	"testing"
	"time"
)

// The §10 CPU cases (plan §4). Every case runs 20 seeds and must pass in each;
// -short runs 5.

func seeds(t *testing.T) []uint64 {
	n := 20
	if testing.Short() {
		n = 5
	}
	s := make([]uint64, n)
	for i := range s {
		s[i] = uint64(i + 1)
	}
	return s
}

func forSeeds(t *testing.T, check func(t *testing.T, seed uint64)) {
	t.Helper()
	for _, seed := range seeds(t) {
		t.Run("seed"+itoa(seed), func(t *testing.T) {
			t.Parallel()
			check(t, seed)
		})
	}
}

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// index returns the first sample at or after at.
func index(tr *Trace, at time.Duration) int {
	for i, a := range tr.At {
		if a >= at {
			return i
		}
	}
	return len(tr.At)
}

// settled is the mean of S over [from, to).
func settled(tr *Trace, from, to time.Duration) float64 {
	var s float64
	var n int
	for i, a := range tr.At {
		if a >= from && a < to {
			s += tr.Results[i].Slots
			n++
		}
	}
	return s / float64(n)
}

// meanCPU is the mean CPU in cores over [from, to).
func meanCPU(tr *Trace, from, to time.Duration) float64 {
	var u, d float64
	for i, a := range tr.At {
		if a >= from && a < to {
			u += tr.Samples[i].Usage.Seconds()
			d += tr.Samples[i].Dt.Seconds()
		}
	}
	return u / d
}

// monotoneUntil checks that S never falls before it first reaches level.
func monotoneUntil(t *testing.T, tr *Trace, from int, level float64) {
	t.Helper()
	for i := from + 1; i < len(tr.Results); i++ {
		if tr.Results[i-1].Slots >= level {
			return
		}
		if tr.Results[i].Slots < tr.Results[i-1].Slots-1e-9 {
			t.Errorf("not monotone: S %.3f then %.3f at %v, before reaching %.2f",
				tr.Results[i-1].Slots, tr.Results[i].Slots, tr.At[i], level)
			return
		}
	}
}

func TestHeadline(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed uint64) {
		cfg := measured(seed)
		tr := Run(cfg)
		s := settled(tr, 60*time.Second, cfg.Duration)
		u := meanCPU(tr, 60*time.Second, cfg.Duration) / cfg.Limit
		if s < 4 || s > 5 {
			t.Errorf("settled at %.2f slots, want between 4 and 5", s)
		}
		if math.Abs(u-0.9) > 0.03 {
			t.Errorf("mean CPU %.1f%% of the limit, want within 3 points of 90%%", 100*u)
		}
		monotoneUntil(t, tr, 0, 0.95*s)
	})
}

// §4.3's table rows, with the plan's pass conditions.

func TestStartFromOneSlot(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed uint64) {
		cfg := measured(seed)
		tr := Run(cfg)
		s := settled(tr, 60*time.Second, cfg.Duration)
		monotoneUntil(t, tr, 0, 0.95*s)
		if got := tr.Results[19].Slots; math.Abs(got-s)/s > 0.05 {
			t.Errorf("after 20 samples S = %.2f, more than 5%% from the settled %.2f", got, s)
		}
	})
}

// stepCase runs base until at, then changed; target is the settled S of a run
// with the changed configuration from the start.
func stepCase(t *testing.T, seed uint64, change func(*Config, time.Duration), within int, tol float64) {
	t.Helper()
	const at = 90 * time.Second
	cfg := measured(seed)
	cfg.Duration = 240 * time.Second
	change(&cfg, at)
	tr := Run(cfg)

	ref := measured(seed + 1000)
	ref.Duration = 180 * time.Second
	change(&ref, 0)
	target := settled(Run(ref), 60*time.Second, ref.Duration)

	i0 := index(tr, at)
	ok := -1
	for i := i0; i < len(tr.Results) && i <= i0+within; i++ {
		if math.Abs(tr.Results[i].Slots-target)/target <= tol {
			ok = i
			break
		}
	}
	if ok < 0 {
		t.Errorf("not within %.0f%% of the new target %.2f within %d samples: %v", 100*tol, target, within, slotsFrom(tr, i0, within+2))
		return
	}
	// No oscillation after: S stays within 15% of the target.
	for i := ok; i < len(tr.Results); i++ {
		if math.Abs(tr.Results[i].Slots-target)/target > 0.15 {
			t.Errorf("oscillation: S %.2f at %v, more than 15%% from the target %.2f", tr.Results[i].Slots, tr.At[i], target)
			return
		}
	}
}

func slotsFrom(tr *Trace, i0, n int) []float64 {
	var out []float64
	for i := i0; i < len(tr.Results) && i < i0+n; i++ {
		out = append(out, math.Round(tr.Results[i].Slots*100)/100)
	}
	return out
}

func classAfter(at time.Duration, before, after Class) func(time.Duration) Class {
	return func(t time.Duration) Class {
		if t >= at {
			return after
		}
		return before
	}
}

func TestCostDoubles(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed uint64) {
		stepCase(t, seed, func(c *Config, at time.Duration) {
			base := Class{CPU: 23 * time.Millisecond, Wait: 400 * time.Millisecond, Jitter: 0.1}
			heavy := base
			heavy.CPU = 46 * time.Millisecond
			c.Queues[0].Class = classAfter(at, base, heavy)
		}, 2, 0.10)
	})
}

func limitAfter(at time.Duration, before, after float64) func(time.Duration) float64 {
	return func(t time.Duration) float64 {
		if t >= at {
			return after
		}
		return before
	}
}

func TestLimitHalves(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed uint64) {
		stepCase(t, seed, func(c *Config, at time.Duration) { c.LimitAt = limitAfter(at, 0.25, 0.125) }, 2, 0.10)
	})
}

func TestLimitDoubles(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed uint64) {
		stepCase(t, seed, func(c *Config, at time.Duration) { c.LimitAt = limitAfter(at, 0.25, 0.5) }, 3, 0.10)
	})
}

func TestWaitDoubles(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed uint64) {
		const at = 90 * time.Second
		cfg := measured(seed)
		cfg.Duration = 240 * time.Second
		base := Class{CPU: 23 * time.Millisecond, Wait: 400 * time.Millisecond, Jitter: 0.1}
		slow := base
		slow.Wait = 800 * time.Millisecond
		cfg.Queues[0].Class = classAfter(at, base, slow)
		tr := Run(cfg)
		from := at + 15*time.Second
		u := meanCPU(tr, from, cfg.Duration) / cfg.Limit
		if u < 0.85 || u > 0.95 {
			t.Errorf("from 15 samples after the wait doubled, mean CPU %.1f%% of the limit, want 85–95%%", 100*u)
		}
	})
}

func TestUsageNoise(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed uint64) {
		cfg := measured(seed)
		cfg.UsageNoise = 0.10
		tr := Run(cfg)
		u := meanCPU(tr, 60*time.Second, cfg.Duration) / cfg.Limit
		if u < 0.85 || u > 0.95 {
			t.Errorf("mean CPU %.1f%% of the limit, want 85–95%%", 100*u)
		}
		var s, s2 float64
		var n int
		for i, a := range tr.At {
			if a >= 60*time.Second {
				x := tr.Results[i].Slots
				s += x
				s2 += x * x
				n++
			}
		}
		m := s / float64(n)
		if sd := math.Sqrt(s2/float64(n) - m*m); sd >= 0.3 {
			t.Errorf("slots' standard deviation %.2f, want under 0.3", sd)
		}
	})
}

// The first review's H1 cases.

// burstyBackground is base cores, with bursts to burst in a share of seconds.
func burstyBackground(seed uint64, base, burst, share float64) func(time.Duration) float64 {
	return func(t time.Duration) float64 {
		sec := uint64(t / time.Second)
		h := (sec*0x9e3779b97f4a7c15 ^ seed*0xbf58476d1ce4e5b9) >> 33
		if float64(h%1000)/1000 < share {
			return burst
		}
		return base
	}
}

func after(at time.Duration) func(time.Duration) bool {
	return func(t time.Duration) bool { return t >= at }
}

func TestH1BiasedBackground(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed uint64) {
		const start = 15 * time.Second
		cfg := Config{
			Seed: seed, Duration: 120 * time.Second, Procs: 2, NodeCPUs: 4, Limit: 0.5, Ceiling: 16,
			Background: burstyBackground(seed, 0.015, 0.060, 0.3),
			Queues: []Queue{{Name: "q", Class: fixedClass(Class{CPU: 3 * time.Millisecond, Wait: 800 * time.Millisecond, Jitter: 0.1}),
				Active: after(start)}},
			Model: defaultModel(),
		}
		tr := Run(cfg)
		i0 := index(tr, start)
		reached := -1
		for i := i0; i < len(tr.Results) && i <= i0+10; i++ {
			if tr.Capacity[i] == cfg.Ceiling {
				reached = i
				break
			}
		}
		if reached < 0 {
			t.Fatalf("not at the ceiling within 10 samples of the work starting: %v", slotsFrom(tr, i0, 12))
		}
		for i := reached; i < len(tr.Results); i++ {
			if tr.Capacity[i] == 1 {
				t.Fatalf("back at the floor at %v", tr.At[i])
			}
		}
	})
}

func TestH1NoisyBackground(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed uint64) {
		cfg := Config{
			Seed: seed, Duration: 10 * time.Minute, Procs: 2, NodeCPUs: 4, Limit: 0.5, Ceiling: 16,
			Queues: []Queue{{Name: "q", Class: fixedClass(Class{CPU: 3 * time.Millisecond, Wait: 800 * time.Millisecond, Jitter: 0.1}),
				Active: always}},
			Model: defaultModel(),
		}
		noise := newNoise(seed)
		cfg.Background = func(t time.Duration) float64 { return math.Max(0, 0.015+0.010*noise.at(t)) }
		tr := Run(cfg)
		warm := -1
		for i := range tr.Results {
			if tr.Capacity[i] == cfg.Ceiling {
				warm = i
				break
			}
		}
		if warm < 0 {
			t.Fatal("never reached the ceiling")
		}
		for i := warm; i < len(tr.Results); i++ {
			if tr.Capacity[i] < cfg.Ceiling {
				t.Fatalf("below the ceiling at %v: %d", tr.At[i], tr.Capacity[i])
			}
		}
	})
}

func TestH1BurstsAboveOneSlot(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed uint64) {
		const start = 15 * time.Second
		cfg := measured(seed)
		cfg.Background = burstyBackground(seed, 0.020, 0.100, 0.3)
		cfg.Queues[0].Active = after(start)
		tr := Run(cfg)
		s := settled(tr, start+60*time.Second, cfg.Duration)
		monotoneUntil(t, tr, index(tr, start), 0.95*s)
		u := meanCPU(tr, start+60*time.Second, cfg.Duration) / cfg.Limit
		if u < 0.85 || u > 0.95 {
			t.Errorf("mean CPU %.1f%% of the limit, want 85–95%%", 100*u)
		}
	})
}

// noise is a per-second N(0,1) sequence, the same for every millisecond of a
// second.
type noise struct{ seed uint64 }

func newNoise(seed uint64) noise { return noise{seed} }

func (n noise) at(t time.Duration) float64 {
	sec := uint64(t/time.Second) + 1
	// Box–Muller on two hashed uniforms.
	h1 := (sec*0x9e3779b97f4a7c15 ^ n.seed*0xbf58476d1ce4e5b9) >> 11
	h2 := (sec*0xc2b2ae3d27d4eb4f ^ n.seed*0x165667b19e3779f9) >> 11
	u1 := (float64(h1%(1<<52)) + 0.5) / (1 << 52)
	u2 := (float64(h2%(1<<52)) + 0.5) / (1 << 52)
	return math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
}

// The fifth review's L1: the CPU change rule's false-restart rate.

func TestFalseRestarts(t *testing.T) {
	for _, tc := range []struct {
		name  string
		class Class
	}{
		{"0.47s", Class{CPU: 23 * time.Millisecond, Wait: 400 * time.Millisecond, Jitter: 0.1}},
		{"30s", Class{CPU: 1500 * time.Millisecond, Wait: 28500 * time.Millisecond, Jitter: 0.1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			forSeeds(t, func(t *testing.T, seed uint64) {
				cfg := measured(seed)
				cfg.Duration = time.Hour
				cfg.Queues[0].Class = fixedClass(tc.class)
				tr := Run(cfg)
				n := 0
				for i, r := range tr.Results {
					if i < 120 || !r.CPU.Restarted {
						continue
					}
					n++
					if r.Slots > tr.Results[i-1].Slots+1e-9 {
						t.Errorf("a restart raised the slots at %v: %.2f to %.2f", tr.At[i], tr.Results[i-1].Slots, r.Slots)
					}
				}
				t.Logf("restarts after warm-up: %d in an hour", n)
			})
		})
	}
}
