package sim

import (
	"fmt"
	"math"
	"time"

	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit/internal/costmodel"
)

// The gate: the plan's §4.5 conditions, on the cost model and on the
// utilization baseline (design §17.6). Each case is a configuration and a check
// of one run; the check states the conditions and, for the cost model only,
// the mechanisms it counts on.

const (
	rho  = 0.9
	rbar = (1 - rho) / 2
)

// controller is what runs a case: the cost model, or the baseline at a
// multiple of its committed gains.
type controller struct {
	name string
	cost bool
	gain float64
}

var (
	ctlCost = controller{name: "cost model", cost: true}
)

func ctlBaseline(gain float64) controller {
	return controller{name: fmt.Sprintf("baseline ×%g", gain), gain: gain}
}

func (c controller) apply(cfg *Config) {
	if c.cost {
		return
	}
	g := baselineGains
	b := Baseline{Kp: g.Kp * c.gain, Ki: g.Ki * c.gain, TauU: g.TauU, Target: rho, PressureThreshold: 50}
	cfg.Controller = func() Controller { return NewBaseline(b) }
}

// verdict is one run's outcome.
type verdict struct {
	fails, reports []string
	invalid        string
}

// gctx is a check's view of one run.
type gctx struct {
	tr   *Trace
	cfg  Config
	cost bool
	v    *verdict
}

func (g *gctx) fail(f string, a ...any)   { g.v.fails = append(g.v.fails, fmt.Sprintf(f, a...)) }
func (g *gctx) report(f string, a ...any) { g.v.reports = append(g.v.reports, fmt.Sprintf(f, a...)) }

func (g *gctx) S(i int) float64 { return g.tr.Results[i].Slots }
func (g *gctx) n() int          { return len(g.tr.Results) }

// limit is the sample's cpu.max in cores.
func (g *gctx) limit(i int) float64 { return g.tr.Samples[i].Limit }

// capped applies §3.2's capped test to the sample, for either controller.
func (g *gctx) capped(i int) bool {
	s := g.tr.Samples[i]
	if s.Limit <= 0 || !s.Throttling || s.Periods < 1 || s.Throttled < s.Periods {
		return false
	}
	slack := math.Min(s.Limit*s.Period.Seconds(), float64(s.Procs)*s.Slice.Seconds())
	return s.Usage.Seconds() >= s.Limit*s.Dt.Seconds()-slack
}

func (g *gctx) stall(i int) float64 {
	s := g.tr.Samples[i]
	return 100 * s.Stall.Seconds() / s.Dt.Seconds()
}

// band is §4.5's asymmetric band around S*: θ⌊S*⌋ to S*(1 + 3r̄), with θ = ρ
// (no background in the band cases).
type band struct{ sStar, lo, hi float64 }

func bandOf(sStar float64) band {
	return band{sStar, rho * math.Floor(sStar), sStar * (1 + 3*rbar)}
}

func (b band) in(S float64) bool { return S >= b.lo-1e-9 && S <= b.hi+1e-9 }

// entry is the first sample at or after from in the band, or -1.
func (g *gctx) entry(b band, from int) int {
	for i := from; i < g.n(); i++ {
		if b.in(g.S(i)) {
			return i
		}
	}
	return -1
}

// outside checks §4.5's rule after entry: at most 5% of samples outside the
// band. For the cost model, untrusted samples are judged with their own
// relative standard error when it exists, and excluded and counted before.
func (g *gctx) outside(b band, from, to int) {
	if from < 0 {
		return
	}
	out, n, excluded := 0, 0, 0
	for i := from; i < to && i < g.n(); i++ {
		hi := b.hi
		if g.cost {
			c := g.tr.Results[i].CPU
			if !c.Trusted {
				if c.RelSE <= 0 || math.IsInf(c.RelSE, 0) || math.IsNaN(c.RelSE) {
					excluded++
					continue
				}
				hi = b.sStar * (1 + 3*math.Max(c.RelSE, rbar))
			}
		}
		n++
		if S := g.S(i); S < b.lo-1e-9 || S > hi+1e-9 {
			out++
		}
	}
	if excluded > 0 {
		g.report("%d untrusted samples without a standard error excluded from the band rule", excluded)
	}
	if n > 0 && float64(out) > 0.05*float64(n) {
		g.fail("outside the band [%.2f, %.2f] in %d of %d samples after entry", b.lo, b.hi, out, n)
	}
}

// monotone checks §4.5's attributed monotone condition over (from, to].
func (g *gctx) monotone(from, to int) {
	if to < 0 {
		to = g.n() - 1
	}
	for i := from + 1; i <= to && i < g.n(); i++ {
		prev, S := g.S(i-1), g.S(i)
		if S >= prev-1e-9 {
			continue
		}
		if g.capped(i) {
			continue
		}
		u := g.tr.Used(i)
		L := g.limit(i)
		if L > 0 && u > rho*L {
			trusted := g.cost && g.tr.Results[i].CPU.Trusted
			if !trusted {
				g.report("an untrusted fall at %v: %.2f to %.2f with U over ρL", g.tr.At[i], prev, S)
				continue
			}
			if S >= prev*(1-3*rbar)-1e-9 {
				continue
			}
			g.fail("a trusted fall at %v larger than 3r̄: %.2f to %.2f", g.tr.At[i], prev, S)
			continue
		}
		g.fail("an unattributed fall at %v: %.2f to %.2f (U %.3f, not capped)", g.tr.At[i], prev, S, u)
	}
}

// slotsFrom lists S from i0, for messages.
func (g *gctx) slotsFrom(i0, k int) []float64 {
	var out []float64
	for i := max(i0, 0); i < g.n() && i < i0+k; i++ {
		out = append(out, math.Round(g.S(i)*100)/100)
	}
	return out
}

func index(tr *Trace, at time.Duration) int {
	for i, a := range tr.At {
		if a >= at {
			return i
		}
	}
	return len(tr.At)
}

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

func ceilSec(d float64) int { return int(math.Ceil(d - 1e-9)) }

// c1 is §4.5's count from one slot to θ⌊S*⌋ on a frozen plant.
func c1(p frozen) int { return p.nStart * (1 + ceilSec(p.dStar)) }

func classAfter(at time.Duration, before, after Class) func(time.Duration) Class {
	return func(t time.Duration) Class {
		if t >= at {
			return after
		}
		return before
	}
}

func limitAfter(at time.Duration, before, after float64) func(time.Duration) float64 {
	return func(t time.Duration) float64 {
		if t >= at {
			return after
		}
		return before
	}
}

func after(at time.Duration) func(time.Duration) bool {
	return func(t time.Duration) bool { return t >= at }
}

func during(from, to time.Duration) func(time.Duration) bool {
	return func(t time.Duration) bool { return t >= from && t < to }
}

// gateCase is one §4.5 case.
type gateCase struct {
	name  string
	cfg   func(seed uint64) Config
	check func(g *gctx)
}

func countRestarts(g *gctx, from int) {
	if !g.cost {
		return
	}
	n := 0
	for i := from; i < g.n(); i++ {
		if g.tr.Results[i].CPU.CRestarted {
			n++
		}
	}
	g.report("ĉ restarts not caused by a scenario change: %d", n)
}

var baseClass = Class{CPU: 23 * time.Millisecond, Wait: 400 * time.Millisecond, Jitter: 0.1}

func heavyClass() Class {
	c := baseClass
	c.CPU = 46 * time.Millisecond
	return c
}

const (
	stepAt  = 90 * time.Second
	drainAt = 3600 * time.Second
)

func gateCases() []gateCase {
	base := frozenPlants["base"]
	return []gateCase{
		{name: "headline", cfg: measured, check: func(g *gctx) {
			s := settled(g.tr, 60*time.Second, g.cfg.Duration)
			u := meanCPU(g.tr, 60*time.Second, g.cfg.Duration) / g.cfg.Limit
			if s < 4 || s > 5 {
				g.fail("settled at %.2f slots, want between 4 and 5", s)
			}
			if math.Abs(u-rho) > 0.03 {
				g.fail("mean CPU %.1f%% of the limit, want within 3 points of 90%%", 100*u)
			}
			g.monotone(0, g.entry(bandOf(base.sStar), 0))
		}},
		{name: "start from one slot", cfg: measured, check: func(g *gctx) {
			b := bandOf(base.sStar)
			e := g.entry(b, 0)
			if e < 0 || e+1 > c1(base)+1 {
				g.fail("into the band [%.2f, %.2f] at sample %d, want by %d: %v", b.lo, b.hi, e+1, c1(base)+1, g.slotsFrom(0, 12))
			}
			g.outside(b, e, g.n())
			countRestarts(g, 0)
		}},
		{name: "cost doubles", cfg: func(seed uint64) Config {
			c := measured(seed)
			c.Duration = 240 * time.Second
			c.Queues[0].Class = classAfter(stepAt, baseClass, heavyClass())
			return c
		}, check: func(g *gctx) {
			p := frozenPlants["cost46"]
			b := bandOf(p.sStar)
			i0 := index(g.tr, stepAt)
			const ell, wc = 2, 2
			e := g.entry(b, i0)
			if e < 0 || e > i0+ell+wc {
				g.fail("into the band [%.2f, %.2f] at +%d samples, want within %d: %v", b.lo, b.hi, e-i0, ell+wc, g.slotsFrom(i0, 10))
			}
			for i := i0; e >= 0 && i < e; i++ {
				if g.S(i) < b.lo {
					g.fail("entered from below at +%d, not from above", i-i0)
					break
				}
			}
			if g.cost {
				capped := false
				for i := i0; i <= i0+ell && i < g.n(); i++ {
					capped = capped || g.capped(i)
				}
				if !capped {
					g.fail("no capped sample within ℓ = %d of the change", ell)
				}
				def, rst := -1, -1
				for i := i0; i < g.n() && i <= i0+20; i++ {
					c := g.tr.Results[i].CPU
					if c.CDeferred && def < 0 {
						def = i
					}
					if c.CRestarted && def >= 0 && i == def+1 {
						rst = i
						break
					}
				}
				if def < 0 || rst < 0 {
					g.fail("no ĉ deferral followed by a restart after the change")
				} else {
					for _, i := range []int{def, rst} {
						if g.S(i) > g.S(i-1)+1e-9 {
							g.fail("a rise in the ĉ deferral or restart at +%d", i-i0)
						}
					}
				}
				rw := false
				for i := i0; i < g.n() && i <= i0+10; i++ {
					rw = rw || g.tr.Results[i].CPU.DRewarmed
				}
				if !rw {
					g.fail("no D̂ warm-start restart at the cut's N̄ move")
				}
			}
			g.outside(b, e, g.n())
		}},
		{name: "limit halves", cfg: func(seed uint64) Config {
			c := measured(seed)
			c.Duration = 240 * time.Second
			c.LimitAt = limitAfter(stepAt, 0.25, 0.125)
			return c
		}, check: func(g *gctx) {
			p := frozenPlants["limit125"]
			b := bandOf(p.sStar)
			i0 := 0
			for i := 1; i < g.n(); i++ {
				if g.limit(i) != g.limit(i-1) {
					i0 = i
					break
				}
			}
			const ell = 1
			e := g.entry(b, i0)
			if e < 0 || e > i0+ell+1 {
				g.fail("into the band [%.2f, %.2f] at +%d samples, want within %d: %v", b.lo, b.hi, e-i0, ell+1, g.slotsFrom(i0, 10))
			}
			if g.cost {
				r := g.tr.Results[i0]
				if !r.CPU.ChangeSample {
					g.fail("the change's sample not treated as such")
				}
				if r.Slots > rho*r.CPU.InFlight+1e-9 {
					g.fail("S₁ %.2f above θ'N̄₁ = %.2f", r.Slots, rho*r.CPU.InFlight)
				}
				first := false
				for i := i0 + 1; i <= i0+ell+1 && i < g.n(); i++ {
					first = first || (g.tr.Results[i].CPU.Kind == costmodel.Capped && g.tr.Results[i].CPU.FirstTerm)
				}
				if !first {
					g.fail("no capped sample at its first term within ℓ + 1")
				}
				holds := 0
				for i := i0; i < g.n() && i <= i0+30; i++ {
					if g.tr.Results[i].CPU.DHold {
						holds++
					}
				}
				if holds > ceilSec(p.dStar)+1 {
					g.fail("the hold lasted %d samples, want at most %d", holds, ceilSec(p.dStar)+1)
				}
			}
			g.outside(b, e, g.n())
		}},
		{name: "limit doubles", cfg: func(seed uint64) Config {
			c := measured(seed)
			c.Duration = 240 * time.Second
			c.LimitAt = limitAfter(stepAt, 0.25, 0.5)
			return c
		}, check: func(g *gctx) {
			p := frozenPlants["limit500"]
			b := bandOf(p.sStar)
			i0 := index(g.tr, stepAt)
			within := 3
			if p.nFrom != 1 {
				within = 3 + (p.nFrom-1)*(1+ceilSec(p.dStar))
			}
			ok := -1
			for i := i0; i < g.n() && i <= i0+within; i++ {
				if math.Abs(g.S(i)-p.sStar)/p.sStar <= 0.10 {
					ok = i
					break
				}
			}
			if ok < 0 {
				g.fail("not within 10%% of %.2f within %d samples: %v", p.sStar, within, g.slotsFrom(i0, within+2))
			}
			g.outside(b, ok, g.n())
		}},
		{name: "wait doubles", cfg: func(seed uint64) Config {
			c := measured(seed)
			c.Duration = 240 * time.Second
			slow := baseClass
			slow.Wait = 800 * time.Millisecond
			c.Queues[0].Class = classAfter(stepAt, baseClass, slow)
			return c
		}, check: func(g *gctx) {
			u := meanCPU(g.tr, stepAt+15*time.Second, g.cfg.Duration) / g.cfg.Limit
			if u < 0.85 || u > 0.95 {
				g.fail("from 15 samples after the wait doubled, mean CPU %.1f%%, want 85–95%%", 100*u)
			}
		}},
		{name: "usage noise", cfg: func(seed uint64) Config {
			c := measured(seed)
			c.UsageNoise = 0.10
			return c
		}, check: func(g *gctx) {
			u := meanCPU(g.tr, 60*time.Second, g.cfg.Duration) / g.cfg.Limit
			if u < 0.85 || u > 0.95 {
				g.fail("mean CPU %.1f%%, want 85–95%%", 100*u)
			}
			var s, s2 float64
			var n int
			for i, a := range g.tr.At {
				if a >= 60*time.Second {
					x := g.S(i)
					s, s2, n = s+x, s2+x*x, n+1
				}
			}
			m := s / float64(n)
			if sd := math.Sqrt(s2/float64(n) - m*m); sd >= 0.3 {
				g.fail("slots' standard deviation %.2f, want under 0.3", sd)
			}
		}},
		{name: "H1 biased background", cfg: func(seed uint64) Config {
			return Config{
				Seed: seed, Duration: 120 * time.Second, Procs: 2, NodeCPUs: 4, Limit: 0.5, Ceiling: 16,
				Background: burstyBackground(seed, 0.015, 0.060, 0.3),
				Queues: []Queue{{Name: "q", Class: fixedClass(Class{CPU: 3 * time.Millisecond, Wait: 800 * time.Millisecond, Jitter: 0.1}),
					Active: after(15 * time.Second)}},
				Model: defaultModel(),
			}
		}, check: func(g *gctx) {
			i0 := index(g.tr, 15*time.Second)
			reached := -1
			for i := i0; i < g.n() && i <= i0+10; i++ {
				if g.tr.Capacity[i] == g.cfg.Ceiling {
					reached = i
					break
				}
			}
			if reached < 0 {
				g.fail("not at the ceiling within 10 samples of the work starting: %v", g.slotsFrom(i0, 12))
				return
			}
			for i := reached; i < g.n(); i++ {
				if g.tr.Capacity[i] == 1 {
					g.fail("back at the floor at %v", g.tr.At[i])
					return
				}
			}
		}},
		{name: "H1 noisy background", cfg: func(seed uint64) Config {
			c := Config{
				Seed: seed, Duration: 10 * time.Minute, Procs: 2, NodeCPUs: 4, Limit: 0.5, Ceiling: 16,
				Queues: []Queue{{Name: "q", Class: fixedClass(Class{CPU: 3 * time.Millisecond, Wait: 800 * time.Millisecond, Jitter: 0.1}),
					Active: always}},
				Model: defaultModel(),
			}
			nz := newNoise(seed)
			c.Background = func(t time.Duration) float64 { return math.Max(0, 0.015+0.010*nz.at(t)) }
			return c
		}, check: func(g *gctx) {
			warm := -1
			for i := range g.tr.Results {
				if g.tr.Capacity[i] == g.cfg.Ceiling {
					warm = i
					break
				}
			}
			if warm < 0 {
				g.fail("never reached the ceiling")
				return
			}
			for i := warm; i < g.n(); i++ {
				if g.tr.Capacity[i] < g.cfg.Ceiling {
					g.fail("below the ceiling at %v: %d", g.tr.At[i], g.tr.Capacity[i])
					return
				}
			}
		}},
		{name: "H1 bursts above one slot", cfg: func(seed uint64) Config {
			c := measured(seed)
			c.Background = burstyBackground(seed, 0.020, 0.100, 0.3)
			c.Queues[0].Active = after(15 * time.Second)
			return c
		}, check: func(g *gctx) {
			i0 := index(g.tr, 15*time.Second)
			g.monotone(i0, g.entry(bandOf(base.sStar), i0))
			u := meanCPU(g.tr, 75*time.Second, g.cfg.Duration) / g.cfg.Limit
			if u < 0.85 || u > 0.95 {
				g.fail("mean CPU %.1f%%, want 85–95%%", 100*u)
			}
		}},
		{name: "false restarts, 0.47s", cfg: func(seed uint64) Config {
			c := measured(seed)
			c.Duration = time.Hour
			return c
		}, check: falseRestarts},
		{name: "false restarts, 30s", cfg: func(seed uint64) Config {
			c := measured(seed)
			c.Duration = time.Hour
			c.Queues[0].Class = fixedClass(drainBase)
			return c
		}, check: falseRestarts},
		{name: "mix shift seen", cfg: func(seed uint64) Config { c, _ := mixShift(seed, true); return c }, check: func(g *gctx) {
			_, shift := mixShift(0, true)
			b := bandOf(base.sStar)
			i0 := index(g.tr, shift)
			capped, out := 0, -1
			for i := i0; i < g.n(); i++ {
				if g.capped(i) {
					capped++
					continue
				}
				if capped > 0 {
					out = i
					break
				}
			}
			if capped > 0 && (out < 0 || out-i0 > 6) {
				g.fail("not out of the cap within 6 samples of the shift: %v", g.slotsFrom(i0, 12))
			}
			// At most one crossing, downward across the whole band.
			side := 0
			switch {
			case g.S(i0) > b.hi:
				side = 1
			case g.S(i0) < b.lo:
				side = -1
			}
			crossings, last := 0, i0
			for i := i0 + 1; i < g.n(); i++ {
				S := g.S(i)
				switch {
				case side >= 0 && S < b.lo:
					if side > 0 {
						crossings++
						last = i
					}
					side = -1
				case side <= 0 && S > b.hi:
					if side < 0 {
						crossings++
						g.fail("an upward crossing at %v", g.tr.At[i])
					}
					side = 1
				}
			}
			if crossings > 1 {
				g.fail("%d crossings of the band after the shift, want at most one", crossings)
			}
			g.monotone(last, g.entry(b, last))
			countRestarts(g, i0+10)
		}},
		{name: "mix shift never seen", cfg: func(seed uint64) Config { c, _ := mixShift(seed, false); return c }, check: func(g *gctx) {
			_, shift := mixShift(0, false)
			for i := index(g.tr, shift); i < g.n(); i++ {
				if !g.capped(i) {
					continue
				}
				if g.S(i) > g.S(i-1)+1e-9 {
					g.fail("rose while capped at %v: %.2f to %.2f", g.tr.At[i], g.S(i-1), g.S(i))
				}
				if n := g.tr.Results[i].CPU.InFlight; g.S(i) > math.Max(1, rho*n)+1e-6 {
					g.fail("capped at %v: S %.2f above θN̄ = %.2f", g.tr.At[i], g.S(i), rho*n)
				}
			}
		}},
		{name: "no limit, pressure only blocks", cfg: func(seed uint64) Config {
			return Config{
				Seed: seed, Duration: 900 * time.Second, Procs: 4, NodeCPUs: 4, Ceiling: 32,
				Share: func(t time.Duration) float64 {
					if t >= 300*time.Second && t < 600*time.Second {
						return 0.8
					}
					return 0.4
				},
				Queues: []Queue{{Name: "q", Class: fixedClass(baseClass), Active: always}},
				Model:  defaultModel(),
			}
		}, check: func(g *gctx) {
			over := 0
			for i := 1; i < g.n(); i++ {
				r := g.tr.Results[i]
				if g.S(i) < g.S(i-1)-1e-9 {
					g.fail("the slots fell at %v with no limit: %.2f to %.2f", g.tr.At[i], g.S(i-1), g.S(i))
					return
				}
				o := g.stall(i) > 50
				if o {
					over++
					if g.S(i) > g.S(i-1)+1e-9 {
						g.fail("a rise at %v with the stall over the threshold", g.tr.At[i])
						return
					}
				}
				if r.CPU.Blocked != o {
					g.fail("blocked %v at %v with the stall at %.1f", r.CPU.Blocked, g.tr.At[i], g.stall(i))
					return
				}
				if r.CPU.Slots != float64(g.cfg.Ceiling) {
					g.fail("S_cpu %.2f at %v, want the ceiling", r.CPU.Slots, g.tr.At[i])
					return
				}
			}
			g.report("samples over the threshold: %d", over)
		}},
		{name: "contention below the quota", cfg: func(seed uint64) Config {
			c := measured(seed)
			c.Share = func(t time.Duration) float64 {
				if t >= 60*time.Second && t < 75*time.Second {
					return 0.125
				}
				return 4
			}
			return c
		}, check: func(g *gctx) {
			b := bandOf(base.sStar)
			from, to := index(g.tr, 60*time.Second), index(g.tr, 75*time.Second)
			onset := -1.0
			for i := from; i < to+1 && i < g.n(); i++ {
				r := g.tr.Results[i]
				o := g.stall(i) > 50
				if r.CPU.Blocked != o {
					g.fail("blocked %v at %v with the stall at %.1f", r.CPU.Blocked, g.tr.At[i], g.stall(i))
				}
				if !o {
					onset = -1
					continue
				}
				if onset < 0 {
					onset = g.S(i - 1)
				}
				if g.S(i) > onset+1e-9 {
					g.fail("a rise past the onset value at %v under pressure", g.tr.At[i])
				}
				if g.S(i) < g.S(i-1)-1e-9 && r.CPU.Slots > g.S(i)+1e-9 {
					g.fail("a fall at %v not computed by the controller", g.tr.At[i])
				}
			}
			above, worst := 0, 0.0
			for i := to; i < g.n(); i++ {
				if g.S(i) > b.hi+1e-9 {
					above++
					worst = math.Max(worst, g.S(i)-b.hi)
				}
			}
			allow := 0
			if !g.cost {
				allow = 1 + ceilSec(base.dStar) + 1
			}
			if above > allow {
				g.fail("above S*(1 + 3r̄) = %.2f in %d samples after it, want at most %d", b.hi, above, allow)
			}
			if above > 0 {
				g.report("the excursion above the band: %d samples, at most %.2f slots over", above, worst)
			}
		}},
		{name: "long drain", cfg: func(seed uint64) Config {
			// The change comes after the 30s plant's own approach from one slot,
			// c₁ + 1 samples (2,977 on the frozen curve), has had time to end.
			c := measured(seed)
			c.Duration = drainAt + 1200*time.Second
			c.Queues[0].Class = classAfter(drainAt, drainBase, drainHeavy())
			return c
		}, check: func(g *gctx) {
			p := frozenPlants["drain"]
			b := bandOf(p.sStar)
			i0 := index(g.tr, drainAt)
			g.report("before the change: S %.2f against the 30s plant's S* %.2f", g.S(i0-1), frozenPlants["drain0"].sStar)
			n0 := 0.0
			k := 0
			for i := index(g.tr, drainAt-60*time.Second); i < i0; i++ {
				n0 += g.tr.Results[i].CPU.InFlight
				k++
			}
			n0 /= float64(k)
			kth := int(math.Ceil(math.Log(n0/(p.sStar*(1+3*rbar))) / math.Log(1/rho)))
			const ellMin = 1
			within := ellMin + 2*max(kth, 0) - 1
			drained := 0
			for i := i0; i < g.n(); i++ {
				if g.tr.Samples[i].Books.NMin > g.tr.Capacity[i-1] {
					drained++
					if g.S(i) > g.S(i-1)+1e-9 {
						g.fail("a rise at %v with N⁻ above the capacity", g.tr.At[i])
					}
				}
			}
			if drained == 0 {
				g.v.invalid = "no drained sample: the case didn't exercise a drain"
			}
			g.report("drained samples: %d; N̄₀ %.2f, k_θ %d", drained, n0, kth)
			e := g.entry(b, i0)
			if e < 0 || e > i0+within {
				g.fail("into the band [%.2f, %.2f] at +%d samples, want within %d: %v", b.lo, b.hi, e-i0, within, g.slotsFrom(i0, 12))
			}
			g.outside(b, e, g.n())
			if g.cost {
				rw, fb, cr := false, 0, 0
				for i := i0; i < g.n(); i++ {
					c := g.tr.Results[i].CPU
					if i <= i0+60 {
						rw = rw || c.DRewarmed
					}
					if c.DFallback {
						fb++
					}
					if c.CRestarted {
						cr++
					}
				}
				if !rw {
					g.fail("no D̂ warm-start restart at the cut")
				}
				g.report("Little's fallback used in %d samples; ĉ restarts %d", fb, cr)
			}
		}},
		{name: "rising cost", cfg: func(seed uint64) Config {
			c := measured(seed)
			c.CostPerSlot = 0.01
			return c
		}, check: func(g *gctx) {
			p := frozenPlants["rising"]
			b := bandOf(p.sStar)
			u := meanCPU(g.tr, g.cfg.Duration-120*time.Second, g.cfg.Duration) / g.cfg.Limit
			if math.Abs(u-rho) > 0.03 {
				g.fail("mean CPU over the last 120s %.1f%%, want within 3 points of 90%%", 100*u)
			}
			e := g.entry(b, 0)
			if e < 0 || e+1 > c1(p)+1 {
				g.fail("into the band [%.2f, %.2f] at sample %d, want by %d", b.lo, b.hi, e+1, c1(p)+1)
			}
			g.monotone(0, e)
			g.outside(b, e, g.n())
			top := 0.0
			for i := range g.tr.Results {
				top = math.Max(top, g.S(i))
			}
			if top > b.hi+1e-9 {
				g.fail("S reached %.2f, above S*(1 + 3r̄) = %.2f", top, b.hi)
			}
			g.report("max S %.2f against S* %.2f (predicted excess 0.01·(S* − 1)·S* = %.2f)", top, p.sStar, 0.01*(p.sStar-1)*p.sStar)
		}},
	}
}

func falseRestarts(g *gctx) {
	if !g.cost {
		return
	}
	n := 0
	for i, r := range g.tr.Results {
		if i < 120 || (!r.CPU.CRestarted && !r.CPU.DRestarted) {
			continue
		}
		n++
		if r.Slots > g.S(i-1)+1e-9 {
			g.fail("a restart raised the slots at %v: %.2f to %.2f", g.tr.At[i], g.S(i-1), r.Slots)
		}
	}
	g.report("restarts after warm-up: %d in an hour", n)
}

// mixShift is the first review's M1: a cheap queue at the ceiling, then an
// expensive one, seen before or not.
func mixShift(seed uint64, seen bool) (Config, time.Duration) {
	cheap := Class{CPU: time.Millisecond, Wait: 2 * time.Second, Jitter: 0.1}
	cfg := Config{Seed: seed, Procs: 2, NodeCPUs: 4, Limit: 0.25, Ceiling: 32, Model: defaultModel()}
	var shift time.Duration
	if seen {
		shift = 120 * time.Second
		cfg.Duration = 240 * time.Second
		cfg.Queues = []Queue{
			{Name: "exp", Class: fixedClass(baseClass), Active: func(t time.Duration) bool { return t < 60*time.Second || t >= shift }},
			{Name: "cheap", Class: fixedClass(cheap), Active: during(60*time.Second, shift)},
		}
	} else {
		shift = 60 * time.Second
		cfg.Duration = 180 * time.Second
		cfg.Queues = []Queue{
			{Name: "cheap", Class: fixedClass(cheap), Active: during(0, shift)},
			{Name: "exp", Class: fixedClass(baseClass), Active: after(shift)},
		}
	}
	return cfg, shift
}

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

// noise is a per-second N(0,1) sequence, the same for every millisecond of a
// second.
type noise struct{ seed uint64 }

func newNoise(seed uint64) noise { return noise{seed} }

func (n noise) at(t time.Duration) float64 {
	sec := uint64(t/time.Second) + 1
	h1 := (sec*0x9e3779b97f4a7c15 ^ n.seed*0xbf58476d1ce4e5b9) >> 11
	h2 := (sec*0xc2b2ae3d27d4eb4f ^ n.seed*0x165667b19e3779f9) >> 11
	u1 := (float64(h1%(1<<52)) + 0.5) / (1 << 52)
	u2 := (float64(h2%(1<<52)) + 0.5) / (1 << 52)
	return math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
}

// runCase runs one case for one controller and seed.
func runCase(c gateCase, ctl controller, seed uint64) verdict {
	cfg := c.cfg(seed)
	ctl.apply(&cfg)
	tr := Run(cfg)
	var v verdict
	c.check(&gctx{tr: tr, cfg: cfg, cost: ctl.cost, v: &v})
	return v
}
