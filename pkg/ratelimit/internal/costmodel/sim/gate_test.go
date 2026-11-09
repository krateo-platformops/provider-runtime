package sim

import (
	"fmt"
	"math"
	"sort"
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

// controller is what runs a case: the cost model, the level PI at a multiple
// of its committed gains, or the reservation-gating baseline at Temporal's
// defaults.
type controller struct {
	name    string
	cost    bool
	reserve bool
	gain    float64
}

var (
	ctlCost    = controller{name: "cost model", cost: true}
	ctlReserve = controller{name: "reservation gate", reserve: true}
)

func ctlBaseline(gain float64) controller {
	return controller{name: fmt.Sprintf("level PI ×%g", gain), gain: gain}
}

func (c controller) apply(cfg *Config) {
	if c.cost {
		return
	}
	if c.reserve {
		r := TemporalReservation()
		cfg.Reserve = &r
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

// window is the first sample of sample i's window W: the most recent samples
// spanning at least max(the longest lifetime among their completions,
// a_oldest) + Δt (design §17.1, §17.8 item 1).
func (g *gctx) window(i int) int {
	b := g.tr.Samples[i].Books
	aOldest := 0.0
	if b.Oldest >= 0 {
		aOldest = (b.End - b.Oldest).Seconds()
	}
	dt := g.tr.Samples[i].Dt.Seconds()
	longest := 0.0
	j := i
	for ; j > 0; j-- {
		for _, l := range g.tr.Samples[j].Books.Lives {
			longest = math.Max(longest, l.Lifetime())
		}
		if (b.End - g.tr.Samples[j].Books.Start).Seconds() >= math.Max(longest, aOldest)+dt {
			break
		}
	}
	return j
}

// nW is n_W, the samples in sample i's window.
func (g *gctx) nW(i int) int { return i - g.window(i) + 1 }

// capped applies design §17.8 item 1's test, over the sample's window and
// against the current limit, for any controller.
func (g *gctx) capped(i int) bool {
	s := g.tr.Samples[i]
	if s.Limit <= 0 || !s.Throttling {
		return false
	}
	j := g.window(i)
	var u, d float64
	var per, thr uint64
	for k := j; k <= i; k++ {
		x := g.tr.Samples[k]
		u += x.Usage.Seconds()
		d += x.Dt.Seconds()
		per += x.Periods
		thr += x.Throttled
	}
	n := i - j + 1
	slack := math.Min(s.Limit*s.Period.Seconds(), float64(s.Procs)*s.Slice.Seconds())
	return per >= 1 && u >= s.Limit*d-slack*float64(n) && thr+uint64(n) >= per
}

// firstCapped is the first window-capped sample at or after from, or -1.
func (g *gctx) firstCapped(from int) int {
	for i := from; i < g.n(); i++ {
		if g.capped(i) {
			return i
		}
	}
	return -1
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
func (g *gctx) outside(b band, from, to int) { g.outsideLo(b, from, to, nil) }

// outsideLo is outside with a lower edge that may vary by sample.
func (g *gctx) outsideLo(b band, from, to int, lo func(i int) float64) {
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
		l := b.lo
		if lo != nil {
			l = lo(i)
		}
		if S := g.S(i); S < l-1e-9 || S > hi+1e-9 {
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
		// The baselines' falls are attributed by the CPU they act on: the
		// level PI's filtered Ū, the reservation gate's reading (plan §4.5).
		u := g.tr.Used(i)
		if !g.cost {
			u = g.tr.Results[i].CPU.Used
		}
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
			g.trace(i)
			continue
		}
		g.fail("an unattributed fall at %v: %.2f to %.2f (U %.3f, not capped)", g.tr.At[i], prev, S, u)
	}
}

// trace reports the samples around i, for a fall the plan asks to trace.
func (g *gctx) trace(i int) {
	for j := max(i-4, 1); j <= i+2 && j < g.n(); j++ {
		r := g.tr.Results[j]
		c := r.CPU
		g.report("trace %v: S %.2f, U %.3f, kind %v, ĉ %.4f, D̂ %.3f, c_W %.4f, N̄ %.2f, C %d, trusted %v, deferred %v, pending %v, restarted %v, rewarmed %v, guarded %v",
			g.tr.At[j], r.Slots, g.tr.Used(j), c.Kind, c.PerCompletion, c.Duration, c.WindowCost, c.InFlight,
			g.tr.Samples[j].Books.Completions, c.Trusted, c.CDeferred, c.CPending, c.CRestarted, c.DRewarmed, c.Guarded)
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
	drainAt = 6000 * time.Second
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
			// Plan §4.5, corrected (a): the entry allows c₁ + 1 + ⌈D̂/Δt⌉.
			b := bandOf(base.sStar)
			by := c1(base) + 1 + ceilSec(base.dStar)
			e := g.entry(b, 0)
			if e < 0 || e+1 > by {
				g.fail("into the band [%.2f, %.2f] at sample %d, want by %d: %v", b.lo, b.hi, e+1, by, g.slotsFrom(0, 12))
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
			// Plan §4.5, the counts under design §17.8.
			p := frozenPlants["cost46"]
			b := bandOf(p.sStar)
			i0 := index(g.tr, stepAt)
			const ell = 2
			fc := g.firstCapped(i0)
			nw := 1
			if fc >= 0 {
				nw = g.nW(fc)
			}
			g.report("n_W %d at the first window-capped sample (+%d)", nw, fc-i0)
			// The floor after a cost increase (design §17.8 item 4), frozen from
			// the plant, until the window after ĉ's restart has closed.
			floor := floorAfterCostIncrease()
			until := i0 + 2*nw + 1
			if g.cost {
				until = -1
				for i := i0; i < g.n(); i++ {
					if g.tr.Results[i].CPU.CRestarted {
						for j := i + 1; j < g.n(); j++ {
							if g.tr.Results[j].CPU.WindowClosed {
								until = j
								break
							}
						}
						break
					}
				}
				if until < 0 {
					until = g.n()
				}
			}
			lo := func(i int) float64 {
				if i <= until {
					return math.Min(b.lo, floor)
				}
				return b.lo
			}
			g.report("the floor %.2f until +%d, then θ⌊S*'⌋ = %.2f", floor, until-i0, b.lo)
			within := ell + 2*nw - 1
			e := -1
			for i := i0; i < g.n(); i++ {
				if S := g.S(i); S >= lo(i)-1e-9 && S <= b.hi+1e-9 {
					e = i
					break
				}
			}
			if e < 0 || e > i0+within {
				g.fail("into the band [%.2f, %.2f] at +%d samples, want within %d: %v", lo(i0), b.hi, e-i0, within, g.slotsFrom(i0, 10))
			}
			for i := i0; e >= 0 && i < e; i++ {
				if g.S(i) < lo(i) {
					g.fail("entered from below at +%d, not from above", i-i0)
					break
				}
			}
			if g.cost {
				if fc < 0 || fc > i0+ell+nw-1 {
					g.fail("no window-capped sample within ℓ + n_W − 1 = %d of the change", ell+nw-1)
				}
				// The change test's mechanism applies to a change of at least
				// 3σ_W; it has 2⌈W/Δt⌉ + 1 samples, and 3⌈W/Δt⌉ + 1 for a
				// change near 3σ_W (the plan author's reading: 3σ_W ≤ Δc < 4σ_W).
				pre := g.tr.Results[i0-1].CPU
				z := 0.0
				if pre.SigmaW > 0 {
					z = pre.PerCompletion / pre.SigmaW
				}
				g.report("σ_W %.5f before the change; the change Δc/σ_W ≈ %.1f", pre.SigmaW, z)
				// Corrected (a): within 2⌈W_test/Δt⌉ + 1, W_test the longer of the
				// deferring and confirming windows, measured; 3⌈W_test/Δt⌉ + 1 for a
				// change near 3σ_W.
				def, rst := -1, -1
				for i := i0; i < g.n() && i <= i0+60; i++ {
					c := g.tr.Results[i].CPU
					if c.CDeferred && def < 0 {
						def = i
					}
					if c.CRestarted && def >= 0 {
						rst = i
						break
					}
				}
				wTest := nw
				horizon := 2*nw + 1
				if def >= 0 && rst >= 0 {
					wTest = max(g.tr.Results[def].CPU.WindowLen, g.tr.Results[rst].CPU.WindowLen)
					horizon = 2*wTest + 1
					if z < 4 {
						horizon = 3*wTest + 1
					}
					g.report("W_test %d samples; the restart at +%d, the horizon %d", wTest, rst-i0, horizon)
				}
				switch {
				case z < 3:
					g.report("a change under 3σ_W: the change test's mechanism isn't required")
				case def < 0 || rst < 0:
					g.fail("no ĉ deferral followed by a restart after the change")
				case rst > i0+horizon:
					g.fail("the ĉ restart at +%d, after the horizon %d", rst-i0, horizon)
				}
				for i := i0; i < g.n() && i <= i0+horizon+nw; i++ {
					c := g.tr.Results[i].CPU
					if (c.CPending || c.CDeferred || c.CRestarted) && g.S(i) > g.S(i-1)+1e-9 {
						g.fail("a rise at +%d while ĉ's test was pending", i-i0)
					}
				}
				rw := false
				for i := i0; i < g.n() && i <= i0+within+1; i++ {
					rw = rw || g.tr.Results[i].CPU.DRewarmed
				}
				if !rw {
					g.fail("no D̂ warm-start restart at the cut")
				}
			}
			g.outsideLo(b, e, g.n(), lo)
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
			// Plan §4.5: within 1 after i0, W judged against the current limit.
			const ell = 1
			e := g.entry(b, i0)
			if e < 0 || e > i0+1 {
				g.fail("into the band [%.2f, %.2f] at +%d samples, want within 1 after the change's sample: %v", b.lo, b.hi, e-i0, g.slotsFrom(i0, 10))
			}
			if g.cost {
				r := g.tr.Results[i0]
				if !r.CPU.ChangeSample {
					g.fail("the change's sample not treated as such")
				}
				// Corrected (c): S₁ = min(S₀, L11) when U₁ > ρL', else S₀.
				s0 := g.S(i0 - 1)
				L := g.limit(i0)
				want := s0
				if u := g.tr.Used(i0); u > rho*L {
					if d := math.Min(u, L) - r.CPU.Background; d > 0 {
						want = math.Min(s0, (rho*L-r.CPU.Background)*r.CPU.InFlight/d)
					}
				}
				if math.Abs(r.Slots-want) > 1e-6 {
					g.fail("S₁ %.3f, want %.3f", r.Slots, want)
				}
				// Corrected (b): a capped sample at its first term, or an active
				// sample at the law with D̂ restarted, within ℓ + 1.
				ok := false
				for i := i0 + 1; i <= i0+ell+1 && i < g.n(); i++ {
					c := g.tr.Results[i].CPU
					ok = ok || (c.Kind == costmodel.Capped && c.FirstTerm) ||
						(c.Kind == costmodel.Active && !c.DHold && !c.CHold && !c.Unresolvable && c.Duration > 0)
				}
				if !ok {
					g.fail("neither a capped sample at its first term nor an active one at the law within ℓ + 1")
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
			// Plan §4.5: the 85–95% bar was never re-derived for §17's law, so
			// the mean CPU is reported against the predicted setpoint shift (a
			// few percent towards less CPU, design §17.1), not judged.
			u := meanCPU(g.tr, 75*time.Second, g.cfg.Duration) / g.cfg.Limit
			g.report("mean CPU %.1f%% of the limit (target 90%%, shifted down by at most a few points)", 100*u)
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
		}, check: func(g *gctx) {
			// Design §17.8 item 1: INVALID unless S reaches its plant's band
			// before the restarts are counted.
			e := g.entry(bandOf(frozenPlants["drain0"].sStar), 0)
			if e < 0 {
				g.v.invalid = "S never reached the 30s plant's band"
				return
			}
			g.report("into the 30s plant's band at %v", g.tr.At[e])
			falseRestartsFrom(g, e)
		}},
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
			// Corrected (f): min(⌈ln(32/S*)/ln(1/θ)⌉, ⌈W/Δt⌉ + 1) + (n_W − 1).
			nw := 1
			if fc := g.firstCapped(i0); fc >= 0 {
				nw = g.nW(fc)
			}
			lim := int(math.Min(math.Ceil(math.Log(32/base.sStar)/math.Log(1/rho)), float64(nw+1))) + nw - 1
			if capped > 0 && (out < 0 || out-i0 > lim) {
				g.fail("not out of the cap within %d samples of the shift: %v", lim, g.slotsFrom(i0, 12))
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
			// Plan §4.5 (i): a share near the demand, so that the stall
			// straddles the threshold. At 1.6 cores the stall was over 50 in 1
			// of 301 samples (seeds 1–3 at 31b9509), at 0.8 in nearly all; 1.2
			// is between them, chosen from that report: a test-configuration
			// parameter, not a condition.
			return Config{
				Seed: seed, Duration: 1200 * time.Second, Procs: 4, NodeCPUs: 4, Ceiling: 32,
				Share: func(t time.Duration) float64 {
					switch {
					case t >= 300*time.Second && t < 600*time.Second:
						return 0.8
					case t >= 900*time.Second && t < 1200*time.Second:
						return 1.2
					}
					return 0.4
				},
				Queues: []Queue{{Name: "q", Class: fixedClass(baseClass), Active: always}},
				Model:  defaultModel(),
			}
		}, check: func(g *gctx) {
			// Plan §4.5: for the baselines, reported, not judged (§5.1 decides
			// no CPU control without a limit).
			if !g.cost {
				defer func() {
					for _, f := range g.v.fails {
						g.report("not judged: %s", f)
					}
					g.v.fails = nil
				}()
			}
			over, near, nearOver := 0, 0, 0
			for i := 1; i < g.n(); i++ {
				r := g.tr.Results[i]
				if g.S(i) < g.S(i-1)-1e-9 {
					g.fail("the slots fell at %v with no limit: %.2f to %.2f", g.tr.At[i], g.S(i-1), g.S(i))
					return
				}
				o := g.stall(i) > 50
				if g.tr.At[i] >= 900*time.Second {
					near++
					if o {
						nearOver++
					}
				}
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
				if g.cost && r.CPU.Slots != float64(g.cfg.Ceiling) {
					g.fail("S_cpu %.2f at %v, want the ceiling", r.CPU.Slots, g.tr.At[i])
					return
				}
			}
			g.report("samples over the threshold: %d; at the share near demand, %d of %d", over, nearOver, near)
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
			// Corrected (e): during the dip, no rise in a sample over the
			// threshold; after it, S above S*(1 + 3r̄) in at most
			// 2k_θ − 1 + (n_W − 1) samples, k_θ from the slots at the dip's end.
			b := bandOf(base.sStar)
			from, to := index(g.tr, 60*time.Second), index(g.tr, 75*time.Second)
			var stalls []float64
			for i := from; i < to && i < g.n(); i++ {
				r := g.tr.Results[i]
				st := g.stall(i)
				stalls = append(stalls, st)
				// Plan §4.5, design §17.9 item 8: blocked in every sample over
				// the threshold with no throttled period, and in no other.
				o := st > 50 && g.tr.Samples[i].Throttled == 0
				if g.cost && r.CPU.Blocked != o {
					g.fail("blocked %v at %v with the stall at %.1f and %d throttled periods", r.CPU.Blocked, g.tr.At[i], st, g.tr.Samples[i].Throttled)
				}
				if o && g.S(i) > g.S(i-1)+1e-9 {
					g.fail("a rise at %v with the stall over the threshold and no throttled period", g.tr.At[i])
				}
			}
			sort.Float64s(stalls)
			if len(stalls) > 0 {
				g.report("the dip's stall: min %.1f, median %.1f, max %.1f, over 50 in %d of %d", stalls[0], stalls[len(stalls)/2], stalls[len(stalls)-1], countOver(stalls, 50), len(stalls))
			}
			// Corrected (e): with design §17.9 item 5's rewarm, only §4.3's
			// convergence from above, the frozen allowance n_above·(1 + ⌈D̂/Δt⌉).
			sEnd := g.S(to - 1)
			kth := base.nAbove
			allow := base.nAbove * (1 + ceilSec(base.dStar))
			above, worst := 0, 0.0
			for i := to; i < g.n(); i++ {
				if g.S(i) > b.hi+1e-9 {
					above++
					worst = math.Max(worst, g.S(i)-b.hi)
				}
			}
			if above > allow {
				g.fail("above S*(1 + 3r̄) = %.2f in %d samples after the dip, want at most %d (n_above %d; S %.2f at the dip's end)", b.hi, above, allow, kth, sEnd)
			}
			if above > 0 {
				g.report("the excursion above the band: %d samples, at most %.2f slots over", above, worst)
			}
		}},
		{name: "long drain", cfg: func(seed uint64) Config {
			c := measured(seed)
			c.Duration = drainAt + 1200*time.Second
			c.Queues[0].Class = classAfter(drainAt, drainBase, drainHeavy())
			return c
		}, check: func(g *gctx) {
			p, p0 := frozenPlants["drain"], frozenPlants["drain0"]
			b := bandOf(p.sStar)
			i0 := index(g.tr, drainAt)
			// The precondition: at steady state before the change, at least
			// τ_ĉ plus 60 completions after entering the 30s plant's band.
			e0 := g.entry(bandOf(p0.sStar), 0)
			if e0 < 0 || e0 >= i0 {
				g.v.invalid = fmt.Sprintf("never in the 30s plant's band before the change (S %.2f)", g.S(i0-1))
				return
			}
			tauC := math.Max(10, p0.dStar/rbar)
			comp := 0
			for i := e0; i < i0; i++ {
				comp += g.tr.Samples[i].Books.Completions
			}
			if (g.tr.At[i0]-g.tr.At[e0]).Seconds() < tauC || comp < 60 {
				g.v.invalid = fmt.Sprintf("not at steady state before the change: %v and %d completions since entry, want τ_ĉ %.0fs and 60", g.tr.At[i0]-g.tr.At[e0], comp, tauC)
				return
			}
			n0 := 0.0
			k := 0
			for i := index(g.tr, drainAt-60*time.Second); i < i0; i++ {
				n0 += g.tr.Results[i].CPU.InFlight
				k++
			}
			n0 /= float64(k)
			if n0 <= p.sStar*(1+3*rbar) {
				g.v.invalid = fmt.Sprintf("N̄₀ %.2f not above S*'(1 + 3r̄) = %.2f: no descent to count", n0, p.sStar*(1+3*rbar))
				return
			}
			kth := int(math.Ceil(math.Log(n0/(p.sStar*(1+3*rbar))) / math.Log(1/rho)))
			// ℓ_min, measured: the samples from the change to the first
			// window-capped sample after it.
			fc := g.firstCapped(i0)
			if fc < 0 {
				g.fail("no window-capped sample after the change: ℓ_min unmeasurable")
				return
			}
			ellMin := fc - i0
			within := ellMin + 2*kth - 1
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
			g.report("drained samples: %d; N̄₀ %.2f, k_θ %d, ℓ_min %d, n_W %d", drained, n0, kth, ellMin, g.nW(fc))
			e := g.entry(b, i0)
			if e < 0 || e > i0+within {
				g.fail("into the band [%.2f, %.2f] at +%d samples, want within %d: %v", b.lo, b.hi, e-i0, within, g.slotsFrom(i0, 12))
			}
			g.outside(b, e, g.n())
			if g.cost {
				rw, fb, cr, st := false, 0, 0, 0
				for i := i0; i < g.n(); i++ {
					c := g.tr.Results[i].CPU
					if i <= e+1 {
						rw = rw || c.DRewarmed
					}
					if c.DFallback {
						fb++
					}
					if c.CRestarted {
						cr++
					}
					st += c.StretchedLives
				}
				if !rw {
					g.fail("no D̂ warm-start restart at the cut")
				}
				g.report("Little's fallback used in %d samples; ĉ restarts %d; cap-stretched lives in D̂ %d", fb, cr, st)
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
			// Corrected (a): entry by c₁ + 1 + ⌈D̂/Δt⌉.
			by := c1(p) + 1 + ceilSec(p.dStar)
			e := g.entry(b, 0)
			if e < 0 || e+1 > by {
				g.fail("into the band [%.2f, %.2f] at sample %d, want by %d", b.lo, b.hi, e+1, by)
			}
			g.monotone(0, e)
			g.outside(b, e, g.n())
			// Corrected (d): untrusted samples judged at 3·max(own SE, r̄),
			// excluded and counted without one.
			top, excluded := 0.0, 0
			for i := range g.tr.Results {
				hi := b.hi
				if g.cost {
					c := g.tr.Results[i].CPU
					if !c.Trusted {
						if c.RelSE <= 0 || math.IsInf(c.RelSE, 0) || math.IsNaN(c.RelSE) {
							excluded++
							continue
						}
						hi = p.sStar * (1 + 3*math.Max(c.RelSE, rbar))
					}
				}
				top = math.Max(top, g.S(i))
				if g.S(i) > hi+1e-9 {
					g.fail("S %.2f at %v above %.2f", g.S(i), g.tr.At[i], hi)
					break
				}
			}
			g.report("max S %.2f against S* %.2f (predicted excess 0.01·(S* − 1)·S* = %.2f); %d samples without a standard error excluded", top, p.sStar, 0.01*(p.sStar-1)*p.sStar, excluded)
		}},
	}
}

func countOver(xs []float64, v float64) int {
	n := 0
	for _, x := range xs {
		if x > v {
			n++
		}
	}
	return n
}

func falseRestarts(g *gctx) { falseRestartsFrom(g, 120) }

func falseRestartsFrom(g *gctx, from int) {
	if !g.cost {
		return
	}
	n := 0
	for i, r := range g.tr.Results {
		if i < from || (!r.CPU.CRestarted && !r.CPU.DRestarted) {
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
