package sim

import (
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

// The plan's §4.5: S* and c from each case plant's fixed-capacity curve,
// computed and committed (frozen_test.go) before any run of the gate.

// plant is one case plant: a configuration run at fixed capacities.
type plant struct {
	name   string
	cfg    func(seed uint64) Config
	maxN   int
	warmup time.Duration
	length time.Duration
}

// The drain plants, calibrated to the GKE plant curve (plan §4.5, 2026-11-11):
// the CPU per reconcile is the GKE runs' mean cgroup CPU per completion,
// 3157 ms (hack/rlgov-measure/results/curve-report.txt), and the 30s plant
// before the change carries the same absolute excess, +157 ms over 1500. The
// waits and the jitter are unchanged.
var drainBase = Class{CPU: 1657 * time.Millisecond, Wait: 28500 * time.Millisecond, Jitter: 0.1}

func drainHeavy() Class {
	c := drainBase
	c.CPU = 3157 * time.Millisecond
	return c
}

func plants() []plant {
	base := Class{CPU: 23 * time.Millisecond, Wait: 400 * time.Millisecond, Jitter: 0.1}
	heavy := base
	heavy.CPU = 46 * time.Millisecond
	short := func(name string, f func(c *Config)) plant {
		return plant{name: name, maxN: 16, warmup: 60 * time.Second, length: 300 * time.Second,
			cfg: func(seed uint64) Config { c := measured(seed); f(&c); return c }}
	}
	long := func(name string, cl Class) plant {
		return plant{name: name, maxN: 12, warmup: 300 * time.Second, length: 1800 * time.Second,
			cfg: func(seed uint64) Config { c := measured(seed); c.Queues[0].Class = fixedClass(cl); return c }}
	}
	return []plant{
		short("base", func(*Config) {}),
		short("cost46", func(c *Config) { c.Queues[0].Class = fixedClass(heavy) }),
		short("limit125", func(c *Config) { c.Limit = 0.125 }),
		short("limit500", func(c *Config) { c.Limit = 0.5; c.Ceiling = 32 }),
		short("rising", func(c *Config) { c.CostPerSlot = 0.01 }),
		long("drain0", drainBase),
		long("drain", drainHeavy()),
	}
}

// curvePoint is the plant at one whole capacity.
type curvePoint struct{ n, u, x, d float64 }

func measureCurve(p plant) []curvePoint { return curveAt(p, 9001, p.warmup, p.length) }

// curveAt is the plant's fixed-capacity curve at a seed, warm-up and length.
func curveAt(p plant, seed uint64, warmup, length time.Duration) []curvePoint {
	var pts []curvePoint
	for n := 1; n <= p.maxN; n++ {
		pts = append(pts, pointAt(p, seed, n, warmup, length))
	}
	return pts
}

// pointAt is the plant at one whole capacity: U, X and D = ΣA/ΣC after the
// warm-up.
func pointAt(p plant, seed uint64, n int, warmup, length time.Duration) curvePoint {
	cfg := p.cfg(seed)
	cfg.Fixed, cfg.Duration = n, length
	tr := Run(cfg)
	w := summarize(tr, warmup)
	var a float64
	var comp int
	for i, at := range tr.At {
		if at >= warmup {
			a += tr.Samples[i].Books.SlotSeconds
			comp += tr.Samples[i].Books.Completions
		}
	}
	return curvePoint{float64(n), w.used, w.tput, a / float64(comp)}
}

// at interpolates the curve at S, linearly between whole capacities.
func at(pts []curvePoint, S float64) curvePoint {
	if S <= pts[0].n {
		r := S / pts[0].n
		return curvePoint{S, pts[0].u * r, pts[0].x * r, pts[0].d}
	}
	for i := 1; i < len(pts); i++ {
		if S <= pts[i].n {
			a, b := pts[i-1], pts[i]
			f := (S - a.n) / (b.n - a.n)
			return curvePoint{S, a.u + f*(b.u-a.u), a.x + f*(b.x-a.x), a.d + f*(b.d-a.d)}
		}
	}
	return pts[len(pts)-1]
}

// target is S*, where U crosses ρL.
func target(pts []curvePoint, limit float64) float64 {
	for i := 1; i < len(pts); i++ {
		a, b := pts[i-1], pts[i]
		if a.u <= 0.9*limit && b.u >= 0.9*limit {
			return a.n + (0.9*limit-a.u)/(b.u-a.u)*(b.n-a.n)
		}
	}
	return math.NaN()
}

// iterate runs the noise-free law S' = min(S + S·Δt/D(S), ρL·D(S)/c(S)) from
// s0 and returns the steps until done(S).
func iterate(pts []curvePoint, limit, s0 float64, done func(float64) bool) int {
	S := s0
	for n := 1; n <= 100; n++ {
		p := at(pts, S)
		c := p.u / p.x
		S = math.Min(S+S/p.d, 0.9*limit*p.d/c)
		if done(S) {
			return n
		}
	}
	return -1
}

// iterateDown is §4.3's convergence from above near the knee (plan §4.5): the
// noise-free law's steps from S_start = X*·D(⌈S*(1 + 3r̄)⌉), the law's value
// from lives one whole capacity above the band's top, down to S*(1 + 3r̄).
// Decreases aren't paced (§7), so each step is the law's value.
func iterateDown(pts []curvePoint, limit, sStar, hi float64) int {
	p := at(pts, sStar)
	xStar := 0.9 * limit / (p.u / p.x)
	S := xStar * at(pts, math.Ceil(hi)).d
	for n := 0; n <= 100; n++ {
		if S <= hi {
			return n
		}
		q := at(pts, S)
		S = 0.9 * limit * q.d / (q.u / q.x)
	}
	return -1
}

// alternation sweeps the no-limit node curve at the demand: the node model
// with no quota, the ceiling's 32 slots held fixed, the share in steps of 0.05
// core; per share, the per-sample stall's mean and standard deviation and the
// mean lifetime.
func alternation(t *testing.T) (over, under float64, stretch int) {
	over, under = math.NaN(), math.NaN()
	var dOver float64
	for sh := 0.2; sh <= 4.0+1e-9; sh += 0.05 {
		cfg := Config{Seed: 9001, Duration: 180 * time.Second, Procs: 4, NodeCPUs: 4, Ceiling: 32, Fixed: 32,
			Share:  func(time.Duration) float64 { return sh },
			Queues: []Queue{{Name: "q", Class: fixedClass(baseClass), Active: always}}}
		tr := Run(cfg)
		var sum, sq, a float64
		var n, comp int
		for i, at := range tr.At {
			if at < 60*time.Second {
				continue
			}
			x := 100 * tr.Samples[i].Stall.Seconds() / tr.Samples[i].Dt.Seconds()
			sum, sq, n = sum+x, sq+x*x, n+1
			a += tr.Samples[i].Books.SlotSeconds
			comp += tr.Samples[i].Books.Completions
		}
		m := sum / float64(n)
		sd := math.Sqrt(math.Max(sq/float64(n)-m*m, 0))
		t.Logf("no-limit share %.2f: stall mean %.1f, sd %.1f, D %.3f", sh, m, sd, a/float64(comp))
		if m >= 50+3*sd {
			over, dOver = sh, a/float64(comp)
		}
		if math.IsNaN(under) && m <= 50-3*sd {
			under = sh
		}
	}
	w := math.Ceil(dOver) + 1
	return over, under, int(3 * w)
}

// TestFreeze prints frozen_test.go's constants when SIM_FREEZE is set.
func TestFreeze(t *testing.T) {
	if os.Getenv("SIM_FREEZE") == "" {
		t.Skip("set SIM_FREEZE")
	}
	var b strings.Builder
	b.WriteString("// Code generated by TestFreeze (SIM_FREEZE=1); frozen before any gate run.\n\npackage sim\n\n")
	b.WriteString("// frozenPlants are each case plant's S*, D(S*) and n_iter from its fixed-\n// capacity curve (plan §4.5). nStart counts from one slot to θ⌊S*⌋; nFrom\n// from the base plant's S* into this plant's band; nAbove from\n// X*·D(⌈S*(1 + 3r̄)⌉) down to S*(1 + 3r̄). The drain plants are from the\n// forty-seed mean curve under the GKE protocol; determined is the plan's\n// determinacy rule.\nvar frozenPlants = map[string]frozen{\n")
	limits := map[string]float64{"limit125": 0.125, "limit500": 0.5}
	var baseS float64
	xcaps := map[string]float64{}
	for _, p := range plants() {
		L := 0.25
		if v, ok := limits[p.name]; ok {
			L = v
		}
		// Plan §4.5: the drain plants from the forty-seed mean curve under the
		// GKE protocol, determined by the measurement's rule; the others from
		// seed 9001, determined by the three-seed rule.
		var pts []curvePoint
		var s float64
		var det bool
		if strings.HasPrefix(p.name, "drain") {
			runs := drainCurves(p)
			pts = meanCurve(runs, allSeeds())
			s = target(pts, L)
			blo, bhi, bsd, none := bootstrapS(runs, L)
			det = !math.IsNaN(s) && !math.IsInf(blo, 0) && !math.IsInf(bhi, 0) && bhi-blo < 0.15*s
			t.Logf("%s: S* %.4f from the mean curve of seeds %d–%d; bootstrap 95%% [%.3f, %.3f], width %.3f against 0.15·S* %.3f, sd %.3f, %d of %d with no crossing: determined %v",
				p.name, s, drainSeed0, drainSeed0+drainSeeds-1, blo, bhi, bhi-blo, 0.15*s, bsd, none, drainBoots, det)
			per := make([]string, len(runs))
			for k := range runs {
				per[k] = fmt.Sprintf("%.3f", crossing(runs[k], L))
			}
			t.Logf("%s: per-seed S*: %s", p.name, strings.Join(per, " "))
			if p.name == "drain0" && !det {
				t.Fatalf("drain0 is undetermined: the freeze stops for review (plan §4.5)")
			}
			if p.name == "drain0" && math.Ceil(s) != 7 {
				t.Fatalf("drain0: N₀ = ⌈S*₀⌉ = %.0f, not 7: the freeze stops for review (plan §4.5)", math.Ceil(s))
			}
			if p.name == "drain" {
				t.Log("\n" + fidelity(runs, s, bsd, drainHeavy().CPU.Seconds()))
			}
		} else {
			pts = measureCurve(p)
			s = target(pts, L)
			s2 := target(curveAt(p, 9002, p.warmup, p.length), L)
			s3 := target(curveAt(p, 9003, p.warmup, p.length), L)
			spread := math.Max(s, math.Max(s2, s3)) - math.Min(s, math.Min(s2, s3))
			det = spread < s*3*rbar
			t.Logf("%s: S* %.4f / %.4f / %.4f on seeds 9001–9003, range %.3f against S*·3r̄ %.3f: determined %v", p.name, s, s2, s3, spread, s*3*rbar, det)
			if !det {
				t.Fatalf("%s is undetermined: the freeze stops for review (plan §4.5)", p.name)
			}
		}
		xcaps[p.name] = pts[len(pts)-1].x
		if p.name == "base" {
			baseS = s
		}
		lo, hi := 0.9*math.Floor(s), s*(1+3*0.05)
		nStart := iterate(pts, L, 1, func(S float64) bool { return S >= lo })
		from := baseS
		if strings.HasPrefix(p.name, "drain") {
			from = 1
		}
		nFrom := iterate(pts, L, from, func(S float64) bool { return S >= lo && S <= hi })
		d := at(pts, s).d
		nAbove := iterateDown(pts, L, s, hi)
		fmt.Fprintf(&b, "\t%q: {sStar: %.4f, dStar: %.4f, nStart: %d, nFrom: %d, nAbove: %d, determined: %v},\n", p.name, s, d, nStart, nFrom, nAbove, det)
		for _, q := range pts {
			t.Logf("%s n=%2.0f U=%.4f X=%.4f D=%.4f", p.name, q.n, q.u, q.x, q.d)
		}
	}
	b.WriteString("}\n\n")
	b.WriteString("// frozenXCap is each plant's throughput at the cap: X at its curve's largest\n// capacity (plan §4.5, design §17.8 item 4).\nvar frozenXCap = map[string]float64{\n")
	for _, p := range plants() {
		fmt.Fprintf(&b, "\t%q: %.4f,\n", p.name, xcaps[p.name])
	}
	b.WriteString("}\n\n")
	over, under, stretch := alternation(t)
	fmt.Fprintf(&b, "// The no-limit case's alternation (plan §4.5, the review of 0ce649f), from\n// the node curve at the demand (32 slots fixed, no limit): s_over, the largest\n// share whose mean stall is at least T + 3σ_s; s_under, the smallest whose mean\n// stall is at most T − 3σ_s (σ_s the per-sample stall's standard deviation in\n// the same run); switching every 3·|W|, |W| = ⌈D/Δt⌉ + 1 at s_over.\nconst (\n\tfrozenAltOver    = %.2f\n\tfrozenAltUnder   = %.2f\n\tfrozenAltStretch = %d * time.Second\n)\n", over, under, stretch)
	t.Log("\n" + b.String())
}
