package sim

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit/internal/costmodel/internal/fpmath"
)

// The drain plants' freeze (plan §4.5, the GKE plant curve, 2026-11-11): each
// drain plant's curve on forty seeds under the GKE runs' protocol, frozen from
// the mean curve; its S* determined by the measurement's rule; and, for the
// drain plant, the pre-registered fidelity tests, reported and not judged.

const (
	drainSeed0    = 9001
	drainSeeds    = 40 // 9001–9040, the user's decision on the re-check's M5
	drainWarmup   = 300 * time.Second
	drainLength   = 2100 * time.Second // 300 s of warm-up, then 1800 s measured
	drainBoots    = 10000
	drainBootSeed = 20261111
	// fidelityDelta is test (i)'s margin, r̄·ρL at the 250m limit.
	fidelityDelta = rbar * rho * 0.25
	// gkeSStar and gkeSE are the measured S* and its standard error for test
	// (iii): SE_g = 0.15, the re-check's 0.13–0.15.
	gkeSStar = 4.14
	gkeSE    = 0.15
	// mvaThink is the drain plant's think time for test (ii).
	mvaThink = 28.5
)

// gkeRuns are the GKE plant curve's per-run mean U in cores, by n, from
// hack/rlgov-measure/results/curve-report.txt (22 runs, every one valid).
var gkeRuns = map[int][]float64{
	3: {0.1897, 0.1854},
	4: {0.2219, 0.2188, 0.2146, 0.2340, 0.2238, 0.2247},
	5: {0.2374, 0.2423, 0.2451, 0.2332, 0.2327, 0.2319},
	6: {0.2405, 0.2428, 0.2445, 0.2433},
	7: {0.2442, 0.2490},
	8: {0.2500, 0.2500},
}

// drainCurves runs the plant at n = 1..maxN on the forty seeds, in parallel;
// runs[k][n−1] is seed 9001+k at capacity n.
func drainCurves(p plant) [][]curvePoint {
	runs := make([][]curvePoint, drainSeeds)
	for k := range runs {
		runs[k] = make([]curvePoint, p.maxN)
	}
	parallel(drainSeeds*p.maxN, func(i int) {
		k, n := i/p.maxN, i%p.maxN
		runs[k][n] = pointAt(p, uint64(drainSeed0+k), n+1, drainWarmup, drainLength)
	})
	return runs
}

// meanCurve is the curve of per-n means over the given seeds' runs.
func meanCurve(runs [][]curvePoint, idx []int) []curvePoint {
	pts := make([]curvePoint, len(runs[0]))
	for n := range pts {
		var q curvePoint
		for _, k := range idx {
			q.u += runs[k][n].u
			q.x += runs[k][n].x
			q.d += runs[k][n].d
		}
		m := float64(len(idx))
		pts[n] = curvePoint{runs[0][n].n, q.u / m, q.x / m, q.d / m}
	}
	return pts
}

func allSeeds() []int {
	idx := make([]int, drainSeeds)
	for k := range idx {
		idx[k] = k
	}
	return idx
}

// crossing is target(), with a curve that never crosses ρL sorted to the end
// it lies at: −Inf if it starts above, +Inf otherwise.
func crossing(pts []curvePoint, limit float64) float64 {
	s := target(pts, limit)
	if math.IsNaN(s) {
		if pts[0].u > 0.9*limit {
			return math.Inf(-1)
		}
		return math.Inf(1)
	}
	return s
}

// bootstrapS resamples the seeds with replacement and returns the 2.5% and
// 97.5% order statistics of the mean curve's crossing, the standard deviation
// of the finite ones, and the count with no crossing.
func bootstrapS(runs [][]curvePoint, limit float64) (lo, hi, sd float64, none int) {
	r := newRNG(drainBootSeed)
	bs := make([]float64, drainBoots)
	idx := make([]int, len(runs))
	for b := range bs {
		for k := range idx {
			idx[k] = int(r.uint64() % uint64(len(runs)))
		}
		bs[b] = crossing(meanCurve(runs, idx), limit)
	}
	sort.Float64s(bs)
	var sum, sq float64
	var m int
	for _, x := range bs {
		if math.IsInf(x, 0) {
			none++
			continue
		}
		sum, sq, m = sum+x, sq+float64(x*x), m+1
	}
	mean := sum / float64(m)
	sd = math.Sqrt((sq - float64(float64(m)*mean*mean)) / float64(m-1))
	ilo, ihi := bootOrder(drainBoots)
	return bs[ilo], bs[ihi], sd, none
}

// bootOrder is the bootstrap's 2.5% and 97.5% order statistics' indices in b
// sorted resamples, as curve_report.py takes them: out[int(0.025·b)] and
// out[int(0.975·b) − 1].
func bootOrder(b int) (lo, hi int) { return b * 25 / 1000, b*975/1000 - 1 }

// meanVar is the mean and the sample variance.
func meanVar(xs []float64) (m, v float64) {
	for _, x := range xs {
		m += x
	}
	m /= float64(len(xs))
	for _, x := range xs {
		v += float64((x - m) * (x - m))
	}
	return m, v / float64(len(xs)-1)
}

// n3Limit is test (i)'s largest |Δ| that can pass at n = 3, pre-registered
// (plan §4.5): δ − t·se with forty seeds, the engine sd at n = 3 taken from the
// uncalibrated plant's ten seeds (0.0068), and GKE's pooled sd at n = 3..5.
const n3Limit = 0.0037

// welch is the standard error of engine − GKE and the one-sided 95% t quantile
// on the Welch–Satterthwaite df nu: the engine's variance ev over k seeds,
// GKE's pooled sd sg over gk runs on dfg df.
func welch(ev, k, sg, gk, dfg float64) (se, t, nu float64) {
	a, b := ev/k, sg*sg/gk
	nu = (a + b) * (a + b) / (a*a/(k-1) + b*b/dfg)
	return math.Sqrt(a + b), tQuantile(0.95, nu), nu
}

// gkePooled is the GKE between-run sd pooled over the n given, and its df.
func gkePooled(ns ...int) (sd float64, df int) {
	var ss float64
	for _, n := range ns {
		_, v := meanVar(gkeRuns[n])
		k := len(gkeRuns[n])
		ss += float64(float64(k-1) * v)
		df += k - 1
	}
	return math.Sqrt(ss / float64(df)), df
}

// mvaU is mean-value analysis of the processor-sharing closed queue: n
// reconciles, think time z seconds, service c/L seconds at the limit L; the
// product form gives U = L·(1 − P₀).
func mvaU(n int, c, z, limit float64) float64 {
	s := c / limit
	sum, term := 1.0, 1.0
	for k := 1; k <= n; k++ {
		term = float64(term * (float64(n-k+1) * s / z))
		sum += term
	}
	return float64(limit * (1 - 1/sum))
}

// fidelity is the pre-registered fidelity report (plan §4.5): (i) equivalence
// with GKE in U at n = 3..7, (ii) the engine against MVA at the same c, (iii)
// S* against the measured 4.14 and the between-run sds. Reported, not judged.
func fidelity(runs [][]curvePoint, sStar, seBoot float64, c float64) string {
	var b strings.Builder
	k := float64(len(runs))
	col := func(n int) []float64 {
		xs := make([]float64, len(runs))
		for i := range runs {
			xs[i] = runs[i][n-1].u
		}
		return xs
	}
	fmt.Fprintf(&b, "fidelity (i), equivalence with GKE in U, δ = r̄·ρL = %.4f cores; 90%% Welch intervals of engine − GKE\n", fidelityDelta)
	all := true
	for n := 3; n <= 7; n++ {
		em, ev := meanVar(col(n))
		gm, _ := meanVar(gkeRuns[n])
		sg, dfg := gkePooled(3, 4, 5)
		if n >= 6 {
			sg, dfg = gkePooled(6, 7)
		}
		se, t, nu := welch(ev, k, sg, float64(len(gkeRuns[n])), float64(dfg))
		d := em - gm
		pass := math.Abs(d)+float64(t*se) < fidelityDelta
		all = all && pass
		fmt.Fprintf(&b, "  n=%d engine %.4f GKE %.4f (k=%d, pooled sd %.4f on %d df) Δ %+.4f [%+.4f, %+.4f] ν %.1f: %s\n",
			n, em, gm, len(gkeRuns[n]), sg, dfg, d, d-float64(t*se), d+float64(t*se), nu, map[bool]string{true: "equivalent", false: "not shown"}[pass])
		if n == 3 && !pass {
			fmt.Fprintf(&b, "    not shown; at n=3 the test can only pass if |Δ| < %.4f. A failure isn't evidence that the plants differ.\n", n3Limit)
			fmt.Fprintf(&b, "    (the limit with this run's own engine sd: |Δ| < %.4f)\n", fidelityDelta-float64(t*se))
		}
	}
	em8, _ := meanVar(col(8))
	gm8, _ := meanVar(gkeRuns[8])
	fmt.Fprintf(&b, "  n=8 reported: engine %.4f GKE %.4f (GKE's two runs read the cap exactly)\n", em8, gm8)
	fmt.Fprintf(&b, "  equivalence at n = 3..7 (all five): %v. Reported, not judged; n = 6..7 sit near the cap, nearly automatic.\n", all)
	fmt.Fprintf(&b, "fidelity (ii), the informative test: engine − MVA at c = %.3f s, think time %.1f s; 95%% t(%d) intervals, against ±δ\n", c, mvaThink, len(runs)-1)
	t39 := tQuantile(0.975, k-1)
	for n := 3; n <= 8; n++ {
		em, ev := meanVar(col(n))
		mv := mvaU(n, c, mvaThink, 0.25)
		gm, _ := meanVar(gkeRuns[n])
		h := float64(t39 * math.Sqrt(ev/k))
		d := em - mv
		fmt.Fprintf(&b, "  n=%d engine %.4f MVA %.4f Δ %+.4f [%+.4f, %+.4f] within ±δ: %v; MVA − GKE %+.4f\n",
			n, em, mv, d, d-h, d+h, math.Abs(d)+h < fidelityDelta, mv-gm)
	}
	mvaPts := make([]curvePoint, 12)
	for n := range mvaPts {
		mvaPts[n] = curvePoint{n: float64(n + 1), u: mvaU(n+1, c, mvaThink, 0.25)}
	}
	fmt.Fprintf(&b, "  MVA's S* at this c: %.3f\n", target(mvaPts, 0.25))
	z := (sStar - gkeSStar) / fpmath.Hypot(seBoot, gkeSE)
	fmt.Fprintf(&b, "fidelity (iii), reported: S*_engine − %.2f = %+.3f, z %.2f (SE_e %.3f from the bootstrap, SE_g %.2f)\n", gkeSStar, sStar-gkeSStar, z, seBoot, gkeSE)
	fmt.Fprintf(&b, "  between-run sd, engine against GKE:")
	for n := 3; n <= 8; n++ {
		_, ev := meanVar(col(n))
		_, gv := meanVar(gkeRuns[n])
		fmt.Fprintf(&b, " n=%d %.4f/%.4f", n, math.Sqrt(ev), math.Sqrt(gv))
	}
	b.WriteString("\n")
	return b.String()
}

// tQuantile is Student's t quantile at probability p on ν degrees of freedom,
// by bisection on the CDF.
func tQuantile(p, nu float64) float64 {
	lo, hi := 0.0, 100.0
	for i := 0; i < 200; i++ {
		mid := (lo + hi) / 2
		if tCDF(mid, nu) < p {
			lo = mid
		} else {
			hi = mid
		}
	}
	return (lo + hi) / 2
}

// tCDF is Student's t CDF for t ≥ 0: 1 − I_x(ν/2, 1/2)/2 with x = ν/(ν + t²).
func tCDF(t, nu float64) float64 {
	return 1 - float64(0.5*betaInc(nu/2, 0.5, nu/(nu+float64(t*t))))
}

// betaInc is the regularized incomplete beta function I_x(a, b), by the
// continued fraction (Numerical Recipes, betacf).
func betaInc(a, b, x float64) float64 {
	if x <= 0 {
		return 0
	}
	if x >= 1 {
		return 1
	}
	la, lb, lc := fpmath.Lgamma(a+b), fpmath.Lgamma(a), fpmath.Lgamma(b)
	front := fpmath.Exp(la - lb - lc + float64(a*fpmath.Log(x)) + float64(b*fpmath.Log(1-x)))
	if x < (a+1)/(a+b+2) {
		return front * betaCF(a, b, x) / a
	}
	return 1 - front*betaCF(b, a, 1-x)/b
}

func betaCF(a, b, x float64) float64 {
	const tiny = 1e-300
	qab, qap, qam := a+b, a+1, a-1
	c, d := 1.0, 1-qab*x/qap
	if math.Abs(d) < tiny {
		d = tiny
	}
	d = 1 / d
	h := d
	for m := 1; m <= 300; m++ {
		fm := float64(m)
		aa := fm * (b - fm) * x / ((qam + 2*fm) * (a + 2*fm))
		d = 1 + float64(aa*d)
		if math.Abs(d) < tiny {
			d = tiny
		}
		c = 1 + aa/c
		if math.Abs(c) < tiny {
			c = tiny
		}
		d = 1 / d
		h *= d * c
		aa = -(a + fm) * (qab + fm) * x / ((a + 2*fm) * (qap + 2*fm))
		d = 1 + float64(aa*d)
		if math.Abs(d) < tiny {
			d = tiny
		}
		c = 1 + aa/c
		if math.Abs(c) < tiny {
			c = tiny
		}
		d = 1 / d
		del := float64(d * c)
		h *= del
		if math.Abs(del-1) < 3e-14 {
			break
		}
	}
	return h
}

// TestFreezeStats checks the freeze's statistics against known values; it
// runs no engine.
func TestFreezeStats(t *testing.T) {
	for _, c := range []struct{ p, nu, want float64 }{
		{0.975, 9, 2.2622}, {0.975, 39, 2.0227}, {0.95, 10, 1.8125}, {0.95, 4, 2.1318}, {0.975, 1, 12.7062},
	} {
		if got := tQuantile(c.p, c.nu); math.Abs(got-c.want) > 5e-4 {
			t.Errorf("tQuantile(%v, %v) = %.4f, want %.4f", c.p, c.nu, got, c.want)
		}
	}
	// MVA at c = 3.157 s, think time 28.5 s, 250m (plan §4.5): 0.1880 at n = 3,
	// 0.2193 at 4, 0.2369 at 5.
	for n, want := range map[int]float64{3: 0.1880, 4: 0.2193, 5: 0.2369} {
		if got := mvaU(n, 3.157, mvaThink, 0.25); math.Abs(got-want) > 5e-5 {
			t.Errorf("mvaU(%d) = %.4f, want %.4f", n, got, want)
		}
	}
	if lo, hi := bootOrder(drainBoots); lo != 250 || hi != 9749 {
		t.Errorf("bootOrder(%d) = %d, %d, want curve_report.py's 250, 9749", drainBoots, lo, hi)
	}
	sd, df := gkePooled(3, 4, 5)
	if df != 11 || math.Abs(sd-0.0058) > 1e-4 {
		t.Errorf("pooled sd at n = 3..5 = %.4f on %d df, want 0.0058 on 11", sd, df)
	}
	// The n = 3 pass limit with the uncalibrated sd 0.0068: 0.0031 at ten seeds
	// (the review's L8 quoted 0.0032, from rounded inputs), and the
	// pre-registered n3Limit at forty (0.00366).
	for _, c := range []struct{ k, want float64 }{{10, 0.0031}, {drainSeeds, n3Limit}} {
		se, tq, _ := welch(0.0068*0.0068, c.k, sd, 2, float64(df))
		if got := fidelityDelta - float64(tq*se); math.Abs(got-c.want) > 5e-5 {
			t.Errorf("the n = 3 limit at %v seeds = %.5f, want %.4f", c.k, got, c.want)
		}
	}
}
