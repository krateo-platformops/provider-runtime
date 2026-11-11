package sim

import (
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

// TestCurveDiag is a development aid for the plan §4.5 change on the measured
// S*: the engine's fixed-capacity curves on several seeds, by the freeze's
// method, and the drain plant under the GKE protocol, with each drain seed's
// curve and every crossing of ρL. Its output at b2e5757, before the drain
// plants' calibration, is testdata/curve-engine-b2e5757.txt. Skipped unless SIM_CURVEDIAG is set.
// Seeds 9001+ only (21–40 are reserved).
func TestCurveDiag(t *testing.T) {
	if os.Getenv("SIM_CURVEDIAG") == "" {
		t.Skip("set SIM_CURVEDIAG")
	}
	var b strings.Builder
	freezeDrain := map[uint64][]curvePoint{}
	limits := map[string]float64{"limit125": 0.125, "limit500": 0.5}
	// 1. Freeze method, per plant, seeds 9001–9003 (the determinacy rule) and,
	// for the drain plants, 9001–9010.
	for _, p := range plants() {
		L := 0.25
		if v, ok := limits[p.name]; ok {
			L = v
		}
		seeds := 3
		if strings.HasPrefix(p.name, "drain") {
			seeds = 10
		}
		var ss []float64
		for k := 0; k < seeds; k++ {
			seed := uint64(9001 + k)
			pts := curveAt(p, seed, p.warmup, p.length)
			ss = append(ss, target(pts, L))
			if p.name == "drain" {
				freezeDrain[seed] = pts
			}
		}
		fmt.Fprintf(&b, "freeze %s:", p.name)
		for k, s := range ss {
			fmt.Fprintf(&b, " %d:%.3f", 9001+k, s)
		}
		fmt.Fprintf(&b, "\n")
	}
	// 2. The drain plant under the GKE protocol: 300s warm-up, 1800s
	// measured, n = 1..10, seeds 9001–9010; per n the mean of run means, the
	// between-run sd, CPU per completion and X.
	p := plant{name: "drain", maxN: 10, warmup: 300 * time.Second, length: 2100 * time.Second,
		cfg: func(seed uint64) Config { c := measured(seed); c.Queues[0].Class = fixedClass(drainHeavy()); return c }}
	const K = 10
	runs := make([][]curvePoint, K)
	for k := 0; k < K; k++ {
		runs[k] = curveAt(p, uint64(9001+k), p.warmup, p.length)
	}
	for n := 0; n < p.maxN; n++ {
		var su, sq, sx, sc float64
		for k := 0; k < K; k++ {
			q := runs[k][n]
			su, sq = su+q.u, sq+float64(q.u*q.u)
			sx += q.x
			sc += q.u / q.x
		}
		m := su / K
		sd := math.Sqrt(math.Max(sq/K-float64(m*m), 0) * K / (K - 1))
		fmt.Fprintf(&b, "gke-protocol drain n=%2d meanU %.4f sd %.4f X %.4f c/compl %.3f\n", n+1, m, sd, sx/K, sc/K)
	}
	fmt.Fprintf(&b, "gke-protocol drain S* per seed:")
	var mean []curvePoint
	for n := 0; n < p.maxN; n++ {
		var su, sx, sd float64
		for k := 0; k < K; k++ {
			su += runs[k][n].u
			sx += runs[k][n].x
			sd += runs[k][n].d
		}
		mean = append(mean, curvePoint{float64(n + 1), su / K, sx / K, sd / K})
	}
	for k := 0; k < K; k++ {
		fmt.Fprintf(&b, " %.3f", target(runs[k], 0.25))
	}
	fmt.Fprintf(&b, "\ngke-protocol drain S* of the mean curve: %.3f\n", target(mean, 0.25))
	// 3. Each drain seed's curve, n = 1..10, and every crossing of ρL, under
	// both protocols (target() takes the first).
	for k := 0; k < K; k++ {
		seed := uint64(9001 + k)
		for _, c := range []struct {
			name string
			pts  []curvePoint
		}{{"freeze", freezeDrain[seed]}, {"gke-protocol", runs[k]}} {
			fmt.Fprintf(&b, "%s drain seed %d U:", c.name, seed)
			for _, q := range c.pts[:10] {
				fmt.Fprintf(&b, " %.4f", q.u)
			}
			fmt.Fprintf(&b, " crossings:")
			for _, x := range crossings(c.pts, 0.25) {
				fmt.Fprintf(&b, " %.3f", x)
			}
			fmt.Fprintf(&b, "\n")
		}
	}
	t.Log("\n" + b.String())
}

// crossings is every point where the curve crosses ρL, up or down, linear
// between whole capacities.
func crossings(pts []curvePoint, limit float64) []float64 {
	var xs []float64
	r := float64(0.9 * limit)
	for i := 1; i < len(pts); i++ {
		a, b := pts[i-1], pts[i]
		if (a.u-r)*(b.u-r) <= 0 && a.u != b.u {
			xs = append(xs, a.n+float64((r-a.u)/(b.u-a.u)*(b.n-a.n)))
		}
	}
	return xs
}
