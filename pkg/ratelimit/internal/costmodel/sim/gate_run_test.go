package sim

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func seeds() []uint64 {
	n := 20
	switch {
	case raceEnabled:
		n = 2
	case testing.Short():
		n = 5
	}
	s := make([]uint64, n)
	for i := range s {
		s[i] = uint64(i + 1)
	}
	return s
}

// TestGate runs every case on the cost model; every seed must pass.
func TestGate(t *testing.T) {
	for _, c := range gateCases() {
		t.Run(c.name, func(t *testing.T) {
			for _, seed := range seeds() {
				t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
					t.Parallel()
					v := runCase(c, ctlCost, seed)
					for _, r := range v.reports {
						t.Log(r)
					}
					if v.invalid != "" {
						t.Errorf("INVALID: %s", v.invalid)
					}
					for _, f := range v.fails {
						t.Error(f)
					}
				})
			}
		})
	}
}

// parallel runs f(i) for i in [0, n) on every CPU.
func parallel(n int, f func(i int)) {
	var wg sync.WaitGroup
	next := make(chan int)
	for w := 0; w < runtime.NumCPU(); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				f(i)
			}
		}()
	}
	for i := 0; i < n; i++ {
		next <- i
	}
	close(next)
	wg.Wait()
}

// TestGateReport runs every case on the three controllers (the level PI at its
// committed gains and at ×0.5 and ×2, and the reservation gate at Temporal's
// defaults) and prints the side-by-side table, when SIM_GATE is set; with
// SIM_GATE=cost, the cost model alone. It fails nothing: it reports.
func TestGateReport(t *testing.T) {
	if os.Getenv("SIM_GATE") == "" {
		t.Skip("set SIM_GATE")
	}
	ctls := []controller{ctlCost, ctlBaseline(1), ctlBaseline(0.5), ctlBaseline(2), ctlReserve}
	if os.Getenv("SIM_GATE") == "cost" {
		ctls = ctls[:1]
	}
	cases := gateCases()
	ss := seeds()
	type job struct{ c, k, s int }
	var jobs []job
	for c := range cases {
		for k := range ctls {
			for s := range ss {
				jobs = append(jobs, job{c, k, s})
			}
		}
	}
	out := make([]verdict, len(jobs))
	start := time.Now()
	parallel(len(jobs), func(i int) {
		j := jobs[i]
		out[i] = runCase(cases[j.c], ctls[j.k], ss[j.s])
	})
	var b strings.Builder
	fmt.Fprintf(&b, "\n%d seeds; %s\n\n| Case |", len(ss), time.Since(start).Round(time.Second))
	for _, k := range ctls {
		fmt.Fprintf(&b, " %s |", k.name)
	}
	b.WriteString("\n|---|")
	for range ctls {
		b.WriteString("---|")
	}
	b.WriteString("\n")
	details := map[string][]string{}
	for c, gc := range cases {
		fmt.Fprintf(&b, "| %s |", gc.name)
		for k, ctl := range ctls {
			pass, invalid := 0, 0
			msgs := map[string]int{}
			var reps []string
			for s := range ss {
				v := out[(c*len(ctls)+k)*len(ss)+s]
				switch {
				case v.invalid != "":
					invalid++
					msgs["INVALID: "+v.invalid]++
				case len(v.fails) == 0:
					pass++
				}
				for _, f := range v.fails {
					msgs[firstWords(f)]++
				}
				if s < 3 {
					for _, r := range v.reports {
						reps = append(reps, fmt.Sprintf("seed %d: %s", ss[s], r))
					}
				}
			}
			cell := fmt.Sprintf(" %d/%d", pass, len(ss))
			if invalid > 0 {
				cell += fmt.Sprintf(" (%d invalid)", invalid)
			}
			b.WriteString(cell + " |")
			key := gc.name + " — " + ctl.name
			var ks []string
			for m, n := range msgs {
				ks = append(ks, fmt.Sprintf("%d× %s", n, m))
			}
			sort.Strings(ks)
			details[key] = append(ks, reps...)
		}
		b.WriteString("\n")
	}
	var keys []string
	for k := range details {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if len(details[k]) == 0 {
			continue
		}
		fmt.Fprintf(&b, "\n%s:\n", k)
		for _, m := range details[k] {
			fmt.Fprintf(&b, "  %s\n", m)
		}
	}
	t.Log(b.String())
}

// firstWords trims a failure to its kind, for counting across seeds.
func firstWords(s string) string {
	if i := strings.IndexAny(s, "0123456789"); i > 0 && i < 60 {
		return strings.TrimSpace(s[:i]) + " …"
	}
	if len(s) > 80 {
		return s[:80] + " …"
	}
	return s
}

// TestTuneBaseline tunes the baseline's gains on the headline case, seeds
// 101–120, by the plan's §4.5 rule, when SIM_TUNE is set, and prints the
// gains to commit (baseline_gains_test.go) before any other case runs.
func TestTuneBaseline(t *testing.T) {
	if os.Getenv("SIM_TUNE") == "" {
		t.Skip("set SIM_TUNE")
	}
	grid := tuningGrid()
	head := gateCases()[0]
	b := bandOf(frozenPlants["base"].sStar)
	type score struct {
		g                          Baseline
		passAll                    bool
		meanEntry, variance, outFr float64
	}
	scores := make([]score, len(grid))
	parallel(len(grid), func(i int) {
		g := grid[i]
		sc := score{g: g, passAll: true}
		var entries, vars, outs float64
		for seed := uint64(101); seed <= 120; seed++ {
			cfg := head.cfg(seed)
			bl := g
			cfg.Controller = func() Controller { return NewBaseline(bl) }
			tr := Run(cfg)
			var v verdict
			gx := &gctx{tr: tr, cfg: cfg, v: &v}
			head.check(gx)
			e := gx.entry(b, 0)
			gx.outside(b, e, gx.n())
			if e < 0 || e+1 > 20 {
				gx.fail("no entry within 20 samples")
				e = gx.n()
			}
			if len(v.fails) > 0 {
				sc.passAll = false
			}
			entries += float64(e + 1)
			var s, s2, out float64
			var n int
			for j := e; j < gx.n(); j++ {
				x := gx.S(j)
				s, s2, n = s+x, s2+x*x, n+1
				if !b.in(x) {
					out++
				}
			}
			if n > 0 {
				m := s / float64(n)
				vars += s2/float64(n) - m*m
				outs += out / float64(n)
			} else {
				outs++
			}
		}
		sc.meanEntry, sc.variance, sc.outFr = entries/20, vars/20, outs/20
		scores[i] = sc
	})
	sort.SliceStable(scores, func(i, j int) bool {
		a, c := scores[i], scores[j]
		if a.passAll != c.passAll {
			return a.passAll
		}
		if !a.passAll {
			return a.outFr < c.outFr
		}
		if a.meanEntry != c.meanEntry {
			return a.meanEntry < c.meanEntry
		}
		return a.variance < c.variance
	})
	for i := 0; i < 10 && i < len(scores); i++ {
		s := scores[i]
		t.Logf("Kp %.4g Ki %.4g τU %g: passes all %v, mean entry %.2f, variance %.4f, outside %.3f", s.g.Kp, s.g.Ki, s.g.TauU, s.passAll, s.meanEntry, s.variance, s.outFr)
	}
	best := scores[0]
	if !best.passAll {
		t.Logf("no grid point passes the headline in all 20 tuning seeds; chosen by the smallest fraction outside")
	}
	t.Logf("\npackage sim\n\nvar baselineGains = Baseline{Kp: %v, Ki: %v, TauU: %v}\n", best.g.Kp, best.g.Ki, best.g.TauU)
}

// tuningGrid is the plan's §4.5 grid, committed before any run: K_p and K_i on
// a 13-point log grid over [0.1, 100] (K_i also 0), τ_U ∈ {0, 1, 2, 5, 10}s;
// and, after the first tuning chose K_p at the grid's edge, K_p ∈
// {0, 0.01, 0.03} added (design §17.8, item 6).
func tuningGrid() []Baseline {
	var logs []float64
	for i := 0; i < 13; i++ {
		logs = append(logs, 0.1*math.Pow(1000, float64(i)/12))
	}
	kis := append([]float64{0}, logs...)
	kps := append([]float64{0, 0.01, 0.03}, logs...)
	var g []Baseline
	for _, tu := range []float64{0, 1, 2, 5, 10} {
		for _, kp := range kps {
			for _, ki := range kis {
				g = append(g, Baseline{Kp: kp, Ki: ki, TauU: tu, Target: rho, PressureThreshold: 50})
			}
		}
	}
	return g
}
