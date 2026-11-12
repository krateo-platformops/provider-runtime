package credentialuse

import (
	"errors"
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

// TestModelStatelessSize measures the differential test's stateless leg
// (docs/credential-use-model-reduction.md §11.1): for each small scenario at
// {0,1} and {1,0} with T = 2, Knuth's estimate of its schedule count, and a
// capped serial search with its rate and its position in the tree. It runs
// only with CREDENTIALUSE_MODEL_SIZE set; CREDENTIALUSE_MODEL_SIZE_PROBES and
// _CAP change N (20,000) and the cap (300,000), _ONLY (a regexp) picks
// scenarios, _JUMPS sets T (2), and CREDENTIALUSE_MODEL_SEED replays the
// probes.
func TestModelStatelessSize(t *testing.T) {
	if os.Getenv("CREDENTIALUSE_MODEL_SIZE") == "" {
		t.Skip("a measurement, run on its own (CREDENTIALUSE_MODEL_SIZE)")
	}
	t.Parallel()
	probes, limit := envInt("CREDENTIALUSE_MODEL_SIZE_PROBES", 20_000), envInt("CREDENTIALUSE_MODEL_SIZE_CAP", 300_000)
	jumps := envInt("CREDENTIALUSE_MODEL_SIZE_JUMPS", testJumps) // T (§11.3)
	only := regexp.MustCompile(os.Getenv("CREDENTIALUSE_MODEL_SIZE_ONLY"))
	seed := walkSeed()
	t.Logf("size: probes %d, cap %d, T %d, seed %d", probes, limit, jumps, seed)
	for name, sc := range smallScenarios {
		if !only.MatchString(name) {
			continue
		}
		for _, b := range []apimodel.Bounds{{Faults: 1, Jumps: jumps}, {Preemptions: 1, Jumps: jumps}} {
			t.Run(fmt.Sprintf("%s, %+v", name, b), func(t *testing.T) {
				t.Parallel()
				label := fmt.Sprintf("%s {%d,%d} T=%d", name, b.Preemptions, b.Faults, b.Jumps)
				knuth(t, label, sc, b, probes, seed)
				capped(t, label, sc, b, limit)
			})
		}
	}
}

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
		return v
	}
	return def
}

// stateless runs sc as the differential test's stateless leg does, rule R
// off, and hands each schedule's path to path. A violation doesn't stop the
// search; only the harness's own errors are failures.
func stateless(sc func(w *world), path func(choices, options []int)) func(s *apimodel.Sched) error {
	inner := runner(sc, nil, quietMode{off: true})
	return func(s *apimodel.Sched) error {
		err := inner(s)
		path(s.Choices())
		var v violation
		if errors.As(err, &v) {
			return nil
		}
		return err
	}
}

// logMean accumulates exp(x) for x given in the log domain: the mean and the
// relative standard error of the mean, without overflow.
type logMean struct {
	xs []float64
}

func (m *logMean) add(x float64) { m.xs = append(m.xs, x) }

func (m *logMean) stats() (mean, rse float64) {
	top := math.Inf(-1)
	for _, x := range m.xs {
		top = max(top, x)
	}
	var s1, s2 float64
	for _, x := range m.xs {
		e := math.Exp(x - top)
		s1 += e
		s2 += e * e
	}
	n := float64(len(m.xs))
	mu := s1 / n
	variance := s2/n - mu*mu
	if n > 1 {
		variance *= n / (n - 1)
	}
	return mu * math.Exp(top), math.Sqrt(math.Max(variance, 0)/n) / mu
}

// knuth is Knuth's estimate: probes uniform schedules, each weighted by the
// product of its option counts.
func knuth(t *testing.T, label string, sc func(w *world), b apimodel.Bounds, probes int, seed int64) {
	var m logMean
	start := time.Now()
	res := apimodel.Walk(b, probes, seed, stateless(sc, func(_, options []int) {
		x := 0.0
		for _, n := range options {
			x += math.Log(float64(n))
		}
		m.add(x)
		if k := len(m.xs); k%(max(probes/10, 1)) == 0 {
			mean, rse := m.stats()
			t.Logf("size: %s knuth probes=%d mean=%.4g rse=%.3f elapsed=%s", label, k, mean, rse, time.Since(start).Round(time.Second))
		}
	}))
	for _, f := range res.Failures {
		t.Errorf("%s: the harness failed: %v\nschedule: %s", label, f.Err, f.Trace)
	}
	mean, rse := m.stats()
	t.Logf("size: %s knuth final probes=%d mean=%.4g rse=%.3f rate=%.1f/s", label, len(m.xs), mean, rse, float64(len(m.xs))/time.Since(start).Seconds())
}

// capped is the serial stateless search, stopped at limit schedules, with
// its rate and its position in the tree.
func capped(t *testing.T, label string, sc func(w *world), b apimodel.Bounds, limit int) {
	b.MaxRuns = limit
	runs, pos := 0, 0.0
	start := time.Now()
	report := func(final string) {
		el := time.Since(start)
		est := math.NaN()
		if pos > 0 {
			est = float64(runs) / pos
		}
		t.Logf("size: %s dfs%s runs=%d elapsed=%s rate=%.1f/s position=%.4g est=%.4g", label, final, runs, el.Round(time.Second), float64(runs)/el.Seconds(), pos, est)
	}
	res := apimodel.Explore(b, stateless(sc, func(choices, options []int) {
		runs++
		// The fraction of leaves before this one, if every subtree at a
		// choice were the same size.
		f, w := 0.0, 1.0
		for i, c := range choices {
			f += w * float64(c) / float64(options[i])
			w /= float64(options[i])
		}
		pos = f
		if runs%10_000 == 0 {
			report("")
		}
	}))
	for _, f := range res.Failures {
		t.Errorf("%s: the harness failed: %v\nschedule: %s", label, f.Err, f.Trace)
	}
	if res.Finished {
		t.Logf("size: %s dfs finished: exactly %d schedules in %s", label, res.Runs, time.Since(start).Round(time.Second))
		return
	}
	report(" capped")
}
