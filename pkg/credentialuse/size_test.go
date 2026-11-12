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
	scenarios := smallScenarios
	if os.Getenv("CREDENTIALUSE_MODEL_SIZE_SHRUNK") != "" {
		scenarios = shrunkScenarios
	}
	for name, sc := range scenarios {
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

// shrunkScenarios are candidate shrinks of the three scenarios whose
// stateless reference is out of reach even at T = 1 (§11.3), for sizing only
// (CREDENTIALUSE_MODEL_SIZE_SHRUNK): each is its small scenario with one
// deletion pass per holder instead of two, and nothing else changed.
var shrunkScenarios = map[string]func(w *world){
	"away and back, small, with a relist, one deletion pass": func(w *world) {
		seed(w.m, w.ns, w.p.Name, []string{"h1"}, true)
		pa := newProcess("A", w.m, w.s.Now())
		pb := newProcess("B", w.m, w.s.Now())
		w.informer("informerA", &pa)
		w.relister("relistA", &pa)
		secret := SecretRef(w.ns, w.p.Name)
		w.holder(holderSpec{
			name: "h1",
			refs: func(pass int) []Ref {
				if pass == 1 {
					return nil
				}
				return []Ref{secret}
			},
			livePasses: 3, deletionPasses: 1,
		}, &pa)
		w.releaser("releaserB", &pb, 1, true, nil)
		w.deleter("teardown", keyOf(w.p), widgetKey(w.ns, "h1"))
	}, "two releasers, one deletion pass": func(w *world) {
		seed(w.m, w.ns, w.p.Name, []string{"h1"}, true)
		pa := newProcess("A", w.m, w.s.Now())
		pb := newProcess("B", w.m, w.s.Now())
		pc := newProcess("C", w.m, w.s.Now())
		w.informer("informerA", &pa)
		w.holder(holderSpec{name: "h1", livePasses: 1, deletionPasses: 1}, &pa)
		w.releaser("releaserB", &pb, 1, true, nil)
		w.releaser("releaserC", &pc, 1, true, nil)
		w.deleter("teardown", keyOf(w.p), widgetKey(w.ns, "h1"))
	}, "two processes, shrunk, one deletion pass each": func(w *world) {
		seed(w.m, w.ns, w.p.Name, []string{"h1"}, true)
		pa := newProcess("A", w.m, w.s.Now())
		pb := newProcess("B", w.m, w.s.Now())
		pc := newProcess("C", w.m, w.s.Now())
		w.informer("informerA", &pa)
		secret := SecretRef(w.ns, w.p.Name)
		w.holder(holderSpec{name: "h1", actor: "h1@A", livePasses: 2, deletionPasses: 1, before: func(n int) func() bool {
			if n != 1 {
				return nil
			}
			return func() bool { return w.started["h1@B"] >= 2 }
		}}, &pa)
		w.holder(holderSpec{
			name: "h1", actor: "h1@B",
			refs: func(pass int) []Ref {
				if pass == 0 {
					return nil
				}
				return []Ref{secret}
			},
			livePasses: 2, deletionPasses: 1,
			before: func(n int) func() bool {
				switch n {
				case 0:
					return func() bool { return w.started["h1@A"] >= 1 }
				case 1:
					return func() bool { return w.batchesStarted["releaserC"] >= 1 }
				}
				return nil
			},
		}, &pb)
		w.releaser("releaserC", &pc, 1, true, stableCond(func() bool { return w.livePasses["h1@B"] >= 1 }))
		w.s.Go("teardown", apimodel.QuietStart, func(a *apimodel.Actor) {
			a.WaitQuiet("live passes done", time.Time{}, func() bool {
				return w.livePasses["h1@A"] >= 2 && w.livePasses["h1@B"] >= 2
			})
			w.call(a, "delete secret", func() error { return w.m.Delete(keyOf(w.p)) })
			w.call(a, "delete h1", func() error { return w.m.Delete(widgetKey(w.ns, "h1")) })
		})
	}}

// TestModelStatefulSize measures the stateful search and the reduced one
// with every prune audited, as the differential test runs them, for the
// scenarios whose stateless reference is out of reach (§11.3): whether
// stateful against reduced fits the 24h rule. Behind
// CREDENTIALUSE_MODEL_STATEFUL_SIZE (a Go duration: each search's time cap);
// CREDENTIALUSE_MODEL_SIZE_ONLY picks scenarios, _SIZE_JUMPS sets T. A
// search that finishes reports its exact counts; one that doesn't reports
// what it reached, with its progress every 10,000 runs.
func TestModelStatefulSize(t *testing.T) {
	limit, err := time.ParseDuration(os.Getenv("CREDENTIALUSE_MODEL_STATEFUL_SIZE"))
	if err != nil || limit <= 0 {
		t.Skip("a measurement, run on its own (CREDENTIALUSE_MODEL_STATEFUL_SIZE=<cap>)")
	}
	t.Parallel()
	only := regexp.MustCompile(os.Getenv("CREDENTIALUSE_MODEL_SIZE_ONLY"))
	jumps := envInt("CREDENTIALUSE_MODEL_SIZE_JUMPS", testJumps)
	for name, sc := range smallScenarios {
		if !only.MatchString(name) {
			continue
		}
		for _, b := range []apimodel.Bounds{{Faults: 1, Jumps: jumps}, {Preemptions: 1, Jumps: jumps}} {
			for _, reduced := range []bool{false, true} {
				leg := "stateful"
				if reduced {
					leg = "reduced"
				}
				t.Run(fmt.Sprintf("%s, %s, %+v", name, leg, b), func(t *testing.T) {
					t.Parallel()
					statefulSize(t, fmt.Sprintf("%s %s {%d,%d} T=%d", name, leg, b.Preemptions, b.Faults, b.Jumps), sc, b, quietMode{off: !reduced}, limit)
				})
			}
		}
	}
}

func statefulSize(t *testing.T, label string, sc func(w *world), b apimodel.Bounds, mode quietMode, limit time.Duration) {
	start := time.Now()
	b.Deadline = start.Add(limit)
	runs, audits, divergences := 0, 0, 0
	inner := runner(sc, nil, mode)
	run := func(s *apimodel.Sched) error {
		err := inner(s)
		runs++
		if runs%10_000 == 0 {
			t.Logf("size: %s stateful runs=%d audits=%d divergences=%d elapsed=%s", label, runs, audits, divergences, time.Since(start).Round(time.Second))
		}
		var v violation
		if errors.As(err, &v) {
			return nil
		}
		return err
	}
	audit := func(pruned, witness apimodel.Witness) {
		audits++
		if msg := auditPrune(b, sc, pruned, witness, 1, mode); msg != "" {
			divergences++
			if divergences <= 3 {
				t.Errorf("%s: merge audit: %s", label, msg)
			}
		}
	}
	res := apimodel.ExploreAudited(b, audit, run)
	for _, f := range res.Failures {
		t.Errorf("%s: the harness failed: %v\nschedule: %s", label, f.Err, f.Trace)
	}
	state := "capped"
	if res.Finished {
		state = "finished"
	}
	t.Logf("size: %s stateful %s runs=%d states=%d pruned=%d audits=%d divergences=%d elapsed=%s",
		label, state, res.Runs, res.States, res.Pruned, audits, divergences, time.Since(start).Round(time.Second))
}

// TestModelSplitThroughput measures the split stateless search's throughput
// for a fixed time, for choosing how a long run is laid out: one process for
// every subtest, or a process each (the allocation's GC is per process).
// Behind CREDENTIALUSE_MODEL_THROUGHPUT=<duration>;
// CREDENTIALUSE_MODEL_THROUGHPUT_ONLY is a regexp over "scenario, {P,F}" with
// T = 2, and CREDENTIALUSE_MODEL_DIFF_WORKERS the workers per search.
func TestModelSplitThroughput(t *testing.T) {
	limit, err := time.ParseDuration(os.Getenv("CREDENTIALUSE_MODEL_THROUGHPUT"))
	if err != nil || limit <= 0 {
		t.Skip("a measurement, run on its own (CREDENTIALUSE_MODEL_THROUGHPUT=<duration>)")
	}
	only := regexp.MustCompile(os.Getenv("CREDENTIALUSE_MODEL_THROUGHPUT_ONLY"))
	workers := envInt("CREDENTIALUSE_MODEL_DIFF_WORKERS", 1)
	for name, sc := range smallScenarios {
		for _, b := range []apimodel.Bounds{{Faults: 1, Jumps: testJumps}, {Preemptions: 1, Jumps: testJumps}} {
			label := fmt.Sprintf("%s, {%d,%d}", name, b.Preemptions, b.Faults)
			if !only.MatchString(label) {
				continue
			}
			t.Run(label, func(t *testing.T) {
				t.Parallel()
				start := time.Now()
				b.Deadline = start.Add(limit)
				r := searchOn(t, sc, b, false, true, workers)
				el := time.Since(start)
				t.Logf("throughput: %s: %d schedules in %s with %d workers, %.0f/s", label, r.res.Runs, el.Round(time.Second), workers, float64(r.res.Runs)/el.Seconds())
			})
		}
	}
}
