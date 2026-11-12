package credentialuse

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

// The stateful search's own tests (docs/credential-use-model-reduction.md
// §10.5): the differential test against the stateless search, and the merge
// audit, which replays every pruned path and the witness of the state it
// reached under the same continuations.

// smallScenarios are searched both ways: every Teardown order, and variants
// of the other scenarios small enough for a stateless search to finish.
var smallScenarios = map[string]func(w *world){
	"teardown, krateo-057": func(w *world) {
		seed(w.m, w.ns, w.p.Name, []string{"h1"}, true)
		pa := newProcess("A", w.m, w.s.Now())
		w.informer("informerA", &pa)
		w.holder(holderSpec{name: "h1", livePasses: 1, deletionPasses: 3}, &pa)
		w.releaser("releaserA", &pa, 3, false, nil)
		w.deleter("teardown", nsKey(w.ns), keyOf(w.p), widgetKey(w.ns, "h1"))
	},
	"teardown, interleaving B": func(w *world) {
		seed(w.m, w.ns, w.p.Name, []string{"h1"}, true)
		pa := newProcess("A", w.m, w.s.Now())
		w.informer("informerA", &pa)
		w.holder(holderSpec{name: "h1", livePasses: 1, deletionPasses: 3}, &pa)
		w.releaser("releaserA", &pa, 3, false, nil)
		w.deleter("teardown", keyOf(w.p), nsKey(w.ns), widgetKey(w.ns, "h1"))
	},
	"away and back, small, with a relist": func(w *world) {
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
			livePasses: 3, deletionPasses: 2,
		}, &pa)
		w.releaser("releaserB", &pb, 1, true, nil)
		w.deleter("teardown", keyOf(w.p), widgetKey(w.ns, "h1"))
	},
	"deletions around the grace, small": func(w *world) {
		seed(w.m, w.ns, w.p.Name, []string{"h1"}, true)
		pa := newProcess("A", w.m, w.s.Now())
		w.informer("informerA", &pa)
		w.holder(holderSpec{name: "h1", livePasses: 1, deletionPasses: 2}, &pa)
		w.releaser("releaserA", &pa, 2, false, nil)
		w.deleter("secret", keyOf(w.p))
		w.s.Go("late deletions", apimodel.QuietStart, func(a *apimodel.Actor) {
			var dt time.Time
			a.Locals = func() []any { return []any{dt} }
			a.Wait("secret deleting", time.Time{}, func() bool {
				o, err := w.m.Get(keyOf(w.p))
				if err == nil && o.DeletionTimestamp != nil {
					dt = o.DeletionTimestamp.Time
				}
				return err != nil || o.DeletionTimestamp != nil
			})
			if dt.IsZero() {
				dt = a.Now()
			}
			a.WaitQuiet("into the grace", dt.Add(testGrace/2), nil)
			w.call(a, "delete ns", func() error {
				err := w.m.Delete(nsKey(w.ns))
				if err == nil {
					w.nsDeleted = a.Now()
				}
				return err
			})
			w.call(a, "delete h1", func() error { return w.m.Delete(widgetKey(w.ns, "h1")) })
		})
	},
	"recreate behind a cached UID, small": func(w *world) {
		seed(w.m, w.ns, w.p.Name, []string{"h1"}, true)
		pa := newProcess("A", w.m, w.s.Now())
		pb := newProcess("B", w.m, w.s.Now())
		w.informer("informerA", &pa)
		var first types.UID
		if o, err := w.m.Get(keyOf(w.p)); err == nil {
			first = o.UID
			w.pinned = append(w.pinned, first)
		}
		w.holder(holderSpec{name: "h1", livePasses: 3, deletionPasses: 2, before: func(n int) func() bool {
			if n != 2 {
				return nil
			}
			return func() bool { o, err := w.m.Get(keyOf(w.p)); return err == nil && o.UID != first }
		}}, &pa)
		w.releaser("releaserB", &pb, 2, true, func() bool {
			o, err := w.m.Get(keyOf(w.p))
			return err != nil || o.DeletionTimestamp != nil
		})
		w.s.Go("rotation", apimodel.QuietStart, func(a *apimodel.Actor) {
			w.call(a, "delete secret", func() error { return w.m.Delete(keyOf(w.p)) })
			a.WaitQuiet("gone", time.Time{}, func() bool { _, err := w.m.Get(keyOf(w.p)); return err != nil })
			w.call(a, "recreate secret", func() error { return w.m.Create(keyOf(w.p), metav1.ObjectMeta{}) })
			a.WaitQuiet("last live pass", time.Time{}, func() bool { return w.livePasses["h1"] >= 3 })
			w.call(a, "delete h1", func() error { return w.m.Delete(widgetKey(w.ns, "h1")) })
		})
	},
	"two releasers": func(w *world) {
		seed(w.m, w.ns, w.p.Name, []string{"h1"}, true)
		pa := newProcess("A", w.m, w.s.Now())
		pb := newProcess("B", w.m, w.s.Now())
		pc := newProcess("C", w.m, w.s.Now())
		w.informer("informerA", &pa)
		w.holder(holderSpec{name: "h1", livePasses: 1, deletionPasses: 2}, &pa)
		w.releaser("releaserB", &pb, 1, true, nil)
		w.releaser("releaserC", &pc, 1, true, nil)
		w.deleter("teardown", keyOf(w.p), widgetKey(w.ns, "h1"))
	},
	"teardown, interleaving A": func(w *world) {
		seed(w.m, w.ns, w.p.Name, []string{"h1"}, true)
		pa := newProcess("A", w.m, w.s.Now())
		w.informer("informerA", &pa)
		w.holder(holderSpec{name: "h1", livePasses: 1, deletionPasses: 3}, &pa)
		w.releaser("releaserA", &pa, 3, false, nil)
		w.deleter("teardown", keyOf(w.p), widgetKey(w.ns, "h1"))
	},
	"acquire against release": func(w *world) {
		seed(w.m, w.ns, w.p.Name, nil, true)
		must(w.m.Create(widgetKey(w.ns, "h2"), metav1.ObjectMeta{}))
		pa := newProcess("A", w.m, w.s.Now())
		pb := newProcess("B", w.m, w.s.Now())
		w.informer("informerA", &pa)
		w.holder(holderSpec{name: "h2", livePasses: 2, deletionPasses: 3}, &pa)
		w.releaser("releaserB", &pb, 2, true, nil)
		w.deleter("teardown", keyOf(w.p), widgetKey(w.ns, "h2"))
	},
	"two processes, shrunk": func(w *world) {
		seed(w.m, w.ns, w.p.Name, []string{"h1"}, true)
		pa := newProcess("A", w.m, w.s.Now())
		pb := newProcess("B", w.m, w.s.Now())
		pc := newProcess("C", w.m, w.s.Now())
		w.informer("informerA", &pa)
		secret := SecretRef(w.ns, w.p.Name)
		w.holder(holderSpec{name: "h1", actor: "h1@A", livePasses: 2, deletionPasses: 2, before: func(n int) func() bool {
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
			livePasses: 2, deletionPasses: 2,
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
	},
}

var digits = regexp.MustCompile(`[0-9]+`)

// normalized strips the values a violation message quotes (times, names),
// which differ between merged states.
func normalized(err error) []string {
	if err == nil {
		return nil
	}
	var out []string
	for _, l := range strings.Split(err.Error(), "\n") {
		out = append(out, digits.ReplaceAllString(l, "#"))
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// violation is a run's error that the scenario's checks found, as against
// one of the harness's own (a deadlock, say).
type violation struct{ err error }

func (v violation) Error() string { return v.err.Error() }
func (v violation) Unwrap() error { return v.err }

func violations(err error) error {
	if err == nil {
		return nil
	}
	return violation{err}
}

// runner runs a scenario with the state hash set, recording each terminal
// state and the violations of each schedule. mode.off turns the quiet-segment
// reduction off (§4). What the checks find comes back as a violation.
func runner(sc func(w *world), terminal func(s *apimodel.Sched, key string, violations []string), mode quietMode) func(s *apimodel.Sched) error {
	return func(s *apimodel.Sched) error {
		w := newWorld(s)
		s.NoQuiet = mode.off
		sc(w)
		s.Key = w.stateKey
		if err := s.Run(); err != nil {
			switch {
			case errors.Is(err, apimodel.ErrPruned):
				return violations(errors.Join(w.errs...))
			case errors.Is(err, apimodel.ErrCapped):
				// The audit must see the cap, and stop there.
				return errors.Join(err, violations(errors.Join(w.errs...)))
			}
			return err
		}
		// Quiet segments are invisible: an unreduced run that ends with one
		// ready but not taken ends, for comparison, where the reduced run
		// would, after it.
		s.Settle()
		key := w.stateKey()
		err := w.check()
		if terminal != nil {
			terminal(s, key, normalized(err))
		}
		return violations(err)
	}
}

func TestModelStatefulAgreesWithStateless(t *testing.T) {
	if raceEnabled || os.Getenv("CREDENTIALUSE_MODEL_BOUNDS") != "" {
		t.Skip("the differential test runs in the model step, on its own bounds")
	}
	t.Parallel()
	// For the measured runs of §11.3: CREDENTIALUSE_MODEL_DIFF_ONLY (a regexp)
	// picks scenarios, and CREDENTIALUSE_MODEL_DIFF_JUMPS sets T; or
	// CREDENTIALUSE_MODEL_DIFF_PLAN, "regexp=T;regexp=T", gives each matching
	// scenario its own T (the first match wins).
	type entry struct {
		re    *regexp.Regexp
		jumps int
	}
	plan := []entry{{regexp.MustCompile(os.Getenv("CREDENTIALUSE_MODEL_DIFF_ONLY")), envInt("CREDENTIALUSE_MODEL_DIFF_JUMPS", testJumps)}}
	if v := os.Getenv("CREDENTIALUSE_MODEL_DIFF_PLAN"); v != "" {
		plan = nil
		for _, part := range strings.Split(v, ";") {
			re, n, ok := strings.Cut(part, "=")
			jumps, err := strconv.Atoi(n)
			if !ok || err != nil {
				t.Fatalf("CREDENTIALUSE_MODEL_DIFF_PLAN=%q: %q isn't regexp=T", v, part)
			}
			plan = append(plan, entry{regexp.MustCompile(re), jumps})
		}
	}
	for name, sc := range smallScenarios {
		jumps := -1
		for _, e := range plan {
			if e.re.MatchString(name) {
				jumps = e.jumps
				break
			}
		}
		if jumps < 0 {
			continue
		}
		for _, b := range []apimodel.Bounds{{Faults: 1, Jumps: jumps}, {Preemptions: 1, Jumps: jumps}} {
			t.Run(fmt.Sprintf("%s, %+v", name, b), func(t *testing.T) {
				t.Parallel()
				differential(t, sc, b)
			})
		}
	}
}

// searchResult is one search's terminal abstract states and violations.
type searchResult struct {
	res                 apimodel.Result
	states, violations  map[string]bool
	traces              map[string]string // the first schedule to each terminal state
	audits, divergences int
}

// search runs one search of the differential test: stateless or stateful,
// with the quiet-segment reduction or without it. A stateful search audits
// every prune.
func search(t *testing.T, sc func(w *world), b apimodel.Bounds, stateful, noQuiet bool) searchResult {
	return searchOn(t, sc, b, stateful, noQuiet, 1)
}

// searchOn is search, with a stateless search split across workers
// (apimodel.ExploreParallel, §11.2) when workers is above 1.
func searchOn(t *testing.T, sc func(w *world), b apimodel.Bounds, stateful, noQuiet bool, workers int) searchResult {
	mode := quietMode{off: noQuiet}
	t.Helper()
	r := searchResult{states: map[string]bool{}, violations: map[string]bool{}, traces: map[string]string{}}
	// A parallel search runs schedules on several goroutines at once.
	var mu sync.Mutex
	inner := runner(sc, func(s *apimodel.Sched, key string, vs []string) {
		mu.Lock()
		defer mu.Unlock()
		if !r.states[key] {
			r.traces[key] = strings.Join(s.Steps(), "\x1f")
		}
		r.states[key] = true
		for _, v := range vs {
			r.violations[v] = true
		}
	}, mode)
	// Every violation is recorded, on a pruned run too, and the search goes
	// on: under a mutant, the searches are compared on every violation they
	// find, not on the first ten. Only the harness's own errors are failures.
	// The stateless search logs its progress every million schedules
	// (CREDENTIALUSE_MODEL_PROGRESS_EVERY), so a
	// long run's throughput and ETA can be read while it runs. The ETA is
	// against an expected total, CREDENTIALUSE_MODEL_DIFF_EXPECT
	// ("regexp=N;regexp=N", matched against the subtest's name; the first
	// match wins), such as the measured Knuth estimate.
	var done atomic.Int64
	start := time.Now()
	expect := expectedTotal(t)
	every := int64(envInt("CREDENTIALUSE_MODEL_PROGRESS_EVERY", 1_000_000))
	run := func(s *apimodel.Sched) error {
		err := inner(s)
		if !stateful {
			if n := done.Add(1); n%every == 0 {
				el := time.Since(start)
				rate := float64(n) / el.Seconds()
				eta := "no expected total"
				if expect > 0 {
					eta = fmt.Sprintf("ETA %s against an expected %.3g", time.Duration(math.Max(float64(expect-n), 0)/rate*float64(time.Second)).Round(time.Minute), float64(expect))
				}
				t.Logf("stateless progress: %d schedules in %s, %.0f/s, %s", n, el.Round(time.Second), rate, eta)
			}
		}
		var v violation
		if errors.As(err, &v) {
			mu.Lock()
			for _, m := range normalized(v.err) {
				r.violations[m] = true
			}
			mu.Unlock()
			return nil
		}
		return err
	}
	if !stateful {
		if workers > 1 {
			r.res = apimodel.ExploreParallel(b, workers, run)
		} else {
			r.res = apimodel.Explore(b, run)
		}
		return r
	}
	audit := func(pruned, witness apimodel.Witness) {
		r.audits++
		if msg := auditPrune(b, sc, pruned, witness, 1, mode); msg != "" {
			r.divergences++
			if os.Getenv("CREDENTIALUSE_MODEL_AUDITSTOP") != "" {
				t.Fatalf("merge audit (stopping at the first, noQuiet=%v): %s\npruned: %v\nwitness: %v", noQuiet, msg, pruned.Choices, witness.Choices)
			}
			if r.divergences <= 5 {
				t.Errorf("merge audit (noQuiet=%v): %s\npruned: %v\nwitness: %v", noQuiet, msg, pruned.Choices, witness.Choices)
			}
		}
	}
	r.res = apimodel.ExploreAudited(b, audit, run)
	return r
}

// differential compares three searches of one scenario and bound: stateless
// without the reduction, stateful without it (the stateful search is sound),
// and stateful with it (the reduction is sound). Terminal abstract states and
// violations must be equal; every prune is audited.
func differential(t *testing.T, sc func(w *world), b apimodel.Bounds) {
	t.Helper()
	// CREDENTIALUSE_MODEL_DIFF_WORKERS splits the stateless leg across that
	// many workers (§11.2); by default it is the serial search. With
	// CREDENTIALUSE_MODEL_DIFF_STATELESS=off there is no stateless leg, and
	// only the stateful search and the reduced one are compared: §11.3's
	// reduced coverage, where the stateless reference is infeasible.
	withStateless := os.Getenv("CREDENTIALUSE_MODEL_DIFF_STATELESS") != "off"
	// The three searches are independent, so they run side by side; the
	// stateful ones are serial, and would otherwise follow a long stateless
	// search.
	var plain, full, reduced searchResult
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); full = search(t, sc, b, true, true) }()
	go func() { defer wg.Done(); reduced = search(t, sc, b, true, false) }()
	if withStateless {
		plain = searchOn(t, sc, b, false, true, envInt("CREDENTIALUSE_MODEL_DIFF_WORKERS", 1))
	} else {
		plain.res.Finished = true
		t.Logf("no stateless leg (CREDENTIALUSE_MODEL_DIFF_STATELESS=off): stateful against reduced only")
	}
	wg.Wait()
	if withStateless && plain.res.Finished {
		saveTerminal(t, plain)
	}
	if !plain.res.Finished {
		for _, f := range plain.res.Failures {
			t.Errorf("stateless: %v\nschedule: %s", f.Err, f.Trace)
		}
		t.Fatalf("stateless search didn't finish (%d schedules)", plain.res.Runs)
	}
	for _, r := range []searchResult{full, reduced} {
		if !r.res.Finished {
			t.Fatalf("a stateful search didn't finish (%d runs)", r.res.Runs)
		}
	}
	for what, r := range map[string]searchResult{"stateless": plain, "stateful": full, "reduced": reduced} {
		for _, f := range r.res.Failures {
			t.Errorf("%s: the harness failed: %v\nschedule: %s", what, f.Err, f.Trace)
		}
	}
	t.Logf("stateless: %d schedules, %d terminal states, %d violations; stateful: %d runs, %d states, %d pruned, %d terminal states, %d violations, %d audits, %d divergences; reduced: %d runs, %d states, %d pruned, %d terminal states, %d violations, %d audits, %d divergences",
		plain.res.Runs, len(plain.states), len(plain.violations),
		full.res.Runs, full.res.States, full.res.Pruned, len(full.states), len(full.violations), full.audits, full.divergences,
		reduced.res.Runs, reduced.res.States, reduced.res.Pruned, len(reduced.states), len(reduced.violations), reduced.audits, reduced.divergences)
	for _, v := range sortedSet(full.violations) {
		t.Logf("violation: %s", v)
	}
	u := plain.res.Usage
	u.Merge(full.res.Usage)
	u.Merge(reduced.res.Usage)
	t.Logf("capacity used (§10.2): at most %d calls between jumps (K = %d), %d jumps chosen or forced, smallest tick %s",
		u.MaxCallsBetweenJumps, apimodel.MaxCallsBetweenJumps, u.MaxJumps, u.MinTick)
	compare := func(what string, a, b searchResult) {
		if !mapsEqual(a.states, b.states) {
			t.Errorf("%s: terminal abstract states differ (%d against %d)", what, len(a.states), len(b.states))
			shown := 0
			for k := range a.states {
				if !b.states[k] && shown < 2 {
					shown++
					t.Logf("only in the first: %s", a.traces[k])
					if rawKeys {
						best, bestN := "", 1<<30
						for k2 := range b.states {
							at, bt := strings.Fields(k), strings.Fields(k2)
							n := 0
							for i := range min(len(at), len(bt)) {
								if at[i] != bt[i] {
									n++
								}
							}
							if n < bestN {
								best, bestN = k2, n
							}
						}
						t.Logf("closest in the second differs in %d tokens:%s", bestN, firstDiff([]string{k}, []string{best}))
					}
				}
			}
			for k := range b.states {
				if !a.states[k] && shown < 4 {
					shown++
					t.Logf("only in the second: %s", b.traces[k])
				}
			}
		}
		if !mapsEqual(a.violations, b.violations) {
			t.Errorf("%s: violations differ: %v against %v", what, sortedSet(a.violations), sortedSet(b.violations))
		}
	}
	if withStateless {
		compare("stateless against stateful", plain, full)
	}
	compare("unreduced against reduced", full, reduced)
}

// auditPrune replays the pruned path and the witness under the same
// continuations: the first option every time, the last, and a seeded random
// one.
// Past its prefix, each keeps the pruned path's budget, so both face the
// same options. It returns "" when they agree.
func auditPrune(b apimodel.Bounds, sc func(w *world), pruned, witness apimodel.Witness, seed int64, mode quietMode) string {
	// First, both prefixes must replay to the state they were merged at.
	for _, w := range []struct {
		name string
		at   apimodel.Witness
	}{{"pruned", pruned}, {"witness", witness}} {
		k, _ := apimodel.Replay(b, w.at.Choices, w.at.Left, -1, 1, runner(sc, nil, mode))
		k2, _ := apimodel.Replay(b, w.at.Choices, w.at.Left, -1, 1, runner(sc, nil, mode))
		if !slices.Equal(k, k2) {
			return fmt.Sprintf("the %s path replays two different ways%s", w.name, firstDiff(k, k2))
		}
		if len(k) == 0 || k[0] != w.at.Key {
			got := ""
			if len(k) > 0 {
				got = k[0]
			}
			return fmt.Sprintf("the %s path doesn't replay to its state%s", w.name, firstDiff([]string{got}, []string{w.at.Key}))
		}
	}
	// The first option every time, the last (jumps and faults), and one
	// seeded at random.
	for _, seed := range []int64{-1, -2, seed & (1<<62 - 1)} {
		var pv, wv []string
		pk, perr := apimodel.Replay(b, pruned.Choices, pruned.Left, seed, 10_000, runner(sc, func(_ *apimodel.Sched, _ string, vs []string) { pv = vs }, mode))
		wk, werr := apimodel.Replay(b, witness.Choices, pruned.Left, seed, 10_000, runner(sc, func(_ *apimodel.Sched, _ string, vs []string) { wv = vs }, mode))
		if errors.Is(perr, apimodel.ErrCapped) || errors.Is(werr, apimodel.ErrCapped) {
			return fmt.Sprintf("continuation %d hit the step cap", seed)
		}
		if !slices.Equal(pk, wk) {
			return fmt.Sprintf("continuation %d: abstract traces differ (%d vs %d steps)%s", seed, len(pk), len(wk), firstDiff(pk, wk))
		}
		if !slices.Equal(pv, wv) || !slices.Equal(normalized(perr), normalized(werr)) {
			return fmt.Sprintf("continuation %d: violations differ: %v vs %v", seed, pv, wv)
		}
	}
	return ""
}

func mapsEqual(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func sortedSet(m map[string]bool) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// firstDiff shows where two abstract traces first differ, when keys are raw.
func firstDiff(a, b []string) string {
	if !rawKeys {
		return ""
	}
	for i := range min(len(a), len(b)) {
		if a[i] == b[i] {
			continue
		}
		at, bt := strings.Fields(a[i]), strings.Fields(b[i])
		for j := range min(len(at), len(bt)) {
			if at[j] != bt[j] {
				lo := max(0, j-12)
				return fmt.Sprintf("\nstep %d, token %d:\n  pruned:  %s\n  witness: %s", i, j,
					strings.Join(at[lo:min(len(at), j+6)], " "), strings.Join(bt[lo:min(len(bt), j+6)], " "))
			}
		}
		return fmt.Sprintf("\nstep %d: lengths %d vs %d", i, len(at), len(bt))
	}
	return ""
}

// quietMode is how a search treats quiet parks (§4): off runs no rule R, so
// that each quiet resume is a step of its own (never a preemption, never the
// last actor, §5).
type quietMode struct{ off bool }

// defaultQuiet is the model tests' mode: the reduction is off unless
// CREDENTIALUSE_MODEL_QUIET is set (work in progress, not reviewed).
func defaultQuiet() quietMode {
	return quietMode{off: os.Getenv("CREDENTIALUSE_MODEL_QUIET") == ""}
}

// TestModelParallelStatelessIsSerial proves the split stateless search
// (§11.2) against the serial one where the serial one finishes: on every
// Teardown order at {0,1} and {1,0} with T = 2, the same schedule count,
// the same terminal abstract states and the same violations. It runs only
// with CREDENTIALUSE_MODEL_PARALLEL_PROOF set (hours, serially); the workers
// per search are CREDENTIALUSE_MODEL_PARALLEL_PROOF (default 4 if not a
// number).
func TestModelParallelStatelessIsSerial(t *testing.T) {
	if os.Getenv("CREDENTIALUSE_MODEL_PARALLEL_PROOF") == "" {
		t.Skip("hours of serial search, run on its own (CREDENTIALUSE_MODEL_PARALLEL_PROOF)")
	}
	t.Parallel()
	workers := envInt("CREDENTIALUSE_MODEL_PARALLEL_PROOF", 4)
	for name, sc := range smallScenarios {
		if !strings.HasPrefix(name, "teardown") {
			continue
		}
		for _, b := range []apimodel.Bounds{{Faults: 1, Jumps: testJumps}, {Preemptions: 1, Jumps: testJumps}} {
			t.Run(fmt.Sprintf("%s, %+v", name, b), func(t *testing.T) {
				t.Parallel()
				// The two searches run side by side, each timed.
				var serial, split searchResult
				var ts, tp time.Duration
				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					start := time.Now()
					serial = searchOn(t, sc, b, false, true, 1)
					ts = time.Since(start)
				}()
				go func() {
					defer wg.Done()
					start := time.Now()
					split = searchOn(t, sc, b, false, true, workers)
					tp = time.Since(start)
				}()
				wg.Wait()
				t.Logf("serial: %d schedules, %d terminal states, %d violations, %s; split over %d workers: %d schedules, %d terminal states, %d violations, %s",
					serial.res.Runs, len(serial.states), len(serial.violations), ts.Round(time.Second),
					workers, split.res.Runs, len(split.states), len(split.violations), tp.Round(time.Second))
				if !serial.res.Finished || !split.res.Finished {
					t.Fatalf("a search didn't finish: serial %v, split %v", serial.res.Finished, split.res.Finished)
				}
				if serial.res.Runs != split.res.Runs || serial.res.Usage != split.res.Usage {
					t.Errorf("runs or usage differ: %d %+v against %d %+v", serial.res.Runs, serial.res.Usage, split.res.Runs, split.res.Usage)
				}
				if !mapsEqual(serial.states, split.states) {
					t.Errorf("terminal abstract states differ: %d against %d", len(serial.states), len(split.states))
				}
				if !mapsEqual(serial.violations, split.violations) {
					t.Errorf("violations differ: %v against %v", sortedSet(serial.violations), sortedSet(split.violations))
				}
				for _, f := range append(serial.res.Failures, split.res.Failures...) {
					t.Errorf("the harness failed: %v\nschedule: %s", f.Err, f.Trace)
				}
			})
		}
	}
}

// The split stateless search on a real scenario, briefly: the world, the
// model and the scheduler share nothing between schedules, so CI's race
// detector run finds no race in it (§11.2).
// It also runs the differential test's three searches side by side, as
// differential does.
func TestModelParallelSearchShares(t *testing.T) {
	sc := smallScenarios["teardown, interleaving A"]
	var full, reduced searchResult
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		full = search(t, sc, apimodel.Bounds{Faults: 1, Jumps: testJumps, MaxRuns: 60}, true, true)
	}()
	go func() {
		defer wg.Done()
		reduced = search(t, sc, apimodel.Bounds{Faults: 1, Jumps: testJumps, MaxRuns: 60}, true, false)
	}()
	r := searchOn(t, sc, apimodel.Bounds{Faults: 1, Jumps: testJumps, MaxRuns: 200}, false, true, 4)
	wg.Wait()
	if r.res.Runs < 200 || len(r.states) == 0 || full.res.Runs < 60 || reduced.res.Runs < 60 {
		t.Errorf("%d schedules, %d terminal states; stateful %d runs, reduced %d", r.res.Runs, len(r.states), full.res.Runs, reduced.res.Runs)
	}
	for _, f := range append(append(r.res.Failures, full.res.Failures...), reduced.res.Failures...) {
		t.Errorf("the harness failed: %v\nschedule: %s", f.Err, f.Trace)
	}
}

// saveTerminal writes the stateless search's terminal abstract states and
// violations to CREDENTIALUSE_MODEL_DIFF_SAVE/<subtest>.stateless.txt, if
// that is set: the unreduced reference a later reduction of the stateless
// search (sleep sets, §12) is compared with, without searching it again. The
// states are the hashed keys, hex-encoded and sorted; they are comparable
// only with keys from the same state encoding (state_test.go).
func saveTerminal(t *testing.T, r searchResult) {
	dir := os.Getenv("CREDENTIALUSE_MODEL_DIFF_SAVE")
	if dir == "" {
		return
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n# stateless: %d schedules, %d terminal states, %d violations\n", t.Name(), r.res.Runs, len(r.states), len(r.violations))
	for _, k := range sortedSet(r.states) {
		fmt.Fprintf(&b, "%x\n", k)
	}
	for _, v := range sortedSet(r.violations) {
		fmt.Fprintf(&b, "violation: %s\n", v)
	}
	name := regexp.MustCompile(`[^A-Za-z0-9_.-]+`).ReplaceAllString(t.Name(), "_")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("saving the terminal states: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, name+".stateless.txt"), []byte(b.String()), 0o644); err != nil {
		t.Fatalf("saving the terminal states: %v", err)
	}
}

func TestSaveTerminal(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CREDENTIALUSE_MODEL_DIFF_SAVE", dir)
	r := searchResult{states: map[string]bool{"\x01\x02": true, "\xff": true}, violations: map[string]bool{"SAFETY: x": true}}
	r.res.Runs = 7
	saveTerminal(t, r)
	got, err := os.ReadFile(filepath.Join(dir, "TestSaveTerminal.stateless.txt"))
	if err != nil {
		t.Fatal(err)
	}
	want := "# TestSaveTerminal\n# stateless: 7 schedules, 2 terminal states, 1 violations\n0102\nff\nviolation: SAFETY: x\n"
	if string(got) != want {
		t.Errorf("saved:\n%s\nwant:\n%s", got, want)
	}
}

// expectedTotal is the subtest's expected schedule count from
// CREDENTIALUSE_MODEL_DIFF_EXPECT, or 0.
func expectedTotal(t *testing.T) int64 {
	for _, part := range strings.Split(os.Getenv("CREDENTIALUSE_MODEL_DIFF_EXPECT"), ";") {
		re, n, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(n, 64)
		if err == nil && regexp.MustCompile(re).MatchString(t.Name()) {
			return int64(v)
		}
	}
	return 0
}

// BenchmarkStatelessSchedule is one schedule of the stateless search, as the
// differential's first leg runs it (rule R off, terminal states recorded), on
// DeletionsAroundTheGrace's small variant at {0,1}: its time and allocation,
// with -benchmem.
func BenchmarkStatelessSchedule(b *testing.B) {
	sc := smallScenarios["deletions around the grace, small"]
	inner := runner(sc, func(*apimodel.Sched, string, []string) {}, quietMode{off: true})
	b.ReportAllocs()
	b.ResetTimer()
	res := apimodel.Explore(apimodel.Bounds{Faults: 1, Jumps: testJumps, MaxRuns: b.N}, func(s *apimodel.Sched) error {
		err := inner(s)
		var v violation
		if errors.As(err, &v) {
			return nil
		}
		return err
	})
	if len(res.Failures) > 0 {
		b.Fatal(res.Failures[0].Err)
	}
}
