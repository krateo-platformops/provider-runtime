package credentialuse

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
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

// runner runs a scenario with the state hash set, recording each terminal
// state and the violations of each schedule. noQuiet turns the quiet-segment
// reduction off (§4).
func runner(sc func(w *world), terminal func(key string, violations []string), mode quietMode) func(s *apimodel.Sched) error {
	return func(s *apimodel.Sched) error {
		w := newWorld(s)
		s.NoQuiet, s.QuietInvisible = mode.off, mode.invisible
		sc(w)
		s.Key = w.stateKey
		if err := s.Run(); err != nil {
			if errors.Is(err, apimodel.ErrPruned) || errors.Is(err, apimodel.ErrCapped) {
				return errors.Join(w.errs...)
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
			terminal(key, normalized(err))
		}
		return err
	}
}

func TestModelStatefulAgreesWithStateless(t *testing.T) {
	if raceEnabled || os.Getenv("CREDENTIALUSE_MODEL_BOUNDS") != "" {
		t.Skip("the differential test runs in the model step, on its own bounds")
	}
	for name, sc := range smallScenarios {
		for _, b := range []apimodel.Bounds{{Faults: 1, Jumps: testJumps}, {Preemptions: 1, Jumps: testJumps}} {
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
	// The differential test's unreduced searches count quiet resumes as
	// invisible, the counting the reduction is sound against (issue 1).
	mode := quietMode{off: noQuiet, invisible: true}
	t.Helper()
	r := searchResult{states: map[string]bool{}, violations: map[string]bool{}, traces: map[string]string{}}
	var cur *apimodel.Sched
	inner := runner(sc, func(key string, vs []string) {
		if !r.states[key] && cur != nil {
			r.traces[key] = strings.Join(cur.Steps(), "\x1f")
		}
		r.states[key] = true
		for _, v := range vs {
			r.violations[v] = true
		}
	}, mode)
	run := func(s *apimodel.Sched) error { cur = s; return inner(s) }
	if !stateful {
		r.res = apimodel.Explore(b, run)
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
	plain := search(t, sc, b, false, true)
	if !plain.res.Finished {
		for _, f := range plain.res.Failures {
			t.Errorf("stateless: %v\nschedule: %s", f.Err, f.Trace)
		}
		t.Fatalf("stateless search didn't finish (%d schedules)", plain.res.Runs)
	}
	full := search(t, sc, b, true, true)
	reduced := search(t, sc, b, true, false)
	for _, r := range []searchResult{full, reduced} {
		if !r.res.Finished {
			t.Fatalf("a stateful search didn't finish (%d runs)", r.res.Runs)
		}
	}
	t.Logf("stateless: %d schedules, %d terminal states; stateful: %d runs, %d states, %d pruned, %d terminal states, %d audits, %d divergences; reduced: %d runs, %d states, %d pruned, %d terminal states, %d audits, %d divergences",
		plain.res.Runs, len(plain.states),
		full.res.Runs, full.res.States, full.res.Pruned, len(full.states), full.audits, full.divergences,
		reduced.res.Runs, reduced.res.States, reduced.res.Pruned, len(reduced.states), reduced.audits, reduced.divergences)
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
	compare("stateless against stateful", plain, full)
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
		pk, perr := apimodel.Replay(b, pruned.Choices, pruned.Left, seed, 10_000, runner(sc, func(_ string, vs []string) { pv = vs }, mode))
		wk, werr := apimodel.Replay(b, witness.Choices, pruned.Left, seed, 10_000, runner(sc, func(_ string, vs []string) { wv = vs }, mode))
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

// quietMode is how a search treats quiet parks (§4): off runs no rule R;
// invisible keeps quiet resumes out of the preemption bound.
type quietMode struct{ off, invisible bool }

// defaultQuiet is the model tests' mode: the reduction is off unless
// CREDENTIALUSE_MODEL_QUIET is set (work in progress, not reviewed).
func defaultQuiet() quietMode {
	return quietMode{off: os.Getenv("CREDENTIALUSE_MODEL_QUIET") == ""}
}
