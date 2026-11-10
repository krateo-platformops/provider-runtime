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

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

// The stateful search's own tests (docs/credential-use-model-reduction.md
// §10.5): the differential test against the stateless search, and the merge
// audit, which replays every pruned path and the witness of the state it
// reached under the same continuations.

// smallScenarios are searched both ways.
var smallScenarios = map[string]func(w *world){
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
		w.releaser("releaserC", &pc, 1, true, func() bool { return w.livePasses["h1@B"] >= 1 })
		w.s.Go("teardown", 0, func(a *apimodel.Actor) {
			a.Wait("live passes done", time.Time{}, func() bool {
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
// state and the violations of each schedule.
func runner(sc func(w *world), terminal func(key string, violations []string)) func(s *apimodel.Sched) error {
	return func(s *apimodel.Sched) error {
		w := newWorld(s)
		sc(w)
		s.Key = w.stateKey
		if err := s.Run(); err != nil {
			if errors.Is(err, apimodel.ErrPruned) || errors.Is(err, apimodel.ErrCapped) {
				return errors.Join(w.errs...)
			}
			return err
		}
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

// differential compares the two searches on one scenario and bound, and
// audits every prune.
func differential(t *testing.T, sc func(w *world), b apimodel.Bounds) {
	t.Helper()
	statelessStates, statelessViolations := map[string]bool{}, map[string]bool{}
	res := apimodel.Explore(b, runner(sc, func(key string, vs []string) {
		statelessStates[key] = true
		for _, v := range vs {
			statelessViolations[v] = true
		}
	}))
	if !res.Finished {
		for _, f := range res.Failures {
			t.Errorf("stateless: %v\nschedule: %s", f.Err, f.Trace)
		}
		t.Fatalf("stateless search didn't finish (%d schedules)", res.Runs)
	}
	statefulStates, statefulViolations := map[string]bool{}, map[string]bool{}
	audits, divergences := 0, 0
	audit := func(pruned, witness apimodel.Witness) {
		audits++
		if msg := auditPrune(b, sc, pruned, witness); msg != "" {
			divergences++
			if os.Getenv("CREDENTIALUSE_MODEL_AUDITSTOP") != "" {
				t.Fatalf("merge audit (stopping at the first): %s\npruned: %v\nwitness: %v", msg, pruned.Choices, witness.Choices)
			}
			if divergences <= 5 {
				t.Errorf("merge audit: %s\npruned: %v\nwitness: %v", msg, pruned.Choices, witness.Choices)
			}
		}
	}
	sres := apimodel.ExploreAudited(b, audit, runner(sc, func(key string, vs []string) {
		statefulStates[key] = true
		for _, v := range vs {
			statefulViolations[v] = true
		}
	}))
	if !sres.Finished {
		t.Fatalf("stateful search didn't finish (%d runs)", sres.Runs)
	}
	t.Logf("stateless: %d schedules, %d terminal states; stateful: %d runs, %d states, %d pruned, %d terminal states; %d audits, %d divergences",
		res.Runs, len(statelessStates), sres.Runs, sres.States, sres.Pruned, len(statefulStates), audits, divergences)
	for k := range statelessStates {
		if !statefulStates[k] {
			t.Errorf("a terminal state of the stateless search is missing from the stateful one (%d vs %d)", len(statelessStates), len(statefulStates))
			break
		}
	}
	for k := range statefulStates {
		if !statelessStates[k] {
			t.Errorf("the stateful search reached a terminal state the stateless one didn't")
			break
		}
	}
	if !mapsEqual(statelessViolations, statefulViolations) {
		t.Errorf("violations differ: stateless %v, stateful %v", sortedSet(statelessViolations), sortedSet(statefulViolations))
	}
}

// auditPrune replays the pruned path and the witness under the same
// continuations: the first option every time, and two seeded random ones.
// Past its prefix, each keeps the pruned path's budget, so both face the
// same options. It returns "" when they agree.
func auditPrune(b apimodel.Bounds, sc func(w *world), pruned, witness apimodel.Witness) string {
	// First, both prefixes must replay to the state they were merged at.
	for _, w := range []struct {
		name string
		at   apimodel.Witness
	}{{"pruned", pruned}, {"witness", witness}} {
		k, _ := apimodel.Replay(b, w.at.Choices, w.at.Left, -1, 1, runner(sc, nil))
		k2, _ := apimodel.Replay(b, w.at.Choices, w.at.Left, -1, 1, runner(sc, nil))
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
	for _, seed := range []int64{-1, 1, 2} {
		var pv, wv []string
		pk, perr := apimodel.Replay(b, pruned.Choices, pruned.Left, seed, 10_000, runner(sc, func(_ string, vs []string) { pv = vs }))
		wk, werr := apimodel.Replay(b, witness.Choices, pruned.Left, seed, 10_000, runner(sc, func(_ string, vs []string) { wv = vs }))
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
