package credentialuse

import (
	"errors"
	"os"
	"testing"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

// How the models of the index and of Disown search (§14).

// modelRuns caps one search. One that reaches it isn't exhaustive, and says
// so.
const modelRuns = 1_000_000

// modelBound is how many preemptions a search is for: deep under
// CREDENTIALUSE_MODEL_DEEP, and ci without it.
func modelBound(ci, deep int) int {
	if os.Getenv("CREDENTIALUSE_MODEL_DEEP") != "" {
		return deep
	}
	return ci
}

// exhaust searches every schedule of scenario with no preemption, then with
// one, and so on up to bound, and stops where a search doesn't finish. Each
// search has its own S, which check sees. It fails if it isn't exhaustive with
// one preemption, or with bound if that is less, and logs how far it is
// otherwise. It returns the last search's S.
func exhaust[S any](t *testing.T, bound int, scenario func(st *S) func(*apimodel.Sched) error, check func(st *S)) *S {
	t.Helper()
	var last *S
	covered := -1
	for p := range bound + 1 {
		last = new(S)
		res := apimodel.Explore(apimodel.Bounds{Preemptions: p, MaxRuns: modelRuns}, scenario(last))
		for _, f := range res.Failures {
			t.Errorf("%v\n  schedule: %s", f.Err, f.Trace)
		}
		check(last)
		t.Logf("%d preemptions: %d schedules, finished %v: %+v", p, res.Runs, res.Finished, *last)
		if !res.Finished {
			break
		}
		covered = p
	}
	switch {
	case covered < min(bound, 1):
		t.Errorf("exhaustive with %d preemptions only", covered)
	case covered < bound:
		t.Logf("NOT EXHAUSTIVE with %d preemptions: every schedule with %d, and the first %d with %d", bound, covered, modelRuns, covered+1)
	}
	return last
}

// errReached is what a scenario returns for a schedule that reaches what is
// looked for.
var errReached = errors.New("reached")

// reachable reports whether some schedule of scenario within bound reaches
// errReached, and stops at the first: it searches every schedule with no
// preemption and with one, then random ones with bound, then every one. It
// says nothing of how many there are.
func reachable(t *testing.T, bound int, scenario func(*apimodel.Sched) error) bool {
	t.Helper()
	found, runs := false, 0
	one := func(s *apimodel.Sched) error {
		if found {
			return nil // no choice made: the search ends here
		}
		runs++
		err := scenario(s)
		if errors.Is(err, errReached) {
			found = true
			t.Logf("reached in schedule %d: %s", runs, s.Trace())
			return nil
		}
		return err
	}
	stop := func(res apimodel.Result) bool {
		for _, f := range res.Failures {
			t.Errorf("%v\n  schedule: %s", f.Err, f.Trace)
		}
		return found || len(res.Failures) > 0
	}
	for p := range min(bound, 2) {
		if stop(apimodel.Explore(apimodel.Bounds{Preemptions: p, MaxRuns: modelRuns}, one)) {
			return found
		}
	}
	if stop(apimodel.Walk(apimodel.Bounds{Preemptions: bound}, 100_000, 1, one)) {
		return found
	}
	res := apimodel.Explore(apimodel.Bounds{Preemptions: bound, MaxRuns: modelRuns}, one)
	stop(res)
	t.Logf("%d schedules searched, found %v, exhaustive %v", runs, found, res.Finished)
	return found
}
