package credentialuse

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/apimodel"
)

// reducedDrops reads testdata/reduced-drops.txt: schedules of
// DeletionsAroundTheGrace's small variant, by bound, whose terminal abstract
// states the reduced stateful search (rule R on) missed in run 3's
// differential and in its reproduction at e5d5af9.
func reducedDrops(t *testing.T) map[apimodel.Bounds][][]string {
	t.Helper()
	f, err := os.Open("testdata/reduced-drops.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	bounds := map[string]apimodel.Bounds{
		"p0f1": {Faults: 1, Jumps: 2},
		"p1f0": {Preemptions: 1, Jumps: 2},
	}
	drops := map[apimodel.Bounds][][]string{}
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		b, ok := bounds[fields[0]]
		if !ok {
			t.Fatalf("testdata/reduced-drops.txt: unknown bound %q", fields[0])
		}
		drops[b] = append(drops[b], fields[1:])
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return drops
}

// TestModelReducedKeepsDroppedTerminals pins the defect run 3 found: on
// DeletionsAroundTheGrace's small variant at T=2, the reduced stateful search
// reaches fewer terminal abstract states than the unreduced one ({1,0}: 920
// against 935; {0,1}: 4212 against 4331), while the stateless and stateful
// unreduced searches agree. The quiet-segment reduction (rule R) is unsound
// there.
//
// The first half always runs and is fast: each recorded schedule, replayed
// with rule R off, must still be followed step by step, end without a
// violation, and give its terminal key. That keeps the evidence valid as the
// model changes. The second half runs the reduced search (minutes for each
// bound), so it runs only with CREDENTIALUSE_MODEL_REDUCED_DROPS set, and it
// FAILS until the reduction is fixed: every replayed key must be among the
// reduced search's terminal states.
func TestModelReducedKeepsDroppedTerminals(t *testing.T) {
	if raceEnabled {
		t.Skip("model searches don't run under the race detector")
	}
	sc := smallScenarios["deletions around the grace, small"]
	for b, schedules := range reducedDrops(t) {
		t.Run(fmt.Sprintf("%+v", b), func(t *testing.T) {
			t.Parallel()
			keys := make([]string, len(schedules))
			for i, labels := range schedules {
				var key string
				unmatched, err := apimodel.ReplayLabels(b, labels, runner(sc, func(_ *apimodel.Sched, k string, _ []string) {
					key = k
				}, quietMode{off: true}))
				if len(unmatched) > 0 {
					t.Fatalf("schedule %d: the model no longer follows steps %q", i, unmatched)
				}
				if err != nil {
					t.Fatalf("schedule %d ends in a violation or a harness error: %v", i, err)
				}
				if key == "" {
					t.Fatalf("schedule %d reached no terminal state", i)
				}
				keys[i] = key
			}
			if os.Getenv("CREDENTIALUSE_MODEL_REDUCED_DROPS") == "" {
				t.Logf("replayed %d schedules; the reduced search runs with CREDENTIALUSE_MODEL_REDUCED_DROPS set", len(keys))
				return
			}
			reduced := search(t, sc, b, true, false)
			if !reduced.res.Finished {
				t.Fatalf("the reduced search didn't finish (%d runs)", reduced.res.Runs)
			}
			for i, key := range keys {
				if !reduced.states[key] {
					t.Errorf("schedule %d: its terminal state is missing from the reduced search's %d\nschedule: %s", i, len(reduced.states), strings.Join(schedules[i], " → "))
				}
			}
		})
	}
}
