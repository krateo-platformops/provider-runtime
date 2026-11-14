package apimodel

import (
	"fmt"
	"maps"
	"sync"
	"testing"
	"time"
)

// The capacity condition (docs/credential-use-model-reduction.md §10.2)
// holds for the user's T = 2 jumps, and not for a third.
func TestCapacity(t *testing.T) {
	if err := CheckCapacity(2); err != nil {
		t.Fatal(err)
	}
	if err := CheckCapacity(3); err == nil {
		t.Error("CheckCapacity(3) = nil: room = min(e·D·B, U/2) should be below (e·D)^4 ns")
	}
}

// The closed form against the tick's own arithmetic, in the worst case it
// is derived for: each level makes K calls, each tick is min(B, gap/D) and
// at least 1ns, and a jump lands on a term whose next larger fine part is
// one of the level before's smallest ticks away. No tick may reach its gap,
// and the deepest level's smallest tick must be at least the closed form's
// bound.
func TestCapacityWorstCase(t *testing.T) {
	const jumps = 2
	gap := TimeUnit / 2
	var smallest time.Duration
	for level := 0; level <= jumps; level++ {
		smallest = BaseTick
		for range MaxCallsBetweenJumps {
			d := min(BaseTick, max(gap/TickDivisor, time.Nanosecond))
			if d >= gap {
				t.Fatalf("level %d: a tick of %s reaches its gap of %s", level, d, gap)
			}
			gap -= d
			smallest = min(smallest, d)
		}
		gap = smallest
	}
	if smallest < time.Nanosecond {
		t.Fatalf("the deepest level's smallest tick is %s", smallest)
	}
	t.Logf("U = %s, B = %s, D = %d, K = %d, T = %d: the deepest level's smallest tick is %s", TimeUnit, BaseTick, TickDivisor, MaxCallsBetweenJumps, jumps, smallest)
}

// ExploreParallel runs exactly the schedules Explore runs, each once
// (docs/credential-use-model-reduction.md §11.2): on a scenario with calls,
// waits, a deadline to jump to, faults and preemptions, the multiset of
// schedules, the run count and the usage are Explore's, for any number of
// workers.
func TestExploreParallelIsExplore(t *testing.T) {
	scenario := func(record func(string)) func(s *Sched) error {
		return func(s *Sched) error {
			flag := false
			for _, n := range []string{"a", "b", "c"} {
				s.Go(n, Faulty, func(a *Actor) {
					_ = a.Call(n+"1", func() error { return nil })
					if n == "b" {
						a.Wait("later", Start.Add(3*TimeUnit), nil)
					}
					_ = a.Call(n+"2", func() error { flag = !flag; return nil })
				})
			}
			err := s.Run()
			record(s.Trace() + fmt.Sprint(flag))
			return err
		}
	}
	for _, b := range []Bounds{{Faults: 1, Jumps: 2}, {Preemptions: 1, Jumps: 2}, {Preemptions: 1, Faults: 1, Jumps: 1}} {
		serial := map[string]int{}
		want := Explore(b, scenario(func(k string) { serial[k]++ }))
		if !want.Finished || want.Runs < 100 {
			t.Fatalf("%+v: the serial search ran %d schedules, finished=%v", b, want.Runs, want.Finished)
		}
		for k, n := range serial {
			if n != 1 {
				t.Fatalf("%+v: the serial search ran %q %d times", b, k, n)
			}
		}
		for _, workers := range []int{1, 2, 3, 8} {
			var mu sync.Mutex
			got := map[string]int{}
			res := ExploreParallel(b, workers, scenario(func(k string) { mu.Lock(); got[k]++; mu.Unlock() }))
			if !res.Finished || res.Runs != want.Runs || res.Usage != want.Usage || len(res.Failures) != len(want.Failures) {
				t.Errorf("%+v, %d workers: %d runs, finished=%v, usage %+v, %d failures; serial: %d runs, usage %+v, %d failures",
					b, workers, res.Runs, res.Finished, res.Usage, len(res.Failures), want.Runs, want.Usage, len(want.Failures))
			}
			if !maps.Equal(got, serial) {
				t.Errorf("%+v, %d workers: the schedules differ from the serial search's (%d against %d distinct)", b, workers, len(got), len(serial))
			}
		}
		t.Logf("%+v: %d schedules, the same with 1, 2, 3 and 8 workers", b, want.Runs)
	}
}
