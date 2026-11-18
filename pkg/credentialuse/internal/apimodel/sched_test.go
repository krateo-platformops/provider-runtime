package apimodel

import (
	"fmt"
	"maps"
	"strings"
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

// A Wait's condition is a guard, not a step (docs/credential-use-model-reduction.md
// §5, C-park). The scenario is run 3's dropped schedules in small: a Secret
// is deleted, then, after a jump to the end of its grace and a releaser's
// requeue, gone; an observer whose start is quiet waits for "deleting or
// gone", then records the deletion it saw, or, if the Secret is already gone,
// "gone".
//
//   - With a condition that captures the deletion as a side effect, and the
//     check off, the reduced search misses "gone": rule R runs the quiet start
//     at once, so the observer waits from the first step, and its condition
//     sees the deletion before the release. Unreduced, the observer can sit at
//     its start, where no condition runs, until the Secret is gone.
//   - With the check on, that condition panics, in both searches.
//   - With the capture moved after the Wait, both searches reach both states.
func TestConditionIsAGuard(t *testing.T) {
	const deleting, gone = 1, 2
	scenario := func(capture, check bool, record func(string)) func(s *Sched) error {
		return func(s *Sched) error {
			s.noCondCheck = !check
			secret := 0
			grace := Start.Add(1000 * TimeUnit)
			s.Go("deleter", QuietStart, func(a *Actor) {
				_ = a.Call("delete", func() error { secret = deleting; return nil })
			})
			s.Go("releaser", 0, func(a *Actor) {
				a.Wait("trigger", time.Time{}, func() bool { return secret >= deleting })
				_ = a.Call("get", func() error { return nil })
				a.Wait("requeue", grace, nil)
				_ = a.Call("release", func() error { secret = gone; return nil })
			})
			s.Go("observer", QuietStart, func(a *Actor) {
				saw := 0
				a.Locals = func() []any { return []any{saw} }
				a.Wait("deleting or gone", time.Time{}, func() bool {
					if capture && secret == deleting {
						saw = deleting // the defect: a condition that writes
					}
					return secret >= deleting
				})
				if !capture || saw == 0 {
					saw = secret
				}
			})
			err := s.Run()
			s.Settle()
			record(fmt.Sprint(secret, " ", s.Views()[2].Locals))
			return err
		}
	}
	terminal := func(capture, check, noQuiet bool) (states map[string]bool, panicked any) {
		states = map[string]bool{}
		defer func() { panicked = recover() }()
		Explore(Bounds{Jumps: 1}, func(s *Sched) error {
			s.NoQuiet = noQuiet
			return scenario(capture, check, func(k string) { states[k] = true })(s)
		})
		return states, nil
	}
	both := map[string]bool{"2 [1]": true, "2 [2]": true}

	// The defect, with the check off.
	unreduced, _ := terminal(true, false, true)
	reduced, _ := terminal(true, false, false)
	if !maps.Equal(unreduced, both) || !maps.Equal(reduced, map[string]bool{"2 [1]": true}) {
		t.Errorf("capturing condition, check off: unreduced %v, reduced %v; want %v and only the capture", unreduced, reduced, both)
	}
	// The check refuses it in both searches.
	for _, noQuiet := range []bool{true, false} {
		if _, p := terminal(true, true, noQuiet); p == nil || !strings.Contains(fmt.Sprint(p), "C-park") {
			t.Errorf("capturing condition, NoQuiet=%v: want the C-park panic, got %v", noQuiet, p)
		}
	}
	// A guard, and the read after the Wait: the searches agree.
	for _, noQuiet := range []bool{true, false} {
		if got, p := terminal(false, true, noQuiet); p != nil || !maps.Equal(got, both) {
			t.Errorf("pure condition, NoQuiet=%v: %v (panic %v), want %v", noQuiet, got, p, both)
		}
	}
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
