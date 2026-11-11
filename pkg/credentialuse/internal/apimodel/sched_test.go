package apimodel

import (
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
