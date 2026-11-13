package ratelimit

import (
	"math"
	"testing"
	"time"
)

// The law, as a pure function of one interval's values
// (docs/design-capacity-model.md §3 and §9). Each rule removed in turn fails a
// test here: the test that pins a rule names it.

const lawCeiling = 32

// lawInterval is a 10s interval at a 500m limit with a 20m background, in
// which inFlight reconciles were in flight on average under a local capacity of
// inForce, the container used the given cores, and 200 reconciles completed.
func lawInterval(inFlight, inForce, used float64) modelInterval {
	const seconds = 10
	return modelInterval{
		elapsed:      seconds * time.Second,
		cpuUsed:      time.Duration(used * seconds * float64(time.Second)),
		slotTime:     time.Duration(inFlight * seconds * float64(time.Second)),
		capacityTime: time.Duration(inForce * seconds * float64(time.Second)),
		completions:  200,
		limit:        0.5,
		background:   0.02,
	}
}

func near(got, want float64) bool { return math.Abs(got-want) < 1e-6 }

// The design's worked example: 12 in flight, 380m used of 500m, 20m of
// background. The target is 450m, so wanted = 12 × 430 ÷ 360 = 14.3.
func TestLawWorkedExample(t *testing.T) {
	got := modelCapacity(12, lawInterval(12, 12, 0.38), DefaultCPUTarget, lawCeiling)
	if want := 12 * 0.43 / 0.36; !near(got, want) {
		t.Errorf("M = %v, want %v", got, want)
	}
}

// Rule 1, completions: 19 completions are no evidence, 20 are.
func TestLawTooFewCompletionsMoveNothing(t *testing.T) {
	iv := lawInterval(12, 12, 0.38)
	iv.completions = minCompletions - 1
	if got := modelCapacity(12, iv, DefaultCPUTarget, lawCeiling); got != 12 {
		t.Errorf("with %d completions M = %v, want 12 unchanged", iv.completions, got)
	}
	iv.completions = minCompletions
	if got := modelCapacity(12, iv, DefaultCPUTarget, lawCeiling); got == 12 {
		t.Errorf("with %d completions M stayed at 12, want it to move", iv.completions)
	}
}

// Rule 1, CPU: with U barely above B the ratio divides by almost nothing. Under
// 5% of the limit above the background, nothing moves.
func TestLawCPUBarelyAboveBackgroundMovesNothing(t *testing.T) {
	// 24m above the background, of a 500m limit: under the 25m that is 5%.
	if got := modelCapacity(12, lawInterval(12, 12, 0.044), DefaultCPUTarget, lawCeiling); got != 12 {
		t.Errorf("U 24m above B: M = %v, want 12 unchanged", got)
	}
	// 26m above it is evidence: wanted is 12 × 430 ÷ 26, capped at twice M.
	if got := modelCapacity(12, lawInterval(12, 12, 0.046), DefaultCPUTarget, lawCeiling); got != 24 {
		t.Errorf("U 26m above B: M = %v, want 24", got)
	}
	// U under B is no evidence either, and not a negative capacity.
	if got := modelCapacity(12, lawInterval(12, 12, 0.01), DefaultCPUTarget, lawCeiling); got != 12 {
		t.Errorf("U under B: M = %v, want 12 unchanged", got)
	}
}

// Rule 1, limit: the interval a limit changes in moves nothing.
func TestLawALimitChangeMovesNothing(t *testing.T) {
	iv := lawInterval(12, 12, 0.38)
	iv.limitChanged = true
	if got := modelCapacity(12, iv, DefaultCPUTarget, lawCeiling); got != 12 {
		t.Errorf("the limit changed in the interval: M = %v, want 12 unchanged", got)
	}
}

// Rule 2: with slots standing idle, more of them would have changed nothing. 6
// in flight under a capacity of 12, at 120m: wanted is 25.8, and M stays.
func TestLawARiseNeedsTheSlotsUsed(t *testing.T) {
	if got := modelCapacity(12, lawInterval(6, 12, 0.12), DefaultCPUTarget, lawCeiling); got != 12 {
		t.Errorf("half the slots idle: M = %v, want 12 unchanged", got)
	}
	// The line is 0.9 of the capacity in force: 10.8 of 12.
	if got := modelCapacity(12, lawInterval(10.7, 12, 0.12), DefaultCPUTarget, lawCeiling); got != 12 {
		t.Errorf("10.7 of 12 slots used: M = %v, want 12 unchanged", got)
	}
	if got := modelCapacity(12, lawInterval(10.9, 12, 0.12), DefaultCPUTarget, lawCeiling); got != 24 {
		t.Errorf("10.9 of 12 slots used: M = %v, want 24", got)
	}
	// The capacity in force is what the slots are measured against, not M:
	// under a step that holds local at 6, 6 in flight is every slot used.
	if got := modelCapacity(12, lawInterval(6, 6, 0.12), DefaultCPUTarget, lawCeiling); got != 24 {
		t.Errorf("every slot of a local capacity of 6 used: M = %v, want 24", got)
	}
}

// Rule 3: wanted within 10% of M moves nothing. From 13.5 the worked example's
// 14.3 is 6% away.
func TestLawToleranceMovesNothing(t *testing.T) {
	if got := modelCapacity(13.5, lawInterval(12, 12, 0.38), DefaultCPUTarget, lawCeiling); got != 13.5 {
		t.Errorf("wanted 14.3 from 13.5: M = %v, want 13.5 unchanged", got)
	}
	// Over the target and within the tolerance: 12 × 430 ÷ 460 = 11.2.
	if got := modelCapacity(12, lawInterval(12, 12, 0.48), DefaultCPUTarget, lawCeiling); got != 12 {
		t.Errorf("wanted 11.2 from 12: M = %v, want 12 unchanged", got)
	}
}

// Rule 4, falls: M falls only if U is over the target. Under it, a wanted below
// M says only that fewer were in flight than M allows.
func TestLawNoFallUnderTheTarget(t *testing.T) {
	// wanted 14.3, M 16, U 380m under the 450m target.
	if got := modelCapacity(16, lawInterval(12, 12, 0.38), DefaultCPUTarget, lawCeiling); got != 16 {
		t.Errorf("wanted 14.3 from 16 at 380m: M = %v, want 16 unchanged", got)
	}
	// At the target exactly: 9 in flight use it, and M stays at 12.
	if got := modelCapacity(12, lawInterval(9, 12, 0.45), DefaultCPUTarget, lawCeiling); got != 12 {
		t.Errorf("at the target: M = %v, want 12 unchanged", got)
	}
}

// Rule 4, rises: M rises only if U is under the target. Right after a cut more
// are in flight than M allows, so wanted is above M with U over the target.
func TestLawNoRiseOverTheTarget(t *testing.T) {
	// 8 in flight under a capacity of 8, at 480m: wanted 7.5, M 4.
	if got := modelCapacity(4, lawInterval(8, 8, 0.48), DefaultCPUTarget, lawCeiling); got != 4 {
		t.Errorf("wanted 7.5 from 4 at 480m: M = %v, want 4 unchanged", got)
	}
}

// Over the target M falls to wanted, slots idle or not, and a fall is not
// capped.
func TestLawOverTheTargetFalls(t *testing.T) {
	// 12 in flight of 16 allowed, at 480m: 12 × 430 ÷ 460.
	got := modelCapacity(16, lawInterval(12, 16, 0.48), DefaultCPUTarget, lawCeiling)
	if want := 12 * 0.43 / 0.46; !near(got, want) {
		t.Errorf("M = %v, want %v", got, want)
	}
	// From 32 to 7.2 in one interval.
	got = modelCapacity(32, lawInterval(8, 8, 0.5), DefaultCPUTarget, lawCeiling)
	if want := 8 * 0.43 / 0.48; !near(got, want) {
		t.Errorf("M = %v, want %v: a fall is not capped", got, want)
	}
}

// Pinned at the limit the law falls by about 10% per interval: correct in
// direction and slow. The step is what answers saturation.
func TestLawPinnedAtTheLimitFallsSlowly(t *testing.T) {
	got := modelCapacity(12, lawInterval(12, 12, 0.5), DefaultCPUTarget, lawCeiling)
	if want := 12 * 0.43 / 0.48; !near(got, want) {
		t.Errorf("M = %v, want %v", got, want)
	}
	if got < 0.89*12 || got > 0.9*12 {
		t.Errorf("M = %v from 12, want a fall of about 10%%", got)
	}
}

// Rule 5: a rise is at most to twice M.
func TestLawRiseCap(t *testing.T) {
	// 4 in flight at 100m: wanted is 4 × 430 ÷ 80 = 21.5.
	if got := modelCapacity(4, lawInterval(4, 4, 0.10), DefaultCPUTarget, lawCeiling); got != 8 {
		t.Errorf("wanted 21.5 from 4: M = %v, want 8", got)
	}
}

// Rules 5 and 6: a rise is at most to the ceiling.
func TestLawRiseStopsAtTheCeiling(t *testing.T) {
	if got := modelCapacity(10, lawInterval(10, 10, 0.10), DefaultCPUTarget, 16); got != 16 {
		t.Errorf("wanted 53.8 from 10 under a ceiling of 16: M = %v, want 16", got)
	}
}

// Rule 6: M is at least 1. Half a reconcile in flight on average, over the
// target, wants 0.47.
func TestLawFloorsAtOne(t *testing.T) {
	if got := modelCapacity(1, lawInterval(0.5, 1, 0.48), DefaultCPUTarget, lawCeiling); got != 1 {
		t.Errorf("wanted 0.47 from 1: M = %v, want 1", got)
	}
	// A background over the target leaves reconciles nothing: wanted is
	// negative, and M is 1.
	iv := lawInterval(8, 8, 0.5)
	iv.background = 0.46
	if got := modelCapacity(8, iv, DefaultCPUTarget, lawCeiling); got != 1 {
		t.Errorf("background over the target: M = %v, want 1", got)
	}
}

// An interval of no length measured nothing.
func TestLawAnEmptyIntervalMovesNothing(t *testing.T) {
	if got := modelCapacity(12, modelInterval{limit: 0.5, completions: 200}, DefaultCPUTarget, lawCeiling); got != 12 {
		t.Errorf("M = %v, want 12 unchanged", got)
	}
}

// modelClock feeds a capacityModel one sample per second.
type modelClock struct {
	m     *capacityModel
	now   time.Time
	usage time.Duration
	work  workTotals
	limit float64
}

func newModelClock() *modelClock {
	c := &modelClock{
		m:     newCapacityModel(DefaultCPUTarget, DefaultModelInterval),
		now:   time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC),
		limit: 0.5,
	}
	c.m.observe(c.now, c.usage, c.limit, 12, c.work, lawCeiling)
	return c
}

// seconds takes n samples, a second apart, in each of which the container used
// the given cores with inFlight reconciles in flight under a local capacity of
// inForce, perSecond reconciles completed and as many arrived.
func (c *modelClock) seconds(n int, used, inFlight float64, inForce int, perSecond int64) {
	for range n {
		c.now = c.now.Add(time.Second)
		c.usage += time.Duration(used * float64(time.Second))
		c.work.slotTime += time.Duration(inFlight * float64(time.Second))
		c.work.completions += perSecond
		c.work.arrivals += perSecond
		c.m.observe(c.now, c.usage, c.limit, inForce, c.work, lawCeiling)
	}
}

// M starts at the ceiling, and follows it until the law has moved it.
func TestModelStartsAtTheCeiling(t *testing.T) {
	c := newModelClock()
	if got := c.m.current(16); got != 16 {
		t.Errorf("a new model's M = %v, want the ceiling 16", got)
	}
	if got := c.m.current(32); got != 32 {
		t.Errorf("after the ceiling rose M = %v, want the new ceiling 32", got)
	}
	// Nine seconds are not an interval: nothing moves yet.
	c.seconds(9, 0.5, 16, 32, 20)
	if got := c.m.current(lawCeiling); got != lawCeiling {
		t.Errorf("before the first interval ended M = %v, want the ceiling", got)
	}
	c.seconds(1, 0.5, 16, 32, 20)
	if got, want := c.m.current(lawCeiling), 16*0.45/0.5; !near(got, want) {
		t.Errorf("after an interval pinned at the limit M = %v, want %v", got, want)
	}
	// Once moved, M no longer follows the ceiling up.
	if got, want := c.m.current(64), 16*0.45/0.5; !near(got, want) {
		t.Errorf("M = %v after the ceiling rose, want it still %v", got, want)
	}
}

// The background is the lowest U among the idle intervals of the last 10
// minutes, and 0 while there is none.
func TestModelBackground(t *testing.T) {
	c := newModelClock()
	c.seconds(10, 0.38, 12, 12, 20)
	if r := c.m.reported(lawCeiling); !r.measured || r.background != 0 || !near(r.used, 0.38) {
		t.Fatalf("with no idle interval yet: %+v, want U 0.38 and B 0", r)
	}
	c.seconds(10, 0.03, 0, 12, 0) // idle, 30m
	c.seconds(10, 0.02, 0, 12, 0) // idle, 20m
	c.seconds(10, 0.04, 0, 12, 0) // idle, 40m
	if r := c.m.reported(lawCeiling); !near(r.background, 0.02) {
		t.Fatalf("B = %v, want the lowest idle U, 0.02", r.background)
	}
	// A busy interval is judged with it: the worked example.
	c.m.capacity, c.m.moved = 12, true
	c.seconds(10, 0.38, 12, 12, 20)
	if got, want := c.m.current(lawCeiling), 12*0.43/0.36; !near(got, want) {
		t.Errorf("M = %v, want %v with B 0.02", got, want)
	}
	// An interval with any slot time is not idle, however little CPU it used.
	c.seconds(10, 0.001, 0.1, 12, 0)
	if r := c.m.reported(lawCeiling); !near(r.background, 0.02) {
		t.Errorf("B = %v after a quiet interval with a reconcile in flight, want 0.02", r.background)
	}
	// The idle intervals ended 20s, 30s and 40s in. At 640s the first two are
	// over ten minutes old, and at 650s the third is.
	c.seconds(580, 0.38, 12, 12, 20)
	if r := c.m.reported(lawCeiling); !near(r.background, 0.04) {
		t.Errorf("B = %v after the 20m and 30m intervals aged out, want 0.04", r.background)
	}
	c.seconds(10, 0.38, 12, 12, 20)
	if r := c.m.reported(lawCeiling); r.background != 0 {
		t.Errorf("B = %v with every idle interval over 10 minutes old, want 0", r.background)
	}
}

// What the model reports: CPU per reconcile is (U − B) ÷ X over the last
// interval with completions, and the required limit is what the arrivals would
// need to run at the target.
func TestModelReportsCostAndRequiredCPU(t *testing.T) {
	c := newModelClock()
	c.seconds(10, 0.02, 0, 12, 0) // idle: B is 20m
	if r := c.m.reported(lawCeiling); r.hasPerReconcile {
		t.Fatalf("CPU per reconcile reported before any reconcile completed: %+v", r)
	}
	// 380m used, 20 reconciles a second: 18ms each.
	c.seconds(10, 0.38, 12, 12, 20)
	r := c.m.reported(lawCeiling)
	if !r.hasPerReconcile || !near(r.perReconcile, 0.018) {
		t.Fatalf("CPU per reconcile = %v (known %v), want 0.018s", r.perReconcile, r.hasPerReconcile)
	}
	// 20 arrive a second: (0.02 + 0.018 × 20) ÷ 0.9.
	if want := (0.02 + 0.018*20) / 0.9; !near(r.required, want) {
		t.Errorf("required CPU = %v, want %v", r.required, want)
	}
	// An interval with arrivals and no completion keeps the last cost, and
	// requires CPU for what arrived: 50 a second.
	for range 10 {
		c.now = c.now.Add(time.Second)
		c.usage += 20 * time.Millisecond
		c.work.arrivals += 50
		c.m.observe(c.now, c.usage, c.limit, 12, c.work, lawCeiling)
	}
	r = c.m.reported(lawCeiling)
	if !near(r.perReconcile, 0.018) {
		t.Errorf("CPU per reconcile = %v after an interval with no completion, want the last one, 0.018", r.perReconcile)
	}
	if want := (0.02 + 0.018*50) / 0.9; !near(r.required, want) {
		t.Errorf("required CPU = %v, want %v", r.required, want)
	}
}

// The interval a limit changes in moves nothing; the next one is judged
// against the new limit.
func TestModelALimitChangeSkipsItsInterval(t *testing.T) {
	c := newModelClock()
	c.seconds(5, 0.5, 32, 32, 20)
	c.limit = 1
	c.seconds(5, 0.5, 32, 32, 20)
	if got := c.m.current(lawCeiling); got != lawCeiling {
		t.Fatalf("the limit changed in the interval: M = %v, want the ceiling unchanged", got)
	}
	// At 1 CPU the target is 900m: 16 in flight at 960m want 15, from 32.
	c.seconds(10, 0.96, 16, 32, 20)
	if got, want := c.m.current(lawCeiling), 16*0.9/0.96; !near(got, want) {
		t.Errorf("M = %v, want %v against the new limit", got, want)
	}
}

// A usage counter that goes backwards is another cgroup's: the interval starts
// again instead of reading the difference as CPU.
func TestModelUsageGoingBackwardsRestartsTheInterval(t *testing.T) {
	c := newModelClock()
	c.seconds(8, 0.5, 16, 32, 20)
	c.usage = 0
	// The first of these samples restarts the interval; nine more seconds
	// are not a whole one.
	c.seconds(10, 0.5, 16, 32, 20)
	if got := c.m.current(lawCeiling); got != lawCeiling {
		t.Fatalf("M = %v, want the ceiling: the interval restarted 9s ago", got)
	}
	if c.m.reported(lawCeiling).measured {
		t.Error("an interval was measured across the counter's restart")
	}
	c.seconds(1, 0.5, 16, 32, 20)
	if got, want := c.m.current(lawCeiling), 16*0.45/0.5; !near(got, want) {
		t.Errorf("M = %v a whole interval after the restart, want %v", got, want)
	}
}

// A cut pulls M down to the local capacity it left, and only down.
func TestModelCutPullsMDown(t *testing.T) {
	c := newModelClock()
	c.m.cutTo(8, 16)
	if got := c.m.current(16); got != 8 {
		t.Errorf("after a cut to 8, M = %v, want 8", got)
	}
	c.m.cutTo(12, 16)
	if got := c.m.current(16); got != 8 {
		t.Errorf("a cut to 12 raised M to %v, want 8", got)
	}
}

// reset forgets M, the interval and the background.
func TestModelReset(t *testing.T) {
	c := newModelClock()
	c.seconds(10, 0.02, 0, 12, 0)
	c.seconds(10, 0.5, 12, 12, 20)
	if c.m.current(lawCeiling) == lawCeiling {
		t.Fatal("M did not move")
	}
	c.m.reset()
	if got := c.m.current(lawCeiling); got != lawCeiling {
		t.Errorf("after reset M = %v, want the ceiling", got)
	}
	if r := c.m.reported(lawCeiling); r.measured || r.hasPerReconcile {
		t.Errorf("after reset the model still reports %+v", r)
	}
	if c.m.cpuTarget != DefaultCPUTarget || c.m.every != DefaultModelInterval {
		t.Errorf("reset lost the settings: target %v, interval %s", c.m.cpuTarget, c.m.every)
	}
}
