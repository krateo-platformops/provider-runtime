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

// Rule 1, nothing in flight: a window with no reconcile in it says nothing
// about what reconciles cost. Without the rule, CPU over the target with
// nothing in flight would want a capacity of 0. The completions don't enter
// the law: they only choose the window's length.
func TestLawNothingInFlightMovesNothing(t *testing.T) {
	if got := modelCapacity(12, lawInterval(0, 12, 0.5), DefaultCPUTarget, lawCeiling); got != 12 {
		t.Errorf("nothing in flight, 500m used: M = %v, want 12 unchanged", got)
	}
	iv := lawInterval(12, 12, 0.38)
	iv.completions = 1
	if got, want := modelCapacity(12, iv, DefaultCPUTarget, lawCeiling), 12*0.43/0.36; !near(got, want) {
		t.Errorf("one completion in the window: M = %v, want %v: a window is evidence whatever it completed", got, want)
	}
}

// CPU barely used: with U − B under 5% of the limit the ratio divides by almost
// nothing, but the direction is certain, so the law asks for the rise cap,
// twice M. It is still a rise: it needs the slots used, and the bounds hold.
func TestLawCPUBarelyUsedAsksForTheRiseCap(t *testing.T) {
	// 24m above the background, of a 500m limit: under the 25m that is 5%.
	if got := modelCapacity(12, lawInterval(12, 12, 0.044), DefaultCPUTarget, lawCeiling); got != 24 {
		t.Errorf("U 24m above B, every slot used: M = %v, want 24", got)
	}
	// U under B is barely used too, not a negative capacity.
	if got := modelCapacity(12, lawInterval(12, 12, 0.01), DefaultCPUTarget, lawCeiling); got != 24 {
		t.Errorf("U under B, every slot used: M = %v, want 24", got)
	}
	// Rule 2 holds: with half the slots idle nothing rises.
	if got := modelCapacity(12, lawInterval(6, 12, 0.044), DefaultCPUTarget, lawCeiling); got != 12 {
		t.Errorf("U 24m above B, half the slots idle: M = %v, want 12 unchanged", got)
	}
	// Rule 6 holds: the ceiling.
	if got := modelCapacity(20, lawInterval(20, 20, 0.044), DefaultCPUTarget, lawCeiling); got != lawCeiling {
		t.Errorf("U 24m above B from 20: M = %v, want the ceiling %d", got, lawCeiling)
	}
	// Rule 4 holds: a background that alone takes CPU use over the target
	// leaves nothing to rise into.
	iv := lawInterval(12, 12, 0.46)
	iv.background = 0.44
	if got := modelCapacity(12, iv, DefaultCPUTarget, lawCeiling); got != 12 {
		t.Errorf("U 20m above a background of 440m, over the target: M = %v, want 12 unchanged", got)
	}
	// 26m above the background the ratio is used: 12 × 430 ÷ 26, capped at
	// twice M.
	if got := modelCapacity(12, lawInterval(12, 12, 0.046), DefaultCPUTarget, lawCeiling); got != 24 {
		t.Errorf("U 26m above B: M = %v, want 24", got)
	}
}

// The case that would otherwise stick: local capacity 1 on a 1 CPU limit,
// after a cut. One reconcile in flight uses 48m, 4.8% of the limit. M doubles
// at the evaluation, and from 2 the ratio takes over.
func TestLawOneSlotOnALargeLimitRises(t *testing.T) {
	window := func(inFlight, used float64) modelInterval {
		return modelInterval{
			elapsed:      10 * time.Second,
			cpuUsed:      time.Duration(used * 10 * float64(time.Second)),
			slotTime:     time.Duration(inFlight * 10 * float64(time.Second)),
			capacityTime: time.Duration(max(1, inFlight)) * 10 * time.Second,
			completions:  3,
			limit:        1,
		}
	}
	if got := modelCapacity(1, window(1, 0.048), DefaultCPUTarget, lawCeiling); got != 2 {
		t.Fatalf("one slot in use at 4.8%% of the limit: M = %v, want 2", got)
	}
	// 96m with two in flight: 2 × 900 ÷ 96 = 18.8, capped at twice M.
	if got := modelCapacity(2, window(2, 0.096), DefaultCPUTarget, lawCeiling); got != 4 {
		t.Errorf("two slots in use at 9.6%%: M = %v, want 4", got)
	}
	// The slot in use half the time is not evidence that more would be used.
	if got := modelCapacity(1, window(0.5, 0.024), DefaultCPUTarget, lawCeiling); got != 1 {
		t.Errorf("one slot in use half the time: M = %v, want 1 unchanged", got)
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

// Rule 3: the tolerance is on the ratio r = (target − B) ÷ (U − B). With CPU use
// within 5% of the target nothing moves, whatever M is.
func TestLawToleranceMovesNothing(t *testing.T) {
	// 440m of a 450m target: r is 430 ÷ 420 = 1.024. Wanted is 12.3, from 10.
	if got := modelCapacity(10, lawInterval(12, 12, 0.44), DefaultCPUTarget, lawCeiling); got != 10 {
		t.Errorf("r 1.024, wanted 12.3 from 10: M = %v, want 10 unchanged", got)
	}
	// 460m: r is 430 ÷ 440 = 0.977. Wanted is 11.7, from 16.
	if got := modelCapacity(16, lawInterval(12, 16, 0.46), DefaultCPUTarget, lawCeiling); got != 16 {
		t.Errorf("r 0.977, wanted 11.7 from 16: M = %v, want 16 unchanged", got)
	}
	// 420m: r is 430 ÷ 400 = 1.075, outside the band.
	got := modelCapacity(10, lawInterval(12, 12, 0.42), DefaultCPUTarget, lawCeiling)
	if want := 12 * 0.43 / 0.40; !near(got, want) {
		t.Errorf("r 1.075: M = %v, want %v", got, want)
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
	// With no background and every slot used, r is the target itself, 0.9:
	// outside the tolerance, so M falls by 10% at every evaluation. A tolerance
	// of 10% on the capacity would sit exactly on this case.
	m := 12.0
	for i, want := range []float64{10.8, 9.72, 8.748} {
		iv := lawInterval(m, m, 0.5)
		iv.background = 0
		if m = modelCapacity(m, iv, DefaultCPUTarget, lawCeiling); !near(m, want) {
			t.Fatalf("evaluation %d pinned at the limit with no background: M = %v, want %v", i+1, m, want)
		}
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

// interval takes one ModelInterval of samples, with the given completions, and
// as many arrivals, counted at its last sample.
func (c *modelClock) interval(used, inFlight float64, inForce int, completions int64) {
	c.seconds(9, used, inFlight, inForce, 0)
	c.work.completions += completions
	c.work.arrivals += completions
	c.seconds(1, used, inFlight, inForce, 0)
}

// Slow reconciles are evidence too: the window grows by whole intervals until
// it holds 20 completions, and the law is applied over all of it. At 5 a
// interval that is the fourth.
func TestModelWindowGrowsUntilItHoldsEnoughCompletions(t *testing.T) {
	c := newModelClock()
	for i := 1; i <= 3; i++ {
		c.interval(0.5, 16, 32, 5)
		if got := c.m.current(lawCeiling); got != lawCeiling {
			t.Fatalf("after %d interval(s), %d completions: M = %v, want the ceiling unchanged", i, 5*i, got)
		}
		if c.m.reported(lawCeiling).measured {
			t.Fatalf("a window was measured after %d interval(s)", i)
		}
	}
	// The fourth interval had 8 in flight: over all four the mean is 14, and
	// pinned at the limit the law wants 14 × 0.9. Over the last alone it
	// would be 7.2.
	c.interval(0.5, 8, 32, 5)
	if got, want := c.m.current(lawCeiling), 14*0.45/0.5; !near(got, want) {
		t.Fatalf("after the fourth interval M = %v, want %v: the law over the whole window", got, want)
	}
	if r := c.m.reported(lawCeiling); !r.measured || !near(r.used, 0.5) || !near(r.perReconcile, 0.5*40/20) {
		t.Errorf("the window's report = %+v, want U 0.5 and 1s of CPU per reconcile over 40s", r)
	}
	// The next window starts from nothing.
	before := c.m.current(lawCeiling)
	for range 3 {
		c.interval(0.5, 4, 32, 5)
	}
	if got := c.m.current(lawCeiling); got != before {
		t.Errorf("M = %v three intervals into the next window, want %v unchanged", got, before)
	}
}

// A window that never holds 20 completions ends after 30 intervals, and is
// evaluated with what it has. One in which nothing was in flight moves nothing.
func TestModelAWindowEndsAtThirtyIntervals(t *testing.T) {
	c := newModelClock()
	for i := 1; i < maxWindowIntervals; i++ {
		completions := int64(1)
		if i >= minCompletions {
			completions = 0 // 19 in all
		}
		c.interval(0.5, 16, 32, completions)
		if got := c.m.current(lawCeiling); got != lawCeiling {
			t.Fatalf("after %d interval(s) M = %v, want the ceiling unchanged", i, got)
		}
	}
	c.interval(0.5, 16, 32, 0)
	if got, want := c.m.current(lawCeiling), 16*0.45/0.5; !near(got, want) {
		t.Fatalf("after 30 intervals with 19 completions M = %v, want %v: the window is evaluated with what it has", got, want)
	}
	if r := c.m.reported(lawCeiling); !r.measured || !near(r.used, 0.5) {
		t.Errorf("the window's report = %+v, want U 0.5", r)
	}
	// Thirty intervals with nothing in flight, CPU still used: nothing moves.
	before := c.m.current(lawCeiling)
	for range maxWindowIntervals {
		c.interval(0.5, 0, 32, 0)
	}
	if got := c.m.current(lawCeiling); got != before {
		t.Errorf("M = %v after a window with nothing in flight, want %v unchanged", got, before)
	}
}

// A limit change inside a window discards it at the end of that interval, and
// the next window is judged against the new limit.
func TestModelALimitChangeDiscardsTheWindow(t *testing.T) {
	c := newModelClock()
	c.interval(0.5, 16, 32, 5)
	c.seconds(5, 0.5, 16, 32, 0)
	c.limit = 1
	c.seconds(4, 0.5, 16, 32, 0)
	c.work.completions += 5
	c.seconds(1, 0.5, 16, 32, 0)
	// Had the window gone on, it would hold 20 completions two intervals
	// from now, and be worth nothing. It restarted, so the fourth interval at
	// the new limit moves M: 16 in flight at 960m of 1 CPU want 15.
	for i := 1; i <= 3; i++ {
		c.interval(0.96, 16, 32, 5)
		if got := c.m.current(lawCeiling); got != lawCeiling {
			t.Fatalf("%d interval(s) after the limit changed M = %v, want the ceiling unchanged", i, got)
		}
	}
	c.interval(0.96, 16, 32, 5)
	if got, want := c.m.current(lawCeiling), 16*0.9/0.96; !near(got, want) {
		t.Errorf("four intervals after the limit changed M = %v, want %v", got, want)
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
	if b := c.m.background(c.now); !near(b, 0.02) {
		t.Fatalf("B = %v, want the lowest idle U, 0.02", b)
	}
	// The idle intervals completed nothing, so they are in the window the next
	// busy interval ends: 3 in flight on average under a capacity of 12. The
	// slots were not used, and nothing rises.
	c.m.capacity, c.m.moved = 12, true
	c.seconds(10, 0.38, 12, 12, 20)
	if got := c.m.current(lawCeiling); got != 12 {
		t.Fatalf("M = %v after a window that was idle for three intervals of four, want 12 unchanged", got)
	}
	if r := c.m.reported(lawCeiling); !near(r.used, (0.03+0.02+0.04+0.38)/4) || !near(r.background, 0.02) {
		t.Fatalf("the window's report = %+v, want U over all four intervals and B 0.02", r)
	}
	// A busy window is judged with B: the worked example.
	c.seconds(10, 0.38, 12, 12, 20)
	if got, want := c.m.current(lawCeiling), 12*0.43/0.36; !near(got, want) {
		t.Errorf("M = %v, want %v with B 0.02", got, want)
	}
	// An interval with any slot time is not idle, however little CPU it used.
	c.seconds(10, 0.001, 0.1, 12, 0)
	if b := c.m.background(c.now); !near(b, 0.02) {
		t.Errorf("B = %v after a quiet interval with a reconcile in flight, want 0.02", b)
	}
	// The idle intervals ended 20s, 30s and 40s in. At 640s the first two are
	// over ten minutes old, and at 650s the third is.
	c.seconds(570, 0.38, 12, 12, 20)
	if b := c.m.background(c.now); !near(b, 0.04) {
		t.Errorf("B = %v after the 20m and 30m intervals aged out, want 0.04", b)
	}
	c.seconds(10, 0.38, 12, 12, 20)
	if b := c.m.background(c.now); b != 0 {
		t.Errorf("B = %v with every idle interval over 10 minutes old, want 0", b)
	}
}

// What the model reports, over the last window: CPU per reconcile is
// (U − B) ÷ X, and the required limit is what the arrivals would need to run at
// the target.
func TestModelReportsCostAndRequiredCPU(t *testing.T) {
	c := newModelClock()
	c.seconds(10, 0.02, 0, 12, 0) // idle: B is 20m, and the window goes on
	if r := c.m.reported(lawCeiling); r.measured || r.hasPerReconcile {
		t.Fatalf("a window was reported before any reconcile completed: %+v", r)
	}
	// 380m used for 10s, 200 reconciles: over the window's 20s that is 200m
	// used, 180m above the background, at 10 reconciles a second: 18ms each.
	c.seconds(10, 0.38, 12, 12, 20)
	r := c.m.reported(lawCeiling)
	if !r.hasPerReconcile || !near(r.perReconcile, 0.018) || !near(r.used, 0.2) {
		t.Fatalf("CPU per reconcile = %v (known %v), U = %v; want 0.018s and 0.2", r.perReconcile, r.hasPerReconcile, r.used)
	}
	// 10 arrive a second: (0.02 + 0.018 × 10) ÷ 0.9.
	if want := (0.02 + 0.018*10) / 0.9; !near(r.required, want) {
		t.Errorf("required CPU = %v, want %v", r.required, want)
	}
	// A window whose first interval only saw arrivals, 50 a second: the rate
	// is over the whole window, (500 + 200) ÷ 20s.
	for range 10 {
		c.now = c.now.Add(time.Second)
		c.usage += 20 * time.Millisecond
		c.work.arrivals += 50
		c.m.observe(c.now, c.usage, c.limit, 12, c.work, lawCeiling)
	}
	if got := c.m.reported(lawCeiling); got != r {
		t.Fatalf("the report changed before the window ended: %+v", got)
	}
	c.seconds(10, 0.38, 12, 12, 20)
	r = c.m.reported(lawCeiling)
	if !near(r.perReconcile, 0.018) {
		t.Errorf("CPU per reconcile = %v, want 0.018", r.perReconcile)
	}
	if want := (0.02 + 0.018*35) / 0.9; !near(r.required, want) {
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
