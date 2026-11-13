package ratelimit

import (
	"math"
	"time"
)

// The capacity model (docs/design-capacity-model.md): local capacity computed
// from the container's measured CPU use, at CPU limits of MinCPULimit and
// above. Nothing in this file does I/O or takes a lock: the sampler goroutine
// feeds it values it has already read.

// Capacity model defaults (design §8).
const (
	// DefaultCPUTarget is the share of the CPU limit the law aims at.
	DefaultCPUTarget = 0.9
	// DefaultMinCPULimit is the CPU limit, in cores, under which the law does
	// not run: below it one reconcile's CPU is a large part of a scheduler
	// period's quota, and CPU use isn't proportional to concurrency.
	DefaultMinCPULimit = 0.5
	// DefaultModelInterval is how often the model's capacity is recomputed.
	DefaultModelInterval = 10 * time.Second
)

// The law's fixed values (design §3).
const (
	// minCompletions is how many completed reconciles a window waits for
	// before it ends. It only sets the window's length.
	minCompletions = 20
	// maxWindowIntervals is how many ModelIntervals a window grows to while it
	// waits for minCompletions. A window that long is evaluated with what it
	// has.
	maxWindowIntervals = 30
	// minReconcileCPUShare is the least CPU above the background, as a share
	// of the limit, a window must show for the ratio to be usable. Under it
	// the CPU is barely used: the ratio divides by almost nothing, but the
	// direction is certain, and the law asks for the rise cap.
	minReconcileCPUShare = 0.05
	// usedSlotsShare is the share of the capacity in force that must have been
	// in flight for a rise.
	usedSlotsShare = 0.9
	// modelTolerance is how far from 1 the ratio (target − B) ÷ (U − B) must be
	// to move the model's capacity: the tolerance of the Horizontal Pod
	// Autoscaler, on the same ratio. It is on the ratio, not on the capacity,
	// so that a container pinned at its limit is always outside it: there the
	// ratio is CPUTarget or less.
	modelTolerance = 0.05
	// maxRiseFactor caps one evaluation's rise.
	maxRiseFactor = 2
	// backgroundWindow is how far back the idle intervals that set the
	// background are kept.
	backgroundWindow = 10 * time.Minute
)

// modelInterval is what one window measured: one ModelInterval, or as many
// whole ones as it took to see minCompletions completions, up to
// maxWindowIntervals.
type modelInterval struct {
	// elapsed is the window's length.
	elapsed time.Duration
	// cpuUsed is the change of cpu.stat's usage_usec.
	cpuUsed time.Duration
	// slotTime is the time local slots were held, summed over the slots.
	slotTime time.Duration
	// capacityTime is the published local capacity summed over the interval's
	// time: divided by elapsed, the capacity in force.
	capacityTime time.Duration
	// completions counts the reconciles that gave their slot back.
	completions int64
	// limit is the CPU limit in cores, and limitChanged reports that it was
	// not the same at every sample.
	limit        float64
	limitChanged bool
	// background is B: the CPU, in cores, the container uses with no
	// reconcile in flight.
	background float64
}

// used is U: the CPU used, in cores.
func (iv modelInterval) used() float64 { return iv.cpuUsed.Seconds() / iv.elapsed.Seconds() }

// inFlight is N: the mean number of reconciles in flight.
func (iv modelInterval) inFlight() float64 { return iv.slotTime.Seconds() / iv.elapsed.Seconds() }

// inForce is the mean local capacity over the interval.
func (iv modelInterval) inForce() float64 { return iv.capacityTime.Seconds() / iv.elapsed.Seconds() }

// modelCapacity is the law: the model's capacity M after a window, from M
// before it and what the window measured. It is a pure function.
//
//	wanted = N × r,  r = (target − B) ÷ (U − B),  target = cpuTarget × limit
//
// Below the limit CPU use grows in proportion to the reconciles in flight, so
// the concurrency that uses the target is the current one scaled by how far U
// is from the target. With the CPU barely used, U − B under 5% of the limit,
// the ratio is not usable and wanted is the rise cap, 2 × M. M moves to wanted
// under six rules, applied in this order.
func modelCapacity(m float64, iv modelInterval, cpuTarget float64, ceiling int) float64 {
	if iv.elapsed <= 0 {
		return m
	}
	used, inFlight := iv.used(), iv.inFlight()
	target := cpuTarget * iv.limit
	reconcileCPU := used - iv.background

	// Rule 1. The window: one with nothing in flight, or with a limit change
	// in it, moves nothing.
	if inFlight == 0 || iv.limitChanged {
		return m
	}
	barelyUsed := reconcileCPU < minReconcileCPUShare*iv.limit
	ratio, wanted := 0.0, maxRiseFactor*m
	if !barelyUsed {
		ratio = (target - iv.background) / reconcileCPU
		wanted = inFlight * ratio
	}

	// Rule 2. A rise needs the slots to have been used: with slots standing
	// idle, more of them would have changed nothing.
	if wanted > m && inFlight < usedSlotsShare*iv.inForce() {
		return m
	}

	// Rule 3. Tolerance, on the ratio: CPU use within 5% of the target moves
	// nothing. With the CPU barely used there is no ratio to judge.
	if !barelyUsed && math.Abs(ratio-1) <= modelTolerance {
		return m
	}

	// Rule 4. Direction: a rise only under the target, a fall only over it.
	if (wanted > m && used >= target) || (wanted < m && used <= target) {
		return m
	}

	// Rule 5. Rise cap: at most to twice M. A fall is not capped. The ceiling,
	// which also caps a rise, is rule 6's.
	if wanted > m {
		wanted = min(wanted, maxRiseFactor*m)
	}

	// Rule 6. Bounds.
	return min(max(wanted, capacityFloor), float64(ceiling))
}

// workTotals are the gated queues' cumulative counts at one sample.
type workTotals struct {
	// slotTime is the time local slots were held, summed over the slots.
	slotTime time.Duration
	// completions counts reconciles that gave their slot back, and arrivals
	// items that became ready to run.
	completions, arrivals int64
}

// modelPoint is what the model keeps of one sample.
type modelPoint struct {
	at    time.Time
	usage time.Duration
	work  workTotals
}

// idleUse is the CPU an interval with no reconcile in flight used.
type idleUse struct {
	at   time.Time
	used float64
}

// modelReport is the model's state for metrics. It is replaced, never mutated.
type modelReport struct {
	// capacity is M.
	capacity float64
	// measured reports that a window has ended; used is its U and background
	// the B it was judged with.
	measured         bool
	used, background float64
	// perReconcile is the CPU seconds one reconcile used, over the last
	// window with completions; hasPerReconcile is false until there was one.
	perReconcile    float64
	hasPerReconcile bool
	// required is the CPU limit, in cores, at which the work that arrived in
	// the last window would run at the target. It needs perReconcile.
	required float64
}

// capacityModel keeps the model's capacity M and the window it is measuring.
// Only the sampler goroutine uses it.
type capacityModel struct {
	cpuTarget float64
	every     time.Duration

	// capacity is M once the law, or a cut, has moved it. Until then M is the
	// ceiling, whatever the ceiling becomes.
	capacity float64
	moved    bool

	// open reports that a window is being measured: it began at start, and
	// last is its latest sample. A window is made of whole ModelIntervals:
	// interval is where the current one began, and intervals how many have
	// ended.
	open                  bool
	start, interval, last modelPoint
	intervals             int
	// limit is the CPU limit at the window's first sample.
	limit        float64
	limitChanged bool
	capacityTime time.Duration

	// idle holds U for the intervals of the last backgroundWindow in which no
	// reconcile was in flight, oldest first.
	idle []idleUse

	report modelReport
}

func newCapacityModel(cpuTarget float64, every time.Duration) *capacityModel {
	return &capacityModel{cpuTarget: cpuTarget, every: every}
}

// current is M: the ceiling until something has moved it.
func (c *capacityModel) current(ceiling int) float64 {
	if !c.moved {
		return float64(ceiling)
	}
	return min(c.capacity, float64(ceiling))
}

// reset forgets everything: the law stopped applying, so M is the ceiling
// again and the next window starts from nothing.
func (c *capacityModel) reset() {
	*c = capacityModel{cpuTarget: c.cpuTarget, every: c.every}
}

// cutTo pulls M down to local, the capacity a cut by the step left, if M was
// above it: the law resumes from there and must find evidence to rise.
func (c *capacityModel) cutTo(local, ceiling int) {
	if c.current(ceiling) > float64(local) {
		c.capacity, c.moved = float64(local), true
	}
}

// observe takes one sample: the cumulative CPU usage and the CPU limit read at
// now, the queues' totals at now, and inForce, the local capacity published
// since the sample before.
//
// The law is applied over a window of whole ModelIntervals. At the end of each
// interval the window ends if it holds minCompletions completions, if the limit
// changed in it, or if it is maxWindowIntervals long; otherwise it grows by
// another interval, so that slow reconciles are measured over enough of them.
// The completions only choose the window's length: the law is applied to every
// window that ends, and decides that one with a limit change, or with nothing
// in flight, moves nothing (rule 1). The next window starts at this sample.
//
// A usage counter that went backwards belongs to another cgroup, and starts
// the window again.
func (c *capacityModel) observe(now time.Time, usage time.Duration, limit float64, inForce int, work workTotals, ceiling int) {
	p := modelPoint{at: now, usage: usage, work: work}
	if !c.open || usage < c.last.usage {
		c.begin(p, limit)
		return
	}
	if !now.After(c.last.at) {
		return
	}
	c.capacityTime += time.Duration(inForce) * now.Sub(c.last.at)
	if limit != c.limit {
		c.limitChanged = true
	}
	c.last = p
	if now.Sub(c.interval.at) < c.every {
		return
	}
	if p.work.slotTime == c.interval.work.slotTime {
		idle := (p.usage - c.interval.usage).Seconds() / now.Sub(c.interval.at).Seconds()
		c.idle = append(c.idle, idleUse{at: now, used: idle})
	}
	c.interval = p
	c.intervals++
	if p.work.completions-c.start.work.completions < minCompletions && !c.limitChanged && c.intervals < maxWindowIntervals {
		return
	}
	c.closeWindow(ceiling)
	c.begin(p, limit)
}

func (c *capacityModel) begin(p modelPoint, limit float64) {
	c.open, c.start, c.interval, c.last, c.intervals = true, p, p, p, 0
	c.limit, c.limitChanged, c.capacityTime = limit, false, 0
}

// closeWindow applies the law to the window from start to last, and records
// what it measured for metrics.
func (c *capacityModel) closeWindow(ceiling int) {
	iv := modelInterval{
		elapsed:      c.last.at.Sub(c.start.at),
		cpuUsed:      c.last.usage - c.start.usage,
		slotTime:     c.last.work.slotTime - c.start.work.slotTime,
		capacityTime: c.capacityTime,
		completions:  c.last.work.completions - c.start.work.completions,
		limit:        c.limit,
		limitChanged: c.limitChanged,
		background:   c.background(c.last.at),
	}

	before := c.current(ceiling)
	if after := modelCapacity(before, iv, c.cpuTarget, ceiling); after != before {
		c.capacity, c.moved = after, true
	}

	r := c.report
	r.measured, r.used, r.background = true, iv.used(), iv.background
	seconds := iv.elapsed.Seconds()
	if iv.completions > 0 {
		// (U − B) ÷ X, for a throughput X. U under B is no CPU, not less than
		// none.
		r.perReconcile = max(0, iv.used()-iv.background) * seconds / float64(iv.completions)
		r.hasPerReconcile = true
	}
	if r.hasPerReconcile {
		arrivals := float64(c.last.work.arrivals-c.start.work.arrivals) / seconds
		r.required = (iv.background + r.perReconcile*arrivals) / c.cpuTarget
	}
	c.report = r
}

// background returns B at now: the lowest U among the ModelIntervals of the
// last backgroundWindow in which no reconcile was in flight, and 0 while there
// is none. Too low a B charges background CPU to reconciles, which lowers the
// law's result: the safe side.
func (c *capacityModel) background(now time.Time) float64 {
	for len(c.idle) > 0 && now.Sub(c.idle[0].at) > backgroundWindow {
		c.idle = c.idle[1:]
	}
	if len(c.idle) == 0 {
		return 0
	}
	lowest := c.idle[0].used
	for _, i := range c.idle[1:] {
		lowest = min(lowest, i.used)
	}
	return lowest
}

// reported returns the model's state for metrics, with M as it is under
// ceiling.
func (c *capacityModel) reported(ceiling int) modelReport {
	r := c.report
	r.capacity = c.current(ceiling)
	return r
}
