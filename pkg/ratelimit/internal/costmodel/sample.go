package costmodel

import "time"

// Sample is one sampler interval (design §3.1, §3.2). Dt is the interval
// measured on the monotonic clock, never the nominal period. Counters are
// deltas over the interval.
type Sample struct {
	// Dt is the interval's length.
	Dt time.Duration

	// Usage is cpu.stat usage_usec's delta: all the cgroup's CPU time.
	Usage time.Duration
	// Limit is cpu.max's quota over its period, in cores; 0 when cpu.max is
	// "max" or unreadable.
	Limit float64
	// Period is cpu.max's period.
	Period time.Duration
	// Throttling reports whether cpu.stat had nr_periods and nr_throttled,
	// which it has only when the cpu controller is enabled.
	Throttling bool
	// Periods and Throttled are nr_periods' and nr_throttled's deltas.
	Periods, Throttled uint64
	// Procs is GOMAXPROCS, the threads that run Go code at once.
	Procs int
	// Slice is the CFS bandwidth slice (sched_cfs_bandwidth_slice_us).
	Slice time.Duration

	// StallPresent reports whether cpu.pressure had a "some total" value.
	StallPresent bool
	// Stall is cpu.pressure "some total"'s delta.
	Stall time.Duration

	// Books is the Governor's slot accounting over the same interval.
	Books Interval
}

// Interval is the slot accounting closed at one sample (design §3.2).
type Interval struct {
	// SlotSeconds is ∫N dt over the interval.
	SlotSeconds float64
	// Completions counts reconciles that ended (Done), not handed-back grants.
	Completions int
	// Queues holds the same per queue, keyed by the gated queue's name.
	Queues map[string]QueueInterval
	// NMin and NMax are the fewest and most in flight during the interval,
	// taken after each critical section's dispatch.
	NMin, NMax int
}

// QueueInterval is one queue's part of an Interval.
type QueueInterval struct {
	SlotSeconds float64
	Completions int
}

// Kind is what a sample measures (design §3.2).
type Kind int

const (
	// Active samples measure the reconciles' cost.
	Active Kind = iota
	// Idle samples (nothing in flight, nothing ended) measure the background.
	Idle
	// Capped samples used the quota in every period: their CPU is censored.
	Capped
	// Pressured samples, with no cpu.max and the stall over the threshold in
	// force, are censored at the CPU the node gave (design §5.1), whether a
	// limit is held or not: an onset window's samples measure the node, not
	// the reconciles' cost. Simulation finding, PR 1 (§5.2).
	Pressured
	// Draining samples had more in flight than the capacity in force at every
	// moment: a cut preempts nothing, so they measure the cut's excess, not
	// the cost at the new capacity. Simulation finding, PR 1.
	Draining
)

func (k Kind) String() string {
	switch k {
	case Idle:
		return "idle"
	case Capped:
		return "capped"
	case Pressured:
		return "pressured"
	case Draining:
		return "draining"
	default:
		return "active"
	}
}
