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
	// Start and End are the interval's instants on the books' clock.
	Start, End time.Duration
	// SlotSeconds is ∫N dt over the interval.
	SlotSeconds float64
	// Completions counts reconciles that ended (Done), not handed-back grants.
	Completions int
	// Queues holds the same per queue, keyed by the gated queue's name.
	Queues map[string]QueueInterval
	// NMin and NMax are the fewest and most in flight during the interval,
	// taken after each critical section's dispatch.
	NMin, NMax int
	// Lives are the completions' grant and Done instants, in order (design
	// §17.1: the gated queue's processing map holds the pop's time).
	Lives []Life
	// Oldest is the grant instant of the oldest reconcile in flight at End,
	// or -1 when none is.
	Oldest time.Duration
}

// Life is one completed reconcile's time in the gate.
type Life struct {
	Queue          string
	Granted, Ended time.Duration
}

// Lifetime is the life's length in seconds.
func (l Life) Lifetime() float64 { return (l.Ended - l.Granted).Seconds() }

// QueueInterval is one queue's part of an Interval.
type QueueInterval struct {
	SlotSeconds float64
	Completions int
}

// Kind is what a sample measures (design §3.2, as amended by §5.1 and §17.3:
// the pressured and draining kinds are removed).
type Kind int

const (
	// Active samples measure the reconciles' cost.
	Active Kind = iota
	// Idle samples (nothing in flight, nothing ended) measure the background.
	Idle
	// Capped samples used the quota in every period: their CPU is censored.
	Capped
)

func (k Kind) String() string {
	switch k {
	case Idle:
		return "idle"
	case Capped:
		return "capped"
	default:
		return "active"
	}
}
