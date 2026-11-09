package costmodel

import "time"

// Books accrues slot-seconds as reconciles take and give back local slots
// (design §3.2). It is pure: the caller passes every instant, as offsets on
// one monotonic clock, and serializes the calls (the Governor holds g.mu).
//
// A critical section that changes the count calls Grant and Release with one
// instant and then Commit: the fewest and most in flight are taken at Commit,
// after the section's dispatch, so a release whose slot is granted on in the
// same section isn't a dip (design §3.2, L6).
//
// Close ends an interval at a past instant, the sampler's read of cpu.stat,
// and carries the changes after it into the next interval (L5).
type Books struct {
	// start is the instant the open interval began.
	start time.Duration
	// inFlight and perQueue are the counts at start, and grants the grant
	// instants of the reconciles in flight then.
	inFlight int
	perQueue map[string]int
	grants   map[time.Duration]int
	// pending are the changes since start, in order.
	pending []change
}

type change struct {
	at        time.Duration
	queue     string
	delta     int
	completed bool
	// granted is a release's grant instant.
	granted time.Duration
	// commit marks the end of a critical section.
	commit bool
}

// NewBooks opens the first interval at start.
func NewBooks(start time.Duration) *Books {
	return &Books{start: start, perQueue: map[string]int{}, grants: map[time.Duration]int{}}
}

// Grant records a slot granted to queue at.
func (b *Books) Grant(at time.Duration, queue string) {
	b.pending = append(b.pending, change{at: at, queue: queue, delta: +1})
}

// Release records a slot given back by queue at, granted at granted (the pop's
// time the gated queue's processing map holds). completed is false for a grant
// handed back unused, which is not a completion.
func (b *Books) Release(at time.Duration, queue string, completed bool, granted time.Duration) {
	b.pending = append(b.pending, change{at: at, queue: queue, delta: -1, completed: completed, granted: granted})
}

// Commit ends a critical section at at.
func (b *Books) Commit(at time.Duration) {
	b.pending = append(b.pending, change{at: at, commit: true})
}

// InFlight reports the count after every recorded change.
func (b *Books) InFlight() int {
	n := b.inFlight
	for _, c := range b.pending {
		n += c.delta
	}
	return n
}

// Close ends the open interval at at and returns it. Changes recorded after at
// stay pending, so they count in the next interval.
func (b *Books) Close(at time.Duration) Interval {
	iv := Interval{Start: b.start, End: at, Queues: map[string]QueueInterval{}, NMin: -1, NMax: -1}
	cur := b.start
	n := b.inFlight
	// A count enters N± only if it lasted some time: the states between the
	// changes of one critical section, made at one instant, never do.
	accrue := func(until time.Duration) {
		if until <= cur {
			return
		}
		if iv.NMin < 0 || n < iv.NMin {
			iv.NMin = n
		}
		if n > iv.NMax {
			iv.NMax = n
		}
		d := (until - cur).Seconds()
		iv.SlotSeconds += float64(n) * d
		for q, k := range b.perQueue {
			if k == 0 {
				continue
			}
			qi := iv.Queues[q]
			qi.SlotSeconds += float64(k) * d
			iv.Queues[q] = qi
		}
		cur = until
	}
	i := 0
	for ; i < len(b.pending); i++ {
		c := b.pending[i]
		if c.at > at {
			break
		}
		accrue(c.at)
		if c.commit {
			continue
		}
		n += c.delta
		b.perQueue[c.queue] += c.delta
		if c.delta > 0 {
			b.grants[c.at]++
		} else {
			if b.grants[c.granted]--; b.grants[c.granted] <= 0 {
				delete(b.grants, c.granted)
			}
		}
		if c.delta < 0 && c.completed {
			iv.Completions++
			qi := iv.Queues[c.queue]
			qi.Completions++
			iv.Queues[c.queue] = qi
			iv.Lives = append(iv.Lives, Life{Queue: c.queue, Granted: c.granted, Ended: c.at})
		}
	}
	accrue(at)
	iv.Oldest = -1
	for g := range b.grants {
		if iv.Oldest < 0 || g < iv.Oldest {
			iv.Oldest = g
		}
	}
	if iv.NMin < 0 {
		// An interval of no length: the count at its instant.
		iv.NMin, iv.NMax = n, n
	}
	b.inFlight = n
	b.start = at
	b.pending = append(b.pending[:0], b.pending[i:]...)
	return iv
}
