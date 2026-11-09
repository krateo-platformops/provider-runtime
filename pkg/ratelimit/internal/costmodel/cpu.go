package costmodel

import (
	"math"
	"time"
)

// CPUConfig configures the CPU path.
type CPUConfig struct {
	// Target is ρ, CPUTarget: the share of the limit the law aims at.
	Target float64
	// PressureThreshold is CPUPressureThreshold, a percentage of the sample's
	// time that some task stalled.
	PressureThreshold float64
	// SamplePeriod is the nominal sample period; τ is at least ten of them.
	SamplePeriod time.Duration
}

// Trust needs this many completions since the estimate's start (Spinner's
// guidance, via the survey; design §3.5).
const trustCompletions = 60

// Phase is the learned limit's state with no cpu.max (design §5.1).
type Phase int

const (
	// Free: no pressure, no CPU cap.
	Free Phase = iota
	// Onset: pressure for less than τ; rises are blocked, nothing is learned.
	Onset
	// Held: a limit was learned and the law runs against it.
	Held
)

func (p Phase) String() string {
	switch p {
	case Onset:
		return "onset"
	case Held:
		return "held"
	default:
		return "free"
	}
}

// CPUResult is the CPU path's output for one sample.
type CPUResult struct {
	// Slots is S_cpu after the sign guard, before pacing and the combination.
	Slots float64
	Kind  Kind
	// Paced reports that rises are limited to one slot per completion (§7).
	Paced bool
	// Blocked reports that pressure blocks rises this sample (§6).
	Blocked bool
	// Trusted is §3.5's trust in k̂.
	Trusted bool
	// Restarted reports that an estimate restarted, which resets the carry.
	Restarted bool
	// LimitChanged reports a new cpu.max or a new learned limit.
	LimitChanged bool
	// Guarded reports that the sign guard held the slots.
	Guarded bool
	// Unresolvable reports §4.2's unresolvable rule.
	Unresolvable bool

	Phase Phase
	// Leff is the learned limit in cores while held, else 0.
	Leff float64
	// Stall is the sample's stall share, a percentage; -1 when absent.
	Stall float64

	// For metrics: U (cores), B̂ (cores), k̂ (cores per slot), ĉ (seconds),
	// D̂ (seconds), the throughput (1/s) and the mean in flight.
	Used, Background, PerSlot, PerCompletion, Duration, Throughput, InFlight float64
}

type timed struct {
	at, v float64
}

// CPU is the CPU path's state (design §3.3–§3.5, §4.2, §5.1, §6). The
// sampler goroutine owns it.
type CPU struct {
	cfg CPUConfig

	t        float64 // seconds since the first sample
	lastL    float64 // the last sample's cpu.max, cores
	k, c, d  ratio
	dq       map[string]*ratio
	shares   map[string]*ratio
	nbar     spread
	since    int     // completions since k̂'s start
	tput     float64 // smoothed completions per second, for τ
	tputN    int
	idle     []timed // idle samples' CPU rates
	activeU  []timed // active samples' CPU rates
	pre      []timed // §5.1: CPU obtained in clear samples, weighted by Dt
	preDt    []timed
	phase    Phase
	leff     float64
	onU      float64 // §5.1 onset window's CPU and time
	onDt     float64
	uPre     float64 // U_pre saved at the onset
	firstRel bool
	clearS   float64 // §5.1 evidence window: stall and time
	clearDt  float64
	runU     float64 // a new pressured run while held
	runDt    float64
	runPre   float64
}

// NewCPU returns the CPU path's state.
func NewCPU(cfg CPUConfig) *CPU {
	return &CPU{cfg: cfg, dq: map[string]*ratio{}, shares: map[string]*ratio{}}
}

// tau is the estimates' time constant: ten samples, and at least 60
// completions at the smoothed throughput (design §3.5).
func (c *CPU) tau() float64 {
	t := 10 * c.cfg.SamplePeriod.Seconds()
	if c.tput > 0 {
		t = math.Max(t, 60/c.tput)
	}
	return t
}

func prune(ts []timed, from float64) []timed {
	i := 0
	for i < len(ts) && ts[i].at < from {
		i++
	}
	return ts[i:]
}

func minOf(ts []timed) (float64, bool) {
	if len(ts) == 0 {
		return 0, false
	}
	m := ts[0].v
	for _, x := range ts[1:] {
		m = math.Min(m, x.v)
	}
	return m, true
}

// background is B̂: the minimum over the idle samples of the last 3τ, bounded
// by every active sample of the same span; 0 with no idle sample (§3.3).
func (c *CPU) background() float64 {
	mi, ok := minOf(c.idle)
	if !ok {
		return 0
	}
	if ma, ok := minOf(c.activeU); ok {
		mi = math.Min(mi, ma)
	}
	return math.Max(mi, 0)
}

func (c *CPU) kind(s Sample, used, stall float64, capacity int) Kind {
	b := s.Books
	if b.SlotSeconds == 0 && b.Completions == 0 {
		return Idle
	}
	if s.Limit > 0 && s.Throttling && s.Periods >= 1 && s.Throttled >= s.Periods {
		slack := math.Min(s.Limit*s.Period.Seconds(), float64(s.Procs)*s.Slice.Seconds())
		if used*s.Dt.Seconds() >= s.Limit*s.Dt.Seconds()-slack {
			return Capped
		}
	}
	if s.Limit == 0 && c.phase == Held && stall > c.cfg.PressureThreshold {
		return Pressured
	}
	if capacity > 0 && b.NMin > capacity {
		return Draining
	}
	return Active
}

// Step folds in one sample and returns S_cpu. prev is the slots before it,
// capacity the whole number in force during the sample, and ceiling the most
// there may be.
func (c *CPU) Step(s Sample, prev float64, capacity, ceiling int) CPUResult {
	dt := s.Dt.Seconds()
	res := CPUResult{Slots: prev, Stall: -1}
	if dt <= 0 {
		return res
	}
	c.t += dt
	used := s.Usage.Seconds() / dt
	nbar := s.Books.SlotSeconds / dt
	x := float64(s.Books.Completions) / dt
	stall := -1.0
	if s.StallPresent {
		stall = 100 * s.Stall.Seconds() / dt
	}
	res.Used, res.InFlight, res.Stall = used, nbar, stall
	rho := c.cfg.Target

	if math.Abs(s.Limit-c.lastL) > 1e-9 {
		res.LimitChanged = true
		if s.Limit > 0 {
			c.phase, c.leff = Free, 0
		}
		// A new limit is a new plant: the CPU per slot-second depends on how
		// much throttling the limit causes (§4.2), so the old estimate
		// describes a plant that is gone. Simulation finding, PR 1.
		if c.lastL != 0 || c.t > dt {
			c.restartCost()
			res.Restarted = true
		}
	}
	c.lastL = s.Limit

	tau := c.tau()
	alpha := 1 - math.Exp(-dt/tau)

	// §5.1 runs before the sample is classified: a pressured sample is one
	// that arrives while a learned limit is held.
	if s.Limit == 0 && s.StallPresent {
		c.nodeLimit(s, used, stall, dt, tau, ceiling, &res)
	}

	kind := c.kind(s, used, stall, capacity)
	res.Kind = kind

	c.idle = prune(c.idle, c.t-3*tau)
	c.activeU = prune(c.activeU, c.t-3*tau)
	switch kind {
	case Idle:
		c.idle = append(c.idle, timed{c.t, used})
	case Active:
		c.activeU = append(c.activeU, timed{c.t, used})
	}
	bg := c.background()
	res.Background = bg

	// Throughput for τ, from every sample with work.
	if kind != Idle {
		c.tputN++
		al := math.Max(alpha, 1/float64(c.tputN))
		c.tput = (1-al)*c.tput + al*x
	}

	cpuNum := s.Usage.Seconds() - bg*dt
	books := s.Books
	stale := false
	switch kind {
	case Active:
		test := !c.nbar.moved(nbar, alpha)
		if books.SlotSeconds > 0 {
			switch c.k.add(cpuNum, books.SlotSeconds, alpha, test) {
			case restarted:
				res.Restarted = true
				c.since = 0
			case heldOut:
				// The sample disagrees with k̂ by 3σ: k̂ may be stale, so it
				// may not raise the slots until the next sample decides.
				// Simulation finding, PR 1.
				stale = true
			}
		}
		if books.Completions > 0 {
			c.since += books.Completions
			c.c.add(cpuNum, float64(books.Completions), alpha, test)
			c.d.add(books.SlotSeconds, float64(books.Completions), alpha, test)
			for q, qi := range books.Queues {
				if qi.Completions == 0 {
					continue
				}
				r := c.dq[q]
				if r == nil {
					r = &ratio{}
					c.dq[q] = r
				}
				r.add(qi.SlotSeconds, float64(qi.Completions), alpha, test)
			}
		}
	case Capped, Pressured, Draining:
		c.nbar.moved(nbar, alpha)
		if books.Completions > 0 {
			c.c.add(cpuNum, float64(books.Completions), alpha, true)
		}
	}
	if kind != Idle && books.SlotSeconds > 0 && c.shareShift(books, alpha) {
		c.restartCost()
		res.Restarted = true
	}

	kh, kok := c.k.value()
	res.PerSlot = kh
	res.PerCompletion, _ = c.c.value()
	res.Duration, _ = c.d.value()
	res.Throughput = c.tput
	se := c.k.se(alpha)
	res.Trusted = kok && c.since >= trustCompletions && c.k.n >= 3 && kh > 0 &&
		se/kh <= (1-rho)/2

	limit := s.Limit
	if limit == 0 && c.phase == Held {
		limit = c.leff
	}

	g := prev
	switch {
	case kind == Idle:
		// Nothing in flight, nothing ended: no information about the work.
	case limit == 0:
		g = float64(ceiling)
		res.Paced = true
	case kind == Capped || kind == Pressured:
		g = c.capped(s, books, limit, bg, nbar, dt)
	default:
		if !kok || kh <= 0 || (c.k.n >= 3 && kh < 2*se) {
			res.Unresolvable = true
			if used < rho*limit {
				g = float64(ceiling)
				res.Paced = true
			}
		} else {
			g = (rho*limit - bg) / kh
		}
	}
	if kind == Draining && g > prev {
		g = prev
	}
	if kind != Idle && limit > 0 {
		switch {
		case used < rho*limit && g < prev:
			g, res.Guarded = prev, true
		case used > rho*limit && g > prev:
			g, res.Guarded = prev, true
		}
	}
	// A restart can't raise the slots (§3.5): in its own sample they hold, and
	// after it the estimate is untrusted, so rises are paced.
	if (res.Restarted || stale) && g > prev {
		g = prev
	}
	if !res.Trusted || (s.Limit == 0 && c.phase == Held) {
		res.Paced = true
	}
	if stall > c.cfg.PressureThreshold || c.phase == Onset {
		res.Blocked = true
	}
	res.Slots = g
	res.Phase, res.Leff = c.phase, 0
	if c.phase == Held {
		res.Leff = c.leff
	}
	return res
}

// capped is the law for a capped or pressured sample (design §4.2):
// min((ρL − B̂)/cᵢ × D_mix, θN̄), with D_mix only when every queue that ended
// a reconcile has a D̂_q.
func (c *CPU) capped(s Sample, b Interval, limit, bg, nbar, dt float64) float64 {
	rho := c.cfg.Target
	theta := (rho*limit - bg) / (limit - bg)
	g := theta * nbar
	if b.Completions == 0 {
		return g
	}
	ci := (s.Usage.Seconds() - bg*dt) / float64(b.Completions)
	if ci <= 0 {
		return g
	}
	var mix float64
	for q, qi := range b.Queues {
		if qi.Completions == 0 {
			continue
		}
		r := c.dq[q]
		if r == nil {
			return g
		}
		dq, ok := r.value()
		if !ok {
			return g
		}
		mix += float64(qi.Completions) * dq
	}
	mix /= float64(b.Completions)
	return math.Min(g, (rho*limit-bg)/ci*mix)
}

// shareShift runs §3.5's change test on each queue's share of the slot-time,
// and reports a shift. Every queue seen so far is tested, at share 0 when it
// had no slot-time: a queue going quiet is a change of mix as much as one
// starting. Simulation finding, PR 1.
func (c *CPU) shareShift(b Interval, alpha float64) bool {
	for q := range b.Queues {
		if c.shares[q] == nil {
			c.shares[q] = &ratio{}
		}
	}
	if len(c.shares) < 2 {
		return false
	}
	shift := false
	for q, r := range c.shares {
		if r.add(b.Queues[q].SlotSeconds, b.SlotSeconds, alpha, true) == restarted {
			shift = true
		}
	}
	return shift
}

// nodeLimit runs §5.1's states with no cpu.max: free, onset (pressure for less
// than τ, which only blocks rises), held (a limit learned from a window that
// spans τ), re-learning and the stepwise release.
func (c *CPU) nodeLimit(s Sample, used, stall, dt, tau float64, ceiling int, res *CPUResult) {
	thr := c.cfg.PressureThreshold
	rho := c.cfg.Target
	over := stall > thr
	// U_pre: the CPU obtained in clear samples over the last τ.
	c.pre = prune(c.pre, c.t-tau)
	c.preDt = prune(c.preDt, c.t-tau)
	upre := func() float64 {
		var u, d float64
		for i := range c.pre {
			u += c.pre[i].v
			d += c.preDt[i].v
		}
		if d == 0 {
			return 0
		}
		return u / d
	}
	if !over {
		c.pre = append(c.pre, timed{c.t, s.Usage.Seconds()})
		c.preDt = append(c.preDt, timed{c.t, dt})
	}

	switch c.phase {
	case Free:
		if over {
			c.phase = Onset
			c.uPre = upre()
			c.onU, c.onDt = s.Usage.Seconds(), dt
			if c.onDt >= tau {
				c.learn(c.onU/c.onDt, res)
			}
		}
	case Onset:
		if !over {
			c.phase = Free
			return
		}
		c.onU += s.Usage.Seconds()
		c.onDt += dt
		if c.onDt >= tau {
			c.learn(c.onU/c.onDt, res)
		}
	case Held:
		if over {
			if c.runDt == 0 {
				c.runPre = upre()
			}
			c.runU += s.Usage.Seconds()
			c.runDt += dt
			c.clearS, c.clearDt = 0, 0
			if c.runDt >= tau {
				c.uPre = c.runPre
				c.learn(c.runU/c.runDt, res)
			}
			return
		}
		c.runU, c.runDt = 0, 0
		c.clearS += stall / 100 * dt
		c.clearDt += dt
		if c.clearDt >= tau && 100*c.clearS/c.clearDt <= thr {
			next := c.leff / rho
			if c.firstRel {
				next = math.Max(next, c.uPre)
				c.firstRel = false
			}
			c.leff = next
			c.clearS, c.clearDt = 0, 0
			res.LimitChanged = true
			if kh, ok := c.k.value(); ok && kh > 0 && (rho*c.leff-c.background())/kh >= float64(ceiling) {
				c.phase, c.leff = Free, 0
			}
		}
	}
}

// restartCost restarts the shared CPU estimates (not D̂_q, §3.4).
func (c *CPU) restartCost() {
	c.k.reset()
	c.c.reset()
	c.d.reset()
	c.since = 0
}

func (c *CPU) learn(leff float64, res *CPUResult) {
	c.phase, c.leff = Held, leff
	c.firstRel = true
	c.clearS, c.clearDt = 0, 0
	c.runU, c.runDt = 0, 0
	res.LimitChanged = true
}
