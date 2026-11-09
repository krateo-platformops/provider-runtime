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
	// Threshold is the pressure threshold in force for this sample, a
	// percentage: CPUPressureThreshold, or with no cpu.max while a limit is
	// held, the one calibrated at the operating point (§5.2).
	Threshold float64
	// Calibrated reports that Threshold is the calibrated one.
	Calibrated bool

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
	uPre     float64 // U_pre saved at the onset
	firstRel bool
	on       window // §5.1: the onset window, while free
	run      window // §5.1: a new run over the threshold, while held
	runPre   float64
	clear    window // §5.1: the evidence window
	past     window // §5.2: the window that learned L_eff: the stall past the share
	op       ratio  // §5.2: the stall share at the operating point, while held
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

func (c *CPU) kind(s Sample, used, stall, thr float64, capacity int) Kind {
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
	if s.Limit == 0 && stall > thr {
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
			c.free()
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

	// The threshold in force is taken before §5.1's states move: the sample
	// that learns a limit is judged against the threshold that opened its
	// window, and is the cut at onset.
	thr := c.threshold(s)
	res.Threshold, res.Calibrated = thr, s.Limit == 0 && c.phase == Held && c.separated()

	// §5.1 runs before the sample is classified, so the sample that learns a
	// limit is classified against it.
	if s.Limit == 0 && s.StallPresent {
		c.nodeLimit(s, stall, thr, dt, tau, alpha, ceiling, &res)
	}

	kind := c.kind(s, used, stall, thr, capacity)
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
	if stall > thr || c.phase == Onset {
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

// window accumulates samples for §5.1's windows: the CPU obtained and the
// stall share, both as ratios of sums.
type window struct {
	u, s, dt float64 // CPU-seconds, stall-seconds, seconds
	n        int
}

func (w *window) add(usage, stall, dt float64) {
	w.u += usage
	w.s += stall / 100 * dt
	w.dt += dt
	w.n++
}

// cpu is the CPU obtained over the window, in cores.
func (w *window) cpu() float64 { return w.u / w.dt }

// stall is the window's stall share, a percentage; 0 when empty.
func (w *window) stall() float64 {
	if w.dt == 0 {
		return 0
	}
	return 100 * w.s / w.dt
}

// nodeLimit runs §5.1's states with no cpu.max: free, onset (pressure for less
// than τ, which only blocks rises), held (a limit learned from a window that
// spans τ), re-learning and the stepwise release; and §5.2's calibration of
// the threshold at the operating point. thr is the threshold in force for
// this sample.
func (c *CPU) nodeLimit(s Sample, stall, thr, dt, tau, alpha float64, ceiling int, res *CPUResult) {
	usage := s.Usage.Seconds()
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
		c.pre = append(c.pre, timed{c.t, usage})
		c.preDt = append(c.preDt, timed{c.t, dt})
	}

	switch c.phase {
	case Free:
		if over {
			c.phase = Onset
			c.uPre = upre()
			c.on = window{}
			c.on.add(usage, stall, dt)
			if c.on.dt >= tau {
				c.learn(c.on, res)
			}
		}
	case Onset:
		if !over {
			c.phase = Free
			return
		}
		c.on.add(usage, stall, dt)
		if c.on.dt >= tau {
			c.learn(c.on, res)
		}
	case Held:
		if over {
			if c.run.n == 0 {
				c.runPre = upre()
			}
			c.run.add(usage, stall, dt)
			c.clear = window{}
			if c.run.dt >= tau {
				c.uPre = c.runPre
				c.learn(c.run, res)
			}
			return
		}
		c.run = window{}
		// §5.2: a clear sample while held is the operating point; it re-learns
		// the threshold over §3.5's window.
		c.op.add(stall*dt, dt, alpha, false)
		c.clear.add(usage, stall, dt)
		if c.clear.dt >= tau && c.clear.stall() <= thr {
			c.release(ceiling, res)
			c.clear = window{}
		}
	}
}

// release raises the held limit after a clear window (§5.1): the first one
// after a learn to max(L_eff/ρ, U_pre), each later one by 1/ρ, and the limit is
// dropped when the law at it reaches the ceiling.
func (c *CPU) release(ceiling int, res *CPUResult) {
	rho := c.cfg.Target
	next := c.leff / rho
	if c.firstRel {
		next = math.Max(next, c.uPre)
		c.firstRel = false
	}
	c.leff = next
	res.LimitChanged = true
	if kh, ok := c.k.value(); ok && kh > 0 && (rho*c.leff-c.background())/kh >= float64(ceiling) {
		c.free()
	}
}

// learn holds the limit obtained over a window that spanned τ, keeps that
// window's stall as the stall past the share, and restarts the operating
// point's (§5.2).
func (c *CPU) learn(w window, res *CPUResult) {
	c.phase, c.leff = Held, w.cpu()
	c.firstRel = true
	c.past = w
	c.clear, c.run = window{}, window{}
	c.op.reset()
	res.LimitChanged = true
}

// free drops the learned limit and the threshold's calibration.
func (c *CPU) free() {
	c.phase, c.leff = Free, 0
	c.past, c.clear, c.run = window{}, window{}, window{}
	c.op.reset()
}

// separated reports that the operating point's stall is measured (from the
// third clear sample, as §3.5's standard errors) and is below the stall past
// the share, so a threshold between them can be calibrated.
func (c *CPU) separated() bool {
	mo, ok := c.op.value()
	return ok && c.op.n >= 3 && mo < c.past.stall()
}

// threshold is the pressure threshold in force (§5.2). With a cpu.max, or
// with none and no limit held, it is CPUPressureThreshold. While a limit is
// held it is calibrated between two stall shares, each a ratio of sums: at the
// operating point, μo (the clear samples since the limit was learned, smoothed
// over τ), and past the share, μp (the window that learned the limit). The
// threshold is their midpoint, so a sample is read as whichever it is closer
// to. Until the operating point is measured, or when it stalls as much as past
// the share, it is μp: only a stall as high as past the share is pressure.
// Never below the option.
func (c *CPU) threshold(s Sample) float64 {
	thr := c.cfg.PressureThreshold
	if s.Limit != 0 || c.phase != Held {
		return thr
	}
	mp := c.past.stall()
	if !c.separated() {
		return math.Max(thr, mp)
	}
	mo, _ := c.op.value()
	return math.Max(thr, (mo+mp)/2)
}

// restartCost restarts the shared CPU estimates (not D̂_q, §3.4).
func (c *CPU) restartCost() {
	c.k.reset()
	c.c.reset()
	c.d.reset()
	c.since = 0
}
