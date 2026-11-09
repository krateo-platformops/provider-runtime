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

// CPUResult is the CPU path's output for one sample.
type CPUResult struct {
	// Slots is S_cpu after the sign guard, before pacing and the combination.
	Slots float64
	Kind  Kind
	// Paced reports that rises are limited to one slot per completion (§7).
	Paced bool
	// Blocked reports that pressure blocks rises this sample (§6).
	Blocked bool
	// Trusted is §3.5's trust, on D̂/ĉ (§17.1).
	Trusted bool
	// Restarted reports that ĉ or the shared estimates restarted, which
	// resets the carry and holds the slots in this sample.
	Restarted bool
	// CRestarted and CDeferred report ĉ's change test: a restart, or a
	// sample deferred (§17.1). DRestarted reports D̂'s.
	CRestarted, CDeferred, DRestarted bool
	// DRewarmed reports that a move of N̄ restarted D̂'s warm start (§17.4);
	// DHold that the active law held for want of a D̂ since; DFallback that D̂
	// is Little's law over uncapped samples (§17.1).
	DRewarmed, DHold, DFallback bool
	// LimitChanged reports a new cpu.max; ChangeSample that this sample is
	// the change's own (§17.2).
	LimitChanged, ChangeSample bool
	// FirstTerm reports a capped sample whose value is the capped rule's first
	// term, (ρL − B̂)·D_mix/c_W, rather than θN̄.
	FirstTerm bool
	// Guarded reports that the sign guard held the slots.
	Guarded bool
	// Unresolvable reports §4.2's unresolvable rule.
	Unresolvable bool
	// Stall is the sample's stall share, a percentage; -1 when absent.
	Stall float64

	// For metrics: U (cores), B̂ (cores), ĉ/D̂ (cores per slot), ĉ (seconds
	// of CPU), D̂ (seconds), c_W, ĉ's standard error, D̂/ĉ's relative
	// standard error, the throughput (1/s) and the mean in flight.
	Used, Background, PerSlot, PerCompletion, Duration, WindowCost, SECost, RelSE, Throughput, InFlight float64
}

type timed struct {
	at, v float64
}

type span struct{ from, to time.Duration }

// costSample is one sample's terms for c_W (§17.1).
type costSample struct {
	from, to    time.Duration
	cpu         float64 // CPU-seconds less the background
	completions int
	longest     float64 // the longest lifetime among its completions, seconds
}

// CPU is the CPU path's state (design §3.3–§3.5, §4.2, §17). The sampler
// goroutine owns it.
type CPU struct {
	cfg CPUConfig

	t     float64 // seconds since the first sample
	lastL float64 // the last sample's cpu.max, cores

	// ĉ: CPU per completion over every sample with work (§17.1).
	c      ratio
	cStart time.Duration // completions count from reconciles granted since
	since  int           // completions since ĉ's start
	prevC  struct {
		ok           bool
		num, alpha   float64
		from         time.Duration
		grants       []time.Duration
		completions  int
		hadLivesInfo bool
	}
	// D̂: per-reconcile lifetimes (§17.1), its warm start since dSince.
	d        ratio
	dSince   time.Duration
	dSinceT  float64
	dHasLife bool
	dq       map[string]*ratio
	little   ratio // Little's fallback, ΣA/ΣC over uncapped samples
	maxLife  float64
	bad      []span // samples a qualifying life may not overlap
	hist     []costSample

	shares  map[string]*ratio
	nbar    spread
	tput    float64 // smoothed completions per second, for τ
	tputN   int
	idle    []timed // idle samples' CPU rates
	activeU []timed // active samples' CPU rates
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

func (c *CPU) kind(s Sample, used float64) Kind {
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
	return Active
}

// costCompletions counts the completions ĉ's denominator takes: those of
// reconciles granted since ĉ's start (§17.1). Books without lives (a caller
// that doesn't record grants) count every completion.
func costCompletions(b Interval, from time.Duration) int {
	if b.Lives == nil {
		return b.Completions
	}
	n := 0
	for _, l := range b.Lives {
		if l.Granted >= from {
			n++
		}
	}
	return n
}

func (c *CPU) overlapsBad(l Life) bool {
	for _, b := range c.bad {
		if l.Granted < b.to && l.Ended > b.from {
			return true
		}
	}
	return false
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
	books := s.Books

	// §17.2: a limit change keeps ĉ and D̂_q, restarts D̂, and its own sample
	// describes neither limit.
	if math.Abs(s.Limit-c.lastL) > 1e-9 {
		res.LimitChanged = true
		if c.lastL != 0 || c.t > dt {
			res.ChangeSample = true
			c.rewarmD(books.End)
		}
	}
	c.lastL = s.Limit

	tau := c.tau()
	alpha := 1 - math.Exp(-dt/tau)
	kind := c.kind(s, used)
	res.Kind = kind
	over := stall > c.cfg.PressureThreshold

	// The samples a qualifying life may not overlap (§17.1).
	if kind == Capped || over || res.ChangeSample {
		c.bad = append(c.bad, span{books.Start, books.End})
	}
	keep := books.End - time.Duration((2*c.maxLife+3*tau)*float64(time.Second))
	for len(c.bad) > 0 && c.bad[0].to < keep {
		c.bad = c.bad[1:]
	}

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

	if kind != Idle {
		c.tputN++
		al := math.Max(alpha, 1/float64(c.tputN))
		c.tput = (1-al)*c.tput + al*x
	}
	if kind == Idle {
		res.Slots = prev
		res.Blocked = over
		c.report(&res)
		return res
	}

	// §17.4: a move of N̄ restarts D̂'s warm start, which takes only the
	// reconciles admitted after it.
	if c.nbar.moved(nbar, alpha) {
		c.rewarmD(books.End)
		res.DRewarmed = true
	}

	// ĉ, from every sample with work, with τ_ĉ = max(τ, D̂/r̄) (§17.1).
	stale := false
	cpuNum := s.Usage.Seconds() - bg*dt
	rbar := (1 - rho) / 2
	tauC := tau
	if dh, ok := c.d.value(); ok {
		tauC = math.Max(tau, dh/rbar)
	}
	alphaC := 1 - math.Exp(-dt/tauC)
	den := costCompletions(books, c.cStart)
	switch c.c.add(cpuNum, float64(den), alphaC, true) {
	case heldOut:
		res.CDeferred, stale = true, true
		c.since += den
	case restarted:
		// The restart's edge is the deferred sample's start: the denominator
		// counts only completions of reconciles granted since (§17.1).
		res.CRestarted, res.Restarted = true, true
		c.cStart = c.prevC.from
		c.c.reset()
		pd := 0
		for _, g := range c.prevC.grants {
			if g >= c.cStart {
				pd++
			}
		}
		if !c.prevC.hadLivesInfo {
			pd = c.prevC.completions
		}
		c.c.fold(c.prevC.num, float64(pd), c.prevC.alpha)
		den = costCompletions(books, c.cStart)
		c.c.fold(cpuNum, float64(den), alphaC)
		c.since = pd + den
	default:
		c.since += den
	}
	c.prevC.ok, c.prevC.num, c.prevC.alpha, c.prevC.from = true, cpuNum, alphaC, books.Start
	c.prevC.grants = c.prevC.grants[:0]
	for _, l := range books.Lives {
		c.prevC.grants = append(c.prevC.grants, l.Granted)
	}
	c.prevC.completions, c.prevC.hadLivesInfo = books.Completions, books.Lives != nil

	// c_W's sample terms (§17.1).
	longest := 0.0
	for _, l := range books.Lives {
		longest = math.Max(longest, l.Lifetime())
		c.maxLife = math.Max(c.maxLife, l.Lifetime())
	}
	c.hist = append(c.hist, costSample{books.Start, books.End, cpuNum, books.Completions, longest})
	aOldest := 0.0
	if books.Oldest >= 0 {
		aOldest = (books.End - books.Oldest).Seconds()
	}
	for len(c.hist) > 1 && (books.End-c.hist[1].from).Seconds() >= math.Max(c.maxLife, aOldest)+2*dt {
		c.hist = c.hist[1:]
	}

	// D̂ and D̂_q, from the lives that overlapped no capped, pressured or
	// change sample; D̂'s since its warm start (§17.1, §17.4).
	var sum float64
	var cnt int
	perQ := map[string][2]float64{}
	for _, l := range books.Lives {
		if c.overlapsBad(l) {
			continue
		}
		q := perQ[l.Queue]
		q[0] += l.Lifetime()
		q[1]++
		perQ[l.Queue] = q
		if l.Granted >= c.dSince {
			sum += l.Lifetime()
			cnt++
		}
	}
	if cnt > 0 {
		switch c.d.add(sum, float64(cnt), alpha, true) {
		case heldOut:
			stale = true
		case restarted:
			res.DRestarted = true
		}
		c.dHasLife = true
	}
	for q, v := range perQ {
		r := c.dq[q]
		if r == nil {
			r = &ratio{}
			c.dq[q] = r
		}
		r.add(v[0], v[1], alpha, false)
	}
	if kind != Capped && books.SlotSeconds > 0 && books.Completions > 0 {
		c.little.add(books.SlotSeconds, float64(books.Completions), alpha, false)
	}

	if books.SlotSeconds > 0 && c.shareShift(books, alpha) {
		c.c.reset()
		c.cStart, c.since = books.End, 0
		c.rewarmD(books.End)
		res.Restarted = true
	}

	ch, cok := c.c.value()
	seC := c.c.seRatio()
	var dv float64
	dok := false
	switch {
	case c.dHasLife:
		dv, dok = c.d.value()
	case c.t-c.dSinceT >= tau:
		if v, ok := c.little.value(); ok {
			dv, dok, res.DFallback = v, true, true
		}
	}
	res.SECost = seC
	if cok && dok && ch > 0 && dv > 0 {
		seD := c.d.seRatio()
		if res.DFallback {
			seD = c.little.seRatio()
		}
		res.RelSE = math.Hypot(seC/ch, seD/dv)
		res.Trusted = c.since >= trustCompletions && c.c.n >= 3 && c.d.n >= 3 && res.RelSE <= rbar
	}

	limit := s.Limit
	g := prev
	switch {
	case limit == 0:
		// §5.1: with no CPU limit, the CPU law doesn't run.
		g = float64(ceiling)
		res.Paced = true
	case res.ChangeSample:
		// §17.2 (L11): the sample's own N̄ scaled by what the new limit
		// allows over what the sample used, its usage counted up to the new
		// limit.
		if d := math.Min(used, limit) - bg; d > 0 {
			g = math.Min(prev, (rho*limit-bg)*nbar/d)
		}
	case kind == Capped:
		g = c.capped(s, books, limit, bg, nbar, dt, &res)
	default:
		switch {
		case !cok || ch <= 0 || (c.c.n >= 3 && ch < 2*seC):
			res.Unresolvable = true
			if used < rho*limit {
				g = float64(ceiling)
				res.Paced = true
			}
		case !dok:
			res.DHold = true
		default:
			g = (rho*limit - bg) * dv / ch
		}
	}
	if limit > 0 {
		switch {
		case used < rho*limit && g < prev:
			g, res.Guarded = prev, true
		case used > rho*limit && g > prev:
			g, res.Guarded = prev, true
		}
	}
	// Neither a restart, a deferred sample nor the change's own sample can
	// raise the slots (§3.5, §17.1, §17.2).
	if (res.Restarted || stale || res.ChangeSample) && g > prev {
		g = prev
	}
	if !res.Trusted {
		res.Paced = true
	}
	res.Blocked = over
	res.Slots = g
	if cok {
		res.PerCompletion = ch
	}
	if dok {
		res.Duration = dv
		if cok && dv > 0 {
			res.PerSlot = ch / dv
		}
	}
	c.report(&res)
	return res
}

// report fills the result's throughput.
func (c *CPU) report(res *CPUResult) {
	res.Throughput = c.tput
}

// rewarmD restarts D̂'s warm start at at: it takes only reconciles granted
// since, and the law holds until one has completed (§17.2, §17.4).
func (c *CPU) rewarmD(at time.Duration) {
	c.d.reset()
	c.dSince, c.dSinceT, c.dHasLife = at, c.t, false
}

// windowCost is c_W: the CPU per completion over the most recent samples
// spanning at least max(the longest lifetime among the window's completions,
// a_oldest) + Δt (§17.1).
func (c *CPU) windowCost(b Interval, dt float64) (float64, bool) {
	aOldest := 0.0
	if b.Oldest >= 0 {
		aOldest = (b.End - b.Oldest).Seconds()
	}
	var cpu float64
	var comp int
	longest := 0.0
	for i := len(c.hist) - 1; i >= 0; i-- {
		h := c.hist[i]
		cpu += h.cpu
		comp += h.completions
		longest = math.Max(longest, h.longest)
		if (b.End - h.from).Seconds() >= math.Max(longest, aOldest)+dt {
			break
		}
	}
	if comp == 0 {
		return 0, false
	}
	return cpu / float64(comp), true
}

// capped is the law for a capped sample (design §4.2, §17.1):
// min((ρL − B̂)·D_mix/c_W, θN̄), with D_mix only when every queue that ended a
// reconcile has a D̂_q.
func (c *CPU) capped(s Sample, b Interval, limit, bg, nbar, dt float64, res *CPUResult) float64 {
	rho := c.cfg.Target
	theta := (rho*limit - bg) / (limit - bg)
	g := theta * nbar
	cw, ok := c.windowCost(b, dt)
	if !ok || cw <= 0 || b.Completions == 0 {
		return g
	}
	res.WindowCost = cw
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
	if first := (rho*limit - bg) * mix / cw; first < g {
		res.FirstTerm = true
		return first
	}
	return g
}

// shareShift runs §3.5's change test on each queue's share of the slot-time,
// and reports a shift. Every queue seen so far is tested, at share 0 when it
// had no slot-time: a queue going quiet is a change of mix as much as one
// starting.
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
