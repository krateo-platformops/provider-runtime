package costmodel

import (
	"math"
	"sort"
	"time"

	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit/internal/costmodel/internal/fpmath"
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
	// Paced reports that rises are limited to one slot per completion (§7);
	// PaceCompletions is the completions the pacing counts, those ĉ's
	// denominator counts (§17.8, item 5).
	Paced           bool
	PaceCompletions int
	// Blocked reports that pressure blocks rises this sample (§6).
	Blocked bool
	// Trusted is §3.5's trust, on D̂/ĉ (§17.1).
	Trusted bool
	// Restarted reports that ĉ, D̂ or the shared estimates restarted, which
	// resets the carry and holds the slots in this sample.
	Restarted bool
	// CRestarted and CDeferred report ĉ's change test over windows: a
	// restart, or a window deferred (§17.8, item 2). CPending reports that no
	// rise is allowed while the test is pending. DRestarted reports D̂'s.
	CRestarted, CDeferred, CPending, DRestarted bool
	// CHold reports that the active law held after a ĉ restart with no
	// completion its denominator counts yet (§17.8, item 5).
	CHold bool
	// DDeferred and DPending report D̂'s change test over the same windows
	// (§17.9, item 7). UnresHold reports that the unresolvable rule was held
	// because a sample of the last τ was window-capped (§17.9, item 4).
	DDeferred, DPending, UnresHold bool
	// ContentionEnd reports that a run of contention samples ended and
	// rewarmed D̂ (§17.9, item 5); InRun that the sample is in a contention
	// run (§17.10 (D)).
	ContentionEnd, InRun bool
	// DMixFallback reports that D_mix used D̂ for a queue whose D̂_q has had no
	// qualifying life since its start (§17.10 (C)); UW is the CPU over the window,
	// which the sign guard reads (§17.10 (A)).
	DMixFallback bool
	UW           float64
	// WindowLen is the closed window's samples when WindowClosed is set.
	WindowLen int
	// WindowClosed reports that a c_W test window closed in this sample;
	// SigmaW is the window test's standard deviation of c_W (seconds).
	WindowClosed bool
	SigmaW       float64
	// WindowSamples is n_W, the samples in this sample's window W.
	WindowSamples int
	// DRewarmed reports that a move of the published S restarted D̂'s warm
	// start (§17.8, item 3); DHold that the active law held for want of a D̂
	// since; DFallback that D̂ is Little's law over uncapped samples (§17.1).
	DRewarmed, DHold, DFallback bool
	// StretchedLives counts the lives taken into D̂ that overlapped a sample
	// whose every period was throttled though its window didn't cap (§17.8,
	// item 7(a)).
	StretchedLives int
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

// histSample is one sample's terms for the window W (§17.1, §17.8).
type histSample struct {
	from, to           time.Duration
	usage, dt          float64 // CPU-seconds and seconds
	periods, throttled uint64
	completions        int
	queues             map[string]int // completions per queue, for D_mix over W
	longest            float64        // the longest lifetime among its completions, seconds
}

// winSample is a sample's part of a ĉ test window, kept to rebuild ĉ's sums
// from the restart's edge.
type winSample struct {
	num, alpha float64
	grants     []time.Duration
	completion int
	hasLives   bool
}

// costWindow is a ĉ test window in progress or closed (§17.8, item 2).
type costWindow struct {
	from    time.Duration
	cpu     float64
	comp    int
	longest float64
	samples []winSample
	side    float64
}

func (w *costWindow) value() (float64, bool) {
	if w.comp == 0 {
		return 0, false
	}
	return w.cpu / float64(w.comp), true
}

// dWindow is D̂'s part of a test window: the qualifying lives since its start.
type dWindow struct {
	sum     float64
	cnt     int
	samples [][3]float64 // sum, count, alpha
	side    float64
}

func (w *dWindow) value() (float64, bool) {
	if w.cnt == 0 {
		return 0, false
	}
	return w.sum / float64(w.cnt), true
}

// CPU is the CPU path's state (design §3.3–§3.5, §4.2, §17). The sampler
// goroutine owns it.
type CPU struct {
	cfg CPUConfig

	t     float64 // seconds since the first sample
	lastL float64 // the last sample's cpu.max, cores

	// ĉ: CPU per completion over every sample with work (§17.1), tested over
	// consecutive windows (§17.8, item 2).
	c        ratio
	cStart   time.Duration // completions count from reconciles granted since
	since    int           // completions since ĉ's start
	win      costWindow    // the window in progress
	pend     *costWindow   // a deferred window
	wn       int           // windows folded into the test's statistics
	wmu, wv  float64
	restartC bool // ĉ restarted and no counted completion since

	// D̂: per-reconcile lifetimes (§17.1), its warm start since dSince.
	d        ratio
	dSince   time.Duration
	dSinceT  float64
	dHasLife bool
	dq       map[string]*ratio
	little   ratio // Little's fallback, ΣA/ΣC over uncapped samples
	maxLife  float64
	bad      []span // window-capped, pressured and change samples
	tight    []span // samples whose every period was throttled, not window-capped
	tightAll []span // every per-sample tight sample (§17.10 (C))
	hist     []histSample

	// The published S's steps, for the rewarm (§17.8 item 3, §17.9 item 2).
	sPrev   float64
	sSteps  spread
	sRecent []timed
	sRef    float64
	sRefT   float64

	// §17.9: the end of the window after a limit change, the last
	// window-capped sample, a contention run in progress, B̂ held.
	changeAfter time.Duration
	lastCapT    float64
	runOn       bool          // a contention run (§17.10 (D))
	lastContEnd time.Duration // the end of the run's last contention sample
	bHat        float64
	bHatOK      bool

	// D̂'s change test over ĉ's windows (§17.9, item 7).
	dWin      dWindow
	dPend     *dWindow
	dwn       int
	dwmu, dwv float64

	shares  map[string]*ratio
	tput    float64 // smoothed completions per second, for τ
	tputN   int
	idle    []timed // idle samples' CPU rates
	activeU []timed // active samples' CPU rates
}

// NewCPU returns the CPU path's state.
func NewCPU(cfg CPUConfig) *CPU {
	return &CPU{cfg: cfg, dq: map[string]*ratio{}, shares: map[string]*ratio{}, sPrev: -1, sRefT: math.Inf(-1), lastCapT: math.Inf(-1)}
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

// window returns the index in hist where the current sample's window W
// starts: the most recent samples spanning at least max(the longest lifetime
// among their completions, a_oldest) + Δt (§17.1).
//
// After a limit change the window starts after the change's own sample, whose
// periods at the old limit break the counter test (§17.9, item 3); it always
// holds the current sample (n_W ≥ 1).
func (c *CPU) window(b Interval, dt float64) int {
	aOldest := 0.0
	if b.Oldest >= 0 {
		aOldest = (b.End - b.Oldest).Seconds()
	}
	longest := 0.0
	last := len(c.hist) - 1
	i := last
	for ; i > 0; i-- {
		h := c.hist[i]
		if h.from < c.changeAfter {
			i++
			break
		}
		longest = math.Max(longest, h.longest)
		if (b.End - h.from).Seconds() >= math.Max(longest, aOldest)+dt {
			break
		}
	}
	return min(max(i, 0), last)
}

// kind classifies the sample (§3.2, as amended by §17.8, item 1): capped
// when its window W, judged against the current limit, used the quota less
// one slice of slack per sample and throttled every period but one boundary
// period per sample.
func (c *CPU) kind(s Sample, from int) (Kind, int) {
	b := s.Books
	if b.SlotSeconds == 0 && b.Completions == 0 {
		return Idle, 0
	}
	n := len(c.hist) - from
	if s.Limit <= 0 || !s.Throttling {
		return Active, n
	}
	var u, d float64
	var per, thr uint64
	for _, h := range c.hist[from:] {
		u += h.usage
		d += h.dt
		per += h.periods
		thr += h.throttled
	}
	slack := math.Min(s.Limit*s.Period.Seconds(), float64(s.Procs)*s.Slice.Seconds())
	if per >= 1 && u >= float64(s.Limit*d)-float64(slack*float64(n)) && thr+uint64(n) >= per {
		return Capped, n
	}
	return Active, n
}

// tightSample reports §3.2's per-sample test: every period throttled, usage
// within the slack.
func tightSample(s Sample, used float64) bool {
	if s.Limit <= 0 || !s.Throttling || s.Periods < 1 || s.Throttled < s.Periods {
		return false
	}
	slack := math.Min(s.Limit*s.Period.Seconds(), float64(s.Procs)*s.Slice.Seconds())
	return used*s.Dt.Seconds() >= float64(s.Limit*s.Dt.Seconds())-slack
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

func overlaps(spans []span, l Life) bool {
	for _, b := range spans {
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
	rbar := (1 - rho) / 2
	books := s.Books

	// §17.2: a limit change keeps ĉ and D̂_q, restarts D̂, and its own sample
	// describes neither limit.
	if math.Abs(s.Limit-c.lastL) > 1e-9 {
		res.LimitChanged = true
		if c.lastL != 0 || c.t > dt {
			res.ChangeSample = true
			c.rewarmD(books.End)
			c.changeAfter = books.End
		}
	}
	c.lastL = s.Limit

	tau := c.tau()
	alpha := 1 - fpmath.Exp(-dt/tau)
	over := stall > c.cfg.PressureThreshold
	// §17.9 items 1, 5 and 8: stall the quota doesn't explain, with no
	// throttled period, is contention; with no quota every stall over the
	// threshold is.
	contention := over && (s.Limit == 0 || s.Throttled == 0)

	// The window W and the sample's kind (§17.8, item 1).
	longest := 0.0
	for _, l := range books.Lives {
		longest = math.Max(longest, l.Lifetime())
		c.maxLife = math.Max(c.maxLife, l.Lifetime())
	}
	perQueue := map[string]int{}
	for q, qi := range books.Queues {
		if qi.Completions > 0 {
			perQueue[q] = qi.Completions
		}
	}
	c.hist = append(c.hist, histSample{books.Start, books.End, s.Usage.Seconds(), dt, s.Periods, s.Throttled, books.Completions, perQueue, longest})
	from := c.window(books, dt)
	kind, nW := c.kind(s, from)
	res.Kind, res.WindowSamples = kind, nW
	if kind == Capped {
		c.lastCapT = c.t
	}
	keepFrom := books.End - time.Duration((float64(2*c.maxLife)+float64(3*tau)+float64(2*dt))*float64(time.Second))
	for len(c.hist) > 1 && c.hist[0].to < keepFrom && from > 0 {
		c.hist = c.hist[1:]
		from--
	}

	// §17.10 (D): a contention run begins at a contention sample and ends
	// only once the window W holds none; at its end D̂ is rewarmed (§17.9
	// item 5).
	switch {
	case contention:
		c.runOn, c.lastContEnd = true, books.End
	case c.runOn && c.hist[from].from >= c.lastContEnd:
		c.runOn = false
		c.rewarmD(books.Start)
		res.ContentionEnd, res.DRewarmed = true, true
	}
	inRun := c.runOn
	res.InRun = inRun

	// §17.10 (A): the CPU over the window, which the sign guard reads.
	var uw, dw float64
	for _, h := range c.hist[from:] {
		uw += h.usage
		dw += h.dt
	}
	uW := used
	if dw > 0 {
		uW = uw / dw
	}
	res.UW = uW

	// The samples a qualifying life may not overlap (§17.1, §17.8, §17.10).
	if kind == Capped || inRun || res.ChangeSample {
		c.bad = append(c.bad, span{books.Start, books.End})
	}
	if tightSample(s, used) {
		c.tightAll = append(c.tightAll, span{books.Start, books.End})
		if kind != Capped {
			c.tight = append(c.tight, span{books.Start, books.End})
		}
	}
	for len(c.bad) > 0 && c.bad[0].to < keepFrom {
		c.bad = c.bad[1:]
	}
	for len(c.tight) > 0 && c.tight[0].to < keepFrom {
		c.tight = c.tight[1:]
	}
	for len(c.tightAll) > 0 && c.tightAll[0].to < keepFrom {
		c.tightAll = c.tightAll[1:]
	}

	c.idle = prune(c.idle, c.t-float64(3*tau))
	c.activeU = prune(c.activeU, c.t-float64(3*tau))
	switch kind {
	case Idle:
		c.idle = append(c.idle, timed{c.t, used})
	case Active:
		c.activeU = append(c.activeU, timed{c.t, used})
	}
	// §17.9 item 6: B̂ never rises because samples age out; it is recomputed
	// from the window only with new idle evidence, and may otherwise only fall,
	// to the active-sample bound.
	switch {
	case kind == Idle:
		c.bHat, c.bHatOK = c.background(), true
	case c.bHatOK:
		if ma, ok := minOf(c.activeU); ok {
			c.bHat = math.Max(math.Min(c.bHat, ma), 0)
		}
	}
	bg := 0.0
	if c.bHatOK {
		bg = c.bHat
	}
	res.Background = bg

	if kind != Idle {
		c.tputN++
		al := math.Max(alpha, 1/float64(c.tputN))
		c.tput = float64((1-al)*c.tput) + float64(al*x)
	}

	// §17.8 item 3: a move of the published S of at least max(3σ of its
	// steps, 1 slot) restarts D̂'s warm start, which takes only the reconciles
	// admitted since the move.
	// §17.9 item 2: on a cumulative move, |S − S_ref| > max(3σ of S's steps,
	// 1 slot), S_ref the slots at the last rewarm, or τ ago without one.
	c.sRecent = prune(c.sRecent, c.t-tau)
	if c.t-c.sRefT >= tau {
		c.sRef = prev
		if len(c.sRecent) > 0 {
			c.sRef = c.sRecent[0].v
		}
	}
	if c.sPrev >= 0 && c.sSteps.n >= 3 && math.Abs(prev-c.sRef) > math.Max(changeSigma*math.Sqrt(c.sSteps.v), 1) {
		c.rewarmD(books.Start)
		res.DRewarmed = true
		c.sRef, c.sRefT = prev, c.t
	}
	c.sSteps.moved(prev, alpha)
	c.sPrev = prev
	c.sRecent = append(c.sRecent, timed{c.t, prev})
	if kind == Idle {
		res.Slots = prev
		res.Blocked = inRun
		c.report(&res)
		return res
	}

	// D̂ and D̂_q, from the lives that overlapped no capped, pressured or
	// change sample; D̂'s since its warm start (§17.1, §17.4).
	stale := false
	var sum float64
	var cnt int
	perQ := map[string][2]float64{}
	for _, l := range books.Lives {
		if overlaps(c.bad, l) {
			continue
		}
		// §17.10 (C): D̂_q also excludes lives over a per-sample tight sample.
		if !overlaps(c.tightAll, l) {
			q := perQ[l.Queue]
			q[0] += l.Lifetime()
			q[1]++
			perQ[l.Queue] = q
		}
		if l.Granted >= c.dSince {
			sum += l.Lifetime()
			cnt++
			if overlaps(c.tight, l) {
				res.StretchedLives++
			}
		}
	}
	if cnt > 0 {
		c.d.add(sum, float64(cnt), alpha, false)
		c.dWin.sum += sum
		c.dWin.cnt += cnt
		c.dWin.samples = append(c.dWin.samples, [3]float64{sum, float64(cnt), alpha})
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
	// §17.8 item 5: Little's fallback takes every uncapped sample with work.
	if kind != Capped && books.SlotSeconds > 0 {
		c.little.add(books.SlotSeconds, float64(books.Completions), alpha, false)
	}

	// The D̂ in force: the per-reconcile estimate, else Little's fallback
	// after τ without a qualifying life, used only at a settled S or under a
	// pressure hold (§17.8, item 7(b)).
	var dv float64
	dok := false
	switch {
	case c.dHasLife:
		dv, dok = c.d.value()
	case c.t-c.dSinceT >= tau && (c.settled() || over):
		if v, ok := c.little.value(); ok {
			dv, dok, res.DFallback = v, true, true
		}
	}

	// ĉ, from every sample with work, with τ_ĉ = max(τ, D̂/r̄) from the D̂ in
	// force (§17.1, §17.8 item 5).
	tauC := tau
	if dok {
		tauC = math.Max(tau, dv/rbar)
	}
	alphaC := 1 - fpmath.Exp(-dt/tauC)
	cpuNum := s.Usage.Seconds() - float64(bg*dt)
	den := costCompletions(books, c.cStart)
	c.c.add(cpuNum, float64(den), alphaC, false)
	c.since += den
	if den > 0 {
		c.restartC = false
	}
	res.PaceCompletions = den
	c.windowTest(books, cpuNum, den, alphaC, dt, tauC, &res)
	c.dWindowTest(dt, tauC, &res)
	if res.CRestarted || res.DRestarted {
		res.Restarted = true
	}
	stale = stale || res.DPending

	if books.SlotSeconds > 0 && c.shareShift(books, alpha) {
		c.c.reset()
		c.cStart, c.since, c.restartC = books.End, 0, true
		c.win, c.pend, c.wn = costWindow{from: books.End}, nil, 0
		c.rewarmD(books.End)
		res.Restarted = true
	}

	ch, cok := c.c.value()
	seC := c.c.seRatio()
	res.SECost = seC
	if cok && dok && ch > 0 && dv > 0 {
		seD := c.d.seRatio()
		if res.DFallback {
			seD = c.little.seRatio()
		}
		res.RelSE = fpmath.Hypot(seC/ch, seD/dv)
		res.Trusted = c.since >= trustCompletions && c.c.n >= 3 && c.d.n >= 3 && res.RelSE <= rbar
	}

	limit := s.Limit
	if limit == 0 {
		// §5.1: with no CPU limit, the CPU law doesn't run: S_cpu is the
		// ceiling, reached through §7's pacing, and pressure only blocks.
		res.Slots, res.Paced, res.Blocked = float64(ceiling), true, inRun
		c.fill(&res, ch, cok, dv, dok)
		return res
	}
	g := prev
	switch {
	case res.ChangeSample:
		// §17.2 (L11): the sample's own N̄ scaled by what the new limit
		// allows over what the sample used, its usage counted up to the new
		// limit.
		if d := math.Min(used, limit) - bg; d > 0 {
			g = math.Min(prev, (float64(rho*limit)-bg)*nbar/d)
		}
	case kind == Capped:
		// §17.10 (A): a window-capped sample never rises.
		g = math.Min(prev, c.capped(from, limit, bg, nbar, dv, dok, &res))
	default:
		switch {
		case c.restartC:
			// §17.8 item 5: after a ĉ restart, with CPU but no completion its
			// denominator counts, the active law holds (reported as such, not
			// as unresolvable).
			res.CHold = true
		case !cok || ch <= 0 || (c.c.n >= 3 && ch < 2*seC):
			if c.t-c.lastCapT < tau {
				// §17.9 item 4: not while a sample of the last τ was
				// window-capped; the law holds instead.
				res.UnresHold = true
				break
			}
			res.Unresolvable = true
			if used < rho*limit {
				g = float64(ceiling)
				res.Paced = true
			}
		case !dok:
			res.DHold = true
		default:
			g = (float64(rho*limit) - bg) * dv / ch
		}
	}
	// The sign guard (§4.2), reading the window's CPU (§17.10 (A)).
	switch {
	case uW < rho*limit && g < prev:
		g, res.Guarded = prev, true
	case uW > rho*limit && g > prev:
		g, res.Guarded = prev, true
	}
	// Neither a restart, a deferred sample, a pending ĉ test nor the change's
	// own sample can raise the slots (§3.5, §17.1, §17.2, §17.8 item 2).
	if (res.Restarted || stale || res.CPending || res.ChangeSample) && g > prev {
		g = prev
	}
	if !res.Trusted {
		res.Paced = true
	}
	// §17.9 item 8 and §17.10 (D): every sample of a contention run blocks;
	// under a quota, a run begins only at stall the quota doesn't explain.
	res.Blocked = inRun
	res.Slots = g
	c.fill(&res, ch, cok, dv, dok)
	return res
}

// settled reports that the published S hasn't moved by a slot or more over
// the last τ (§17.8, item 7(b)).
func (c *CPU) settled() bool {
	if len(c.sRecent) == 0 {
		return true
	}
	lo, hi := c.sRecent[0].v, c.sRecent[0].v
	for _, x := range c.sRecent {
		lo, hi = math.Min(lo, x.v), math.Max(hi, x.v)
	}
	return hi-lo < 1
}

// windowTest runs ĉ's change test over consecutive, non-overlapping c_W
// windows (§17.8, item 2). The sums are fed every sample regardless; the test
// decides restarts and when no sample may raise the slots.
func (c *CPU) windowTest(b Interval, num float64, den int, alpha, dt, tauC float64, res *CPUResult) {
	ws := winSample{num: num, alpha: alpha, completion: b.Completions, hasLives: b.Lives != nil}
	longest := 0.0
	for _, l := range b.Lives {
		ws.grants = append(ws.grants, l.Granted)
		longest = math.Max(longest, l.Lifetime())
	}
	if len(c.win.samples) == 0 {
		c.win.from = b.Start
	}
	c.win.samples = append(c.win.samples, ws)
	c.win.cpu += num
	c.win.comp += den
	c.win.longest = math.Max(c.win.longest, longest)
	sd := math.Sqrt(math.Max(c.wv, 0))
	sd = math.Max(sd, 1e-9*math.Max(math.Abs(c.wmu), 1e-9))
	res.SigmaW = sd
	dev := func(x float64) float64 { return (x - c.wmu) / sd }

	aOldest := 0.0
	if b.Oldest >= 0 {
		aOldest = (b.End - b.Oldest).Seconds()
	}
	closed := (b.End - c.win.from).Seconds() >= math.Max(c.win.longest, aOldest)+dt
	if !closed {
		// A window in progress already beyond 3σ, on either side, is pending.
		if x, ok := c.win.value(); ok && c.wn >= 3 && math.Abs(dev(x)) > changeSigma {
			res.CPending = true
		}
		if c.pend != nil {
			res.CPending = true
		}
		return
	}
	res.WindowClosed = true
	res.WindowLen = len(c.win.samples)
	w := c.win
	c.win = costWindow{from: b.End}
	x, ok := w.value()
	if !ok {
		if c.pend != nil {
			res.CPending = true
		}
		return
	}
	aw := 1 - fpmath.Exp(-(b.End-w.from).Seconds()/tauC)
	fold := func(v float64) {
		c.wn++
		a := math.Max(aw, 1/float64(c.wn))
		if c.wn == 1 {
			c.wmu, c.wv = v, 0
			return
		}
		d := v - c.wmu
		c.wmu += float64(a * d)
		c.wv = (1 - a) * (c.wv + float64(a*d*d))
	}
	if c.wn < 3 {
		if p := c.pend; p != nil {
			c.pend = nil
			if pv, ok := p.value(); ok {
				fold(pv)
			}
		}
		fold(x)
		return
	}
	z := dev(x)
	if p := c.pend; p != nil {
		c.pend = nil
		if math.Abs(z) > changeSigma && sign(z) == p.side {
			c.restartFrom(p, &w)
			res.CRestarted = true
			return
		}
		if pv, ok := p.value(); ok {
			fold(pv)
		}
		z = dev(x)
	}
	if math.Abs(z) > changeSigma {
		w.side = sign(z)
		c.pend = &w
		res.CDeferred, res.CPending = true, true
		return
	}
	fold(x)
}

// restartFrom restarts ĉ from the deferred window p and the window w, with the
// edge at p's start: the denominator counts only completions of reconciles
// granted since (§17.1).
func (c *CPU) restartFrom(p, w *costWindow) {
	c.cStart = p.from
	c.c.reset()
	c.since = 0
	c.wn, c.wmu, c.wv = 0, 0, 0
	for _, win := range []*costWindow{p, w} {
		for _, ws := range win.samples {
			d := ws.completion
			if ws.hasLives {
				d = 0
				for _, g := range ws.grants {
					if g >= c.cStart {
						d++
					}
				}
			}
			c.c.fold(ws.num, float64(d), ws.alpha)
			c.since += d
		}
	}
	c.restartC = c.since == 0
	for _, win := range []*costWindow{p, w} {
		if v, ok := win.value(); ok {
			c.wn++
			if c.wn == 1 {
				c.wmu = v
			} else {
				d := v - c.wmu
				c.wmu += float64(d / 2)
				c.wv = (c.wv + float64(d*d/2)) / 2
			}
		}
	}
}

// fill reports the estimates for the metrics.
func (c *CPU) fill(res *CPUResult, ch float64, cok bool, dv float64, dok bool) {
	if cok {
		res.PerCompletion = ch
	}
	if dok {
		res.Duration = dv
		if cok && dv > 0 {
			res.PerSlot = ch / dv
		}
	}
	c.report(res)
}

// report fills the result's throughput.
func (c *CPU) report(res *CPUResult) {
	res.Throughput = c.tput
}

// rewarmD restarts D̂'s warm start at at: it takes only reconciles granted
// since, and the law holds until one has completed (§17.2, §17.4). Its window
// test restarts with it.
func (c *CPU) rewarmD(at time.Duration) {
	c.d.reset()
	c.dSince, c.dSinceT, c.dHasLife = at, c.t, false
	c.dWin, c.dPend, c.dwn, c.dwmu, c.dwv = dWindow{}, nil, 0, 0, 0
}

// dWindowTest runs D̂'s change test on the windows ĉ's test closes (§17.9,
// item 7): each closed window's mean lifetime is one observation; a deferred
// window, or one in progress beyond 3σ, is pending and blocks rises; the
// second beyond 3σ on the same side restarts D̂ from the two windows' lives.
func (c *CPU) dWindowTest(dt, tauC float64, res *CPUResult) {
	sd := math.Sqrt(math.Max(c.dwv, 0))
	sd = math.Max(sd, 1e-9*math.Max(math.Abs(c.dwmu), 1e-9))
	dev := func(x float64) float64 { return (x - c.dwmu) / sd }
	if !res.WindowClosed {
		if x, ok := c.dWin.value(); ok && c.dwn >= 3 && math.Abs(dev(x)) > changeSigma {
			res.DPending = true
		}
		if c.dPend != nil {
			res.DPending = true
		}
		return
	}
	w := c.dWin
	c.dWin = dWindow{}
	x, ok := w.value()
	if !ok {
		if c.dPend != nil {
			res.DPending = true
		}
		return
	}
	aw := 1 - fpmath.Exp(-float64(res.WindowLen)*dt/tauC)
	fold := func(v float64) {
		c.dwn++
		a := math.Max(aw, 1/float64(c.dwn))
		if c.dwn == 1 {
			c.dwmu, c.dwv = v, 0
			return
		}
		d := v - c.dwmu
		c.dwmu += float64(a * d)
		c.dwv = (1 - a) * (c.dwv + float64(a*d*d))
	}
	if c.dwn < 3 {
		if p := c.dPend; p != nil {
			c.dPend = nil
			if pv, ok := p.value(); ok {
				fold(pv)
			}
		}
		fold(x)
		return
	}
	z := dev(x)
	if p := c.dPend; p != nil {
		c.dPend = nil
		if math.Abs(z) > changeSigma && sign(z) == p.side {
			c.d.reset()
			for _, win := range []*dWindow{p, &w} {
				for _, smp := range win.samples {
					c.d.fold(smp[0], smp[1], smp[2])
				}
			}
			c.dwn, c.dwmu, c.dwv = 0, 0, 0
			for _, win := range []*dWindow{p, &w} {
				if v, ok := win.value(); ok {
					fold(v)
				}
			}
			res.DRestarted = true
			return
		}
		if pv, ok := p.value(); ok {
			fold(pv)
		}
		z = dev(x)
	}
	if math.Abs(z) > changeSigma {
		w.side = sign(z)
		c.dPend = &w
		res.DDeferred, res.DPending = true, true
		return
	}
	fold(x)
}

// windowCost is c_W: the CPU per completion over the window W (§17.1).
func (c *CPU) windowCost(from int, bg float64) (float64, bool) {
	var cpu float64
	var comp int
	for _, h := range c.hist[from:] {
		cpu += h.usage - float64(bg*h.dt)
		comp += h.completions
	}
	if comp == 0 {
		return 0, false
	}
	return cpu / float64(comp), true
}

// capped is the law for a capped sample (design §4.2, §17.1):
// min((ρL − B̂)·D_mix/c_W, θN̄). D_mix is weighted by the completions over the
// window W (§17.10 (B)); a queue whose D̂_q has had no qualifying life since
// its start uses the D̂ in force (§17.10 (C)); without either, only θN̄.
func (c *CPU) capped(from int, limit, bg, nbar, dv float64, dok bool, res *CPUResult) float64 {
	rho := c.cfg.Target
	theta := (float64(rho*limit) - bg) / (limit - bg)
	g := theta * nbar
	cw, ok := c.windowCost(from, bg)
	if !ok || cw <= 0 {
		return g
	}
	res.WindowCost = cw
	per := map[string]int{}
	total := 0
	for _, h := range c.hist[from:] {
		for q, n := range h.queues {
			per[q] += n
			total += n
		}
	}
	if total == 0 {
		return g
	}
	queues := make([]string, 0, len(per))
	for q := range per {
		queues = append(queues, q)
	}
	sort.Strings(queues) // a fixed order: a float sum depends on it
	var mix float64
	for _, q := range queues {
		n := per[q]
		var d float64
		r := c.dq[q]
		v, vok := 0.0, false
		if r != nil {
			v, vok = r.value()
		}
		switch {
		case vok:
			// A D̂_q with any qualifying life since its start is the intended
			// memory; D̂ stands in only for a queue that has none (§17.10 (C)).
			d = v
		case dok:
			d = dv
			res.DMixFallback = true
		default:
			return g
		}
		mix += float64(float64(n) * d)
	}
	mix /= float64(total)
	if first := (float64(rho*limit) - bg) * mix / cw; first < g {
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
