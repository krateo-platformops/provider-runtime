// Package sim is the design's §10 simulation: a discrete-event model of a
// container running reconciles, which builds the samples a sampler would read
// and drives the real costmodel code with them. Nothing here reimplements an
// estimator or a law.
//
// The model, at one-millisecond resolution:
//   - CFS bandwidth: a quota refilled at each period's start and handed to the
//     threads' CPUs in slices; a CPU that can't get runtime is throttled until
//     the next period (sched-bwc.rst);
//   - GOMAXPROCS threads running the runnable goroutines, shared equally;
//   - a node that gives the container a share of its CPUs, under a quota or
//     with none;
//   - PSI "some": per millisecond, the most any wanting thread waited;
//   - a gate with the gated queue's semantics: a reconcile starts while in
//     flight is under capacity, a cut preempts nothing, and freed slots go
//     round-robin across queues with work;
//   - a sampler that runs every period, but only once the container can run,
//     so a throttled container delays it, as it does a real one.
package sim

import (
	"math"
	"sort"
	"time"

	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit/internal/costmodel"
)

const tick = time.Millisecond

// DefaultPSIScale calibrates the model's stall to the kernel's cpu.pressure
// "some". At the cap the model's threads wait 87.5% of the time (a 250m quota
// on two threads), but GKE's cgroup reported 56% (the off arm) and 59% (the
// threshold-75 arm), both at 16 steady slots (design §1). The kernel's
// aggregation of per-CPU stall into the cgroup's figure isn't modelled; one
// factor fitted to those steady points stands for it. The measurements at
// fewer slots changed capacity every few seconds, so avg10's lag mixes them,
// and they aren't used. It enters no cut: it decides only which samples the
// pressure block holds (design §10), and with a node share the same factor is
// an assumption, as there is no measurement of that case.
const DefaultPSIScale = 0.62

// Class is one kind of reconcile: CPU, then an off-CPU wait.
type Class struct {
	CPU, Wait time.Duration
	// Jitter is the relative standard deviation of both.
	Jitter float64
}

// Queue is one controller's queue. It has a backlog whenever Active says so;
// Class may change over time.
type Queue struct {
	Name   string
	Class  func(t time.Duration) Class
	Active func(t time.Duration) bool
}

// Config is one simulated run.
type Config struct {
	Seed     uint64
	Duration time.Duration
	// Procs is GOMAXPROCS; NodeCPUs the node's CPUs.
	Procs, NodeCPUs int
	// Limit is the quota in cores, 0 for none; Period and Slice as cpu.max
	// and the CFS slice.
	Limit         float64
	Period, Slice time.Duration
	// LimitAt, when set, overrides Limit over time (a resize).
	LimitAt func(t time.Duration) float64
	// Share is the cores the node gives the container, under a quota or with
	// none; nil means all of NodeCPUs.
	Share func(t time.Duration) float64
	// CostPerSlot, when positive, scales each reconcile's CPU by
	// (1 + CostPerSlot·N), N the reconciles in flight when it starts (design
	// §17.5).
	CostPerSlot float64
	// Controller, when set, builds the controller in place of the cost model
	// (the utilization baseline, design §17.6).
	Controller func() Controller
	// Background is the cores the process's other goroutines want.
	Background func(t time.Duration) float64
	Ceiling    int
	Queues     []Queue
	// SamplePeriod is the sampler's nominal period.
	SamplePeriod time.Duration
	// Model configures costmodel; ignored when Fixed is set.
	Model costmodel.Config
	// Fixed, when positive, holds the capacity there and runs no model.
	Fixed int
	// UsageNoise, when positive, multiplies each sample's usage reading by
	// (1 + UsageNoise·N(0,1)), for §4.3's noise row.
	UsageNoise float64
	// PSIScale scales the stall the model counts into what cpu.pressure
	// reports; 0 means DefaultPSIScale.
	PSIScale float64
	// Observe, when set, sees every sample and its result.
	Observe func(t time.Duration, s costmodel.Sample, r costmodel.Result)
}

// Controller turns samples into a capacity; costmodel.Model is one.
type Controller interface {
	Step(s costmodel.Sample, ceiling int) costmodel.Result
}

type recon struct {
	queue    int
	granted  time.Duration
	cpuLeft  float64 // ms
	waitLeft float64 // ms
}

// Trace is a run's per-sample record.
type Trace struct {
	At       []time.Duration
	Samples  []costmodel.Sample
	Results  []costmodel.Result
	Capacity []int
}

// Used is a sample's CPU in cores.
func (tr *Trace) Used(i int) float64 {
	return tr.Samples[i].Usage.Seconds() / tr.Samples[i].Dt.Seconds()
}

// Run simulates one run and returns its trace.
func Run(cfg Config) *Trace {
	rng := newRNG(cfg.Seed)
	if cfg.Period == 0 {
		cfg.Period = 100 * time.Millisecond
	}
	if cfg.Slice == 0 {
		cfg.Slice = 5 * time.Millisecond
	}
	if cfg.SamplePeriod == 0 {
		cfg.SamplePeriod = time.Second
	}
	if cfg.NodeCPUs == 0 {
		cfg.NodeCPUs = 4
	}
	if cfg.PSIScale == 0 {
		cfg.PSIScale = DefaultPSIScale
	}
	var model Controller = costmodel.NewModel(cfg.Model)
	if cfg.Controller != nil {
		model = cfg.Controller()
	}
	books := costmodel.NewBooks(0)
	capacity := 1
	if cfg.Fixed > 0 {
		capacity = cfg.Fixed
	}
	tr := &Trace{}

	var (
		inflight         []*recon
		perQueue         = make([]int, len(cfg.Queues))
		nextQueue        int
		pool             float64 // ms of runtime left in the period
		local            = make([]float64, cfg.Procs)
		periodDemand     bool
		periodThrottled  bool
		nrPeriods, nrThr uint64
		usage, stall     float64 // ms
		bgLeft           float64 // ms of background work pending
		last             struct {
			at                    time.Duration
			usage, stall          float64
			nrPeriods, nrThrottle uint64
		}
		nextSample = cfg.SamplePeriod
		demand     []float64
		avail      = make([]float64, cfg.Procs)
	)
	limitAt := func(t time.Duration) float64 {
		if cfg.LimitAt != nil {
			return cfg.LimitAt(t)
		}
		return cfg.Limit
	}
	draw := func(mean time.Duration, j float64) float64 {
		ms := float64(mean) / float64(tick)
		if j <= 0 {
			return ms
		}
		return math.Max(0.1*ms, ms*(1+j*rng.normFloat64()))
	}
	admit := func(t time.Duration) bool {
		changed := false
		for len(inflight) < capacity {
			started := false
			for i := range cfg.Queues {
				qi := (nextQueue + i) % len(cfg.Queues)
				q := cfg.Queues[qi]
				if !q.Active(t) {
					continue
				}
				c := q.Class(t)
				cpu := draw(c.CPU, c.Jitter)
				if cfg.CostPerSlot > 0 {
					cpu *= 1 + cfg.CostPerSlot*float64(len(inflight))
				}
				inflight = append(inflight, &recon{queue: qi, granted: t, cpuLeft: cpu, waitLeft: draw(c.Wait, c.Jitter)})
				perQueue[qi]++
				books.Grant(t, q.Name)
				nextQueue = (qi + 1) % len(cfg.Queues)
				started, changed = true, true
				break
			}
			if !started {
				break
			}
		}
		return changed
	}

	steps := int(cfg.Duration / tick)
	for i := 0; i < steps; i++ {
		t := time.Duration(i) * tick
		L := limitAt(t)
		quota := L * float64(cfg.Period) / float64(tick)
		if L > 0 && t%cfg.Period == 0 {
			if periodDemand {
				nrPeriods++
				if periodThrottled {
					nrThr++
				}
			}
			periodDemand, periodThrottled = false, false
			pool = quota
			for j := range local {
				local[j] = 0
			}
		}
		if admit(t) {
			books.Commit(t)
		}

		// Background demand accrues; it is one goroutine.
		if cfg.Background != nil {
			bgLeft += cfg.Background(t)
		}
		// Each runnable goroutine can use at most one millisecond of CPU this
		// millisecond, and no more than its work needs.
		demand := demand[:0]
		for _, r := range inflight {
			if r.cpuLeft > 1e-9 {
				demand = append(demand, math.Min(1, r.cpuLeft))
			}
		}
		if bgLeft > 1e-9 {
			demand = append(demand, math.Min(1, bgLeft))
		}
		runnable := len(demand)
		w := min(cfg.Procs, runnable)
		ran, worst := 0.0, 0.0
		var share float64
		if w > 0 {
			periodDemand = true
			give := float64(min(w, cfg.NodeCPUs))
			if cfg.Share != nil {
				give = math.Min(give, cfg.Share(t))
			}
			rate := give / float64(w)
			// What each wanting thread could run, from the quota's slices.
			capacity := 0.0
			for j := 0; j < w; j++ {
				a := rate
				if L > 0 {
					if local[j] < rate {
						pull := math.Min(float64(cfg.Slice)/float64(tick), pool)
						pool -= pull
						local[j] += pull
					}
					a = math.Min(rate, local[j])
				}
				avail[j] = a
				capacity += a
			}
			// Processor sharing: water-fill the capacity over the demands.
			total := 0.0
			for _, d := range demand {
				total += d
			}
			share = waterFill(demand, capacity)
			ran = math.Min(capacity, total)
			// Charge the slices for what ran, and record throttling where a
			// thread wanted more than its CPU could give.
			want := math.Min(1, total/float64(w))
			for j := 0; j < w; j++ {
				used := 0.0
				if capacity > 0 {
					used = avail[j] * ran / capacity
				}
				if L > 0 {
					local[j] -= used
					if avail[j] < want-1e-12 {
						periodThrottled = true
					}
				}
				worst = math.Max(worst, want-used)
			}
		}
		usage += ran
		stall += cfg.PSIScale * math.Max(0, worst)
		if share > 0 {
			for _, r := range inflight {
				if r.cpuLeft > 1e-9 {
					r.cpuLeft -= math.Min(share, math.Min(1, r.cpuLeft))
				}
			}
			if bgLeft > 1e-9 {
				bgLeft = math.Max(0, bgLeft-math.Min(share, math.Min(1, bgLeft)))
			}
		}
		// Waits progress whatever the CPU does; a reconcile ends after both.
		changed := false
		kept := inflight[:0]
		for _, r := range inflight {
			if r.cpuLeft <= 1e-9 {
				r.waitLeft--
				if r.waitLeft <= 0 {
					perQueue[r.queue]--
					books.Release(t+tick, cfg.Queues[r.queue].Name, true, r.granted)
					changed = true
					continue
				}
			}
			kept = append(kept, r)
		}
		inflight = kept
		end := t + tick
		if changed {
			admit(end)
			books.Commit(end)
		}

		// The sampler runs once the container can: with a quota, not while
		// every wanting thread is throttled.
		if end >= nextSample {
			blocked := L > 0 && w > 0 && ran == 0
			if !blocked {
				s := costmodel.Sample{
					Dt:           end - last.at,
					Usage:        time.Duration((usage - last.usage) * float64(tick)),
					Limit:        L,
					Period:       cfg.Period,
					Throttling:   L > 0,
					Periods:      nrPeriods - last.nrPeriods,
					Throttled:    nrThr - last.nrThrottle,
					Procs:        cfg.Procs,
					Slice:        cfg.Slice,
					StallPresent: true,
					Stall:        time.Duration((stall - last.stall) * float64(tick)),
					Books:        books.Close(end),
				}
				if cfg.UsageNoise > 0 {
					s.Usage = time.Duration(float64(s.Usage) * math.Max(0, 1+cfg.UsageNoise*rng.normFloat64()))
				}
				last.at, last.usage, last.stall = end, usage, stall
				last.nrPeriods, last.nrThrottle = nrPeriods, nrThr
				var res costmodel.Result
				if cfg.Fixed > 0 {
					res = costmodel.Result{Capacity: cfg.Fixed, Slots: float64(cfg.Fixed)}
				} else {
					res = model.Step(s, cfg.Ceiling)
					capacity = res.Capacity
				}
				tr.At = append(tr.At, end)
				tr.Samples = append(tr.Samples, s)
				tr.Results = append(tr.Results, res)
				tr.Capacity = append(tr.Capacity, capacity)
				if cfg.Observe != nil {
					cfg.Observe(end, s, res)
				}
				for nextSample <= end {
					nextSample += cfg.SamplePeriod
				}
				if admit(end) {
					books.Commit(end)
				}
			}
		}
	}
	return tr
}

// waterFill shares capacity over demands, each capped at its demand, and
// returns the level: every demand above it gets the level, the rest all of
// theirs.
func waterFill(demand []float64, capacity float64) float64 {
	if len(demand) == 0 || capacity <= 0 {
		return 0
	}
	sorted := append([]float64(nil), demand...)
	sort.Float64s(sorted)
	left := capacity
	for i, d := range sorted {
		level := left / float64(len(sorted)-i)
		if d >= level {
			return level
		}
		left -= d
	}
	return sorted[len(sorted)-1]
}
