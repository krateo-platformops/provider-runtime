package ratelimit

import (
	"bufio"
	"bytes"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"k8s.io/utils/ptr"

	"github.com/krateo-platformops/provider-runtime/pkg/errors"
	"github.com/krateo-platformops/provider-runtime/pkg/logging"
)

// Saturation defaults (design §7), decided by the user on 2026-11-05 from the
// kind measurement in docs/configuration.md.
//
// The CPU pressure threshold is PROVISIONAL: the measurement supports 50, and
// core-provider is validating it. The working-set ratios belong to the
// experimental memory input, which is off by default: re-measured, that input
// did not prevent OOMKills (design §11).
const (
	// DefaultSamplePeriod is how often the sampler reads the cgroup files. CPU
	// pressure gains little from a faster sample, because it is measured over
	// CPUPressureWindow, and a faster sample costs CPU in the container it
	// protects. A shorter period only matters to the experimental memory
	// input.
	DefaultSamplePeriod = time.Second
	// DefaultStartupRampInterval is how long local capacity waits between
	// steps of +1, from the start and after a CPU cut, so 16 workers are all
	// admitted after about 30s. It is not tied to the sample period.
	DefaultStartupRampInterval = 2 * time.Second
	// DefaultCPUPressureThreshold is the CPU pressure percentage above which
	// the controller counts as CPU-saturated: the share of the last
	// CPUPressureWindow in which some task of the container waited for CPU.
	// PROVISIONAL.
	DefaultCPUPressureThreshold = 50.0
	// CPUPressureWindow is the time CPU pressure is measured over, from the
	// change of cpu.pressure's cumulative "some total" counter, and the least
	// time between two CPU cuts. A cut needs a whole window of evidence, and
	// the next one a whole window measured after it, so no cut is made twice
	// on the same stall. The kernel's own "avg10" isn't used: it still holds
	// most of a stall 10s after it ended.
	CPUPressureWindow = 10 * time.Second
	// DefaultWorkingSetSoftRatio is the working-set share of memory.max at
	// which the experimental memory input halves local capacity. PROVISIONAL.
	DefaultWorkingSetSoftRatio = 0.80
	// DefaultWorkingSetHardRatio is the working-set share of memory.max at
	// which the experimental memory input drops local capacity straight to 1.
	// PROVISIONAL.
	DefaultWorkingSetHardRatio = 0.90
)

// Saturation inputs, as named in the metrics.
const (
	InputCPUPressure      = "cpu_pressure"
	InputMemoryWorkingSet = "memory_working_set"
)

// allInputs are the inputs the sampler reads. memory.pressure is not one of
// them: on the kind measurement it read 0.00 up to every OOMKill, because a Go
// controller's anonymous memory, with no swap, gives the kernel nothing to
// reclaim and so nothing to stall on (design §11).
var allInputs = []string{InputCPUPressure, InputMemoryWorkingSet}

const (
	defaultCgroupMount = "/sys/fs/cgroup"
	procSelfDir        = "/proc/self"
)

// SaturationOptions configures the local input: the controller's own CPU
// saturation, read from its container's cgroup v2 files. With it enabled, every
// gated call (Connect and each ExternalClient method) needs a slot in the
// process-wide local capacity, whether or not the provider implements a
// RateLimitKeyer. Local capacity starts at 1 and rises by one per clean
// StartupRampInterval, up to the ceiling. CPU pressure over its threshold for
// a whole CPUPressureWindow halves it, at most once per window. After a cut by
// the experimental memory input it rises by one per clean RecoveryInterval.
//
// Memory is not part of this unless ExperimentalMemoryWorkingSet is set.
//
// With CapacityModel set, and a CPU limit of MinCPULimit or more, local
// capacity is also held at what the container's measured CPU use allows
// (docs/design-capacity-model.md): it is the lower of the two.
//
// Each controller's queue has a guaranteed share of the local capacity, which
// it lends while it doesn't use it.
type SaturationOptions struct {
	// Enabled turns the local input on, independently of the rate-limit input.
	// It reads CPU pressure only.
	Enabled bool

	// ExperimentalMemoryWorkingSet adds the memory working set to the local
	// input. EXPERIMENTAL, off by default, and not memory protection: on the
	// kind re-measurement it did not prevent OOMKills, in 0 of 19 runs on an
	// overcommitted workload (docs/configuration.md). The calls admitted
	// between two samples, and the single call the floor always lets through,
	// can still cross the limit.
	ExperimentalMemoryWorkingSet bool

	// CgroupDir is the cgroup v2 directory to read. Empty means the process's
	// own cgroup: /sys/fs/cgroup joined with the path in /proc/self/cgroup
	// when that directory exists, else /sys/fs/cgroup (which, with a private
	// cgroup namespace, as Kubernetes gives containers on cgroup v2, is the
	// container's own).
	CgroupDir string

	// SamplePeriod is how often the files are read. Defaults to 1s. A shorter
	// period only matters to the experimental memory input: CPU pressure is
	// measured over CPUPressureWindow, so sampling it faster gains little and
	// costs CPU in the container it protects.
	SamplePeriod time.Duration

	// StartupRampInterval is the time between steps of +1, from the start and
	// after a CPU cut. Defaults to 2s. A step happens on the first sample after
	// the interval has passed, so a SamplePeriod above it sets the pace.
	StartupRampInterval time.Duration

	// CPUPressureThreshold is the CPU pressure percentage (0-100) above which
	// the controller is saturated: the share of the last CPUPressureWindow in
	// which some task waited for CPU, from cpu.pressure's "some total".
	// Defaults to DefaultCPUPressureThreshold (provisional).
	CPUPressureThreshold float64

	// WorkingSetSoftRatio is the share of memory.max (0-1) at or above which
	// the working set (memory.current minus inactive_file) halves local
	// capacity. Read only with ExperimentalMemoryWorkingSet. Defaults to 0.80
	// (provisional).
	WorkingSetSoftRatio float64

	// WorkingSetHardRatio is the share of memory.max (0-1) at or above which
	// local capacity drops straight to 1. Read only with
	// ExperimentalMemoryWorkingSet. Defaults to 0.90 (provisional).
	WorkingSetHardRatio float64

	// CapacityModel turns on the capacity model: every ModelInterval, local
	// capacity is sized from the CPU the container used and the reconciles
	// that were in flight, so that CPU use settles at CPUTarget of the limit.
	// The CPU pressure step stays, and may only lower what the model allows.
	// Off by default. It needs Enabled, and is ignored without it. The model
	// runs only while cpu.max gives a CPU limit of at least MinCPULimit and
	// cpu.stat is readable; otherwise the step alone governs.
	CapacityModel bool

	// CPUTarget is the share of the CPU limit the capacity model aims at, in
	// (0, 1]. Defaults to 0.9. Read only with CapacityModel.
	CPUTarget float64

	// MinCPULimit is the CPU limit, in cores, under which the capacity model
	// doesn't run. Defaults to 0.5. Read only with CapacityModel.
	MinCPULimit float64

	// ModelInterval is how often the capacity model recomputes its capacity.
	// Defaults to 10s. A recomputation happens on the first sample after the
	// interval has passed, so a SamplePeriod above it sets the pace. Read only
	// with CapacityModel.
	ModelInterval time.Duration
}

func (o SaturationOptions) withDefaults() (SaturationOptions, error) {
	if o.SamplePeriod == 0 {
		o.SamplePeriod = DefaultSamplePeriod
	}
	if o.StartupRampInterval == 0 {
		o.StartupRampInterval = DefaultStartupRampInterval
	}
	if o.CPUPressureThreshold == 0 {
		o.CPUPressureThreshold = DefaultCPUPressureThreshold
	}
	if o.WorkingSetSoftRatio == 0 {
		o.WorkingSetSoftRatio = DefaultWorkingSetSoftRatio
	}
	if o.WorkingSetHardRatio == 0 {
		o.WorkingSetHardRatio = DefaultWorkingSetHardRatio
	}
	if o.CPUTarget == 0 {
		o.CPUTarget = DefaultCPUTarget
	}
	if o.MinCPULimit == 0 {
		o.MinCPULimit = DefaultMinCPULimit
	}
	if o.ModelInterval == 0 {
		o.ModelInterval = DefaultModelInterval
	}
	switch {
	case o.SamplePeriod < 0:
		return o, errors.Errorf("ratelimit: saturation SamplePeriod must be positive, got %s", o.SamplePeriod)
	case o.StartupRampInterval < 0:
		return o, errors.Errorf("ratelimit: saturation StartupRampInterval must be positive, got %s", o.StartupRampInterval)
	case o.CPUPressureThreshold < 0 || o.CPUPressureThreshold > 100:
		return o, errors.Errorf("ratelimit: saturation CPUPressureThreshold must be a percentage in (0, 100], got %v", o.CPUPressureThreshold)
	case o.WorkingSetSoftRatio < 0 || o.WorkingSetHardRatio > 1 || o.WorkingSetSoftRatio > o.WorkingSetHardRatio:
		return o, errors.Errorf("ratelimit: saturation working-set ratios must satisfy 0 < soft (%v) <= hard (%v) <= 1",
			o.WorkingSetSoftRatio, o.WorkingSetHardRatio)
	case math.IsNaN(o.CPUTarget) || o.CPUTarget < 0 || o.CPUTarget > 1:
		return o, errors.Errorf("ratelimit: saturation CPUTarget must be a share in (0, 1], got %v", o.CPUTarget)
	case math.IsNaN(o.MinCPULimit) || o.MinCPULimit < 0:
		return o, errors.Errorf("ratelimit: saturation MinCPULimit must be positive, got %v cores", o.MinCPULimit)
	case o.ModelInterval < 0:
		return o, errors.Errorf("ratelimit: saturation ModelInterval must be positive, got %s", o.ModelInterval)
	}
	return o, nil
}

// reading is one input's value in one sample.
type reading struct {
	enabled bool
	// percent is the value, meaningful only when enabled.
	percent float64
	// disabledBecause says why the input is off, when it is.
	disabledBecause string
}

func disabled(why string) reading { return reading{disabledBecause: why} }

// cgroupSample is one read of the cgroup files.
type cgroupSample struct {
	// cpuPressure's percent is the stall share over the window, which
	// saturation.read fills in from cpuStall.
	cpuPressure reading
	workingSet  reading
	// cpuStall is cpu.pressure's cumulative "some total", as the reader found
	// it. cpuConfirmed reports that cpuPressure's percent covers a whole
	// CPUPressureWindow.
	cpuStall     time.Duration
	cpuConfirmed bool
	// cpuLimit is the CPU limit cpu.max gives, and cpuUsage cpu.stat's
	// cumulative usage_usec. cpuUsage is read only for the capacity model.
	cpuLimit cpuLimit
	cpuUsage cpuUsage
}

// cpuLimit is the container's CPU limit in one sample.
type cpuLimit struct {
	// set is false when there is no limit, or cpu.max can't be read; why then
	// says which.
	set   bool
	cores float64
	why   string
}

// cpuUsage is the container's cumulative CPU usage in one sample.
type cpuUsage struct {
	// ok is false when cpu.stat wasn't read, or can't be; why then says which.
	ok    bool
	total time.Duration
	why   string
}

// stallPoint is cpu.pressure's cumulative stall at one sample.
type stallPoint struct {
	at    time.Time
	total time.Duration
}

func (s cgroupSample) byInput(input string) reading {
	switch input {
	case InputCPUPressure:
		return s.cpuPressure
	default:
		return s.workingSet
	}
}

// cgroupReader reads one sample. It is the only part of the saturation input
// that does I/O, and only the sampler goroutine calls it.
type cgroupReader interface {
	read() cgroupSample
}

type saturation struct {
	opts   SaturationOptions
	dir    string
	reader cgroupReader
	log    logging.Logger

	// last is the latest sample, for metrics.
	last atomic.Pointer[cgroupSample]

	// Owned by the sampler goroutine.
	loggedDisabled map[string]bool
	// lastStep is when capacity last changed, or a sample was last saturated.
	// It is zero until the first sample.
	lastStep time.Time
	// stall is the cumulative CPU stall at the samples of the last
	// CPUPressureWindow, oldest first; its first point is the window's start.
	stall []stallPoint
	// lastCPUCut is when CPU pressure last cut capacity; zero until it has.
	lastCPUCut time.Time
	// memoryCut reports that the last cut was the experimental memory
	// input's: recovery is then one step per RecoveryInterval.
	memoryCut bool
	// stepAbove is how far the step's own capacity P is above the published
	// local capacity. It is 0 unless the capacity model holds local under P:
	// with the model off, or not binding, P is the published capacity itself.
	stepAbove int
	// model is the capacity model; nil unless CapacityModel is set.
	model *capacityModel
	// modelApplies and modelWhy are what was last logged about whether the
	// law runs; modelLogged is false until something was.
	modelApplies, modelLogged bool
	modelWhy                  string

	// cpuCuts and memoryCuts count the cuts by the step that lowered local
	// capacity, by input. modelReport is the model's state while the law
	// applies, nil otherwise. They are for metrics, read by any goroutine.
	cpuCuts, memoryCuts atomic.Int64
	modelReport         atomic.Pointer[modelReport]
}

func newSaturation(o SaturationOptions, log logging.Logger) (*saturation, error) {
	o, err := o.withDefaults()
	if err != nil {
		return nil, err
	}
	dir := o.CgroupDir
	if dir == "" {
		dir = ownCgroupDir(procSelfDir, defaultCgroupMount)
	}
	s := &saturation{
		opts:           o,
		dir:            dir,
		reader:         cgroupFiles{dir: dir, memory: o.ExperimentalMemoryWorkingSet, usage: o.CapacityModel},
		log:            log,
		loggedDisabled: map[string]bool{},
	}
	if o.CapacityModel {
		s.model = newCapacityModel(o.CPUTarget, o.ModelInterval)
	}
	return s, nil
}

// read takes one sample from the cgroup files at now, turns the cumulative
// CPU stall into its share of the window, publishes the sample for metrics,
// and logs each input the first time it is found disabled.
func (s *saturation) read(now time.Time) cgroupSample {
	smp := s.reader.read()
	if smp.cpuPressure.enabled {
		smp.cpuPressure.percent, smp.cpuConfirmed = s.stallShare(now, smp.cpuStall)
	} else {
		s.stall = nil
	}
	s.last.Store(&smp)
	s.logDisabledOnce(smp)
	return smp
}

// stallShare records the cumulative CPU stall read at now and returns the
// percentage of time stalled since the window's start, and whether that span
// is a whole CPUPressureWindow. The window's start is the latest sample at
// least a window old, or the first sample while there is none. A counter that
// went backwards belongs to another cgroup, and starts the window again.
func (s *saturation) stallShare(now time.Time, total time.Duration) (percent float64, confirmed bool) {
	if n := len(s.stall); n > 0 && total < s.stall[n-1].total {
		s.stall = nil
	}
	if n := len(s.stall); n == 0 || now.After(s.stall[n-1].at) {
		s.stall = append(s.stall, stallPoint{at: now, total: total})
	}
	for len(s.stall) > 1 && now.Sub(s.stall[1].at) >= CPUPressureWindow {
		s.stall = s.stall[1:]
	}
	start := s.stall[0]
	span := now.Sub(start.at)
	if span <= 0 {
		return 0, false
	}
	return min(100, 100*float64(total-start.total)/float64(span)), span >= CPUPressureWindow
}

// advance returns the local state that follows cur after sample smp, taken at
// now, and the input whose cut lowered it, if one did.
//
// The step keeps its own capacity P (see step), the capacity model its
// capacity M (see capacityModel), and
//
//	local = min(P, floor(M)), at least 1
//
// so the step may only lower what the model allows. While the law doesn't
// apply, M is the ceiling and local is P: the step alone governs. A cut by the
// step also pulls M down to the local capacity it left.
func (s *saturation) advance(now time.Time, smp cgroupSample, cur localState, ceiling int, recovery time.Duration, work workTotals) (next localState, cutBy string) {
	p, cause, cutBy := s.step(now, smp, cur.capacity, cur.capacity+s.stepAbove, ceiling, recovery)
	next = localState{capacity: p, cause: cause}
	if s.observeModel(now, smp, cur.capacity, ceiling, work) {
		next.capacity = max(capacityFloor, min(p, int(math.Floor(s.model.current(ceiling)))))
		if cutBy != "" {
			s.model.cutTo(next.capacity, ceiling)
		}
		r := s.model.reported(ceiling)
		s.modelReport.Store(&r)
	}
	s.stepAbove = p - next.capacity
	if next.capacity >= cur.capacity {
		return next, ""
	}
	switch cutBy {
	case InputCPUPressure:
		s.cpuCuts.Add(1)
	case InputMemoryWorkingSet:
		s.memoryCuts.Add(1)
	}
	return next, cutBy
}

// observeModel feeds the sample to the capacity model when the law applies
// (design §5), and reports whether it does: CapacityModel is set, cpu.max gives
// a limit of at least MinCPULimit, and cpu.stat is readable. When it doesn't,
// the model forgets what it knew, so M is the ceiling. Each change of whether
// it applies, or of why not, is logged once.
func (s *saturation) observeModel(now time.Time, smp cgroupSample, inForce, ceiling int, work workTotals) bool {
	if s.model == nil {
		return false
	}
	var why string
	switch {
	case !smp.cpuLimit.set:
		why = smp.cpuLimit.why
	case smp.cpuLimit.cores < s.opts.MinCPULimit:
		why = "the CPU limit of " + formatCores(smp.cpuLimit.cores) + " cores is under MinCPULimit, " + formatCores(s.opts.MinCPULimit)
	case !smp.cpuUsage.ok:
		why = smp.cpuUsage.why
	}
	applies := why == ""
	if !s.modelLogged || applies != s.modelApplies || why != s.modelWhy {
		s.modelLogged, s.modelApplies, s.modelWhy = true, applies, why
		if applies {
			s.log.Info("Capacity model applies: local capacity is the lower of the model's and the CPU pressure step's",
				"cpuLimit", formatCores(smp.cpuLimit.cores), "cpuTarget", s.opts.CPUTarget, "cgroup", s.dir)
		} else {
			s.log.Info("Capacity model does not apply: the CPU pressure step alone governs", "reason", why, "cgroup", s.dir)
		}
	}
	if !applies {
		s.model.reset()
		s.modelReport.Store(nil)
		return false
	}
	before := s.model.current(ceiling)
	s.model.observe(now, smp.cpuUsage.total, smp.cpuLimit.cores, inForce, work, ceiling)
	if after := s.model.current(ceiling); after != before {
		s.log.Debug("Capacity model moved its capacity", "from", before, "to", after,
			"cpuUsed", s.model.report.used, "cpuBackground", s.model.report.background, "cpuLimit", smp.cpuLimit.cores)
	}
	return true
}

func formatCores(c float64) string {
	return strconv.FormatFloat(c, 'f', -1, 64)
}

// step returns the step's capacity P that follows p after sample smp, taken at
// now, with local the published local capacity; cause, which names what made
// the sample saturated; and cutBy, the input that cut, if one did. The
// sampler's first call only starts the clock.
//
//   - Working set at or above the hard ratio (experimental memory input): a cut,
//     capacity 1 at once.
//   - Working set at or above the soft ratio: a cut, capacity halves, minimum 1,
//     once per sample.
//   - CPU pressure over its threshold: capacity halves, minimum 1, if the
//     pressure covers a whole CPUPressureWindow and the last CPU cut is at
//     least a window old. Otherwise capacity holds: the stall isn't confirmed
//     yet, or it is the one the last cut already answered.
//   - A clean sample: capacity +1, up to ceiling, once enough time has passed
//     since the last step or saturated sample. That time is
//     StartupRampInterval, from the start and after a CPU cut, so a restarted
//     controller eases into its backlog. After a cut by the memory input it is
//     the RecoveryInterval, the same rule as for API limits.
//
// What halves is the published local capacity, not P, so a cut is a cut of
// what is running. The two differ only while the capacity model holds local
// under P.
//
// cause is empty for a clean sample. A saturated sample restarts the clean
// interval even when it doesn't cut.
func (s *saturation) step(now time.Time, smp cgroupSample, local, p, ceiling int, recovery time.Duration) (next int, cause, cutBy string) {
	if s.lastStep.IsZero() {
		s.lastStep = now
	}
	hard, memory, cpu, cause := s.evaluate(smp)
	next = p
	switch {
	case hard:
		next, cutBy = capacityFloor, InputMemoryWorkingSet
		s.memoryCut, s.lastStep = true, now
	case memory:
		next, cutBy = max(capacityFloor, local/2), InputMemoryWorkingSet
		s.memoryCut, s.lastStep = true, now
	case cpu:
		s.lastStep = now
		if smp.cpuConfirmed && (s.lastCPUCut.IsZero() || now.Sub(s.lastCPUCut) >= CPUPressureWindow) {
			next, cutBy = max(capacityFloor, local/2), InputCPUPressure
			s.memoryCut, s.lastCPUCut = false, now
		}
	default:
		wait := s.opts.StartupRampInterval
		if s.memoryCut {
			wait = recovery
		}
		if p < ceiling && now.Sub(s.lastStep) >= wait {
			next = p + 1
			s.lastStep = now
		}
	}
	return next, cause, cutBy
}

// evaluate reports whether smp is over the hard working-set ratio, over the
// soft one, over the CPU pressure threshold, and which inputs made it so.
func (s *saturation) evaluate(smp cgroupSample) (hard, memory, cpu bool, cause string) {
	var causes []string
	if r := smp.cpuPressure; r.enabled && r.percent > s.opts.CPUPressureThreshold {
		cpu = true
		causes = append(causes, "CPU pressure "+formatPercent(r.percent)+" (some, over "+CPUPressureWindow.String()+")")
	}
	if r := smp.workingSet; r.enabled && r.percent >= s.opts.WorkingSetSoftRatio*100 {
		memory = true
		causes = append(causes, "memory working set "+formatPercent(r.percent)+" of limit")
		hard = r.percent >= s.opts.WorkingSetHardRatio*100
	}
	return hard, memory, cpu, strings.Join(causes, ", ")
}

func formatPercent(p float64) string {
	return strconv.FormatFloat(p, 'f', 0, 64) + "%"
}

// logDisabledOnce logs each input the first time it is found disabled, at
// Info: a missing input is how the node was built, not an error.
func (s *saturation) logDisabledOnce(smp cgroupSample) {
	for _, in := range allInputs {
		r := smp.byInput(in)
		if r.enabled || s.loggedDisabled[in] {
			continue
		}
		s.loggedDisabled[in] = true
		s.log.Info("Saturation input disabled; the other inputs still apply", "input", in, "reason", r.disabledBecause, "cgroup", s.dir)
	}
}

// inputs returns each input's last sample. With saturation off, or before the
// first sample, every input reports as disabled. s may be nil.
func (s *saturation) inputs() []InputSnapshot {
	out := make([]InputSnapshot, 0, len(allInputs))
	var smp *cgroupSample
	if s != nil {
		smp = s.last.Load()
	}
	for _, in := range allInputs {
		is := InputSnapshot{Input: in}
		if smp != nil {
			if r := smp.byInput(in); r.enabled {
				is.Enabled, is.Percent = true, r.percent
			}
		}
		out = append(out, is)
	}
	return out
}

// cuts returns the cuts each input's step made, for metrics.
func (s *saturation) cuts() []CutSnapshot {
	return []CutSnapshot{
		{Input: InputCPUPressure, Cuts: s.cpuCuts.Load()},
		{Input: InputMemoryWorkingSet, Cuts: s.memoryCuts.Load()},
	}
}

// cpu returns the CPU limit of the last sample and, while the law applies,
// what the capacity model measured, for metrics.
func (s *saturation) cpu() CPUSnapshot {
	var out CPUSnapshot
	if smp := s.last.Load(); smp != nil && smp.cpuLimit.set {
		out.Limit = ptr.To(smp.cpuLimit.cores)
	}
	r := s.modelReport.Load()
	if r == nil {
		return out
	}
	out.ModelCapacity = ptr.To(r.capacity)
	if r.measured {
		out.Used, out.Background = ptr.To(r.used), ptr.To(r.background)
	}
	if r.hasPerReconcile {
		out.PerReconcile, out.Required = ptr.To(r.perReconcile), ptr.To(r.required)
	}
	return out
}

// cgroupFiles reads the cgroup v2 interface files in dir. The memory files are
// read only when memory is set: the experimental input is off by default, and
// reading memory.stat is most of a sample's cost. cpu.stat is read only when
// usage is set, for the capacity model. cpu.max is read at every sample: an
// in-place resize changes the limit under a running process.
type cgroupFiles struct {
	dir    string
	memory bool
	usage  bool
}

const capacityModelOff = "the capacity model is off (SaturationOptions.CapacityModel)"

const memoryInputOff = "the experimental memory input is off (SaturationOptions.ExperimentalMemoryWorkingSet)"

func (c cgroupFiles) read() cgroupSample {
	root, err := os.OpenRoot(c.dir)
	if err != nil {
		why := "no cgroup v2 directory at " + c.dir + ": " + err.Error()
		return cgroupSample{cpuPressure: disabled(why), workingSet: disabled(why), cpuLimit: cpuLimit{why: why}, cpuUsage: cpuUsage{why: why}}
	}
	defer func() { _ = root.Close() }()

	smp := cgroupSample{workingSet: disabled(memoryInputOff), cpuUsage: cpuUsage{why: capacityModelOff}}
	smp.cpuPressure, smp.cpuStall = readPressure(root, "cpu.pressure", "some")
	if c.memory {
		smp.workingSet = readWorkingSet(root)
	}
	smp.cpuLimit = readCPULimit(root)
	if c.usage {
		smp.cpuUsage = readCPUUsage(root)
	}
	return smp
}

// readCPULimit returns the CPU limit of cpu.max, quota ÷ period, in cores:
//
//	50000 100000
//
// A quota of "max" is no limit.
func readCPULimit(root *os.Root) cpuLimit {
	b, err := root.ReadFile("cpu.max")
	if err != nil {
		return cpuLimit{why: unreadableWhy("cpu.max", err)}
	}
	text := strings.TrimSpace(string(b))
	fields := strings.Fields(text)
	if len(fields) != 2 {
		return cpuLimit{why: "cpu.max: cannot parse " + strconv.Quote(text)}
	}
	if fields[0] == "max" {
		return cpuLimit{why: "no CPU limit (cpu.max is max)"}
	}
	quota, qerr := strconv.ParseUint(fields[0], 10, 64)
	period, perr := strconv.ParseUint(fields[1], 10, 64)
	if qerr != nil || perr != nil || quota == 0 || period == 0 {
		return cpuLimit{why: "cpu.max: cannot parse " + strconv.Quote(text)}
	}
	return cpuLimit{set: true, cores: float64(quota) / float64(period)}
}

// readCPUUsage returns usage_usec of cpu.stat: the CPU time the container has
// used, cumulative.
func readCPUUsage(root *os.Root) cpuUsage {
	b, err := root.ReadFile("cpu.stat")
	if err != nil {
		return cpuUsage{why: unreadableWhy("cpu.stat", err)}
	}
	us, ok := statValue(b, "usage_usec")
	if !ok || us > uint64(maxStall/time.Microsecond) {
		return cpuUsage{why: "cpu.stat: no usage_usec value"}
	}
	return cpuUsage{ok: true, total: time.Duration(us) * time.Microsecond}
}

func unreadableWhy(file string, err error) string {
	if errors.Is(err, fs.ErrNotExist) {
		return file + " not found"
	}
	return file + ": " + err.Error()
}

// readPressure returns total, the cumulative stall in microseconds, of the
// given line ("some" or "full") of a PSI file, such as cpu.pressure:
//
//	some avg10=1.53 avg60=0.87 avg300=0.24 total=1234567
//	full avg10=0.00 avg60=0.00 avg300=0.00 total=0
//
// The reading is enabled with no percent: the caller derives it from the
// change in the total.
func readPressure(root *os.Root, file, line string) (reading, time.Duration) {
	b, err := root.ReadFile(file)
	if err != nil {
		return disabled(missingWhy(file, err)), 0
	}
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 || fields[0] != line {
			continue
		}
		for _, f := range fields[1:] {
			v, ok := strings.CutPrefix(f, "total=")
			if !ok {
				continue
			}
			us, err := strconv.ParseInt(v, 10, 64)
			if err != nil || us < 0 || us > int64(maxStall/time.Microsecond) {
				return disabled(file + ": cannot parse " + strconv.Quote(f)), 0
			}
			return reading{enabled: true}, time.Duration(us) * time.Microsecond
		}
	}
	return disabled(file + ": no " + strconv.Quote(line) + " total value"), 0
}

// maxStall is the largest cumulative stall a time.Duration holds, about 292
// years.
const maxStall = time.Duration(1<<63 - 1)

// readWorkingSet returns memory.current minus inactive_file (from memory.stat)
// as a percentage of memory.max: the working set kubelet evicts on and the OOM
// killer acts on. Page cache the kernel can drop is excluded, so a large but
// reclaimable cache doesn't count.
func readWorkingSet(root *os.Root) reading {
	limitRaw, err := root.ReadFile("memory.max")
	if err != nil {
		return disabled(missingWhy("memory.max", err))
	}
	limitText := strings.TrimSpace(string(limitRaw))
	if limitText == "max" {
		return disabled("no memory limit (memory.max is max)")
	}
	limit, err := strconv.ParseUint(limitText, 10, 64)
	if err != nil || limit == 0 {
		return disabled("memory.max: cannot parse " + strconv.Quote(limitText))
	}

	currentRaw, err := root.ReadFile("memory.current")
	if err != nil {
		return disabled(missingWhy("memory.current", err))
	}
	current, err := strconv.ParseUint(strings.TrimSpace(string(currentRaw)), 10, 64)
	if err != nil {
		return disabled("memory.current: cannot parse " + strconv.Quote(strings.TrimSpace(string(currentRaw))))
	}

	stat, err := root.ReadFile("memory.stat")
	if err != nil {
		return disabled(missingWhy("memory.stat", err))
	}
	inactive, ok := statValue(stat, "inactive_file")
	if !ok {
		return disabled("memory.stat: no inactive_file value")
	}

	ws := uint64(0)
	if current > inactive {
		ws = current - inactive
	}
	return reading{enabled: true, percent: float64(ws) / float64(limit) * 100}
}

func statValue(stat []byte, key string) (uint64, bool) {
	sc := bufio.NewScanner(bytes.NewReader(stat))
	for sc.Scan() {
		k, v, ok := strings.Cut(sc.Text(), " ")
		if !ok || k != key {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
		return n, err == nil
	}
	return 0, false
}

func missingWhy(file string, err error) string {
	if errors.Is(err, fs.ErrNotExist) {
		return file + " not found (no cgroup v2 or no PSI on this kernel)"
	}
	return file + ": " + err.Error()
}

// ownCgroupDir finds the process's own cgroup v2 directory: the "0::<path>"
// line of <proc>/cgroup under mount, when that directory exists, else mount.
//
// The path comes from a file's contents, so it is looked up through an
// os.Root opened at mount: it can name a directory under the cgroup mount and
// nothing outside it, whatever "..", absolute or symlinked components it holds.
func ownCgroupDir(proc, mount string) string {
	procRoot, err := os.OpenRoot(proc)
	if err != nil {
		return mount
	}
	defer func() { _ = procRoot.Close() }()
	b, err := procRoot.ReadFile("cgroup")
	if err != nil {
		return mount
	}

	var rel string
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		if p, ok := strings.CutPrefix(sc.Text(), "0::"); ok {
			rel = strings.TrimPrefix(filepath.Clean("/"+p), "/")
			break
		}
	}
	if rel == "" {
		return mount
	}

	mountRoot, err := os.OpenRoot(mount)
	if err != nil {
		return mount
	}
	defer func() { _ = mountRoot.Close() }()
	if fi, err := mountRoot.Stat(rel); err != nil || !fi.IsDir() {
		return mount
	}
	return filepath.Join(mount, rel)
}
