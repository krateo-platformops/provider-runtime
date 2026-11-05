package ratelimit

import (
	"bufio"
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/krateo-platformops/provider-runtime/pkg/errors"
	"github.com/krateo-platformops/provider-runtime/pkg/logging"
)

// Saturation defaults (design §7). Decided by the user on 2026-11-05 from the
// kind measurement in docs/configuration.md.
//
// The CPU pressure threshold is PROVISIONAL: the measurement supports 50, and
// core-provider is validating it. The working-set ratios and the sample
// period are decided: with 16 MiB calls against a 256Mi limit, 0.90/0.95 at
// 1s let calls admitted between two samples cross the limit; 0.80/0.90 at
// 250ms leave room for several such calls and see the growth four times as
// often.
const (
	// DefaultSamplePeriod is how often the sampler reads the cgroup files.
	DefaultSamplePeriod = 250 * time.Millisecond
	// DefaultCPUPressureThreshold is the cpu.pressure "some avg10" percentage
	// above which the controller counts as CPU-saturated. PROVISIONAL.
	DefaultCPUPressureThreshold = 50.0
	// DefaultWorkingSetSoftRatio is the working-set share of memory.max at
	// which local capacity halves.
	DefaultWorkingSetSoftRatio = 0.80
	// DefaultWorkingSetHardRatio is the working-set share of memory.max at
	// which local capacity drops straight to 1.
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

// SaturationOptions configures the local input: the controller's own CPU and
// memory saturation, read from its container's cgroup v2 files. With it
// enabled, every gated call (Connect and each ExternalClient method) needs a
// slot in the process-wide local capacity, whether or not the provider
// implements a RateLimitKeyer. Local capacity starts at 1 and rises by one per
// clean sample up to the ceiling.
type SaturationOptions struct {
	// Enabled turns the local input on, independently of the rate-limit input.
	Enabled bool

	// CgroupDir is the cgroup v2 directory to read. Empty means the process's
	// own cgroup: /sys/fs/cgroup joined with the path in /proc/self/cgroup
	// when that directory exists, else /sys/fs/cgroup (which, with a private
	// cgroup namespace, as Kubernetes gives containers on cgroup v2, is the
	// container's own).
	CgroupDir string

	// SamplePeriod is how often the files are read, and how fast local
	// capacity ramps up: +1 per clean sample. Defaults to 250ms. A shorter
	// period makes the working-set input react faster; it doesn't make CPU
	// pressure react faster, because "avg10" is already a 10-second average.
	SamplePeriod time.Duration

	// CPUPressureThreshold is the cpu.pressure "some avg10" percentage (0-100)
	// above which the controller is saturated. Defaults to
	// DefaultCPUPressureThreshold (provisional).
	CPUPressureThreshold float64

	// WorkingSetSoftRatio is the share of memory.max (0-1) at or above which
	// the working set (memory.current minus inactive_file) halves local
	// capacity. Defaults to 0.80.
	WorkingSetSoftRatio float64

	// WorkingSetHardRatio is the share of memory.max (0-1) at or above which
	// local capacity drops straight to 1. Defaults to 0.90.
	WorkingSetHardRatio float64
}

func (o SaturationOptions) withDefaults() (SaturationOptions, error) {
	if o.SamplePeriod == 0 {
		o.SamplePeriod = DefaultSamplePeriod
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
	switch {
	case o.SamplePeriod < 0:
		return o, errors.Errorf("ratelimit: saturation SamplePeriod must be positive, got %s", o.SamplePeriod)
	case o.CPUPressureThreshold < 0 || o.CPUPressureThreshold > 100:
		return o, errors.Errorf("ratelimit: saturation CPUPressureThreshold must be a percentage in (0, 100], got %v", o.CPUPressureThreshold)
	case o.WorkingSetSoftRatio < 0 || o.WorkingSetHardRatio > 1 || o.WorkingSetSoftRatio > o.WorkingSetHardRatio:
		return o, errors.Errorf("ratelimit: saturation working-set ratios must satisfy 0 < soft (%v) <= hard (%v) <= 1",
			o.WorkingSetSoftRatio, o.WorkingSetHardRatio)
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
	cpuPressure reading
	workingSet  reading
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
	return &saturation{
		opts:           o,
		dir:            dir,
		reader:         cgroupFiles{dir: dir},
		log:            log,
		loggedDisabled: map[string]bool{},
	}, nil
}

// read takes one sample from the cgroup files, publishes it for metrics, and
// logs each input the first time it is found disabled.
func (s *saturation) read() cgroupSample {
	smp := s.reader.read()
	s.last.Store(&smp)
	s.logDisabledOnce(smp)
	return smp
}

// step returns the local state that follows cur after sample smp:
//
//   - working set at or above the hard ratio: capacity 1 at once;
//   - any input over its threshold: capacity halves, minimum 1. step runs once
//     per sample, so this halves at most once per sample period;
//   - otherwise, a clean sample: capacity +1, up to ceiling. Local capacity
//     starts at 1 (slow start), so a controller restarted after an OOMKill
//     ramps into its backlog instead of meeting all of it at once.
//
// cause names what made the last sample saturated, and is cleared by a clean
// one.
func (s *saturation) step(smp cgroupSample, cur localState, ceiling int) localState {
	hard, saturated, cause := s.evaluate(smp)
	next := cur
	switch {
	case hard:
		next.capacity = capacityFloor
		next.cause = cause
	case saturated:
		next.capacity = max(capacityFloor, cur.capacity/2)
		next.cause = cause
	default:
		next.capacity = min(ceiling, cur.capacity+1)
		next.cause = ""
	}
	return next
}

// evaluate reports whether smp is over the hard working-set ratio, whether it
// is saturated at all, and which inputs made it so.
func (s *saturation) evaluate(smp cgroupSample) (hard, saturated bool, cause string) {
	var causes []string
	if r := smp.cpuPressure; r.enabled && r.percent > s.opts.CPUPressureThreshold {
		causes = append(causes, "CPU pressure "+formatPercent(r.percent)+" (some avg10)")
	}
	if r := smp.workingSet; r.enabled && r.percent >= s.opts.WorkingSetSoftRatio*100 {
		causes = append(causes, "memory working set "+formatPercent(r.percent)+" of limit")
		hard = r.percent >= s.opts.WorkingSetHardRatio*100
	}
	return hard, len(causes) > 0, strings.Join(causes, ", ")
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

// cgroupFiles reads the cgroup v2 interface files in dir.
type cgroupFiles struct{ dir string }

func (c cgroupFiles) read() cgroupSample {
	root, err := os.OpenRoot(c.dir)
	if err != nil {
		why := "no cgroup v2 directory at " + c.dir + ": " + err.Error()
		return cgroupSample{cpuPressure: disabled(why), workingSet: disabled(why)}
	}
	defer func() { _ = root.Close() }()

	return cgroupSample{
		cpuPressure: readPressure(root, "cpu.pressure", "some"),
		workingSet:  readWorkingSet(root),
	}
}

// readPressure returns avg10 of the given line ("some" or "full") of a PSI
// file, such as cpu.pressure:
//
//	some avg10=1.53 avg60=0.87 avg300=0.24 total=1234567
//	full avg10=0.00 avg60=0.00 avg300=0.00 total=0
func readPressure(root *os.Root, file, line string) reading {
	b, err := root.ReadFile(file)
	if err != nil {
		return disabled(missingWhy(file, err))
	}
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 || fields[0] != line {
			continue
		}
		for _, f := range fields[1:] {
			v, ok := strings.CutPrefix(f, "avg10=")
			if !ok {
				continue
			}
			p, err := strconv.ParseFloat(v, 64)
			if err != nil || p < 0 {
				return disabled(file + ": cannot parse " + strconv.Quote(f))
			}
			return reading{enabled: true, percent: p}
		}
	}
	return disabled(file + ": no " + strconv.Quote(line) + " avg10 value")
}

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
