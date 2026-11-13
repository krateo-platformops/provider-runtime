package ratelimit

import (
	"context"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/provider-runtime/pkg/logging"
)

const testRecovery = 30 * time.Second

// saturationGovernor returns a governor with the local input on, reading the
// fixture directory dir under testdata, CPU only unless memory is set. Tests
// drive it with sample(), one call per sample period, instead of Start's
// ticker.
func saturationGovernor(t *testing.T, ceiling int, dir string, memory bool) (*Governor, *clock) {
	t.Helper()
	g, c := testGovernor(t, Options{
		MaxConcurrentReconciles: ceiling,
		RecoveryInterval:        testRecovery,
		Saturation:              SaturationOptions{Enabled: true, CgroupDir: fixture(dir), ExperimentalMemoryWorkingSet: memory},
	})
	return g, c
}

func fixture(dir string) string { return filepath.Join("testdata", dir) }

func useFixture(g *Governor, dir string) {
	g.sat.reader = cgroupFiles{dir: fixture(dir), memory: g.sat.opts.ExperimentalMemoryWorkingSet}
}

// rampToCeiling takes clean samples, one startup step apart, until local
// capacity reaches the ceiling.
func rampToCeiling(t *testing.T, g *Governor, c *clock) {
	t.Helper()
	useFixture(g, "healthy")
	g.sample()
	for range g.Ceiling() - 1 {
		c.add(DefaultStartupRampInterval)
		g.sample()
	}
	if got := g.LocalCapacity(); got != g.Ceiling() {
		t.Fatalf("after the startup ramp local capacity = %d, want the ceiling %d", got, g.Ceiling())
	}
}

func inputByName(t *testing.T, g *Governor, name string) InputSnapshot {
	t.Helper()
	for _, in := range g.Snapshot().Inputs {
		if in.Input == name {
			return in
		}
	}
	t.Fatalf("no input %q in the snapshot", name)
	return InputSnapshot{}
}

func TestCgroupFixturesParse(t *testing.T) {
	smp := cgroupFiles{dir: fixture("healthy"), memory: true}.read()
	if !smp.cpuPressure.enabled || smp.cpuStall != 81234567*time.Microsecond {
		t.Errorf("cpu.pressure some total = %+v, %s, want 81.234567s", smp.cpuPressure, smp.cpuStall)
	}
	// (104857600 - 10485760) / 1073741824
	if !smp.workingSet.enabled || smp.workingSet.percent < 8.78 || smp.workingSet.percent > 8.79 {
		t.Errorf("working set = %+v, want 8.79%% of the limit", smp.workingSet)
	}
}

// memory.pressure is not an input: on the kind measurement it read 0.00 up to
// every OOMKill.
func TestMemoryPressureIsNotAnInput(t *testing.T) {
	for _, in := range allInputs {
		if strings.Contains(in, "pressure") && in != InputCPUPressure {
			t.Errorf("input %q: memory.pressure was dropped as an input", in)
		}
	}
	g, _ := saturationGovernor(t, 2, "healthy", false)
	if got := len(g.Snapshot().Inputs); got != 2 {
		t.Errorf("the snapshot reports %d inputs, want 2 (cpu_pressure, memory_working_set)", got)
	}
}

// The memory input is off by default: a provider that enables saturation gets
// CPU only. A working set at 98% of the limit neither cuts capacity nor is
// reported as a value, because the experimental input was not asked for.
func TestMemoryInputIsOffByDefault(t *testing.T) {
	g, c := saturationGovernor(t, 4, "healthy", false)
	rampToCeiling(t, g, c)
	useFixture(g, "no-cache-same-total")
	g.sample()
	if got := g.LocalCapacity(); got != 4 {
		t.Errorf("working set 98%% with the memory input off: local capacity = %d, want 4", got)
	}
	if ws := inputByName(t, g, InputMemoryWorkingSet); ws.Enabled {
		t.Errorf("memory_working_set reported as enabled (%v%%) with the experimental option unset", ws.Percent)
	}
	if !inputByName(t, g, InputCPUPressure).Enabled {
		t.Error("cpu_pressure reported as disabled: saturation must still read CPU")
	}
	if smp := (cgroupFiles{dir: fixture("no-cache-same-total")}).read(); smp.workingSet.enabled || smp.workingSet.disabledBecause != memoryInputOff {
		t.Errorf("working set sample = %+v, want it disabled because the input is off", smp.workingSet)
	}

	// The defaults that say so.
	o, err := SaturationOptions{}.withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if o.ExperimentalMemoryWorkingSet || o.SamplePeriod != time.Second || o.StartupRampInterval != 2*time.Second {
		t.Errorf("defaults = %+v, want memory off, a 1s sample period and a 2s startup step", o)
	}
	if o.WorkingSetSoftRatio != 0.80 || o.WorkingSetHardRatio != 0.90 {
		t.Errorf("provisional ratios = %v/%v, want 0.80/0.90", o.WorkingSetSoftRatio, o.WorkingSetHardRatio)
	}
}

func TestLocalCapacitySlowStartsAndRampsPerStartupStep(t *testing.T) {
	const ceiling = 16
	g, c := saturationGovernor(t, ceiling, "healthy", false)
	if got := g.LocalCapacity(); got != 1 {
		t.Fatalf("a fresh governor's local capacity = %d, want 1: a restart must not meet its backlog at full concurrency", got)
	}
	g.sample() // starts the clock
	if got := g.LocalCapacity(); got != 1 {
		t.Fatalf("the first sample raised local capacity to %d, want 1", got)
	}
	var elapsed time.Duration
	for want := 2; want <= ceiling; want++ {
		for range 8 { // 2s at a 250ms period
			if got := g.LocalCapacity(); got != want-1 {
				t.Fatalf("at %s local capacity = %d, want %d: +1 per 2s, not per sample", elapsed, got, want-1)
			}
			c.add(250 * time.Millisecond)
			elapsed += 250 * time.Millisecond
			g.sample()
		}
		if got := g.LocalCapacity(); got != want {
			t.Fatalf("at %s local capacity = %d, want %d", elapsed, got, want)
		}
	}
	if elapsed != 30*time.Second {
		t.Errorf("reached the ceiling of 16 after %s, want 30s", elapsed)
	}
	c.add(time.Hour)
	g.sample()
	if got := g.LocalCapacity(); got != ceiling {
		t.Errorf("local capacity = %d, want it to stay at the ceiling %d", got, ceiling)
	}
}

// The step is a setting of its own, not tied to the sample period.
func TestStartupRampIntervalIsConfigurable(t *testing.T) {
	g, c := testGovernor(t, Options{MaxConcurrentReconciles: 4, Saturation: SaturationOptions{
		Enabled: true, CgroupDir: fixture("healthy"), StartupRampInterval: 5 * time.Second,
	}})
	g.sample()
	c.add(4 * time.Second)
	g.sample()
	if got := g.LocalCapacity(); got != 1 {
		t.Fatalf("after 4s local capacity = %d, want 1 with a 5s step", got)
	}
	c.add(time.Second)
	g.sample()
	if got := g.LocalCapacity(); got != 2 {
		t.Errorf("after 5s local capacity = %d, want 2", got)
	}
}

// cpuFeed is a cgroup reader whose cumulative CPU stall the test advances, a
// second at a time, with the governor's clock.
type cpuFeed struct {
	g     *Governor
	c     *clock
	total time.Duration
}

// newCPUFeed returns a governor reading f, after its first sample.
func newCPUFeed(t *testing.T, ceiling int) *cpuFeed {
	t.Helper()
	g, c := saturationGovernor(t, ceiling, "healthy", false)
	f := &cpuFeed{g: g, c: c}
	g.sat.reader = f
	g.sample()
	return f
}

func (f *cpuFeed) read() cgroupSample {
	return cgroupSample{cpuPressure: reading{enabled: true}, cpuStall: f.total, workingSet: disabled(memoryInputOff)}
}

// second advances the clock by a second in which some task stalled for share
// of the time, samples, and returns the local capacity.
func (f *cpuFeed) second(share float64) int {
	f.c.add(time.Second)
	f.total += time.Duration(share * float64(time.Second))
	f.g.sample()
	return f.g.LocalCapacity()
}

// ramp takes clean seconds until local capacity is at the ceiling.
func (f *cpuFeed) ramp(t *testing.T) {
	t.Helper()
	for range 2 * (f.g.Ceiling() - 1) {
		f.second(0)
	}
	if got := f.g.LocalCapacity(); got != f.g.Ceiling() {
		t.Fatalf("after the ramp local capacity = %d, want the ceiling %d", got, f.g.Ceiling())
	}
}

func (f *cpuFeed) pressure(t *testing.T) float64 {
	t.Helper()
	return inputByName(t, f.g, InputCPUPressure).Percent
}

// CPU pressure is the share of the last CPUPressureWindow that some task
// stalled, from the change of the cumulative counter: a stall shows in the
// next sample, and is gone a window after it ends.
func TestCPUPressureIsTheStallShareOverTheWindow(t *testing.T) {
	f := newCPUFeed(t, 4)
	for range 10 {
		f.second(0.2)
	}
	if got := f.pressure(t); got < 19.99 || got > 20.01 {
		t.Fatalf("after 10s stalled a fifth of the time, CPU pressure = %v%%, want 20", got)
	}
	f.second(1)
	if got := f.pressure(t); got < 27.99 || got > 28.01 {
		t.Fatalf("one fully stalled second after nine at a fifth: CPU pressure = %v%%, want 28", got)
	}
	for range 10 {
		f.second(0)
	}
	if got := f.pressure(t); got != 0 {
		t.Errorf("a window after the stall ended, CPU pressure = %v%%, want 0", got)
	}
}

// A counter that goes backwards is another cgroup's: the window starts again
// instead of reading the difference as a stall.
func TestCPUStallCounterGoingBackwardsRestartsTheWindow(t *testing.T) {
	f := newCPUFeed(t, 8)
	f.ramp(t)
	for range 10 {
		f.second(0.3)
	}
	if got := f.pressure(t); got < 29.99 || got > 30.01 {
		t.Fatalf("CPU pressure = %v%%, want 30", got)
	}
	f.total = 0
	for i := range 12 {
		if got := f.second(0); got != 8 {
			t.Fatalf("%ds after the counter restarted local capacity = %d, want 8", i+1, got)
		}
		if got := f.pressure(t); got != 0 {
			t.Fatalf("%ds after the counter restarted CPU pressure = %v%%, want 0", i+1, got)
		}
	}
}

// A CPU cut needs a whole window of evidence: pressure over the threshold in
// the first seconds of a governor's life holds capacity, and doesn't cut it.
func TestACPUCutNeedsAWholeWindow(t *testing.T) {
	f := newCPUFeed(t, 8)
	f.g.local.Store(&localState{capacity: 8})
	for i := 1; i < 10; i++ {
		if got := f.second(1); got != 8 {
			t.Fatalf("%ds into the first window local capacity = %d, want 8: the stall isn't confirmed yet", i, got)
		}
	}
	if got := f.second(1); got != 4 {
		t.Errorf("after a whole stalled window local capacity = %d, want 8 halved to 4", got)
	}
}

// CPU pressure cuts at most once per window, however long it stays over the
// threshold: the next cut needs a window measured after the last one. The
// floor of one call is kept.
func TestCPUPressureCutsOncePerWindowAndFloorsAtOne(t *testing.T) {
	f := newCPUFeed(t, 16)
	f.ramp(t)
	// The window holds 10%, 20%, ... of stall: over 50% at the sixth second.
	for i := 1; i <= 5; i++ {
		if got := f.second(1); got != 16 {
			t.Fatalf("after %d stalled seconds (%.0f%%) local capacity = %d, want 16", i, f.pressure(t), got)
		}
	}
	want := 16
	for _, cut := range []int{8, 4, 2, 1, 1} {
		if got := f.second(1); got != cut {
			t.Fatalf("local capacity = %d, want %d halved to %d", got, want, cut)
		}
		want = cut
		for i := 1; i < 10; i++ {
			if got := f.second(1); got != want {
				t.Fatalf("%ds after a cut, still stalled: local capacity = %d, want %d (one cut per window)", i, got, want)
			}
		}
	}
}

// After a CPU cut, capacity holds while the window still shows the stall, and
// then rises by one per StartupRampInterval, not per RecoveryInterval.
func TestAfterACPUCutCapacityRecoversPerRampInterval(t *testing.T) {
	f := newCPUFeed(t, 16)
	f.ramp(t)
	for range 6 {
		f.second(1)
	}
	if got := f.g.LocalCapacity(); got != 8 {
		t.Fatalf("after six stalled seconds local capacity = %d, want 8", got)
	}
	// The six stalled seconds stay in the window, over the threshold, for
	// four more samples; the fifth reads 50%, which is clean.
	for i := 1; i <= 5; i++ {
		if got := f.second(0); got != 8 {
			t.Fatalf("%ds after the stall ended (%.0f%%) local capacity = %d, want 8", i, f.pressure(t), got)
		}
	}
	for want := 9; want <= 16; want++ {
		if got := f.second(0); got != want {
			t.Fatalf("local capacity = %d, want %d: +1 per 2s after a CPU cut", got, want)
		}
		if want < 16 {
			if got := f.second(0); got != want {
				t.Fatalf("local capacity = %d, want %d: +1 per 2s, not per sample", got, want)
			}
		}
	}
}

// Pressure over the threshold that doesn't cut still isn't clean: capacity
// doesn't rise during the startup ramp while it lasts.
func TestUnconfirmedCPUPressureHoldsTheStartupRamp(t *testing.T) {
	f := newCPUFeed(t, 8)
	for i := 1; i <= 6; i++ {
		if got := f.second(1); got != 1 {
			t.Fatalf("%ds of stall from the start: local capacity = %d, want 1", i, got)
		}
	}
}

// The working set excludes reclaimable cache: memory.current at 98% of the
// limit, mostly inactive_file, does not slow down. The same total with no
// inactive_file does.
func TestWorkingSetExcludesReclaimableCache(t *testing.T) {
	g, c := saturationGovernor(t, 8, "healthy", true)
	rampToCeiling(t, g, c)
	useFixture(g, "cache-heavy")
	g.sample()
	if got := g.LocalCapacity(); got != 8 {
		t.Errorf("cache-heavy: local capacity = %d, want 8: page cache the kernel can drop must not slow anything down", got)
	}
	if ws := inputByName(t, g, InputMemoryWorkingSet); !ws.Enabled || ws.Percent > 15 {
		t.Errorf("cache-heavy working set = %+v, want about 14%%", ws)
	}

	useFixture(g, "no-cache-same-total")
	g.sample()
	if got := g.LocalCapacity(); got != 1 {
		t.Errorf("same total, no inactive_file: local capacity = %d, want 1 (98%% working set is over the hard ratio)", got)
	}
}

// Between the soft and the hard ratio, local capacity halves.
func TestSoftWorkingSetRatioHalves(t *testing.T) {
	g, c := saturationGovernor(t, 8, "healthy", true)
	rampToCeiling(t, g, c)
	useFixture(g, "working-set-soft")
	g.sample()
	if got := g.LocalCapacity(); got != 4 {
		t.Errorf("working set 85%%: local capacity = %d, want 4 (halved at the 0.80 soft ratio)", got)
	}
}

// The hard memory ratio drops to 1 at once: at 90% working set, local goes
// from N to 1 in one sample.
func TestHardWorkingSetRatioDropsToOneAtOnce(t *testing.T) {
	g, c := saturationGovernor(t, 16, "healthy", true)
	rampToCeiling(t, g, c)
	useFixture(g, "working-set-hard")
	g.sample()
	if got := g.LocalCapacity(); got != 1 {
		t.Errorf("working set 90%%: local capacity = %d after one sample, want 1", got)
	}
	_, d := g.Acquire(context.Background(), "a")
	if d != nil {
		t.Fatalf("one call must always be let through at the floor, got %+v", d)
	}
}

// A missing input is disabled, not saturated: with no PSI file, or memory.max
// set to max, the controller still ramps to full capacity and the snapshot says
// the input is disabled.
func TestMissingInputIsDisabledNotSaturated(t *testing.T) {
	for _, tc := range []struct {
		dir      string
		disabled []string
		enabled  []string
	}{
		{"no-psi", []string{InputCPUPressure}, []string{InputMemoryWorkingSet}},
		{"no-memory-limit", []string{InputMemoryWorkingSet}, []string{InputCPUPressure}},
		{"does-not-exist", []string{InputCPUPressure, InputMemoryWorkingSet}, nil},
	} {
		t.Run(tc.dir, func(t *testing.T) {
			logs := &infoLog{}
			g, c := testGovernor(t, Options{MaxConcurrentReconciles: 6, Logger: logs, Saturation: SaturationOptions{
				Enabled: true, CgroupDir: fixture(tc.dir), ExperimentalMemoryWorkingSet: true,
			}})
			g.sample()
			for range 5 {
				c.add(DefaultStartupRampInterval)
				g.sample()
			}
			if got := g.LocalCapacity(); got != 6 {
				t.Errorf("local capacity = %d after the ramp, want 6: a missing input must not hold it back", got)
			}
			for _, in := range tc.disabled {
				if is := inputByName(t, g, in); is.Enabled {
					t.Errorf("%s reported as enabled (%v%%), want disabled", in, is.Percent)
				}
			}
			for _, in := range tc.enabled {
				if is := inputByName(t, g, in); !is.Enabled {
					t.Errorf("%s reported as disabled, want enabled: only the missing input is off", in)
				}
			}
			if got := logs.countWith("Saturation input disabled"); got != len(tc.disabled) {
				t.Errorf("logged %d disabled-input lines over the samples, want %d (once per input)", got, len(tc.disabled))
			}
		})
	}
}

// With saturation off, every input reports as disabled.
func TestSaturationOffReportsInputsDisabled(t *testing.T) {
	g, _ := testGovernor(t, Options{MaxConcurrentReconciles: 2})
	for _, in := range g.Snapshot().Inputs {
		if in.Enabled {
			t.Errorf("%s reported enabled with saturation off", in.Input)
		}
	}
}

// stubReader is a cgroup reader that fails the test when it is read outside
// the sampler's tick.
type stubReader struct {
	t       *testing.T
	inTick  atomic.Bool
	reads   atomic.Int64
	smp     cgroupSample
	outside atomic.Int64
}

func (s *stubReader) read() cgroupSample {
	s.reads.Add(1)
	if !s.inTick.Load() {
		s.outside.Add(1)
		s.t.Error("the cgroup files were read outside the sampler's tick: the hot path must do no I/O")
	}
	return s.smp
}

// The hot path does no I/O: Acquire reads the published value only.
func TestAcquireDoesNoIO(t *testing.T) {
	g, _ := testGovernor(t, Options{MaxConcurrentReconciles: 2, Saturation: SaturationOptions{Enabled: true, CgroupDir: fixture("healthy")}})
	stub := &stubReader{t: t, smp: cgroupSample{
		cpuPressure: reading{enabled: true},
		workingSet:  reading{enabled: true, percent: 10},
	}}
	g.sat.reader = stub

	stub.inTick.Store(true)
	g.sample()
	stub.inTick.Store(false)
	if stub.reads.Load() != 1 {
		t.Fatalf("the tick read the files %d times, want 1", stub.reads.Load())
	}

	for range 1000 {
		if release, _ := g.Acquire(context.Background(), "h"); release != nil {
			release()
		}
	}
	if g.LocalCapacity() != 1 {
		t.Errorf("local capacity = %d, want 1 from the published sample", g.LocalCapacity())
	}
	if n := stub.outside.Load(); n != 0 {
		t.Errorf("Acquire read the cgroup files %d times", n)
	}
}

func TestOwnCgroupDirFollowsProcSelfCgroup(t *testing.T) {
	got := ownCgroupDir(fixture("proc-self"), fixture("cgroup-mount"))
	if want := filepath.Join(fixture("cgroup-mount"), "kubepods", "pod1", "ctr"); got != want {
		t.Errorf("ownCgroupDir = %q, want %q", got, want)
	}
	if got := ownCgroupDir(fixture("does-not-exist"), "/sys/fs/cgroup"); got != "/sys/fs/cgroup" {
		t.Errorf("without /proc/self/cgroup, ownCgroupDir = %q, want the mount", got)
	}
	// A path that climbs out of the mount stays under it: here it names no
	// directory there, so the mount itself is used.
	if got := ownCgroupDir(fixture("proc-escape"), fixture("cgroup-mount")); got != fixture("cgroup-mount") {
		t.Errorf("with a cgroup path climbing out of the mount, ownCgroupDir = %q, want the mount", got)
	}
}

func TestStartStopsWithItsContext(t *testing.T) {
	for _, sat := range []bool{false, true} {
		g, err := New(Options{MaxConcurrentReconciles: 2, Saturation: SaturationOptions{Enabled: sat, CgroupDir: fixture("healthy"), SamplePeriod: 10 * time.Millisecond}})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- g.Start(ctx) }()
		time.Sleep(30 * time.Millisecond)
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Start returned %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Start did not return after its context was cancelled")
		}
		if sat && !inputByName(t, g, InputCPUPressure).Enabled {
			t.Error("the running sampler did not publish a sample")
		}
	}
}

// BenchmarkSample measures one sampler tick: reading the cgroup files and
// publishing the new local capacity, on fixture files. Real cgroup files cost
// more, and that cost moved about tenfold with host load (docs/configuration.md).
// Use it to compare changes, not to estimate the cost.
func BenchmarkSample(b *testing.B) {
	g, err := New(Options{MaxConcurrentReconciles: 16, Saturation: SaturationOptions{Enabled: true, CgroupDir: fixture("healthy")}})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		g.sample()
	}
}

// infoLog counts Info lines.
type infoLog struct{ lines []string }

func (l *infoLog) Info(msg string, _ ...any)        { l.lines = append(l.lines, msg) }
func (l *infoLog) Debug(string, ...any)             {}
func (l *infoLog) Warn(string, ...any)              {}
func (l *infoLog) Error(error, string, ...any)      {}
func (l *infoLog) WithName(string) logging.Logger   { return l }
func (l *infoLog) WithValues(...any) logging.Logger { return l }

func (l *infoLog) countWith(prefix string) int {
	n := 0
	for _, s := range l.lines {
		if strings.HasPrefix(s, prefix) {
			n++
		}
	}
	return n
}
