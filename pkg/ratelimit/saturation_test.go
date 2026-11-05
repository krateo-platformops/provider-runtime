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
	return testGovernor(t, Options{
		MaxConcurrentReconciles: ceiling,
		RecoveryInterval:        testRecovery,
		Saturation:              SaturationOptions{Enabled: true, CgroupDir: fixture(dir), ExperimentalMemoryWorkingSet: memory},
	})
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
	if !smp.cpuPressure.enabled || smp.cpuPressure.percent != 2.10 {
		t.Errorf("cpu.pressure some avg10 = %+v, want 2.10", smp.cpuPressure)
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

// Slow start: a fresh governor's local capacity is 1, and it rises by 1 per
// StartupRampInterval until the first cut, whatever the sample period: sampled
// four times a second, it still takes 2s a step, so 16 workers take 30s.
func TestLocalCapacitySlowStartsAndRampsPerStartupStep(t *testing.T) {
	const ceiling = 16
	g, c := saturationGovernor(t, ceiling, "healthy", false)
	if got := g.LocalCapacity(); got != 1 {
		t.Fatalf("a fresh governor's local capacity = %d, want 1: a restart must not meet its backlog at full concurrency", got)
	}
	held := mustAcquire(t, g, "a")
	_, d := g.Acquire(context.Background(), "b")
	if d == nil || !d.Local || d.Reason != DeferRamping || !strings.Contains(d.Message(), "ramping up") {
		t.Fatalf("a second call at start got %+v, want a local Ramping deferral that says it is ramping up", d)
	}
	if strings.Contains(d.Message(), " b ") {
		t.Errorf("a local deferral's message names the key: %q", d.Message())
	}
	held()

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

// After any cut, local capacity recovers at +1 per clean RecoveryInterval, the
// same rule as API limits, not at the startup step: a clean interval starts
// after the last saturated sample, and a saturated sample during recovery
// halves and restarts it.
func TestAfterACutLocalRecoversPerCleanRecoveryInterval(t *testing.T) {
	g, c := saturationGovernor(t, 8, "healthy", false)
	rampToCeiling(t, g, c)
	useFixture(g, "cpu-pressure")
	g.sample()
	if got := g.LocalCapacity(); got != 4 {
		t.Fatalf("after a cut local capacity = %d, want 4", got)
	}

	useFixture(g, "healthy")
	for elapsed := time.Second; elapsed < testRecovery; elapsed += time.Second {
		c.add(time.Second)
		g.sample()
		if got := g.LocalCapacity(); got != 4 {
			t.Fatalf("%s after the cut local capacity = %d, want 4: recovery takes a whole RecoveryInterval", elapsed, got)
		}
	}
	c.add(time.Second)
	g.sample()
	if got := g.LocalCapacity(); got != 5 {
		t.Fatalf("after one clean RecoveryInterval local capacity = %d, want 5", got)
	}

	// A saturated sample halves and restarts the interval.
	c.add(testRecovery - time.Second)
	useFixture(g, "cpu-pressure")
	g.sample()
	if got := g.LocalCapacity(); got != 2 {
		t.Fatalf("a saturated sample during recovery left local capacity at %d, want 5 halved to 2", got)
	}
	useFixture(g, "healthy")
	c.add(testRecovery - time.Second)
	g.sample()
	if got := g.LocalCapacity(); got != 2 {
		t.Errorf("local capacity = %d, want 2: the clean interval restarts at the last saturated sample", got)
	}
	c.add(time.Second)
	g.sample()
	if got := g.LocalCapacity(); got != 3 {
		t.Errorf("local capacity = %d after a whole clean interval, want 3", got)
	}
}

// A cut during the startup ramp ends it, even at the floor where capacity
// can't fall: recovery is then +1 per RecoveryInterval.
func TestACutEndsTheStartupRamp(t *testing.T) {
	g, c := saturationGovernor(t, 8, "healthy", false)
	g.sample()
	c.add(2 * time.Second)
	g.sample()
	c.add(2 * time.Second)
	g.sample()
	if got := g.LocalCapacity(); got != 3 {
		t.Fatalf("mid-ramp local capacity = %d, want 3", got)
	}
	useFixture(g, "cpu-pressure")
	g.sample()
	if got := g.LocalCapacity(); got != 1 {
		t.Fatalf("a saturated sample mid-ramp left local capacity at %d, want 3 halved to 1", got)
	}
	useFixture(g, "healthy")
	c.add(2 * time.Second)
	g.sample()
	if got := g.LocalCapacity(); got != 1 {
		t.Fatalf("2s after the cut local capacity = %d, want 1: the startup ramp is over", got)
	}

	floor, c2 := saturationGovernor(t, 8, "cpu-pressure", false)
	floor.sample()
	useFixture(floor, "healthy")
	c2.add(2 * time.Second)
	floor.sample()
	if got := floor.LocalCapacity(); got != 1 {
		t.Errorf("a saturated sample at the floor didn't end the startup ramp: local capacity = %d, want 1", got)
	}
}

// Saturation halves local capacity once per sample, never below 1. The one-call
// floor is kept: with capacity at 1, a call still goes through.
func TestSaturationHalvesOncePerSampleAndFloorsAtOne(t *testing.T) {
	g, c := saturationGovernor(t, 8, "healthy", false)
	rampToCeiling(t, g, c)
	useFixture(g, "cpu-pressure")
	for i, want := range []int{4, 2, 1, 1} {
		g.sample()
		if got := g.LocalCapacity(); got != want {
			t.Fatalf("after saturated sample %d: local capacity = %d, want %d (halve once per sample, floor 1)", i+1, got, want)
		}
	}

	held := mustAcquire(t, g, "a")
	defer held()
	_, d := g.Acquire(context.Background(), "b")
	if d == nil || d.Reason != DeferSaturated || !d.Local {
		t.Fatalf("with local capacity 1 and one call in flight, a second key got %+v, want a local Saturated deferral", d)
	}
	if !strings.Contains(d.Cause, "CPU pressure 81%") || !strings.Contains(d.Message(), "controller saturated: CPU pressure 81%") {
		t.Errorf("cause %q / message %q, want them to name the CPU pressure", d.Cause, d.Message())
	}

	// Once clean the message stops saying saturated, while capacity recovers.
	useFixture(g, "healthy")
	g.sample()
	if _, d := g.Acquire(context.Background(), "b"); d == nil || d.Reason != DeferAtCapacity || !d.Local || !strings.Contains(d.Message(), "recovering") {
		t.Errorf("after a clean sample at capacity 1, a second call got %+v, want a local AtCapacity deferral that says it is recovering", d)
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

// The deferral says which state the local capacity is in, as a reason of its
// own: on the startup ramp from the start until the first cut, and never again
// after one, even once the samples are clean.
func TestRampingIsADistinctReasonUntilTheFirstCut(t *testing.T) {
	g, c := saturationGovernor(t, 8, "healthy", false)
	held := mustAcquire(t, g, "a")
	defer held()
	want := func(reason DeferReason, cause string) {
		t.Helper()
		_, d := g.Acquire(context.Background(), "b")
		if d == nil || d.Reason != reason || !d.Local {
			t.Fatalf("deferral = %+v, want a local %s", d, reason)
		}
		if got := d.Reason.Cause(); got != cause {
			t.Errorf("the metric cause of %s = %q, want %q", d.Reason, got, cause)
		}
	}
	want(DeferRamping, "ramping")
	g.sample()
	want(DeferRamping, "ramping")

	useFixture(g, "cpu-pressure")
	c.add(time.Second)
	g.sample()
	want(DeferSaturated, "saturated")

	useFixture(g, "healthy")
	c.add(time.Second)
	g.sample()
	want(DeferAtCapacity, "capacity")
}

// The ramp ends at the ceiling too: a local capacity that is full at the ceiling
// is at capacity, not ramping.
func TestRampingEndsAtTheCeiling(t *testing.T) {
	g, c := saturationGovernor(t, 2, "healthy", false)
	rampToCeiling(t, g, c)
	first, second := mustAcquire(t, g, "a"), mustAcquire(t, g, "b")
	defer first()
	defer second()
	if _, d := g.Acquire(context.Background(), ""); d == nil || d.Reason != DeferAtCapacity {
		t.Errorf("full at the ceiling, a call got %+v, want an AtCapacity deferral", d)
	}
}

// On the ramp, a deferred object is back before the ramp ends: the delay is at
// most the time the ramp still needs, capped at the RecoveryInterval. The
// RecoveryInterval alone could hold it up to 30s, past the end of a shorter
// ramp.
func TestRampingRequeueIsBoundedByTheRampLeft(t *testing.T) {
	const draws = 2000
	maxDelay := func(g *Governor) (most time.Duration) {
		t.Helper()
		for range draws {
			_, d := g.Acquire(context.Background(), "")
			if d == nil || d.Reason != DeferRamping {
				t.Fatalf("deferral = %+v, want a Ramping one", d)
			}
			if d.RequeueAfter < time.Second {
				t.Fatalf("RequeueAfter = %s, below the 1s minimum", d.RequeueAfter)
			}
			most = max(most, d.RequeueAfter)
		}
		return most
	}

	// A ceiling of 4: three 2s steps left, 6s.
	g, c := saturationGovernor(t, 4, "healthy", false)
	held := mustAcquire(t, g, "")
	defer held()
	if most := maxDelay(g); most > 6*time.Second {
		t.Errorf("with 6s of ramp left, a requeue took %s", most)
	}

	// One step taken, two left: the bound shrinks with the ramp, to 4s.
	g.sample()
	c.add(2 * time.Second)
	g.sample()
	if got := g.LocalCapacity(); got != 2 {
		t.Fatalf("local capacity = %d, want 2", got)
	}
	second := mustAcquire(t, g, "")
	defer second()
	if most := maxDelay(g); most > 4*time.Second {
		t.Errorf("with 4s of ramp left, a requeue took %s", most)
	}

	// A ramp longer than the RecoveryInterval is capped by it.
	big, _ := saturationGovernor(t, 64, "healthy", false)
	heldBig := mustAcquire(t, big, "")
	defer heldBig()
	if most := maxDelay(big); most > testRecovery {
		t.Errorf("a requeue took %s, above the RecoveryInterval %s", most, testRecovery)
	}

	// A step can't happen between samples, so a longer sample period sets the
	// pace: three steps at 5s.
	slow, _ := testGovernor(t, Options{MaxConcurrentReconciles: 4, Saturation: SaturationOptions{
		Enabled: true, CgroupDir: fixture("healthy"), SamplePeriod: 5 * time.Second,
	}})
	heldSlow := mustAcquire(t, slow, "")
	defer heldSlow()
	if most := maxDelay(slow); most <= 6*time.Second || most > 15*time.Second {
		t.Errorf("with a 5s sample period and 15s of ramp left, the longest requeue was %s", most)
	}
}

// A call without a key asks for a local slot only: saturation bounds it, and
// it creates no key state.
func TestKeylessCallNeedsOnlyALocalSlot(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	release := mustAcquire(t, g, "")
	_, d := g.Acquire(context.Background(), "")
	if d == nil || !d.Local {
		t.Fatalf("a second keyless call at local capacity 1 got %+v, want a local deferral", d)
	}
	release()
	mustAcquire(t, g, "")()
	if n := len(g.Snapshot().Keys); n != 0 {
		t.Errorf("keyless calls created %d key state(s)", n)
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
		cpuPressure: reading{enabled: true, percent: 90},
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
