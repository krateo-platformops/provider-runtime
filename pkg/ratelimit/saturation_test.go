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

// saturationGovernor returns a governor with the local input on, reading the
// fixture directory dir under testdata. Tests drive it with g.sample(), one
// call per sample period, instead of running Start's ticker.
func saturationGovernor(t *testing.T, ceiling int, recovery time.Duration, dir string) (*Governor, *clock) {
	t.Helper()
	g, c := testGovernor(t, Options{
		MaxConcurrentReconciles: ceiling,
		RecoveryInterval:        recovery,
		Saturation:              SaturationOptions{Enabled: true, CgroupDir: fixture(dir)},
	})
	return g, c
}

func fixture(dir string) string { return filepath.Join("testdata", dir) }

func useFixture(g *Governor, dir string) { g.sat.reader = cgroupFiles{dir: fixture(dir)} }

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
	smp := cgroupFiles{dir: fixture("healthy")}.read()
	if !smp.cpuPressure.enabled || smp.cpuPressure.percent != 2.10 {
		t.Errorf("cpu.pressure some avg10 = %+v, want 2.10", smp.cpuPressure)
	}
	if !smp.memoryPressure.enabled || smp.memoryPressure.percent != 0 {
		t.Errorf("memory.pressure full avg10 = %+v, want 0.00 (the full line, not some)", smp.memoryPressure)
	}
	// (104857600 - 10485760) / 1073741824
	if !smp.workingSet.enabled || smp.workingSet.percent < 8.78 || smp.workingSet.percent > 8.79 {
		t.Errorf("working set = %+v, want 8.79%% of the limit", smp.workingSet)
	}
}

// Saturation halves local capacity once per sample, never below 1, and clean
// samples add +1 per RecoveryInterval.
func TestSaturationHalvesOncePerSampleFloorsAtOneRecoversAdditively(t *testing.T) {
	const ceiling = 8
	g, c := saturationGovernor(t, ceiling, 10*time.Second, "cpu-pressure")

	for i, want := range []int{4, 2, 1, 1, 1} {
		g.sample()
		if got := g.LocalCapacity(); got != want {
			t.Fatalf("after saturated sample %d: local capacity = %d, want %d (halve once per sample, floor 1)", i+1, got, want)
		}
		c.add(time.Second)
	}

	held := mustAcquire(t, g, "a")
	defer held()
	_, d := g.Acquire(context.Background(), "b")
	if d == nil || d.Reason != DeferSaturated {
		t.Fatalf("with local capacity 1 and one call in flight, a second key got %+v, want a Saturated deferral", d)
	}
	if !strings.Contains(d.Cause, "CPU pressure 81%") {
		t.Errorf("cause = %q, want it to name the CPU pressure", d.Cause)
	}

	useFixture(g, "memory-pressure")
	g.sample()
	if got := g.LocalCapacity(); got != 1 {
		t.Fatalf("memory pressure: local capacity = %d, want 1", got)
	}

	useFixture(g, "healthy")
	last := 1
	for elapsed := time.Duration(0); last < ceiling && elapsed < 5*time.Minute; elapsed += time.Second {
		c.add(time.Second)
		g.sample()
		got := g.LocalCapacity()
		if got > last+1 {
			t.Fatalf("local capacity jumped from %d to %d in one sample; recovery is +1", last, got)
		}
		last = got
	}
	if last != ceiling {
		t.Fatalf("local capacity recovered to %d, want the ceiling %d", last, ceiling)
	}
}

// Recovery takes a whole RecoveryInterval per step.
func TestSaturationRecoveryWaitsAWholeInterval(t *testing.T) {
	g, c := saturationGovernor(t, 4, 10*time.Second, "cpu-pressure")
	g.sample() // 4 -> 2
	useFixture(g, "healthy")
	for range 9 {
		c.add(time.Second)
		g.sample()
	}
	if got := g.LocalCapacity(); got != 2 {
		t.Fatalf("local capacity = %d nine seconds after saturation, want 2 (RecoveryInterval is 10s)", got)
	}
	c.add(time.Second)
	g.sample()
	if got := g.LocalCapacity(); got != 3 {
		t.Fatalf("local capacity = %d after one RecoveryInterval clean, want 3", got)
	}
}

// The working set excludes reclaimable cache: memory.current at 98% of the
// limit, mostly inactive_file, does not slow down. The same total with no
// inactive_file does.
func TestWorkingSetExcludesReclaimableCache(t *testing.T) {
	g, _ := saturationGovernor(t, 8, 30*time.Second, "cache-heavy")
	g.sample()
	if got := g.LocalCapacity(); got != 8 {
		t.Errorf("cache-heavy: local capacity = %d, want 8: page cache the kernel can drop must not slow anything down", got)
	}
	if ws := inputByName(t, g, InputMemoryWorkingSet); !ws.Enabled || ws.Percent > 15 {
		t.Errorf("cache-heavy working set = %+v, want about 14%%", ws)
	}

	g2, _ := saturationGovernor(t, 8, 30*time.Second, "no-cache-same-total")
	g2.sample()
	if got := g2.LocalCapacity(); got != 1 {
		t.Errorf("same total, no inactive_file: local capacity = %d, want 1 (98%% working set is over the hard ratio)", got)
	}
}

// At the soft ratio, local capacity halves.
func TestSoftWorkingSetRatioHalves(t *testing.T) {
	g, _ := saturationGovernor(t, 8, 30*time.Second, "working-set-soft")
	g.sample()
	if got := g.LocalCapacity(); got != 4 {
		t.Errorf("working set 92%%: local capacity = %d, want 4", got)
	}
}

// The hard memory ratio drops to 1 at once: at 95% working set, local goes
// from N to 1 in one sample.
func TestHardWorkingSetRatioDropsToOneAtOnce(t *testing.T) {
	g, _ := saturationGovernor(t, 16, 30*time.Second, "working-set-hard")
	g.sample()
	if got := g.LocalCapacity(); got != 1 {
		t.Errorf("working set 95%%: local capacity = %d after one sample, want 1", got)
	}
	_, d := g.Acquire(context.Background(), "a")
	if d != nil {
		t.Fatalf("one call must always be let through at the floor, got %+v", d)
	}
}

// A missing input is disabled, not saturated: with no PSI files, or memory.max
// set to max, the controller runs at full capacity and the snapshot says the
// input is disabled.
func TestMissingInputIsDisabledNotSaturated(t *testing.T) {
	for _, tc := range []struct {
		dir      string
		disabled []string
		enabled  []string
	}{
		{"no-psi", []string{InputCPUPressure, InputMemoryPressure}, []string{InputMemoryWorkingSet}},
		{"no-memory-limit", []string{InputMemoryWorkingSet}, []string{InputCPUPressure, InputMemoryPressure}},
		{"does-not-exist", []string{InputCPUPressure, InputMemoryPressure, InputMemoryWorkingSet}, nil},
	} {
		t.Run(tc.dir, func(t *testing.T) {
			logs := &infoLog{}
			g, err := New(Options{MaxConcurrentReconciles: 6, Logger: logs, Saturation: SaturationOptions{Enabled: true, CgroupDir: fixture(tc.dir)}})
			if err != nil {
				t.Fatal(err)
			}
			g.sample()
			g.sample()
			if got := g.LocalCapacity(); got != 6 {
				t.Errorf("local capacity = %d, want 6: a missing input must not slow the controller", got)
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
				t.Errorf("logged %d disabled-input lines over two samples, want %d (once per input)", got, len(tc.disabled))
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
		cpuPressure:    reading{enabled: true, percent: 90},
		memoryPressure: reading{enabled: true},
		workingSet:     reading{enabled: true, percent: 10},
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
