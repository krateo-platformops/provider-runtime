package ratelimit

import (
	"fmt"
	"math"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strings"
	"testing"
)

// headroomGovernor returns a governor with the memory input on and a Go memory
// limit of 1000, at the returned used value, reading the fixture dir.
func headroomGovernor(t *testing.T, ceiling int, dir string, o SaturationOptions) (*Governor, *clock, *goMemoryStats) {
	t.Helper()
	o.Enabled, o.CgroupDir, o.ExperimentalMemory = true, fixture(dir), true
	g, c := testGovernor(t, Options{MaxConcurrentReconciles: ceiling, RecoveryInterval: testRecovery, Saturation: o})
	m := &goMemoryStats{limit: 1000, used: 100}
	g.sat.goMemory = func() goMemoryStats { return *m }
	return g, c, m
}

// Headroom at or below the ratio halves capacity, and above it nothing happens.
// The ratio is inclusive, and the input reports the headroom itself.
func TestGoMemoryHeadroomHalvesAtItsRatio(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ratio   float64
		used    uint64
		percent float64
		want    int
	}{
		{"above the default ratio", 0, 849, 15.1, 8},
		{"at the default ratio", 0, 850, 15, 4},
		{"below the default ratio", 0, 900, 10, 4},
		{"above a set ratio", 0.5, 400, 60, 8},
		{"below a set ratio", 0.5, 600, 40, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, c, m := headroomGovernor(t, 8, "healthy", SaturationOptions{ExperimentalGoMemoryHeadroomRatio: tc.ratio})
			rampToCeiling(t, g, c)
			m.used = tc.used
			g.sample()
			if got := g.LocalCapacity(); got != tc.want {
				t.Errorf("local capacity = %d, want %d", got, tc.want)
			}
			in := inputByName(t, g, InputGoMemoryHeadroom)
			if !in.Enabled || in.Percent < tc.percent-0.01 || in.Percent > tc.percent+0.01 {
				t.Errorf("headroom input = %+v, want %v%%", in, tc.percent)
			}
		})
	}
}

// There is no hard level: even at no headroom at all, capacity halves. Dropping
// straight to 1 cost throughput and gained no margin when measured.
func TestGoMemoryHeadroomHasNoHardLevel(t *testing.T) {
	g, c, m := headroomGovernor(t, 8, "healthy", SaturationOptions{})
	rampToCeiling(t, g, c)
	m.used = 2000
	g.sample()
	if got := g.LocalCapacity(); got != 4 {
		t.Errorf("past the limit local capacity = %d, want 4", got)
	}
}

// cutLog keeps each Info line with its key-value pairs.
type cutLog struct{ infoLog }

func (l *cutLog) Info(msg string, kv ...any) { l.lines = append(l.lines, fmt.Sprint(msg, kv)) }

// A cut by the headroom is logged naming it, as the operator's only sign of it:
// with the local gate in the queue nothing is deferred and nothing is written
// on the object.
func TestGoMemoryHeadroomNamesItselfInTheCut(t *testing.T) {
	logs := &cutLog{}
	g, c, m := headroomGovernor(t, 2, "healthy", SaturationOptions{})
	g.log = logs
	rampToCeiling(t, g, c)
	m.used = 900
	g.sample()
	if got := g.LocalCapacity(); got != 1 {
		t.Fatalf("local capacity = %d, want 1", got)
	}
	for _, l := range logs.lines {
		if strings.HasPrefix(l, "Controller saturated: lowering local capacity") && strings.Contains(l, "Go memory headroom 10% of limit") {
			return
		}
	}
	t.Errorf("no cut logged naming the headroom: %v", logs.lines)
}

// memoryReader records what it was asked for, returning a working set past the
// hard ratio when the memory files are read.
type memoryReader struct{ asked []bool }

func (r *memoryReader) read(memory bool) cgroupSample {
	r.asked = append(r.asked, memory)
	smp := cgroupSample{cpuPressure: reading{enabled: true}, workingSet: disabled(memoryInputOff)}
	if memory {
		smp.workingSet = reading{enabled: true, percent: 98}
	}
	return smp
}

// While there is a Go memory limit the headroom is the input: a working set at
// 98% cuts nothing, and memory.stat isn't read.
func TestGoMemoryHeadroomReplacesTheWorkingSet(t *testing.T) {
	logs := &infoLog{}
	g, c := testGovernor(t, Options{MaxConcurrentReconciles: 4, Logger: logs, Saturation: SaturationOptions{
		Enabled: true, CgroupDir: fixture("healthy"), ExperimentalMemory: true,
	}})
	g.sat.goMemory = func() goMemoryStats { return goMemoryStats{limit: 1000, used: 100} }
	r := &memoryReader{}
	g.sat.reader = r
	g.sample()
	for range 3 {
		c.add(DefaultStartupRampInterval)
		g.sample()
	}
	if got := g.LocalCapacity(); got != 4 {
		t.Errorf("local capacity = %d, want 4: the working set is not the input here", got)
	}
	for _, memory := range r.asked {
		if memory {
			t.Fatal("the memory files were read while the headroom was the input")
		}
	}
	if ws := inputByName(t, g, InputMemoryWorkingSet); ws.Enabled {
		t.Errorf("working set reported as enabled (%v%%) while the headroom replaces it", ws.Percent)
	}
	if got := logs.countWith("Saturation input disabled"); got != 1 {
		t.Errorf("logged %d disabled-input lines, want 1 saying the working set was replaced: %v", got, logs.lines)
	}
	// The log is how an operator tells which memory input is running.
	if why := g.sat.last.Load().workingSet.disabledBecause; !strings.Contains(why, "headroom replaces it") {
		t.Errorf("working set disabled because %q, want it to say the headroom replaced it", why)
	}
}

// With no Go memory limit there is nothing to measure headroom against, so the
// working set is the input. GOMEMLIMIT=off is the case that reaches this.
func TestWorkingSetIsTheInputWithNoGoMemoryLimit(t *testing.T) {
	g, c, m := headroomGovernor(t, 4, "healthy", SaturationOptions{})
	m.limit = math.MaxInt64
	rampToCeiling(t, g, c)
	r := &memoryReader{}
	g.sat.reader = r
	g.sample()
	if got := g.LocalCapacity(); got != 1 {
		t.Errorf("working set 98%% with no Go memory limit: local capacity = %d, want 1", got)
	}
	if len(r.asked) != 1 || !r.asked[0] {
		t.Errorf("memory files asked for = %v, want true", r.asked)
	}
	if in := inputByName(t, g, InputGoMemoryHeadroom); in.Enabled {
		t.Errorf("headroom reported as enabled with no limit: %+v", in)
	}
}

// With the memory input off the runtime's memory isn't read, and the input it
// feeds isn't mentioned in the log: nobody asked for it.
func TestGoMemoryHeadroomIsOffWithTheMemoryInputOff(t *testing.T) {
	logs := &infoLog{}
	g, _ := testGovernor(t, Options{MaxConcurrentReconciles: 4, Logger: logs, Saturation: SaturationOptions{
		Enabled: true, CgroupDir: fixture("healthy"),
	}})
	reads := 0
	g.sat.goMemory = func() goMemoryStats { reads++; return goMemoryStats{limit: 1000, used: 999} }
	g.sample()
	if reads != 0 {
		t.Errorf("the runtime's memory was read %d times with the input off", reads)
	}
	if in := inputByName(t, g, InputGoMemoryHeadroom); in.Enabled {
		t.Errorf("headroom reported as enabled with the input off: %+v", in)
	}
	if got := logs.countWith("Saturation input disabled"); got != 1 {
		t.Errorf("logged %d disabled-input lines, want 1 for the working set alone: %v", got, logs.lines)
	}
}

func TestGoHeadroomReading(t *testing.T) {
	for _, tc := range []struct {
		name    string
		m       goMemoryStats
		enabled bool
		percent float64
	}{
		{"no limit", goMemoryStats{limit: math.MaxInt64, used: 1}, false, 0},
		{"limit not reported", goMemoryStats{}, false, 0},
		{"half used", goMemoryStats{limit: 1000, used: 500}, true, 50},
		{"nothing used", goMemoryStats{limit: 1000}, true, 100},
		{"at the limit", goMemoryStats{limit: 1000, used: 1000}, true, 0},
		{"past the limit", goMemoryStats{limit: 1000, used: 1500}, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := goHeadroom(tc.m)
			if r.enabled != tc.enabled || r.percent != tc.percent {
				t.Errorf("reading = %+v, want enabled %t at %v%%", r, tc.enabled, tc.percent)
			}
			if !r.enabled && r.disabledBecause == "" {
				t.Error("a disabled reading gave no reason")
			}
		})
	}
}

// The real runtime: used follows the live heap and leaves out garbage. With a
// limit set, a figure that counts garbage sits at the limit whatever the load.
func TestGoMemoryReaderCountsLiveHeapNotGarbage(t *testing.T) {
	const size = 64 << 20
	r := newGoMemoryReader()
	b := make([]byte, size)
	for i := 0; i < len(b); i += 4096 {
		b[i] = 1
	}
	runtime.GC()
	held := r.read()
	runtime.KeepAlive(b)
	runtime.GC()
	freed := r.read()

	if held.used < size {
		t.Errorf("used with %d bytes live = %d", size, held.used)
	}
	if held.used < freed.used+size*9/10 {
		t.Errorf("used = %d held and %d freed, want it to fall with the live heap", held.used, freed.used)
	}
	if want := uint64(debug.SetMemoryLimit(-1)); held.limit != want {
		t.Errorf("limit = %d, want the runtime's %d", held.limit, want)
	}
}

// The runtime's own memory counts too: stacks, GC metadata and the rest are
// charged to the container and are not in the live heap. The GC is off so the
// live heap can't move between the two reads.
func TestGoMemoryReaderCountsMemoryOutsideTheHeap(t *testing.T) {
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	runtime.GC()
	live := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(live)
	if got, want := newGoMemoryReader().read().used, live[0].Value.Uint64()+1<<20; got < want {
		t.Errorf("used = %d, want at least the live heap plus 1 MiB of runtime memory, %d", got, want)
	}
}

// A metric a future runtime drops must not take the sampler down: Uint64 panics
// on one. It reads as no limit reported, so the input turns itself off.
func TestGoMemoryReaderSurvivesAnUnknownMetric(t *testing.T) {
	defer func() {
		if p := recover(); p != nil {
			t.Errorf("read panicked on an unknown metric: %v", p)
		}
	}()
	r := newGoMemoryReader()
	r.samples[1].Name = "/no/such/metric:bytes"
	if got := r.read(); got != (goMemoryStats{}) {
		t.Errorf("read = %+v, want the zero value", got)
	}
}

// One sample per second must stay cheap: a read allocates nothing.
func TestGoMemoryReaderDoesNotAllocate(t *testing.T) {
	r := newGoMemoryReader()
	if got := testing.AllocsPerRun(100, func() { r.read() }); got != 0 {
		t.Errorf("a read allocated %v times", got)
	}
}

func TestGoMemoryHeadroomRatioMustBeAShare(t *testing.T) {
	for _, ratio := range []float64{-0.1, 1.5} {
		_, err := New(Options{MaxConcurrentReconciles: 1, Saturation: SaturationOptions{Enabled: true, ExperimentalGoMemoryHeadroomRatio: ratio}})
		if err == nil || !strings.Contains(err.Error(), "ExperimentalGoMemoryHeadroomRatio") {
			t.Errorf("ratio %v: err = %v, want one naming the option", ratio, err)
		}
	}
}
