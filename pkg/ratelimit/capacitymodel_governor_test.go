package ratelimit

import (
	"context"
	"math"
	"testing"
	"time"
)

// The capacity model in the Governor: with the CPU pressure step, and where it
// applies (docs/design-capacity-model.md §4, §5 and §9).

// modelFeed is a cgroup reader whose CPU usage, stall and limit the test moves,
// a second at a time, with the governor's clock and the work its queues would
// have reported.
type modelFeed struct {
	g            *Governor
	c            *clock
	stall, usage time.Duration
	limit        cpuLimit
	// usageWhy, when set, makes cpu.stat unreadable.
	usageWhy   string
	workingSet reading
	logs       *infoLog
}

// load is what one second holds: the cores used, the share of it some task
// stalled, the reconciles in flight, and how many completed and arrived.
type load struct {
	used, stalled, inFlight float64
	perSecond               int64
}

var (
	// idleLoad is a second with no reconcile: 20m of background CPU.
	idleLoad = load{used: 0.02}
	// atTarget is a second at the 450m target of a 500m limit.
	atTarget = func(inFlight float64) load { return load{used: 0.45, inFlight: inFlight, perSecond: 30} }
	// pinned is a second at the 500m limit, with or without stall.
	pinned = func(inFlight, stalled float64) load {
		return load{used: 0.5, stalled: stalled, inFlight: inFlight, perSecond: 30}
	}
)

// newModelFeed returns a governor with a ceiling of 16 reading the feed, after
// its first sample, at a 500m CPU limit.
func newModelFeed(t *testing.T, o SaturationOptions) *modelFeed {
	t.Helper()
	o.Enabled, o.CgroupDir = true, fixture("healthy")
	logs := &infoLog{}
	g, c := testGovernor(t, Options{MaxConcurrentReconciles: 16, RecoveryInterval: testRecovery, Saturation: o, Logger: logs})
	f := &modelFeed{g: g, c: c, limit: cpuLimit{set: true, cores: 0.5}, workingSet: disabled(memoryInputOff), logs: logs}
	g.sat.reader = f
	g.sample()
	return f
}

func (f *modelFeed) read() cgroupSample {
	smp := cgroupSample{cpuPressure: reading{enabled: true}, cpuStall: f.stall, workingSet: f.workingSet, cpuLimit: f.limit}
	if f.usageWhy != "" {
		smp.cpuUsage = cpuUsage{why: f.usageWhy}
	} else {
		smp.cpuUsage = cpuUsage{ok: true, total: f.usage}
	}
	return smp
}

// seconds takes n samples a second apart, each after a second of l, and
// returns the local capacity.
func (f *modelFeed) seconds(n int, l load) int {
	for range n {
		f.c.add(time.Second)
		f.usage += time.Duration(l.used * float64(time.Second))
		f.stall += time.Duration(l.stalled * float64(time.Second))
		f.g.mu.Lock()
		f.g.slotTime += time.Duration(l.inFlight * float64(time.Second))
		f.g.mu.Unlock()
		f.g.completions.Add(l.perSecond)
		f.g.arrivals.Add(l.perSecond)
		f.g.sample()
	}
	return f.g.LocalCapacity()
}

// modelCapacity returns M as the snapshot reports it, and whether it does.
func (f *modelFeed) modelCapacity() (float64, bool) {
	if m := f.g.Snapshot().CPU.ModelCapacity; m != nil {
		return *m, true
	}
	return 0, false
}

// appliesLines counts the log lines that say the law applies, or doesn't.
func (f *modelFeed) appliesLines() int {
	return f.logs.countWith("Capacity model applies") + f.logs.countWith("Capacity model does not apply")
}

// stepCapacity returns the step's own capacity P.
func (f *modelFeed) stepCapacity() int { return f.g.LocalCapacity() + f.g.sat.stepAbove }

// settle ramps the step to the ceiling of 16 over 30 idle seconds, which also
// measure a background of 20m, and then runs one interval pinned at the limit
// with every slot used. The law gives 16 × 430 ÷ 480 = 14.3.
func (f *modelFeed) settle() int {
	f.seconds(30, idleLoad)
	return f.seconds(10, pinned(16, 0))
}

func modelOn() SaturationOptions { return SaturationOptions{CapacityModel: true} }

const settledM = 16 * 0.43 / 0.48

// Local capacity is the lower of the step's and the floor of the model's.
func TestLocalIsTheLowerOfTheStepAndTheModel(t *testing.T) {
	f := newModelFeed(t, modelOn())
	// Until the law has evidence, M is the ceiling and the step alone governs.
	for want := 2; want <= 16; want++ {
		f.seconds(1, idleLoad)
		if got := f.seconds(1, idleLoad); got != want {
			t.Fatalf("on the startup ramp local capacity = %d, want %d: the step's", got, want)
		}
		if m, ok := f.modelCapacity(); !ok || m != 16 {
			t.Fatalf("with no evidence yet M = %v (reported %v), want the ceiling 16", m, ok)
		}
	}
	for i := 1; i < 10; i++ {
		if got := f.seconds(1, pinned(16, 0)); got != 16 {
			t.Fatalf("%ds into the interval local capacity = %d, want 16: M moves when the interval ends", i, got)
		}
	}
	if got := f.seconds(1, pinned(16, 0)); got != 14 {
		t.Fatalf("local capacity = %d, want 14: the floor of M", got)
	}
	if m, _ := f.modelCapacity(); !near(m, settledM) {
		t.Errorf("M = %v, want %v", m, settledM)
	}
	if p := f.stepCapacity(); p != 16 {
		t.Errorf("the step's capacity = %d, want it still 16: the model lowered local, not the step", p)
	}
	cpu := f.g.Snapshot().CPU
	if cpu.Limit == nil || *cpu.Limit != 0.5 || cpu.Used == nil || !near(*cpu.Used, 0.5) || cpu.Background == nil || !near(*cpu.Background, 0.02) {
		t.Errorf("the snapshot's CPU = limit %v, used %v, background %v; want 0.5, 0.5 and 0.02", cpu.Limit, cpu.Used, cpu.Background)
	}
	// 30 reconciles a second at 480m above the background: 16ms each.
	if cpu.PerReconcile == nil || !near(*cpu.PerReconcile, 0.016) {
		t.Errorf("CPU per reconcile = %v, want 0.016s", cpu.PerReconcile)
	}
	if want := (0.02 + 0.016*30) / 0.9; cpu.Required == nil || !near(*cpu.Required, want) {
		t.Errorf("required CPU = %v, want %v", cpu.Required, want)
	}
	// At the target, with the step above the model, nothing moves.
	for range 20 {
		if got := f.seconds(1, atTarget(14)); got != 14 {
			t.Fatalf("at the target local capacity = %d, want 14", got)
		}
	}
	for _, c := range f.g.Snapshot().Cuts {
		if c.Cuts != 0 {
			t.Errorf("%d cut(s) by %s counted: the model lowering local is not a cut by the step", c.Cuts, c.Input)
		}
	}
}

// A CPU cut halves the published local capacity, not the step's own, and pulls
// M down to what it left: 14 running become 7, not the 8 that half of P is.
func TestACutHalvesLocalAndPullsTheModelDown(t *testing.T) {
	f := newModelFeed(t, modelOn())
	if got := f.settle(); got != 14 || f.stepCapacity() != 16 {
		t.Fatalf("settled at local %d with the step at %d, want 14 and 16", got, f.stepCapacity())
	}
	// Over the threshold at the sixth stalled second of a whole window.
	if got := f.seconds(5, pinned(14, 1)); got != 14 {
		t.Fatalf("after five stalled seconds local capacity = %d, want 14", got)
	}
	if got := f.seconds(1, pinned(14, 1)); got != 7 {
		t.Fatalf("after the cut local capacity = %d, want 14 halved to 7", got)
	}
	if p := f.stepCapacity(); p != 7 {
		t.Errorf("the step's capacity = %d, want 7: it halves what is running", p)
	}
	if m, _ := f.modelCapacity(); m != 7 {
		t.Errorf("M = %v after the cut, want 7: the law resumes from what the cut left", m)
	}
	cuts := map[string]int64{}
	for _, c := range f.g.Snapshot().Cuts {
		cuts[c.Input] = c.Cuts
	}
	if cuts[InputCPUPressure] != 1 || cuts[InputMemoryWorkingSet] != 0 {
		t.Errorf("cuts = %v, want one by cpu_pressure", cuts)
	}
	// The interval ends still over the target, with more in flight on average
	// than M: the law must not raise M on that.
	if got := f.seconds(4, pinned(7, 1)); got != 7 {
		t.Fatalf("local capacity = %d, want 7: one cut per window, and no rise over the target", got)
	}
	if m, _ := f.modelCapacity(); m != 7 {
		t.Errorf("M = %v, want 7", m)
	}
}

// After a cut the step climbs back by one per StartupRampInterval, and stops
// mattering once it passes M: local stays where the model holds it until the
// law finds evidence to rise.
func TestTheStepClimbingPastTheModelChangesNothing(t *testing.T) {
	f := newModelFeed(t, modelOn())
	f.settle()
	f.seconds(6, pinned(14, 1))
	f.seconds(4, pinned(7, 1))
	for i := 1; i <= 40; i++ {
		if got := f.seconds(1, atTarget(7)); got != 7 {
			t.Fatalf("%ds after the stall local capacity = %d, want 7: the step climbing past M changes nothing", i, got)
		}
	}
	if p := f.stepCapacity(); p != 16 {
		t.Fatalf("the step's capacity = %d, want it back at the ceiling 16", p)
	}
	// Every slot used at 200m: wanted is 7 × 430 ÷ 180 = 16.7, capped at
	// twice M.
	if got := f.seconds(10, load{used: 0.2, inFlight: 7, perSecond: 30}); got != 14 {
		t.Errorf("with evidence to rise local capacity = %d, want 14: twice M, under the step's 16", got)
	}
}

// A cut by the experimental memory input is a cut by the step too.
func TestAMemoryCutPullsTheModelDown(t *testing.T) {
	f := newModelFeed(t, SaturationOptions{CapacityModel: true, ExperimentalMemoryWorkingSet: true})
	f.workingSet = reading{enabled: true, percent: 10}
	f.settle()
	f.workingSet.percent = 85
	if got := f.seconds(1, atTarget(14)); got != 7 {
		t.Fatalf("working set 85%%: local capacity = %d, want 14 halved to 7", got)
	}
	if m, _ := f.modelCapacity(); m != 7 {
		t.Errorf("M = %v, want 7", m)
	}
	f.workingSet.percent = 95
	if got := f.seconds(1, atTarget(7)); got != 1 {
		t.Fatalf("working set 95%%: local capacity = %d, want 1", got)
	}
	if m, _ := f.modelCapacity(); m != 1 {
		t.Errorf("M = %v, want 1", m)
	}
	for _, c := range f.g.Snapshot().Cuts {
		if want := map[string]int64{InputCPUPressure: 0, InputMemoryWorkingSet: 2}[c.Input]; c.Cuts != want {
			t.Errorf("%d cut(s) by %s, want %d", c.Cuts, c.Input, want)
		}
	}
}

// The law runs only with a CPU limit of at least MinCPULimit and a readable
// cpu.stat. Otherwise M is the ceiling, nothing of the model is reported, and
// the step alone governs.
func TestTheLawAppliesOnlyWhereItsPremiseHolds(t *testing.T) {
	for _, tc := range []struct {
		name      string
		opts      SaturationOptions
		set       func(*modelFeed)
		applies   bool
		wantLimit float64
	}{
		{name: "a 500m limit", opts: modelOn(), set: func(*modelFeed) {}, applies: true, wantLimit: 0.5},
		{name: "no limit", opts: modelOn(), set: func(f *modelFeed) { f.limit = cpuLimit{why: "no CPU limit (cpu.max is max)"} }},
		{name: "a limit under 500m", opts: modelOn(), set: func(f *modelFeed) { f.limit.cores = 0.499 }, wantLimit: 0.499},
		{name: "cpu.stat missing", opts: modelOn(), set: func(f *modelFeed) { f.usageWhy = "cpu.stat not found" }, wantLimit: 0.5},
		{name: "the option off", opts: SaturationOptions{}, set: func(*modelFeed) {}, wantLimit: 0.5},
		{name: "a 250m limit with MinCPULimit lowered", opts: SaturationOptions{CapacityModel: true, MinCPULimit: 0.25},
			set: func(f *modelFeed) { f.limit.cores = 0.25 }, applies: true, wantLimit: 0.25},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newModelFeed(t, tc.opts)
			tc.set(f)
			f.seconds(30, idleLoad)
			// Pinned at whatever the limit is, every slot used.
			busy := pinned(16, 0)
			if f.limit.set {
				busy.used = f.limit.cores
				busy.perSecond = 30
			}
			got := f.seconds(10, busy)
			_, reported := f.modelCapacity()
			cpu := f.g.Snapshot().CPU
			if tc.applies {
				if got >= 16 || !reported || cpu.Used == nil {
					t.Errorf("local capacity = %d, M reported %v, U %v; want the law to have lowered local and both reported", got, reported, cpu.Used)
				}
			} else {
				if got != 16 {
					t.Errorf("local capacity = %d, want 16: the step alone governs", got)
				}
				if reported || cpu.Used != nil || cpu.Background != nil || cpu.PerReconcile != nil || cpu.Required != nil {
					t.Errorf("the model reported values while the law doesn't apply: %+v", cpu)
				}
			}
			// The limit is reported whenever there is one, law or not.
			switch {
			case tc.wantLimit == 0 && cpu.Limit != nil:
				t.Errorf("CPU limit reported as %v with none", *cpu.Limit)
			case tc.wantLimit != 0 && (cpu.Limit == nil || *cpu.Limit != tc.wantLimit):
				t.Errorf("CPU limit = %v, want %v", cpu.Limit, tc.wantLimit)
			}
			// Said once per change, not once per sample: the feed's first
			// sample was at a 500m limit, where the law applies. Never with the
			// option off.
			wantLines := 2
			switch {
			case !tc.opts.CapacityModel:
				wantLines = 0
			case tc.applies:
				wantLines = 1
			}
			if n := f.appliesLines(); n != wantLines {
				t.Errorf("%d capacity model log lines over 41 samples, want %d: %v", n, wantLines, f.logs.lines)
			}
		})
	}
}

// Where the law doesn't apply, local capacity is exactly what the option off
// gives, sample for sample: through the startup ramp, a load pinned at the
// limit, a stall that cuts, and the recovery.
func TestWithoutTheLawLocalIsExactlyTheStep(t *testing.T) {
	script := []struct {
		n int
		l load
	}{
		{30, idleLoad},
		{10, load{used: 0.25, inFlight: 16, perSecond: 30}},
		{8, load{used: 0.25, stalled: 1, inFlight: 16, perSecond: 30}},
		{14, load{used: 0.25, stalled: 0.2, inFlight: 8, perSecond: 30}},
		{12, load{used: 0.25, stalled: 1, inFlight: 8, perSecond: 30}},
		{40, load{used: 0.1, inFlight: 4, perSecond: 30}},
	}
	for name, set := range map[string]func(*modelFeed){
		"a 250m limit":     func(f *modelFeed) { f.limit.cores = 0.25 },
		"no limit":         func(f *modelFeed) { f.limit = cpuLimit{why: "no CPU limit (cpu.max is max)"} },
		"cpu.stat missing": func(f *modelFeed) { f.usageWhy = "cpu.stat not found" },
	} {
		t.Run(name, func(t *testing.T) {
			off := newModelFeed(t, SaturationOptions{})
			on := newModelFeed(t, modelOn())
			set(on)
			second, cuts := 0, 0
			for _, step := range script {
				for range step.n {
					second++
					before := off.g.LocalCapacity()
					want, got := off.seconds(1, step.l), on.seconds(1, step.l)
					if want < before {
						cuts++
					}
					if got != want {
						t.Fatalf("second %d: local capacity = %d with the law not applying, %d with the option off", second, got, want)
					}
					if a, b := on.g.local.Load().cause, off.g.local.Load().cause; a != b {
						t.Fatalf("second %d: cause %q with the law not applying, %q with the option off", second, a, b)
					}
				}
			}
			if cuts < 2 {
				t.Errorf("the script cut %d time(s), want at least 2: it must exercise the step", cuts)
			}
		})
	}
}

// An in-place resize is picked up at the next sample: under MinCPULimit the
// law stops and local is the step's again; back above it, the law starts from
// the ceiling.
func TestAResizeIsPickedUpAtTheNextSample(t *testing.T) {
	f := newModelFeed(t, modelOn())
	if got := f.settle(); got != 14 {
		t.Fatalf("settled at %d, want 14", got)
	}
	f.limit.cores = 0.25
	if got := f.seconds(1, atTarget(14)); got != 16 {
		t.Fatalf("a sample after the limit fell to 250m local capacity = %d, want the step's 16", got)
	}
	if m, ok := f.modelCapacity(); ok {
		t.Errorf("M reported as %v at a 250m limit", m)
	}
	if l := f.g.Snapshot().CPU.Limit; l == nil || *l != 0.25 {
		t.Errorf("CPU limit = %v, want 0.25", l)
	}
	f.limit.cores = 1
	if got := f.seconds(1, atTarget(16)); got != 16 {
		t.Fatalf("local capacity = %d a sample after the limit rose to 1 CPU, want 16", got)
	}
	if m, ok := f.modelCapacity(); !ok || m != 16 {
		t.Errorf("M = %v (reported %v), want the ceiling 16: the law starts again", m, ok)
	}
	// One whole interval at the new limit, pinned at it with 15 in flight. The
	// background was forgotten with the rest, so it is 0: 15 × 900 ÷ 1000.
	if got := f.seconds(10, load{used: 1, inFlight: 15, perSecond: 30}); got != 13 {
		t.Errorf("local capacity = %d, want 13: the floor of 13.5", got)
	}
	// A limit that changes while the law applies moves nothing in its
	// interval: judged against the old limit, this one would lower M to 11.7.
	f.seconds(5, load{used: 1, inFlight: 13, perSecond: 30})
	f.limit.cores = 2
	if got := f.seconds(5, load{used: 1, inFlight: 13, perSecond: 30}); got != 13 {
		t.Errorf("local capacity = %d in the interval the limit changed in, want 13 unchanged", got)
	}
	if n := f.appliesLines(); n != 3 {
		t.Errorf("%d capacity model log lines, want 3 (applies, does not, applies): %v", n, f.logs.lines)
	}
}

func TestCapacityModelOptions(t *testing.T) {
	o, err := SaturationOptions{}.withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if o.CapacityModel || o.CPUTarget != 0.9 || o.MinCPULimit != 0.5 || o.ModelInterval != 10*time.Second {
		t.Errorf("defaults = %+v, want the model off, a target of 0.9, a minimum limit of 0.5 and a 10s interval", o)
	}
	for name, bad := range map[string]SaturationOptions{
		"a negative target":     {CPUTarget: -0.1},
		"a target over 1":       {CPUTarget: 1.01},
		"a negative limit":      {MinCPULimit: -1},
		"a negative interval":   {ModelInterval: -time.Second},
		"a target not a number": {CPUTarget: math.NaN()},
	} {
		if _, err := bad.withDefaults(); err == nil {
			t.Errorf("%s was accepted", name)
		}
		bad.Enabled = true
		if _, err := New(Options{MaxConcurrentReconciles: 1, Saturation: bad}); err == nil {
			t.Errorf("New accepted %s", name)
		}
	}
	if o, err := (SaturationOptions{CPUTarget: 1, MinCPULimit: 2, ModelInterval: time.Minute}).withDefaults(); err != nil || o.CPUTarget != 1 || o.MinCPULimit != 2 || o.ModelInterval != time.Minute {
		t.Errorf("valid settings: %+v, %v", o, err)
	}
	// The model needs the saturation input: without it the option is ignored,
	// like the experimental memory input.
	g, err := New(Options{MaxConcurrentReconciles: 4, Saturation: SaturationOptions{CapacityModel: true}})
	if err != nil {
		t.Fatal(err)
	}
	if g.SaturationEnabled() || g.LocalCapacity() != 4 {
		t.Errorf("CapacityModel without Enabled: saturation on %v, local capacity %d; want off and 4", g.SaturationEnabled(), g.LocalCapacity())
	}
}

// cpu.max and cpu.stat, read from files.
func TestCPUFixturesParse(t *testing.T) {
	smp := cgroupFiles{dir: fixture("cpu-limited"), usage: true}.read()
	if !smp.cpuLimit.set || smp.cpuLimit.cores != 0.5 {
		t.Errorf("cpu.max 50000 100000 = %+v, want a limit of 0.5 cores", smp.cpuLimit)
	}
	if !smp.cpuUsage.ok || smp.cpuUsage.total != 123456789*time.Microsecond {
		t.Errorf("cpu.stat usage_usec = %+v, want 123.456789s", smp.cpuUsage)
	}
	if !smp.cpuPressure.enabled {
		t.Errorf("cpu.pressure = %+v, want it still read", smp.cpuPressure)
	}

	smp = cgroupFiles{dir: fixture("cpu-unlimited"), usage: true}.read()
	if smp.cpuLimit.set || smp.cpuLimit.why != "no CPU limit (cpu.max is max)" {
		t.Errorf("cpu.max max = %+v, want no limit", smp.cpuLimit)
	}
	if !smp.cpuUsage.ok {
		t.Errorf("cpu.stat = %+v, want it read with no limit", smp.cpuUsage)
	}

	// cpu.stat is the model's: with the option off it isn't read. cpu.max is.
	smp = cgroupFiles{dir: fixture("cpu-limited")}.read()
	if smp.cpuUsage.ok || smp.cpuUsage.why != capacityModelOff {
		t.Errorf("cpu.stat with the model off = %+v, want it not read", smp.cpuUsage)
	}
	if !smp.cpuLimit.set {
		t.Errorf("cpu.max with the model off = %+v, want it read", smp.cpuLimit)
	}

	// Files that are not there name themselves.
	smp = cgroupFiles{dir: fixture("healthy"), usage: true}.read()
	if smp.cpuLimit.set || smp.cpuLimit.why != "cpu.max not found" || smp.cpuUsage.ok || smp.cpuUsage.why != "cpu.stat not found" {
		t.Errorf("with neither file: limit %+v, usage %+v", smp.cpuLimit, smp.cpuUsage)
	}
	smp = cgroupFiles{dir: fixture("does-not-exist"), usage: true}.read()
	if smp.cpuLimit.set || smp.cpuUsage.ok || smp.cpuLimit.why == "" || smp.cpuUsage.why == "" {
		t.Errorf("with no cgroup directory: limit %+v, usage %+v", smp.cpuLimit, smp.cpuUsage)
	}
}

// The model on real files: New wires the reader to read cpu.stat, and the law
// applies at the fixture's 500m limit.
func TestCapacityModelReadsItsFiles(t *testing.T) {
	g, c := testGovernor(t, Options{MaxConcurrentReconciles: 4, Saturation: SaturationOptions{
		Enabled: true, CapacityModel: true, CgroupDir: fixture("cpu-limited"),
	}})
	g.sample()
	c.add(time.Second)
	g.sample()
	cpu := g.Snapshot().CPU
	if cpu.Limit == nil || *cpu.Limit != 0.5 {
		t.Errorf("CPU limit = %v, want 0.5", cpu.Limit)
	}
	if cpu.ModelCapacity == nil || *cpu.ModelCapacity != 4 {
		t.Errorf("M = %v, want the ceiling 4: the law applies and has no evidence", cpu.ModelCapacity)
	}
	if cpu.Used != nil {
		t.Errorf("U reported as %v before an interval ended", *cpu.Used)
	}
}

// With the model on, the sampler's tick is still the only reader of the cgroup
// files: Acquire, the queues and the snapshot read nothing.
func TestOnlyTheSamplerReadsWithTheModelOn(t *testing.T) {
	g, c := testGovernor(t, Options{MaxConcurrentReconciles: 2, Saturation: SaturationOptions{
		Enabled: true, CapacityModel: true, CgroupDir: fixture("cpu-limited"),
	}})
	stub := &stubReader{t: t, smp: cgroupSample{
		cpuPressure: reading{enabled: true},
		workingSet:  disabled(memoryInputOff),
		cpuLimit:    cpuLimit{set: true, cores: 0.5},
		cpuUsage:    cpuUsage{ok: true},
	}}
	g.sat.reader = stub
	tick := func() {
		stub.inTick.Store(true)
		g.sample()
		stub.inTick.Store(false)
	}
	tick()
	c.add(DefaultStartupRampInterval)
	tick()
	if stub.reads.Load() != 2 {
		t.Fatalf("two ticks read the files %d times, want 2", stub.reads.Load())
	}

	q := gated(t, g, "no-io")
	for range 200 {
		q.Add("a")
		q.Add("b")
		for range 2 {
			q.Done(popWithin(t, q))
		}
		if release, _ := g.Acquire(context.Background(), "h"); release != nil {
			release()
		}
		g.Snapshot()
	}
	if n := stub.outside.Load(); n != 0 {
		t.Errorf("the cgroup files were read %d times outside the sampler's tick", n)
	}
	if n := stub.reads.Load(); n != 2 {
		t.Errorf("the files were read %d times, want only the 2 ticks", n)
	}
}

// What the queues report: the time slots were held, summed over the slots,
// from each grant to its Done; completions; and arrivals, where an item added
// again while it waits counts once.
func TestQueuesReportSlotTimeCompletionsAndArrivals(t *testing.T) {
	g, c := saturationGovernor(t, 4, "healthy", false)
	g.setLocalCapacity(2)
	q := gated(t, g, "reports")
	slotTime := func() time.Duration {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.advanceSlotTimeLocked(g.now())
		return g.slotTime
	}

	q.Add("a")
	q.Add("a") // again while it waits: one arrival
	q.Add("b")
	q.AddAfter("later", time.Hour) // not ready: no arrival
	if got := g.arrivals.Load(); got != 2 {
		t.Fatalf("%d arrivals, want 2", got)
	}
	first := popWithin(t, q)
	c.add(3 * time.Second) // one slot held for 3s
	second := popWithin(t, q)
	c.add(2 * time.Second) // two slots held for 2s
	if got := slotTime(); got != 7*time.Second {
		t.Fatalf("slot time = %s, want 7s: 3s of one slot and 2s of two", got)
	}
	q.Add(first) // while it is processed: it arrives when its Done makes it ready
	if got := g.arrivals.Load(); got != 2 {
		t.Fatalf("%d arrivals after a re-add during processing, want 2 until its Done", got)
	}
	q.Done(first)
	if got := g.arrivals.Load(); got != 3 {
		t.Fatalf("%d arrivals after the Done, want 3", got)
	}
	if got := g.completions.Load(); got != 1 {
		t.Fatalf("%d completions, want 1", got)
	}
	// The freed slot goes to the re-added item: two slots are held again.
	third := popWithin(t, q)
	c.add(time.Second)
	if got := slotTime(); got != 9*time.Second {
		t.Fatalf("slot time = %s, want 9s", got)
	}
	q.Done(second)
	q.Done(third)
	c.add(time.Minute) // nothing held: no slot time
	if got := slotTime(); got != 9*time.Second {
		t.Errorf("slot time = %s with nothing in flight for a minute, want 9s", got)
	}
	if got := g.completions.Load(); got != 3 {
		t.Errorf("%d completions, want 3", got)
	}
	cs := g.Snapshot().Controllers
	if len(cs) != 1 || cs[0].Controller != "reports" || cs[0].Arrivals != 3 || cs[0].Completions != 3 || cs[0].InFlight != 0 || cs[0].Waiting != 0 {
		t.Errorf("the snapshot's controllers = %+v, want reports with 3 arrivals, 3 completions and nothing held or waiting", cs)
	}
}

// The sampler hands the model what the queues reported: a real queue's
// reconciles, at the limit, lower M.
func TestTheSamplerFeedsTheModelFromTheQueues(t *testing.T) {
	f := newModelFeed(t, modelOn())
	f.g.setLocalCapacity(16)
	q := gated(t, f.g, "fed")
	const inFlight = 8
	for i := range inFlight {
		q.Add(string(rune('a' + i)))
	}
	held := make([]string, 0, inFlight)
	for range inFlight {
		held = append(held, popWithin(t, q))
	}
	// Ten seconds pinned at the limit with 8 in flight, each replaced three
	// times a second: the queue reports the slot time and the completions.
	for range 10 {
		f.c.add(time.Second)
		f.usage += 500 * time.Millisecond
		for range 3 {
			k := held[0]
			q.Add(k)
			q.Done(k)
			held = append(held[1:], popWithin(t, q))
		}
		f.g.sample()
	}
	if m, _ := f.modelCapacity(); !near(m, inFlight*0.45/0.5) {
		t.Errorf("M = %v, want %v: 8 in flight at the limit", m, inFlight*0.45/0.5)
	}
	for _, k := range held {
		q.Done(k)
	}
}
