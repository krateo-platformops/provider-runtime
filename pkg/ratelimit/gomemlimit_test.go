package ratelimit

import (
	"context"
	"math"
	"runtime/debug"
	"strings"
	"testing"
	"time"
)

// fakeLimit stands in for debug.SetMemoryLimit, which is process-wide. It starts
// at the runtime's "no limit" value unless told otherwise.
type fakeLimit struct {
	current int64
	calls   []int64
}

func (f *fakeLimit) set(v int64) int64 {
	f.calls = append(f.calls, v)
	old := f.current
	if v >= 0 {
		f.current = v
	}
	return old
}

// writes returns the calls that changed the limit, leaving out reads (-1).
func (f *fakeLimit) writes() []int64 {
	var out []int64
	for _, v := range f.calls {
		if v >= 0 {
			out = append(out, v)
		}
	}
	return out
}

// limitGovernor returns a governor whose Go memory limit calls go to the
// returned fake, reading the fixture directory dir, with GOMEMLIMIT set to env.
func limitGovernor(t *testing.T, dir, env string, o SaturationOptions) (*Governor, *fakeLimit, *infoLog) {
	t.Helper()
	logs := &infoLog{}
	o.Enabled, o.CgroupDir = true, fixture(dir)
	g, _ := testGovernor(t, Options{MaxConcurrentReconciles: 4, Logger: logs, Saturation: o})
	f := &fakeLimit{current: math.MaxInt64}
	g.sat.setMemoryLimit = f.set
	g.sat.getenv = func(k string) string {
		if k == "GOMEMLIMIT" {
			return env
		}
		return ""
	}
	return g, f, logs
}

// The fixture's memory.max is 1 GiB: 0.8 of it is the limit.
func TestGoMemoryLimitComesFromMemoryMax(t *testing.T) {
	g, f, logs := limitGovernor(t, "healthy", "", SaturationOptions{ExperimentalMemory: true})
	g.sat.applyGoMemoryLimit()
	const want = int64(858_993_459) // 0.8 of 1 GiB
	if got := f.writes(); len(got) != 1 || got[0] != want {
		t.Fatalf("SetMemoryLimit writes = %v, want one of %d", got, want)
	}
	if got := logs.countWith("Go memory limit set"); got != 1 {
		t.Errorf("logged %d lines about setting the limit, want 1: %v", got, logs.lines)
	}
}

// With the memory input off the limit is never touched, not even read, and an
// option doesn't turn it on. A library changing the limit otherwise is what
// design §7 ruled out.
func TestGoMemoryLimitUntouchedWithTheMemoryInputOff(t *testing.T) {
	for name, o := range map[string]SaturationOptions{
		"defaults":    {},
		"with option": {ExperimentalGoMemoryLimit: 1 << 28},
	} {
		t.Run(name, func(t *testing.T) {
			g, f, logs := limitGovernor(t, "healthy", "", o)
			g.sat.applyGoMemoryLimit()
			if len(f.calls) != 0 {
				t.Errorf("SetMemoryLimit calls = %v, want none", f.calls)
			}
			if got := logs.countWith("Go memory limit"); got != 0 {
				t.Errorf("logged about the limit with the input off: %v", logs.lines)
			}
		})
	}
}

// An explicit value wins: GOMEMLIMIT, including "off", over everything, then a
// limit the program already set, then the option, then memory.max.
func TestGoMemoryLimitExplicitValueWins(t *testing.T) {
	const option = int64(1 << 28)
	for _, tc := range []struct {
		name    string
		env     string
		current int64
		option  int64
		want    []int64
		log     string
	}{
		{name: "GOMEMLIMIT", env: "200MiB", current: 200 << 20, log: "Go memory limit left as set by GOMEMLIMIT"},
		{name: "GOMEMLIMIT beats the option", env: "200MiB", current: 200 << 20, option: option, log: "Go memory limit left as set by GOMEMLIMIT"},
		{name: "GOMEMLIMIT=off means no limit", env: "off", current: math.MaxInt64, log: "Go memory limit left as set by GOMEMLIMIT"},
		{name: "the program's own limit", current: 300 << 20, log: "Go memory limit left as the program set it"},
		{name: "the program's limit beats the option", current: 300 << 20, option: option, log: "Go memory limit left as the program set it"},
		{name: "the option beats memory.max", current: math.MaxInt64, option: option, want: []int64{option}, log: "Go memory limit set from the option"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, f, logs := limitGovernor(t, "healthy", tc.env, SaturationOptions{ExperimentalMemory: true, ExperimentalGoMemoryLimit: tc.option})
			f.current = tc.current
			g.sat.applyGoMemoryLimit()
			got := f.writes()
			if len(got) != len(tc.want) || (len(got) == 1 && got[0] != tc.want[0]) {
				t.Errorf("SetMemoryLimit writes = %v, want %v", got, tc.want)
			}
			if logs.countWith(tc.log) != 1 {
				t.Errorf("logs = %v, want one starting %q", logs.lines, tc.log)
			}
		})
	}
}

// With no real memory.max there is nothing to derive the limit from, so it is
// left alone and the log says why.
func TestGoMemoryLimitNeedsARealMemoryMax(t *testing.T) {
	for _, dir := range []string{"no-memory-limit", "does-not-exist"} {
		t.Run(dir, func(t *testing.T) {
			g, f, logs := limitGovernor(t, dir, "", SaturationOptions{ExperimentalMemory: true})
			g.sat.applyGoMemoryLimit()
			if got := f.writes(); len(got) != 0 {
				t.Errorf("SetMemoryLimit writes = %v, want none", got)
			}
			if got := logs.countWith("Go memory limit not set"); got != 1 {
				t.Errorf("logs = %v, want one saying the limit was not set", logs.lines)
			}
		})
	}
}

func TestGoMemoryLimitOptionMustNotBeNegative(t *testing.T) {
	_, err := New(Options{MaxConcurrentReconciles: 1, Saturation: SaturationOptions{Enabled: true, ExperimentalGoMemoryLimit: -1}})
	if err == nil || !strings.Contains(err.Error(), "ExperimentalGoMemoryLimit") {
		t.Errorf("New with a negative limit: err = %v, want one naming the option", err)
	}
}

// The wiring, with the real runtime: Start sets the limit when the memory input
// is on, and leaves it alone when it is off.
func TestStartAppliesTheGoMemoryLimitToTheRuntime(t *testing.T) {
	before := debug.SetMemoryLimit(-1)
	if before != math.MaxInt64 {
		t.Skipf("the test process already has a limit (%d), so GOMEMLIMIT is set", before)
	}
	t.Cleanup(func() { debug.SetMemoryLimit(before) })

	start := func(memory bool) {
		g, err := New(Options{MaxConcurrentReconciles: 1, Saturation: SaturationOptions{
			Enabled: true, CgroupDir: fixture("healthy"), SamplePeriod: time.Hour, ExperimentalMemory: memory,
		}})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Start does its one-time work, then sees the context is done.
		if err := g.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}

	start(false)
	if got := debug.SetMemoryLimit(-1); got != before {
		t.Fatalf("with the memory input off Start changed the limit to %d", got)
	}
	start(true)
	const want = int64(858_993_459) // 0.8 of 1 GiB
	if got := debug.SetMemoryLimit(-1); got != want {
		t.Errorf("with the memory input on the limit = %d, want %d", got, want)
	}
}
