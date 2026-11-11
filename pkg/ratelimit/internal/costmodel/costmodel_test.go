package costmodel

import (
	"math"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const ms = time.Millisecond

func near(t *testing.T, what string, got, want, tol float64) {
	t.Helper()
	if math.Abs(got-want) > tol {
		t.Errorf("%s = %v, want %v (±%v)", what, got, want, tol)
	}
}

// TestNoIO pins the package's rule: no file, clock or runtime I/O, so the
// simulation and the Governor run the same code (plan §2).
func TestNoIO(t *testing.T) {
	// Direct imports: the standard library's own packages reach syscall
	// through time, which is not I/O done here.
	// internal/fpmath, the portable elementary functions, may import math alone.
	const fpmath = "github.com/krateo-platformops/provider-runtime/pkg/ratelimit/internal/costmodel/internal/fpmath"
	for _, c := range []struct {
		pkg     string
		allowed map[string]bool
	}{
		{".", map[string]bool{"math": true, "time": true, "sort": true, fpmath: true}},
		{"./internal/fpmath", map[string]bool{"math": true}},
	} {
		out, err := exec.Command("go", "list", "-f", "{{join .Imports \" \"}}", c.pkg).Output()
		if err != nil {
			t.Skipf("go list: %v", err)
		}
		for _, dep := range strings.Fields(string(out)) {
			if !c.allowed[dep] {
				t.Errorf("%s imports %s; only %v are allowed", c.pkg, dep, c.allowed)
			}
		}
	}
}

func TestBooksAccrueAndCarry(t *testing.T) {
	b := NewBooks(0)
	b.Grant(0, "a")
	b.Commit(0)
	b.Grant(200*ms, "b")
	b.Commit(200 * ms)
	b.Release(600*ms, "a", true, 0)
	b.Commit(600 * ms)
	// A change after the read instant belongs to the next interval (L5).
	b.Release(1100*ms, "b", true, 200*ms)
	b.Commit(1100 * ms)

	iv := b.Close(time.Second)
	// a: 0–0.6s; b: 0.2–1.0s.
	near(t, "A", iv.SlotSeconds, 0.6+0.8, 1e-9)
	near(t, "A_a", iv.Queues["a"].SlotSeconds, 0.6, 1e-9)
	near(t, "A_b", iv.Queues["b"].SlotSeconds, 0.8, 1e-9)
	if iv.Completions != 1 || iv.Queues["a"].Completions != 1 {
		t.Errorf("completions %d (a %d), want 1 (a 1)", iv.Completions, iv.Queues["a"].Completions)
	}
	if iv.NMin != 1 || iv.NMax != 2 {
		t.Errorf("N± = %d, %d, want 1, 2", iv.NMin, iv.NMax)
	}
	iv = b.Close(2 * time.Second)
	near(t, "A next", iv.SlotSeconds, 0.1, 1e-9)
	if iv.Completions != 1 {
		t.Errorf("next interval's completions %d, want the carried 1", iv.Completions)
	}
}

func TestBooksZeroLengthDip(t *testing.T) {
	b := NewBooks(0)
	b.Grant(0, "a")
	b.Grant(0, "a")
	b.Commit(0)
	// A release re-granted in the same critical section isn't a dip (L6).
	b.Release(500*ms, "a", true, 0)
	b.Grant(500*ms, "a")
	b.Commit(500 * ms)
	iv := b.Close(time.Second)
	if iv.NMin != 2 {
		t.Errorf("N⁻ = %d, want 2: the dip lasted no time", iv.NMin)
	}
}

func TestBooksHandBackIsNotACompletion(t *testing.T) {
	b := NewBooks(0)
	b.Grant(0, "a")
	b.Commit(0)
	b.Release(100*ms, "a", false, 0)
	b.Commit(100 * ms)
	if iv := b.Close(time.Second); iv.Completions != 0 {
		t.Errorf("a handed-back grant counted as a completion")
	}
}

func TestRatioWarmStartHoldOutAndRestart(t *testing.T) {
	var r ratio
	for i := 0; i < 20; i++ {
		x := 1.0 + float64(0.01*float64(i%3-1))
		if r.add(x, 1, 0.1, true) != averaged {
			t.Fatalf("sample %d not averaged", i)
		}
	}
	v, _ := r.value()
	near(t, "mean", v, 1, 0.02)
	// One outlier is deferred: the estimate doesn't move yet.
	if r.add(5, 1, 0.1, true) != heldOut {
		t.Fatal("an outlier was averaged in")
	}
	if v2, _ := r.value(); v2 != v {
		t.Errorf("a deferred sample moved the estimate: %v to %v", v, v2)
	}
	// A second on the same side restarts from the two.
	if r.add(5, 1, 0.1, true) != restarted {
		t.Fatal("two outliers in a row didn't restart")
	}
	if v3, _ := r.value(); v3 != 5 || r.n != 2 {
		t.Errorf("after the restart the estimate is %v with n %d, want 5 with 2", v3, r.n)
	}
	// Warm start: the first samples are a plain mean.
	var w ratio
	w.add(1, 1, 0.01, false)
	w.add(3, 1, 0.01, false)
	if v, _ := w.value(); v != 2 {
		t.Errorf("warm start: %v, want the plain mean 2", v)
	}
	if !math.IsInf(w.seRatio(), 1) {
		t.Error("a standard error before the third sample")
	}
}

func sample(dt time.Duration, used, limit float64, a float64, c int) Sample {
	return Sample{
		Dt: dt, Usage: time.Duration(used * float64(dt)), Limit: limit, Period: 100 * ms,
		Throttling: true, Periods: 10, Throttled: 0, Procs: 2, Slice: 5 * ms,
		StallPresent: true,
		Books:        Interval{SlotSeconds: a, Completions: c, Queues: map[string]QueueInterval{"q": {SlotSeconds: a, Completions: c}}, NMin: int(a), NMax: int(a)},
	}
}

func cpuCfg() CPUConfig {
	return CPUConfig{Target: 0.9, PressureThreshold: 50, SamplePeriod: time.Second}
}

func TestKinds(t *testing.T) {
	kindOf := func(s Sample) Kind {
		c := NewCPU(cpuCfg())
		return c.Step(s, 5, 5, 16).Kind
	}
	if k := kindOf(sample(time.Second, 0.01, 0.25, 0, 0)); k != Idle {
		t.Errorf("nothing in flight: %v", k)
	}
	s := sample(time.Second, 0.25, 0.25, 5, 10)
	s.Throttled = 10
	if k := kindOf(s); k != Capped {
		t.Errorf("every period throttled at the cap: %v", k)
	}
	// Throttled every period but usage beyond the slack below the cap: active.
	s.Usage = time.Duration(0.2 * float64(time.Second))
	if k := kindOf(s); k != Active {
		t.Errorf("throttled but 50 ms/s under the cap: %v", k)
	}
	// Usage within the slack (2 × 5ms in a 1s sample) of the cap: capped.
	s.Usage = time.Duration(0.241 * float64(time.Second))
	if k := kindOf(s); k != Capped {
		t.Errorf("throttled and within the slack: %v", k)
	}
	// §17.8 item 1: at the cap's usage but with most periods unthrottled, as
	// one thread's burst at one slot: not capped. One boundary period per
	// sample is allowed.
	s.Usage = time.Duration(0.25 * float64(time.Second))
	s.Throttled = 2
	if k := kindOf(s); k != Active {
		t.Errorf("two of ten periods throttled: %v", k)
	}
	s.Throttled = 9
	if k := kindOf(s); k != Capped {
		t.Errorf("nine of ten periods throttled, one boundary period: %v", k)
	}
}

// TestLawSettlesAndGuards drives the CPU law with a linear plant: CPU per slot
// 0.05 cores, no background, a 0.25-core limit.
func TestLawSettlesAndGuards(t *testing.T) {
	m := NewModel(Config{CPU: cpuCfg()})
	S := 1.0
	for i := 0; i < 120; i++ {
		used := math.Min(0.25, 0.05*S)
		r := m.Step(sample(time.Second, used, 0.25, S, int(10*S)), 16)
		S = r.Slots
	}
	near(t, "settled S", S, 0.9*0.25/0.05, 0.05)
}

func TestErrorDiffusionAverages(t *testing.T) {
	m := &Model{cpu: NewCPU(cpuCfg()), prev: 4.56, capacity: 4}
	sum := 0
	const n = 1000
	for i := 0; i < n; i++ {
		// Hold S at 4.56 by publishing directly through the carry.
		whole := math.Floor(4.56 + m.carry)
		m.carry = 4.56 + m.carry - whole
		if m.carry < 0 || m.carry >= 1 {
			t.Fatalf("carry %v outside [0, 1)", m.carry)
		}
		sum += int(whole)
	}
	near(t, "mean capacity", float64(sum)/n, 4.56, 1.0/n+1e-9)
}

func TestPressureHoldsAtOnsetValue(t *testing.T) {
	m := NewModel(Config{CPU: cpuCfg()})
	m.prev = 4
	s := sample(time.Second, 0.1, 0.25, 4, 40)
	s.Stall = 600 * ms // 60%: over the threshold
	r := m.Step(s, 16)
	if r.Slots > 4 {
		t.Errorf("pressure let the slots rise: %v", r.Slots)
	}
}

// §17.1: a deferred sample that the next doesn't confirm is folded back with
// the weight it would have had, so the sums equal the sums with every sample.
func TestDeferralLosesNothing(t *testing.T) {
	var tested, plain ratio
	xs := []float64{1, 1.01, 0.99, 1, 1.02, 0.98, 1, 1.01, 9, 1, 0.99, 1.01, 1}
	for _, x := range xs {
		tested.add(x, 1, 0.2, true)
		plain.add(x, 1, 0.2, false)
	}
	tv, _ := tested.value()
	pv, _ := plain.value()
	near(t, "estimate with the deferral", tv, pv, 1e-12)
	near(t, "the test's mean", tested.mu, plain.mu, 1e-12)
	near(t, "the test's variance", tested.v, plain.v, 1e-12)
	if tested.n != plain.n {
		t.Errorf("n %d with the deferral, %d without", tested.n, plain.n)
	}
}

func TestBooksLivesAndOldest(t *testing.T) {
	b := NewBooks(0)
	b.Grant(0, "a")
	b.Grant(100*ms, "a")
	b.Commit(100 * ms)
	b.Release(700*ms, "a", true, 0)
	b.Commit(700 * ms)
	iv := b.Close(time.Second)
	if len(iv.Lives) != 1 || iv.Lives[0].Granted != 0 || iv.Lives[0].Ended != 700*ms {
		t.Fatalf("lives %+v, want one from 0 to 0.7s", iv.Lives)
	}
	if iv.Oldest != 100*ms {
		t.Errorf("oldest in flight granted at %v, want 0.1s", iv.Oldest)
	}
	if iv.Start != 0 || iv.End != time.Second {
		t.Errorf("interval %v–%v, want 0–1s", iv.Start, iv.End)
	}
}

// §5.1: with no CPU limit, pressure only blocks; nothing cuts.
func TestNoLimitOnlyBlocks(t *testing.T) {
	m := NewModel(Config{CPU: cpuCfg()})
	S := 1.0
	for i := 0; i < 40; i++ {
		s := sample(time.Second, 0.4, 0, S, int(4*S))
		if i >= 20 {
			s.Stall = 800 * ms
		}
		r := m.Step(s, 16)
		if r.Slots < S-1e-9 {
			t.Fatalf("sample %d: the slots fell from %v to %v with no limit", i, S, r.Slots)
		}
		if i >= 20 && r.Slots > S+1e-9 {
			t.Fatalf("sample %d: a rise under pressure", i)
		}
		S = r.Slots
	}
	if S != 16 {
		t.Errorf("slots %v before the pressure's hold, want the ceiling 16", S)
	}
}

// §17.2 (L11): in the change's own sample the slots are its N̄ scaled by what
// the new limit allows over what it used, and can't rise.
func TestLimitChangeSample(t *testing.T) {
	c := NewCPU(cpuCfg())
	for i := 0; i < 5; i++ {
		c.Step(sample(time.Second, 0.2, 0.25, 4, 40), 4, 4, 16)
	}
	r := c.Step(sample(time.Second, 0.2, 0.125, 4, 40), 4, 4, 16)
	if !r.ChangeSample {
		t.Fatal("the change's sample not marked")
	}
	near(t, "S in the change's sample", r.Slots, 0.9*0.125*4/0.125, 1e-9)
}

// §17.9 item 8: under a quota, only stall the quota doesn't explain blocks.
func TestBlockUnderQuota(t *testing.T) {
	c := NewCPU(cpuCfg())
	s := sample(time.Second, 0.2, 0.25, 4, 40)
	s.Stall = 600 * ms
	s.Throttled = 3
	if r := c.Step(s, 4, 4, 16); r.Blocked {
		t.Error("a stall the quota explains (throttled periods) blocked a rise")
	}
	s.Throttled = 0
	if r := c.Step(s, 4, 4, 16); !r.Blocked {
		t.Error("a stall with no throttled period didn't block")
	}
}
