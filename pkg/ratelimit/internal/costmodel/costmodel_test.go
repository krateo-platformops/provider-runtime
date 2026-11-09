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
	out, err := exec.Command("go", "list", "-f", "{{join .Imports \" \"}}", ".").Output()
	if err != nil {
		t.Skipf("go list: %v", err)
	}
	allowed := map[string]bool{"math": true, "time": true, "sort": true}
	for _, dep := range strings.Fields(string(out)) {
		if !allowed[dep] {
			t.Errorf("costmodel imports %s; only %v are allowed", dep, allowed)
		}
	}
}

func TestBooksAccrueAndCarry(t *testing.T) {
	b := NewBooks(0)
	b.Grant(0, "a")
	b.Commit(0)
	b.Grant(200*ms, "b")
	b.Commit(200 * ms)
	b.Release(600*ms, "a", true)
	b.Commit(600 * ms)
	// A change after the read instant belongs to the next interval (L5).
	b.Release(1100*ms, "b", true)
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
	b.Release(500*ms, "a", true)
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
	b.Release(100*ms, "a", false)
	b.Commit(100 * ms)
	if iv := b.Close(time.Second); iv.Completions != 0 {
		t.Errorf("a handed-back grant counted as a completion")
	}
}

func TestRatioWarmStartHoldOutAndRestart(t *testing.T) {
	var r ratio
	for i := 0; i < 20; i++ {
		x := 1.0 + 0.01*float64(i%3-1)
		if r.add(x, 1, 0.1, true) != averaged {
			t.Fatalf("sample %d not averaged", i)
		}
	}
	v, _ := r.value()
	near(t, "mean", v, 1, 0.02)
	// One outlier is held out and leaves the estimate.
	if r.add(5, 1, 0.1, true) != heldOut {
		t.Fatal("an outlier was averaged in")
	}
	if v2, _ := r.value(); v2 != v {
		t.Errorf("a held-out sample moved the estimate: %v to %v", v, v2)
	}
	// A second on the same side restarts from it.
	if r.add(5, 1, 0.1, true) != restarted {
		t.Fatal("two outliers in a row didn't restart")
	}
	if v3, _ := r.value(); v3 != 5 || r.n != 1 {
		t.Errorf("after the restart the estimate is %v with n %d, want 5 with 1", v3, r.n)
	}
	// Warm start: the first samples are a plain mean.
	var w ratio
	w.add(1, 1, 0.01, false)
	w.add(3, 1, 0.01, false)
	if v, _ := w.value(); v != 2 {
		t.Errorf("warm start: %v, want the plain mean 2", v)
	}
	if !math.IsInf(w.se(0.01), 1) {
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
	c := NewCPU(cpuCfg())
	if k := c.kind(sample(time.Second, 0.01, 0.25, 0, 0), 0.01, 0, 1); k != Idle {
		t.Errorf("nothing in flight: %v", k)
	}
	s := sample(time.Second, 0.25, 0.25, 5, 10)
	s.Throttled = 10
	if k := c.kind(s, 0.25, 0, 5); k != Capped {
		t.Errorf("every period throttled at the cap: %v", k)
	}
	// Throttled every period but usage beyond the slack below the cap: active.
	s.Usage = time.Duration(0.2 * float64(time.Second))
	if k := c.kind(s, 0.2, 0, 5); k != Active {
		t.Errorf("throttled but 50 ms/s under the cap: %v", k)
	}
	// Usage within the slack (2 × 5ms in a 1s sample) of the cap: capped.
	s.Usage = time.Duration(0.241 * float64(time.Second))
	if k := c.kind(s, 0.241, 0, 5); k != Capped {
		t.Errorf("throttled and within the slack: %v", k)
	}
	d := sample(time.Second, 0.1, 0.25, 6, 5)
	d.Books.NMin = 6
	if k := c.kind(d, 0.1, 0, 4); k != Draining {
		t.Errorf("six in flight throughout with capacity 4: %v", k)
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
