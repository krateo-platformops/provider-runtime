//go:build linux || darwin

package main

import (
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
)

// processCPU is the whole process's CPU time, user and system: a check of
// burnCPU that doesn't use its clock.
func processCPU(t *testing.T) time.Duration {
	t.Helper()
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		t.Fatal(err)
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// Tolerances: burnCPU's own measure is the request plus at most 2ms (one chunk
// is tens of microseconds). The process's CPU, which also counts the Go
// runtime's own threads, is within 90% to 130% of the request, plus 5ms.
func within(t *testing.T, what string, got, want time.Duration) {
	t.Helper()
	lo, hi := want*9/10, want*13/10+5*time.Millisecond
	if got < lo || got > hi {
		t.Errorf("%s = %s, want %s (between %s and %s)", what, got, want, lo, hi)
	}
}

func TestBurnCPUSpendsTheCPUTimeAskedFor(t *testing.T) {
	for _, want := range []time.Duration{5 * time.Millisecond, 50 * time.Millisecond} {
		before := processCPU(t)
		used := burnCPU(want)
		process := processCPU(t) - before
		if used < want || used > want+2*time.Millisecond {
			t.Errorf("burnCPU(%s) measured %s, want %s to %s", want, used, want, want+2*time.Millisecond)
		}
		within(t, "process CPU for burnCPU("+want.String()+")", process, want)
		t.Logf("asked %s: thread %s, process %s", want, used, process)
	}
}

// Calls that share one CPU each still spend what they ask for, and take longer
// in wall time: the cost is CPU time, as under CFS throttling, not wall time.
func TestBurnCPUIsCPUTimeNotWallTime(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	const n, each = 4, 50 * time.Millisecond
	used := make([]time.Duration, n)
	before := processCPU(t)
	start := time.Now()
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { used[i] = burnCPU(each) })
	}
	wg.Wait()
	wall := time.Since(start)
	process := processCPU(t) - before
	for i, u := range used {
		if u < each || u > each+2*time.Millisecond {
			t.Errorf("call %d measured %s, want %s to %s", i, u, each, each+2*time.Millisecond)
		}
	}
	within(t, "process CPU for four calls on one CPU", process, n*each)
	if wall < n*each*9/10 {
		t.Errorf("four calls of %s on one CPU took %s of wall time, want at least %s", each, wall, n*each*9/10)
	}
	t.Logf("four calls of %s on one CPU: wall %s, process CPU %s", each, wall, process)
}
