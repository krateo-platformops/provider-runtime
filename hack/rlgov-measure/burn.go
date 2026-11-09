//go:build linux || darwin

package main

import (
	"crypto/sha256"
	"runtime"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// burnSink keeps the hashing from being optimised away.
var burnSink atomic.Uint32

// burnCPU spends d of this goroutine's CPU time, measured, and returns what it
// spent: d plus at most one chunk, a few tens of microseconds. CPU time, not
// wall time, and not a number of iterations: CFS throttling makes the call take
// longer, never cheaper, and the cost is the same on any node, however fast its
// cores. (The first version counted sha256 rounds: 3000 meant about 100ms on the
// driver's machine and cost about 0.8s per call on GKE's amd64 node.)
//
// It reads the thread's CPU clock, clock_gettime(CLOCK_THREAD_CPUTIME_ID), so
// the goroutine is locked to its OS thread for the call: otherwise the
// scheduler may move it between threads, and the clock would read another
// thread's time. The clock has nanosecond resolution and comes from the
// scheduler's own accounting. getrusage(RUSAGE_THREAD) reads the same
// accounting on Linux, but in microseconds and on Linux only; this one also
// runs on macOS, so the unit test runs on the driver's machine too.
func burnCPU(d time.Duration) time.Duration {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	start := threadCPU()
	var buf [4096]byte
	for {
		// One chunk: eight rounds over 4 KiB, then a look at the clock.
		for range 8 {
			sum := sha256.Sum256(buf[:])
			buf[0] = sum[0]
		}
		if used := threadCPU() - start; used >= d {
			burnSink.Store(uint32(buf[0]))
			return used
		}
	}
}

// threadCPU is the CPU time the calling OS thread has used.
func threadCPU() time.Duration {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_THREAD_CPUTIME_ID, &ts); err != nil {
		panic("clock_gettime(CLOCK_THREAD_CPUTIME_ID): " + err.Error())
	}
	return time.Duration(ts.Nano())
}
