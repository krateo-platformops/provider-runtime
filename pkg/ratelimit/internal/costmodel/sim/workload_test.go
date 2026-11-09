package sim

import (
	"time"

	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit/internal/costmodel"
)

// The measured workload (design §1): a 250m limit, GOMAXPROCS 2, 23ms of CPU
// and 400ms of wait per reconcile, 16 workers, a backlog.
func measured(seed uint64) Config {
	return Config{
		Seed:     seed,
		Duration: 180 * time.Second,
		Procs:    2,
		NodeCPUs: 4,
		Limit:    0.25,
		Ceiling:  16,
		Queues: []Queue{{
			Name:   "greeting",
			Class:  fixedClass(Class{CPU: 23 * time.Millisecond, Wait: 400 * time.Millisecond, Jitter: 0.1}),
			Active: always,
		}},
		Model: defaultModel(),
	}
}

func defaultModel() costmodel.Config {
	return costmodel.Config{CPU: costmodel.CPUConfig{Target: 0.9, PressureThreshold: 50, SamplePeriod: time.Second}}
}

func fixedClass(c Class) func(time.Duration) Class { return func(time.Duration) Class { return c } }
func always(time.Duration) bool                    { return true }

// window summarizes a trace from `from` on: mean CPU in cores, completions per
// second, mean slots and mean capacity.
type window struct {
	used, tput, slots, capacity, stall float64
	n                                  int
}

func summarize(tr *Trace, from time.Duration) window {
	var w window
	var dt, use, stall float64
	var comp int
	for i, at := range tr.At {
		if at < from {
			continue
		}
		s := tr.Samples[i]
		dt += s.Dt.Seconds()
		use += s.Usage.Seconds()
		stall += s.Stall.Seconds()
		comp += s.Books.Completions
		w.slots += tr.Results[i].Slots
		w.capacity += float64(tr.Capacity[i])
		w.n++
	}
	if w.n == 0 {
		return w
	}
	w.used = use / dt
	w.stall = stall / dt
	w.tput = float64(comp) / dt
	w.slots /= float64(w.n)
	w.capacity /= float64(w.n)
	return w
}
