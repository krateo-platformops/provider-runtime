package sim

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// TestExplore prints a run's trajectory when SIM_EXPLORE names a scenario. It
// is a development aid, skipped otherwise.
func TestExplore(t *testing.T) {
	name := os.Getenv("SIM_EXPLORE")
	if name == "" {
		t.Skip("set SIM_EXPLORE")
	}
	sc, ok := scenarios()[name]
	if !ok {
		t.Fatalf("no scenario %q", name)
	}
	cfg := sc(1)
	tr := Run(cfg)
	var b strings.Builder
	for i, at := range tr.At {
		r := tr.Results[i]
		fmt.Fprintf(&b, "t=%5.1f S=%6.2f cap=%2d U=%.3f kind=%-9s k=%.4f B=%.3f N=%.2f C=%2d stall=%5.1f tr=%v pace=%v guard=%v unres=%v held=%v ph=%s leff=%.3f thr=%.1f cal=%v\n",
			at.Seconds(), r.Slots, tr.Capacity[i], tr.Used(i), r.CPU.Kind, r.CPU.PerSlot, r.CPU.Background,
			r.CPU.InFlight, tr.Samples[i].Books.Completions, r.CPU.Stall, r.CPU.Trusted, r.CPU.Paced,
			r.CPU.Guarded, r.CPU.Unresolvable, r.Held, r.CPU.Phase, r.CPU.Leff, r.CPU.Threshold, r.CPU.Calibrated)
	}
	t.Log("\n" + b.String())
	w := summarize(tr, 60*time.Second)
	t.Logf("from 60s: CPU %.3f, X %.2f, slots %.2f, capacity %.2f", w.used, w.tput, w.slots, w.capacity)
}
