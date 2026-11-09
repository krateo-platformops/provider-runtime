package sim

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// TestExplore prints a gate case's trajectory when SIM_EXPLORE names it, with
// SIM_CONTROLLER optionally "baseline" or "reserve". A development aid, skipped otherwise.
func TestExplore(t *testing.T) {
	name := os.Getenv("SIM_EXPLORE")
	if name == "" {
		t.Skip("set SIM_EXPLORE")
	}
	var gc *gateCase
	for _, c := range gateCases() {
		if c.name == name {
			gc = &c
			break
		}
	}
	if gc == nil {
		t.Fatalf("no case %q", name)
	}
	cfg := gc.cfg(1)
	switch os.Getenv("SIM_CONTROLLER") {
	case "baseline":
		ctlBaseline(1).apply(&cfg)
	case "reserve":
		ctlReserve.apply(&cfg)
	}
	tr := Run(cfg)
	var b strings.Builder
	for i, at := range tr.At {
		r := tr.Results[i]
		c := r.CPU
		fmt.Fprintf(&b, "t=%6.1f S=%6.2f cap=%2d U=%.3f L=%.3f kind=%-6s c=%.4f D=%.3f N=%.2f C=%2d stall=%5.1f thr=%d/%d tr=%v pace=%v guard=%v unres=%v held=%v def=%v crs=%v rw=%v hold=%v fb=%v chg=%v first=%v\n",
			at.Seconds(), r.Slots, tr.Capacity[i], tr.Used(i), tr.Samples[i].Limit, c.Kind, c.PerCompletion, c.Duration,
			c.InFlight, tr.Samples[i].Books.Completions, c.Stall, tr.Samples[i].Throttled, tr.Samples[i].Periods, c.Trusted, c.Paced, c.Guarded, c.Unresolvable, r.Held,
			c.CDeferred, c.CRestarted, c.DRewarmed, c.DHold, c.DFallback, c.ChangeSample, c.FirstTerm)
	}
	t.Log("\n" + b.String())
}
