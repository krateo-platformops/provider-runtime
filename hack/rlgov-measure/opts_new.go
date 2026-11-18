//go:build !ctrl

package main

import (
	"fmt"
	"os"

	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
)

// applyExperimental sets the options whose names differ from the control build
// (opts_ctrl.go), or that it doesn't have.
func applyExperimental(s *ratelimit.SaturationOptions) {
	s.ExperimentalMemoryWorkingSet = os.Getenv("EXPERIMENTAL_MEMORY") == "true"
	s.CapacityModel = os.Getenv("CAPACITY_MODEL") == "true"
	s.CPUTarget = envFloat("CPU_TARGET")
}

// modelStat is the capacity model's part of a STAT line: M, the CPU the model
// measured (millicores), the CPU one reconcile used (ms), the required CPU
// (millicores), and the items waiting for a slot over every controller. A
// value the governor doesn't report reads "-".
func modelStat(s ratelimit.Snapshot) string {
	f := func(v *float64, scale float64, format string) string {
		if v == nil {
			return "-"
		}
		return fmt.Sprintf(format, *v*scale)
	}
	waiting := 0
	for _, c := range s.Controllers {
		waiting += c.Waiting
	}
	return fmt.Sprintf("model=%s model_used_m=%s model_bg_m=%s per_rec_ms=%s required_m=%s waiting=%d",
		f(s.CPU.ModelCapacity, 1, "%.2f"), f(s.CPU.Used, 1000, "%.0f"), f(s.CPU.Background, 1000, "%.0f"),
		f(s.CPU.PerReconcile, 1000, "%.1f"), f(s.CPU.Required, 1000, "%.0f"), waiting)
}
