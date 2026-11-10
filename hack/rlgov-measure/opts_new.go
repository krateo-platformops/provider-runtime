//go:build !ctrl && !mainline

package main

import (
	"os"

	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
)

// applyExperimental sets the options whose names differ from the control build
// (opts_ctrl.go), or that it doesn't have.
func applyExperimental(s *ratelimit.SaturationOptions) {
	s.ExperimentalMemory = os.Getenv("EXPERIMENTAL_MEMORY") == "true"
	s.ExperimentalGoMemoryHeadroomRatio = envFloat("HEADROOM_RATIO")
}
