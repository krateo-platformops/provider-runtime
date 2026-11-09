//go:build ctrl

package main

import (
	"os"

	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
)

// applyExperimental sets the memory input under its v1.10.0 name: the control
// builds against that tag, where the field is still called this.
func applyExperimental(s *ratelimit.SaturationOptions) {
	s.ExperimentalMemoryWorkingSet = os.Getenv("EXPERIMENTAL_MEMORY") == "true"
}
