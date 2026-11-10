//go:build mainline

package main

import (
	"os"

	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
)

// applyExperimental sets the memory input under its name on main, which is the
// control's (opts_ctrl.go): the cost-model branch builds the harness against
// this worktree, where #48's renamed options don't exist. The fixed-capacity
// plant curve (results/curve-settings.txt) leaves it off.
func applyExperimental(s *ratelimit.SaturationOptions) {
	s.ExperimentalMemoryWorkingSet = os.Getenv("EXPERIMENTAL_MEMORY") == "true"
}
