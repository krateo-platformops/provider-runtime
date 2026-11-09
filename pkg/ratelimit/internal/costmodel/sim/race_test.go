//go:build race

package sim

// Under the race detector the cases run two seeds: the detector checks the
// code, and the twenty-seed gate runs without it, in its own CI job.
const raceEnabled = true
