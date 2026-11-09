// Package costmodel holds the saturation cost model's estimators, tests and
// laws (docs/design-saturation-cost-model.md) as pure code: every input is a
// value in a Sample, every output a value, and nothing here reads a file, a
// clock or the Go runtime. The Governor's sampler and the §10 simulation
// (package sim) both drive it, so what the simulation checks is the code the
// Governor runs.
//
// Units: CPU in cores (CPU-seconds per second), CPU time as time.Duration,
// slot-time in slot-seconds, slots as float64 until they are published.
package costmodel
