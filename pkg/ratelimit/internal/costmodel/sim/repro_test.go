package sim

import (
	"crypto/sha256"
	"fmt"
	"testing"
	"time"
)

// The simulation is bit-reproducible across platforms (plan §4.5, the user's
// decision of 2026-11-11): no fused multiply-add in the engine, the cost
// model or the tests' statistics, elementary functions from fpmath, and sums
// in a fixed order. These runs' hashes are committed; they must match on
// linux/amd64 (CI) and darwin/arm64 alike.

// traceHash is the SHA-256 of a run's every sample, result and capacity,
// printed with %+v: floats in the shortest form that round-trips, so that
// equal hashes mean equal bits, and maps in key order.
func traceHash(tr *Trace) string {
	h := sha256.New()
	for i := range tr.Samples {
		fmt.Fprintf(h, "%v|%+v|%+v|%d\n", tr.At[i], tr.Samples[i], tr.Results[i], tr.Capacity[i])
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func gateCfg(name string, seed uint64) Config {
	for _, c := range gateCases() {
		if c.name == name {
			return c.cfg(seed)
		}
	}
	panic("no gate case " + name)
}

var reproRuns = []struct {
	name string
	cfg  func() Config
	want string
}{
	{"limit halves, seed 12, cost model", func() Config { return gateCfg("limit halves", 12) }, ""},
	{"limit halves, seed 12, level PI ×1", func() Config {
		c := gateCfg("limit halves", 12)
		ctlBaseline(1).apply(&c)
		return c
	}, ""},
	{"limit halves, seed 12, reservation gate", func() Config {
		c := gateCfg("limit halves", 12)
		ctlReserve.apply(&c)
		return c
	}, ""},
	{"mix shift seen, seed 1, cost model", func() Config { return gateCfg("mix shift seen", 1) }, ""},
	{"drain plant at 4 slots, seed 9001, 600 s", func() Config {
		c := measured(9001)
		c.Queues[0].Class = fixedClass(drainHeavy())
		c.Fixed, c.Duration = 4, 600*time.Second
		return c
	}, ""},
}

// TestReproducible checks each run's trace against its committed hash.
func TestReproducible(t *testing.T) {
	for _, r := range reproRuns {
		if got := traceHash(Run(r.cfg())); got != r.want {
			t.Errorf("%s: trace hash %s, want %s", r.name, got, r.want)
		}
	}
}
