package sim

import (
	"math"

	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit/internal/costmodel/internal/fpmath"
)

// rng is SplitMix64, a small seeded generator. The simulation needs its runs
// reproducible from a seed, which crypto/rand can't give; this is not used for
// anything that needs unpredictability. Written here rather than taken from
// math/rand so that no gosec exception (G404) is needed, as the repository
// avoids them (pkg/ratelimit/governor.go, jitterBetween).
type rng struct {
	state uint64
	spare float64
	has   bool
}

func newRNG(seed uint64) *rng { return &rng{state: seed} }

func (r *rng) uint64() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// float64 is uniform in (0, 1).
func (r *rng) float64() float64 {
	return (float64(r.uint64()>>11) + 0.5) / (1 << 53)
}

// normFloat64 is a standard normal, by the Box–Muller transform.
func (r *rng) normFloat64() float64 {
	if r.has {
		r.has = false
		return r.spare
	}
	u1, u2 := r.float64(), r.float64()
	m := math.Sqrt(-2 * fpmath.Log(u1))
	r.spare, r.has = m*fpmath.Sin(2*math.Pi*u2), true
	return m * fpmath.Cos(2*math.Pi*u2)
}
