package fpmath

import (
	"crypto/sha256"
	"fmt"
	"math"
	"testing"
)

// bitsWant is the SHA-256 of the functions' bits over TestAgainstMath's ranges,
// committed:
// the same on every platform (plan §4.5, the user's decision of 2026-11-11).
const bitsWant = "826d45fc550c0ba89d5b8161ddd3131b48e9b727b69ca928d81b4788b04e2f8e"

// TestBits checks the functions' exact bits over a grid against bitsWant.
func TestBits(t *testing.T) {
	h := sha256.New()
	for i := 0; i <= 4000; i++ {
		// The inputs are exact (math.Ldexp of a dyadic mantissa) or one rounded
		// operation, so they are the same on every platform; math.Pow isn't.
		f := float64(i) / 4000
		m := 1 + float64(float64(i%64)/64)
		e := math.Ldexp(m, -996+i*993/4000) // 2⁻⁹⁹⁶ ≈ 1e-300 … 2⁻³
		for _, v := range []float64{
			Exp(-40 + float64(80*f)),
			Log(math.Ldexp(m, -53+i*93/4000)), // 2⁻⁵³ … 2⁴⁰
			Sin(-7 + float64(14*f)), Cos(-7 + float64(14*f)),
			Sin(e), Sin(-e), Cos(e), Cos(-e),
			Hypot(math.Ldexp(m, -20+i*27/4000), math.Ldexp(2-m, 7-i*27/4000)), // about 1e-6 … 128
			Lgamma(0.5 + float64(60*f)),
		} {
			_, _ = fmt.Fprintf(h, "%016x\n", math.Float64bits(v))
		}
	}
	if got := fmt.Sprintf("%x", h.Sum(nil)); got != bitsWant {
		t.Errorf("bits hash %s, want %s", got, bitsWant)
	}
}
