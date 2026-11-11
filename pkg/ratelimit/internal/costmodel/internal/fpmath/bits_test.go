package fpmath

import (
	"crypto/sha256"
	"fmt"
	"math"
	"testing"
)

// bitsWant is the SHA-256 of the functions' bits over a fixed grid, committed:
// the same on every platform (plan §4.5, the user's decision of 2026-11-11).
const bitsWant = ""

// TestBits checks the functions' exact bits over a grid against bitsWant.
func TestBits(t *testing.T) {
	h := sha256.New()
	for i := 0; i <= 4000; i++ {
		f := float64(i) / 4000
		for _, v := range []float64{
			Exp(-40 + float64(80*f)), Log(1e-12 + float64(1e3*f)), Sin(2 * math.Pi * f), Cos(2 * math.Pi * f),
			Hypot(f, 1-f), Lgamma(0.5 + float64(60*f)),
		} {
			fmt.Fprintf(h, "%016x\n", math.Float64bits(v))
		}
	}
	if got := fmt.Sprintf("%x", h.Sum(nil)); got != bitsWant {
		t.Errorf("bits hash %s, want %s", got, bitsWant)
	}
}
