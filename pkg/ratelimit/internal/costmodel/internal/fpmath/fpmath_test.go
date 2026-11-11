package fpmath

import (
	"math"
	"testing"
)

// ulps is the distance between a and b in units in the last place.
func ulps(a, b float64) uint64 {
	x, y := math.Float64bits(a), math.Float64bits(b)
	if x > y {
		return x - y
	}
	return y - x
}

// TestAgainstMath checks each function against the math package over the
// ranges the cost model and its simulation use: within 2 ulps (Lgamma within
// 1e-13 relative).
func TestAgainstMath(t *testing.T) {
	for i := 0; i <= 20000; i++ {
		x := -40 + 80*float64(i)/20000
		if d := ulps(Exp(x), math.Exp(x)); d > 2 {
			t.Fatalf("Exp(%v) = %v, math %v: %d ulps", x, Exp(x), math.Exp(x), d)
		}
		y := math.Pow(10, -12+24*float64(i)/20000)
		if d := ulps(Log(y), math.Log(y)); d > 2 {
			t.Fatalf("Log(%v) = %v, math %v: %d ulps", y, Log(y), math.Log(y), d)
		}
		z := -7 + 14*float64(i)/20000
		if math.Abs(Sin(z)-math.Sin(z)) > 2e-16 || math.Abs(Cos(z)-math.Cos(z)) > 2e-16 {
			t.Fatalf("Sin/Cos(%v) = %v, %v; math %v, %v", z, Sin(z), Cos(z), math.Sin(z), math.Cos(z))
		}
		g := 0.5 + 60*float64(i)/20000
		want, _ := math.Lgamma(g)
		if math.Abs(Lgamma(g)-want) > 1e-13*math.Max(1, math.Abs(want)) {
			t.Fatalf("Lgamma(%v) = %v, math %v", g, Lgamma(g), want)
		}
	}
	if d := ulps(Hypot(3e-3, 4e-3), math.Hypot(3e-3, 4e-3)); d > 1 {
		t.Fatalf("Hypot: %d ulps", d)
	}
	for _, c := range []struct{ got, want float64 }{
		{Exp(0), 1}, {Log(1), 0}, {Sin(0), 0}, {Cos(0), 1},
	} {
		if c.got != c.want {
			t.Errorf("got %v, want %v", c.got, c.want)
		}
	}
	if !math.IsNaN(Sin(1<<30)) || !math.IsNaN(Log(-1)) || !math.IsInf(Exp(1000), 1) || Exp(-1000) != 0 {
		t.Error("special cases")
	}
}
