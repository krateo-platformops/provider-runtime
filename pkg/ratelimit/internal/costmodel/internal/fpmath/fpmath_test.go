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
// ranges the cost model and its simulation use, within 2 ulps: Exp on
// [−40, 40]; Log from 2⁻⁵³ (the smallest uniform draw is about 1e-16) to 1e12;
// Sin and Cos on [−7, 7] and near 0 on both sides; Hypot over a grid. Lgamma,
// by Lanczos rather than ported, within 1e-13 relative.
func TestAgainstMath(t *testing.T) {
	check := func(name string, x, got, want float64) {
		t.Helper()
		if d := ulps(got, want); d > 2 {
			t.Fatalf("%s(%v) = %v, math %v: %d ulps", name, x, got, want, d)
		}
	}
	const n = 20000
	for i := 0; i <= n; i++ {
		f := float64(i) / n
		x := -40 + float64(80*f)
		check("Exp", x, Exp(x), math.Exp(x))
		y := math.Ldexp(1, -53) * math.Pow(2, float64(93*f)) // 2⁻⁵³ … 2⁴⁰
		check("Log", y, Log(y), math.Log(y))
		z := -7 + float64(14*f)
		check("Sin", z, Sin(z), math.Sin(z))
		check("Cos", z, Cos(z), math.Cos(z))
		e := math.Pow(10, -300+float64(299*f)) // 1e-300 … 0.1, both signs
		for _, v := range []float64{e, -e} {
			check("Sin", v, Sin(v), math.Sin(v))
			check("Cos", v, Cos(v), math.Cos(v))
		}
		g := 0.5 + float64(60*f)
		want, _ := math.Lgamma(g)
		if math.Abs(Lgamma(g)-want) > 1e-13*math.Max(1, math.Abs(want)) {
			t.Fatalf("Lgamma(%v) = %v, math %v", g, Lgamma(g), want)
		}
	}
	for i := 0; i <= 200; i++ {
		for j := 0; j <= 200; j++ {
			p, q := math.Pow(10, -6+float64(float64(i)*0.04)), math.Pow(10, -6+float64(float64(j)*0.04)) // 1e-6 … 100
			check("Hypot", p, Hypot(p, q), math.Hypot(p, q))
		}
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
