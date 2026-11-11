// Exp, Log, Sin and Cos in this file are derived from Go's math package
// (src/math/exp.go, log.go and sin.go), under this licence:
//
//	Copyright 2009 The Go Authors.
//
//	Redistribution and use in source and binary forms, with or without
//	modification, are permitted provided that the following conditions are
//	met:
//
//	   * Redistributions of source code must retain the above copyright
//	notice, this list of conditions and the following disclaimer.
//	   * Redistributions in binary form must reproduce the above
//	copyright notice, this list of conditions and the following disclaimer
//	in the documentation and/or other materials provided with the
//	distribution.
//	   * Neither the name of Google LLC nor the names of its
//	contributors may be used to endorse or promote products derived from
//	this software without specific prior written permission.
//
//	THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
//	"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
//	LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
//	A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
//	OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
//	SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
//	LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
//	DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
//	THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
//	(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
//	OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// Go's exp.go and log.go are from FreeBSD's msun (e_exp.c, e_log.c), which
// came with these notices:
//
//	Copyright (C) 2004 by Sun Microsystems, Inc. All rights reserved.
//
//	Permission to use, copy, modify, and distribute this
//	software is freely granted, provided that this notice
//	is preserved.
//
//	Copyright (C) 1993 by Sun Microsystems, Inc. All rights reserved.
//
//	Developed at SunPro, a Sun Microsystems, Inc. business.
//	Permission to use, copy, modify, and distribute this
//	software is freely granted, provided that this notice
//	is preserved.
//
// Go's sin.go is from the Cephes library
// (http://netlib.sandia.gov/cephes/cmath/sin.c):
//
//	Cephes Math Library Release 2.8:  June, 2000
//	Copyright 1984, 1987, 1989, 1992, 2000 by Stephen L. Moshier
//
//	The readme file at http://netlib.sandia.gov/cephes/ says:
//	   Some software in this archive may be from the book _Methods and
//	Programs for Mathematical Functions_ (Prentice-Hall or Simon & Schuster
//	International, 1989) or from the Cephes Mathematical Library, a
//	commercial product. In either event, it is copyrighted by the author.
//	What you see here may be used freely but it comes with no support or
//	guarantee.

// Package fpmath is the few elementary functions the cost model and its
// simulation need, computed the same way on every platform.
//
// The standard library's math.Exp, math.Log and math.Hypot are assembly on
// some platforms and not others (math.Exp on amd64 also changes path with the
// CPU's FMA support), and its pure-Go functions are compiled with fused
// multiply-adds on arm64. Either makes a simulation's results depend on the
// machine. These functions are the pure-Go algorithms of Go's math package,
// with every product that feeds a sum rounded by an explicit float64
// conversion, which the Go specification says prevents fusion
// ("Floating-point operators"). Their results are the same bits on every
// platform; they may differ from the math package's in the last place.
//
// Exp, Log, Sin and Cos are derived from Go's math package, whose licence and
// notices are at the top of this file and in the repository's NOTICE.
package fpmath

import "math"

// Exp is e**x.
func Exp(x float64) float64 {
	const (
		ln2Hi = 6.93147180369123816490e-01
		ln2Lo = 1.90821492927058770002e-10
		log2e = 1.44269504088896338700e+00

		overflow  = 7.09782712893383973096e+02
		underflow = -7.45133219101941108420e+02
		nearZero  = 1.0 / (1 << 28)

		p1 = 1.66666666666666657415e-01
		p2 = -2.77777777770155933842e-03
		p3 = 6.61375632143793436117e-05
		p4 = -1.65339022054652515390e-06
		p5 = 4.13813679705723846039e-08
	)
	switch {
	case math.IsNaN(x):
		return x
	case x > overflow:
		return math.Inf(1)
	case x < underflow:
		return 0
	case -nearZero < x && x < nearZero:
		return 1 + x
	}
	var k int
	switch {
	case x < 0:
		k = int(float64(log2e*x) - 0.5)
	case x > 0:
		k = int(float64(log2e*x) + 0.5)
	}
	hi := x - float64(float64(k)*ln2Hi)
	lo := float64(float64(k) * ln2Lo)
	r := hi - lo
	t := r * r
	p := float64(t*p5) + p4
	p = float64(t*p) + p3
	p = float64(t*p) + p2
	p = float64(t*p) + p1
	c := r - float64(t*p)
	y := 1 - ((lo - float64(r*c)/(2-c)) - hi)
	return math.Ldexp(y, k)
}

// Log is the natural logarithm of x.
func Log(x float64) float64 {
	const (
		ln2Hi = 6.93147180369123816490e-01
		ln2Lo = 1.90821492927058770002e-10
		l1    = 6.666666666666735130e-01
		l2    = 3.999999999940941908e-01
		l3    = 2.857142874366239149e-01
		l4    = 2.222219843214978396e-01
		l5    = 1.818357216161805012e-01
		l6    = 1.531383769920937332e-01
		l7    = 1.479819860511658591e-01
	)
	switch {
	case math.IsNaN(x) || math.IsInf(x, 1):
		return x
	case x < 0:
		return math.NaN()
	case x == 0:
		return math.Inf(-1)
	}
	f1, ki := math.Frexp(x)
	if f1 < math.Sqrt2/2 {
		f1 *= 2
		ki--
	}
	f := f1 - 1
	k := float64(ki)
	s := f / (2 + f)
	s2 := s * s
	s4 := s2 * s2
	a := float64(s4*l7) + l5
	a = float64(s4*a) + l3
	a = float64(s4*a) + l1
	t1 := float64(s2 * a)
	b := float64(s4*l6) + l4
	b = float64(s4*b) + l2
	t2 := float64(s4 * b)
	R := t1 + t2
	hfsq := float64(float64(0.5*f) * f)
	return float64(k*ln2Hi) - ((hfsq - (float64(s*(hfsq+R)) + float64(k*ln2Lo))) - f)
}

// The polynomial coefficients of sin and cos on [0, π/4] (Cephes).
var (
	sinC = [...]float64{
		1.58962301576546568060e-10,
		-2.50507477628578072866e-8,
		2.75573136213857245213e-6,
		-1.98412698295895385996e-4,
		8.33333333332211858878e-3,
		-1.66666666666666307295e-1,
	}
	cosC = [...]float64{
		-1.13585365213876817300e-11,
		2.08757008419747316778e-9,
		-2.75573141792967388112e-7,
		2.48015872888517045348e-5,
		-1.38888888888730564116e-3,
		4.16666666666665929218e-2,
	}
)

// reduceMax bounds the argument of Sin and Cos: beyond it the Cody–Waite
// reduction loses precision, and Go's math package switches to Payne–Hanek,
// which isn't ported. The simulation's arguments are under 2π.
const reduceMax = 1 << 29

// octant reduces x ≥ 0 to z in [−π/4, π/4] and its octant j (Cody–Waite, π/4
// in three parts).
func octant(x float64) (j uint64, z float64) {
	const (
		pi4A = 7.85398125648498535156e-1
		pi4B = 3.77489470793079817668e-8
		pi4C = 2.69515142907905952645e-15
	)
	j = uint64(x * (4 / math.Pi))
	y := float64(j)
	if j&1 == 1 {
		j++
		y++
	}
	j &= 7
	z = ((x - float64(y*pi4A)) - float64(y*pi4B)) - float64(y*pi4C)
	return j, z
}

func sinPoly(z, zz float64) float64 {
	p := float64(sinC[0]*zz) + sinC[1]
	for _, c := range sinC[2:] {
		p = float64(p*zz) + c
	}
	return z + float64(float64(z*zz)*p)
}

func cosPoly(zz float64) float64 {
	p := float64(cosC[0]*zz) + cosC[1]
	for _, c := range cosC[2:] {
		p = float64(p*zz) + c
	}
	return 1.0 - float64(0.5*zz) + float64(float64(zz*zz)*p)
}

// Sin is the sine of x, for |x| < 2²⁹; NaN beyond.
func Sin(x float64) float64 {
	switch {
	case x == 0 || math.IsNaN(x):
		return x
	case math.IsInf(x, 0) || math.Abs(x) >= reduceMax:
		return math.NaN()
	}
	sign := false
	if x < 0 {
		x, sign = -x, true
	}
	j, z := octant(x)
	if j > 3 {
		sign = !sign
		j -= 4
	}
	zz := z * z
	var y float64
	if j == 1 || j == 2 {
		y = cosPoly(zz)
	} else {
		y = sinPoly(z, zz)
	}
	if sign {
		y = -y
	}
	return y
}

// Cos is the cosine of x, for |x| < 2²⁹; NaN beyond.
func Cos(x float64) float64 {
	switch {
	case math.IsNaN(x) || math.IsInf(x, 0) || math.Abs(x) >= reduceMax:
		return math.NaN()
	}
	sign := false
	x = math.Abs(x)
	j, z := octant(x)
	if j > 3 {
		j -= 4
		sign = !sign
	}
	if j > 1 {
		sign = !sign
	}
	zz := z * z
	var y float64
	if j == 1 || j == 2 {
		y = sinPoly(z, zz)
	} else {
		y = cosPoly(zz)
	}
	if sign {
		y = -y
	}
	return y
}

// Hypot is √(p² + q²), for the magnitudes the cost model uses (no rescaling
// against overflow). math.Sqrt is correctly rounded on every platform.
func Hypot(p, q float64) float64 {
	return math.Sqrt(float64(p*p) + float64(q*q))
}

// Lgamma is ln Γ(x) for x ≥ 0.5, by the Lanczos approximation (g = 7, nine
// terms), accurate to about 1e-15 relative: enough for the t quantiles the
// simulation's reports use.
func Lgamma(x float64) float64 {
	const g = 7
	coef := [...]float64{
		0.99999999999980993, 676.5203681218851, -1259.1392167224028,
		771.32342877765313, -176.61502916214059, 12.507343278686905,
		-0.13857109526572012, 9.9843695780195716e-6, 1.5056327351493116e-7,
	}
	x--
	a := coef[0]
	t := x + g + 0.5
	for i := 1; i < len(coef); i++ {
		a += coef[i] / (x + float64(i))
	}
	return float64(0.5*Log(2*math.Pi)) + float64((x+0.5)*Log(t)) - t + Log(a)
}
