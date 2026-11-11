package costmodel

import "math"

// changeSigma is the change rule's threshold, in standard deviations (§3.5).
const changeSigma = 3.0

// ratio is a ratio of two exponentially weighted sums with a warm start, the
// change test and the two-in-a-row restart (design §3.5, as amended by §17.1).
//
// Each sample's own ratio x = num/den, when den > 0, is tracked too, as an
// exponentially weighted mean and variance, for the change test. The test
// never removes a sample from the sums: a sample beyond 3σ is deferred, and
// folded back, with the weight it would have had, unless the next sample
// confirms the change (§17.1).
type ratio struct {
	// n counts the samples folded in since the last start or restart.
	n        int
	num, den float64
	// qnn, qnd and qdd are Σw²num², Σw²num·den and Σw²den², for the ratio
	// estimator's standard error.
	qnn, qnd, qdd float64
	// xn counts the samples with a per-sample ratio; mu and v are those
	// ratios' weighted mean and variance.
	xn    int
	mu, v float64
	// pend is a deferred sample, side the sign of its deviation.
	pend *deferred
}

type deferred struct {
	num, den, alpha float64
	side            float64
}

// value reports the estimate, and false before any sample.
func (r *ratio) value() (float64, bool) {
	if r.n == 0 || r.den <= 0 {
		return 0, false
	}
	return r.num / r.den, true
}

// seRatio is the ratio estimator's standard error (§17.1):
// √(Σw²(numᵢ − r̂·denᵢ)²)/Σw·denᵢ, +Inf before the third sample.
func (r *ratio) seRatio() float64 {
	v, ok := r.value()
	if !ok || r.n < 3 {
		return math.Inf(1)
	}
	q := r.qnn - float64(2*v*r.qnd) + float64(v*v*r.qdd)
	return math.Sqrt(math.Max(q, 0)) / r.den
}

func (r *ratio) reset() { *r = ratio{} }

// outcome is what one call to add did.
type outcome int

const (
	averaged outcome = iota
	// heldOut: the sample is deferred; it may not raise the slots.
	heldOut
	// restarted: the estimate restarted from the deferred sample and this one.
	restarted
)

// fold adds one sample with the warm-start weight max(alpha, 1/n), and updates
// the test's statistics with it.
func (r *ratio) fold(num, den, alpha float64) {
	r.n++
	a := math.Max(alpha, 1/float64(r.n))
	r.num = float64((1-a)*r.num) + float64(a*num)
	r.den = float64((1-a)*r.den) + float64(a*den)
	b, a2 := (1-a)*(1-a), a*a
	r.qnn = float64(b*r.qnn) + float64(a2*num*num)
	r.qnd = float64(b*r.qnd) + float64(a2*num*den)
	r.qdd = float64(b*r.qdd) + float64(a2*den*den)
	if den <= 0 {
		return
	}
	x := num / den
	r.xn++
	if r.xn == 1 {
		r.mu, r.v = x, 0
		return
	}
	d := x - r.mu
	r.mu += float64(a * d)
	r.v = (1 - a) * (r.v + float64(a*d*d))
}

// deviation is a sample's distance from the mean in standard deviations, and
// whether the test can be run on it.
func (r *ratio) deviation(num, den float64) (float64, bool) {
	if den <= 0 || r.xn < 3 {
		return 0, false
	}
	sd := math.Sqrt(math.Max(r.v, 0))
	// A floor only against division by zero, not a tuning.
	sd = math.Max(sd, 1e-9*math.Max(math.Abs(r.mu), 1))
	return (num/den - r.mu) / sd, true
}

// add folds one sample in with weight alpha. With test set, a sample more than
// three standard deviations from the mean is deferred; if the next one is
// beyond three on the same side the estimate restarts from the two, and
// otherwise the deferred one is folded back first. den may be 0: such a sample
// adds to the numerator and isn't tested.
func (r *ratio) add(num, den, alpha float64, test bool) outcome {
	if p := r.pend; p != nil {
		r.pend = nil
		if dev, ok := r.deviation(num, den); test && ok && math.Abs(dev) > changeSigma && sign(dev) == p.side {
			r.reset()
			r.fold(p.num, p.den, p.alpha)
			r.fold(num, den, alpha)
			return restarted
		}
		r.fold(p.num, p.den, p.alpha)
	}
	if dev, ok := r.deviation(num, den); test && ok && math.Abs(dev) > changeSigma {
		r.pend = &deferred{num: num, den: den, alpha: alpha, side: sign(dev)}
		return heldOut
	}
	r.fold(num, den, alpha)
	return averaged
}

func sign(x float64) float64 {
	switch {
	case x > 0:
		return 1
	case x < 0:
		return -1
	}
	return 0
}

// spread tracks the exponentially weighted variance of a series' steps, for
// the §3.5 exemption: a move in the mean in flight beyond three of its own
// standard deviations is not tested as a change in cost.
type spread struct {
	n    int
	prev float64
	v    float64
}

// moved adds x and reports whether its step from the previous value is beyond
// three standard deviations of the steps seen so far.
func (s *spread) moved(x, alpha float64) bool {
	if s.n == 0 {
		s.n, s.prev = 1, x
		return false
	}
	d := x - s.prev
	s.prev = x
	big := s.n >= 3 && d*d > changeSigma*changeSigma*s.v
	s.n++
	al := math.Max(alpha, 1/float64(s.n-1))
	s.v = float64((1-al)*s.v) + float64(al*d*d)
	return big
}
