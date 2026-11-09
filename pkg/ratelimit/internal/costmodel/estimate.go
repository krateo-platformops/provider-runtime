package costmodel

import "math"

// changeSigma is the change rule's threshold, in standard deviations (§3.5).
const changeSigma = 3.0

// ratio is a ratio of two exponentially weighted sums with a warm start, the
// 3σ hold-out and the two-in-a-row restart (design §3.5).
//
// Each sample's own ratio x = num/den is tracked too, as an exponentially
// weighted mean and variance, for the change test and the standard error.
type ratio struct {
	// n counts the samples averaged in since the last start or restart.
	n        int
	num, den float64
	// mu and v are the weighted mean and variance of the per-sample ratios.
	mu, v float64
	// run counts consecutive outliers on one side, signed.
	run int
}

// value reports the estimate, and false before any sample.
func (r *ratio) value() (float64, bool) {
	if r.n == 0 || r.den == 0 {
		return 0, false
	}
	return r.num / r.den, true
}

// effective is the number of samples the estimate effectively averages.
func effective(n int, alpha float64) float64 {
	if alpha <= 0 {
		return float64(n)
	}
	return math.Min(float64(n), (2-alpha)/alpha)
}

// se is the standard error of the estimate, +Inf before the third sample.
func (r *ratio) se(alpha float64) float64 {
	if r.n < 3 {
		return math.Inf(1)
	}
	return math.Sqrt(math.Max(r.v, 0) / effective(r.n, alpha))
}

func (r *ratio) reset() { *r = ratio{} }

// outcome is what one call to add did.
type outcome int

const (
	averaged outcome = iota
	heldOut
	restarted
)

// add folds one sample in with weight alpha. With test set, a sample more than
// three standard deviations from the mean is held out, and the second such in
// a row on the same side restarts the estimate from that sample. den must be
// positive.
func (r *ratio) add(num, den, alpha float64, test bool) outcome {
	x := num / den
	out := averaged
	if test && r.n >= 3 {
		sd := math.Sqrt(math.Max(r.v, 0))
		// A floor only against division by zero, not a tuning.
		sd = math.Max(sd, 1e-9*math.Max(math.Abs(r.mu), 1))
		dev := (x - r.mu) / sd
		switch {
		case math.Abs(dev) <= changeSigma:
			r.run = 0
		case sign(dev) == sign(float64(r.run)) && r.run != 0:
			r.run += int(sign(dev))
		default:
			r.run = int(sign(dev))
		}
		if abs(r.run) >= 2 {
			r.reset()
			out = restarted
		} else if r.run != 0 {
			return heldOut
		}
	}
	r.n++
	al := math.Max(alpha, 1/float64(r.n))
	r.num = (1-al)*r.num + al*num
	r.den = (1-al)*r.den + al*den
	if r.n == 1 {
		r.mu, r.v = x, 0
	} else {
		d := x - r.mu
		r.mu += al * d
		r.v = (1 - al) * (r.v + al*d*d)
	}
	return out
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

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
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
	s.v = (1-al)*s.v + al*d*d
	return big
}
