package sim

// frozen is one case plant's values from its fixed-capacity curve (plan
// §4.5): S*, the duration at S*, and n_iter from one slot (nStart) and from
// the case's starting point (nFrom).
type frozen struct {
	sStar, dStar  float64
	nStart, nFrom int
}
