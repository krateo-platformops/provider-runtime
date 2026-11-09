package sim

import "math"

// frozen is one case plant's values from its fixed-capacity curve (plan
// §4.5): S*, the duration at S*, and n_iter from one slot (nStart), from the
// case's starting point (nFrom), and down from above the band (nAbove).
type frozen struct {
	sStar, dStar          float64
	nStart, nFrom, nAbove int
}

// frozenXCap is the plant's throughput at the cap, X at the curve's largest
// capacity, from the same TestFreeze run as frozenPlants (seed 9001: the
// cost46 plant's n=16 row, X = 5.438/s).
var frozenXCap = map[string]float64{"cost46": 5.438}

// floorAfterCostIncrease is design §17.8 item 4's lower edge after the cost
// doubles, frozen from the plants: S*'·(D_mix/D*')·C_W/(C_W + N₀), with
// N₀ = ⌈S_pre⌉, C_W = X'_cap·|W| and D_mix the base plant's D (item 7(c)).
// |W| is ⌈N₀/X'_cap⌉ + 1 seconds, the window while capped (the reviewer's
// definition at the review of 8fdfbd4): 2 for cost46, a floor of about 1.61
// against θ⌊S*'⌋ = 1.8.
func floorAfterCostIncrease() float64 {
	p, b := frozenPlants["cost46"], frozenPlants["base"]
	n0 := math.Ceil(b.sStar)
	x := frozenXCap["cost46"]
	cw := x * (math.Ceil(n0/x) + 1)
	return p.sStar * (b.dStar / p.dStar) * cw / (cw + n0)
}
