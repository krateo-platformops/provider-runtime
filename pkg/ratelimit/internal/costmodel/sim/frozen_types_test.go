package sim

import "math"

// frozen is one case plant's values from its fixed-capacity curve (plan
// §4.5): S*, the duration at S*, and n_iter from one slot (nStart), from the
// case's starting point (nFrom), and down from above the band (nAbove);
// determined is the plan's determinacy rule for S*.
type frozen struct {
	sStar, dStar          float64
	nStart, nFrom, nAbove int
	determined            bool
}

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
	cw := float64(x * (math.Ceil(n0/x) + 1))
	return p.sStar * (b.dStar / p.dStar) * cw / (cw + n0)
}

// restartEdge is D'_cap + |W'| in samples, the life and the window at the cap
// (plan §4.5's restart-edge floor): D'_cap = N₀/X'_cap, |W'| = ⌈D'_cap/Δt⌉ + 1.
func restartEdge(n0, xcap float64) int {
	dcap := n0 / xcap
	return int(math.Ceil(dcap + math.Ceil(dcap) + 1))
}

// drainN0 is the long drain's N₀ = ⌈S_pre⌉, frozen from the 30s plant's S*.
func drainN0() float64 { return math.Ceil(frozenPlants["drain0"].sStar) }

// drainCount is the plan's §4.5 long-drain count, frozen from the drain plant:
// ⌈(D'_cap + |W'|)/Δt⌉ + 1 samples, with D'_cap = N₀/X'_cap the life at the
// cap and |W'| = ⌈D'_cap/Δt⌉ + 1 the window while capped.
func drainCount() int {
	dcap := drainN0() / frozenXCap["drain"]
	w := math.Ceil(dcap) + 1
	return int(math.Ceil(dcap+w)) + 1
}

// floorAfterDrainChange is design §17.8 item 4's lower edge for the long
// drain, frozen from its plants: S*'·(D_mix/D*')·C_W/(C_W + N₀), D_mix the 30s
// plant's D, C_W = X'_cap·|W'|.
func floorAfterDrainChange() float64 {
	p, b := frozenPlants["drain"], frozenPlants["drain0"]
	n0 := drainN0()
	x := frozenXCap["drain"]
	cw := float64(x * (math.Ceil(n0/x) + 1))
	return p.sStar * (b.dStar / p.dStar) * cw / (cw + n0)
}
