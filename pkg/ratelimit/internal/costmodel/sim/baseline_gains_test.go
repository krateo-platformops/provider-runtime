package sim

// baselineGains are the level PI's gains, chosen by TestTuneBaseline on the
// headline case, seeds 101–120, by the plan's §4.5 rule, over the grid with
// K_p ∈ {0, 0.01, 0.03} added (1,120 points), and committed before any gate
// run at 8fdfbd4. The re-tune chose the same point as the first (2a23517):
// with the headline passed in all 20 tuning seeds, it ties with K_p 0 and 0.01
// on mean entry (3.00 samples) and has the smallest variance after entry
// (0.0267 against 0.0285 and 0.0286), so K_p 0.1 is no longer at the grid's
// edge.
var baselineGains = Baseline{Kp: 0.1, Ki: 1.778279410038923, TauU: 1}
