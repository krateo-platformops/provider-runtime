package sim

// baselineGains are the utilization baseline's gains, chosen by
// TestTuneBaseline on the headline case, seeds 101–120, by the plan's §4.5
// rule, and committed before any other case ran on the baseline. Of the 910
// grid points, this one passed the headline's conditions in all 20 tuning
// seeds with the smallest mean entry (3.00 samples) and variance after entry
// (0.0267). K_p is at the grid's lower edge, 0.1.
var baselineGains = Baseline{Kp: 0.1, Ki: 1.778279410038923, TauU: 1}
