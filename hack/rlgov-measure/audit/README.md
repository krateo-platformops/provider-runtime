# Plant-curve audit: the scripts behind its numbers

These are the scripts behind the independent audit of the GKE plant curve (`../results/curve-audit.md`).
They also cover its two follow-on reviews of plan §4.5, `docs/plan-saturation-cost-model-implementation.md`:
the engine calibration (73e1d72) and its revisions (45b35f8, f85197b).

The scripts use the Python 3 standard library only, and they write only to stdout. Run them from the
repository root as `python3 hack/rlgov-measure/audit/<script>.py`, without `-I`: each script imports
`auditlib.py` from its own directory.

## Inputs

All the inputs are already committed, so nothing is copied here:

- `hack/rlgov-measure/results/pc-*.log`: the 22 runs. Override with `--results <dir>`, for example to
  point at a re-measure.
- `pkg/ratelimit/internal/costmodel/sim/testdata/curve-engine-b2e5757.txt`: the engine's drain curves
  under the GKE protocol, seeds 9001–9010, at 3000 ms. Override with `--engine <file>`.
- `mva.py` uses one constant, the frozen `X_cap,0 = 0.1620/s` from
  `pkg/ratelimit/internal/costmodel/sim/frozen_test.go` (drain0, at 1500 ms).

The parser (`auditlib.load_runs`) is independent of `curve_report.py`. Its U is the measured window's
total CPU over the elapsed time, which equals the report's mean of 1-s samples to 4 decimals in every run.

## Which script gives which number

| Number(s) | Command |
|---|---|
| Per-run validity: burn checks A and B, seed = cksum(name), the node constant, the schedules from seeds 20261110 and 20261111 | `runs.py` |
| Stage 1: Ŝ* 4.156, β̂ 0.01716, ŝ 0.00697 on 6 df, t 2.447, k* 6 (raw 5.13); the midpoint factor 0.858 giving k* 8 | `curve.py` |
| Final per-n means and sds; S* 4.1445; bootstrap [3.925, 4.37], about 20% of resamples crossing in 3–4; the report's [3.9250, 4.3751] reproduced | `curve.py` |
| Delta SE 0.163, widths 0.639 (z), 0.727 (t10); Fieller 0.78 / 0.95 / 0.87 | `curve.py` |
| Small-sample-corrected bootstrap widths 0.47 / 0.52 / 0.575; PCHIP bootstrap 0.44 | `curve.py` |
| Leave-one-out 4.03–4.26; interpolants 4.086 / 4.114 / 4.094 / 4.104 / 4.141 | `curve.py` |
| Percentile coverage about 85.5–87%; "determined" in 96% of simulations | `coverage.py`, `coverage.py --truth pchip`, `coverage.py --sd obs` (a few minutes each) |
| S*-hat spread against an sd multiplier (span 0.60 at ×1.4, 0.64 at ×1.5); the sd's 95% bounds ×0.70–×1.75; P(σ > 1.45 ŝ) = 0.093 | `coverage.py --spread` |
| Day × n interaction, exact permutation p = 0.005 (exploratory); S* by day, 4.28 / 3.94, and by stage | `drift.py` |
| 300-s block means, the per-n profile, and S* against the warm-up (the withdrawn caveat 3) | `drift.py` |
| CPU per completion 3156.5 ms = 3012.2 drawn + 10.2 edge + 134.0 non-burn (se 8.3); the background intercept −0.008 ± 0.009 cores; n/X = burn wall time + hold within noise | `calibration.py` |
| MVA S* 4.477 / 4.347 / 4.325 at 3.000 / 3.134 / 3.157 s; MVA against GKE (within 0.004) and the engine against MVA per n | `mva.py` |
| drain0 at 1500 / 1634 / 1657 ms: S*₀ 7.109 / 6.710 / 6.649, D*₀ 48.96 s, τ 979 s, X_cap,0 0.1467/s | `mva.py` |
| Engine against GKE: z per n, the Welch 90% intervals, GKE pooled sds 0.0058 (11 df) and 0.0022 (4 df) | `fidelity.py` |
| The equivalence test's pass limit: at n = 3, 0.0031 at 10 seeds and 0.0037 at 40 (`--k` sets the seed count) | `fidelity.py` |
| Variance-ratio F-tests, p 0.357 / 0.157 / 0.148 | `fidelity.py` |
| The engine's mean-curve S* 4.639, seed bootstrap [4.393, 4.986] (width 0.593), corrected widths 0.628 / 0.718; jackknife SE 0.136; the width at 10–40 seeds 0.53 → 0.27 | `fidelity.py` |

The prediction for the calibrated engine, S* ≈ 4.47, is MVA's 4.325 at 3.157 s plus the engine's offset
from MVA at 3.000 s (4.639 − 4.477). Both terms come from `mva.py` and `fidelity.py`.

## For the GKE re-measure after PR 5

`calibration.py` and `mva.py` are the checks to repeat:

- whether c = U/X still decomposes as drawn burn + edge + a per-reconcile excess;
- whether the cycle is still the burn wall time plus the hold;
- whether MVA at the new c still reproduces the window means.

Point `--results` at the new logs. The lesson from this audit: randomize across days, so that
stage 2 isn't a single time block.
