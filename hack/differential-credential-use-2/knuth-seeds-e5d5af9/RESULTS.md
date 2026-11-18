# The size estimate, repeated with 22 seeds (2026-11-13)

Run: `hack/credentialuse-model-vm/knuth-seeds.sh` at e5d5af9 on a disposable c3-highcpu-22,
14:38:01 to 15:17:35 UTC, every replicate exit 0. 22 seeds (1001 to 1022) × 300,000 probes,
T = 2, for the four scenarios below: 6.6M probes each, against §11.1's 20,000. Raw output in out/.

The spread is measured between seeds, not from one sample's own variance.

| scenario | bounds | pooled mean | between-seed SE | seed means: min / median / max | true size | pooled ÷ true |
|---|---|---|---|---|---|---|
| deletions around the grace, small | {1,0} | 4.19e8 | 4% | 3.04e8 / 4.21e8 / 5.81e8 | 435,439,566 | 0.96 |
| deletions around the grace, small | {0,1} | 4.57e8 | 13% | 2.50e8 / 3.75e8 / 1.57e9 | 541,817,533 | 0.84 |
| acquire against release | {1,0} | 4.71e8 | 27% | 1.83e8 / 2.80e8 / 2.95e9 | unknown | |
| acquire against release | {0,1} | 1.49e9 | 42% | 2.74e8 / 4.67e8 / 1.10e10 | unknown | |

Read:
- On the two known sizes the pooled mean is within about one standard error of the truth, so the
  estimator is unbiased, as its code says. 13 and 17 of 22 single seeds are below the truth: one
  sample of 300,000 probes is likelier low than high.
- The tail is heavy. One seed's mean is 6 to 23 times the median seed's on the acquire scenarios,
  and each sample's own relative standard error (0.15 to 0.36) understates the between-seed spread.
- §11.1's figures for acquire against release, 1.45e8 and 1.72e8, were 3 to 9 times too low.
- Acquire {0,1} is still not pinned: 1.5e9 with a 42% standard error, most of it from one seed.
