# The stateless reference's size, measured (spec §11.1, pre-registered in 6d1e705)

Run: TestModelStatelessSize at 487543c (model code 8cc8ff8's), on cred-model-size
(c3-highcpu-22, europe-west3-a), 2026-11-12 08:46:36 to 09:55:09 UTC, go test exit 0,
1:08:30 wall, 0.9 GB peak RSS. 18 subtests at once, one per vCPU: the rates are per vCPU
under that load. Probe seed 1794473206418999007. Raw output in out/.

Per the pre-registration: the total is Knuth's mean from 20,000 probes; the DFS-position
estimate (300,000-schedule capped search) is beside it, not mixed in; time = total / rate
(the capped search's) on 1 core, and on C cores total / (rate·C), a lower bound since the
split's imbalance isn't counted; above 24h on 128 cores is reported infeasible.

Calibration: on the six Teardown subtests, whose exact counts are known (19e986c's
baseline), Knuth/actual is 0.62 to 0.88, inside [1/3, 3], so the estimates stand. They
run low on all six, as a heavy-tailed estimator with too few probes does, so the totals
below are likelier under than over. No capped search finished under its cap.

| scenario | bounds | Knuth mean | RSE | DFS-position est. | rate /s | Knuth/actual | DFS/actual | 1 core h | 22 h | 64 h | 128 h |
|---|---|---|---|---|---|---|---|---|---|---|---|
| two processes, shrunk | {1,0} | 1.08e+12 | 0.35 | 9.03e+13 | 81 |  |  | 3.72e+06 | 1.69e+05 | 5.81e+04 | 2.91e+04 |
| two processes, shrunk | {0,1} | 8.67e+11 | 0.52 | 1e+20 | 79 |  |  | 3.05e+06 | 1.39e+05 | 4.77e+04 | 2.38e+04 |
| two releasers | {1,0} | 7.98e+10 | 0.36 | 7.42e+09 | 96 |  |  | 2.3e+05 | 1.05e+04 | 3.6e+03 | 1.8e+03 |
| away and back, small, with a relist | {1,0} | 5.82e+10 | 0.82 | 1.01e+11 | 88 |  |  | 1.84e+05 | 8.38e+03 | 2.88e+03 | 1.44e+03 |
| two releasers | {0,1} | 2.51e+10 | 0.30 | 2.28e+11 | 91 |  |  | 7.64e+04 | 3.47e+03 | 1.19e+03 | 597 |
| away and back, small, with a relist | {0,1} | 9.12e+09 | 0.35 | 2.23e+15 | 84 |  |  | 3.02e+04 | 1.37e+03 | 472 | 236 |
| recreate behind a cached UID, small | {1,0} | 1.83e+09 | 0.59 | 1.19e+08 | 85 |  |  | 6e+03 | 273 | 93.8 | 46.9 |
| recreate behind a cached UID, small | {0,1} | 1e+09 | 0.23 | 5.26e+10 | 84 |  |  | 3.3e+03 | 150 | 51.5 | 25.8 |
| deletions around the grace, small | {0,1} | 3.44e+08 | 0.33 | 6.55e+09 | 120 |  |  | 796 | 36.2 | 12.4 | 6.22 |
| deletions around the grace, small | {1,0} | 2.19e+08 | 0.14 | 4.25e+08 | 125 |  |  | 486 | 22.1 | 7.6 | 3.8 |
| acquire against release | {0,1} | 1.72e+08 | 0.16 | 2.2e+13 | 82 |  |  | 581 | 26.4 | 9.07 | 4.54 |
| acquire against release | {1,0} | 1.45e+08 | 0.30 | 5.29e+09 | 85 |  |  | 475 | 21.6 | 7.43 | 3.71 |
| teardown, interleaving B | {1,0} | 1.68e+06 | 0.29 | 1.25e+06 | 114 | 0.86 | 0.64 | 4.09 | 0.186 | 0.0638 | 0.0319 |
| teardown, krateo-057 | {1,0} | 8.88e+05 | 0.40 | 9.65e+05 | 112 | 0.63 | 0.69 | 2.2 | 0.0999 | 0.0343 | 0.0172 |
| teardown, interleaving B | {0,1} | 5.66e+05 | 0.25 | 1.01e+06 | 131 | 0.67 | 1.20 | 1.2 | 0.0546 | 0.0188 | 0.00938 |
| teardown, krateo-057 | {0,1} | 5.57e+05 | 0.26 | 1.01e+06 | 130 | 0.73 | 1.32 | 1.19 | 0.054 | 0.0186 | 0.00928 |
| teardown, interleaving A | {1,0} | 5.41e+05 | 0.17 | 6.85e+05 | 133 | 0.88 | 1.11 | 1.13 | 0.0512 | 0.0176 | 0.00881 |
| teardown, interleaving A | {0,1} | 4.29e+05 | 0.14 | 9.83e+05 | 134 | 0.62 | 1.42 | 0.887 | 0.0403 | 0.0139 | 0.00693 |

Infeasible at 128 cores (projected above 24h, lower bounds):
- two processes, shrunk: about 24,000h at {0,1} and 29,000h at {1,0};
- two releasers: about 600h at {0,1} and 1,800h at {1,0};
- away and back, small, with a relist: about 240h at {0,1} and 1,440h at {1,0};
- recreate behind a cached UID, small: about 26h at {0,1} and 47h at {1,0}.

Feasible: Teardown ×3 (minutes on 22 cores); deletions around the grace, small, and
acquire against release (22 to 36h on 22 cores, 3.7 to 6.2h on 128).
