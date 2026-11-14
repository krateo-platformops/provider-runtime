# Session 1: the split's proof on Teardown, and the T = 1 sizes (spec §11.3)

Run: `git archive 749ccb4` (model code 8cc8ff8's) on cred-model-proof (n2-highcpu-48,
europe-west3-a), 2026-11-12 11:38:06 to 13:24:25 UTC, go test exit 0, DONE "exit 0".
time -v: 1:45:58 wall, 57,777 s user + 7,167 s system = 18.0 CPU-hours, 1021% CPU,
0.9 GB peak RSS. The VM powered itself off; it was restarted at 13:39 to copy out.tar.gz,
then deleted with its disk.

## Step 1, the split's proof: PASS

On every Teardown subtest at T = 2, the serial and split (4 workers) stateless searches,
run side by side, report the same schedule count, the same terminal abstract states and
the same violations (none), and no harness failure (TestModelParallelStatelessIsSerial):

| subtest | schedules | terminal states | serial | split, 4 workers |
|---|---|---|---|---|
| interleaving A {1,0} | 616,721 | 240 | 1h08m | 28m |
| interleaving A {0,1} | 693,798 | 419 | 1h10m | 30m |
| krateo-057 {0,1} | 764,469 | 419 | 1h13m | 34m |
| interleaving B {0,1} | 844,875 | 514 | 1h16m | 37m |
| krateo-057 {1,0} | 1,409,097 | 268 | 1h35m | 58m |
| interleaving B {1,0} | 1,951,942 | 464 | 1h46m | 1h08m |

Usage (calls between jumps, jumps, smallest tick) is equal too: the test fails otherwise.
The counts equal 19e986c's serial differential baseline.

## Step 3's first part, the four scenarios' size at T = 1

Read by §11.1's rules: Knuth's mean (20,000 probes), rate from the 300,000-schedule capped
search, time = total/rate, lower bounds. Calibration: Teardown A {1,0}'s capped search
finished, exactly 243,911 schedules; Knuth 2.139e5, ratio 0.88, inside [1/3, 3]. The other
five Teardown subtests didn't finish under the cap, so calibration at T = 1 rests on that one
point (at T = 2 it rested on six, 0.62 to 0.88).

| scenario | bounds | Knuth T=1 | RSE | rate /s | core-hours | on 96 | on 128 |
|---|---|---|---|---|---|---|---|
| two processes, shrunk | {1,0} | 4.83e11 | 0.22 | 77 | 1,742,000 | 18,100h | 13,600h |
| two processes, shrunk | {0,1} | 1.22e11 | 0.48 | 75 | 454,000 | 4,720h | 3,540h |
| away and back, small | {0,1} | 2.79e9 | 0.20 | 80 | 9,700 | 101h | 76h |
| two releasers | {1,0} | 2.50e9 | 0.26 | 88 | 7,880 | 82h | 62h |
| two releasers | {0,1} | 1.15e9 | 0.20 | 84 | 3,800 | 40h | 30h |
| away and back, small | {1,0} | 1.14e9 | 0.32 | 82 | 3,820 | 40h | 30h |
| recreate, small | {0,1} | 1.12e8 | 0.24 | 81 | 390 | 4.0h | 3.0h |
| recreate, small | {1,0} | 8.54e7 | 0.10 | 82 | 290 | 3.0h | 2.3h |

Infeasible at T = 1 by the pre-registered rule (above 24h on 128 cores): away and back,
two releasers and two processes, at both bounds. Feasible: recreate.

## The sizing side process

A separate go test (ad72b93, the candidate shrinks, sizing only) ran on the same VM from
12:24:41 to 12:25:18 UTC, 37 s, 12 subtests, about 6 CPU-minutes; load average 14.7 against
48 vCPU at 12:24:43. Its logs are shrunk-T1.log and shrunk-T2.log. Its effect on the
capped searches' rates: each rate is over 300,000 schedules and about 55 to 65 minutes, so
37 s of overlap can move it by at most about 1% even if a search had stopped outright. The
searches' own 10,000-schedule intervals around the window are inside each search's normal
spread (interval rates range from about 32 to 244/s in every big search; those near the
window are 0 to 33% below each search's median, as many other intervals are).
