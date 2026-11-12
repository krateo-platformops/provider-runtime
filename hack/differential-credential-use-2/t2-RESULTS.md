# Step 2 (spec §11.3): the feasible set at T = 2, recreate at T = 1, the stateful sizing

On cred-model-t2 (n2-highcpu-96, europe-west3-a), one go test per run, through
hack/credentialuse-model-vm/remote-run.sh (poweroff trap). The model code under test is
8cc8ff8's throughout; every commit after it changes the harness or hack/ only.

Common to every run:
- `-run '^(TestModelStatefulAgreesWithStateless|TestModelStatefulSize)$'`, go test timeout 72h;
- `CREDENTIALUSE_MODEL_DIFF_PLAN="^(teardown|acquire against release|deletions around the grace)=2;^recreate=1"`;
- `CREDENTIALUSE_MODEL_DIFF_WORKERS=96`, `CREDENTIALUSE_MODEL_DIFF_SAVE=~/out/terminal`;
- `CREDENTIALUSE_MODEL_STATEFUL_SIZE=8h`,
  `CREDENTIALUSE_MODEL_SIZE_ONLY="^(recreate|away and back|two releasers|two processes)"`,
  `CREDENTIALUSE_MODEL_SIZE_JUMPS=2`.

## Run 1: d7fa08b, default GOGC (100), stopped

- 15:10:56 to about 15:35 UTC. At 15:27: 58% user CPU, 40% idle, the binary on about 42
  of 96 vCPU. The stateless leg logged no progress, so its rate is unknown.
- Stopped by SIGKILL to the script (so its trap didn't power the VM off) and SIGQUIT to the
  test binary: t2-attempts/out-run1-gogc100/ (the log, with the goroutine dump, gzipped).

## Run 2: 3674059, GOGC=800, GOMEMLIMIT=64GiB, stopped

- 15:36:13 to 16:00 UTC. At 15:47: 45.6% user, 52% idle, binary at 48.7 cores; 63% at 15:48;
  7.5% at 16:00.
- Progress lines, DeletionsAroundTheGrace: {0,1} 1M in 11m29s (1,451/s), 2M in 21m06s
  (1,579/s); {1,0} 1M in 11m48s (1,412/s), 2M in 21m35s (1,545/s).
- Why: the dump at 16:00 (t2-attempts/out-run2-gogc800/) has 554 goroutines in semacquire,
  every one in runtime.gcStart from mallocgc, and 133 runnable in gcTrigger.test: about
  12 × 96 workers and 2,000 coroutines allocating 450 to 620 KB per schedule, so a GC about
  every second, and everyone queues on it.

## Run 3: e5d5af9, GOGC=off, GOMEMLIMIT=80GiB, running

- e5d5af9 adds the tick's terms-only walk (the same terms, by test) to 3674059's progress
  lines with an ETA against Knuth's totals (`CREDENTIALUSE_MODEL_DIFF_EXPECT`).
- Started 16:02:52 UTC. It was started before the lead's answer to the options, against the
  agreed order; the lead kept it, under the stop rule below.
- At 16:08: 81.8% user, binary at about 80 cores; CPU then swings with GC at the limit (26 to
  83% in 20-second samples), RSS pinned at 79.3 to 79.7 GB.
- First 1M rates: DeletionsAroundTheGrace {1,0} 3,527/s, {0,1} 3,438/s; AcquireAgainstRelease
  2,452 and 2,415/s; recreate T=1 2,308 and 2,259/s; Teardown B {1,0} 3,227/s.

### The stop rule (the lead's)

Version 1, set at about 16:10 UTC: stop if user CPU is below 60% at two consecutive
15-minute checks once Teardown has finished; or RSS is pinned within 2 GiB of the limit
while GC CPU exceeds 25%; or any search's rate falls below half its first-1M rate; or the
projected wall time exceeds 51h or the cost $226.

**Version 2, which replaced it at 16:15 UTC, before anything had tripped it.** CPU share and
RSS are proxies; what matters is throughput and cost, and the early samples already sat
near version 1's CPU and RSS lines with the rates 2.6 times run 2's. Stop and report if:
- DeletionsAroundTheGrace {0,1}'s cumulative rate falls below 2,000/s;
- any search's 1M-interval rate falls below half its first-1M rate for two consecutive
  intervals;
- the projected wall time exceeds 51h, or the projected cost $226 (at $4.43/h);
- RSS goes past the limit, or the OOM killer acts.

CPU share and RSS are reported as diagnostics only. The projection is the longest
remaining search's ETA at its cumulative rate, against Knuth's totals, which ran 0.62 to
0.88 of actual on Teardown.
