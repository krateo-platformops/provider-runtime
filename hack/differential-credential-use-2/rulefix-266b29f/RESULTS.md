# Rule R fix (C-park): runs A and B at 266b29f

The fix is c0be9ea, a Wait's condition is a guard and writes nothing (the doc
is 266b29f, docs/credential-use-model-reduction.md §5, C-park). It changes
"deletions around the grace" in both searches: the late deletions' `dt` is
read after the Wait, not captured by its condition. So run 3's results for
that scenario (t2-RESULTS.md) were measured on the old model, and two runs
follow, approved by the user as "A, then B if A passes":
- **A:** the stateful search against the reduced one on the new model, and
  TestModelReducedKeepsDroppedTerminals with CREDENTIALUSE_MODEL_REDUCED_DROPS=1.
- **B:** the stateless re-run of the same scenario on the new model, at
  {1,0} and {0,1}, against the stateful and reduced searches.

Both runs use go1.26.10 linux-amd64 (sha256 ccfe7ae8...b20c5), a git bundle
of 266b29f copied to the VM (no GitHub credentials there), and
hack/credentialuse-model-vm/remote-run-bundle.sh. Project
operations-dev-krateo-io, zone europe-west3-a, image debian-12, 20GB boot
disk.

## Local smokes, before either VM

On the bundled tree, behind the load gate:
- A at T=0 (CREDENTIALUSE_MODEL_DIFF_STATELESS=off): stateful equals reduced
  at both bounds, 2 = 2 and 42 = 42 terminal states, 0 divergences. 28 CPU-s.
- B at T=0, with the stateless leg on 2 workers: progress lines at 100,000
  and 200,000 schedules (2,400 to 3,500/s), and no harness error. Stopped by
  its watchdog, by design, at 188 CPU-s; an earlier attempt was stopped at
  7 CPU-min with no output, because its progress interval was the default
  1M.

## Run A: results

cred-rulefix-a, n2-highcpu-8. Raw output in a/out/; a/attempt1/ is the first
launch, which exited after 8 seconds: `git bundle verify` needs a repository
and ran outside one. The script now verifies inside the clone, and the second
launch ran on the same VM.

| run | env | wall | user + sys CPU | result |
|---|---|---|---|---|
| red | CREDENTIALUSE_MODEL_DIFF_STATELESS=off, DIFF_PLAN=^deletions around the grace=2 | 9:31 | 2,215 + 27 s | pass |
| pin | CREDENTIALUSE_MODEL_REDUCED_DROPS=1 | 4:35 | 692 + 9 s | pass |

| bounds | stateful runs | stateful terminal states | reduced runs | reduced terminal states | violations | audits, divergences |
|---|---|---|---|---|---|---|
| {1,0} | 51,847 | 935 | 29,533 | **935** | 0 | 48,657 and 26,343, 0 |
| {0,1} | 54,822 | 4,331 | 34,446 | **4,331** | 0 | 46,515 and 26,139, 0 |

Read:
- **The reduced search now reaches every terminal state the stateful one
  does**, at both bounds, where run 3 had 920 against 935 and 4,212 against
  4,331. The four pinned schedules' terminal states are among them.
- 49 CPU-min in all. The VM was up about 21 minutes (19 for the run, and a
  restart to copy the outputs back), about $0.12. It is deleted.
- The new model's stateful counts, 935 and 4,331, equal run 3's on the old
  model. Equal counts don't make equal sets; B's stateless leg is the check
  on the new model's stateful search.

## Run B: results

cred-rulefix-b, n2-highcpu-96, created 04:04:27 UTC. Hard stop: go test
-timeout 15h40m on the VM, and a local loop that would stop it at 20:04 UTC.
It finished well before either. Raw output in b/out/, with the stateless
terminal sets in b/out/terminal/.

Env: GOGC=off GOMEMLIMIT=80GiB CREDENTIALUSE_MODEL_DIFF_WORKERS=96
CREDENTIALUSE_MODEL_DIFF_PLAN=^deletions around the grace=2, with DIFF_SAVE,
and DIFF_EXPECT set to run 3's counts.

| bounds | stateless schedules | stateless | stateful (runs) | reduced (runs) | violations | audits, divergences | subtest wall |
|---|---|---|---|---|---|---|---|
| {1,0} | 435,775,541 | 935 | 935 (51,847) | 935 (29,533) | 0 | 48,657 and 26,343, 0 | 3:51:58 |
| {0,1} | 543,501,839 | 4,331 | 4,331 (54,822) | 4,331 (34,446) | 0 | 46,515 and 26,139, 0 | 4:25:11 |

go test exit 0. 4:25:14 wall, 1,332,537 s user + 5,305 s system CPU
(372 CPU-hours, 84 of 96 vCPU on average), peak RSS 79.7 GB. The VM was up
about 4h29m (4h26m for the run, and a restart to copy the outputs back), so
about $20 at $4.43/h. It is deleted.

Read:
- **Stateless = stateful = reduced on the new model**, at both bounds. With
  run A, the C-park fix is checked three ways on the scenario it changed.
- **The terminal sets are the same as on the old model.** B's saved
  stateless sets equal run 3's (run3-e5d5af9/out/terminal) key for key: 0
  only in either, at both bounds. The schedules changed (435,439,566 to
  435,775,541, and 541,817,533 to 543,501,839), but not the terminal states.
  A state the old harness reached with `dt` captured early, the new one
  reaches with the read after the Wait on some other schedule, and the
  other way round.
- Run 3's results for the other five scenarios stand. The fix changes only
  this scenario's harness, and its checks never fired on the others (the
  pinned state keys of every other small scenario are byte-identical).

## Run C: rule R in CI's model step (at ed7f68e)

The user's decision on #59's CI failure: measure, then R on in CI, with the
caps unchanged. cred-rulefix-c, n2-highcpu-8, a bundle of ed7f68e, two runs
with CREDENTIALUSE_MODEL_QUIET=1. Raw output in c/out/.

**Run 1, the two scenarios that ran past their caps**, at CI's profiles and
caps (go test exit 0, 45:15 wall, 5,135 s user CPU):

| test, profile | cap | runs | distinct states | finished | audits, divergences |
|---|---|---|---|---|---|
| AwayAndBack/3, 1 fault | 200,000 | 60,098 | 121,929 | yes | 428, 0 |
| AwayAndBack/3, 1 preemption | 200,000 | 90,999 | 190,961 | yes | 698, 0 |
| AwayAndBack/3, 1 preemption, 1 fault | 5,000,000 | 1,443,687 | 2,495,170 | yes | sampled, 0 |
| AwayAndBack/4, 1 fault | 200,000 | 14,703 | 32,154 | yes | 108, 0 |
| AwayAndBack/4, 1 preemption | 200,000 | 19,258 | 42,577 | yes | 151, 0 |
| AwayAndBack/4, 1 preemption, 1 fault | 5,000,000 | 341,909 | | yes | sampled, 0 |
| TwoReleasers, 1 fault | 200,000 | 64,695 | 113,872 | yes | sampled, 0 |
| TwoReleasers, 1 preemption | 200,000 | 110,100 | 187,985 | yes | sampled, 0 |

Subtest wall: AwayAndBack/3 2,480 s, AwayAndBack/4 707 s, TwoReleasers
221 s. Without R (CI at 266b29f): AwayAndBack/3 stopped at its caps
unfinished (211,334 and 268,512 states at 200,000; 4,641,511 at 5,000,000),
and TwoReleasers at 200,000 (196,793 and 201,755 states).

**Run 2, the whole `^TestModel` step**, as CI runs it, with GOMAXPROCS=4 for
a GitHub runner's 4 vCPU and the step's 90m timeout: **it timed out.** Every
scenario test passed inside it (AwayAndBack/3 in 2,488 s, AcquireAgainstRelease
871 s, AwayAndBack/4 694 s, TwoReleasers 431 s, DeletionsAroundTheGrace 115 s,
Recreate 31 s, the rest under 11 s). What was still running at 90m was
**TestModelStatefulAgreesWithStateless**, which `^TestModel` matches: its
default plan is every small scenario at T = 2 with a stateless leg, the
searches that took run 3 1,953 CPU-hours on 96 vCPU. It was also among the
tests running at CI's own timeouts since 1c3ff46.

The VM was up about 2h20m in all, about $0.80. It is deleted.

**What follows (the lead's decision, under §11.3).** The full differential
runs only under a measured plan (CREDENTIALUSE_MODEL_DIFF_PLAN, DIFF_ONLY or
DIFF_JUMPS), since it is a VM measurement that CI's step was never meant to
run. Without one, TestModelStatefulAgreesWithStateless runs CI's plan: the
three Teardown scenarios with the stateless leg, and AcquireAgainstRelease
stateful against reduced, at T = 2, {0,1} and {1,0}, so that CI still
compares rule R against no R. Another CREDENTIALUSE_MODEL_DIFF_* variable
without a plan fails the test. Rule R is on by default in the model tests
(f41b8b9). CI's model step is the measurement of whether it all fits in 90m.
