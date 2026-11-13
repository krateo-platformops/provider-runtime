#!/bin/bash
# Usage: step_block.sh <prefix> <cpu limit, e.g. 250m> <rounds> <old tag> <new tag>
#
# The CPU step block, decided by the user on 2026-11-13: does the step of
# provider-runtime#95 (CPU pressure over a 10s window, one cut per window, +1
# per 2s after a cut) meet the bars the user set on 2026-11-08, where the step
# before it settled at about 70% of the CPU limit?
#
# Three arms, order rotating each round, one run at a time, on the CPU block
# re-run's workload (cpu_block.sh) with the probe incident-provider ships:
#   off  the new image, saturation unset;
#   old  the old image (main before #95), SATURATION=true SAT_CPU=50;
#   new  the new image (#95),             SATURATION=true SAT_CPU=50.
#
# The settings and the bars are written to results/<prefix>-settings.txt and
# committed before run 1. A run prun.sh discards stays in the count; a failed
# setup stops the block with exit code 2. Results are committed after each
# round, by explicit path. step_report.py <prefix> reads them.
cd "$(dirname "$0")" || exit 1
P=${1:?usage: step_block.sh <prefix> <cpu limit> <rounds> <old tag> <new tag>}
export RLGOV_CPU=${2:?cpu limit}
ROUNDS=${3:?rounds}
OLD=${4:?old tag}
NEW=${5:?new tag}
bash setup.sh || { echo "BLOCK-STOPPED: cluster setup failed"; exit 2; }
export RLGOV_PROBE=incident-provider
SECS=360
root=$(git rev-parse --show-toplevel)
rel=hack/rlgov-measure

commit_results() { # message, then paths relative to the repository root
  local msg=$1; shift
  git -C "$root" add -- "$@" || { echo "COMMIT-FAILED add: $msg"; return; }
  if [ "$(git -C "$root" diff --cached --name-only | sort)" != "$(printf '%s\n' "$@" | sort)" ]; then
    echo "COMMIT-REFUSED the staged list is not the expected one: $msg"
    git -C "$root" diff --cached --name-only
    git -C "$root" reset -q
    return
  fi
  git -C "$root" commit -q -m "$msg

Co-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01N8HNbBN72N8dxJ36iFc63W" || echo "COMMIT-FAILED commit: $msg"
}

arm() { # name round
  local a=$1 i=$2 tag=$NEW env=""
  case $a in
    off) ;;
    old) tag=$OLD; env="SATURATION=true SAT_CPU=50" ;;
    new) env="SATURATION=true SAT_CPU=50" ;;
  esac
  # shellcheck disable=SC2086
  bash prun.sh "$P-$a-$i" cpu "$SECS" "$tag" $env || { echo "BLOCK-STOPPED at $P-$a-$i"; bash nodeid.sh "$P-stopped"; exit 2; }
  bash nodeid.sh "after-$P-$a-$i"
}

for t in "$OLD" "$NEW"; do
  [ -s "results/image-$t.txt" ] || { echo "BLOCK-REFUSED: no results/image-$t.txt"; exit 2; }
done

cat > "results/$P-settings.txt" <<SETTINGS
CPU step block $P, started $(date -u +%Y-%m-%dT%H:%M:%SZ)
question: does provider-runtime#95's CPU step meet the user's bars of 2026-11-08, where the step before it settled at about 70% of the CPU limit with 31% less throughput?
old image: $(cat "results/image-$OLD.txt")
new image: $(cat "results/image-$NEW.txt")
arms: off (the new image, SATURATION unset), old (the old image, SATURATION=true SAT_CPU=50), new (the new image, SATURATION=true SAT_CPU=50); $ROUNDS runs each, order rotating each round, one run at a time
workload: the CPU block re-run's (results/cpu2-settings.txt), unchanged: cpu mode, $RLGOV_CPU limit, 512Mi, 64 Greetings, 16 workers, poll 1s, ${SECS}s per run; each Observe spends 20ms of CPU measured on its own thread, then waits 400ms. The memory input is off in every arm.
liveness probe: incident-provider's (httpGet /healthz, periodSeconds 10, timeoutSeconds 1, failureThreshold 3)

the bars, set by the user on 2026-11-08, applied to the new arm, figures from 40s on:
- CPU used: the median over the runs of the mean cpu_ms/s is at least 90% of the limit;
- pressure about 50: the mean over the runs of cpu.pressure some avg10 is between 40 and 60 (the band is this block's reading of "about", fixed here before run 1);
- throughput: the median Observes per second is at least 90% of the off arm's median;
- no restarts in any run of the new arm.
At 250m all four decide whether #95 merges. The user set no bar for another limit: at any other limit the same four are reported as information, and decide nothing.

validity, checked per run and reported, never used to drop a run: the median burn_cpu_ms/call is 20.0 to 20.5; in the off arm, CPU is at least 90% of the limit and pressure is over 50. If the off arm doesn't saturate, the block shows nothing about the step, and that is the result; the workload is not retuned.

expected, written before run 1, from the step's rules and the 2026-11-08 calibration (off at 250m: pressure about 60, CPU at the limit, 11.4 Observes a second):
- old at 250m: as on 2026-11-08, local cut from 16 towards 1 on one stall, CPU about 70% of the limit, Observes about 30% under off.
- new at 250m: the window's share passes 50 only with most of a window near off's pressure, so local should cut from somewhere above the knee (about 5) to about half and climb back by one per 2s: local mostly between about 5 and 12, CPU at 95% of the limit or more, pressure 45 to 58, Observes within 10% of off. If so the bars are met by running close to the limit, and the step's effect shows in local, in the calls in flight and in each call's wall time, which are reported.
- at 100m: one call's 20ms of CPU is two periods of quota, so pressure is high at any concurrency. No figure is predicted; the block reports what happens.

also reported per arm: local (mean and minimum), cuts, calls in flight (median), each call's CPU stretched to wall time (median), failed probes, liveness kills.
driver power: $(pmset -g ps | head -1)
SETTINGS
bash nodeid.sh "$P-start"
commit_results "chore(hack): settings for the CPU step block $P" "$rel/results/$P-settings.txt" "$rel/results/node-identity.txt"

arms=(off old new)
for i in $(seq 1 "$ROUNDS"); do
  logs=()
  for j in 0 1 2; do
    a=${arms[$(( (i - 1 + j) % 3 ))]}
    arm "$a" "$i"
    logs+=("$rel/results/$P-$a-$i.log")
  done
  commit_results "chore(hack): CPU step block $P, round $i" "${logs[@]}" "$rel/results/node-identity.txt"
done
bash nodeid.sh "$P-end"
commit_results "chore(hack): CPU step block $P finished" "$rel/results/node-identity.txt"
echo BLOCK-DONE
