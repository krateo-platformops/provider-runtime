#!/bin/bash
# Usage: model_block.sh <prefix> <cpu limit, e.g. 500m or 1000m> <workers> <rounds> <image tag>
#
# The capacity model's acceptance block (docs/design-capacity-model.md §10):
# is the CPU pressure step with the capacity model better than the step alone?
#
# Three arms on one image, order rotating each round, one run at a time, on
# the CPU block's workload (20ms of CPU then a 400ms wait per Observe, 64
# Greetings polled every 1s) with the probe incident-provider ships:
#   off    saturation unset;
#   step   SATURATION=true SAT_CPU=50 (v1.19.0's step alone);
#   model  SATURATION=true SAT_CPU=50 CAPACITY_MODEL=true.
#
# The settings and the bars are written to results/<prefix>-settings.txt and
# committed before run 1. A run prun.sh discards stays in the count; a failed
# setup stops the block with exit code 2. Results are committed after each
# round, by explicit path. model_report.py <prefix> reads them.
cd "$(dirname "$0")" || exit 1
P=${1:?usage: model_block.sh <prefix> <cpu limit> <workers> <rounds> <image tag>}
export RLGOV_CPU=${2:?cpu limit}
export RLGOV_WORKERS=${3:?workers}
ROUNDS=${4:?rounds}
TAG=${5:?image tag}
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
Claude-Session: https://claude.ai/code/session_01NSM9VYtekkTV1h9dK4WxM4" || echo "COMMIT-FAILED commit: $msg"
}

arm() { # name round
  local a=$1 i=$2 env=""
  case $a in
    off)   ;;
    step)  env="SATURATION=true SAT_CPU=50" ;;
    model) env="SATURATION=true SAT_CPU=50 CAPACITY_MODEL=true" ;;
  esac
  # shellcheck disable=SC2086
  bash prun.sh "$P-$a-$i" cpu "$SECS" "$TAG" $env || { echo "BLOCK-STOPPED at $P-$a-$i"; bash nodeid.sh "$P-stopped"; exit 2; }
  bash nodeid.sh "after-$P-$a-$i"
}

[ -s "results/image-$TAG.txt" ] || { echo "BLOCK-REFUSED: no results/image-$TAG.txt"; exit 2; }

cat > "results/$P-settings.txt" <<SETTINGS
Capacity model block $P, started $(date -u +%Y-%m-%dT%H:%M:%SZ)
question (docs/design-capacity-model.md §10): is the CPU pressure step with the capacity model better than the step alone? If it is not, the model does not ship.
image: $(cat "results/image-$TAG.txt")
arms: off (SATURATION unset), step (SATURATION=true SAT_CPU=50), model (SATURATION=true SAT_CPU=50 CAPACITY_MODEL=true); $ROUNDS runs each, order rotating each round, one run at a time
workload: cpu mode, $RLGOV_CPU limit, 512Mi, 64 Greetings, $RLGOV_WORKERS workers, poll 1s, ${SECS}s per run; each Observe spends 20ms of CPU measured on its own thread, then waits 400ms. The memory input is off in every arm. The workers are $RLGOV_WORKERS, not the step block's 16, so that the ungoverned arm asks for more CPU than the limit at 1 CPU too: $RLGOV_WORKERS workers at about 22ms of CPU per 420ms want about $(python3 -c "print(round($RLGOV_WORKERS*22/420,2))") cores.
liveness probe: incident-provider's (httpGet /healthz, periodSeconds 10, timeoutSeconds 1, failureThreshold 3)

the knee, computed by model_report.py from the off arm's own figures: 0.9 x limit / c x (c + 0.4s), c the off arm's CPU per Observe (cpu_ms/s over obs/s, median of its runs). With c about 22ms that is about $(python3 -c "l=float('$RLGOV_CPU'.rstrip('m'))/1000; print(round(0.9*l/0.022*0.422,1))") slots.

the bars, decided by the user on 2026-11-13 ("a latency win at near-equal throughput"), written before run 1; figures from 40s on, medians over the runs:
- wall time: the model arm's wall time for a call's 20ms of CPU is at least 20% lower than the step arm's;
- throughput: the model arm's Observes per second are at least 95% of the step arm's;
- CPU used: the model arm uses at least 85% of the limit;
- local: the model arm's mean local is within 20% of the knee;
- no restarts in any run of the model arm.
All five decide.

validity, checked per run and reported, never used to drop a run: the median burn_cpu_ms/call is 20.0 to 20.5; in the off arm, CPU is at least 90% of the limit. If the off arm doesn't saturate, the block shows nothing, and that is the result; the workload is not retuned.

expected, written before run 1: the step alone moves local between a cut and the ceiling, as at 250m (mean 6.3 around a knee of about 5, wall 144ms). The model should hold local near the knee with few cuts, so with a lower wall time and about the same CPU and throughput. The risk to it: at 90% of the limit the 10s stall share may still pass 50, so the step keeps cutting and the model changes little; the cuts per run show that.

also reported per arm: local (mean, minimum, standard deviation), cuts, the model's M, calls in flight, failed probes, liveness kills.
driver power: $(pmset -g ps | head -1)
SETTINGS
bash nodeid.sh "$P-start"
commit_results "chore(hack): settings for the capacity model block $P" "$rel/results/$P-settings.txt" "$rel/results/node-identity.txt"

arms=(off step model)
for i in $(seq 1 "$ROUNDS"); do
  logs=()
  for j in 0 1 2; do
    a=${arms[$(( (i - 1 + j) % 3 ))]}
    arm "$a" "$i"
    logs+=("$rel/results/$P-$a-$i.log")
  done
  commit_results "chore(hack): capacity model block $P, round $i" "${logs[@]}" "$rel/results/node-identity.txt"
done
bash nodeid.sh "$P-end"
commit_results "chore(hack): capacity model block $P finished" "$rel/results/node-identity.txt"
echo BLOCK-DONE
