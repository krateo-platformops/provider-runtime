#!/bin/bash
# Usage: cpu_block.sh <image tag>
#
# The CPU threshold block, re-run. Settings approved by the coordinator on
# 2026-11-08 (remeasure-plan.txt); the cluster run waits for the user's approval.
#
# Does CPU threshold 50 prevent liveness kills, and at what cost? The first block
# (results/c-*.log, cpu-settings.txt, cpu-block.txt) tied: its Observe cost about
# 0.8s of CPU on GKE, not the 100ms intended, and with no wait in the call one
# call in flight kept 250m saturated, so local=1 couldn't lower the pressure.
# This re-run spends 20ms of measured CPU per Observe and then waits 400ms, so
# off saturates 250m with many calls in flight and the gate can bring it under.
#
# Three arms, 5 runs each, order rotating each round, one at a time, with the
# probe incident-provider ships. The settings and what counts are written to
# results/cpu2-settings.txt and committed before run 1. A run prun.sh discards
# stays in the count; a failed setup stops the block with exit code 2. Results
# are committed after each round, by explicit path. Runs are named c2-*, so the
# first block's c-* logs are never overwritten.
cd "$(dirname "$0")" || exit 1
NEW=${1:?usage: cpu_block.sh <image tag>}
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

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01N8HNbCRkKScdEeft3u5kfr" || echo "COMMIT-FAILED commit: $msg"
}

arm() { # name round
  local a=$1 i=$2 env=""
  case $a in
    off) env="" ;;
    75)  env="SATURATION=true SAT_CPU=75" ;;
    50)  env="SATURATION=true SAT_CPU=50" ;;
  esac
  # shellcheck disable=SC2086
  bash prun.sh "c2-$a-$i" cpu "$SECS" "$NEW" $env || { echo "BLOCK-STOPPED at c2-$a-$i"; bash nodeid.sh cpu2-stopped; exit 2; }
  bash nodeid.sh "after-c2-$a-$i"
}

[ -s "results/image-$NEW.txt" ] || { echo "BLOCK-REFUSED: no results/image-$NEW.txt"; exit 2; }
built=$(awk '{print $2}' "results/image-$NEW.txt")
if ! git -C "$root" diff --quiet "$built" HEAD -- pkg apis go.mod go.sum "$rel/main.go" "$rel/opts_new.go" "$rel/burn.go" "$rel/go.mod" "$rel/go.sum"; then
  echo "BLOCK-REFUSED: the source differs from what $NEW was built from ($built)"
  exit 2
fi

cat > results/cpu2-settings.txt <<EOF
CPU threshold block, re-run, started $(date -u +%Y-%m-%dT%H:%M:%SZ)
image: $(cat "results/image-$NEW.txt")
memory input: off in every arm
workload changed from the first block: that block couldn't separate the arms by construction. Its calls were CPU-bound with no time off the CPU (about 0.72s of CPU each, no wait), so one call in flight kept 250m saturated and no threshold could lower the pressure. This workload was decided before any run of this block, from the first block's logs and a model, not after seeing results. The two blocks' figures don't compare with each other.
workload: cpu mode, 250m limit, 512Mi, 64 Greetings, 16 workers, poll 1s, ${SECS}s per run. Each Observe spends 20ms of CPU measured on its own thread (LOAD_CPU_MS=20, burn.go), then waits 400ms (LOAD_HOLD_MS=400). The controller's own work adds a few ms per reconcile; the STAT lines report both.
expected for off: about 25ms of CPU a reconcile against 250ms a second, so at most about 10 reconciles a second, against the 37 that 16 workers would run unthrottled: about 12 of the 16 runnable at any moment, the quota spent in every period, pressure (cpu.pressure some avg10) about 75 to 85, as the first block's off arm read 79 to 81 with its workers always runnable.
expected for 50: four calls in flight use about the quota (4 x 25ms per 425ms), so local should settle between about 2 and 5, cutting when a fifth call saturates and recovering one step per 30s.
arms: off (SATURATION unset), 75 (SATURATION=true SAT_CPU=75), 50 (SATURATION=true SAT_CPU=50); 5 runs each
order: rotating. Round 1 off,75,50; round 2 75,50,off; round 3 50,off,75; round 4 off,75,50; round 5 75,50,off.

liveness probe: httpGet /healthz, no initial delay, periodSeconds 10, timeoutSeconds 1, failureThreshold 3, about 30s to a kill
source of the probe: incident-provider, helm/incident-provider/templates/deployment.yaml:68: httpGet /healthz with no period, timeout or failure set, so Kubernetes' defaults apply. Read from its default branch by the coordinator.
health server: controller-runtime's HealthProbeBindAddress with AddHealthzCheck and healthz.Ping, as incident-provider's main.go does.
scope of the result: a controller with this probe. It does not carry to consumers with none, such as git-provider.

validity, checked per run and reported, never used to drop a run: the median burn_cpu_ms/call is 20.0 to 20.5; in the off arm, CPU is at least 225ms/s (90% of the limit), 12 or more calls are in flight (median) and pressure is over 50. If the off arm doesn't saturate, the block shows nothing about the threshold, and that is the result.

what counts, decided before run 1:
benefit: the off arm gets liveness kills, and 50 removes or clearly cuts them, by cause. Clearly: off has 3 or more liveness kills across its 5 runs, and 50 has at most half as many. If the off arm has no liveness kills, the result is "no benefit shown here". The workload and the probe are not tuned to produce kills.
cost: Observes per second, median of 5 runs, against off; and failed probes that did not kill, per arm.
also reported: whether 75 engaged at all, as cuts, local below 16 and calls in flight. With the local gate in the queue (provider-runtime#40) a gated arm defers nothing for local capacity, so deferrals no longer show it.

driver power: $(pmset -g ps | head -1)
EOF
bash nodeid.sh cpu2-start
commit_results "chore(hack): settings for the CPU threshold block re-run" "$rel/results/cpu2-settings.txt" "$rel/results/node-identity.txt"

round() { # index, then the three arms in order
  local i=$1; shift
  for a in "$@"; do arm "$a" "$i"; done
  commit_results "chore(hack): CPU threshold block re-run, round $i" \
    "$rel/results/c2-off-$i.log" "$rel/results/c2-75-$i.log" "$rel/results/c2-50-$i.log" "$rel/results/node-identity.txt"
}
round 1 off 75 50
round 2 75 50 off
round 3 50 off 75
round 4 off 75 50
round 5 75 50 off
bash nodeid.sh cpu2-end
commit_results "chore(hack): CPU threshold block re-run finished" "$rel/results/node-identity.txt"
echo BLOCK-DONE
