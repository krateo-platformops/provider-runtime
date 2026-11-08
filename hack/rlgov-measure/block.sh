#!/bin/bash
# Usage: block.sh <image tag of the fixed commit>
#
# The confirmation block: design §11 item 16. Settings are fixed before it starts
# and don't change during it. Every run is reported.
#
#   ramp: 20 new, 20 control. cold: 10 new, 10 control.
#
# One run at a time, alternating in ABBA order, so neither arm always runs first.
# Both arms run with the memory input on and nothing else set: the new arm is the
# commit its image was built from (results/image-<tag>.txt), the control is
# v1.10.0, d0632f1.
#
# results/block-settings.txt is written and committed before the first run. The
# block refuses to start if the source differs from what the image was built
# from. The node's identity is recorded after every run, so a repair or an upgrade
# is tied to a run. A run prun.sh discards stays in the count. A failed setup
# stops the block with exit code 2: nothing was measured, and the next run would
# fail the same way.
#
# Results are committed after each pair, by explicit path, and only if the staged
# list is exactly those files. A failed commit doesn't stop the block: the logs
# are on disk.
cd "$(dirname "$0")" || exit 1
NEW=${1:?usage: block.sh <image tag of the fixed commit>}
CTRL=ctrl2
M="SATURATION=true EXPERIMENTAL_MEMORY=true"
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

r() {
  bash prun.sh "$@" || { echo "BLOCK-STOPPED at $1"; bash nodeid.sh block-stopped; exit 2; }
  bash nodeid.sh "after-$1"
}

pair() { # index mode prefix
  local i=$1 mode=$2 p=$3
  if [ $((i % 2)) = 1 ]; then
    r "b-$p-new-$i" "$mode" 240 "$NEW" $M
    r "b-$p-ctrl-$i" "$mode" 240 "$CTRL" $M
  else
    r "b-$p-ctrl-$i" "$mode" 240 "$CTRL" $M
    r "b-$p-new-$i" "$mode" 240 "$NEW" $M
  fi
  commit_results "chore(hack): confirmation block, $p pair $i" \
    "$rel/results/b-$p-new-$i.log" "$rel/results/b-$p-ctrl-$i.log" "$rel/results/node-identity.txt"
}

for f in "results/image-$NEW.txt" "results/image-$CTRL.txt"; do
  [ -s "$f" ] || { echo "BLOCK-REFUSED: no $f"; exit 2; }
done
built=$(awk '{print $2}' "results/image-$NEW.txt")
if ! git -C "$root" diff --quiet "$built" HEAD -- pkg apis examples go.mod go.sum "$rel/main.go" "$rel/opts_new.go" "$rel/burn.go" "$rel/go.mod" "$rel/go.sum" "$rel/prun.sh"; then
  echo "BLOCK-REFUSED: the source differs from what $NEW was built from ($built)"
  exit 2
fi
if [ -n "$(git -C "$root" status --short -- pkg apis examples go.mod go.sum "$rel/main.go" "$rel/opts_new.go" "$rel/burn.go" "$rel/go.mod" "$rel/go.sum" "$rel/prun.sh")" ]; then
  echo "BLOCK-REFUSED: uncommitted changes to the source"
  exit 2
fi

{
  echo "block started $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "new arm:     $(cat "results/image-$NEW.txt")"
  echo "control arm: $(cat "results/image-$CTRL.txt")"
  echo "env, both arms: $M"
  echo "workload ramp: 16 workers, 16 MiB held 1s per Observe, poll 500ms, 2 Greetings every 5s from t=15, 256Mi, 2 CPU, 240s"
  echo "workload cold: 64 Greetings before the controller starts, 16 workers, 16 MiB held 300ms, poll 1s, 256Mi, 2 CPU, 240s"
  echo "new arm's settings, all defaults: Go limit 0.8 of memory.max, headroom soft cut 15%, sample 1s, CPU threshold 50, startup ramp 2s, recovery 30s"
  echo "control's settings, all defaults: working-set ratios 0.80/0.90, sample 1s, CPU threshold 50, startup ramp 2s, recovery 30s"
  echo "order: ABBA, ramp pairs 1 to 20, then cold pairs 1 to 10; pair i runs new first when i is odd"
  echo "driver power: $(pmset -g ps | head -1)"
  echo "driver sleep setting: $(pmset -g | grep -E '^ *sleep ' | cut -c1-100)"
} > results/block-settings.txt
bash nodeid.sh block-start
commit_results "chore(hack): settings for the confirmation block" "$rel/results/block-settings.txt" "$rel/results/node-identity.txt"

for i in $(seq 1 20); do pair "$i" ramp ramp; done
for i in $(seq 1 10); do pair "$i" burst cold; done
bash nodeid.sh block-end
commit_results "chore(hack): confirmation block finished" "$rel/results/node-identity.txt"
echo BLOCK-DONE
