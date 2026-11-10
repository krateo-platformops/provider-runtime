#!/bin/bash
# Usage: curve_block.sh <image tag> <full|small>
#
# The fixed-capacity plant curve of the long drain's workload (results/
# curve-settings.txt, committed before run 1). One run at a time, in the
# settings' randomized order, each at MaxConcurrentReconciles = n with no
# governor. After each run its validity is checked (curve_report.py --check);
# a run that fails a check, or that prun.sh discards, is re-run at the same n
# and position, and both are kept and reported. A failed setup stops the block
# with exit code 2. Results are committed after each run, by explicit path.
#
# It refuses to start unless the settings name the chosen budget, and unless
# the source matches what the image was built from.
cd "$(dirname "$0")" || exit 1
bash setup.sh || { echo "BLOCK-STOPPED: cluster setup failed"; exit 2; }
TAG=${1:?usage: curve_block.sh <image tag> <full|small>}
BUDGET=${2:?usage: curve_block.sh <image tag> <full|small>}
SECS=2100
root=$(git rev-parse --show-toplevel)
rel=hack/rlgov-measure

grep -q "^chosen budget: $BUDGET\$" results/curve-settings.txt || { echo "BLOCK-REFUSED: results/curve-settings.txt doesn't name '$BUDGET' as the chosen budget"; exit 2; }
git -C "$root" diff --quiet HEAD -- "$rel/results/curve-settings.txt" || { echo "BLOCK-REFUSED: results/curve-settings.txt has uncommitted changes"; exit 2; }
[ -s "results/image-$TAG.txt" ] || { echo "BLOCK-REFUSED: no results/image-$TAG.txt"; exit 2; }
built=$(awk '{print $2}' "results/image-$TAG.txt")
if ! git -C "$root" diff --quiet "$built" HEAD -- pkg apis go.mod go.sum "$rel/main.go" "$rel/opts_main.go" "$rel/burn.go" "$rel/go.mod" "$rel/go.sum"; then
  echo "BLOCK-REFUSED: the source differs from what $TAG was built from ($built)"
  exit 2
fi

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
Claude-Session: https://claude.ai/code/session_01NSM9VXZsNd3K23XxTSQGo6" || echo "COMMIT-FAILED commit: $msg"
}

case $BUDGET in
  small) schedule="1:4,3,7,5,6 2:8,4,5,6 3:3,5,4,6,7 4:5,4,6,8" ;;
  full)  schedule="1:5,4,3,6 2:4,5,6,7 3:8,5,4,6 4:4,6,3,5 5:4,6,7,5 6:5,4,6,8 7:4,3,6,5 8:7,6,4,5 9:6,4,8,5 10:6,5,4 11:6,4,5 12:5,6,4 13:4,5,6" ;;
  *) echo "BLOCK-REFUSED: budget is full or small"; exit 2 ;;
esac

for repn in $schedule; do
  r=${repn%%:*}
  for n in ${repn#*:}; do
    n=${n//,/ }
    for m in $n; do
      attempt=1
      while true; do
        name="pc-n$m-r$r"
        [ "$attempt" -gt 1 ] && name="pc-n$m-r$r-a$attempt"
        bash prun.sh "$name" curve "$SECS" "$TAG" "WORKERS=$m" || { echo "BLOCK-STOPPED at $name"; bash nodeid.sh curve-stopped; exit 2; }
        bash nodeid.sh "after-$name"
        commit_results "results(rlgov): plant curve run $name" "$rel/results/$name.log"
        if python3 curve_report.py --check "results/$name.log"; then
          break
        fi
        echo "RE-RUN $name failed a validity check; re-running at n=$m, repetition $r"
        attempt=$((attempt + 1))
        [ "$attempt" -gt 3 ] && { echo "BLOCK-STOPPED: $name failed validity three times"; exit 2; }
      done
    done
  done
done
echo "BLOCK-DONE: python3 curve_report.py results/pc-*.log"
