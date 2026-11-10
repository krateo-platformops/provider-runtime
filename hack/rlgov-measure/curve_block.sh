#!/bin/bash
# Usage: curve_block.sh <image tag> stage1
#        curve_block.sh <image tag> stage2 <n_a> <n_b> <m>
#
# The fixed-capacity plant curve of the long drain's workload, in the two
# stages of results/curve-settings.txt (committed before run 1). One run at a
# time, each at MaxConcurrentReconciles = n with no governor.
#
# stage1 runs the settings' 18-run randomized schedule. Then
#   python3 curve_report.py --stage1 results/pc-n*-r*.log > results/curve-stage1.txt
# computes k* and the decision, which is committed. stage2 runs m = k* − 4 more
# at each of the bracketing pair n_a and n_b, re-randomized, and refuses unless
# results/curve-stage1.txt is committed and says to continue with that pair and m.
#
# After each run its validity is checked (curve_report.py --check); a run that
# fails a check, or that prun.sh discards, is re-run at the same n and position,
# and both are kept and reported. The node's name and CPU model must be the
# same in every run: a change stops the block. A failed setup stops the block
# with exit code 2. Results are committed after each run, by explicit path.
#
# A committed log is never overwritten, so the block resumes where it stopped:
# run again with the same stage, it skips each position whose committed log
# passes the check and keeps the run names.
cd "$(dirname "$0")" || exit 1
bash setup.sh || { echo "BLOCK-STOPPED: cluster setup failed"; exit 2; }
TAG=${1:?usage: curve_block.sh <image tag> stage1|stage2 <n_a> <n_b> <m>}
STAGE=${2:?usage: curve_block.sh <image tag> stage1|stage2 <n_a> <n_b> <m>}
SECS=2100
root=$(git rev-parse --show-toplevel)
rel=hack/rlgov-measure

grep -q "^chosen budget: two-stage" results/curve-settings.txt || { echo "BLOCK-REFUSED: results/curve-settings.txt doesn't name the two-stage budget"; exit 2; }
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

case $STAGE in
  stage1)
    prefix=r
    schedule="1:4,3,7,5,6 2:8,4,5,6 3:3,5,4,6,7 4:5,4,6,8"
    ;;
  stage2)
    na=${3:?stage2 needs n_a}; nb=${4:?stage2 needs n_b}; m=${5:?stage2 needs m}
    git -C "$root" diff --quiet HEAD -- "$rel/results/curve-stage1.txt" && [ -s results/curve-stage1.txt ] || { echo "BLOCK-REFUSED: results/curve-stage1.txt is missing or uncommitted"; exit 2; }
    grep -q "^stage 2: continue with n=$na and n=$nb, $m more runs each\$" results/curve-stage1.txt || { echo "BLOCK-REFUSED: results/curve-stage1.txt doesn't say to continue with n=$na and n=$nb, $m more each"; exit 2; }
    prefix=s2-r
    # Re-randomized: each repetition holds n_a and n_b in an order drawn with
    # the settings' second seed.
    schedule=$(python3 -c "
import random
r = random.Random(20261111)
reps = []
for i in range($m):
    ns = [$na, $nb]
    r.shuffle(ns)
    reps.append('%d:%s' % (i + 1, ','.join(map(str, ns))))
print(' '.join(reps))")
    ;;
  *) echo "BLOCK-REFUSED: the stage is stage1 or stage2"; exit 2 ;;
esac
echo "schedule ($STAGE): $schedule"

for repn in $schedule; do
  r=${repn%%:*}
  list=${repn#*:}
  for m_ in ${list//,/ }; do
    attempt=1
    while true; do
      name="pc-n$m_-$prefix$r"
      [ "$attempt" -gt 1 ] && name="$name-a$attempt"
      # A committed log is never overwritten. On a resume, a position whose
      # committed log passes the check is done; one whose log fails it goes on
      # to its next attempt, as if it had just run. A log that exists but isn't
      # committed as it is stops the block: a person decides.
      if [ -e "results/$name.log" ]; then
        if ! git -C "$root" ls-files --error-unmatch "$rel/results/$name.log" >/dev/null 2>&1 ||
          ! git -C "$root" diff --quiet HEAD -- "$rel/results/$name.log"; then
          echo "BLOCK-REFUSED: results/$name.log exists but isn't committed as it is; it is not overwritten"
          exit 2
        fi
        if python3 curve_report.py --check "results/$name.log"; then
          echo "SKIP $name: its committed log passes the check"
          break
        fi
        echo "PRIOR $name failed a validity check; its next attempt follows"
        attempt=$((attempt + 1))
        [ "$attempt" -gt 3 ] && { echo "BLOCK-STOPPED: $name failed validity three times"; exit 2; }
        continue
      fi
      seed=$(printf '%s' "$name" | cksum | awk '{print $1}')
      bash prun.sh "$name" curve "$SECS" "$TAG" "WORKERS=$m_" "LOAD_SEED=$seed" || { echo "BLOCK-STOPPED at $name"; bash nodeid.sh curve-stopped; exit 2; }
      bash nodeid.sh "after-$name"
      commit_results "results(rlgov): plant curve run $name" "$rel/results/$name.log"
      # The node must be the same in every run.
      node=$(python3 curve_report.py --node "results/$name.log")
      if [ ! -s results/curve-node.txt ]; then
        echo "$node" > results/curve-node.txt
        commit_results "results(rlgov): the plant curve's node" "$rel/results/curve-node.txt"
      elif [ "$node" != "$(cat results/curve-node.txt)" ]; then
        echo "BLOCK-STOPPED: the node changed at $name: '$node', not '$(cat results/curve-node.txt)'"
        exit 2
      fi
      if python3 curve_report.py --check "results/$name.log"; then
        break
      fi
      echo "RE-RUN $name failed a validity check; re-running at n=$m_, repetition $r"
      attempt=$((attempt + 1))
      [ "$attempt" -gt 3 ] && { echo "BLOCK-STOPPED: $name failed validity three times"; exit 2; }
    done
  done
done
echo "BLOCK-DONE ($STAGE)"
