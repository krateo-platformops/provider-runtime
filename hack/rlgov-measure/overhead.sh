#!/bin/bash
# The sampler's own cost in a container, at the chosen period of 1s, one run at a
# time: process CPU over 60s with the sampler running against 60s idle. Alternates
# the new code with v1.10.0, both with the memory input on. Runs after the block,
# so it doesn't disturb it. The re-run writes ovh2-* and uses ctrl3, so the
# first runs (ovh-*, against ctrl2) are never overwritten.
cd "$(dirname "$0")" || exit 1
A="SAT_PERIOD_MS=1000 PHASE_S=60 EXPERIMENTAL_MEMORY=true"
NEW=${1:?usage: overhead.sh <image tag of the fixed commit>}
bash nodeid.sh overhead2-start
for i in 1 2 3; do
  bash prun.sh "ovh2-new-$i" overhead 135 "$NEW" $A || exit 2
  bash prun.sh "ovh2-ctrl-$i" overhead 135 ctrl3 $A || exit 2
done
bash nodeid.sh overhead2-end
echo OVERHEAD-DONE
