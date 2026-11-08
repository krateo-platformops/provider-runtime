#!/bin/bash
# The sampler's own cost in a container, at the chosen period of 1s, one run at a
# time: process CPU over 60s with the sampler running against 60s idle. Alternates
# the new code with v1.10.0, both with the memory input on. Runs after the block,
# so it doesn't disturb it.
cd "$(dirname "$0")" || exit 1
A="SAT_PERIOD_MS=1000 PHASE_S=60 EXPERIMENTAL_MEMORY=true"
NEW=${1:?usage: overhead.sh <image tag of the fixed commit>}
bash nodeid.sh overhead-start
for i in 1 2 3; do
  bash prun.sh "ovh-new-$i" overhead 135 "$NEW" $A || exit 2
  bash prun.sh "ovh-ctrl-$i" overhead 135 ctrl2 $A || exit 2
done
bash nodeid.sh overhead-end
echo OVERHEAD-DONE
