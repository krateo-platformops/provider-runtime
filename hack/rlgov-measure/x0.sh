#!/bin/bash
# Does the control fail on this node? v1.10.0 with the memory input on. Three
# ramp runs, one cold start, one at a time. If it runs clean the workload doesn't
# overcommit this node and the bar can't be judged.
cd "$(dirname "$0")" || exit 1
M="SATURATION=true EXPERIMENTAL_MEMORY=true"
bash nodeid.sh x0-start
for i in 1 2 3; do
  bash prun.sh x0-ctrl-$i ramp 240 ctrl $M
done
bash prun.sh x0-ctrl-cold-1 burst 240 ctrl $M
bash nodeid.sh x0-end
echo X0-DONE
