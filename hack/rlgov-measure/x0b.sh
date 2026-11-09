#!/bin/bash
# Rest of the control check. x0 gave ramp 0, 1, 1 restarts and one clean cold
# start, so: is cold start clean on this node, and does the workload overcommit
# it with the governor off? One at a time.
cd "$(dirname "$0")" || exit 1
M="SATURATION=true EXPERIMENTAL_MEMORY=true"
bash nodeid.sh x0b-start
bash prun.sh x0b-off-ramp-1 ramp 240 ctrl
for i in 2 3 4; do
  bash prun.sh x0b-ctrl-cold-$i burst 240 ctrl $M
done
bash prun.sh x0b-off-cold-1 burst 240 ctrl
bash nodeid.sh x0b-end
echo X0B-DONE
