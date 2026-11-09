#!/bin/bash
# Smoke check, not a confirmation run: the final commit with the plain flag gives
# the measured behaviour in a real container. Sampler's reasons and limit are in
# each log's LOG lines.
cd "$(dirname "$0")" || exit 1
M="SATURATION=true EXPERIMENTAL_MEMORY=true"
bash prun.sh x3-final-ramp ramp 240 final1 $M || exit 2
bash prun.sh x3-final-cold burst 240 final1 $M || exit 2
bash nodeid.sh x3-end
echo X3-DONE
