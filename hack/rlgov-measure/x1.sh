#!/bin/bash
# Exploration 1, not a confirmation block. One run at a time, control first in
# each round. Tests the open point (limit at 0.8 against soft ratio 0.80) and
# the two headroom formulas.
cd "$(dirname "$0")" || exit 1
M="SATURATION=true EXPERIMENTAL_MEMORY=true"
bash nodeid.sh x1-start
for i in 1 2; do
  bash prun.sh x1-ctrl-$i ramp 240 ctrl $M
  bash prun.sh x1-ws80-$i ramp 240 new $M
  bash prun.sh x1-ws75-$i ramp 240 new $M LIMIT_RATIO=0.75
  bash prun.sh x1-live-$i ramp 240 new $M HEADROOM_FORMULA=live HEADROOM_SOFT=0.20 HEADROOM_HARD=0.10
  bash prun.sh x1-map-$i ramp 240 new $M HEADROOM_FORMULA=mapped HEADROOM_SOFT=0.10 HEADROOM_HARD=0.05
done
bash nodeid.sh x1-end
echo X1-DONE
