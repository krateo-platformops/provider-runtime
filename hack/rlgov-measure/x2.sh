#!/bin/bash
# Exploration 2, not a confirmation block. One run at a time. Finishes x1's second
# round (ws75, live, map) and adds what x1 pointed at: the limit alone with the
# working-set ratios (ws80), and live headroom with a soft cut only (lsoft), since
# the 10% hard cut dropped capacity from 16 to 1. A failed setup stops the block.
cd "$(dirname "$0")" || exit 1
M="SATURATION=true EXPERIMENTAL_MEMORY=true"
LS="HEADROOM_FORMULA=live HEADROOM_SOFT=0.15 HEADROOM_HARD=0"
r() { bash prun.sh "$@" || { echo "BLOCK-STOPPED at $1"; bash nodeid.sh x2-stopped; exit 2; }; }
bash nodeid.sh x2-start
for i in 1 2 3; do
  r x2-ctrl-$i ramp 240 ctrl $M
  r x2-ws80-$i ramp 240 new $M
  r x2-lsoft-$i ramp 240 new $M $LS
done
r x1-ws75-2 ramp 240 new $M LIMIT_RATIO=0.75
r x1-live-2 ramp 240 new $M HEADROOM_FORMULA=live HEADROOM_SOFT=0.20 HEADROOM_HARD=0.10
r x1-map-2 ramp 240 new $M HEADROOM_FORMULA=mapped HEADROOM_SOFT=0.10 HEADROOM_HARD=0.05
for i in 1 2; do
  r x2-ws80-cold-$i burst 240 new $M
  r x2-lsoft-cold-$i burst 240 new $M $LS
done
bash nodeid.sh x2-end
echo X2-DONE
