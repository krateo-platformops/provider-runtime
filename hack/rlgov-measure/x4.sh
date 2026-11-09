#!/bin/bash
# Smoke check of the two images the block uses, 90s each, in no count: does the
# renamed flag reach each build? Read each log's LOG lines and inputs.
cd "$(dirname "$0")" || exit 1
M="SATURATION=true EXPERIMENTAL_MEMORY=true"
bash prun.sh x4-final2 ramp 90 final2 $M || exit 2
bash prun.sh x4-ctrl2 ramp 90 ctrl2 $M || exit 2
bash nodeid.sh x4-end
echo X4-DONE
