#!/bin/bash
# Smoke check, in no count: incident-provider's probe reaches the cluster, in the
# cpu workload with the memory input off, and the controller answers it.
cd "$(dirname "$0")" || exit 1
RLGOV_PROBE=incident-provider bash prun.sh x5-probe cpu 70 final2 SATURATION=true SAT_CPU=50 || exit 2
echo X5-DONE
