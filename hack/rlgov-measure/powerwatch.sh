#!/bin/bash
# Usage: powerwatch.sh <pid of the block script> <log>
#
# Watches the driver's power while a block runs. Every 60s it appends the power
# source and battery level to <log>. Every 15s it checks: on battery below 25%,
# it stops the block script with SIGSTOP, writes PAUSED to <log>, prints it and
# exits. A run in progress is a child of the block script and finishes on its
# own; the block can't start another until it is resumed with SIGCONT. A pause
# is better than a run cut short by a dead battery. It also exits when the block
# script ends.
pid=${1:?usage: powerwatch.sh <pid> <log>}
log=${2:?usage: powerwatch.sh <pid> <log>}
n=0
while kill -0 "$pid" 2>/dev/null; do
  batt=$(pmset -g batt)
  source=$(echo "$batt" | head -1 | sed "s/^Now drawing from '//; s/'\$//")
  pct=$(echo "$batt" | grep -o '[0-9]*%' | head -1 | tr -d %)
  if [ $((n % 4)) = 0 ]; then
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $source ${pct}%" >> "$log"
  fi
  if [ "$source" != "AC Power" ] && [ -n "$pct" ] && [ "$pct" -lt 25 ]; then
    kill -STOP "$pid"
    msg="$(date -u +%Y-%m-%dT%H:%M:%SZ) PAUSED the block (pid $pid): $source at ${pct}%; the run in progress finishes, no new run starts until SIGCONT"
    echo "$msg" >> "$log"
    echo "$msg"
    exit 0
  fi
  n=$((n + 1))
  sleep 15
done
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) the block script ended" >> "$log"
echo "block ended"
