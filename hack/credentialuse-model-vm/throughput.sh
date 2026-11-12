#!/bin/bash
# Measures the split stateless search's throughput on a disposable VM in four
# layouts, for laying out a long run: the four big searches of step 2
# (DeletionsAroundTheGrace and AcquireAgainstRelease small, at {0,1} and
# {1,0}, T = 2), each with one worker per vCPU, either in one test process or
# in a process each, at the default GOGC and with GOGC=off under a memory
# limit. Each layout runs for $DUR (default 10m). User CPU and the test
# processes' total RSS are sampled every 20 s, and each search logs its
# progress every 50,000 schedules, so the rate can be read before and after
# steady state (for GOGC=off: once RSS reaches the memory limit).
#
# As hack/case-freeze's scripts do, it installs go1.26.10 from the tarball
# checked against the sha256 given, and however it ends writes ~/out/DONE,
# packs ~/out into ~/out.tar.gz and powers the VM off.
#
# Usage: throughput.sh <go sha256> <src.tar> [duration, default 5m]
set -uo pipefail
want=$1 tarf=$2 DUR=${3:-10m}
mkdir -p "$HOME/out"
finish() {
  rc=$?
  echo "exit $rc at $(date -u '+%Y-%m-%d %H:%M:%S UTC')" > "$HOME/out/DONE"
  tar -C "$HOME" -czf "$HOME/out.tar.gz" out
  sync
  sudo poweroff
}
trap finish EXIT
set -e
cd "$HOME"
if [ ! -x /usr/local/go/bin/go ]; then
  curl -fsSLO https://go.dev/dl/go1.26.10.linux-amd64.tar.gz
  echo "$want  go1.26.10.linux-amd64.tar.gz" | sha256sum -c -
  sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.26.10.linux-amd64.tar.gz
fi
export PATH=/usr/local/go/bin:$PATH GOTOOLCHAIN=local
dir="$HOME/src-${tarf%.tar}"; mkdir -p "$dir" && tar -C "$dir" -xf "$HOME/$tarf"; cd "$dir"
go test -c -o "$HOME/ct.test" ./pkg/credentialuse/
n=$(nproc); mem=$(free -g | awk '/Mem:/{print $2}')
echo "source $tarf, machine $(curl -fsS -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/machine-type | sed 's|.*/||'), $n vCPU, ${mem} GB, $DUR per layout" > "$HOME/out/header"
searches=('^deletions around the grace, small, \{0,1\}$' '^deletions around the grace, small, \{1,0\}$' '^acquire against release, \{0,1\}$' '^acquire against release, \{1,0\}$')
all='^(deletions around the grace, small|acquire against release), \{[01],[01]\}$'
cd "$dir/pkg/credentialuse"
sample() { for i in $(seq 1 $(( $(echo "$DUR" | sed 's/m//') * 3 - 1 ))); do sleep 20; r=$(ps -C ct.test -o rss= | awk '{s+=$1} END {printf "%.1f", s/1e6}'); top -b -n1 | sed -n 3p | awk -v t="$(date -u +%T)" -v r="$r" '{print t, $2"% user", r" GB"}'; done; }
run() { # name, GOGC, GOMEMLIMIT (total), layout (one|each)
  name=$1 gogc=$2 lim=$3 layout=$4
  echo "== $name: GOGC=$gogc GOMEMLIMIT=$lim layout=$layout, start $(date -u +%T) (epoch $(date -u +%s))" >> "$HOME/out/layouts.txt"
  sample > "$HOME/out/$name.cpu" &
  sp=$!
  if [ "$layout" = one ]; then
    env GOGC=$gogc GOMEMLIMIT=$lim CREDENTIALUSE_MODEL_THROUGHPUT=$DUR "CREDENTIALUSE_MODEL_THROUGHPUT_ONLY=$all" CREDENTIALUSE_MODEL_DIFF_WORKERS=$n CREDENTIALUSE_MODEL_PROGRESS_EVERY=50000 \
      "$HOME/ct.test" -test.run '^TestModelSplitThroughput$' -test.v -test.timeout 30m > "$HOME/out/$name.log" 2>&1
  else
    per=$(( ${lim%GiB} / 4 ))GiB; [ "$lim" = off ] && per=off
    i=0; for s in "${searches[@]}"; do
      env GOGC=$gogc GOMEMLIMIT=$per CREDENTIALUSE_MODEL_THROUGHPUT=$DUR "CREDENTIALUSE_MODEL_THROUGHPUT_ONLY=$s" CREDENTIALUSE_MODEL_DIFF_WORKERS=$n CREDENTIALUSE_MODEL_PROGRESS_EVERY=50000 \
        "$HOME/ct.test" -test.run '^TestModelSplitThroughput$' -test.v -test.timeout 30m > "$HOME/out/$name.$i.log" 2>&1 &
      i=$((i+1))
    done
    wait $(jobs -p | grep -v "^$sp$") 2>/dev/null || true
  fi
  kill $sp 2>/dev/null || true
  grep -h "throughput:" "$HOME/out/$name"*.log | sed "s/^ */$name /" >> "$HOME/out/layouts.txt"
  awk '{gsub("%","",$2); s+=$2; n++} END {printf "'"$name"' user CPU: mean %.1f%% over %d samples\n", s/n, n}' "$HOME/out/$name.cpu" >> "$HOME/out/layouts.txt"
}
lim=$(( mem * 7 / 10 ))GiB
run shared-gogc100 100 off one
run perprocess-gogc100 100 off each
run shared-gogcoff off $lim one
run perprocess-gogcoff off $lim each
