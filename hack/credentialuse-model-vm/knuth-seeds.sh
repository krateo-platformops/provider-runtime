#!/bin/bash
# Runs TestModelStatelessSize's Knuth estimate as R independent processes on
# a disposable VM, each with its own probe seed, so the estimate's spread is
# measured between replicates and not read from one sample's own variance
# (which a heavy tail understates). Set up as remote-run.sh: go1.26.10 from
# the checked tarball, the source from a `git archive` tar in $HOME.
#
# Each replicate writes ~/out/knuth-<seed>.log. The capped search is cut to
# one schedule (CREDENTIALUSE_MODEL_SIZE_CAP=1): only the estimate is wanted.
# However it ends, it writes ~/out/DONE, packs ~/out into ~/out.tar.gz and
# powers the VM off.
#
# Usage: knuth-seeds.sh <go sha256> <src.tar> <replicates> <probes> <first seed> <scenario regexp> [jumps]
# e.g.   knuth-seeds.sh ccfe7ae8...c5 src-e5d5af9.tar 22 300000 1001 '^(acquire against release|deletions around the grace, small)$'
set -uo pipefail
want=$1 tarf=$2 reps=$3 probes=$4 first=$5 only=$6 jumps=${7:-2}
mkdir -p "$HOME/out"
finish() {
  rc=$?
  echo "exit $rc at $(date -u '+%Y-%m-%d %H:%M:%S UTC')" > "$HOME/out/DONE"
  uptime >> "$HOME/out/DONE"
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
dir="$HOME/src-${tarf%.tar}"
mkdir -p "$dir" && tar -C "$dir" -xf "$HOME/$tarf"
cd "$dir"
{
  echo "source $tarf, machine $(curl -fsS -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/machine-type | sed 's|.*/||'), $(nproc) vCPU"
  echo "start $(date -u '+%Y-%m-%d %H:%M:%S UTC'); $(uptime)"
  echo "replicates $reps, probes $probes each, seeds $first to $((first + reps - 1)), T $jumps, scenarios $only"
} > "$HOME/out/knuth.header"
go test -c -o "$HOME/credentialuse.test" ./pkg/credentialuse/
cd pkg/credentialuse
set +e
start=$(date +%s)
pids=()
for i in $(seq 0 $((reps - 1))); do
  seed=$((first + i))
  env CREDENTIALUSE_MODEL_SIZE=1 CREDENTIALUSE_MODEL_SIZE_PROBES="$probes" CREDENTIALUSE_MODEL_SIZE_CAP=1 \
    CREDENTIALUSE_MODEL_SIZE_JUMPS="$jumps" CREDENTIALUSE_MODEL_SIZE_ONLY="$only" CREDENTIALUSE_MODEL_SEED="$seed" \
    "$HOME/credentialuse.test" -test.v -test.timeout 600m -test.run '^TestModelStatelessSize$' \
    > "$HOME/out/knuth-$seed.log" 2>&1 &
  pids+=($!)
done
rc=0
for p in "${pids[@]}"; do
  wait "$p" || rc=$?
done
# CPU seconds of every replicate, from this shell's children.
t=$(times | tail -1)
echo "end $(date -u '+%Y-%m-%d %H:%M:%S UTC'); wall $(( $(date +%s) - start ))s; children user/sys $t; worst exit $rc; $(uptime)" >> "$HOME/out/knuth.header"
exit $rc
